//! Initial seed of the archive from IMAP: one pass over every archivable
//! folder, oldest UID first within each. Downloads are sequential — one
//! IMAP session is one command pipeline, unlike the Gmail engine's
//! concurrent HTTP fetches.
//!
//! Resumability: the cursor persisted in imap_state.backfill_cursor holds
//! the frozen folder list, the index of the folder in progress, and the
//! last UID whose page was durably stored. If a folder's UIDVALIDITY
//! changes mid-resume its walk restarts from UID 0 — idempotent upserts
//! and the content-hash identity absorb the overlap.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::db::accounts;
use crate::imap::ImapClient;
use crate::imap::sync::{HEADER_BATCH, Ingested, ingest_uid, is_archivable};
use crate::ingest::{BackfillOptions, BackfillStats};
use crate::maildir::MessageStore;

#[derive(Debug, Default)]
struct Cursor {
    folders: Vec<String>,
    folder_index: usize,
    uidvalidity: i64,
    last_uid: i64,
}

impl Cursor {
    fn from_json(value: Option<&serde_json::Value>) -> Self {
        let Some(value) = value else {
            return Self::default();
        };
        Self {
            folders: value
                .get("folders")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            folder_index: value
                .get("folder_index")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
            uidvalidity: value
                .get("uidvalidity")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            last_uid: value.get("last_uid").and_then(|v| v.as_i64()).unwrap_or(0),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "folders": self.folders,
            "folder_index": self.folder_index,
            "uidvalidity": self.uidvalidity,
            "last_uid": self.last_uid,
        })
    }
}

/// `job`: when present, per-page progress is written to that jobs row
/// and its cancel flag is honored at page boundaries (the persisted
/// cursor makes a later run resume where a cancelled one stopped).
pub async fn backfill_account(
    pool: &PgPool,
    client: &ImapClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    options: &BackfillOptions,
    job: Option<i64>,
) -> Result<BackfillStats> {
    let state = accounts::get_imap_state(pool, account.id)
        .await?
        .context("account has no imap_state row")?;

    let mut cursor = Cursor::from_json(state.backfill_cursor.as_ref());
    if cursor.folders.is_empty() {
        // Freeze the folder list up front so resume walks the same set;
        // folders created mid-backfill are picked up by the first poll.
        cursor.folders = client
            .list_folders()
            .await?
            .into_iter()
            .filter(is_archivable)
            .map(|f| f.name)
            .collect();
        cursor.folders.sort_unstable();
        accounts::set_imap_backfill_cursor(pool, account.id, Some(&cursor.to_json())).await?;
    }

    let mut stats = BackfillStats::default();
    'folders: while cursor.folder_index < cursor.folders.len() {
        let folder = cursor.folders[cursor.folder_index].clone();
        let status = match client.examine(&folder).await {
            Ok(status) => status,
            Err(e) => {
                // A folder deleted or renamed mid-backfill must not wedge
                // the sweep; the first poll reconciles whatever replaced it.
                tracing::warn!(
                    account = account.id,
                    folder = %folder,
                    error = %e,
                    "backfill: folder not selectable, skipping"
                );
                cursor.folder_index += 1;
                cursor.uidvalidity = 0;
                cursor.last_uid = 0;
                accounts::set_imap_backfill_cursor(pool, account.id, Some(&cursor.to_json()))
                    .await?;
                continue;
            }
        };
        if cursor.last_uid > 0 && cursor.uidvalidity != status.uidvalidity as i64 {
            tracing::warn!(
                account = account.id,
                folder = %folder,
                "backfill: UIDVALIDITY changed mid-resume, restarting folder"
            );
            cursor.last_uid = 0;
        }
        cursor.uidvalidity = status.uidvalidity as i64;
        crate::db::imap_map::upsert_folder(
            pool,
            account.id,
            &folder,
            status.uidvalidity as i64,
            status.uidnext as i64,
        )
        .await?;

        let entries = client.uid_list(status.exists).await?;
        let flags_by_uid: HashMap<i64, Vec<String>> = entries
            .iter()
            .map(|e| (e.uid as i64, e.flags.clone()))
            .collect();
        let since_set: Option<HashSet<u32>> = match options.since {
            Some(since) => Some(client.uid_search_since(since).await?.into_iter().collect()),
            None => None,
        };
        let mut uids: Vec<i64> = entries
            .iter()
            .map(|e| e.uid as i64)
            .filter(|&uid| uid > cursor.last_uid)
            .filter(|&uid| {
                since_set
                    .as_ref()
                    .map(|s| s.contains(&(uid as u32)))
                    .unwrap_or(true)
            })
            .collect();
        uids.sort_unstable();

        let page_size = (options.page_size as usize).clamp(1, HEADER_BATCH);
        for page in uids.chunks(page_size) {
            let remaining = options.limit.map(|l| l.saturating_sub(stats.fetched));
            if remaining == Some(0) {
                break 'folders;
            }

            let peek: Vec<u32> = page.iter().map(|&u| u as u32).collect();
            let headers = client.fetch_message_id_headers(&peek).await?;
            static EMPTY: Vec<String> = Vec::new();
            for &uid in page {
                let hdr = headers.get(&(uid as u32)).cloned().flatten();
                let flags = flags_by_uid.get(&uid).unwrap_or(&EMPTY);
                // Collect every result rather than aborting on the first
                // error: failures whose metadata landed are recovered by
                // fetch_missing_blobs; the rest by the restart overlap.
                match ingest_uid(
                    pool,
                    client,
                    store,
                    account,
                    &folder,
                    status.uidvalidity,
                    uid as u32,
                    flags,
                    hdr.as_deref(),
                )
                .await
                {
                    Ok(Ingested::Fetched) => stats.fetched += 1,
                    Ok(Ingested::Rehomed | Ingested::Refreshed | Ingested::Skipped) => {}
                    Err(e) => {
                        stats.failed += 1;
                        tracing::warn!(
                            folder = %folder,
                            uid,
                            error = %format!("{e:#}"),
                            "backfill: message ingest failed, deferring to fetch_missing_blobs"
                        );
                    }
                }
            }
            stats.pages += 1;

            // Page durable: advance the resume cursor.
            cursor.last_uid = *page.last().expect("non-empty page");
            accounts::set_imap_backfill_cursor(pool, account.id, Some(&cursor.to_json())).await?;

            if let Some(job_id) = job {
                crate::db::jobs::update_stats(pool, job_id, &serde_json::to_value(&stats)?).await?;
                if crate::db::jobs::is_cancel_requested(pool, job_id).await? {
                    stats.cancelled = true;
                    return Ok(stats);
                }
            }
        }

        cursor.folder_index += 1;
        cursor.uidvalidity = 0;
        cursor.last_uid = 0;
        accounts::set_imap_backfill_cursor(pool, account.id, Some(&cursor.to_json())).await?;
    }

    if cursor.folder_index >= cursor.folders.len() {
        stats.complete = true;
    }
    if stats.complete && options.limit.is_none() && options.since.is_none() {
        accounts::set_imap_backfill_done(pool, account.id).await?;
    }
    Ok(stats)
}
