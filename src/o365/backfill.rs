//! Initial seed of the archive from Office 365. The initial delta walk of
//! each folder *is* the backfill: poll needs a delta link per folder and
//! only a completed walk produces one, so a separate listing would just
//! double the enumeration. Resumability is per folder and per page
//! (`o365_folders.next_link`); a finished folder is skipped on resume.
//!
//! Delta order is not newest-first, so a `limit`-capped smoke test
//! returns "some" messages rather than the newest; promotion orders by
//! received_at, so the archive is unaffected.
//!
//! `since` becomes a `receivedDateTime` floor baked into the delta link:
//! future mail always qualifies, but an older message the user later
//! moves into a folder is not reported. The floor is recorded in
//! `o365_state.backfill_since`; a later backfill asking for an earlier
//! (or no) floor re-walks every folder.

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::db::{accounts, audit, messages, o365_folders};
use crate::ingest::{BackfillOptions, BackfillStats};
use crate::maildir::MessageStore;
use crate::o365::sync::{Ingested, WalkOptions, ingest_item, walk_initial_delta};
use crate::o365::{O365Client, O365Error, list_archivable};

/// `job`: when present, per-page progress is written to that jobs row
/// and its cancel flag is honored at page boundaries.
pub async fn backfill_account(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    options: &BackfillOptions,
    job: Option<i64>,
) -> Result<BackfillStats> {
    let state = accounts::get_o365_state(pool, account.id)
        .await?
        .context("account has no o365_state row")?;

    // A wider floor than the folders were walked with invalidates every
    // delta link (the old floor is baked into them).
    let widen = match (options.since, state.backfill_since) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(requested), Some(stored)) => requested < stored,
    };
    let since = if widen {
        options.since
    } else {
        state.backfill_since.or(options.since)
    };
    if widen {
        tracing::info!(
            account = account.id,
            "backfill floor widened; re-walking every folder"
        );
        o365_folders::clear_all_cursors(pool, account.id).await?;
        accounts::set_o365_backfill_done(pool, account.id, false).await?;
    }
    if since != state.backfill_since {
        accounts::set_o365_backfill_since(pool, account.id, since).await?;
    }

    let live = list_archivable(client).await?;
    for f in &live {
        o365_folders::upsert(
            pool,
            account.id,
            &f.id,
            &f.path,
            f.well_known_name.as_deref(),
            f.total_item_count,
        )
        .await?;
    }
    let mut stats = BackfillStats {
        total: Some(
            live.iter()
                .filter_map(|f| f.total_item_count)
                .map(|c| c.max(0) as u64)
                .sum(),
        ),
        ..Default::default()
    };

    // Inbox first, then by path: predictable smoke tests.
    let mut ordered = live.clone();
    ordered.sort_by(|a, b| {
        let a_inbox = a.well_known_name.as_deref() == Some("inbox");
        let b_inbox = b.well_known_name.as_deref() == Some("inbox");
        b_inbox.cmp(&a_inbox).then_with(|| a.path.cmp(&b.path))
    });
    for f in &ordered {
        let row = o365_folders::get(pool, account.id, &f.id)
            .await?
            .context("folder row vanished mid-backfill")?;
        if row.delta_link.is_some() {
            continue;
        }
        let remaining = options.limit.map(|l| l.saturating_sub(stats.fetched));
        if remaining == Some(0) {
            return Ok(stats);
        }
        let out = walk_initial_delta(
            pool,
            client,
            store,
            account,
            f,
            &WalkOptions {
                since,
                remaining,
                job,
                collect_seen: false,
            },
            &mut stats,
        )
        .await?;
        if out.cancelled {
            stats.cancelled = true;
            return Ok(stats);
        }
        if !out.complete {
            return Ok(stats);
        }
    }

    let rows = o365_folders::list(pool, account.id).await?;
    stats.complete = live.iter().all(|f| {
        rows.iter()
            .any(|r| r.folder_id == f.id && r.delta_link.is_some())
    });
    if stats.complete && options.limit.is_none() {
        accounts::set_o365_backfill_done(pool, account.id, true).await?;
    }
    Ok(stats)
}

/// Retry messages whose metadata landed but whose body was never durably
/// stored (`maildir_path IS NULL`). A message that 404s here vanished
/// upstream before we ever stored it — nothing to retain, so the ledger
/// row is removed (with an audit record) rather than left permanently
/// unfetchable.
pub async fn fetch_missing_blobs(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
) -> Result<u64> {
    let mut total = 0u64;
    loop {
        let missing = messages::unfetched(pool, account.id, 500).await?;
        if missing.is_empty() {
            return Ok(total);
        }
        let mut progressed = 0u64;
        for row in &missing {
            let meta = match client.get_message_meta(&row.blob_id).await {
                Ok(meta) => Some(meta),
                Err(O365Error::Status(404, _)) => None,
                Err(e) => {
                    tracing::warn!(
                        message = %row.blob_id,
                        error = %e,
                        "fetch_missing_blobs: metadata fetch failed, leaving deferred"
                    );
                    continue;
                }
            };
            let outcome = match meta {
                None => Ok(Ingested::Skipped),
                Some(meta) => {
                    // Prefer the folder the message is in now; fall back
                    // to the placement recorded when the metadata landed.
                    let current = match meta.parent_folder_id.as_deref() {
                        Some(pid) => o365_folders::get(pool, account.id, pid)
                            .await?
                            .map(|f| f.display_path),
                        None => None,
                    };
                    let recorded = row
                        .mailbox_ids
                        .as_array()
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    let path = current.or(recorded).unwrap_or_default();
                    ingest_item(pool, client, store, account.id, &meta, &path).await
                }
            };
            match outcome {
                Ok(Ingested::Fetched) => {
                    total += 1;
                    progressed += 1;
                }
                Ok(Ingested::Refreshed) => {}
                Ok(Ingested::Skipped) => {
                    messages::delete_row(pool, row.id).await?;
                    audit::record(
                        pool,
                        Some(account.user_id),
                        "system:backfill",
                        "message_vanished_before_fetch",
                        Some(&row.id.to_string()),
                        None,
                    )
                    .await?;
                    progressed += 1;
                }
                Err(e) => tracing::warn!(
                    message = %row.blob_id,
                    error = %format!("{e:#}"),
                    "fetch_missing_blobs: message still failing, leaving deferred"
                ),
            }
        }
        // A pass that recovered nothing means the remainder is wedged
        // upstream; stop rather than spin.
        if progressed == 0 {
            return Ok(total);
        }
    }
}
