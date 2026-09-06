//! Office 365 sync engine over Graph message delta. Same crash-safety
//! contract as the other engines (spec §7.3): a folder's cursor advances
//! only after the corresponding batch is durably stored, and every step
//! is idempotent.
//!
//! Identity is the Graph immutable id (`Prefer: IdType="ImmutableId"`),
//! stored in `messages.jmap_email_id`/`blob_id`; it survives folder
//! moves. Placement is the folder's display path, the single element of
//! `messages.mailbox_ids`. Delta is per folder, so a move shows up as a
//! tombstone in the source folder and an add in the destination with
//! the same id. A poll therefore runs in two phases, like IMAP: apply
//! every folder's adds and updates first, then the buffered removes —
//! and a remove destroys a row only if it is still placed in that
//! folder *and* a metadata fetch confirms the message is gone (or sits
//! in a non-archivable folder such as Deleted Items). That verify step
//! closes the race where the destination folder's delta was read before
//! the move landed.
//!
//! A folder's incremental delta link is persisted only after its
//! removes are applied: a crash in between re-walks the folder from the
//! old link next run, which costs metadata paging only. Initial walks
//! (backfill, new folders, expired-token resyncs) carry no tombstones
//! and persist their `nextLink` per page.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use mail_parser::MessageParser;
use sqlx::PgPool;

use crate::config::DeletionPolicy;
use crate::db::o365_folders::O365Folder;
use crate::db::{accounts, messages, o365_folders};
use crate::ingest::{BackfillStats, SyncStats, apply_destroyed};
use crate::maildir::MessageStore;
use crate::o365::client::DELTA_PAGE_SIZE;
use crate::o365::types::DeltaItem;
use crate::o365::{
    ArchivableFolder, DOWNLOAD_CONCURRENCY, O365Client, O365Error, checkpoint_token,
    list_archivable,
};
use crate::search::SearchClient;

/// What one [`ingest_item`] call did.
#[derive(Debug)]
pub enum Ingested {
    /// Body downloaded and durably stored under a new or existing row.
    Fetched,
    /// Body was already stored; placement and keywords refreshed.
    Refreshed,
    /// The message vanished upstream between the listing and the fetch.
    Skipped,
}

/// JMAP-style keywords from the delta item's flag properties.
pub fn keywords_from(item: &DeltaItem) -> serde_json::Value {
    let mut keywords = serde_json::Map::new();
    if item.is_read == Some(true) {
        keywords.insert("$seen".into(), serde_json::Value::Bool(true));
    }
    if item.is_flagged() {
        keywords.insert("$flagged".into(), serde_json::Value::Bool(true));
    }
    if item.is_draft == Some(true) {
        keywords.insert("$draft".into(), serde_json::Value::Bool(true));
    }
    serde_json::Value::Object(keywords)
}

pub fn placement(path: &str) -> serde_json::Value {
    serde_json::json!([path])
}

fn bracketed(id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.starts_with('<') {
        trimmed.to_string()
    } else {
        format!("<{trimmed}>")
    }
}

/// Full metadata from the raw RFC822 bytes plus the delta envelope.
/// Header conventions match the other engines (angle-bracketed
/// Message-ID via mail-parser).
fn meta_from_raw(item: &DeltaItem, raw: &[u8], path: &str) -> messages::MessageMeta {
    let parsed = MessageParser::default().parse(raw);
    let message_id_hdr = parsed
        .as_ref()
        .and_then(|m| m.message_id())
        .map(bracketed)
        .or_else(|| item.internet_message_id.as_deref().map(bracketed));
    let from_addr = parsed.as_ref().and_then(|m| {
        m.from()
            .and_then(|a| a.first())
            .and_then(|a| a.address())
            .map(str::to_string)
    });
    let subject = parsed
        .as_ref()
        .and_then(|m| m.subject())
        .map(str::to_string);
    let has_attachments = parsed
        .as_ref()
        .map(|m| m.attachment_count() > 0)
        .unwrap_or(false);
    let received_at = item
        .received_date_time
        .or_else(|| {
            parsed
                .as_ref()
                .and_then(|m| m.date())
                .and_then(|d| DateTime::from_timestamp(d.to_timestamp(), 0))
        })
        .unwrap_or_default();
    messages::MessageMeta {
        jmap_email_id: item.id.clone(),
        blob_id: item.id.clone(),
        message_id_hdr,
        thread_id: item.conversation_id.clone(),
        received_at,
        size: raw.len() as i64,
        has_attachments,
        from_addr,
        subject,
        mailbox_ids: placement(path),
        keywords: keywords_from(item),
    }
}

/// Metadata for refreshing an already-stored message. Only the fields
/// `upsert_meta` touches on conflict (blob_id, mailbox_ids, keywords)
/// matter here.
fn meta_for_refresh(
    item: &DeltaItem,
    row: &messages::Message,
    path: &str,
) -> messages::MessageMeta {
    messages::MessageMeta {
        jmap_email_id: item.id.clone(),
        blob_id: item.id.clone(),
        message_id_hdr: row.message_id_hdr.clone(),
        thread_id: row.thread_id.clone(),
        received_at: row.received_at,
        size: row.size,
        has_attachments: row.has_attachments,
        from_addr: row.from_addr.clone(),
        subject: row.subject.clone(),
        mailbox_ids: placement(path),
        keywords: keywords_from(item),
    }
}

/// Fetch and durably store one message, or refresh its placement and
/// keywords if the body is already stored. Idempotent, and tolerant of
/// the message vanishing upstream (404 → [`Ingested::Skipped`]; the
/// tombstone arrives via delta). The delta item already carries every
/// property a refresh needs, so a stored row costs no API call.
pub async fn ingest_item(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    mail_account_id: i64,
    item: &DeltaItem,
    path: &str,
) -> Result<Ingested> {
    let existing = messages::get_by_jmap_id(pool, mail_account_id, &item.id).await?;
    if let Some(row) = &existing
        && row.maildir_path.is_some()
    {
        messages::upsert_meta(pool, mail_account_id, &meta_for_refresh(item, row, path)).await?;
        return Ok(Ingested::Refreshed);
    }

    let raw = match client.get_message_raw(&item.id).await {
        Ok(raw) => raw,
        Err(O365Error::Status(404, _)) => return Ok(Ingested::Skipped),
        Err(e) => return Err(e).with_context(|| format!("fetching graph message {}", item.id)),
    };
    // Placement lands before the blob so fetch_missing_blobs can retry.
    let row_id =
        messages::upsert_meta(pool, mail_account_id, &meta_from_raw(item, &raw, path)).await?;
    let rel_path = store.write(&item.id, &raw)?;
    let mut conn = pool.acquire().await?;
    messages::set_stored(&mut conn, row_id, &rel_path).await?;
    Ok(Ingested::Fetched)
}

pub struct WalkOptions {
    /// `receivedDateTime` floor for a fresh walk (baked into the delta
    /// link; ignored when resuming a stored link).
    pub since: Option<DateTime<Utc>>,
    /// Fetch budget for this walk; None = unbounded.
    pub remaining: Option<u64>,
    /// Jobs row to report progress to and honor cancellation from.
    pub job: Option<i64>,
    /// Collect the ids seen, for a post-walk diff.
    pub collect_seen: bool,
}

#[derive(Debug, Default)]
pub struct WalkOutcome {
    pub fetched: u64,
    pub refreshed: u64,
    pub failed: u64,
    pub pages: u64,
    /// The folder now has a delta link.
    pub complete: bool,
    pub cancelled: bool,
    pub seen_ids: HashSet<String>,
}

/// Walk a folder's initial delta (no tombstones), resuming from a stored
/// `next_link`, and leave it with a `delta_link`. The cursor advances
/// per durable page; `stats` accumulates fetched/pages/failed for the
/// caller's job record. A stored link the server rejects restarts the
/// folder from scratch (idempotent: stored rows refresh without a
/// download). A rejected `receivedDateTime` filter falls back to an
/// unfiltered walk with client-side date filtering.
pub async fn walk_initial_delta(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    folder: &ArchivableFolder,
    opts: &WalkOptions,
    stats: &mut BackfillStats,
) -> Result<WalkOutcome> {
    let row = o365_folders::get(pool, account.id, &folder.id)
        .await?
        .with_context(|| format!("folder {} has no o365_folders row", folder.path))?;
    let mut link = row.next_link.clone();
    let mut unfiltered = false;
    let mut outcome = WalkOutcome::default();
    loop {
        let remaining = opts.remaining.map(|l| l.saturating_sub(outcome.fetched));
        if remaining == Some(0) {
            return Ok(outcome);
        }
        let page_size = remaining
            .map(|r| r.min(DELTA_PAGE_SIZE as u64) as u32)
            .unwrap_or(DELTA_PAGE_SIZE);
        let since = if unfiltered { None } else { opts.since };
        let page = match client
            .delta_page(&folder.id, link.as_deref(), since, page_size)
            .await
        {
            Ok(page) => page,
            Err(O365Error::DeltaExpired | O365Error::Status(400, _)) if link.is_some() => {
                tracing::warn!(
                    account = account.id,
                    folder = %folder.path,
                    "stored nextLink rejected; restarting the folder walk"
                );
                link = None;
                o365_folders::set_next_link(pool, account.id, &folder.id, None).await?;
                continue;
            }
            Err(O365Error::Status(400, body)) if since.is_some() => {
                tracing::warn!(
                    account = account.id,
                    folder = %folder.path,
                    body = %body,
                    "receivedDateTime filter rejected; walking unfiltered with client-side floor"
                );
                unfiltered = true;
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let items: Vec<DeltaItem> = page
            .value
            .into_iter()
            .filter(|i| !i.is_removed())
            .filter(|i| {
                !unfiltered
                    || opts.since.is_none_or(|floor| {
                        i.received_date_time
                            .is_none_or(|received| received >= floor)
                    })
            })
            .collect();
        if opts.collect_seen {
            outcome.seen_ids.extend(items.iter().map(|i| i.id.clone()));
        }

        // Collect every result rather than aborting on the first error:
        // one failing message must not abort a sweep of thousands.
        // Failures whose metadata landed are recovered by
        // `fetch_missing_blobs`; the rest by the next walk of the folder.
        let path = folder.path.as_str();
        let results: Vec<Result<Ingested>> =
            stream::iter(items)
                .map(|item| async move {
                    ingest_item(pool, client, store, account.id, &item, path).await
                })
                .buffer_unordered(DOWNLOAD_CONCURRENCY)
                .collect()
                .await;
        for result in results {
            match result {
                Ok(Ingested::Fetched) => {
                    stats.fetched += 1;
                    outcome.fetched += 1;
                }
                Ok(Ingested::Refreshed) => outcome.refreshed += 1,
                Ok(Ingested::Skipped) => {}
                Err(e) => {
                    stats.failed += 1;
                    outcome.failed += 1;
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        "o365: message ingest failed, deferring to fetch_missing_blobs"
                    );
                }
            }
        }
        stats.pages += 1;
        outcome.pages += 1;

        // Page durable: advance the cursor.
        match (page.delta_link, page.next_link) {
            (Some(delta), _) => {
                o365_folders::set_delta_link(pool, account.id, &folder.id, &delta).await?;
                outcome.complete = true;
            }
            (None, Some(next)) => {
                o365_folders::set_next_link(pool, account.id, &folder.id, Some(&next)).await?;
                link = Some(next);
            }
            (None, None) => anyhow::bail!(
                "delta page for {} carried neither nextLink nor deltaLink",
                folder.path
            ),
        }
        checkpoint_token(pool, account.id, client).await?;

        if let Some(job_id) = opts.job {
            crate::db::jobs::update_stats(pool, job_id, &serde_json::to_value(&*stats)?).await?;
            if crate::db::jobs::is_cancel_requested(pool, job_id).await? {
                outcome.cancelled = true;
                return Ok(outcome);
            }
        }
        if outcome.complete {
            return Ok(outcome);
        }
    }
}

enum Incremental {
    Done {
        delta_link: String,
        removes: Vec<String>,
    },
    Expired,
}

/// Follow a folder's stored delta link to the end of the cycle. Adds and
/// updates are applied as they arrive; tombstones are returned for the
/// caller's second phase. Writes nothing to `o365_folders`.
async fn walk_incremental(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    folder: &ArchivableFolder,
    delta_link: &str,
    stats: &mut SyncStats,
) -> Result<Incremental> {
    let mut link = delta_link.to_string();
    let mut removes = Vec::new();
    loop {
        let page = match client
            .delta_page(&folder.id, Some(&link), None, DELTA_PAGE_SIZE)
            .await
        {
            Ok(page) => page,
            Err(O365Error::DeltaExpired) => return Ok(Incremental::Expired),
            Err(e) => return Err(e.into()),
        };
        for item in page.value {
            if item.is_removed() {
                removes.push(item.id);
                continue;
            }
            match ingest_item(pool, client, store, account.id, &item, &folder.path).await? {
                Ingested::Fetched => stats.fetched += 1,
                Ingested::Refreshed => stats.updated += 1,
                Ingested::Skipped => {}
            }
        }
        match (page.delta_link, page.next_link) {
            (Some(delta_link), _) => {
                return Ok(Incremental::Done {
                    delta_link,
                    removes,
                });
            }
            (None, Some(next)) => link = next,
            (None, None) => anyhow::bail!(
                "delta page for {} carried neither nextLink nor deltaLink",
                folder.path
            ),
        }
    }
}

/// Apply one tombstone from `folder_path`. Nothing happens if the row is
/// unknown or already re-placed elsewhere by a phase-1 add. Otherwise a
/// metadata fetch decides: gone (404) or now in a non-archivable folder
/// → deletion policy; still in an archivable folder → re-place there
/// (the delta for that folder was read before the move landed).
#[allow(clippy::too_many_arguments)]
async fn apply_remove(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    folder_path: &str,
    id: &str,
    live_paths: &HashMap<String, String>,
    policy: DeletionPolicy,
    stats: &mut SyncStats,
) -> Result<()> {
    let Some(row) = messages::get_by_jmap_id(pool, account.id, id).await? else {
        return Ok(());
    };
    if row.mailbox_ids != placement(folder_path) {
        return Ok(()); // already re-homed by a phase-1 add
    }
    match client.get_message_meta(id).await {
        Err(O365Error::Status(404, _)) => {}
        Ok(meta) => {
            if let Some(path) = meta
                .parent_folder_id
                .as_deref()
                .and_then(|pid| live_paths.get(pid))
            {
                messages::set_placement_meta(pool, row.id, &placement(path), &keywords_from(&meta))
                    .await?;
                stats.updated += 1;
                return Ok(());
            }
        }
        Err(e) => return Err(e).with_context(|| format!("verifying removed message {id}")),
    }
    apply_destroyed(pool, store, search, account.id, account.user_id, id, policy).await?;
    stats.destroyed += 1;
    Ok(())
}

struct PendingRemoves {
    folder: ArchivableFolder,
    /// Set for an incremental cycle: adopted after the removes apply.
    delta_link: Option<String>,
    /// Set after a resync walk: cleared after the diff applies.
    clear_resync: bool,
    removes: Vec<String>,
}

/// Re-walk a folder from scratch and diff the rows still placed there
/// against what the walk saw. Used when a delta token has expired.
async fn resync_folder(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    folder: &ArchivableFolder,
    since: Option<DateTime<Utc>>,
    stats: &mut SyncStats,
) -> Result<PendingRemoves> {
    let mut scratch = BackfillStats::default();
    let out = walk_initial_delta(
        pool,
        client,
        store,
        account,
        folder,
        &WalkOptions {
            since,
            remaining: None,
            job: None,
            collect_seen: true,
        },
        &mut scratch,
    )
    .await?;
    stats.fetched += out.fetched;
    stats.updated += out.refreshed;
    stats.resynced = true;
    // Rows placed here after the walk are either ones it saw (refreshed
    // or fetched) or stale ones the tombstones would have reported.
    let missing = o365_folders::message_ids_in_path(pool, account.id, &folder.path)
        .await?
        .into_iter()
        .filter(|id| !out.seen_ids.contains(id))
        .collect();
    Ok(PendingRemoves {
        folder: folder.clone(),
        delta_link: None,
        clear_resync: true,
        removes: missing,
    })
}

/// One poll run over every archivable folder. Phase 1 reconciles the
/// folder tree (renames rewrite placements) and applies each folder's
/// adds and updates; phase 2 applies the buffered tombstones and only
/// then advances each folder's delta link. Folders that vanished from
/// the tree are swept last.
pub async fn poll_account(
    pool: &PgPool,
    client: &O365Client,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    policy: DeletionPolicy,
) -> Result<SyncStats> {
    let state = accounts::get_o365_state(pool, account.id)
        .await?
        .context("account has no o365_state row")?;
    if !state.backfill_done {
        anyhow::bail!(
            "account {} has not completed backfill; run backfill first",
            account.id
        );
    }

    let mut stats = SyncStats::default();
    let live = list_archivable(client).await?;
    let live_paths: HashMap<String, String> = live
        .iter()
        .map(|f| (f.id.clone(), f.path.clone()))
        .collect();
    let known = o365_folders::list(pool, account.id).await?;
    let known_by_id: HashMap<&str, &O365Folder> =
        known.iter().map(|k| (k.folder_id.as_str(), k)).collect();
    for f in &live {
        if let Some(k) = known_by_id.get(f.id.as_str())
            && k.display_path != f.path
        {
            let n = o365_folders::rename_path(pool, account.id, &k.display_path, &f.path).await?;
            tracing::info!(
                account = account.id,
                from = %k.display_path,
                to = %f.path,
                rows = n,
                "folder renamed; placements rewritten"
            );
            stats.updated += n;
        }
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
    let vanished: Vec<&O365Folder> = known
        .iter()
        .filter(|k| !live_paths.contains_key(&k.folder_id))
        .collect();

    // Phase 1: adds and updates for every folder.
    let mut pending: Vec<PendingRemoves> = Vec::new();
    for f in &live {
        let row = o365_folders::get(pool, account.id, &f.id)
            .await?
            .context("folder row vanished mid-poll")?;
        match row.delta_link.as_deref() {
            Some(_) if row.resync_pending => {
                // A resync walk finished but its diff never ran (crash in
                // between); the seen set is gone, so walk again.
                o365_folders::mark_resync(pool, account.id, &f.id, true).await?;
                pending.push(
                    resync_folder(
                        pool,
                        client,
                        store,
                        account,
                        f,
                        state.backfill_since,
                        &mut stats,
                    )
                    .await?,
                );
            }
            Some(link) => {
                match walk_incremental(pool, client, store, account, f, link, &mut stats).await? {
                    Incremental::Done {
                        delta_link,
                        removes,
                    } => pending.push(PendingRemoves {
                        folder: f.clone(),
                        delta_link: Some(delta_link),
                        clear_resync: false,
                        removes,
                    }),
                    Incremental::Expired => {
                        tracing::warn!(
                            account = account.id,
                            folder = %f.path,
                            "delta token expired; resyncing folder"
                        );
                        o365_folders::mark_resync(pool, account.id, &f.id, true).await?;
                        pending.push(
                            resync_folder(
                                pool,
                                client,
                                store,
                                account,
                                f,
                                state.backfill_since,
                                &mut stats,
                            )
                            .await?,
                        );
                    }
                }
            }
            None if row.resync_pending => pending.push(
                resync_folder(
                    pool,
                    client,
                    store,
                    account,
                    f,
                    state.backfill_since,
                    &mut stats,
                )
                .await?,
            ),
            None => {
                // New folder (or a walk interrupted mid-way): no
                // tombstones in an initial walk, so no phase-2 entry.
                let mut scratch = BackfillStats::default();
                let out = walk_initial_delta(
                    pool,
                    client,
                    store,
                    account,
                    f,
                    &WalkOptions {
                        since: state.backfill_since,
                        remaining: None,
                        job: None,
                        collect_seen: false,
                    },
                    &mut scratch,
                )
                .await?;
                stats.fetched += out.fetched;
                stats.updated += out.refreshed;
            }
        }
        checkpoint_token(pool, account.id, client).await?;
    }

    // Phase 2: tombstones, then cursor advance, per folder.
    for p in pending {
        for id in &p.removes {
            apply_remove(
                pool,
                client,
                store,
                search,
                account,
                &p.folder.path,
                id,
                &live_paths,
                policy,
                &mut stats,
            )
            .await?;
        }
        if let Some(link) = &p.delta_link {
            o365_folders::set_delta_link(pool, account.id, &p.folder.id, link).await?;
        }
        if p.clear_resync {
            o365_folders::clear_resync_pending(pool, account.id, &p.folder.id).await?;
        }
    }
    for k in vanished {
        let ids = o365_folders::message_ids_in_path(pool, account.id, &k.display_path).await?;
        tracing::info!(
            account = account.id,
            folder = %k.display_path,
            rows = ids.len(),
            "folder vanished; sweeping its placements"
        );
        for id in &ids {
            apply_remove(
                pool,
                client,
                store,
                search,
                account,
                &k.display_path,
                id,
                &live_paths,
                policy,
                &mut stats,
            )
            .await?;
        }
        o365_folders::delete(pool, account.id, &k.folder_id).await?;
    }

    accounts::touch_o365_state(pool, account.id).await?;
    checkpoint_token(pool, account.id, client).await?;
    Ok(stats)
}
