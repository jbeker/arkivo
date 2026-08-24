//! IMAP sync engine. Same crash-safety contract as the other engines
//! (spec §7.3): per-folder cursors and the backfill cursor advance only
//! after the corresponding batch is durably stored. Every step is
//! idempotent.
//!
//! Identity is content-based: `messages.jmap_email_id` (and `blob_id`) is
//! `"imap-" + sha256(raw RFC822 bytes)`. IMAP UIDs are per-folder and a
//! move re-numbers the message, so location lives in `imap_uid_map` and a
//! row's `mailbox_ids`/`keywords` are recomputed from its placements. A
//! message moved between polls is "re-homed" — matched to its existing
//! row by Message-ID header without re-downloading the body — and a row
//! is destroyed (per deletion policy) only when its last placement
//! vanishes. Every poll walks all selectable folders except Trash/Junk,
//! so mail filed anywhere between polls is picked up — but a folder
//! whose EXAMINE proves it unchanged (same UIDVALIDITY, same UIDNEXT,
//! EXISTS matching our placement count) skips its O(folder) UID sweep.
//! Accepted tradeoff: flag-only changes in such a folder stay stale
//! until the folder next gains or loses a message (CONDSTORE is the
//! future exact upgrade).

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use mail_parser::MessageParser;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::config::DeletionPolicy;
use crate::db::{accounts, imap_map, messages};
use crate::imap::types::{FolderInfo, FolderStatus, RawMessage, SpecialUse};
use crate::imap::{ImapClient, ImapError};
use crate::ingest::{SyncStats, apply_destroyed};
use crate::maildir::MessageStore;
use crate::search::SearchClient;

/// UIDs per Message-ID header-peek batch.
pub const HEADER_BATCH: usize = 100;

/// What one [`ingest_uid`] call did.
#[derive(Debug)]
pub enum Ingested {
    /// Body downloaded and durably stored under a new or existing row.
    Fetched,
    /// Matched an already-stored row by Message-ID; placement recorded,
    /// no download.
    Rehomed,
    /// Body was already stored (byte-identical copy); placement recorded.
    Refreshed,
    /// The UID vanished upstream between the listing and the fetch.
    Skipped,
}

/// Folders the archive covers: selectable, not Trash/Junk by SPECIAL-USE,
/// and not named like a trash/spam folder (for servers without
/// SPECIAL-USE).
pub fn is_archivable(folder: &FolderInfo) -> bool {
    if !folder.selectable {
        return false;
    }
    if matches!(
        folder.special_use,
        Some(SpecialUse::Trash | SpecialUse::Junk)
    ) {
        return false;
    }
    const SKIP_NAMES: [&str; 5] = ["trash", "junk", "spam", "deleted messages", "deleted items"];
    let lower = folder.name.to_lowercase();
    !SKIP_NAMES
        .iter()
        .any(|n| lower == *n || lower.ends_with(&format!("/{n}")) || lower.ends_with(&format!(".{n}")))
}

pub fn content_id(raw: &[u8]) -> String {
    format!("imap-{}", hex::encode(Sha256::digest(raw)))
}

fn flags_to_keywords<'a>(flags: impl Iterator<Item = &'a str>) -> serde_json::Value {
    let mut keywords = serde_json::Map::new();
    for flag in flags {
        let kw = match flag {
            "\\Seen" => "$seen",
            "\\Flagged" => "$flagged",
            "\\Answered" => "$answered",
            "\\Draft" => "$draft",
            _ => continue,
        };
        keywords.insert(kw.into(), serde_json::Value::Bool(true));
    }
    serde_json::Value::Object(keywords)
}

fn flags_json(flags: &[String]) -> serde_json::Value {
    let mut sorted: Vec<&str> = flags.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    serde_json::json!(sorted)
}

fn json_flags(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Full metadata from the raw RFC822 bytes. Header conventions match the
/// JMAP/Gmail paths (angle-bracketed Message-ID via mail-parser).
/// mailbox_ids/keywords are provisional — [`recompute_placement_meta`]
/// overwrites them from the placement map right after the upsert.
fn meta_from_raw(hash_id: &str, msg: &RawMessage) -> messages::MessageMeta {
    let parsed = MessageParser::default().parse(&msg.raw);
    let message_id_hdr = parsed
        .as_ref()
        .and_then(|m| m.message_id())
        .map(|id| format!("<{id}>"));
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
    messages::MessageMeta {
        jmap_email_id: hash_id.to_string(),
        blob_id: hash_id.to_string(),
        message_id_hdr,
        thread_id: None,
        received_at: msg.internal_date.unwrap_or_default(),
        size: msg.raw.len() as i64,
        has_attachments,
        from_addr,
        subject,
        mailbox_ids: serde_json::json!([]),
        keywords: serde_json::json!({}),
    }
}

/// Recompute the placement-derived row fields: `mailbox_ids` is the
/// sorted distinct folder list, `keywords` the union of every placement's
/// flags (documented simplification — a copy read in one folder shows
/// `$seen`).
pub async fn recompute_placement_meta(pool: &PgPool, message_id: i64) -> Result<()> {
    let placements = imap_map::placements_for_message(pool, message_id).await?;
    let mut folders: Vec<&str> = placements.iter().map(|p| p.folder.as_str()).collect();
    folders.sort_unstable();
    folders.dedup();
    let all_flags: Vec<String> = placements.iter().flat_map(|p| json_flags(&p.flags)).collect();
    let keywords = flags_to_keywords(all_flags.iter().map(String::as_str));
    messages::set_placement_meta(pool, message_id, &serde_json::json!(folders), &keywords).await?;
    Ok(())
}

/// Ingest one (folder, uid): re-home by Message-ID when unambiguous,
/// otherwise download; then record the placement and recompute the
/// placement-derived metadata. Idempotent — a byte-identical body
/// converges onto the existing row via the content-hash conflict.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_uid(
    pool: &PgPool,
    client: &ImapClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    folder: &str,
    uidvalidity: u32,
    uid: u32,
    flags: &[String],
    message_id_hdr: Option<&str>,
) -> Result<Ingested> {
    // Re-home fast path: exactly one stored row with this Message-ID means
    // the message was archived before and (probably) moved — record the
    // placement, skip the download. Ambiguous or absent → download; the
    // content hash still dedups identical bytes. Never merge on a missing
    // Message-ID.
    if let Some(hdr) = message_id_hdr {
        let candidates = imap_map::find_by_message_id_hdr(pool, account.id, hdr).await?;
        if candidates.len() == 1 {
            let row_id = candidates[0];
            imap_map::add_placement(
                pool,
                account.id,
                folder,
                uidvalidity as i64,
                uid as i64,
                row_id,
                &flags_json(flags),
            )
            .await?;
            recompute_placement_meta(pool, row_id).await?;
            return Ok(Ingested::Rehomed);
        }
    }

    let raw_msg = match client.fetch_raw(uid).await {
        Ok(Some(msg)) => msg,
        Ok(None) => return Ok(Ingested::Skipped),
        Err(e) => return Err(e).with_context(|| format!("fetching {folder} uid {uid}")),
    };
    let hash_id = content_id(&raw_msg.raw);
    let row_id = messages::upsert_meta(pool, account.id, &meta_from_raw(&hash_id, &raw_msg)).await?;
    // Placement lands before the blob write: if the write fails, the row
    // stays `maildir_path IS NULL` with a live placement, exactly what
    // fetch_missing_blobs needs to retry it.
    imap_map::add_placement(
        pool,
        account.id,
        folder,
        uidvalidity as i64,
        uid as i64,
        row_id,
        &flags_json(&raw_msg.flags),
    )
    .await?;
    recompute_placement_meta(pool, row_id).await?;
    let row = messages::get(pool, row_id)
        .await?
        .context("row vanished during ingest")?;
    let fetched = row.maildir_path.is_none();
    if fetched {
        let rel_path = store.write(&hash_id, &raw_msg.raw)?;
        let mut conn = pool.acquire().await?;
        messages::set_stored(&mut conn, row_id, &rel_path).await?;
    }
    Ok(if fetched {
        Ingested::Fetched
    } else {
        Ingested::Refreshed
    })
}

struct FolderSnapshot {
    folder: String,
    /// Server truth at examine time: uid → flags.
    server: HashMap<i64, Vec<String>>,
    /// Ledger placements before this poll touched the folder.
    known: HashMap<i64, imap_map::KnownPlacement>,
}

/// One poll run over every archivable folder. Two phases: ingest new UIDs
/// across ALL folders first (so a moved message re-homes onto its row
/// before its old UID is processed), then apply vanished UIDs and flag
/// drift. A vanished UID whose row still has other placements just drops
/// that placement; only the last placement's disappearance applies the
/// deletion policy.
pub async fn poll_account(
    pool: &PgPool,
    client: &ImapClient,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    policy: DeletionPolicy,
) -> Result<SyncStats> {
    let state = accounts::get_imap_state(pool, account.id)
        .await?
        .context("account has no imap_state row")?;
    if !state.backfill_done {
        anyhow::bail!(
            "account {} has not completed backfill; run backfill first",
            account.id
        );
    }

    let mut stats = SyncStats::default();
    let folders: Vec<FolderInfo> = client
        .list_folders()
        .await?
        .into_iter()
        .filter(is_archivable)
        .collect();

    // Phase 1: snapshot each folder and ingest its new UIDs.
    let mut snapshots: Vec<FolderSnapshot> = Vec::new();
    for info in &folders {
        let status = client.examine(&info.name).await?;
        let prev = imap_map::get_folder(pool, account.id, &info.name).await?;
        if let Some(prev) = &prev
            && prev.uidvalidity != status.uidvalidity as i64
        {
            tracing::warn!(
                account = account.id,
                folder = %info.name,
                "UIDVALIDITY changed; resyncing folder"
            );
            resync_folder(
                pool, client, store, search, account, &info.name, status, policy, &mut stats,
            )
            .await?;
            continue;
        }

        // Provably unchanged folder: same UIDVALIDITY epoch, no additions
        // (any add bumps UIDNEXT), and no removals (with no additions an
        // expunge strictly shrinks EXISTS, which still matches our
        // placement count). Skip the O(folder) sweep. Flag-only changes
        // here stay stale until the folder next changes — accepted.
        if let Some(prev) = &prev
            && prev.uidvalidity == status.uidvalidity as i64
            && prev.last_seen_uidnext == status.uidnext as i64
            && imap_map::count_uids(pool, account.id, &info.name).await? == status.exists as i64
        {
            tracing::debug!(
                account = account.id,
                folder = %info.name,
                "unchanged; skipping UID sweep"
            );
            continue;
        }

        let server: HashMap<i64, Vec<String>> = client
            .uid_list(status.exists)
            .await?
            .into_iter()
            .map(|e| (e.uid as i64, e.flags))
            .collect();
        let known = imap_map::known_uids(pool, account.id, &info.name).await?;

        let mut new_uids: Vec<i64> = server
            .keys()
            .filter(|uid| !known.contains_key(uid))
            .copied()
            .collect();
        new_uids.sort_unstable();
        ingest_new_uids(
            pool, client, store, account, &info.name, status.uidvalidity, &new_uids, &server,
            &mut stats,
        )
        .await?;

        imap_map::upsert_folder(
            pool,
            account.id,
            &info.name,
            status.uidvalidity as i64,
            status.uidnext as i64,
        )
        .await?;
        snapshots.push(FolderSnapshot {
            folder: info.name.clone(),
            server,
            known,
        });
    }

    // Phase 2: vanished UIDs (deletes and move-sources), then flag drift.
    for snap in &snapshots {
        for uid in snap.known.keys() {
            if snap.server.contains_key(uid) {
                continue;
            }
            let Some(msg_id) =
                imap_map::remove_placement(pool, account.id, &snap.folder, *uid).await?
            else {
                continue;
            };
            let remaining = imap_map::placements_for_message(pool, msg_id).await?;
            if remaining.is_empty() {
                if let Some(row) = messages::get(pool, msg_id).await? {
                    apply_destroyed(
                        pool,
                        store,
                        search,
                        account.id,
                        account.user_id,
                        &row.jmap_email_id,
                        policy,
                    )
                    .await?;
                    stats.destroyed += 1;
                }
            } else {
                recompute_placement_meta(pool, msg_id).await?;
                stats.updated += 1;
            }
        }

        let mut flag_touched: HashSet<i64> = HashSet::new();
        for (uid, placement) in &snap.known {
            let Some(server_flags) = snap.server.get(uid) else {
                continue;
            };
            let server_json = flags_json(server_flags);
            if server_json != placement.flags {
                imap_map::set_placement_flags(pool, account.id, &snap.folder, *uid, &server_json)
                    .await?;
                flag_touched.insert(placement.message_id);
            }
        }
        for msg_id in flag_touched {
            recompute_placement_meta(pool, msg_id).await?;
            stats.updated += 1;
        }
    }

    Ok(stats)
}

/// Header-peek + ingest a sorted list of new UIDs in batches.
#[allow(clippy::too_many_arguments)]
async fn ingest_new_uids(
    pool: &PgPool,
    client: &ImapClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    folder: &str,
    uidvalidity: u32,
    new_uids: &[i64],
    server: &HashMap<i64, Vec<String>>,
    stats: &mut SyncStats,
) -> Result<()> {
    static EMPTY: Vec<String> = Vec::new();
    for chunk in new_uids.chunks(HEADER_BATCH) {
        let peek: Vec<u32> = chunk.iter().map(|&u| u as u32).collect();
        let headers = client.fetch_message_id_headers(&peek).await?;
        for &uid in chunk {
            let hdr = headers.get(&(uid as u32)).cloned().flatten();
            let flags = server.get(&uid).unwrap_or(&EMPTY);
            match ingest_uid(
                pool,
                client,
                store,
                account,
                folder,
                uidvalidity,
                uid as u32,
                flags,
                hdr.as_deref(),
            )
            .await?
            {
                Ingested::Fetched => stats.fetched += 1,
                Ingested::Rehomed | Ingested::Refreshed => stats.updated += 1,
                Ingested::Skipped => {}
            }
        }
    }
    Ok(())
}

/// UIDVALIDITY changed: every stored UID for the folder is meaningless.
/// Drop all placements, re-walk the folder (re-home matches rows by
/// Message-ID and identical bodies converge via the content hash — no
/// blob is stored twice), then apply the deletion policy to rows left
/// with no placement anywhere.
#[allow(clippy::too_many_arguments)]
async fn resync_folder(
    pool: &PgPool,
    client: &ImapClient,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    folder: &str,
    status: FolderStatus,
    policy: DeletionPolicy,
    stats: &mut SyncStats,
) -> Result<()> {
    let old_msg_ids: HashSet<i64> = imap_map::clear_folder(pool, account.id, folder)
        .await?
        .into_iter()
        .collect();

    let server: HashMap<i64, Vec<String>> = client
        .uid_list(status.exists)
        .await?
        .into_iter()
        .map(|e| (e.uid as i64, e.flags))
        .collect();
    let mut uids: Vec<i64> = server.keys().copied().collect();
    uids.sort_unstable();
    ingest_new_uids(
        pool,
        client,
        store,
        account,
        folder,
        status.uidvalidity,
        &uids,
        &server,
        stats,
    )
    .await?;

    for msg_id in old_msg_ids {
        let remaining = imap_map::placements_for_message(pool, msg_id).await?;
        if remaining.is_empty() {
            if let Some(row) = messages::get(pool, msg_id).await? {
                apply_destroyed(
                    pool,
                    store,
                    search,
                    account.id,
                    account.user_id,
                    &row.jmap_email_id,
                    policy,
                )
                .await?;
                stats.destroyed += 1;
            }
        } else {
            recompute_placement_meta(pool, msg_id).await?;
        }
    }

    imap_map::upsert_folder(
        pool,
        account.id,
        folder,
        status.uidvalidity as i64,
        status.uidnext as i64,
    )
    .await?;
    stats.resynced = true;
    Ok(())
}

/// Retry messages whose metadata landed but whose body was never durably
/// stored (`maildir_path IS NULL`), re-fetching through a live placement.
/// A row whose placements all vanished upstream never had its blob stored
/// — nothing to retain, so the row is removed with an audit record.
pub async fn fetch_missing_blobs(
    pool: &PgPool,
    client: &ImapClient,
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
            match recover_one(pool, client, store, account, row).await {
                Ok(Recovery::Recovered) => {
                    total += 1;
                    progressed += 1;
                }
                Ok(Recovery::Pruned) => progressed += 1,
                Ok(Recovery::Deferred) => {}
                Err(e) => tracing::warn!(
                    message = row.id,
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

enum Recovery {
    /// Blob durably stored.
    Recovered,
    /// Every placement vanished upstream; the row was removed.
    Pruned,
    /// Still failing; left for a later pass.
    Deferred,
}

/// Recover one blobless row.
async fn recover_one(
    pool: &PgPool,
    client: &ImapClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    row: &messages::Message,
) -> Result<Recovery> {
    let placements = imap_map::placements_for_message(pool, row.id).await?;

    let mut vanished = 0usize;
    for p in &placements {
        let status = match client.examine(&p.folder).await {
            Ok(status) => status,
            Err(ImapError::Imap(async_imap::error::Error::No(_))) => {
                vanished += 1; // folder gone
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        // Full ingest of the placement (hdr=None: this row is blobless so
        // the re-home path cannot match it). Identical bytes land on this
        // row via the content hash; changed bytes land on a fresh row and
        // strand this one placement-less, pruned below.
        let flags = json_flags(&p.flags);
        match ingest_uid(
            pool,
            client,
            store,
            account,
            &p.folder,
            status.uidvalidity,
            p.uid as u32,
            &flags,
            None,
        )
        .await?
        {
            Ingested::Skipped => {
                vanished += 1; // uid gone; try the next placement
                continue;
            }
            Ingested::Fetched | Ingested::Refreshed | Ingested::Rehomed => {
                let refreshed = messages::get(pool, row.id).await?;
                if refreshed.map(|r| r.maildir_path.is_some()).unwrap_or(false) {
                    return Ok(Recovery::Recovered);
                }
                // Blob landed on a different row (content changed
                // upstream); stop and fall through to the orphan check.
                break;
            }
        }
    }

    // Every placement vanished upstream (or none existed): the blob was
    // never stored and never will be — nothing to retain, prune the row.
    let remaining = imap_map::placements_for_message(pool, row.id).await?;
    if placements.is_empty() || vanished == placements.len() || remaining.is_empty() {
        for p in &remaining {
            imap_map::remove_placement(pool, account.id, &p.folder, p.uid).await?;
        }
        messages::delete_row(pool, row.id).await?;
        crate::db::audit::record(
            pool,
            Some(account.user_id),
            "system:backfill",
            "message_vanished_before_fetch",
            Some(&row.id.to_string()),
            None,
        )
        .await?;
        return Ok(Recovery::Pruned);
    }
    Ok(Recovery::Deferred)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folder(name: &str, selectable: bool, special_use: Option<SpecialUse>) -> FolderInfo {
        FolderInfo {
            name: name.into(),
            selectable,
            special_use,
        }
    }

    #[test]
    fn archivable_folder_filtering() {
        assert!(is_archivable(&folder("INBOX", true, None)));
        assert!(is_archivable(&folder("Archive/2026", true, None)));
        assert!(is_archivable(&folder(
            "Sent",
            true,
            Some(SpecialUse::Sent)
        )));
        assert!(!is_archivable(&folder("INBOX", false, None)));
        assert!(!is_archivable(&folder(
            "Rubbish",
            true,
            Some(SpecialUse::Trash)
        )));
        assert!(!is_archivable(&folder(
            "Whatever",
            true,
            Some(SpecialUse::Junk)
        )));
        assert!(!is_archivable(&folder("Trash", true, None)));
        assert!(!is_archivable(&folder("Spam", true, None)));
        assert!(!is_archivable(&folder("Deleted Messages", true, None)));
        assert!(!is_archivable(&folder("Sub/Junk", true, None)));
        assert!(!is_archivable(&folder("INBOX.Trash", true, None)));
        // Only exact/last-segment matches are skipped.
        assert!(is_archivable(&folder("Trashcan Research", true, None)));
    }

    #[test]
    fn keywords_from_flags() {
        let kw = flags_to_keywords(["\\Seen", "\\Flagged", "\\Recent"].into_iter());
        assert_eq!(kw["$seen"], true);
        assert_eq!(kw["$flagged"], true);
        assert!(kw.get("$answered").is_none());
    }

    #[test]
    fn content_id_stable() {
        assert_eq!(content_id(b"abc"), content_id(b"abc"));
        assert_ne!(content_id(b"abc"), content_id(b"abd"));
        assert!(content_id(b"abc").starts_with("imap-"));
    }
}
