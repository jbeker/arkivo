//! Gmail incremental sync (poll) and full-resync engine. Same
//! crash-safety contract as the JMAP engine (spec §7.3): the stored
//! history id advances only after every message in the batch is durably
//! in the Maildir and recorded in the `messages` ledger. Every step is
//! idempotent.
//!
//! Naming debt, accepted (migration 0004): for gmail accounts the
//! `messages.jmap_email_id` and `messages.blob_id` columns both hold the
//! Gmail message id — it is the identity and the `format=raw` fetch key.

use std::collections::HashSet;

use anyhow::{Context, Result};
use mail_parser::MessageParser;
use sqlx::PgPool;

use crate::config::DeletionPolicy;
use crate::db::{accounts, messages};
use crate::gmail::types::GmailMessage;
use crate::gmail::{GmailClient, GmailError};
use crate::ingest::{SyncStats, apply_destroyed};
use crate::maildir::MessageStore;
use crate::search::SearchClient;

/// What one [`ingest_message`] call did, with the message's internalDate
/// (epoch millis) when the server reported one — backfill tracks the
/// oldest date seen to build its pageToken-expiry fallback window.
#[derive(Debug)]
pub enum Ingested {
    /// Body downloaded and durably stored.
    Fetched(Option<i64>),
    /// Already stored; mutable metadata (labels/keywords) refreshed.
    Refreshed(Option<i64>),
    /// Not ingested: spam/trash we don't archive, or the message vanished
    /// upstream between the history event and the fetch.
    Skipped,
}

fn labels_to_keywords(label_ids: &[String]) -> serde_json::Value {
    let mut keywords = serde_json::Map::new();
    if !label_ids.iter().any(|l| l == "UNREAD") {
        keywords.insert("$seen".into(), serde_json::Value::Bool(true));
    }
    if label_ids.iter().any(|l| l == "STARRED") {
        keywords.insert("$flagged".into(), serde_json::Value::Bool(true));
    }
    serde_json::Value::Object(keywords)
}

fn sorted_labels(label_ids: &[String]) -> serde_json::Value {
    let mut labels: Vec<&str> = label_ids.iter().map(String::as_str).collect();
    labels.sort_unstable();
    serde_json::json!(labels)
}

fn received_at(msg: &GmailMessage) -> chrono::DateTime<chrono::Utc> {
    msg.internal_date
        .and_then(chrono::DateTime::from_timestamp_millis)
        .unwrap_or_default()
}

/// Full metadata, from the raw RFC822 bytes plus the API envelope. The
/// header-derived fields (Message-ID for promotion dedup, from, subject)
/// match the conventions of the JMAP path — angle-bracketed Message-ID
/// included.
fn meta_from_raw(msg: &GmailMessage, raw: &[u8]) -> messages::MessageMeta {
    let parsed = MessageParser::default().parse(raw);
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
        jmap_email_id: msg.id.clone(),
        blob_id: msg.id.clone(),
        message_id_hdr,
        thread_id: msg.thread_id.clone(),
        received_at: received_at(msg),
        size: msg.size_estimate.unwrap_or(raw.len() as i64),
        has_attachments,
        from_addr,
        subject,
        mailbox_ids: sorted_labels(&msg.label_ids),
        keywords: labels_to_keywords(&msg.label_ids),
    }
}

/// Metadata for refreshing an already-stored message from a bodyless
/// `format=minimal` fetch. Only the fields `upsert_meta` touches on
/// conflict (blob_id, mailbox_ids, keywords) matter here; the rest are
/// ignored because the row already exists.
fn meta_for_refresh(msg: &GmailMessage, row: &messages::Message) -> messages::MessageMeta {
    messages::MessageMeta {
        jmap_email_id: msg.id.clone(),
        blob_id: msg.id.clone(),
        message_id_hdr: row.message_id_hdr.clone(),
        thread_id: row.thread_id.clone(),
        received_at: row.received_at,
        size: row.size,
        has_attachments: row.has_attachments,
        from_addr: row.from_addr.clone(),
        subject: row.subject.clone(),
        mailbox_ids: sorted_labels(&msg.label_ids),
        keywords: labels_to_keywords(&msg.label_ids),
    }
}

/// Fetch and durably store one message, or refresh its metadata if the
/// body is already stored. Idempotent, and tolerant of the message
/// vanishing upstream (404 → [`Ingested::Skipped`]; the deletion arrives
/// via history or resync). New messages labeled SPAM or TRASH are not
/// archived.
pub async fn ingest_message(
    pool: &PgPool,
    client: &GmailClient,
    store: &dyn MessageStore,
    mail_account_id: i64,
    gmail_id: &str,
) -> Result<Ingested> {
    let existing = messages::get_by_jmap_id(pool, mail_account_id, gmail_id).await?;
    if let Some(row) = &existing
        && row.maildir_path.is_some()
    {
        // Stored: labels/keywords refresh only, no body re-download.
        let msg = match client.get_message_metadata(gmail_id).await {
            Ok(msg) => msg,
            Err(GmailError::Status(404, _)) => return Ok(Ingested::Skipped),
            Err(e) => return Err(e.into()),
        };
        messages::upsert_meta(pool, mail_account_id, &meta_for_refresh(&msg, row)).await?;
        return Ok(Ingested::Refreshed(msg.internal_date));
    }

    let msg = match client.get_message_raw(gmail_id).await {
        Ok(msg) => msg,
        Err(GmailError::Status(404, _)) => return Ok(Ingested::Skipped),
        Err(e) => return Err(e).with_context(|| format!("fetching gmail message {gmail_id}")),
    };
    if existing.is_none() && msg.label_ids.iter().any(|l| l == "SPAM" || l == "TRASH") {
        return Ok(Ingested::Skipped);
    }
    let raw = msg
        .raw_bytes()
        .map_err(|e| anyhow::anyhow!("{gmail_id}: {e}"))?;
    let row_id = messages::upsert_meta(pool, mail_account_id, &meta_from_raw(&msg, &raw)).await?;
    let rel_path = store.write(&msg.id, &raw)?;
    let mut conn = pool.acquire().await?;
    messages::set_stored(&mut conn, row_id, &rel_path).await?;
    Ok(Ingested::Fetched(msg.internal_date))
}

/// One poll run: walk history.list pages until exhausted, ingesting as we
/// go. Falls back to a full resync when the stored history id is too old
/// (Google keeps roughly a week).
pub async fn poll_account(
    pool: &PgPool,
    client: &GmailClient,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    policy: DeletionPolicy,
) -> Result<SyncStats> {
    let state = accounts::get_gmail_state(pool, account.id)
        .await?
        .context("account has no gmail_state row")?;
    let Some(start_history_id) = state.history_id else {
        anyhow::bail!(
            "account {} has no stored history id; run backfill first",
            account.id
        );
    };

    let mut stats = SyncStats::default();
    let mut page_token: Option<String> = None;
    loop {
        let page = match client
            .list_history(&start_history_id, page_token.as_deref())
            .await
        {
            Ok(page) => page,
            Err(GmailError::HistoryExpired) => {
                tracing::warn!(
                    account = account.id,
                    "history id too old; falling back to full resync"
                );
                let resync = resync_account(pool, client, store, search, account, policy).await?;
                stats.fetched += resync.fetched;
                stats.destroyed += resync.destroyed;
                stats.resynced = true;
                return Ok(stats);
            }
            Err(e) => return Err(e.into()),
        };

        // Coalesce the page: a message added then relabeled twice needs
        // one ingest, and one deleted later in the page needs none (the
        // fetch would 404 anyway; skip the round trip).
        let mut deleted: Vec<String> = Vec::new();
        let mut seen_deleted: HashSet<String> = HashSet::new();
        for record in &page.history {
            for d in &record.messages_deleted {
                if seen_deleted.insert(d.message.id.clone()) {
                    deleted.push(d.message.id.clone());
                }
            }
        }
        let mut touched: Vec<String> = Vec::new();
        let mut seen_touched: HashSet<String> = HashSet::new();
        for record in &page.history {
            let ids = record
                .messages_added
                .iter()
                .map(|m| &m.message.id)
                .chain(record.labels_added.iter().map(|l| &l.message.id))
                .chain(record.labels_removed.iter().map(|l| &l.message.id));
            for id in ids {
                if !seen_deleted.contains(id) && seen_touched.insert(id.clone()) {
                    touched.push(id.clone());
                }
            }
        }

        for id in &touched {
            match ingest_message(pool, client, store, account.id, id).await? {
                Ingested::Fetched(_) => stats.fetched += 1,
                Ingested::Refreshed(_) => stats.updated += 1,
                Ingested::Skipped => {}
            }
        }
        for id in &deleted {
            apply_destroyed(pool, store, search, account.id, account.user_id, id, policy).await?;
            stats.destroyed += 1;
        }

        // The whole page is durable; only now may the cursor advance —
        // to the last record's history id, or (on the final page) to the
        // mailbox's current id so quiet periods don't re-walk history.
        let advance_to = match (&page.next_page_token, page.history.last()) {
            (None, _) => page.history_id.clone(),
            (Some(_), Some(last)) => Some(last.id.clone()),
            (Some(_), None) => None,
        };
        if let Some(history_id) = advance_to {
            accounts::set_history_id(pool, account.id, &history_id).await?;
        }

        match page.next_page_token {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }
    Ok(stats)
}

/// Full resync, the fallback when history has expired: record the
/// mailbox's current history id, sweep all ids (spam/trash included, so a
/// message merely moved to trash is not mistaken for a deleted one), diff
/// against the local ledger, fetch what's missing and apply the deletion
/// policy to what vanished, then adopt the recorded id. Changes that land
/// mid-sweep surface on the next poll.
pub async fn resync_account(
    pool: &PgPool,
    client: &GmailClient,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    policy: DeletionPolicy,
) -> Result<SyncStats> {
    let profile = client.get_profile().await?;

    let mut server_ids: HashSet<String> = HashSet::new();
    let mut page_token: Option<String> = None;
    loop {
        let page = client
            .list_messages(None, page_token.as_deref(), 500, true)
            .await?;
        server_ids.extend(page.messages.into_iter().map(|m| m.id));
        match page.next_page_token {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }

    let local_ids: HashSet<String> = sqlx::query_scalar!(
        "select jmap_email_id from messages where mail_account_id = $1 and deleted_at is null",
        account.id,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    let mut stats = SyncStats {
        resynced: true,
        ..Default::default()
    };

    for missing in server_ids.difference(&local_ids) {
        // ingest_message skips spam/trash, so the inclusive listing above
        // never pulls messages the archive policy excludes.
        if let Ingested::Fetched(_) =
            ingest_message(pool, client, store, account.id, missing).await?
        {
            stats.fetched += 1;
        }
    }

    for gone in local_ids.difference(&server_ids) {
        apply_destroyed(
            pool,
            store,
            search,
            account.id,
            account.user_id,
            gone,
            policy,
        )
        .await?;
        stats.destroyed += 1;
    }

    accounts::set_history_id(pool, account.id, &profile.history_id).await?;
    Ok(stats)
}
