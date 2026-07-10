//! Incremental sync (poll) and full-resync engine. The crash-safety
//! contract (spec §7.3): the stored JMAP state advances only after every
//! message in the batch is durably in the Maildir and recorded in the
//! `messages` ledger. A run that dies partway retries from the last good
//! state; every step is idempotent.

use std::collections::HashSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::config::DeletionPolicy;
use crate::db::{accounts, messages};
use crate::ingest::apply_destroyed;
use crate::jmap::types::Email;
use crate::jmap::{JmapClient, JmapError};
use crate::maildir::MessageStore;
use crate::search::SearchClient;

pub use crate::ingest::SyncStats;

fn to_meta(email: &Email) -> messages::MessageMeta {
    messages::MessageMeta {
        jmap_email_id: email.id.clone(),
        blob_id: email.blob_id.clone(),
        message_id_hdr: email.message_id_hdr(),
        thread_id: email.thread_id.clone(),
        received_at: email.received_at,
        size: email.size,
        has_attachments: email.has_attachment,
        from_addr: email.from_addr(),
        subject: email.subject.clone(),
        mailbox_ids: serde_json::json!(email.mailbox_id_list()),
        keywords: serde_json::to_value(&email.keywords).unwrap_or_default(),
    }
}

/// Record metadata, download the raw blob, durably store it, and mark the
/// ledger row. Idempotent: an already-stored message only refreshes its
/// mutable metadata.
pub async fn ingest_email(
    pool: &PgPool,
    client: &JmapClient,
    store: &dyn MessageStore,
    mail_account_id: i64,
    email: &Email,
) -> Result<bool> {
    let row_id = messages::upsert_meta(pool, mail_account_id, &to_meta(email)).await?;
    let existing = messages::get(pool, row_id)
        .await?
        .context("row just upserted")?;
    if existing.maildir_path.is_some() {
        return Ok(false); // blob already durable; metadata refresh only
    }
    let raw = client
        .download_blob(&email.blob_id)
        .await
        .with_context(|| format!("downloading blob for {}", email.id))?;
    let rel_path = store.write(&email.id, &raw)?;
    let mut conn = pool.acquire().await?;
    messages::set_stored(&mut conn, row_id, &rel_path).await?;
    Ok(true)
}

/// One poll run: walk Email/changes pages until exhausted, ingesting as
/// we go. Falls back to a full resync on cannotCalculateChanges.
pub async fn poll_account(
    pool: &PgPool,
    client: &JmapClient,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    policy: DeletionPolicy,
) -> Result<SyncStats> {
    let state = accounts::get_jmap_state(pool, account.id)
        .await?
        .context("account has no jmap_state row")?;
    let Some(mut email_state) = state.email_state else {
        anyhow::bail!(
            "account {} has no stored JMAP state; run backfill first",
            account.id
        );
    };

    let mut stats = SyncStats::default();
    loop {
        let page = match client.changes(&email_state).await {
            Ok(page) => page,
            Err(JmapError::CannotCalculateChanges) => {
                tracing::warn!(
                    account = account.id,
                    "state too old; falling back to full resync"
                );
                let resync = resync_account(pool, client, store, search, account, policy).await?;
                stats.fetched += resync.fetched;
                stats.destroyed += resync.destroyed;
                stats.resynced = true;
                return Ok(stats);
            }
            Err(e) => return Err(e.into()),
        };

        for email in page.created.iter().chain(page.updated.iter()) {
            if ingest_email(pool, client, store, account.id, email).await? {
                stats.fetched += 1;
            } else {
                stats.updated += 1;
            }
        }
        for destroyed_id in &page.changes.destroyed {
            apply_destroyed(
                pool,
                store,
                search,
                account.id,
                account.user_id,
                destroyed_id,
                policy,
            )
            .await?;
            stats.destroyed += 1;
        }

        // The whole page is durable; only now may the state advance.
        accounts::set_email_state(pool, account.id, &page.changes.new_state).await?;
        email_state = page.changes.new_state.clone();

        if !page.changes.has_more_changes {
            break;
        }
    }
    Ok(stats)
}

/// Full resync (spec §7.3 fallback): record the server's current state,
/// sweep all IDs, diff against the local ledger, fetch what's missing and
/// apply the deletion policy to what vanished, then adopt the recorded
/// state. Changes that land mid-sweep surface on the next poll.
pub async fn resync_account(
    pool: &PgPool,
    client: &JmapClient,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    account: &accounts::MailAccount,
    policy: DeletionPolicy,
) -> Result<SyncStats> {
    let state_now = client.email_state_now().await?;
    let server_ids: HashSet<String> = client.query_all_ids().await?.into_iter().collect();

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

    let missing: Vec<String> = server_ids.difference(&local_ids).cloned().collect();
    for email in client.email_get(&missing).await? {
        if ingest_email(pool, client, store, account.id, &email).await? {
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

    accounts::set_email_state(pool, account.id, &state_now).await?;
    Ok(stats)
}
