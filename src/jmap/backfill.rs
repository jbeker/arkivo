//! Initial seed of the archive (spec §7.2). Records the account state
//! first, then pages through all message IDs oldest-first with anchor
//! pagination, downloading blobs with bounded concurrency. The anchor
//! cursor persists in jmap_state.backfill_anchor, so an interrupted run
//! resumes where it left off; the recorded state means the first
//! incremental poll picks up whatever changed during the sweep.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use sqlx::PgPool;

use crate::db::{accounts, messages};
use crate::jmap::JmapClient;
use crate::jmap::sync::ingest_email;
use crate::maildir::MessageStore;

/// Concurrent blob downloads. Deliberately modest: Fastmail publishes no
/// hard rate limits, so we stay polite (plan: 4-8).
pub const DOWNLOAD_CONCURRENCY: usize = 6;

#[derive(Debug, Default, serde::Serialize)]
pub struct BackfillStats {
    pub fetched: u64,
    pub pages: u64,
    pub complete: bool,
}

pub struct BackfillOptions {
    pub limit: Option<u64>,
    pub since: Option<DateTime<Utc>>,
    pub page_size: u64,
}

impl Default for BackfillOptions {
    fn default() -> Self {
        Self {
            limit: None,
            since: None,
            page_size: 100,
        }
    }
}

pub async fn backfill_account(
    pool: &PgPool,
    client: &JmapClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    options: &BackfillOptions,
) -> Result<BackfillStats> {
    let state = accounts::get_jmap_state(pool, account.id)
        .await?
        .context("account has no jmap_state row")?;

    // Record the state up front (only on a fresh backfill): everything
    // that changes during the sweep is covered by the first poll.
    if state.email_state.is_none() {
        let state_now = client.email_state_now().await?;
        accounts::set_email_state(pool, account.id, &state_now).await?;
    }

    let mut anchor: Option<String> = state
        .backfill_anchor
        .as_ref()
        .and_then(|a| a.get("anchor"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let mut stats = BackfillStats::default();
    loop {
        let remaining = options.limit.map(|l| l.saturating_sub(stats.fetched));
        if remaining == Some(0) {
            break;
        }
        // The client clamps to the server's maxObjectsInGet; use the
        // effective size for full-page detection below.
        let page_size = remaining
            .map(|r| r.min(options.page_size))
            .unwrap_or(options.page_size)
            .min(client.batch_limit());

        let page = client
            .query_page(anchor.as_deref(), page_size, options.since)
            .await?;
        if page.emails.is_empty() {
            stats.complete = true;
            break;
        }
        stats.pages += 1;

        let stored: Vec<bool> = stream::iter(page.emails.iter())
            .map(|email| ingest_email(pool, client, store, account.id, email))
            .buffer_unordered(DOWNLOAD_CONCURRENCY)
            .try_collect()
            .await?;
        stats.fetched += stored.into_iter().filter(|s| *s).count() as u64;

        // Page durable: advance the resume cursor.
        anchor = page.query.ids.last().cloned();
        accounts::set_backfill_anchor(
            pool,
            account.id,
            Some(&serde_json::json!({"anchor": anchor})),
        )
        .await?;

        if (page.query.ids.len() as u64) < page_size {
            stats.complete = true;
            break;
        }
    }

    if stats.complete && options.limit.is_none() && options.since.is_none() {
        accounts::set_backfill_done(pool, account.id).await?;
    }
    Ok(stats)
}

/// Retry blobs whose metadata landed but whose download failed or was
/// interrupted (`maildir_path IS NULL`).
pub async fn fetch_missing_blobs(
    pool: &PgPool,
    client: &JmapClient,
    store: &dyn MessageStore,
    mail_account_id: i64,
) -> Result<u64> {
    let mut total = 0u64;
    loop {
        let missing = messages::unfetched(pool, mail_account_id, 500).await?;
        if missing.is_empty() {
            return Ok(total);
        }
        let ids: Vec<String> = missing.iter().map(|m| m.jmap_email_id.clone()).collect();
        let emails = client.email_get(&ids).await?;
        if emails.is_empty() {
            return Ok(total);
        }
        for email in &emails {
            if ingest_email(pool, client, store, mail_account_id, email).await? {
                total += 1;
            }
        }
    }
}
