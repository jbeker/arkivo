//! Initial seed of the archive from Gmail. Records the mailbox's current
//! history id first (so the first poll covers whatever changes during
//! the sweep), then pages through users.messages.list — newest first,
//! the only order Gmail offers; direction doesn't matter because ingest
//! is idempotent and promotion orders by received_at.
//!
//! Resumability: the cursor persisted in gmail_state.backfill_cursor
//! holds the next pageToken plus the oldest internalDate ingested so
//! far. Gmail pageTokens are not guaranteed to survive between runs; if
//! a resumed token is rejected, the sweep restarts bounded to
//! `before:<oldest + 1 day>` — everything newer is already ingested, and
//! the one-day overlap is absorbed by idempotent upserts.

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use sqlx::PgPool;

use crate::db::{accounts, audit, messages};
use crate::gmail::sync::{Ingested, ingest_message};
use crate::gmail::{GmailClient, GmailError};
use crate::ingest::{BackfillOptions, BackfillStats};
use crate::maildir::MessageStore;

/// Concurrent message downloads. messages.get costs 5 quota units against
/// a per-user budget of 15,000/min (~50/s), so this stays well inside it.
pub const DOWNLOAD_CONCURRENCY: usize = 6;

/// Gmail's maxResults ceiling for messages.list.
const MAX_PAGE_SIZE: u64 = 500;

#[derive(Debug, Default)]
struct Cursor {
    page_token: Option<String>,
    oldest_internal_date: Option<i64>,
}

impl Cursor {
    fn from_json(value: Option<&serde_json::Value>) -> Self {
        let Some(value) = value else {
            return Self::default();
        };
        Self {
            page_token: value
                .get("page_token")
                .and_then(|v| v.as_str())
                .map(String::from),
            oldest_internal_date: value.get("oldest_internal_date").and_then(|v| v.as_i64()),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "page_token": self.page_token,
            "oldest_internal_date": self.oldest_internal_date,
        })
    }
}

fn track_oldest(cursor: &mut Cursor, internal_date: Option<i64>) {
    if let Some(date) = internal_date {
        cursor.oldest_internal_date = Some(
            cursor
                .oldest_internal_date
                .map_or(date, |oldest| oldest.min(date)),
        );
    }
}

/// `job`: when present, per-page progress is written to that jobs row
/// and its cancel flag is honored at page boundaries (the persisted
/// cursor makes a later run resume where a cancelled one stopped).
pub async fn backfill_account(
    pool: &PgPool,
    client: &GmailClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    options: &BackfillOptions,
    job: Option<i64>,
) -> Result<BackfillStats> {
    let state = accounts::get_gmail_state(pool, account.id)
        .await?
        .context("account has no gmail_state row")?;

    // Record the history id up front (only on a fresh backfill).
    if state.history_id.is_none() {
        let profile = client.get_profile().await?;
        accounts::set_history_id(pool, account.id, &profile.history_id).await?;
    }

    let mut cursor = Cursor::from_json(state.backfill_cursor.as_ref());
    let base_q = options.since.map(|since| format!("after:{}", since.timestamp()));
    // Set when a resumed pageToken is rejected: the date-window fallback.
    let mut window_q: Option<String> = None;

    let mut stats = BackfillStats::default();
    loop {
        let remaining = options.limit.map(|l| l.saturating_sub(stats.fetched));
        if remaining == Some(0) {
            break;
        }
        let page_size = remaining
            .map(|r| r.min(options.page_size))
            .unwrap_or(options.page_size)
            .min(MAX_PAGE_SIZE);

        let q = match (&base_q, &window_q) {
            (Some(a), Some(b)) => Some(format!("{a} {b}")),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };
        let page = match client
            .list_messages(q.as_deref(), cursor.page_token.as_deref(), page_size as u32, false)
            .await
        {
            Ok(page) => page,
            Err(GmailError::Status(400, body)) if cursor.page_token.is_some() => {
                tracing::warn!(
                    account = account.id,
                    body = %body,
                    "stored pageToken rejected; restarting sweep from a date window"
                );
                window_q = cursor
                    .oldest_internal_date
                    .map(|ms| format!("before:{}", ms / 1000 + 86_400));
                cursor.page_token = None;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if page.messages.is_empty() {
            stats.complete = true;
            break;
        }
        stats.pages += 1;
        if stats.total.is_none() {
            stats.total = page.result_size_estimate;
        }

        // Collect every result rather than aborting on the first error:
        // one message that fails even after retries must not abort a
        // sweep of hundreds of thousands. Failures are recovered by
        // `fetch_missing_blobs` if their metadata landed, or by the
        // date-window overlap on a later run if it didn't.
        let results: Vec<Result<Ingested>> = stream::iter(page.messages)
            .map(|m| async move { ingest_message(pool, client, store, account.id, &m.id).await })
            .buffer_unordered(DOWNLOAD_CONCURRENCY)
            .collect()
            .await;
        for result in results {
            match result {
                Ok(Ingested::Fetched(date)) => {
                    stats.fetched += 1;
                    track_oldest(&mut cursor, date);
                }
                Ok(Ingested::Refreshed(date)) => track_oldest(&mut cursor, date),
                Ok(Ingested::Skipped) => {}
                Err(e) => {
                    stats.failed += 1;
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        "backfill: message ingest failed, deferring to fetch_missing_blobs"
                    );
                }
            }
        }

        // Page durable: advance the resume cursor.
        cursor.page_token = page.next_page_token;
        accounts::set_gmail_backfill_cursor(pool, account.id, Some(&cursor.to_json())).await?;
        if cursor.page_token.is_none() {
            stats.complete = true;
            break;
        }

        if let Some(job_id) = job {
            crate::db::jobs::update_stats(pool, job_id, &serde_json::to_value(&stats)?).await?;
            if crate::db::jobs::is_cancel_requested(pool, job_id).await? {
                stats.cancelled = true;
                break;
            }
        }
    }

    if stats.complete && options.limit.is_none() && options.since.is_none() {
        accounts::set_gmail_backfill_done(pool, account.id).await?;
    }
    Ok(stats)
}

/// Retry messages whose metadata landed but whose body was never durably
/// stored (`maildir_path IS NULL`). A message that 404s here vanished
/// upstream before we ever stored it — there is nothing to retain, so
/// the ledger row is removed (with an audit record) rather than left
/// permanently unfetchable.
pub async fn fetch_missing_blobs(
    pool: &PgPool,
    client: &GmailClient,
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
            match ingest_message(pool, client, store, account.id, &row.blob_id).await {
                Ok(Ingested::Fetched(_)) => {
                    total += 1;
                    progressed += 1;
                }
                Ok(Ingested::Refreshed(_)) => {}
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
