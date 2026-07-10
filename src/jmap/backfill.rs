//! Initial seed of the archive (spec §7.2). Records the account state
//! first, then pages through all message IDs oldest-first with anchor
//! pagination, downloading blobs with bounded concurrency. The anchor
//! cursor persists in jmap_state.backfill_anchor, so an interrupted run
//! resumes where it left off; the recorded state means the first
//! incremental poll picks up whatever changed during the sweep.

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use sqlx::PgPool;

use crate::db::{accounts, messages};
use crate::jmap::JmapClient;
use crate::jmap::sync::ingest_email;
use crate::maildir::MessageStore;

/// Concurrent blob downloads. Deliberately modest: Fastmail publishes no
/// hard rate limits, so we stay polite (plan: 4-8).
pub const DOWNLOAD_CONCURRENCY: usize = 6;

pub use crate::ingest::{BackfillOptions, BackfillStats};

/// `job`: when present, per-page progress is written to that jobs row
/// and its cancel flag is honored at page boundaries (the anchor cursor
/// makes a later run resume where a cancelled one stopped).
pub async fn backfill_account(
    pool: &PgPool,
    client: &JmapClient,
    store: &dyn MessageStore,
    account: &accounts::MailAccount,
    options: &BackfillOptions,
    job: Option<i64>,
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
        if stats.total.is_none() {
            stats.total = page.query.total;
        }

        // Owned emails, not `.iter()`: a closure over `&Email` trips
        // rustc's higher-ranked lifetime check (#89976) once this future
        // runs under tokio::spawn for web-triggered jobs.
        //
        // Collect every result rather than `try_collect`: one blob that
        // fails even after retries must not abort a sweep of hundreds of
        // thousands. The metadata row is already persisted (ingest upserts
        // before downloading), so a failure here is recovered by
        // `fetch_missing_blobs` once the sweep reaches the end.
        let results: Vec<Result<bool>> = stream::iter(page.emails)
            .map(|email| async move { ingest_email(pool, client, store, account.id, &email).await })
            .buffer_unordered(DOWNLOAD_CONCURRENCY)
            .collect()
            .await;
        for result in results {
            match result {
                Ok(true) => stats.fetched += 1,
                Ok(false) => {}
                Err(e) => {
                    stats.failed += 1;
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        "backfill: blob ingest failed, deferring to fetch_missing_blobs"
                    );
                }
            }
        }

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

        if let Some(job_id) = job {
            crate::db::jobs::update_stats(pool, job_id, &serde_json::to_value(&stats)?).await?;
            if crate::db::jobs::is_cancel_requested(pool, job_id).await? {
                stats.cancelled = true;
                break;
            }
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
        // Tolerate blobs that are still failing: a single wedged message
        // upstream must not fail an otherwise-complete import. Count how
        // many we actually recovered this pass.
        let mut progressed = 0u64;
        for email in &emails {
            match ingest_email(pool, client, store, mail_account_id, email).await {
                Ok(true) => {
                    total += 1;
                    progressed += 1;
                }
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    email = %email.id,
                    error = %format!("{e:#}"),
                    "fetch_missing_blobs: blob still failing, leaving deferred"
                ),
            }
        }
        // A pass that recovered nothing means the remainder is wedged
        // upstream; stop rather than spin. They stay `unfetched` for a
        // later run (or a manual re-import) to retry.
        if progressed == 0 {
            return Ok(total);
        }
    }
}
