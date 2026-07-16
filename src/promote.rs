//! Promotion (spec §7.5): move messages that have aged past the recency
//! cutoff from the staged canonical store through sanitization into the
//! search indices. Runs on its own cadence, independent of polling —
//! this is the boundary that keeps fresh (attacker-influenceable) mail
//! out of agent reach.

use anyhow::Result;
use chrono::Duration;
use sqlx::PgPool;

use crate::clock::Clock;
use crate::db::{accounts::MailAccount, messages};
use crate::embed::EmbeddingProvider;
use crate::extract;
use crate::maildir::MessageStore;
use crate::sanitize::{SanitizeOutcome, SanitizePolicy, sanitize};
use crate::search::SearchClient;
use crate::search::indexer::{Indexer, PIPELINE_VERSION};

#[derive(Debug, Default, serde::Serialize)]
pub struct PromoteStats {
    pub promoted: u64,
    pub quarantined: u64,
    pub deduped: u64,
    pub failed: u64,
    pub chunks: u64,
    /// Stopped early because cancellation was requested.
    pub cancelled: bool,
}

const BATCH: i64 = 200;

/// How often in-flight progress is written to the jobs row. Frequent
/// enough that the dashboard (5s poll) never reads stale zeros, cheap
/// next to per-message embedding cost.
const PROGRESS_EVERY: std::time::Duration = std::time::Duration::from_secs(2);

/// Stats plus the message about to be processed, so a slow message (a
/// 70 MB pathological email) is visible in the UI instead of the job
/// looking frozen.
fn progress_value(stats: &PromoteStats, current: &messages::Message) -> Result<serde_json::Value> {
    let mut value = serde_json::to_value(stats)?;
    value["current"] = serde_json::json!({
        "id": current.id,
        "subject": current.subject,
        "size": current.size,
    });
    Ok(value)
}

/// `job`: when present, progress (including the in-flight message) is
/// written to that jobs row every couple of seconds and its cancel flag
/// is honored between messages.
pub async fn promote_account(
    pool: &PgPool,
    store: &dyn MessageStore,
    search: &SearchClient,
    embedder: &dyn EmbeddingProvider,
    clock: &dyn Clock,
    account: &MailAccount,
    job: Option<i64>,
) -> Result<PromoteStats> {
    let cutoff = clock.now() - Duration::days(account.recency_cutoff_days as i64);
    let policy = SanitizePolicy::from_value(account.sanitize_policy.as_ref());
    search
        .ensure_user_indices(account.user_id, embedder.dimension())
        .await?;
    let indexer = Indexer { search, embedder };

    let mut stats = PromoteStats::default();
    // None forces a progress write before the very first message: if that
    // one is slow, the UI must already show what the job is chewing on.
    let mut last_progress: Option<std::time::Instant> = None;
    'sweep: loop {
        let batch = messages::promotable(pool, account.id, cutoff, BATCH).await?;
        if batch.is_empty() {
            break;
        }
        for msg in &batch {
            if let Some(job_id) = job
                && last_progress.is_none_or(|at| at.elapsed() >= PROGRESS_EVERY)
            {
                crate::db::jobs::update_stats(pool, job_id, &progress_value(&stats, msg)?).await?;
                last_progress = Some(std::time::Instant::now());
                if crate::db::jobs::is_cancel_requested(pool, job_id).await? {
                    stats.cancelled = true;
                    break 'sweep;
                }
            }
            match promote_one(pool, store, &indexer, &policy, account, msg, &mut stats).await {
                Ok(()) => {}
                Err(e) => {
                    // One bad message must not stall the pipeline: record
                    // and move on. Infrastructure errors (search down)
                    // will fail every message and surface via stats.
                    tracing::warn!(message = msg.id, error = %format!("{e:#}"), "promotion failed");
                    messages::mark_status(pool, msg.id, "failed", Some(&format!("{e:#}"))).await?;
                    stats.failed += 1;
                }
            }
        }

        if let Some(job_id) = job {
            crate::db::jobs::update_stats(pool, job_id, &serde_json::to_value(&stats)?).await?;
            if crate::db::jobs::is_cancel_requested(pool, job_id).await? {
                stats.cancelled = true;
                break;
            }
        }
    }
    Ok(stats)
}

async fn promote_one(
    pool: &PgPool,
    store: &dyn MessageStore,
    indexer: &Indexer<'_>,
    policy: &SanitizePolicy,
    account: &MailAccount,
    msg: &messages::Message,
    stats: &mut PromoteStats,
) -> Result<()> {
    // Message-ID dedup (spec §8): a copy filed into a second folder is
    // marked done without re-embedding identical content.
    if let Some(hdr) = &msg.message_id_hdr
        && messages::is_duplicate_indexed(pool, account.id, hdr, msg.id).await?
    {
        messages::mark_indexed(pool, msg.id, PIPELINE_VERSION).await?;
        stats.deduped += 1;
        return Ok(());
    }

    let raw = store.read(
        msg.maildir_path
            .as_deref()
            .expect("promotable implies stored"),
    )?;
    let mut email = extract::extract(&raw);

    match sanitize(
        policy,
        email.from.first().map(String::as_str),
        &email.body_text,
    ) {
        SanitizeOutcome::Quarantined { reason } => {
            messages::mark_status(pool, msg.id, "quarantined", Some(&reason)).await?;
            stats.quarantined += 1;
            return Ok(());
        }
        SanitizeOutcome::Clean { body, redacted } => {
            email.body_text = body;
            let outcome = indexer
                .index_message(account.user_id, msg, &email, redacted)
                .await?;
            messages::mark_indexed(pool, msg.id, PIPELINE_VERSION).await?;
            stats.promoted += 1;
            stats.chunks += outcome.chunks as u64;
        }
    }
    Ok(())
}
