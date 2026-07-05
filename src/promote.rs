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

/// `job`: when present, per-batch progress is written to that jobs row
/// and its cancel flag is honored at batch boundaries.
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
    loop {
        let batch = messages::promotable(pool, account.id, cutoff, BATCH).await?;
        if batch.is_empty() {
            break;
        }
        for msg in &batch {
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
