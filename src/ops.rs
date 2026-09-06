//! Per-account job orchestration shared by the CLI subcommands and the
//! web API. Split in two phases so the web can respond immediately:
//! [`try_start`] synchronously takes the advisory lock and creates the
//! jobs row (returning `None` when the account is busy); [`run`] does
//! the long work, finishes the jobs row, and releases the lock.

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::PgPool;

use crate::clock::SystemClock;
use crate::cmd::context::{AccountContext, SourceClient};
use crate::config::AppConfig;
use crate::db::{accounts, jobs, locks::AdvisoryLock, messages};
use crate::embed::OllamaEmbedder;
use crate::ingest::BackfillOptions;
use crate::maildir::Maildir;
use crate::promote::promote_account;
use crate::search::SearchClient;

#[derive(Debug, Clone)]
pub enum JobKind {
    Backfill {
        limit: Option<u64>,
        since: Option<chrono::DateTime<chrono::Utc>>,
    },
    Poll,
    Promote,
    Reindex {
        recreate: bool,
    },
}

impl JobKind {
    pub fn name(&self) -> &'static str {
        match self {
            JobKind::Backfill { .. } => "backfill",
            JobKind::Poll => "poll",
            JobKind::Promote => "promote",
            JobKind::Reindex { .. } => "reindex",
        }
    }

    /// Advisory-lock key: backfill and poll share "sync" so they can
    /// never interleave writes on one account; promote and reindex
    /// share "promote".
    fn lock_name(&self) -> &'static str {
        match self {
            JobKind::Backfill { .. } | JobKind::Poll => "sync",
            JobKind::Promote | JobKind::Reindex { .. } => "promote",
        }
    }
}

pub struct StartedJob {
    pub job_id: i64,
    kind: JobKind,
    account_id: i64,
    lock: AdvisoryLock,
}

/// Take the lock and create the jobs row. `Ok(None)` = another job of
/// this family is already running for the account.
pub async fn try_start(
    config: &AppConfig,
    pool: &PgPool,
    account_id: i64,
    kind: JobKind,
) -> Result<Option<StartedJob>> {
    // Verify the account exists before locking.
    accounts::get(pool, account_id)
        .await?
        .with_context(|| format!("mail account {account_id} not found"))?;
    let Some(lock) =
        AdvisoryLock::try_acquire(&config.database_url, kind.lock_name(), account_id).await?
    else {
        return Ok(None);
    };
    let job_id = jobs::start(pool, kind.name(), Some(account_id)).await?;
    Ok(Some(StartedJob {
        job_id,
        kind,
        account_id,
        lock,
    }))
}

/// Execute the job to completion, record the outcome, release the lock.
pub async fn run(config: &AppConfig, pool: &PgPool, started: StartedJob) -> Result<()> {
    let StartedJob {
        job_id,
        kind,
        account_id,
        lock,
    } = started;

    let outcome = execute(config, pool, account_id, &kind, job_id).await;
    match &outcome {
        Ok(Outcome::Succeeded(stats)) => {
            jobs::succeed(pool, job_id, stats).await?;
            tracing::info!(account = account_id, job = job_id, kind = kind.name(),
                stats = %stats, "job complete");
        }
        Ok(Outcome::Cancelled(stats)) => {
            jobs::update_stats(pool, job_id, stats).await?;
            jobs::mark_cancelled(pool, job_id).await?;
            tracing::info!(
                account = account_id,
                job = job_id,
                kind = kind.name(),
                "job cancelled by request"
            );
        }
        Err(e) => {
            jobs::fail(pool, job_id, &format!("{e:#}")).await?;
        }
    }
    lock.release().await?;
    outcome.map(|_| ())
}

enum Outcome {
    Succeeded(serde_json::Value),
    Cancelled(serde_json::Value),
}

async fn execute(
    config: &AppConfig,
    pool: &PgPool,
    account_id: i64,
    kind: &JobKind,
    job_id: i64,
) -> Result<Outcome> {
    match kind {
        JobKind::Backfill { limit, since } => {
            let ctx = AccountContext::open(config, pool, account_id).await?;
            let options = BackfillOptions {
                limit: *limit,
                since: *since,
                ..Default::default()
            };
            let (stats, retried) = match &ctx.client {
                SourceClient::Jmap(client) => {
                    let stats = crate::jmap::backfill::backfill_account(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                        &options,
                        Some(job_id),
                    )
                    .await?;
                    if stats.cancelled {
                        return Ok(Outcome::Cancelled(serde_json::to_value(&stats)?));
                    }
                    let retried = crate::jmap::backfill::fetch_missing_blobs(
                        pool,
                        client,
                        &ctx.maildir,
                        account_id,
                    )
                    .await?;
                    (stats, retried)
                }
                SourceClient::Gmail(client) => {
                    let stats = crate::gmail::backfill::backfill_account(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                        &options,
                        Some(job_id),
                    )
                    .await?;
                    if stats.cancelled {
                        return Ok(Outcome::Cancelled(serde_json::to_value(&stats)?));
                    }
                    let retried = crate::gmail::backfill::fetch_missing_blobs(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                    )
                    .await?;
                    (stats, retried)
                }
                SourceClient::Imap(client) => {
                    let stats = crate::imap::backfill::backfill_account(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                        &options,
                        Some(job_id),
                    )
                    .await?;
                    if stats.cancelled {
                        return Ok(Outcome::Cancelled(serde_json::to_value(&stats)?));
                    }
                    let retried = crate::imap::sync::fetch_missing_blobs(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                    )
                    .await?;
                    (stats, retried)
                }
                SourceClient::O365(client) => {
                    // The refresh token may rotate mid-run; persist it
                    // whether or not the engine succeeded.
                    let result = crate::o365::backfill::backfill_account(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                        &options,
                        Some(job_id),
                    )
                    .await;
                    crate::o365::checkpoint_token(pool, account_id, client).await?;
                    let stats = result?;
                    if stats.cancelled {
                        return Ok(Outcome::Cancelled(serde_json::to_value(&stats)?));
                    }
                    let retried = crate::o365::backfill::fetch_missing_blobs(
                        pool,
                        client,
                        &ctx.maildir,
                        &ctx.account,
                    )
                    .await;
                    crate::o365::checkpoint_token(pool, account_id, client).await?;
                    (stats, retried?)
                }
            };
            let mut value = serde_json::to_value(&stats)?;
            value["retried_blobs"] = json!(retried);
            Ok(Outcome::Succeeded(value))
        }
        JobKind::Poll => {
            let ctx = AccountContext::open(config, pool, account_id).await?;
            let search = SearchClient::new(&config.opensearch)?;
            let stats = match &ctx.client {
                SourceClient::Jmap(client) => {
                    crate::jmap::sync::poll_account(
                        pool,
                        client,
                        &ctx.maildir,
                        Some(&search),
                        &ctx.account,
                        ctx.deletion_policy(),
                    )
                    .await?
                }
                SourceClient::Gmail(client) => {
                    crate::gmail::sync::poll_account(
                        pool,
                        client,
                        &ctx.maildir,
                        Some(&search),
                        &ctx.account,
                        ctx.deletion_policy(),
                    )
                    .await?
                }
                SourceClient::Imap(client) => {
                    crate::imap::sync::poll_account(
                        pool,
                        client,
                        &ctx.maildir,
                        Some(&search),
                        &ctx.account,
                        ctx.deletion_policy(),
                    )
                    .await?
                }
                SourceClient::O365(client) => {
                    let result = crate::o365::sync::poll_account(
                        pool,
                        client,
                        &ctx.maildir,
                        Some(&search),
                        &ctx.account,
                        ctx.deletion_policy(),
                    )
                    .await;
                    crate::o365::checkpoint_token(pool, account_id, client).await?;
                    result?
                }
            };
            Ok(Outcome::Succeeded(serde_json::to_value(&stats)?))
        }
        JobKind::Promote | JobKind::Reindex { .. } => {
            // No JMAP credential is unsealed on this path.
            let account = accounts::get(pool, account_id)
                .await?
                .context("account vanished mid-job")?;
            let maildir = Maildir::open_or_create(
                config
                    .maildir_root
                    .join(format!("user-{}", account.user_id))
                    .join(format!("account-{}", account.id)),
            )?;
            let search = SearchClient::new(&config.opensearch)?;
            let embedder = OllamaEmbedder::new(&config.embedding)?;

            if let JobKind::Reindex { recreate } = kind {
                if *recreate {
                    search.delete_user_indices(account.user_id).await?;
                }
                let restaged = messages::reset_index_state(pool, account.id).await?;
                tracing::info!(account = account.id, restaged, "restaged for reindex");
            }

            let stats = promote_account(
                pool,
                &maildir,
                &search,
                &embedder,
                &SystemClock,
                &account,
                Some(job_id),
            )
            .await?;
            if stats.cancelled {
                Ok(Outcome::Cancelled(serde_json::to_value(&stats)?))
            } else {
                Ok(Outcome::Succeeded(serde_json::to_value(&stats)?))
            }
        }
    }
}

/// The advisory-lock family a persisted job kind belongs to, mirroring
/// [`JobKind::lock_name`] for a kind read back from the database.
fn lock_family_for_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "backfill" | "poll" => Some("sync"),
        "promote" | "reindex" => Some("promote"),
        _ => None,
    }
}

/// Mark jobs left `running` by a crashed or restarted process as failed.
/// Safe across containers: a job counts as orphaned only if its advisory
/// lock is free — i.e. no process is actually running it. A job a live
/// worker still holds keeps its lock, so we leave it untouched. Call once
/// at web startup, before the server accepts requests.
pub async fn reconcile_orphaned_jobs(config: &AppConfig, pool: &PgPool) -> Result<u64> {
    let mut cleaned = 0u64;
    for job in jobs::running_all(pool).await? {
        let (Some(account_id), Some(family)) =
            (job.mail_account_id, lock_family_for_kind(&job.kind))
        else {
            continue;
        };
        // Acquiring the lock proves nobody is running this family for the
        // account; release it immediately and retire the orphaned row.
        if let Some(lock) =
            AdvisoryLock::try_acquire(&config.database_url, family, account_id).await?
        {
            lock.release().await?;
            jobs::fail(pool, job.id, "interrupted by service restart").await?;
            cleaned += 1;
            tracing::warn!(job = job.id, kind = %job.kind, "reconciled orphaned running job");
        }
    }
    Ok(cleaned)
}

/// Detached execution for web-triggered jobs: logs failures instead of
/// propagating (the jobs row carries the error for the UI).
pub fn spawn_detached(config: AppConfig, pool: PgPool, started: StartedJob) {
    tokio::spawn(run_owned(config, pool, started));
}

async fn run_owned(config: AppConfig, pool: PgPool, started: StartedJob) {
    let job_id = started.job_id;
    if let Err(e) = run(&config, &pool, started).await {
        tracing::error!(job = job_id, error = %format!("{e:#}"), "web-spawned job failed");
    }
}

/// CLI-shaped entry: start-or-skip, then run to completion.
pub async fn run_blocking(
    config: &AppConfig,
    pool: &PgPool,
    account_id: i64,
    kind: JobKind,
) -> Result<()> {
    match try_start(config, pool, account_id, kind.clone()).await? {
        Some(started) => run(config, pool, started).await,
        None => {
            tracing::info!(
                account = account_id,
                kind = kind.name(),
                "another {} job is running; skipping",
                kind.lock_name()
            );
            Ok(())
        }
    }
}
