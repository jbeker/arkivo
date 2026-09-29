use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    pub id: i64,
    pub kind: String,
    pub mail_account_id: Option<i64>,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stats: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// Periodic progress update from a running job (per page/batch).
pub async fn update_stats(pool: &PgPool, id: i64, stats: &serde_json::Value) -> Result<()> {
    sqlx::query!("update jobs set stats = $2 where id = $1", id, stats)
        .execute(pool)
        .await?;
    Ok(())
}

/// Flag a job for cooperative cancellation. Ownership is enforced
/// through the account join: users can only cancel their own jobs.
pub async fn request_cancel(pool: &PgPool, job_id: i64, user_id: i64) -> Result<bool> {
    let result = sqlx::query!(
        r#"update jobs j set cancel_requested_at = now()
           from mail_accounts a
           where j.id = $1 and j.status = 'running'
             and a.id = j.mail_account_id and a.user_id = $2"#,
        job_id,
        user_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn is_cancel_requested(pool: &PgPool, id: i64) -> Result<bool> {
    let rec = sqlx::query!(
        r#"select cancel_requested_at is not null as "cancelled!" from jobs where id = $1"#,
        id,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.cancelled)
}

pub async fn mark_cancelled(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query!(
        "update jobs set status = 'cancelled', finished_at = now() where id = $1",
        id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Running jobs across all of a user's accounts (dashboard progress).
pub async fn running_for_user(pool: &PgPool, user_id: i64) -> Result<Vec<Job>> {
    let jobs = sqlx::query_as!(
        Job,
        r#"select j.id, j.kind, j.mail_account_id, j.status, j.started_at,
                  j.finished_at, j.stats, j.error
           from jobs j
           join mail_accounts a on a.id = j.mail_account_id
           where a.user_id = $1 and j.status = 'running'
           order by j.started_at"#,
        user_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(jobs)
}

/// Every running job, regardless of owner. The startup reconciler uses
/// this to find rows orphaned by a crashed or restarted process. Runtime
/// query (not the macro) so no new offline metadata is needed to build.
pub async fn running_all(pool: &PgPool) -> Result<Vec<Job>> {
    let jobs = sqlx::query_as::<_, Job>(
        r#"select id, kind, mail_account_id, status, started_at,
                  finished_at, stats, error
           from jobs where status = 'running' order by started_at"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(jobs)
}

pub async fn start(pool: &PgPool, kind: &str, mail_account_id: Option<i64>) -> Result<i64> {
    let rec = sqlx::query!(
        "insert into jobs (kind, mail_account_id) values ($1, $2) returning id",
        kind,
        mail_account_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.id)
}

pub async fn succeed(pool: &PgPool, id: i64, stats: &serde_json::Value) -> Result<()> {
    sqlx::query!(
        r#"update jobs set status = 'succeeded', finished_at = now(), stats = $2
           where id = $1"#,
        id,
        stats,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn fail(pool: &PgPool, id: i64, error: &str) -> Result<()> {
    sqlx::query!(
        r#"update jobs set status = 'failed', finished_at = now(), error = $2
           where id = $1"#,
        id,
        error,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn recent(pool: &PgPool, mail_account_id: i64, limit: i64) -> Result<Vec<Job>> {
    let jobs = sqlx::query_as!(
        Job,
        r#"select id, kind, mail_account_id, status, started_at, finished_at, stats, error
           from jobs where mail_account_id = $1
           order by started_at desc limit $2"#,
        mail_account_id,
        limit,
    )
    .fetch_all(pool)
    .await?;
    Ok(jobs)
}

/// Most recent failed jobs of any kind for one account, newest first.
/// Backs the dashboard's "recent errors" list, which must outlive newer
/// successful jobs of other kinds (a succeeded promote should not hide a
/// failed poll).
pub async fn recent_failures(pool: &PgPool, mail_account_id: i64, limit: i64) -> Result<Vec<Job>> {
    let jobs = sqlx::query_as!(
        Job,
        r#"select id, kind, mail_account_id, status, started_at, finished_at, stats, error
           from jobs where mail_account_id = $1 and status = 'failed'
           order by started_at desc limit $2"#,
        mail_account_id,
        limit,
    )
    .fetch_all(pool)
    .await?;
    Ok(jobs)
}

/// Per-account mail-sync health derived from the `jobs` history. "Sync"
/// means `poll` and `backfill`; promote/reindex do not touch the mailbox
/// and are excluded. Accounts with no terminal sync job have no row.
#[derive(Debug, Clone)]
pub struct SyncSummary {
    pub mail_account_id: i64,
    /// `finished_at` of the newest succeeded sync job.
    pub last_success_at: Option<DateTime<Utc>>,
    /// The newest sync job that reached a terminal status, whatever it was.
    pub last_attempt_at: DateTime<Utc>,
    pub last_attempt_finished_at: Option<DateTime<Utc>>,
    pub last_attempt_kind: String,
    pub last_attempt_status: String,
    pub last_attempt_error: Option<String>,
    /// Failed sync jobs started after the last success (all of them when
    /// there has never been a success). Zero when the last attempt succeeded.
    pub consecutive_failures: i64,
}

/// One [`SyncSummary`] per account owned by `user_id` that has at least one
/// finished sync job. Batched so the dashboard reads it in one round trip.
pub async fn sync_summary_for_user(pool: &PgPool, user_id: i64) -> Result<Vec<SyncSummary>> {
    let rows = sqlx::query_as!(
        SyncSummary,
        r#"with sync_jobs as (
               select j.mail_account_id, j.kind, j.status, j.started_at, j.finished_at, j.error
               from jobs j
               join mail_accounts a on a.id = j.mail_account_id
               where a.user_id = $1
                 and j.kind in ('poll', 'backfill')
                 and j.status <> 'running'
           ),
           last_success as (
               select mail_account_id, max(finished_at) as at
               from sync_jobs where status = 'succeeded'
               group by mail_account_id
           ),
           last_attempt as (
               select distinct on (mail_account_id)
                      mail_account_id, kind, status, started_at, finished_at, error
               from sync_jobs
               order by mail_account_id, started_at desc
           )
           select la.mail_account_id as "mail_account_id!",
                  ls.at as "last_success_at?",
                  la.started_at as "last_attempt_at!",
                  la.finished_at as "last_attempt_finished_at?",
                  la.kind as "last_attempt_kind!",
                  la.status as "last_attempt_status!",
                  la.error as "last_attempt_error?",
                  (select count(*) from sync_jobs f
                    where f.mail_account_id = la.mail_account_id
                      and f.status = 'failed'
                      and f.started_at > coalesce(ls.at, '-infinity'::timestamptz))
                      as "consecutive_failures!"
           from last_attempt la
           left join last_success ls on ls.mail_account_id = la.mail_account_id
           order by la.mail_account_id"#,
        user_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
