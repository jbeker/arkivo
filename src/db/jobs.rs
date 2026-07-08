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
