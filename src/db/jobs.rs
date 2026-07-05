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
