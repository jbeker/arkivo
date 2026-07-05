use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AuditEntry {
    pub id: i64,
    pub user_id: Option<i64>,
    pub actor: String,
    pub action: String,
    pub message_ref: Option<String>,
    pub detail: Option<serde_json::Value>,
    pub at: DateTime<Utc>,
}

pub async fn record(
    pool: &PgPool,
    user_id: Option<i64>,
    actor: &str,
    action: &str,
    message_ref: Option<&str>,
    detail: Option<&serde_json::Value>,
) -> Result<()> {
    sqlx::query!(
        r#"insert into audit_log (user_id, actor, action, message_ref, detail)
           values ($1, $2, $3, $4, $5)"#,
        user_id,
        actor,
        action,
        message_ref,
        detail,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn recent_for_user(pool: &PgPool, user_id: i64, limit: i64) -> Result<Vec<AuditEntry>> {
    let entries = sqlx::query_as!(
        AuditEntry,
        r#"select id, user_id, actor, action, message_ref, detail, at
           from audit_log where user_id = $1
           order by at desc limit $2"#,
        user_id,
        limit,
    )
    .fetch_all(pool)
    .await?;
    Ok(entries)
}
