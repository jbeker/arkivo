use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct McpToken {
    pub id: i64,
    pub user_id: i64,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

pub async fn mint(pool: &PgPool, user_id: i64, token_hash: &[u8], label: &str) -> Result<McpToken> {
    let token = sqlx::query_as!(
        McpToken,
        r#"insert into mcp_tokens (user_id, token_hash, label)
           values ($1, $2, $3)
           returning id, user_id, label, created_at, last_used_at, revoked_at"#,
        user_id,
        token_hash,
        label,
    )
    .fetch_one(pool)
    .await?;
    Ok(token)
}

/// Resolve a presented token hash to its owner, refusing revoked tokens
/// and disabled users. Touches last_used_at as a side effect.
pub async fn resolve_active(pool: &PgPool, token_hash: &[u8]) -> Result<Option<McpToken>> {
    let token = sqlx::query_as!(
        McpToken,
        r#"update mcp_tokens t
           set last_used_at = now()
           from users u
           where t.token_hash = $1
             and t.revoked_at is null
             and u.id = t.user_id
             and u.disabled_at is null
           returning t.id, t.user_id, t.label, t.created_at, t.last_used_at, t.revoked_at"#,
        token_hash,
    )
    .fetch_optional(pool)
    .await?;
    Ok(token)
}

pub async fn revoke(pool: &PgPool, user_id: i64, token_id: i64) -> Result<bool> {
    let result = sqlx::query!(
        r#"update mcp_tokens set revoked_at = now()
           where id = $1 and user_id = $2 and revoked_at is null"#,
        token_id,
        user_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_for_user(pool: &PgPool, user_id: i64) -> Result<Vec<McpToken>> {
    let tokens = sqlx::query_as!(
        McpToken,
        r#"select id, user_id, label, created_at, last_used_at, revoked_at
           from mcp_tokens where user_id = $1 order by id"#,
        user_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(tokens)
}
