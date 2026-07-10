use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MailAccount {
    pub id: i64,
    pub user_id: i64,
    /// "jmap" or "gmail".
    pub provider: String,
    /// Set for provider="jmap" only.
    pub jmap_session_url: Option<String>,
    /// JMAP accountId, or the granted Gmail address for provider="gmail".
    pub account_id: Option<String>,
    pub sealed_token: Vec<u8>,
    pub seal_key_id: String,
    pub recency_cutoff_days: i32,
    pub deletion_policy: String,
    pub poll_interval_secs: Option<i32>,
    pub sanitize_policy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JmapState {
    pub mail_account_id: i64,
    pub email_state: Option<String>,
    pub backfill_done: bool,
    pub backfill_anchor: Option<serde_json::Value>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GmailState {
    pub mail_account_id: i64,
    pub history_id: Option<String>,
    pub backfill_done: bool,
    pub backfill_cursor: Option<serde_json::Value>,
    pub updated_at: DateTime<Utc>,
}

macro_rules! account_query {
    ($sql:literal $(, $bind:expr)*) => {
        sqlx::query_as!(
            MailAccount,
            $sql
            $(, $bind)*
        )
    };
}

pub async fn create(
    pool: &PgPool,
    user_id: i64,
    jmap_session_url: &str,
    sealed_token: &[u8],
    seal_key_id: &str,
) -> Result<MailAccount> {
    let mut tx = pool.begin().await?;
    let account = account_query!(
        r#"insert into mail_accounts (user_id, jmap_session_url, sealed_token, seal_key_id)
           values ($1, $2, $3, $4)
           returning id, user_id, provider, jmap_session_url, account_id, sealed_token,
                     seal_key_id, recency_cutoff_days, deletion_policy,
                     poll_interval_secs, sanitize_policy, created_at"#,
        user_id,
        jmap_session_url,
        sealed_token,
        seal_key_id
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query!(
        "insert into jmap_state (mail_account_id) values ($1)",
        account.id,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(account)
}

pub async fn get(pool: &PgPool, id: i64) -> Result<Option<MailAccount>> {
    let account = account_query!(
        r#"select id, user_id, provider, jmap_session_url, account_id, sealed_token,
                  seal_key_id, recency_cutoff_days, deletion_policy,
                  poll_interval_secs, sanitize_policy, created_at
           from mail_accounts where id = $1"#,
        id
    )
    .fetch_optional(pool)
    .await?;
    Ok(account)
}

pub async fn list_for_user(pool: &PgPool, user_id: i64) -> Result<Vec<MailAccount>> {
    let accounts = account_query!(
        r#"select id, user_id, provider, jmap_session_url, account_id, sealed_token,
                  seal_key_id, recency_cutoff_days, deletion_policy,
                  poll_interval_secs, sanitize_policy, created_at
           from mail_accounts where user_id = $1 order by id"#,
        user_id
    )
    .fetch_all(pool)
    .await?;
    Ok(accounts)
}

pub async fn delete(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query!("delete from mail_accounts where id = $1", id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_jmap_account_id(pool: &PgPool, id: i64, account_id: &str) -> Result<()> {
    sqlx::query!(
        "update mail_accounts set account_id = $2 where id = $1",
        id,
        account_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_jmap_state(pool: &PgPool, mail_account_id: i64) -> Result<Option<JmapState>> {
    let state = sqlx::query_as!(
        JmapState,
        r#"select mail_account_id, email_state, backfill_done, backfill_anchor, updated_at
           from jmap_state where mail_account_id = $1"#,
        mail_account_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(state)
}

/// Advance the stored JMAP state. Callers must only invoke this after the
/// corresponding batch is durably written (spec §7.3).
pub async fn set_email_state(pool: &PgPool, mail_account_id: i64, email_state: &str) -> Result<()> {
    sqlx::query!(
        r#"update jmap_state set email_state = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        email_state,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_backfill_anchor(
    pool: &PgPool,
    mail_account_id: i64,
    anchor: Option<&serde_json::Value>,
) -> Result<()> {
    sqlx::query!(
        r#"update jmap_state set backfill_anchor = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        anchor,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_backfill_done(pool: &PgPool, mail_account_id: i64) -> Result<()> {
    sqlx::query!(
        r#"update jmap_state
           set backfill_done = true, backfill_anchor = null, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn create_gmail(
    pool: &PgPool,
    user_id: i64,
    email: &str,
    sealed_refresh_token: &[u8],
    seal_key_id: &str,
) -> Result<MailAccount> {
    let mut tx = pool.begin().await?;
    let account = account_query!(
        r#"insert into mail_accounts (user_id, provider, account_id, sealed_token, seal_key_id)
           values ($1, 'gmail', $2, $3, $4)
           returning id, user_id, provider, jmap_session_url, account_id, sealed_token,
                     seal_key_id, recency_cutoff_days, deletion_policy,
                     poll_interval_secs, sanitize_policy, created_at"#,
        user_id,
        email,
        sealed_refresh_token,
        seal_key_id
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query!(
        "insert into gmail_state (mail_account_id) values ($1)",
        account.id,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(account)
}

pub async fn get_gmail_state(pool: &PgPool, mail_account_id: i64) -> Result<Option<GmailState>> {
    let state = sqlx::query_as!(
        GmailState,
        r#"select mail_account_id, history_id, backfill_done, backfill_cursor, updated_at
           from gmail_state where mail_account_id = $1"#,
        mail_account_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(state)
}

/// Advance the stored Gmail history id. Callers must only invoke this
/// after the corresponding batch is durably written (spec §7.3 applies
/// to Gmail the same as to JMAP state strings).
pub async fn set_history_id(pool: &PgPool, mail_account_id: i64, history_id: &str) -> Result<()> {
    sqlx::query!(
        r#"update gmail_state set history_id = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        history_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_gmail_backfill_cursor(
    pool: &PgPool,
    mail_account_id: i64,
    cursor: Option<&serde_json::Value>,
) -> Result<()> {
    sqlx::query!(
        r#"update gmail_state set backfill_cursor = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        cursor,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_gmail_backfill_done(pool: &PgPool, mail_account_id: i64) -> Result<()> {
    sqlx::query!(
        r#"update gmail_state
           set backfill_done = true, backfill_cursor = null, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}
