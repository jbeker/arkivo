use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MailAccount {
    pub id: i64,
    pub user_id: i64,
    /// "jmap", "gmail", "imap", or "o365".
    pub provider: String,
    /// Set for provider="jmap" only.
    pub jmap_session_url: Option<String>,
    /// JMAP accountId, the granted Gmail address for provider="gmail",
    /// the login username for provider="imap", or the user principal
    /// name for provider="o365".
    pub account_id: Option<String>,
    pub sealed_token: Vec<u8>,
    pub seal_key_id: String,
    pub recency_cutoff_days: i32,
    pub deletion_policy: String,
    pub poll_interval_secs: Option<i32>,
    pub sanitize_policy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    /// Set = paused: scheduled polling skips this account (manual jobs
    /// and scheduled promotion still run).
    pub disabled_at: Option<DateTime<Utc>>,
    /// Set for provider="imap" only.
    pub imap_host: Option<String>,
    pub imap_port: Option<i32>,
    /// "implicit", "starttls", or "none".
    pub imap_tls: Option<String>,
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
pub struct ImapState {
    pub mail_account_id: i64,
    pub backfill_done: bool,
    pub backfill_cursor: Option<serde_json::Value>,
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct O365State {
    pub mail_account_id: i64,
    pub backfill_done: bool,
    pub backfill_since: Option<DateTime<Utc>>,
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
                     poll_interval_secs, sanitize_policy, created_at, disabled_at,
                     imap_host, imap_port, imap_tls"#,
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
                  poll_interval_secs, sanitize_policy, created_at, disabled_at,
                  imap_host, imap_port, imap_tls
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
                  poll_interval_secs, sanitize_policy, created_at, disabled_at,
                  imap_host, imap_port, imap_tls
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

/// Pause or resume scheduled polling for an account. Idempotent;
/// re-disabling refreshes the timestamp.
pub async fn set_disabled(pool: &PgPool, id: i64, disabled: bool) -> Result<()> {
    sqlx::query!(
        r#"update mail_accounts
           set disabled_at = case when $2 then now() else null end
           where id = $1"#,
        id,
        disabled,
    )
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
                     poll_interval_secs, sanitize_policy, created_at, disabled_at,
                     imap_host, imap_port, imap_tls"#,
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

#[allow(clippy::too_many_arguments)]
pub async fn create_imap(
    pool: &PgPool,
    user_id: i64,
    username: &str,
    host: &str,
    port: i32,
    tls: &str,
    sealed_password: &[u8],
    seal_key_id: &str,
) -> Result<MailAccount> {
    let mut tx = pool.begin().await?;
    let account = account_query!(
        r#"insert into mail_accounts
               (user_id, provider, account_id, imap_host, imap_port, imap_tls,
                sealed_token, seal_key_id)
           values ($1, 'imap', $2, $3, $4, $5, $6, $7)
           returning id, user_id, provider, jmap_session_url, account_id, sealed_token,
                     seal_key_id, recency_cutoff_days, deletion_policy,
                     poll_interval_secs, sanitize_policy, created_at, disabled_at,
                     imap_host, imap_port, imap_tls"#,
        user_id,
        username,
        host,
        port,
        tls,
        sealed_password,
        seal_key_id
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query!(
        "insert into imap_state (mail_account_id) values ($1)",
        account.id,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(account)
}

pub async fn get_imap_state(pool: &PgPool, mail_account_id: i64) -> Result<Option<ImapState>> {
    let state = sqlx::query_as!(
        ImapState,
        r#"select mail_account_id, backfill_done, backfill_cursor, updated_at
           from imap_state where mail_account_id = $1"#,
        mail_account_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(state)
}

/// Persist the resumable backfill cursor. Callers must only invoke this
/// after the corresponding page is durably written (spec §7.3).
pub async fn set_imap_backfill_cursor(
    pool: &PgPool,
    mail_account_id: i64,
    cursor: Option<&serde_json::Value>,
) -> Result<()> {
    sqlx::query!(
        r#"update imap_state set backfill_cursor = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        cursor,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_imap_backfill_done(pool: &PgPool, mail_account_id: i64) -> Result<()> {
    sqlx::query!(
        r#"update imap_state
           set backfill_done = true, backfill_cursor = null, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn create_o365(
    pool: &PgPool,
    user_id: i64,
    principal: &str,
    sealed_refresh_token: &[u8],
    seal_key_id: &str,
) -> Result<MailAccount> {
    let mut tx = pool.begin().await?;
    let account = account_query!(
        r#"insert into mail_accounts (user_id, provider, account_id, sealed_token, seal_key_id)
           values ($1, 'o365', $2, $3, $4)
           returning id, user_id, provider, jmap_session_url, account_id, sealed_token,
                     seal_key_id, recency_cutoff_days, deletion_policy,
                     poll_interval_secs, sanitize_policy, created_at, disabled_at,
                     imap_host, imap_port, imap_tls"#,
        user_id,
        principal,
        sealed_refresh_token,
        seal_key_id
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query!(
        "insert into o365_state (mail_account_id) values ($1)",
        account.id,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(account)
}

pub async fn get_o365_state(pool: &PgPool, mail_account_id: i64) -> Result<Option<O365State>> {
    let state = sqlx::query_as!(
        O365State,
        r#"select mail_account_id, backfill_done, backfill_since, updated_at
           from o365_state where mail_account_id = $1"#,
        mail_account_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(state)
}

/// Flip the backfill-done flag. Takes a bool because a backfill with a
/// wider date floor re-walks every folder and must un-set it first.
pub async fn set_o365_backfill_done(pool: &PgPool, mail_account_id: i64, done: bool) -> Result<()> {
    sqlx::query!(
        r#"update o365_state set backfill_done = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        done,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_o365_backfill_since(
    pool: &PgPool,
    mail_account_id: i64,
    since: Option<DateTime<Utc>>,
) -> Result<()> {
    sqlx::query!(
        r#"update o365_state set backfill_since = $2, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
        since,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Bump the state row's timestamp so the dashboard's "last sync" reflects
/// a poll that changed only per-folder rows.
pub async fn touch_o365_state(pool: &PgPool, mail_account_id: i64) -> Result<()> {
    sqlx::query!(
        "update o365_state set updated_at = now() where mail_account_id = $1",
        mail_account_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Replace the sealed credential in place. Microsoft rotates refresh
/// tokens on every redemption; the key id is unchanged because the same
/// sealer that unsealed the old token sealed the new one.
pub async fn set_sealed_token(pool: &PgPool, id: i64, sealed: &[u8]) -> Result<()> {
    sqlx::query!(
        "update mail_accounts set sealed_token = $2 where id = $1",
        id,
        sealed,
    )
    .execute(pool)
    .await?;
    Ok(())
}
