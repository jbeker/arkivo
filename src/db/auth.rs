//! Repos for the passkey-only auth surface (spec §11): passkey
//! credentials, admin-issued invites, and single-use recovery codes.

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PasskeyRow {
    pub id: i64,
    pub user_id: i64,
    pub credential_id: Vec<u8>,
    /// Serialized webauthn_rs::prelude::Passkey.
    pub passkey: serde_json::Value,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

pub async fn insert_passkey(
    pool: &PgPool,
    user_id: i64,
    credential_id: &[u8],
    passkey: &serde_json::Value,
    label: &str,
) -> Result<i64> {
    let rec = sqlx::query!(
        r#"insert into passkeys (user_id, credential_id, passkey, label)
           values ($1, $2, $3, $4) returning id"#,
        user_id,
        credential_id,
        passkey,
        label,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.id)
}

pub async fn passkeys_for_user(pool: &PgPool, user_id: i64) -> Result<Vec<PasskeyRow>> {
    let rows = sqlx::query_as!(
        PasskeyRow,
        r#"select id, user_id, credential_id, passkey, label, created_at, last_used_at
           from passkeys where user_id = $1 order by id"#,
        user_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn update_passkey(pool: &PgPool, id: i64, passkey: &serde_json::Value) -> Result<()> {
    sqlx::query!(
        "update passkeys set passkey = $2, last_used_at = now() where id = $1",
        id,
        passkey,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete one of the user's passkeys, refusing to remove the last one
/// (passkeys are the only way in; spec §11.1).
pub async fn delete_passkey(pool: &PgPool, user_id: i64, passkey_id: i64) -> Result<bool> {
    let result = sqlx::query!(
        r#"delete from passkeys
           where id = $1 and user_id = $2
             and (select count(*) from passkeys where user_id = $2) > 1"#,
        passkey_id,
        user_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn user_webauthn_uuid(pool: &PgPool, user_id: i64) -> Result<Uuid> {
    let rec = sqlx::query!("select webauthn_uuid from users where id = $1", user_id)
        .fetch_one(pool)
        .await?;
    Ok(rec.webauthn_uuid)
}

/// Create a user inside a registration transaction, with the ceremony's
/// UUID as the WebAuthn handle.
pub async fn create_user_with_uuid(
    tx: &mut sqlx::PgConnection,
    handle: &str,
    role: &str,
    webauthn_uuid: Uuid,
) -> Result<i64> {
    let rec = sqlx::query!(
        r#"insert into users (handle, role, webauthn_uuid)
           values ($1, $2, $3) returning id"#,
        handle,
        role,
        webauthn_uuid,
    )
    .fetch_one(tx)
    .await?;
    Ok(rec.id)
}

// ---- invites -------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Invite {
    pub id: i64,
    pub role: String,
    pub expires_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}

pub async fn create_invite(
    pool: &PgPool,
    code_hash: &[u8],
    created_by: Option<i64>,
    role: &str,
    ttl_days: i64,
) -> Result<i64> {
    let rec = sqlx::query!(
        r#"insert into invites (code_hash, created_by, role, expires_at)
           values ($1, $2, $3, now() + make_interval(days => $4::int))
           returning id"#,
        code_hash,
        created_by,
        role,
        ttl_days as i32,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.id)
}

/// Look up a live (unconsumed, unexpired) invite by code hash.
pub async fn find_live_invite(pool: &PgPool, code_hash: &[u8]) -> Result<Option<Invite>> {
    let invite = sqlx::query_as!(
        Invite,
        r#"select id, role, expires_at, consumed_at from invites
           where code_hash = $1 and consumed_at is null and expires_at > now()"#,
        code_hash,
    )
    .fetch_optional(pool)
    .await?;
    Ok(invite)
}

/// Consume atomically; returns false if it was already consumed (race).
pub async fn consume_invite(tx: &mut sqlx::PgConnection, id: i64, user_id: i64) -> Result<bool> {
    let result = sqlx::query!(
        r#"update invites set consumed_at = now(), consumed_by = $2
           where id = $1 and consumed_at is null and expires_at > now()"#,
        id,
        user_id,
    )
    .execute(tx)
    .await?;
    Ok(result.rows_affected() > 0)
}

// ---- recovery codes -------------------------------------------------------

pub async fn create_recovery_code(
    pool: &PgPool,
    user_id: i64,
    code_hash: &[u8],
    ttl_days: i64,
) -> Result<i64> {
    let rec = sqlx::query!(
        r#"insert into recovery_codes (user_id, code_hash, expires_at)
           values ($1, $2, now() + make_interval(days => $3::int))
           returning id"#,
        user_id,
        code_hash,
        ttl_days as i32,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.id)
}

pub async fn find_live_recovery(
    pool: &PgPool,
    user_id: i64,
    code_hash: &[u8],
) -> Result<Option<i64>> {
    let rec = sqlx::query!(
        r#"select id from recovery_codes
           where user_id = $1 and code_hash = $2
             and consumed_at is null and expires_at > now()"#,
        user_id,
        code_hash,
    )
    .fetch_optional(pool)
    .await?;
    Ok(rec.map(|r| r.id))
}

pub async fn consume_recovery(tx: &mut sqlx::PgConnection, id: i64) -> Result<bool> {
    let result = sqlx::query!(
        r#"update recovery_codes set consumed_at = now()
           where id = $1 and consumed_at is null and expires_at > now()"#,
        id,
    )
    .execute(tx)
    .await?;
    Ok(result.rows_affected() > 0)
}
