//! OAuth handshake state (migration 0004). The session cookie is
//! SameSite=Strict, so it is not sent on the cross-site redirect back
//! from accounts.google.com — the callback authenticates via one of
//! these rows instead: single-use, expiring, bound to the user who
//! started the flow, and stored as a hash so a database read can't
//! forge a live state parameter.

use anyhow::Result;
use sqlx::PgPool;

pub struct ConsumedState {
    pub user_id: i64,
    pub pkce_verifier: String,
}

pub async fn create(
    pool: &PgPool,
    user_id: i64,
    state_hash: &[u8],
    pkce_verifier: &str,
    ttl_secs: i64,
) -> Result<()> {
    // Opportunistic cleanup keeps the table from accumulating abandoned
    // handshakes; no scheduled job needed at this scale.
    sqlx::query!("delete from oauth_states where expires_at < now() - interval '1 hour'")
        .execute(pool)
        .await?;
    sqlx::query!(
        r#"insert into oauth_states (user_id, state_hash, pkce_verifier, expires_at)
           values ($1, $2, $3, now() + make_interval(secs => $4))"#,
        user_id,
        state_hash,
        pkce_verifier,
        ttl_secs as f64,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Atomically consume a live state row: exactly one caller can ever get
/// `Some` for a given state, however many callbacks race.
pub async fn consume_by_hash(pool: &PgPool, state_hash: &[u8]) -> Result<Option<ConsumedState>> {
    let row = sqlx::query_as!(
        ConsumedState,
        r#"update oauth_states set consumed_at = now()
           where state_hash = $1 and consumed_at is null and expires_at > now()
           returning user_id, pkce_verifier"#,
        state_hash,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row)
}
