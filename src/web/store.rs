//! Postgres session store for tower-sessions, on the app's own sqlx
//! pool (the published sqlx store pins an older sqlx major).

use async_trait::async_trait;
use time::OffsetDateTime;
use tower_sessions::SessionStore;
use tower_sessions::session::{Id, Record};
use tower_sessions::session_store::{Error, Result};

#[derive(Debug, Clone)]
pub struct PgSessionStore {
    pool: sqlx::PgPool,
}

impl PgSessionStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Remove expired rows; call opportunistically or from cron.
    pub async fn cleanup_expired(&self) -> anyhow::Result<u64> {
        let result = sqlx::query!(
            "delete from sessions where expiry_unix < extract(epoch from now())::bigint"
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

fn backend(e: impl std::fmt::Display) -> Error {
    Error::Backend(e.to_string())
}

#[async_trait]
impl SessionStore for PgSessionStore {
    async fn save(&self, record: &Record) -> Result<()> {
        let data = serde_json::to_value(&record.data).map_err(backend)?;
        sqlx::query!(
            r#"insert into sessions (id, data, expiry_unix) values ($1, $2, $3)
               on conflict (id) do update set data = excluded.data,
                                              expiry_unix = excluded.expiry_unix"#,
            record.id.to_string(),
            data,
            record.expiry_date.unix_timestamp(),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> Result<Option<Record>> {
        let row = sqlx::query!(
            r#"select data, expiry_unix from sessions
               where id = $1 and expiry_unix >= extract(epoch from now())::bigint"#,
            session_id.to_string(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        let Some(row) = row else { return Ok(None) };
        let data = serde_json::from_value(row.data).map_err(backend)?;
        let expiry_date = OffsetDateTime::from_unix_timestamp(row.expiry_unix).map_err(backend)?;
        Ok(Some(Record {
            id: *session_id,
            data,
            expiry_date,
        }))
    }

    async fn delete(&self, session_id: &Id) -> Result<()> {
        sqlx::query!("delete from sessions where id = $1", session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }
}
