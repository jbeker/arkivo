//! Single-instance guard for batch subcommands (spec §7.4), built on
//! Postgres advisory locks instead of a lock table: the lock is tied to
//! the connection's backend session, so a crashed run can never wedge
//! the next scheduled start.

use anyhow::Result;
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection};

pub struct AdvisoryLock {
    conn: Option<PgConnection>,
    key: i64,
}

fn lock_key(name: &str, mail_account_id: i64) -> i64 {
    let digest = Sha256::digest(format!("arkivo:{name}:{mail_account_id}").as_bytes());
    i64::from_le_bytes(digest[..8].try_into().unwrap())
}

impl AdvisoryLock {
    /// Try to take the (name, account) lock on a dedicated connection.
    /// Returns None if another holder has it.
    pub async fn try_acquire(
        database_url: &str,
        name: &str,
        mail_account_id: i64,
    ) -> Result<Option<Self>> {
        let mut conn = PgConnection::connect(database_url).await?;
        let key = lock_key(name, mail_account_id);
        let acquired: bool = sqlx::query_scalar("select pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut conn)
            .await?;
        if acquired {
            Ok(Some(Self {
                conn: Some(conn),
                key,
            }))
        } else {
            conn.close().await.ok();
            Ok(None)
        }
    }

    /// Release explicitly. Dropping without release also frees the lock
    /// (the connection closes and the backend session ends), just less
    /// gracefully.
    pub async fn release(mut self) -> Result<()> {
        if let Some(mut conn) = self.conn.take() {
            sqlx::query("select pg_advisory_unlock($1)")
                .bind(self.key)
                .execute(&mut conn)
                .await?;
            conn.close().await?;
        }
        Ok(())
    }
}
