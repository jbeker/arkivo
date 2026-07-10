//! Source-independent pieces of the ingestion engines. Both the JMAP and
//! Gmail sync/backfill modules share these: the stats shapes the jobs UI
//! renders, and the deletion-policy application (spec §7.6), which must
//! behave identically — audit records and index cleanup included —
//! whichever source reported the deletion.

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::config::DeletionPolicy;
use crate::db::{audit, messages};
use crate::maildir::MessageStore;
use crate::search::SearchClient;

#[derive(Debug, Default, serde::Serialize)]
pub struct SyncStats {
    pub fetched: u64,
    pub updated: u64,
    pub destroyed: u64,
    pub resynced: bool,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct BackfillStats {
    pub fetched: u64,
    pub pages: u64,
    pub complete: bool,
    /// Server-reported mailbox total, from the first listing page.
    pub total: Option<u64>,
    /// Stopped early because cancellation was requested.
    pub cancelled: bool,
    /// Messages whose metadata landed but whose body download failed this
    /// sweep. Their rows carry `maildir_path IS NULL`; the source's
    /// missing-blob retry pass recovers them after the sweep completes.
    pub failed: u64,
}

pub struct BackfillOptions {
    pub limit: Option<u64>,
    pub since: Option<DateTime<Utc>>,
    pub page_size: u64,
}

impl Default for BackfillOptions {
    fn default() -> Self {
        Self {
            limit: None,
            since: None,
            page_size: 100,
        }
    }
}

/// Apply one upstream-destroyed message per the account's deletion policy
/// (spec §7.6). `remote_id` is the source-side message id — JMAP Email id
/// or Gmail message id, whichever the account's provider uses. `search`
/// is optional so credential-free test paths can skip index cleanup;
/// production polling always passes it.
pub async fn apply_destroyed(
    pool: &PgPool,
    store: &dyn MessageStore,
    search: Option<&SearchClient>,
    mail_account_id: i64,
    user_id: i64,
    remote_id: &str,
    policy: DeletionPolicy,
) -> Result<()> {
    let Some(msg) = messages::get_by_jmap_id(pool, mail_account_id, remote_id).await? else {
        return Ok(()); // never knew this message
    };
    match policy {
        DeletionPolicy::Retain => {
            audit::record(
                pool,
                Some(user_id),
                "system:poll",
                "message_destroyed_upstream_retained",
                Some(&msg.id.to_string()),
                None,
            )
            .await?;
        }
        DeletionPolicy::Mirror => {
            if let Some(path) = &msg.maildir_path {
                match store.remove(path) {
                    Ok(()) => {}
                    // Already gone is fine; anything else is not.
                    Err(e) if !file_missing(&e) => return Err(e),
                    Err(_) => {}
                }
            }
            if let Some(search) = search {
                search.delete_message_docs(user_id, msg.id).await?;
            }
            messages::delete_row(pool, msg.id).await?;
            audit::record(
                pool,
                Some(user_id),
                "system:poll",
                "message_destroyed_upstream_mirrored",
                Some(&msg.id.to_string()),
                None,
            )
            .await?;
        }
    }
    Ok(())
}

fn file_missing(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .map(|io| io.kind() == std::io::ErrorKind::NotFound)
        .unwrap_or(false)
}
