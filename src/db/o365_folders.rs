//! Per-folder Graph delta state for Office 365 accounts. Graph's message
//! delta is per folder, so both the in-progress initial-walk cursor
//! (`next_link`) and the incremental cursor (`delta_link`) live here,
//! one row per archivable folder. Placement itself is not tracked here:
//! an Exchange message is in exactly one folder, and that folder's
//! display path is the single element of `messages.mailbox_ids`.

use anyhow::Result;
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct O365Folder {
    pub folder_id: String,
    pub display_path: String,
    pub well_known_name: Option<String>,
    pub delta_link: Option<String>,
    pub next_link: Option<String>,
    pub resync_pending: bool,
    pub total_item_count: Option<i64>,
}

pub async fn list(pool: &PgPool, mail_account_id: i64) -> Result<Vec<O365Folder>> {
    let rows = sqlx::query_as!(
        O365Folder,
        r#"select folder_id, display_path, well_known_name, delta_link, next_link,
                  resync_pending, total_item_count
           from o365_folders where mail_account_id = $1
           order by display_path"#,
        mail_account_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn get(
    pool: &PgPool,
    mail_account_id: i64,
    folder_id: &str,
) -> Result<Option<O365Folder>> {
    let row = sqlx::query_as!(
        O365Folder,
        r#"select folder_id, display_path, well_known_name, delta_link, next_link,
                  resync_pending, total_item_count
           from o365_folders where mail_account_id = $1 and folder_id = $2"#,
        mail_account_id,
        folder_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Record a folder's current name/path and size. Never touches the
/// cursor columns, so calling it on every run is safe.
pub async fn upsert(
    pool: &PgPool,
    mail_account_id: i64,
    folder_id: &str,
    display_path: &str,
    well_known_name: Option<&str>,
    total_item_count: Option<i64>,
) -> Result<()> {
    sqlx::query!(
        r#"insert into o365_folders
               (mail_account_id, folder_id, display_path, well_known_name, total_item_count)
           values ($1, $2, $3, $4, $5)
           on conflict (mail_account_id, folder_id) do update
               set display_path = excluded.display_path,
                   well_known_name = excluded.well_known_name,
                   total_item_count = excluded.total_item_count,
                   updated_at = now()"#,
        mail_account_id,
        folder_id,
        display_path,
        well_known_name,
        total_item_count,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Persist the initial-walk cursor. Callers must only invoke this after
/// the corresponding page is durably written (spec §7.3).
pub async fn set_next_link(
    pool: &PgPool,
    mail_account_id: i64,
    folder_id: &str,
    next_link: Option<&str>,
) -> Result<()> {
    sqlx::query!(
        r#"update o365_folders set next_link = $3, updated_at = now()
           where mail_account_id = $1 and folder_id = $2"#,
        mail_account_id,
        folder_id,
        next_link,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Adopt a delta link: the folder is fully walked (or its incremental
/// changes fully applied). Clears the in-progress cursor. The
/// pending-resync flag is left alone: it is cleared by
/// [`clear_resync_pending`] only after the post-walk diff has run.
pub async fn set_delta_link(
    pool: &PgPool,
    mail_account_id: i64,
    folder_id: &str,
    delta_link: &str,
) -> Result<()> {
    sqlx::query!(
        r#"update o365_folders
           set delta_link = $3, next_link = null, updated_at = now()
           where mail_account_id = $1 and folder_id = $2"#,
        mail_account_id,
        folder_id,
        delta_link,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop both cursors so the folder is re-walked from scratch; with
/// `resync_pending` set, the walk is followed by a diff against the rows
/// still placed in the folder.
pub async fn mark_resync(
    pool: &PgPool,
    mail_account_id: i64,
    folder_id: &str,
    resync_pending: bool,
) -> Result<()> {
    sqlx::query!(
        r#"update o365_folders
           set delta_link = null, next_link = null, resync_pending = $3, updated_at = now()
           where mail_account_id = $1 and folder_id = $2"#,
        mail_account_id,
        folder_id,
        resync_pending,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn clear_resync_pending(
    pool: &PgPool,
    mail_account_id: i64,
    folder_id: &str,
) -> Result<()> {
    sqlx::query!(
        r#"update o365_folders set resync_pending = false, updated_at = now()
           where mail_account_id = $1 and folder_id = $2"#,
        mail_account_id,
        folder_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop every folder's cursors (a backfill with a wider date floor).
pub async fn clear_all_cursors(pool: &PgPool, mail_account_id: i64) -> Result<()> {
    sqlx::query!(
        r#"update o365_folders
           set delta_link = null, next_link = null, resync_pending = false, updated_at = now()
           where mail_account_id = $1"#,
        mail_account_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete(pool: &PgPool, mail_account_id: i64, folder_id: &str) -> Result<()> {
    sqlx::query!(
        "delete from o365_folders where mail_account_id = $1 and folder_id = $2",
        mail_account_id,
        folder_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Remote ids of live ledger rows placed in exactly this folder path.
pub async fn message_ids_in_path(
    pool: &PgPool,
    mail_account_id: i64,
    display_path: &str,
) -> Result<Vec<String>> {
    let placement = serde_json::json!([display_path]);
    let rows = sqlx::query!(
        r#"select jmap_email_id from messages
           where mail_account_id = $1 and mailbox_ids = $2 and deleted_at is null"#,
        mail_account_id,
        placement,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.jmap_email_id).collect())
}

/// Folder rename or re-parent: rewrite every row's single-element
/// placement from the old path to the new one. Returns rows touched.
pub async fn rename_path(
    pool: &PgPool,
    mail_account_id: i64,
    old_path: &str,
    new_path: &str,
) -> Result<u64> {
    let old = serde_json::json!([old_path]);
    let new = serde_json::json!([new_path]);
    let result = sqlx::query!(
        r#"update messages set mailbox_ids = $3
           where mail_account_id = $1 and mailbox_ids = $2"#,
        mail_account_id,
        old,
        new,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
