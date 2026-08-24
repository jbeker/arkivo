//! IMAP placement map: which (folder, uid) currently holds which ledger
//! row. IMAP UIDs are per-folder and a move re-numbers the message, so
//! placement is kept out of `messages` entirely — a row's `mailbox_ids`
//! and `keywords` are recomputed from this table, and the row is only
//! destroyed when its last placement vanishes.

use std::collections::HashMap;

use anyhow::Result;
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ImapFolder {
    pub folder: String,
    pub uidvalidity: i64,
    pub last_seen_uidnext: i64,
}

pub async fn get_folder(
    pool: &PgPool,
    mail_account_id: i64,
    folder: &str,
) -> Result<Option<ImapFolder>> {
    let rec = sqlx::query_as!(
        ImapFolder,
        r#"select folder, uidvalidity, last_seen_uidnext
           from imap_folders where mail_account_id = $1 and folder = $2"#,
        mail_account_id,
        folder,
    )
    .fetch_optional(pool)
    .await?;
    Ok(rec)
}

pub async fn upsert_folder(
    pool: &PgPool,
    mail_account_id: i64,
    folder: &str,
    uidvalidity: i64,
    last_seen_uidnext: i64,
) -> Result<()> {
    sqlx::query!(
        r#"insert into imap_folders (mail_account_id, folder, uidvalidity, last_seen_uidnext)
           values ($1, $2, $3, $4)
           on conflict (mail_account_id, folder) do update
               set uidvalidity = excluded.uidvalidity,
                   last_seen_uidnext = excluded.last_seen_uidnext,
                   updated_at = now()"#,
        mail_account_id,
        folder,
        uidvalidity,
        last_seen_uidnext,
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct KnownPlacement {
    pub message_id: i64,
    pub flags: serde_json::Value,
}

/// Every placement recorded for a folder: uid → (messages.id, last flags).
pub async fn known_uids(
    pool: &PgPool,
    mail_account_id: i64,
    folder: &str,
) -> Result<HashMap<i64, KnownPlacement>> {
    let rows = sqlx::query!(
        r#"select uid, message_id, flags from imap_uid_map
           where mail_account_id = $1 and folder = $2"#,
        mail_account_id,
        folder,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.uid,
                KnownPlacement {
                    message_id: r.message_id,
                    flags: r.flags,
                },
            )
        })
        .collect())
}

pub async fn add_placement(
    pool: &PgPool,
    mail_account_id: i64,
    folder: &str,
    uidvalidity: i64,
    uid: i64,
    message_id: i64,
    flags: &serde_json::Value,
) -> Result<()> {
    sqlx::query!(
        r#"insert into imap_uid_map
               (mail_account_id, folder, uidvalidity, uid, message_id, flags)
           values ($1, $2, $3, $4, $5, $6)
           on conflict (mail_account_id, folder, uid) do update
               set uidvalidity = excluded.uidvalidity,
                   message_id = excluded.message_id,
                   flags = excluded.flags"#,
        mail_account_id,
        folder,
        uidvalidity,
        uid,
        message_id,
        flags,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_placement_flags(
    pool: &PgPool,
    mail_account_id: i64,
    folder: &str,
    uid: i64,
    flags: &serde_json::Value,
) -> Result<()> {
    sqlx::query!(
        r#"update imap_uid_map set flags = $4
           where mail_account_id = $1 and folder = $2 and uid = $3"#,
        mail_account_id,
        folder,
        uid,
        flags,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop one placement, returning the messages.id it pointed at (None if
/// the placement was already gone).
pub async fn remove_placement(
    pool: &PgPool,
    mail_account_id: i64,
    folder: &str,
    uid: i64,
) -> Result<Option<i64>> {
    let rec = sqlx::query!(
        r#"delete from imap_uid_map
           where mail_account_id = $1 and folder = $2 and uid = $3
           returning message_id"#,
        mail_account_id,
        folder,
        uid,
    )
    .fetch_optional(pool)
    .await?;
    Ok(rec.map(|r| r.message_id))
}

/// Drop every placement in a folder (UIDVALIDITY resync), returning the
/// affected messages.ids for the caller's orphan sweep.
pub async fn clear_folder(pool: &PgPool, mail_account_id: i64, folder: &str) -> Result<Vec<i64>> {
    let rows = sqlx::query!(
        r#"delete from imap_uid_map
           where mail_account_id = $1 and folder = $2
           returning message_id"#,
        mail_account_id,
        folder,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.message_id).collect())
}

#[derive(Debug, Clone)]
pub struct Placement {
    pub folder: String,
    pub uid: i64,
    pub flags: serde_json::Value,
}

pub async fn placements_for_message(pool: &PgPool, message_id: i64) -> Result<Vec<Placement>> {
    let rows = sqlx::query_as!(
        Placement,
        r#"select folder, uid, flags from imap_uid_map
           where message_id = $1 order by folder, uid"#,
        message_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Stored, non-deleted ledger rows matching a Message-ID header — the
/// re-home candidates for a UID that appeared in a new folder. The caller
/// only re-homes on an unambiguous single match.
pub async fn find_by_message_id_hdr(
    pool: &PgPool,
    mail_account_id: i64,
    message_id_hdr: &str,
) -> Result<Vec<i64>> {
    let rows = sqlx::query!(
        r#"select id from messages
           where mail_account_id = $1 and message_id_hdr = $2
             and deleted_at is null and maildir_path is not null"#,
        mail_account_id,
        message_id_hdr,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.id).collect())
}

/// Number of placements recorded for a folder — compared against EXISTS
/// to prove a folder unchanged without loading the full map.
pub async fn count_uids(pool: &PgPool, mail_account_id: i64, folder: &str) -> Result<i64> {
    let rec = sqlx::query!(
        r#"select count(*) as "count!" from imap_uid_map
           where mail_account_id = $1 and folder = $2"#,
        mail_account_id,
        folder,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.count)
}
