use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

/// Index-relevant metadata fetched from `Email/get`. The raw RFC822 blob
/// lands in the Maildir separately; this is what Postgres tracks.
#[derive(Debug, Clone)]
pub struct MessageMeta {
    pub jmap_email_id: String,
    pub blob_id: String,
    pub message_id_hdr: Option<String>,
    pub thread_id: Option<String>,
    pub received_at: DateTime<Utc>,
    pub size: i64,
    pub has_attachments: bool,
    pub from_addr: Option<String>,
    pub subject: Option<String>,
    pub mailbox_ids: serde_json::Value,
    pub keywords: serde_json::Value,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Message {
    pub id: i64,
    pub mail_account_id: i64,
    pub jmap_email_id: String,
    pub blob_id: String,
    pub message_id_hdr: Option<String>,
    pub thread_id: Option<String>,
    pub received_at: DateTime<Utc>,
    pub size: i64,
    pub has_attachments: bool,
    pub from_addr: Option<String>,
    pub subject: Option<String>,
    pub mailbox_ids: serde_json::Value,
    pub keywords: serde_json::Value,
    pub maildir_path: Option<String>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub indexed_at: Option<DateTime<Utc>>,
    pub index_status: String,
    pub pipeline_version: Option<i32>,
    pub error: Option<String>,
    pub deleted_at: Option<DateTime<Utc>>,
}

// Column list for query_as!(Message, ...) selects. sqlx 0.9 requires
// literal SQL, so the list is repeated at each call site; keep them in
// sync with the struct above.

/// Insert or refresh metadata for a message. On conflict (message already
/// known) only the mutable JMAP properties are refreshed; local pipeline
/// state (maildir_path, indexed_at, ...) is preserved.
pub async fn upsert_meta(pool: &PgPool, mail_account_id: i64, meta: &MessageMeta) -> Result<i64> {
    let rec = sqlx::query!(
        r#"insert into messages
               (mail_account_id, jmap_email_id, blob_id, message_id_hdr, thread_id,
                received_at, size, has_attachments, from_addr, subject,
                mailbox_ids, keywords)
           values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           on conflict (mail_account_id, jmap_email_id) do update
               set blob_id = excluded.blob_id,
                   mailbox_ids = excluded.mailbox_ids,
                   keywords = excluded.keywords
           returning id"#,
        mail_account_id,
        meta.jmap_email_id,
        meta.blob_id,
        meta.message_id_hdr,
        meta.thread_id,
        meta.received_at,
        meta.size,
        meta.has_attachments,
        meta.from_addr,
        meta.subject,
        meta.mailbox_ids,
        meta.keywords,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.id)
}

/// Record that the raw blob is durably in the Maildir. Runs inside the
/// caller's transaction so the ledger and the fsync'd file commit together.
pub async fn set_stored(tx: &mut sqlx::PgConnection, id: i64, maildir_path: &str) -> Result<()> {
    sqlx::query!(
        "update messages set maildir_path = $2, fetched_at = now() where id = $1",
        id,
        maildir_path,
    )
    .execute(tx)
    .await?;
    Ok(())
}

/// Messages whose metadata is known but whose blob has not been durably
/// stored — the refetch list after a crash or partial backfill.
pub async fn unfetched(pool: &PgPool, mail_account_id: i64, limit: i64) -> Result<Vec<Message>> {
    let msgs = sqlx::query_as!(
        Message,
        r#"select id, mail_account_id, jmap_email_id, blob_id, message_id_hdr, thread_id,
                  received_at, size, has_attachments, from_addr, subject, mailbox_ids,
                  keywords, maildir_path, fetched_at, indexed_at, index_status,
                  pipeline_version, error, deleted_at
           from messages
           where mail_account_id = $1 and maildir_path is null and deleted_at is null
           order by id limit $2"#,
        mail_account_id,
        limit,
    )
    .fetch_all(pool)
    .await?;
    Ok(msgs)
}

/// Stored-but-unindexed messages older than `cutoff`, oldest first —
/// the promotion scan (hits idx_messages_promotable).
pub async fn promotable(
    pool: &PgPool,
    mail_account_id: i64,
    cutoff: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<Message>> {
    let msgs = sqlx::query_as!(
        Message,
        r#"select id, mail_account_id, jmap_email_id, blob_id, message_id_hdr, thread_id,
                  received_at, size, has_attachments, from_addr, subject, mailbox_ids,
                  keywords, maildir_path, fetched_at, indexed_at, index_status,
                  pipeline_version, error, deleted_at
           from messages
           where mail_account_id = $1
             and received_at < $2
             and indexed_at is null
             and deleted_at is null
             and maildir_path is not null
             and index_status = 'staged'
           order by received_at limit $3"#,
        mail_account_id,
        cutoff,
        limit,
    )
    .fetch_all(pool)
    .await?;
    Ok(msgs)
}

pub async fn get(pool: &PgPool, id: i64) -> Result<Option<Message>> {
    let msg = sqlx::query_as!(
        Message,
        r#"select id, mail_account_id, jmap_email_id, blob_id, message_id_hdr, thread_id,
                  received_at, size, has_attachments, from_addr, subject, mailbox_ids,
                  keywords, maildir_path, fetched_at, indexed_at, index_status,
                  pipeline_version, error, deleted_at
           from messages where id = $1"#,
        id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(msg)
}

pub async fn get_by_jmap_id(
    pool: &PgPool,
    mail_account_id: i64,
    jmap_email_id: &str,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as!(
        Message,
        r#"select id, mail_account_id, jmap_email_id, blob_id, message_id_hdr, thread_id,
                  received_at, size, has_attachments, from_addr, subject, mailbox_ids,
                  keywords, maildir_path, fetched_at, indexed_at, index_status,
                  pipeline_version, error, deleted_at
           from messages where mail_account_id = $1 and jmap_email_id = $2"#,
        mail_account_id,
        jmap_email_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(msg)
}

/// True if another message with the same Message-ID header is already
/// indexed for this account (same message filed into a second folder).
pub async fn is_duplicate_indexed(
    pool: &PgPool,
    mail_account_id: i64,
    message_id_hdr: &str,
    excluding_id: i64,
) -> Result<bool> {
    let rec = sqlx::query!(
        r#"select exists(
               select 1 from messages
               where mail_account_id = $1 and message_id_hdr = $2
                 and id <> $3 and index_status = 'indexed'
           ) as "exists!""#,
        mail_account_id,
        message_id_hdr,
        excluding_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.exists)
}

pub async fn mark_indexed(pool: &PgPool, id: i64, pipeline_version: i32) -> Result<()> {
    sqlx::query!(
        r#"update messages
           set indexed_at = now(), index_status = 'indexed',
               pipeline_version = $2, error = null
           where id = $1"#,
        id,
        pipeline_version,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_status(pool: &PgPool, id: i64, status: &str, error: Option<&str>) -> Result<()> {
    sqlx::query!(
        "update messages set index_status = $2, error = $3 where id = $1",
        id,
        status,
        error,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Reset pipeline state for every message in the account so `reindex`
/// re-promotes from the canonical store.
pub async fn reset_index_state(pool: &PgPool, mail_account_id: i64) -> Result<u64> {
    let result = sqlx::query!(
        r#"update messages
           set indexed_at = null, index_status = 'staged', pipeline_version = null, error = null
           where mail_account_id = $1 and deleted_at is null and maildir_path is not null"#,
        mail_account_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Remove a message row entirely (deletion policy `mirror`).
pub async fn delete_row(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query!("delete from messages where id = $1", id)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct AccountCounts {
    pub total: i64,
    pub staged: i64,
    pub indexed: i64,
    pub quarantined: i64,
    pub failed: i64,
    pub unfetched: i64,
}

pub async fn counts(pool: &PgPool, mail_account_id: i64) -> Result<AccountCounts> {
    let rec = sqlx::query!(
        r#"select
               count(*) as "total!",
               count(*) filter (where index_status = 'staged') as "staged!",
               count(*) filter (where index_status = 'indexed') as "indexed!",
               count(*) filter (where index_status = 'quarantined') as "quarantined!",
               count(*) filter (where index_status = 'failed') as "failed!",
               count(*) filter (where maildir_path is null) as "unfetched!"
           from messages
           where mail_account_id = $1 and deleted_at is null"#,
        mail_account_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(AccountCounts {
        total: rec.total,
        staged: rec.staged,
        indexed: rec.indexed,
        quarantined: rec.quarantined,
        failed: rec.failed,
        unfetched: rec.unfetched,
    })
}
