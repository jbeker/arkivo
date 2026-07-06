//! Promotion pipeline tests: FakeClock drives the recency boundary;
//! OpenSearch parts are env-gated (ARKIVO_TEST_OPENSEARCH_URL) like
//! search_test.rs.

use arkivo::clock::FakeClock;
use arkivo::config::OpenSearchConfig;
use arkivo::db::{accounts, messages, users};
use arkivo::embed::FakeEmbedder;
use arkivo::maildir::{Maildir, MessageStore};
use arkivo::promote::promote_account;
use arkivo::search::SearchClient;
use arkivo::search::client::SearchFilter;
use chrono::{Duration, TimeZone, Utc};
use sqlx::PgPool;

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap()
}

fn search_client() -> Option<SearchClient> {
    let url = std::env::var("ARKIVO_TEST_OPENSEARCH_URL").ok()?;
    Some(
        SearchClient::new(&OpenSearchConfig {
            url,
            username: None,
            password: None,
        })
        .unwrap(),
    )
}

macro_rules! require_opensearch {
    () => {
        match search_client() {
            Some(c) => c,
            None => {
                eprintln!("skipping: ARKIVO_TEST_OPENSEARCH_URL not set");
                return;
            }
        }
    };
}

struct Fixture {
    account: accounts::MailAccount,
    maildir: Maildir,
    _dir: tempfile::TempDir,
}

/// Unique user id per test so OpenSearch indices don't collide across
/// parallel tests: each sqlx::test gets a fresh Postgres DB whose
/// identity column would otherwise hand out user_id=1 to every test,
/// and index names derive from user_id.
async fn fixture(pool: &PgPool, sanitize_policy: Option<serde_json::Value>) -> Fixture {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let unique_id = 800_000_000
        + (std::process::id() as i64 % 90_000) * 1_000
        + NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "alter table users alter column id restart with {unique_id}"
    )))
    .execute(pool)
    .await
    .unwrap();
    let user = users::create(pool, "owner", "user").await.unwrap();
    let account = accounts::create(
        pool,
        user.id,
        "https://unused.example/jmap",
        b"x",
        "primary",
    )
    .await
    .unwrap();
    if let Some(policy) = sanitize_policy {
        sqlx::query("update mail_accounts set sanitize_policy = $2 where id = $1")
            .bind(account.id)
            .bind(policy)
            .execute(pool)
            .await
            .unwrap();
    }
    let account = accounts::get(pool, account.id).await.unwrap().unwrap();
    let dir = tempfile::tempdir().unwrap();
    Fixture {
        account,
        maildir: Maildir::open_or_create(dir.path()).unwrap(),
        _dir: dir,
    }
}

/// Stage a message: raw blob in the Maildir + ledger row marked stored.
async fn stage_message(
    pool: &PgPool,
    fx: &Fixture,
    jmap_id: &str,
    from: &str,
    subject: &str,
    body: &str,
    age_days: i64,
) -> i64 {
    let raw = format!(
        "From: {from}\r\nTo: owner@example.com\r\nSubject: {subject}\r\n\
         Message-ID: <{jmap_id}@example.com>\r\nContent-Type: text/plain\r\n\r\n{body}\r\n"
    );
    let meta = messages::MessageMeta {
        jmap_email_id: jmap_id.to_string(),
        blob_id: format!("blob-{jmap_id}"),
        message_id_hdr: Some(format!("<{jmap_id}@example.com>")),
        thread_id: None,
        received_at: now() - Duration::days(age_days),
        size: raw.len() as i64,
        has_attachments: false,
        from_addr: Some(from.to_string()),
        subject: Some(subject.to_string()),
        mailbox_ids: serde_json::json!(["inbox"]),
        keywords: serde_json::json!({}),
    };
    let id = messages::upsert_meta(pool, fx.account.id, &meta)
        .await
        .unwrap();
    let rel = fx.maildir.write(jmap_id, raw.as_bytes()).unwrap();
    let mut conn = pool.acquire().await.unwrap();
    messages::set_stored(&mut conn, id, &rel).await.unwrap();
    id
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn cutoff_boundary_is_exact(pool: PgPool) {
    let search = require_opensearch!();
    let fx = fixture(&pool, None).await;
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(now());

    // Account default cutoff is 7 days. One message 1 second younger
    // than the boundary, one 1 second older.
    let too_recent = stage_message(&pool, &fx, "recent", "a@x.com", "recent", "body", 0).await;
    sqlx::query("update messages set received_at = $2 where id = $1")
        .bind(too_recent)
        .bind(now() - Duration::days(7) + Duration::seconds(1))
        .execute(&pool)
        .await
        .unwrap();
    let old_enough = stage_message(&pool, &fx, "old", "a@x.com", "old", "body", 0).await;
    sqlx::query("update messages set received_at = $2 where id = $1")
        .bind(old_enough)
        .bind(now() - Duration::days(7) - Duration::seconds(1))
        .execute(&pool)
        .await
        .unwrap();

    let stats = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.promoted, 1, "only the message past the cutoff");

    let recent_row = messages::get(&pool, too_recent).await.unwrap().unwrap();
    assert_eq!(recent_row.index_status, "staged");
    let old_row = messages::get(&pool, old_enough).await.unwrap().unwrap();
    assert_eq!(old_row.index_status, "indexed");

    // Advance the clock past the boundary: the recent one promotes.
    clock.advance(Duration::seconds(2));
    let stats = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.promoted, 1);

    search
        .delete_user_indices(fx.account.user_id)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn promotion_is_idempotent(pool: PgPool) {
    let search = require_opensearch!();
    let fx = fixture(&pool, None).await;
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(now());

    stage_message(
        &pool,
        &fx,
        "m1",
        "a@x.com",
        "subject",
        "old message body",
        30,
    )
    .await;

    let first = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.promoted, 1);

    let second = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(second.promoted, 0, "second run must be a no-op");

    search
        .delete_user_indices(fx.account.user_id)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn reset_links_are_redacted_in_index(pool: PgPool) {
    let search = require_opensearch!();
    let fx = fixture(&pool, None).await;
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(now());

    stage_message(
        &pool,
        &fx,
        "reset",
        "noreply@service.example",
        "Password reset",
        "Reset here: https://service.example/reset?token=SECRETTOKEN and your code is 918273.",
        30,
    )
    .await;

    promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    search
        .refresh_user_indices(fx.account.user_id)
        .await
        .unwrap();

    let hits = search
        .bm25_search(
            fx.account.user_id,
            "password reset",
            10,
            &SearchFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let body = hits[0].source.get("body_text").unwrap().as_str().unwrap();
    assert!(
        !body.contains("SECRETTOKEN"),
        "reset token must be redacted"
    );
    assert!(!body.contains("918273"), "OTP must be redacted");
    assert_eq!(
        hits[0].source.get("sanitized"),
        Some(&serde_json::json!(true))
    );

    search
        .delete_user_indices(fx.account.user_id)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn quarantined_sender_never_reaches_index(pool: PgPool) {
    let search = require_opensearch!();
    let fx = fixture(
        &pool,
        Some(serde_json::json!({"quarantine_senders": ["@bank.example"]})),
    )
    .await;
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(now());

    let quarantined = stage_message(
        &pool,
        &fx,
        "q1",
        "alerts@bank.example",
        "Statement",
        "balance info",
        30,
    )
    .await;
    stage_message(
        &pool,
        &fx,
        "ok1",
        "friend@gmail.com",
        "Dinner",
        "see you at 7",
        30,
    )
    .await;

    let stats = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.promoted, 1);
    assert_eq!(stats.quarantined, 1);

    let row = messages::get(&pool, quarantined).await.unwrap().unwrap();
    assert_eq!(row.index_status, "quarantined");

    search
        .refresh_user_indices(fx.account.user_id)
        .await
        .unwrap();
    let hits = search
        .bm25_search(fx.account.user_id, "balance", 10, &SearchFilter::default())
        .await
        .unwrap();
    assert!(
        hits.is_empty(),
        "quarantined content must not be searchable"
    );

    search
        .delete_user_indices(fx.account.user_id)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn duplicate_message_id_is_not_reembedded(pool: PgPool) {
    let search = require_opensearch!();
    let fx = fixture(&pool, None).await;
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(now());

    // Same Message-ID filed in two folders = two JMAP ids, one header.
    let raw_body = "identical content in both copies";
    let first = stage_message(&pool, &fx, "copy1", "a@x.com", "dup", raw_body, 30).await;
    let second = stage_message(&pool, &fx, "copy2", "a@x.com", "dup", raw_body, 30).await;
    sqlx::query("update messages set message_id_hdr = '<same@example.com>' where id in ($1, $2)")
        .bind(first)
        .bind(second)
        .execute(&pool)
        .await
        .unwrap();

    let stats = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.promoted, 1);
    assert_eq!(stats.deduped, 1);

    search
        .refresh_user_indices(fx.account.user_id)
        .await
        .unwrap();
    let hits = search
        .bm25_search(
            fx.account.user_id,
            "identical content",
            10,
            &SearchFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "only one copy indexed");

    search
        .delete_user_indices(fx.account.user_id)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn unreadable_blob_marks_failed_and_continues(pool: PgPool) {
    let search = require_opensearch!();
    let fx = fixture(&pool, None).await;
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(now());

    let broken = stage_message(&pool, &fx, "broken", "a@x.com", "broken", "body", 30).await;
    stage_message(&pool, &fx, "fine", "a@x.com", "fine", "body", 30).await;
    // Remove the file behind the first message's back.
    let path = messages::get(&pool, broken)
        .await
        .unwrap()
        .unwrap()
        .maildir_path
        .unwrap();
    fx.maildir.remove(&path).unwrap();

    let stats = promote_account(
        &pool,
        &fx.maildir,
        &search,
        &embedder,
        &clock,
        &fx.account,
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.promoted, 1, "healthy message still promotes");

    let row = messages::get(&pool, broken).await.unwrap().unwrap();
    assert_eq!(row.index_status, "failed");
    assert!(row.error.is_some());

    search
        .delete_user_indices(fx.account.user_id)
        .await
        .unwrap();
}
