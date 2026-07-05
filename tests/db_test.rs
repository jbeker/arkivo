use arkivo::db::{self, accounts, audit, jobs, messages, tokens, users};
use chrono::{Duration, Utc};
use sqlx::PgPool;

fn meta(id: &str, received_days_ago: i64) -> messages::MessageMeta {
    messages::MessageMeta {
        jmap_email_id: id.to_string(),
        blob_id: format!("blob-{id}"),
        message_id_hdr: Some(format!("<{id}@example.com>")),
        thread_id: Some("T1".into()),
        received_at: Utc::now() - Duration::days(received_days_ago),
        size: 1024,
        has_attachments: false,
        from_addr: Some("alice@example.com".into()),
        subject: Some("hello".into()),
        mailbox_ids: serde_json::json!(["inbox"]),
        keywords: serde_json::json!({}),
    }
}

async fn account_fixture(pool: &PgPool) -> accounts::MailAccount {
    let user = users::create(pool, "alice", "user").await.unwrap();
    accounts::create(
        pool,
        user.id,
        "https://api.fastmail.com/jmap/session",
        b"sealed",
        "k1",
    )
    .await
    .unwrap()
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn user_roundtrip_and_disable(pool: PgPool) {
    let user = users::create(&pool, "alice", "admin").await.unwrap();
    assert!(user.is_admin());
    assert!(user.is_active());

    users::set_disabled(&pool, user.id, true).await.unwrap();
    let user = users::get_by_handle(&pool, "alice").await.unwrap().unwrap();
    assert!(!user.is_active());
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn account_create_seeds_jmap_state(pool: PgPool) {
    let account = account_fixture(&pool).await;

    let state = accounts::get_jmap_state(&pool, account.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.email_state, None);
    assert!(!state.backfill_done);

    accounts::set_email_state(&pool, account.id, "s42")
        .await
        .unwrap();
    let state = accounts::get_jmap_state(&pool, account.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.email_state.as_deref(), Some("s42"));

    assert_eq!(account.recency_cutoff_days, 7);
    assert_eq!(account.deletion_policy, "retain");
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn upsert_meta_is_idempotent_and_preserves_local_state(pool: PgPool) {
    let account = account_fixture(&pool).await;

    let id1 = messages::upsert_meta(&pool, account.id, &meta("m1", 30))
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    messages::set_stored(&mut conn, id1, "new/m1.eml")
        .await
        .unwrap();
    drop(conn);

    // Re-fetch with changed mailboxes: same row, refreshed keywords,
    // maildir_path untouched.
    let mut updated = meta("m1", 30);
    updated.mailbox_ids = serde_json::json!(["archive"]);
    let id2 = messages::upsert_meta(&pool, account.id, &updated)
        .await
        .unwrap();
    assert_eq!(id1, id2);

    let msg = messages::get(&pool, id1).await.unwrap().unwrap();
    assert_eq!(msg.mailbox_ids, serde_json::json!(["archive"]));
    assert_eq!(msg.maildir_path.as_deref(), Some("new/m1.eml"));
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn unfetched_lists_only_missing_blobs(pool: PgPool) {
    let account = account_fixture(&pool).await;

    let id1 = messages::upsert_meta(&pool, account.id, &meta("m1", 30))
        .await
        .unwrap();
    messages::upsert_meta(&pool, account.id, &meta("m2", 30))
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    messages::set_stored(&mut conn, id1, "new/m1.eml")
        .await
        .unwrap();
    drop(conn);

    let missing = messages::unfetched(&pool, account.id, 100).await.unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].jmap_email_id, "m2");
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn promotable_respects_cutoff_storage_and_status(pool: PgPool) {
    let account = account_fixture(&pool).await;
    let cutoff = Utc::now() - Duration::days(7);

    // Old and stored: promotable.
    let old_stored = messages::upsert_meta(&pool, account.id, &meta("old", 30))
        .await
        .unwrap();
    // Old but blob not yet fetched: not promotable.
    messages::upsert_meta(&pool, account.id, &meta("old-unfetched", 30))
        .await
        .unwrap();
    // Too recent: not promotable.
    let recent = messages::upsert_meta(&pool, account.id, &meta("recent", 1))
        .await
        .unwrap();
    // Old, stored, but already indexed: not promotable.
    let done = messages::upsert_meta(&pool, account.id, &meta("done", 30))
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    messages::set_stored(&mut conn, old_stored, "new/old.eml")
        .await
        .unwrap();
    messages::set_stored(&mut conn, recent, "new/recent.eml")
        .await
        .unwrap();
    messages::set_stored(&mut conn, done, "new/done.eml")
        .await
        .unwrap();
    drop(conn);
    messages::mark_indexed(&pool, done, 1).await.unwrap();

    let promotable = messages::promotable(&pool, account.id, cutoff, 100)
        .await
        .unwrap();
    assert_eq!(promotable.len(), 1);
    assert_eq!(promotable[0].id, old_stored);
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn reset_index_state_restages_indexed_messages(pool: PgPool) {
    let account = account_fixture(&pool).await;
    let id = messages::upsert_meta(&pool, account.id, &meta("m1", 30))
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    messages::set_stored(&mut conn, id, "new/m1.eml")
        .await
        .unwrap();
    drop(conn);
    messages::mark_indexed(&pool, id, 1).await.unwrap();

    let restaged = messages::reset_index_state(&pool, account.id)
        .await
        .unwrap();
    assert_eq!(restaged, 1);

    let cutoff = Utc::now() - Duration::days(7);
    assert_eq!(
        messages::promotable(&pool, account.id, cutoff, 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn duplicate_detection_by_message_id_header(pool: PgPool) {
    let account = account_fixture(&pool).await;

    let mut copy1 = meta("m1", 30);
    let mut copy2 = meta("m2", 30);
    copy1.message_id_hdr = Some("<same@example.com>".into());
    copy2.message_id_hdr = Some("<same@example.com>".into());

    let id1 = messages::upsert_meta(&pool, account.id, &copy1)
        .await
        .unwrap();
    let id2 = messages::upsert_meta(&pool, account.id, &copy2)
        .await
        .unwrap();
    messages::mark_indexed(&pool, id1, 1).await.unwrap();

    assert!(
        messages::is_duplicate_indexed(&pool, account.id, "<same@example.com>", id2)
            .await
            .unwrap()
    );
    assert!(
        !messages::is_duplicate_indexed(&pool, account.id, "<same@example.com>", id1)
            .await
            .unwrap()
    );
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn token_lifecycle(pool: PgPool) {
    let user = users::create(&pool, "alice", "user").await.unwrap();
    let generated = arkivo::crypto::generate_token("mcp");

    let token = tokens::mint(&pool, user.id, &generated.hash, "laptop")
        .await
        .unwrap();

    // Resolves while active.
    let resolved = tokens::resolve_active(&pool, &generated.hash)
        .await
        .unwrap();
    assert_eq!(resolved.unwrap().id, token.id);

    // Wrong hash resolves nothing.
    let other = arkivo::crypto::generate_token("mcp");
    assert!(
        tokens::resolve_active(&pool, &other.hash)
            .await
            .unwrap()
            .is_none()
    );

    // Revoked token stops resolving.
    assert!(tokens::revoke(&pool, user.id, token.id).await.unwrap());
    assert!(
        tokens::resolve_active(&pool, &generated.hash)
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn disabled_user_token_does_not_resolve(pool: PgPool) {
    let user = users::create(&pool, "alice", "user").await.unwrap();
    let generated = arkivo::crypto::generate_token("mcp");
    tokens::mint(&pool, user.id, &generated.hash, "laptop")
        .await
        .unwrap();

    users::set_disabled(&pool, user.id, true).await.unwrap();
    assert!(
        tokens::resolve_active(&pool, &generated.hash)
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn job_lifecycle_and_counts(pool: PgPool) {
    let account = account_fixture(&pool).await;

    let job_id = jobs::start(&pool, "poll", Some(account.id)).await.unwrap();
    jobs::succeed(&pool, job_id, &serde_json::json!({"fetched": 3}))
        .await
        .unwrap();

    let failed_id = jobs::start(&pool, "poll", Some(account.id)).await.unwrap();
    jobs::fail(&pool, failed_id, "boom").await.unwrap();

    let recent = jobs::recent(&pool, account.id, 10).await.unwrap();
    assert_eq!(recent.len(), 2);

    messages::upsert_meta(&pool, account.id, &meta("m1", 30))
        .await
        .unwrap();
    let counts = messages::counts(&pool, account.id).await.unwrap();
    assert_eq!(counts.total, 1);
    assert_eq!(counts.staged, 1);
    assert_eq!(counts.unfetched, 1);
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn audit_log_roundtrip(pool: PgPool) {
    let user = users::create(&pool, "alice", "user").await.unwrap();
    audit::record(
        &pool,
        Some(user.id),
        "mcp:1",
        "get_message",
        Some("42"),
        None,
    )
    .await
    .unwrap();

    let entries = audit::recent_for_user(&pool, user.id, 10).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].action, "get_message");
    assert_eq!(entries[0].actor, "mcp:1");
}
