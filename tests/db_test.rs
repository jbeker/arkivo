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

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn disabled_account_is_skipped_by_scheduled_resolution(pool: PgPool) {
    let user = users::create(&pool, "alice", "user").await.unwrap();
    let active = accounts::create(&pool, user.id, "https://a.example/jmap", b"s", "k1")
        .await
        .unwrap();
    let paused = accounts::create(&pool, user.id, "https://b.example/jmap", b"s", "k1")
        .await
        .unwrap();
    accounts::set_disabled(&pool, paused.id, true)
        .await
        .unwrap();

    // Scheduled poll (--all, active_only) skips the paused account.
    let polled = arkivo::cmd::resolve_account_ids(&pool, None, true, true)
        .await
        .unwrap();
    assert_eq!(polled, vec![active.id]);

    // Scheduled promote (--all, not active_only) still includes it.
    let promoted = arkivo::cmd::resolve_account_ids(&pool, None, true, false)
        .await
        .unwrap();
    assert_eq!(promoted, vec![active.id, paused.id]);

    // Explicit --account always resolves: manual invocation is intentional.
    let explicit = arkivo::cmd::resolve_account_ids(&pool, Some(paused.id), false, true)
        .await
        .unwrap();
    assert_eq!(explicit, vec![paused.id]);

    // Re-enable restores scheduled polling.
    accounts::set_disabled(&pool, paused.id, false)
        .await
        .unwrap();
    let polled = arkivo::cmd::resolve_account_ids(&pool, None, true, true)
        .await
        .unwrap();
    assert_eq!(polled, vec![active.id, paused.id]);
    let account = accounts::get(&pool, paused.id).await.unwrap().unwrap();
    assert!(account.disabled_at.is_none());
}

/// Seed one account with the full spread of problem states: two failed
/// messages whose errors differ only by an embedded id (must group as one
/// cause), one failed with a distinct error, one quarantined, one failed
/// with no blob (not retryable), and one healthy indexed message.
async fn problem_fixture(pool: &PgPool) -> (accounts::MailAccount, Vec<i64>) {
    let account = account_fixture(pool).await;
    let mut ids = Vec::new();
    for (name, days) in [
        ("p1", 40),
        ("p2", 39),
        ("p3", 38),
        ("p4", 37),
        ("p5", 36),
        ("ok", 35),
    ] {
        ids.push(
            messages::upsert_meta(pool, account.id, &meta(name, days))
                .await
                .unwrap(),
        );
    }
    let mut conn = pool.acquire().await.unwrap();
    for (i, id) in ids.iter().enumerate() {
        if i != 4 {
            // p5 stays blob-less: failed but not retryable.
            messages::set_stored(&mut conn, *id, &format!("new/m{i}.eml"))
                .await
                .unwrap();
        }
    }
    drop(conn);

    messages::mark_status(
        pool,
        ids[0],
        "failed",
        Some("OpenSearch /msg/_doc/123 returned 429"),
    )
    .await
    .unwrap();
    messages::mark_status(
        pool,
        ids[1],
        "failed",
        Some("OpenSearch /msg/_doc/456 returned 429"),
    )
    .await
    .unwrap();
    messages::mark_status(pool, ids[2], "failed", Some("embedding request failed"))
        .await
        .unwrap();
    messages::mark_status(pool, ids[3], "quarantined", Some("sanitize: blocked html"))
        .await
        .unwrap();
    messages::mark_status(pool, ids[4], "failed", Some("embedding request failed"))
        .await
        .unwrap();
    messages::mark_indexed(pool, ids[5], 1).await.unwrap();
    (account, ids)
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn problem_groups_collapse_by_normalized_error(pool: PgPool) {
    let (account, _) = problem_fixture(&pool).await;

    let groups = messages::problem_groups(&pool, account.id).await.unwrap();
    assert_eq!(groups.len(), 3);

    // Order among equal-size groups is unspecified; find by content.
    let opensearch = groups
        .iter()
        .find(|g| g.error_key.as_deref() == Some("OpenSearch /msg/_doc/# returned #"))
        .expect("digit-normalized 429 group");
    assert_eq!(opensearch.count, 2);
    assert_eq!(opensearch.index_status, "failed");
    assert!(
        opensearch
            .sample_error
            .as_deref()
            .unwrap()
            .starts_with("OpenSearch /msg/_doc/")
    );

    let embedding = groups
        .iter()
        .find(|g| g.error_key.as_deref() == Some("embedding request failed"))
        .unwrap();
    assert_eq!(embedding.count, 2);

    let quarantine = groups
        .iter()
        .find(|g| g.index_status == "quarantined")
        .unwrap();
    assert_eq!(quarantine.count, 1);
    assert_eq!(
        quarantine.sample_error.as_deref(),
        Some("sanitize: blocked html")
    );
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn problem_messages_filter_by_status_and_group(pool: PgPool) {
    let (account, ids) = problem_fixture(&pool).await;

    // Whole failed status, newest received first.
    let failed = messages::problem_messages(&pool, account.id, "failed", None, 50)
        .await
        .unwrap();
    assert_eq!(failed.len(), 4);
    assert!(
        failed
            .windows(2)
            .all(|w| w[0].received_at >= w[1].received_at)
    );

    // Scoped to one normalized group.
    let scoped = messages::problem_messages(
        &pool,
        account.id,
        "failed",
        Some("OpenSearch /msg/_doc/# returned #"),
        50,
    )
    .await
    .unwrap();
    assert_eq!(scoped.len(), 2);
    assert!(scoped.iter().all(|m| m.retryable));

    // The blob-less failure reports retryable = false.
    let embedding = messages::problem_messages(
        &pool,
        account.id,
        "failed",
        Some("embedding request failed"),
        50,
    )
    .await
    .unwrap();
    let blobless = embedding.iter().find(|m| m.id == ids[4]).unwrap();
    assert!(!blobless.retryable);

    let quarantined = messages::problem_messages(&pool, account.id, "quarantined", None, 50)
        .await
        .unwrap();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(
        quarantined[0].error.as_deref(),
        Some("sanitize: blocked html")
    );
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn requeue_failed_respects_scope_and_skips_unretryable(pool: PgPool) {
    let (account, ids) = problem_fixture(&pool).await;

    // Scoped requeue touches only the 429 group.
    let n = messages::requeue_failed(&pool, account.id, Some("OpenSearch /msg/_doc/# returned #"))
        .await
        .unwrap();
    assert_eq!(n, 2);
    let m = messages::get(&pool, ids[0]).await.unwrap().unwrap();
    assert_eq!(m.index_status, "staged");
    assert_eq!(m.error, None);

    // Unscoped requeue picks up the remaining stored failure but skips the
    // quarantined row and the blob-less one.
    let n = messages::requeue_failed(&pool, account.id, None)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        messages::get(&pool, ids[3])
            .await
            .unwrap()
            .unwrap()
            .index_status,
        "quarantined"
    );
    assert_eq!(
        messages::get(&pool, ids[4])
            .await
            .unwrap()
            .unwrap()
            .index_status,
        "failed"
    );

    // Re-staged rows are visible to the ordinary promote scan.
    let cutoff = Utc::now() - Duration::days(7);
    let promotable = messages::promotable(&pool, account.id, cutoff, 100)
        .await
        .unwrap();
    let promotable_ids: Vec<i64> = promotable.iter().map(|m| m.id).collect();
    assert!(promotable_ids.contains(&ids[0]));
    assert!(promotable_ids.contains(&ids[1]));
    assert!(promotable_ids.contains(&ids[2]));
}

#[sqlx::test(migrator = "db::MIGRATOR")]
async fn requeue_one_guards_ownership_and_state(pool: PgPool) {
    let (account, ids) = problem_fixture(&pool).await;

    // Failed and quarantined rows are both retryable one at a time.
    assert!(
        messages::requeue_one(&pool, ids[0], account.id)
            .await
            .unwrap()
    );
    assert!(
        messages::requeue_one(&pool, ids[3], account.id)
            .await
            .unwrap()
    );
    let q = messages::get(&pool, ids[3]).await.unwrap().unwrap();
    assert_eq!(q.index_status, "staged");
    assert_eq!(q.error, None);

    // Already staged, indexed, blob-less, or foreign-account rows refuse.
    assert!(
        !messages::requeue_one(&pool, ids[0], account.id)
            .await
            .unwrap()
    );
    assert!(
        !messages::requeue_one(&pool, ids[5], account.id)
            .await
            .unwrap()
    );
    assert!(
        !messages::requeue_one(&pool, ids[4], account.id)
            .await
            .unwrap()
    );
    assert!(
        !messages::requeue_one(&pool, ids[2], account.id + 999)
            .await
            .unwrap()
    );
}
