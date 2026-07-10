mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arkivo::config::DeletionPolicy;
use arkivo::db::{accounts, audit, messages, users};
use arkivo::gmail::GmailClient;
use arkivo::gmail::backfill::{backfill_account, fetch_missing_blobs};
use arkivo::gmail::sync::poll_account;
use arkivo::ingest::{BackfillOptions, SyncStats};
use arkivo::jmap::RetryPolicy;
use arkivo::maildir::{Maildir, MessageStore};
use chrono::{TimeZone, Utc};
use sqlx::PgPool;
use support::fake_gmail::{EMAIL, FakeGmail, REFRESH_TOKEN};

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

struct Harness {
    fake: FakeGmail,
    client: GmailClient,
    account: accounts::MailAccount,
    maildir: Maildir,
    _dir: tempfile::TempDir,
}

async fn harness(pool: &PgPool) -> Harness {
    let fake = FakeGmail::start().await;
    let client = GmailClient::with_endpoints(
        &fake.google_config(),
        REFRESH_TOKEN.into(),
        RetryPolicy {
            max_retries: 3,
            base_delay: Duration::from_millis(5),
        },
        &fake.token_url(),
        &fake.api_base(),
    )
    .unwrap();
    let user = users::create(pool, "alice", "user").await.unwrap();
    let account = accounts::create_gmail(pool, user.id, EMAIL, b"sealed", "primary")
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let maildir = Maildir::open_or_create(dir.path()).unwrap();
    Harness {
        fake,
        client,
        account,
        maildir,
        _dir: dir,
    }
}

fn file_count(maildir: &Maildir) -> usize {
    std::fs::read_dir(maildir.root().join("new"))
        .unwrap()
        .count()
}

async fn run_backfill(pool: &PgPool, h: &Harness, options: &BackfillOptions) -> u64 {
    backfill_account(pool, &h.client, &h.maildir, &h.account, options, None)
        .await
        .unwrap()
        .fetched
}

async fn run_poll(pool: &PgPool, h: &Harness, policy: DeletionPolicy) -> SyncStats {
    poll_account(pool, &h.client, &h.maildir, None, &h.account, policy)
        .await
        .unwrap()
}

/// Store wrapper that starts failing after N successful writes —
/// simulates a crash mid-batch.
struct FailingStore<'a> {
    inner: &'a Maildir,
    allowed: AtomicUsize,
}

impl<'a> FailingStore<'a> {
    fn new(inner: &'a Maildir, allowed: usize) -> Self {
        Self {
            inner,
            allowed: AtomicUsize::new(allowed),
        }
    }
}

impl MessageStore for FailingStore<'_> {
    fn write(&self, name_hint: &str, contents: &[u8]) -> anyhow::Result<String> {
        if self.allowed.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.allowed.store(0, Ordering::SeqCst);
            anyhow::bail!("injected write failure");
        }
        self.inner.write(name_hint, contents)
    }

    fn read(&self, rel_path: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.read(rel_path)
    }

    fn remove(&self, rel_path: &str) -> anyhow::Result<()> {
        self.inner.remove(rel_path)
    }
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_seeds_archive_and_first_poll_continues(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=5 {
        h.fake
            .add_message(&format!("old {day}"), "a@example.com", ts(day), &["INBOX"]);
    }

    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched, 5);
    assert_eq!(file_count(&h.maildir), 5);

    let state = accounts::get_gmail_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
    assert!(state.history_id.is_some());
    assert!(state.backfill_cursor.is_none(), "cursor cleared when done");

    // A message arriving after backfill is caught by the first poll —
    // proves the history id was recorded before the sweep.
    h.fake
        .add_message("new arrival", "b@example.com", ts(20), &["INBOX", "UNREAD"]);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.fetched, 1);
    assert!(!stats.resynced);
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().total,
        6
    );

    // Header-derived fields landed for promotion dedup.
    let row = messages::get_by_jmap_id(&pool, h.account.id, "g1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.message_id_hdr.as_deref(), Some("<g1@fake.gmail>"));
    assert_eq!(row.from_addr.as_deref(), Some("a@example.com"));
    assert_eq!(row.subject.as_deref(), Some("old 1"));
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_respects_limit_and_resumes_from_cursor(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=5 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day), &["INBOX"]);
    }

    let options = BackfillOptions {
        limit: Some(2),
        page_size: 2,
        ..Default::default()
    };
    assert_eq!(run_backfill(&pool, &h, &options).await, 2);
    let state = accounts::get_gmail_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!state.backfill_done);
    assert!(state.backfill_cursor.is_some());

    // Second run continues from the cursor and finishes.
    assert_eq!(
        run_backfill(&pool, &h, &BackfillOptions::default()).await,
        3
    );
    let state = accounts::get_gmail_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
    assert_eq!(file_count(&h.maildir), 5);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_never_ingests_spam_or_trash(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake
        .add_message("real", "a@example.com", ts(1), &["INBOX"]);
    h.fake
        .add_message("junk", "s@example.com", ts(2), &["SPAM"]);
    h.fake
        .add_message("binned", "t@example.com", ts(3), &["TRASH"]);

    assert_eq!(
        run_backfill(&pool, &h, &BackfillOptions::default()).await,
        1
    );
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().total,
        1
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_falls_back_to_date_window_when_page_token_expires(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=5 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day), &["INBOX"]);
    }

    // First run stops mid-sweep with a stored pageToken.
    let options = BackfillOptions {
        limit: Some(2),
        page_size: 2,
        ..Default::default()
    };
    assert_eq!(run_backfill(&pool, &h, &options).await, 2);

    // The token dies between runs (Gmail gives no validity guarantee).
    h.fake.reject_page_tokens(true);
    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched, 3, "date-window fallback covers the remainder");

    let counts = messages::counts(&pool, h.account.id).await.unwrap();
    assert_eq!(counts.total, 5, "overlap absorbed by idempotent upserts");
    assert_eq!(file_count(&h.maildir), 5, "no duplicate files");
    let state = accounts::get_gmail_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_label_change_refreshes_metadata_without_redownload(pool: PgPool) {
    let h = harness(&pool).await;
    let m1 = h
        .fake
        .add_message("hello", "a@example.com", ts(1), &["INBOX", "UNREAD"]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.change_labels(&m1, &["STARRED"], &["UNREAD"]);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.fetched, 0);
    assert_eq!(stats.updated, 1);

    let msg = messages::get_by_jmap_id(&pool, h.account.id, &m1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.keywords.get("$seen"), Some(&serde_json::json!(true)));
    assert_eq!(msg.keywords.get("$flagged"), Some(&serde_json::json!(true)));
    assert!(
        msg.mailbox_ids
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("STARRED"))
    );
    assert_eq!(
        file_count(&h.maildir),
        1,
        "no duplicate blob for a label change"
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_skips_new_spam_and_same_page_deletes(pool: PgPool) {
    let h = harness(&pool).await;
    run_backfill(&pool, &h, &BackfillOptions::default()).await; // records history id

    h.fake
        .add_message("junk", "s@example.com", ts(1), &["SPAM"]);
    let flash = h
        .fake
        .add_message("gone already", "a@example.com", ts(2), &["INBOX"]);
    h.fake.destroy(&flash);

    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.fetched, 0, "spam skipped; added+deleted coalesced");
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().total,
        0
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn history_id_not_advanced_when_write_fails_and_retry_has_no_duplicates(pool: PgPool) {
    let h = harness(&pool).await;
    run_backfill(&pool, &h, &BackfillOptions::default()).await; // records history id
    let state_before = accounts::get_gmail_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap()
        .history_id;

    for day in 1..=3 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day), &["INBOX"]);
    }

    // First write succeeds, second dies mid-batch.
    let failing = FailingStore::new(&h.maildir, 1);
    let result = poll_account(
        &pool,
        &h.client,
        &failing,
        None,
        &h.account,
        DeletionPolicy::Retain,
    )
    .await;
    assert!(result.is_err(), "injected failure must surface");

    let state_after = accounts::get_gmail_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap()
        .history_id;
    assert_eq!(
        state_before, state_after,
        "history id must not advance past a failed batch"
    );

    // Healthy retry completes the batch exactly once.
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(
        stats.fetched, 2,
        "only the two unwritten messages are fetched"
    );
    let counts = messages::counts(&pool, h.account.id).await.unwrap();
    assert_eq!(counts.total, 3);
    assert_eq!(counts.unfetched, 0);
    assert_eq!(file_count(&h.maildir), 3, "no duplicate files after retry");
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn destroyed_with_retain_keeps_message_and_audits(pool: PgPool) {
    let h = harness(&pool).await;
    let m1 = h
        .fake
        .add_message("keep me", "a@example.com", ts(1), &["INBOX"]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.destroy(&m1);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.destroyed, 1);

    let msg = messages::get_by_jmap_id(&pool, h.account.id, &m1)
        .await
        .unwrap();
    assert!(msg.is_some());
    assert_eq!(file_count(&h.maildir), 1);

    let entries = audit::recent_for_user(&pool, h.account.user_id, 10)
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.action == "message_destroyed_upstream_retained")
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn destroyed_with_mirror_removes_row_and_file(pool: PgPool) {
    let h = harness(&pool).await;
    let m1 = h
        .fake
        .add_message("remove me", "a@example.com", ts(1), &["INBOX"]);
    h.fake
        .add_message("keep me", "a@example.com", ts(2), &["INBOX"]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.destroy(&m1);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 1);

    assert!(
        messages::get_by_jmap_id(&pool, h.account.id, &m1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(file_count(&h.maildir), 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn expired_history_triggers_full_resync_without_destroying_trashed(pool: PgPool) {
    let h = harness(&pool).await;
    let m1 = h
        .fake
        .add_message("original", "a@example.com", ts(1), &["INBOX"]);
    let m2 = h
        .fake
        .add_message("to trash", "a@example.com", ts(2), &["INBOX"]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    // History expires; meanwhile: one message purged, one merely
    // trashed, one new, one new spam.
    h.fake.destroy(&m1);
    h.fake.change_labels(&m2, &["TRASH"], &["INBOX"]);
    h.fake
        .add_message("post-expiry", "b@example.com", ts(10), &["INBOX"]);
    h.fake
        .add_message("junk", "s@example.com", ts(11), &["SPAM"]);
    h.fake.invalidate_history();

    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert!(stats.resynced);
    assert_eq!(stats.fetched, 1, "new message fetched, spam skipped");
    assert_eq!(stats.destroyed, 1, "purged message detected by diff");
    assert!(
        messages::get_by_jmap_id(&pool, h.account.id, &m2)
            .await
            .unwrap()
            .is_some(),
        "trashed-but-present message must not be mirrored away"
    );
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().total,
        2
    );

    // Subsequent poll works from the resync-recorded history id.
    h.fake
        .add_message("after resync", "b@example.com", ts(12), &["INBOX"]);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert!(!stats.resynced);
    assert_eq!(stats.fetched, 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn fetch_missing_blobs_recovers_and_prunes_vanished(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=3 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day), &["INBOX"]);
    }

    // One write succeeds, the rest die: metadata lands, bodies don't.
    let failing = FailingStore::new(&h.maildir, 1);
    let stats = backfill_account(
        &pool,
        &h.client,
        &failing,
        &h.account,
        &BackfillOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.failed, 2);
    let unfetched = messages::unfetched(&pool, h.account.id, 10).await.unwrap();
    assert_eq!(unfetched.len(), 2);

    // One of the unfetched vanishes upstream before recovery runs.
    h.fake.destroy(&unfetched[0].blob_id);

    let recovered = fetch_missing_blobs(&pool, &h.client, &h.maildir, &h.account)
        .await
        .unwrap();
    assert_eq!(recovered, 1);

    let counts = messages::counts(&pool, h.account.id).await.unwrap();
    assert_eq!(counts.unfetched, 0);
    assert_eq!(counts.total, 2, "vanished row pruned");
    let entries = audit::recent_for_user(&pool, h.account.user_id, 10)
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.action == "message_vanished_before_fetch")
    );
}
