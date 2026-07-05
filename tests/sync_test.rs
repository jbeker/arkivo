mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arkivo::config::DeletionPolicy;
use arkivo::db::{accounts, audit, locks::AdvisoryLock, messages, users};
use arkivo::jmap::backfill::{BackfillOptions, backfill_account};
use arkivo::jmap::sync::poll_account;
use arkivo::jmap::{JmapClient, RetryPolicy};
use arkivo::maildir::{Maildir, MessageStore};
use chrono::{TimeZone, Utc};
use sqlx::PgPool;
use support::fake_jmap::FakeJmap;

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

struct Harness {
    fake: FakeJmap,
    client: JmapClient,
    account: accounts::MailAccount,
    maildir: Maildir,
    _dir: tempfile::TempDir,
}

async fn harness(pool: &PgPool) -> Harness {
    let fake = FakeJmap::start().await;
    let client = JmapClient::connect(
        &fake.session_url(),
        fake.token(),
        RetryPolicy {
            max_retries: 3,
            base_delay: Duration::from_millis(5),
        },
    )
    .await
    .unwrap();
    let user = users::create(pool, "alice", "user").await.unwrap();
    let account = accounts::create(pool, user.id, &fake.session_url(), b"sealed", "primary")
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
    backfill_account(pool, &h.client, &h.maildir, &h.account, options)
        .await
        .unwrap()
        .fetched
}

async fn run_poll(
    pool: &PgPool,
    h: &Harness,
    policy: DeletionPolicy,
) -> arkivo::jmap::sync::SyncStats {
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
            .add_message(&format!("old {day}"), "a@example.com", ts(day));
    }

    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched, 5);
    assert_eq!(file_count(&h.maildir), 5);

    let state = accounts::get_jmap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
    assert!(state.email_state.is_some());
    assert!(state.backfill_anchor.is_none(), "anchor cleared when done");

    // A message arriving after backfill is caught by the first poll —
    // proves the state was recorded before the sweep.
    h.fake.add_message("new arrival", "b@example.com", ts(20));
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.fetched, 1);
    assert!(!stats.resynced);
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().total,
        6
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_respects_limit_and_resumes_from_anchor(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=5 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day));
    }

    let options = BackfillOptions {
        limit: Some(2),
        ..Default::default()
    };
    assert_eq!(run_backfill(&pool, &h, &options).await, 2);
    let state = accounts::get_jmap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!state.backfill_done);
    assert!(state.backfill_anchor.is_some());

    // Second run continues from the anchor and finishes.
    assert_eq!(
        run_backfill(&pool, &h, &BackfillOptions::default()).await,
        3
    );
    let state = accounts::get_jmap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
    assert_eq!(file_count(&h.maildir), 5);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_pages_with_small_server_limit(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.set_max_objects_in_get(2);
    for day in 1..=7 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day));
    }
    // Client reconnect to pick up the lowered limit.
    let client = JmapClient::connect(
        &h.fake.session_url(),
        h.fake.token(),
        RetryPolicy::default(),
    )
    .await
    .unwrap();
    let stats = backfill_account(
        &pool,
        &client,
        &h.maildir,
        &h.account,
        &BackfillOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(stats.fetched, 7);
    assert!(stats.pages >= 4, "7 messages at page cap 2");
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_refreshes_metadata_for_updated_messages(pool: PgPool) {
    let h = harness(&pool).await;
    let m1 = h.fake.add_message("hello", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.update_message(&m1);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.fetched, 0);
    assert_eq!(stats.updated, 1);

    let msg = messages::get_by_jmap_id(&pool, h.account.id, &m1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.keywords.get("$seen"), Some(&serde_json::json!(true)));
    assert_eq!(
        file_count(&h.maildir),
        1,
        "no duplicate blob for metadata update"
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn state_not_advanced_when_write_fails_and_retry_has_no_duplicates(pool: PgPool) {
    let h = harness(&pool).await;
    run_backfill(&pool, &h, &BackfillOptions::default()).await; // records state
    let state_before = accounts::get_jmap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap()
        .email_state;

    for day in 1..=3 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day));
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

    let state_after = accounts::get_jmap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap()
        .email_state;
    assert_eq!(
        state_before, state_after,
        "state must not advance past a failed batch"
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
    let m1 = h.fake.add_message("keep me", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.destroy(&m1);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.destroyed, 1);

    // Message survives in ledger and store.
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
    let m1 = h.fake.add_message("remove me", "a@example.com", ts(1));
    h.fake.add_message("keep me", "a@example.com", ts(2));
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
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().total,
        1
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn cannot_calculate_changes_triggers_full_resync(pool: PgPool) {
    let h = harness(&pool).await;
    let m1 = h.fake.add_message("original", "a@example.com", ts(1));
    h.fake.add_message("also original", "a@example.com", ts(2));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    // Server forgets our state; mailbox changes underneath us.
    h.fake.destroy(&m1);
    h.fake
        .add_message("post-invalidation", "b@example.com", ts(10));
    h.fake.invalidate_state();

    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert!(stats.resynced);
    assert_eq!(stats.fetched, 1, "new message fetched during resync");
    assert_eq!(stats.destroyed, 1, "vanished message detected by diff");

    let counts = messages::counts(&pool, h.account.id).await.unwrap();
    assert_eq!(counts.total, 2);

    // Subsequent poll works from the resync-recorded state.
    h.fake.add_message("after resync", "b@example.com", ts(11));
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert!(!stats.resynced);
    assert_eq!(stats.fetched, 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn advisory_lock_blocks_concurrent_holder(_pool: PgPool) {
    let url = std::env::var("DATABASE_URL").unwrap();
    let first = AdvisoryLock::try_acquire(&url, "poll", 999_888)
        .await
        .unwrap();
    assert!(first.is_some());

    let second = AdvisoryLock::try_acquire(&url, "poll", 999_888)
        .await
        .unwrap();
    assert!(second.is_none(), "same account+kind must be exclusive");

    let other_account = AdvisoryLock::try_acquire(&url, "poll", 999_889)
        .await
        .unwrap();
    assert!(other_account.is_some(), "different account is independent");

    first.unwrap().release().await.unwrap();
    let third = AdvisoryLock::try_acquire(&url, "poll", 999_888)
        .await
        .unwrap();
    assert!(third.is_some(), "released lock is reacquirable");
    third.unwrap().release().await.unwrap();
    other_account.unwrap().release().await.unwrap();
}
