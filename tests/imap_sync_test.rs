mod support;

use std::sync::atomic::{AtomicUsize, Ordering};

use arkivo::config::DeletionPolicy;
use arkivo::db::{accounts, audit, imap_map, messages, users};
use arkivo::imap::backfill::backfill_account;
use arkivo::imap::sync::{fetch_missing_blobs, poll_account};
use arkivo::imap::{ImapClient, TlsMode};
use arkivo::ingest::{BackfillOptions, SyncStats};
use arkivo::maildir::{Maildir, MessageStore};
use chrono::{TimeZone, Utc};
use sqlx::PgPool;
use support::fake_imap::{FakeImap, PASSWORD, USERNAME};

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

struct Harness {
    fake: FakeImap,
    client: ImapClient,
    account: accounts::MailAccount,
    maildir: Maildir,
    _dir: tempfile::TempDir,
}

async fn harness(pool: &PgPool) -> Harness {
    let fake = FakeImap::start().await;
    let client = ImapClient::connect(
        &fake.host(),
        fake.port(),
        TlsMode::None,
        USERNAME,
        PASSWORD,
    )
    .await
    .unwrap();
    let user = users::create(pool, "alice", "user").await.unwrap();
    let account = accounts::create_imap(
        pool,
        user.id,
        USERNAME,
        &fake.host(),
        fake.port() as i32,
        "none",
        b"sealed",
        "primary",
    )
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

async fn row_count(pool: &PgPool, account_id: i64) -> i64 {
    messages::counts(pool, account_id).await.unwrap().total
}

async fn mailboxes_of(pool: &PgPool, account_id: i64, subject: &str) -> Vec<String> {
    let row = sqlx::query!(
        "select mailbox_ids from messages where mail_account_id = $1 and subject = $2",
        account_id,
        subject,
    )
    .fetch_one(pool)
    .await
    .unwrap();
    serde_json::from_value(row.mailbox_ids).unwrap()
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
async fn backfill_seeds_multi_folder_archive(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.add_folder("Archive", None);
    for day in 1..=3 {
        h.fake
            .add_message("INBOX", &format!("in {day}"), "a@example.com", ts(day), &[]);
    }
    h.fake
        .add_message("Archive", "filed", "a@example.com", ts(4), &["\\Seen"]);

    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched, 4);
    assert_eq!(file_count(&h.maildir), 4);

    let state = accounts::get_imap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
    assert!(state.backfill_cursor.is_none(), "cursor cleared when done");

    for folder in ["INBOX", "Archive"] {
        assert!(
            imap_map::get_folder(&pool, h.account.id, folder)
                .await
                .unwrap()
                .is_some(),
            "{folder} cursor row exists"
        );
    }
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "filed").await,
        vec!["Archive"]
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn move_between_polls_no_redownload_no_duplicate(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.add_folder("Archive", None);
    let uid = h
        .fake
        .add_message("INBOX", "moved", "a@example.com", ts(1), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let downloads_after_backfill = h.fake.body_fetch_count();
    assert_eq!(downloads_after_backfill, 1);

    h.fake.move_message("INBOX", uid, "Archive");
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;

    assert_eq!(
        h.fake.body_fetch_count(),
        downloads_after_backfill,
        "a move must not re-download the body"
    );
    assert_eq!(stats.fetched, 0);
    assert_eq!(stats.destroyed, 0, "a move is not a delete");
    assert!(stats.updated >= 1);
    assert_eq!(row_count(&pool, h.account.id).await, 1);
    assert_eq!(file_count(&h.maildir), 1);
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "moved").await,
        vec!["Archive"]
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn move_within_single_poll_never_destroys(pool: PgPool) {
    // Under Mirror, a wrong phase order (destroys before adds) would
    // delete the blob and re-download it. Assert neither happens.
    let h = harness(&pool).await;
    h.fake.add_folder("Projects/2020", None);
    let uid = h
        .fake
        .add_message("INBOX", "filed fast", "a@example.com", ts(1), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.move_message("INBOX", uid, "Projects/2020");
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;

    assert_eq!(stats.destroyed, 0);
    assert_eq!(h.fake.body_fetch_count(), 1, "backfill's download only");
    let entries = audit::recent_for_user(&pool, h.account.user_id, 10)
        .await
        .unwrap();
    assert!(
        !entries
            .iter()
            .any(|e| e.action.starts_with("message_destroyed")),
        "no destroy was applied during the move"
    );
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "filed fast").await,
        vec!["Projects/2020"]
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn copy_keeps_one_row_until_last_placement_gone(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.add_folder("Archive", None);
    let uid = h
        .fake
        .add_message("INBOX", "copied", "a@example.com", ts(1), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    let copy_uid = h.fake.copy_message("INBOX", uid, "Archive");
    run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(row_count(&pool, h.account.id).await, 1, "copy dedups");
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "copied").await,
        vec!["Archive", "INBOX"]
    );

    // Deleting one copy only drops that placement.
    h.fake.delete_message("INBOX", uid);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 0);
    assert_eq!(row_count(&pool, h.account.id).await, 1);
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "copied").await,
        vec!["Archive"]
    );

    // The last placement vanishing applies the policy.
    h.fake.delete_message("Archive", copy_uid);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 1);
    assert_eq!(row_count(&pool, h.account.id).await, 0);
    assert_eq!(file_count(&h.maildir), 0);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn destroyed_with_retain_keeps_message_and_audits(pool: PgPool) {
    let h = harness(&pool).await;
    let uid = h
        .fake
        .add_message("INBOX", "keep me", "a@example.com", ts(1), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.delete_message("INBOX", uid);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.destroyed, 1);

    assert_eq!(row_count(&pool, h.account.id).await, 1);
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
    let uid = h
        .fake
        .add_message("INBOX", "remove me", "a@example.com", ts(1), &[]);
    h.fake
        .add_message("INBOX", "keep me", "a@example.com", ts(2), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.delete_message("INBOX", uid);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 1);
    assert_eq!(row_count(&pool, h.account.id).await, 1);
    assert_eq!(file_count(&h.maildir), 1);
    let entries = audit::recent_for_user(&pool, h.account.user_id, 10)
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.action == "message_destroyed_upstream_mirrored")
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn uidvalidity_bump_resyncs_without_duplicates(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=3 {
        h.fake
            .add_message("INBOX", &format!("m {day}"), "a@example.com", ts(day), &[]);
    }
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let downloads = h.fake.body_fetch_count();

    h.fake.bump_uidvalidity("INBOX");
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;

    assert!(stats.resynced);
    assert_eq!(stats.destroyed, 0);
    assert_eq!(row_count(&pool, h.account.id).await, 3);
    assert_eq!(file_count(&h.maildir), 3);
    assert_eq!(
        h.fake.body_fetch_count(),
        downloads,
        "resync re-homes by Message-ID, no re-download"
    );

    let folder = imap_map::get_folder(&pool, h.account.id, "INBOX")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(folder.uidvalidity, 1001, "new uidvalidity adopted");
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn flag_change_maps_to_keywords_once_folder_changes(pool: PgPool) {
    let h = harness(&pool).await;
    let uid = h
        .fake
        .add_message("INBOX", "flagged later", "a@example.com", ts(1), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    let keywords_of = |subject: &str| {
        let pool = pool.clone();
        let account_id = h.account.id;
        let subject = subject.to_string();
        async move {
            sqlx::query!(
                "select keywords from messages where mail_account_id = $1 and subject = $2",
                account_id,
                subject,
            )
            .fetch_one(&pool)
            .await
            .unwrap()
            .keywords
        }
    };

    // A flag flip alone doesn't change folder membership, so the
    // unchanged-folder skip leaves it stale — the accepted tradeoff.
    h.fake.set_flags("INBOX", uid, &["\\Seen", "\\Flagged"]);
    run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    let kw = keywords_of("flagged later").await;
    assert!(kw.get("$seen").is_none(), "quiet folder: flags stay stale");

    // Any membership change dirties the folder and the sweep picks the
    // flags up.
    h.fake
        .add_message("INBOX", "newer", "a@example.com", ts(2), &[]);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.fetched, 1);
    assert!(stats.updated >= 1);
    let kw = keywords_of("flagged later").await;
    assert_eq!(kw["$seen"], serde_json::json!(true));
    assert_eq!(kw["$flagged"], serde_json::json!(true));
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn unchanged_folders_skip_uid_sweep(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.add_folder("Archive", None);
    h.fake
        .add_message("INBOX", "one", "a@example.com", ts(1), &[]);
    h.fake
        .add_message("Archive", "two", "a@example.com", ts(2), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let sweeps_after_backfill = h.fake.uid_sweep_count();

    // Nothing changed: no folder is swept.
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(h.fake.uid_sweep_count(), sweeps_after_backfill);
    assert_eq!(stats.fetched, 0);
    assert_eq!(stats.updated, 0);
    assert_eq!(stats.destroyed, 0);

    // One new message in INBOX: exactly that folder is swept.
    h.fake
        .add_message("INBOX", "three", "a@example.com", ts(3), &[]);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(h.fake.uid_sweep_count(), sweeps_after_backfill + 1);
    assert_eq!(stats.fetched, 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn skip_does_not_miss_deletions(pool: PgPool) {
    // A delete leaves UIDNEXT untouched; only the EXISTS-vs-placements
    // comparison catches it. Guards against skipping on UIDNEXT alone.
    let h = harness(&pool).await;
    let uid = h
        .fake
        .add_message("INBOX", "doomed", "a@example.com", ts(1), &[]);
    h.fake
        .add_message("INBOX", "stays", "a@example.com", ts(2), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.delete_message("INBOX", uid);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 1);
    assert_eq!(row_count(&pool, h.account.id).await, 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn missing_message_id_never_merges_but_hash_dedups(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.add_folder("Archive", None);
    let uid1 = h
        .fake
        .add_message_with_msgid("INBOX", "anon one", "a@example.com", ts(1), &[], None);
    h.fake
        .add_message_with_msgid("INBOX", "anon two", "a@example.com", ts(2), &[], None);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(row_count(&pool, h.account.id).await, 2);
    assert_eq!(h.fake.body_fetch_count(), 2);

    // Moving a no-Message-ID message forces a re-download, but the
    // content hash converges on the existing row — no duplicate.
    h.fake.move_message("INBOX", uid1, "Archive");
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(h.fake.body_fetch_count(), 3, "no Message-ID → download");
    assert_eq!(stats.destroyed, 0);
    assert_eq!(row_count(&pool, h.account.id).await, 2, "no duplicate row");
    assert_eq!(file_count(&h.maildir), 2);
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "anon one").await,
        vec!["Archive"]
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn trash_and_junk_folders_are_skipped(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake.add_folder("Rubbish", Some("\\Trash"));
    h.fake.add_folder("Spam", None); // by name heuristic
    h.fake
        .add_message("INBOX", "wanted", "a@example.com", ts(1), &[]);
    h.fake
        .add_message("Rubbish", "binned", "a@example.com", ts(2), &[]);
    h.fake
        .add_message("Spam", "junky", "a@example.com", ts(3), &[]);

    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched, 1, "only INBOX is archived");
    run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(row_count(&pool, h.account.id).await, 1);
    assert!(
        imap_map::get_folder(&pool, h.account.id, "Rubbish")
            .await
            .unwrap()
            .is_none(),
        "no cursor rows for skipped folders"
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn failed_writes_defer_and_recover(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=3 {
        h.fake
            .add_message("INBOX", &format!("m {day}"), "a@example.com", ts(day), &[]);
    }

    // First write succeeds, the rest fail — like a disk filling mid-page.
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
    assert_eq!(stats.fetched, 1);
    assert_eq!(stats.failed, 2);
    assert!(stats.complete);

    let counts = messages::counts(&pool, h.account.id).await.unwrap();
    assert_eq!(counts.total, 3, "metadata landed for every message");
    assert_eq!(counts.unfetched, 2);

    // The retry pass with a healthy store recovers the bodies.
    let recovered = fetch_missing_blobs(&pool, &h.client, &h.maildir, &h.account)
        .await
        .unwrap();
    assert_eq!(recovered, 2);
    let counts = messages::counts(&pool, h.account.id).await.unwrap();
    assert_eq!(counts.unfetched, 0);
    assert_eq!(file_count(&h.maildir), 3, "no duplicate files after retry");
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_cursor_resumes_and_cancellation_stops_at_page(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=6 {
        h.fake
            .add_message("INBOX", &format!("m {day}"), "a@example.com", ts(day), &[]);
    }

    // Page size 2 with a limit of 2: one durable page, then stop.
    let options = BackfillOptions {
        limit: Some(2),
        page_size: 2,
        ..Default::default()
    };
    let stats = backfill_account(&pool, &h.client, &h.maildir, &h.account, &options, None)
        .await
        .unwrap();
    assert_eq!(stats.fetched, 2);
    assert!(!stats.complete);
    let state = accounts::get_imap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!state.backfill_done, "limited run never marks done");
    let cursor = state.backfill_cursor.expect("cursor persisted");
    assert_eq!(cursor["last_uid"], serde_json::json!(2));

    // Cancellation via a jobs row stops at the next page boundary.
    let job_id = arkivo::db::jobs::start(&pool, "backfill", Some(h.account.id))
        .await
        .unwrap();
    arkivo::db::jobs::request_cancel(&pool, job_id, h.account.user_id)
        .await
        .unwrap();
    let options = BackfillOptions {
        page_size: 2,
        ..Default::default()
    };
    let stats = backfill_account(
        &pool,
        &h.client,
        &h.maildir,
        &h.account,
        &options,
        Some(job_id),
    )
    .await
    .unwrap();
    assert!(stats.cancelled);
    assert_eq!(stats.fetched, 2, "one page ingested before the cancel");

    // A clean rerun finishes from the cursor without re-downloading.
    let downloads = h.fake.body_fetch_count();
    assert_eq!(downloads, 4);
    let stats = backfill_account(
        &pool,
        &h.client,
        &h.maildir,
        &h.account,
        &BackfillOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert!(stats.complete);
    assert_eq!(stats.fetched, 2, "only the remaining messages");
    assert_eq!(h.fake.body_fetch_count(), 6, "no page re-downloaded");
    assert_eq!(file_count(&h.maildir), 6);
    let state = accounts::get_imap_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.backfill_done);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn fetch_missing_blobs_prunes_vanished_rows(pool: PgPool) {
    let h = harness(&pool).await;
    let uid = h
        .fake
        .add_message("INBOX", "gone soon", "a@example.com", ts(1), &[]);

    // Backfill with every write failing: metadata lands, no blob.
    let failing = FailingStore::new(&h.maildir, 0);
    backfill_account(
        &pool,
        &h.client,
        &failing,
        &h.account,
        &BackfillOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        messages::counts(&pool, h.account.id).await.unwrap().unfetched,
        1
    );

    // The message vanishes upstream before the blob is ever stored:
    // nothing to retain, the row is pruned with an audit record.
    h.fake.delete_message("INBOX", uid);
    let recovered = fetch_missing_blobs(&pool, &h.client, &h.maildir, &h.account)
        .await
        .unwrap();
    assert_eq!(recovered, 0);
    assert_eq!(row_count(&pool, h.account.id).await, 0);
    let entries = audit::recent_for_user(&pool, h.account.user_id, 10)
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.action == "message_vanished_before_fetch")
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_requires_backfill_first(pool: PgPool) {
    let h = harness(&pool).await;
    let err = poll_account(
        &pool,
        &h.client,
        &h.maildir,
        None,
        &h.account,
        DeletionPolicy::Retain,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("backfill"));
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn new_folder_between_polls_is_picked_up(pool: PgPool) {
    let h = harness(&pool).await;
    h.fake
        .add_message("INBOX", "first", "a@example.com", ts(1), &[]);
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    // A folder created after backfill, with mail filed straight into it.
    h.fake.add_folder("Receipts", None);
    h.fake
        .add_message("Receipts", "invoice", "a@example.com", ts(2), &["\\Seen"]);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.fetched, 1);
    assert_eq!(
        mailboxes_of(&pool, h.account.id, "invoice").await,
        vec!["Receipts"]
    );
}
