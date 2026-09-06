mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arkivo::config::DeletionPolicy;
use arkivo::crypto::Sealer;
use arkivo::db::{accounts, audit, jobs, messages, o365_folders, users};
use arkivo::ingest::{BackfillOptions, SyncStats};
use arkivo::jmap::RetryPolicy;
use arkivo::maildir::{Maildir, MessageStore};
use arkivo::o365::O365Client;
use arkivo::o365::backfill::{backfill_account, fetch_missing_blobs};
use arkivo::o365::sync::poll_account;
use chrono::{TimeZone, Utc};
use sqlx::PgPool;
use support::fake_o365::{FakeO365, MAIL, REFRESH_TOKEN};

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

fn sealer() -> Arc<Sealer> {
    Arc::new(Sealer::new(&[7u8; 32], "primary").unwrap())
}

struct Harness {
    fake: FakeO365,
    client: O365Client,
    account: accounts::MailAccount,
    maildir: Maildir,
    inbox: String,
    _dir: tempfile::TempDir,
}

fn make_client(fake: &FakeO365, refresh_token: &str) -> O365Client {
    O365Client::with_endpoints(
        &fake.microsoft_config(),
        refresh_token.into(),
        RetryPolicy {
            max_retries: 3,
            base_delay: Duration::from_millis(5),
        },
        sealer(),
        &fake.token_url(),
        &fake.api_base(),
    )
    .unwrap()
}

async fn harness(pool: &PgPool) -> Harness {
    let fake = FakeO365::start().await;
    let client = make_client(&fake, REFRESH_TOKEN);
    let user = users::create(pool, "alice", "user").await.unwrap();
    let sealed = sealer().seal(REFRESH_TOKEN.as_bytes()).unwrap();
    let account = accounts::create_o365(pool, user.id, MAIL, &sealed, "primary")
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let maildir = Maildir::open_or_create(dir.path()).unwrap();
    let inbox = fake.folder_id("inbox");
    Harness {
        fake,
        client,
        account,
        maildir,
        inbox,
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

async fn row(pool: &PgPool, h: &Harness, id: &str) -> Option<messages::Message> {
    messages::get_by_jmap_id(pool, h.account.id, id)
        .await
        .unwrap()
}

async fn placement(pool: &PgPool, h: &Harness, id: &str) -> serde_json::Value {
    row(pool, h, id).await.expect("row").mailbox_ids
}

async fn folder_rows(pool: &PgPool, h: &Harness) -> Vec<o365_folders::O365Folder> {
    o365_folders::list(pool, h.account.id).await.unwrap()
}

async fn backfill_done(pool: &PgPool, h: &Harness) -> bool {
    accounts::get_o365_state(pool, h.account.id)
        .await
        .unwrap()
        .unwrap()
        .backfill_done
}

async fn audit_actions(pool: &PgPool, h: &Harness) -> Vec<String> {
    audit::recent_for_user(pool, h.account.user_id, 50)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.action)
        .collect()
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
async fn backfill_seeds_folders_with_paths_and_skips_system_folders(pool: PgPool) {
    let h = harness(&pool).await;
    let archive = h.fake.folder_id("archive");
    let y2024 = h.fake.add_folder("2024", Some(&archive), None);
    let a = h
        .fake
        .add_message(&h.inbox, "in inbox", "a@example.com", ts(1));
    let b = h
        .fake
        .add_message(&y2024, "in archive", "a@example.com", ts(2));
    h.fake.add_message(
        &h.fake.folder_id("deleteditems"),
        "trashed",
        "a@example.com",
        ts(3),
    );
    h.fake.add_message(
        &h.fake.folder_id("junkemail"),
        "spam",
        "a@example.com",
        ts(4),
    );

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
    assert_eq!(stats.fetched, 2);
    assert!(stats.complete);
    assert_eq!(file_count(&h.maildir), 2);
    assert_eq!(placement(&pool, &h, &a).await, serde_json::json!(["Inbox"]));
    assert_eq!(
        placement(&pool, &h, &b).await,
        serde_json::json!(["Archive/2024"])
    );
    let stored = row(&pool, &h, &b).await.unwrap();
    assert_eq!(
        stored.message_id_hdr.as_deref().map(|s| s.starts_with('<')),
        Some(true)
    );
    assert_eq!(stored.received_at, ts(2));
    assert!(backfill_done(&pool, &h).await);
    let rows = folder_rows(&pool, &h).await;
    assert!(
        rows.iter()
            .all(|r| r.delta_link.is_some() && r.next_link.is_none())
    );
    assert!(rows.iter().any(|r| r.display_path == "Archive/2024"));
    assert!(!rows.iter().any(|r| r.display_path == "Deleted Items"));

    // A quiet poll walks nothing new.
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (0, 0, 0));
    assert_eq!(h.fake.prefer_violations(), 0);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_refuses_before_backfill(pool: PgPool) {
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
    assert!(err.to_string().contains("run backfill first"));
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_limit_leaves_next_link_and_resumes_without_redownload(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=7 {
        h.fake
            .add_message(&h.inbox, &format!("m{day}"), "a@example.com", ts(day));
    }
    let stats = backfill_account(
        &pool,
        &h.client,
        &h.maildir,
        &h.account,
        &BackfillOptions {
            limit: Some(3),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.fetched, 3);
    assert!(!stats.complete);
    assert!(!backfill_done(&pool, &h).await);
    let inbox_row = folder_rows(&pool, &h)
        .await
        .into_iter()
        .find(|r| r.display_path == "Inbox")
        .unwrap();
    assert!(inbox_row.next_link.is_some());
    assert!(inbox_row.delta_link.is_none());

    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched, 4);
    assert!(backfill_done(&pool, &h).await);
    assert_eq!(h.fake.value_fetches(), 7);
    assert_eq!(file_count(&h.maildir), 7);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_since_is_sticky_until_widened(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=5 {
        h.fake
            .add_message(&h.inbox, &format!("m{day}"), "a@example.com", ts(day));
    }
    let since = BackfillOptions {
        since: Some(ts(3)),
        ..Default::default()
    };
    assert_eq!(run_backfill(&pool, &h, &since).await, 3);
    assert!(backfill_done(&pool, &h).await);
    let state = accounts::get_o365_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.backfill_since, Some(ts(3)));

    // A later floor changes nothing; the stored one stays.
    let later = BackfillOptions {
        since: Some(ts(4)),
        ..Default::default()
    };
    assert_eq!(run_backfill(&pool, &h, &later).await, 0);

    // A narrower request re-walks everything (stored rows just refresh).
    assert_eq!(
        run_backfill(&pool, &h, &BackfillOptions::default()).await,
        2
    );
    assert_eq!(h.fake.value_fetches(), 5);
    let state = accounts::get_o365_state(&pool, h.account.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.backfill_since, None);
    assert!(backfill_done(&pool, &h).await);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_since_falls_back_to_client_side_filter(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=5 {
        h.fake
            .add_message(&h.inbox, &format!("m{day}"), "a@example.com", ts(day));
    }
    h.fake.reject_filter(true);
    let since = BackfillOptions {
        since: Some(ts(3)),
        ..Default::default()
    };
    assert_eq!(run_backfill(&pool, &h, &since).await, 3);
    assert_eq!(h.fake.value_fetches(), 3);
    assert!(backfill_done(&pool, &h).await);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn fetch_missing_blobs_recovers_and_prunes_vanished(pool: PgPool) {
    let h = harness(&pool).await;
    let ids: Vec<String> = (1..=3)
        .map(|day| {
            h.fake
                .add_message(&h.inbox, &format!("m{day}"), "a@example.com", ts(day))
        })
        .collect();
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
    assert_eq!(
        messages::unfetched(&pool, h.account.id, 10)
            .await
            .unwrap()
            .len(),
        2
    );

    // One of the stragglers vanishes upstream before recovery.
    let unfetched = messages::unfetched(&pool, h.account.id, 10).await.unwrap();
    let doomed = unfetched[0].blob_id.clone();
    assert!(ids.contains(&doomed));
    h.fake.destroy(&doomed);

    let recovered = fetch_missing_blobs(&pool, &h.client, &h.maildir, &h.account)
        .await
        .unwrap();
    assert_eq!(recovered, 1);
    assert!(
        messages::unfetched(&pool, h.account.id, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(row(&pool, &h, &doomed).await.is_none());
    assert!(
        audit_actions(&pool, &h)
            .await
            .contains(&"message_vanished_before_fetch".to_string())
    );
    assert_eq!(file_count(&h.maildir), 2);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_honors_cancel_and_resumes(pool: PgPool) {
    let h = harness(&pool).await;
    for day in 1..=4 {
        h.fake
            .add_message(&h.inbox, &format!("m{day}"), "a@example.com", ts(day));
    }
    let job = jobs::start(&pool, "backfill", Some(h.account.id))
        .await
        .unwrap();
    assert!(
        jobs::request_cancel(&pool, job, h.account.user_id)
            .await
            .unwrap()
    );
    let stats = backfill_account(
        &pool,
        &h.client,
        &h.maildir,
        &h.account,
        &BackfillOptions {
            limit: Some(2),
            ..Default::default()
        },
        Some(job),
    )
    .await
    .unwrap();
    assert!(stats.cancelled);
    assert!(!backfill_done(&pool, &h).await);
    let fetched_first = stats.fetched;
    let fetched = run_backfill(&pool, &h, &BackfillOptions::default()).await;
    assert_eq!(fetched_first + fetched, 4);
    assert_eq!(h.fake.value_fetches(), 4);
    assert!(backfill_done(&pool, &h).await);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_fetches_new_and_refreshes_flags_without_download(pool: PgPool) {
    let h = harness(&pool).await;
    let a = h.fake.add_message(&h.inbox, "old", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    let b = h.fake.add_message(&h.inbox, "new", "b@example.com", ts(2));
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (1, 0, 0));
    assert_eq!(placement(&pool, &h, &b).await, serde_json::json!(["Inbox"]));

    let downloads = h.fake.value_fetches();
    h.fake.set_read(&a, true);
    h.fake.set_flagged(&a, true);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (0, 1, 0));
    assert_eq!(
        row(&pool, &h, &a).await.unwrap().keywords,
        serde_json::json!({"$seen": true, "$flagged": true})
    );
    assert_eq!(h.fake.value_fetches(), downloads);
    assert_eq!(file_count(&h.maildir), 2);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn poll_move_rehomes_without_download_or_destroy(pool: PgPool) {
    let h = harness(&pool).await;
    let archive = h.fake.folder_id("archive");
    let y2024 = h.fake.add_folder("2024", Some(&archive), None);
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    let b = h.fake.add_message(&y2024, "b", "a@example.com", ts(2));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let downloads = h.fake.value_fetches();
    let metas = h.fake.meta_fetches();

    // Destination read before source (path order) and the reverse.
    h.fake.move_message(&a, &y2024);
    h.fake.move_message(&b, &h.inbox);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 0);
    assert_eq!(stats.updated, 2);
    assert_eq!(
        placement(&pool, &h, &a).await,
        serde_json::json!(["Archive/2024"])
    );
    assert_eq!(placement(&pool, &h, &b).await, serde_json::json!(["Inbox"]));
    assert_eq!(h.fake.value_fetches(), downloads);
    assert_eq!(h.fake.meta_fetches(), metas);
    assert_eq!(file_count(&h.maildir), 2);

    // Quiet afterwards: the tombstones were consumed with the cursor.
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (0, 0, 0));
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn destroyed_with_retain_keeps_message_and_audits(pool: PgPool) {
    let h = harness(&pool).await;
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    h.fake.destroy(&a);
    let stats = run_poll(&pool, &h, DeletionPolicy::Retain).await;
    assert_eq!(stats.destroyed, 1);
    assert!(row(&pool, &h, &a).await.is_some());
    assert_eq!(file_count(&h.maildir), 1);
    assert!(
        audit_actions(&pool, &h)
            .await
            .contains(&"message_destroyed_upstream_retained".to_string())
    );
    // Exactly one verify fetch for the genuine delete.
    assert_eq!(h.fake.meta_fetches(), 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn destroyed_with_mirror_removes_row_and_file(pool: PgPool) {
    let h = harness(&pool).await;
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    h.fake.add_message(&h.inbox, "b", "a@example.com", ts(2));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    h.fake.destroy(&a);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 1);
    assert!(row(&pool, &h, &a).await.is_none());
    assert_eq!(file_count(&h.maildir), 1);
    assert!(
        audit_actions(&pool, &h)
            .await
            .contains(&"message_destroyed_upstream_mirrored".to_string())
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn move_to_deleted_items_counts_as_deletion_and_restore_reingests(pool: PgPool) {
    let h = harness(&pool).await;
    let deleted = h.fake.folder_id("deleteditems");
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    let b = h.fake.add_message(&h.inbox, "b", "a@example.com", ts(2));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.move_message(&a, &deleted);
    h.fake.move_message(&b, &deleted);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 2);
    assert!(row(&pool, &h, &a).await.is_none());
    assert_eq!(file_count(&h.maildir), 0);

    // Restored: fetched again under mirror.
    h.fake.move_message(&a, &h.inbox);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.destroyed), (1, 0));
    assert_eq!(placement(&pool, &h, &a).await, serde_json::json!(["Inbox"]));
    assert_eq!(file_count(&h.maildir), 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn new_folder_is_walked_and_moved_in_mail_is_rehomed(pool: PgPool) {
    let h = harness(&pool).await;
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let downloads = h.fake.value_fetches();

    let projects = h.fake.add_folder("Projects", None, None);
    h.fake.move_message(&a, &projects);
    let b = h.fake.add_message(&projects, "b", "a@example.com", ts(2));
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (1, 1, 0));
    assert_eq!(
        placement(&pool, &h, &a).await,
        serde_json::json!(["Projects"])
    );
    assert_eq!(
        placement(&pool, &h, &b).await,
        serde_json::json!(["Projects"])
    );
    assert_eq!(h.fake.value_fetches(), downloads + 1);
    let rows = folder_rows(&pool, &h).await;
    let projects_row = rows.iter().find(|r| r.folder_id == projects).unwrap();
    assert!(projects_row.delta_link.is_some());
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn folder_rename_rewrites_placements_and_deletion_destroys(pool: PgPool) {
    let h = harness(&pool).await;
    let projects = h.fake.add_folder("Projects", None, None);
    let sub = h.fake.add_folder("Alpha", Some(&projects), None);
    let a = h.fake.add_message(&projects, "a", "a@example.com", ts(1));
    let b = h.fake.add_message(&sub, "b", "a@example.com", ts(2));
    let c = h.fake.add_message(&h.inbox, "c", "a@example.com", ts(3));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;

    h.fake.rename_folder(&projects, "Work");
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.updated, 2);
    assert_eq!(placement(&pool, &h, &a).await, serde_json::json!(["Work"]));
    assert_eq!(
        placement(&pool, &h, &b).await,
        serde_json::json!(["Work/Alpha"])
    );
    assert_eq!(placement(&pool, &h, &c).await, serde_json::json!(["Inbox"]));

    // Deleting to Deleted Items: the subtree leaves the archivable set.
    h.fake.delete_folder(&projects, false);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 2);
    assert!(row(&pool, &h, &a).await.is_none());
    assert!(row(&pool, &h, &b).await.is_none());
    assert!(row(&pool, &h, &c).await.is_some());
    let rows = folder_rows(&pool, &h).await;
    assert!(
        !rows
            .iter()
            .any(|r| r.folder_id == projects || r.folder_id == sub)
    );

    // Purging a folder outright.
    let scratch = h.fake.add_folder("Scratch", None, None);
    let d = h.fake.add_message(&scratch, "d", "a@example.com", ts(4));
    run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert!(row(&pool, &h, &d).await.is_some());
    h.fake.delete_folder(&scratch, true);
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 1);
    assert!(row(&pool, &h, &d).await.is_none());
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn expired_delta_resyncs_one_folder(pool: PgPool) {
    let h = harness(&pool).await;
    let archive = h.fake.folder_id("archive");
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    let b = h.fake.add_message(&h.inbox, "b", "a@example.com", ts(2));
    let c = h.fake.add_message(&archive, "c", "a@example.com", ts(3));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    // Make Archive's link the only surviving one by cycling it after
    // the expiry point.
    h.fake.expire_delta_tokens();
    let downloads = h.fake.value_fetches();

    h.fake.destroy(&a);
    let d = h.fake.add_message(&h.inbox, "d", "a@example.com", ts(4));
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert!(stats.resynced);
    assert_eq!(stats.destroyed, 1);
    assert_eq!(stats.fetched, 1);
    assert!(row(&pool, &h, &a).await.is_none());
    assert!(row(&pool, &h, &b).await.is_some());
    assert!(row(&pool, &h, &c).await.is_some());
    assert_eq!(placement(&pool, &h, &d).await, serde_json::json!(["Inbox"]));
    assert_eq!(h.fake.value_fetches(), downloads + 1);
    let rows = folder_rows(&pool, &h).await;
    assert!(
        rows.iter()
            .all(|r| r.delta_link.is_some() && !r.resync_pending)
    );

    // Quiet afterwards.
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (0, 0, 0));
    assert!(!stats.resynced);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn delta_link_not_advanced_when_a_later_folder_fails(pool: PgPool) {
    let h = harness(&pool).await;
    let sent = h.fake.folder_id("sentitems");
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let inbox_link_before = folder_rows(&pool, &h)
        .await
        .into_iter()
        .find(|r| r.folder_id == h.inbox)
        .unwrap()
        .delta_link;

    // Inbox (read first) reports a delete; Sent Items (read later) has
    // a new message whose write fails.
    h.fake.destroy(&a);
    let s = h.fake.add_message(&sent, "sent", "a@example.com", ts(2));
    let failing = FailingStore::new(&h.maildir, 0);
    let err = poll_account(
        &pool,
        &h.client,
        &failing,
        None,
        &h.account,
        DeletionPolicy::Mirror,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("injected write failure"));
    assert!(row(&pool, &h, &a).await.is_some(), "phase 2 never ran");
    let inbox_link_after = folder_rows(&pool, &h)
        .await
        .into_iter()
        .find(|r| r.folder_id == h.inbox)
        .unwrap()
        .delta_link;
    assert_eq!(inbox_link_before, inbox_link_after);

    // Rerun with a healthy store: the delete applies exactly once.
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.destroyed), (1, 1));
    assert!(row(&pool, &h, &a).await.is_none());
    assert_eq!(
        placement(&pool, &h, &s).await,
        serde_json::json!(["Sent Items"])
    );
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (0, 0, 0));
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn move_landing_between_folder_reads_is_rescued_by_verify(pool: PgPool) {
    let h = harness(&pool).await;
    let archive = h.fake.folder_id("archive");
    let a = h.fake.add_message(&h.inbox, "a", "a@example.com", ts(1));
    run_backfill(&pool, &h, &BackfillOptions::default()).await;
    let downloads = h.fake.value_fetches();

    // Archive's delta is read before Inbox's; the move lands in between.
    let fired = Arc::new(AtomicBool::new(false));
    let inbox = h.inbox.clone();
    let id = a.clone();
    let archive_id = archive.clone();
    h.fake.on_delta(move |folder, fake| {
        if folder == inbox && !fired.swap(true, Ordering::SeqCst) {
            fake.move_message(&id, &archive_id);
        }
    });
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!(stats.destroyed, 0);
    assert_eq!(stats.updated, 1);
    assert_eq!(
        placement(&pool, &h, &a).await,
        serde_json::json!(["Archive"])
    );
    assert_eq!(h.fake.meta_fetches(), 1);

    // Next poll sees Archive's add as a refresh, never a download.
    let stats = run_poll(&pool, &h, DeletionPolicy::Mirror).await;
    assert_eq!((stats.fetched, stats.updated, stats.destroyed), (0, 1, 0));
    assert_eq!(h.fake.value_fetches(), downloads);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn rotated_refresh_token_is_persisted_and_usable(pool: PgPool) {
    let h = harness(&pool).await;
    // Every request refreshes: the token is always "about to expire".
    h.fake.set_expires_in(30);
    h.fake.invalidate_previous_refresh_tokens(true);
    for day in 1..=3 {
        h.fake
            .add_message(&h.inbox, &format!("m{day}"), "a@example.com", ts(day));
    }
    assert_eq!(
        run_backfill(&pool, &h, &BackfillOptions::default()).await,
        3
    );
    assert!(h.fake.token_refreshes() > 1);

    let account = accounts::get(&pool, h.account.id).await.unwrap().unwrap();
    let persisted = String::from_utf8(sealer().unseal(&account.sealed_token).unwrap()).unwrap();
    assert_eq!(
        Some(&persisted),
        h.fake.refresh_tokens_issued().last(),
        "sealed_token holds the newest refresh token"
    );

    // A fresh client (a new job) built from the persisted token works
    // even though every older token is dead.
    let next_job = make_client(&h.fake, &persisted);
    next_job.get_me().await.unwrap();
}
