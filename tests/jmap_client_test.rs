mod support;

use std::time::Duration;

use arkivo::jmap::{JmapClient, JmapError, RetryPolicy};
use chrono::{TimeZone, Utc};
use support::fake_jmap::FakeJmap;

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 5,
        base_delay: Duration::from_millis(5),
    }
}

async fn connect(fake: &FakeJmap) -> JmapClient {
    JmapClient::connect(&fake.session_url(), fake.token(), fast_retry())
        .await
        .unwrap()
}

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

#[tokio::test]
async fn connect_reads_session_and_limits() {
    let fake = FakeJmap::start().await;
    fake.set_max_objects_in_get(42);
    let client = connect(&fake).await;
    assert_eq!(client.account_id(), support::fake_jmap::ACCOUNT_ID);
    assert_eq!(client.batch_limit(), 42);
}

#[tokio::test]
async fn connect_rejects_bad_token() {
    let fake = FakeJmap::start().await;
    let result = JmapClient::connect(&fake.session_url(), "wrong-token", fast_retry()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn chained_changes_get_is_single_round_trip() {
    let fake = FakeJmap::start().await;
    let base_state = fake.current_state();
    fake.add_message("hello", "alice@example.com", ts(1));
    fake.add_message("world", "bob@example.com", ts(2));

    let client = connect(&fake).await;
    let before = fake.api_calls();
    let page = client.changes(&base_state).await.unwrap();
    assert_eq!(
        fake.api_calls() - before,
        1,
        "changes+get must chain in one round trip"
    );

    assert_eq!(page.changes.created.len(), 2);
    assert_eq!(
        page.created.len(),
        2,
        "created metadata fetched via back-reference"
    );
    assert!(!page.changes.has_more_changes);
    let subjects: Vec<_> = page.created.iter().map(|e| e.subject.as_deref()).collect();
    assert!(subjects.contains(&Some("hello")) && subjects.contains(&Some("world")));
}

#[tokio::test]
async fn changes_reports_updates_and_destroys() {
    let fake = FakeJmap::start().await;
    let m1 = fake.add_message("one", "a@example.com", ts(1));
    let m2 = fake.add_message("two", "a@example.com", ts(2));
    let state = fake.current_state();

    fake.update_message(&m1);
    fake.destroy(&m2);

    let client = connect(&fake).await;
    let page = client.changes(&state).await.unwrap();
    assert_eq!(page.changes.updated, vec![m1.clone()]);
    assert_eq!(page.changes.destroyed, vec![m2]);
    assert_eq!(page.updated.len(), 1);
    assert!(page.updated[0].keywords.contains_key("$seen"));
}

#[tokio::test]
async fn changes_respects_max_objects_and_pages() {
    let fake = FakeJmap::start().await;
    fake.set_max_objects_in_get(2);
    let base_state = fake.current_state();
    for day in 1..=5 {
        fake.add_message(&format!("msg {day}"), "a@example.com", ts(day));
    }

    let client = connect(&fake).await;
    // Walk the change feed exactly as the sync loop will.
    let mut state = base_state;
    let mut seen = 0;
    let mut rounds = 0;
    loop {
        let page = client.changes(&state).await.unwrap();
        assert!(page.changes.created.len() <= 2);
        seen += page.created.len();
        state = page.changes.new_state.clone();
        rounds += 1;
        if !page.changes.has_more_changes {
            break;
        }
        assert!(rounds < 10, "pagination must terminate");
    }
    assert_eq!(seen, 5);
    assert!(rounds >= 3);
}

#[tokio::test]
async fn email_get_batches_by_server_limit() {
    let fake = FakeJmap::start().await;
    fake.set_max_objects_in_get(2);
    let ids: Vec<String> = (1..=5)
        .map(|day| fake.add_message(&format!("m{day}"), "a@example.com", ts(day)))
        .collect();

    let client = connect(&fake).await;
    let before = fake.api_calls();
    let emails = client.email_get(&ids).await.unwrap();
    assert_eq!(emails.len(), 5);
    assert_eq!(fake.api_calls() - before, 3, "5 ids at limit 2 = 3 batches");
}

#[tokio::test]
async fn cannot_calculate_changes_is_mapped() {
    let fake = FakeJmap::start().await;
    let old_state = fake.current_state();
    fake.add_message("x", "a@example.com", ts(1));
    fake.invalidate_state();

    let client = connect(&fake).await;
    match client.changes(&old_state).await {
        Err(JmapError::CannotCalculateChanges) => {}
        other => panic!("expected CannotCalculateChanges, got {other:?}"),
    }
}

#[tokio::test]
async fn rate_limit_backs_off_and_recovers() {
    let fake = FakeJmap::start().await;
    let state = fake.current_state();
    fake.add_message("x", "a@example.com", ts(1));

    let client = connect(&fake).await;
    fake.fail_next(2);
    let page = client.changes(&state).await.unwrap();
    assert_eq!(page.created.len(), 1);
}

#[tokio::test]
async fn rate_limit_gives_up_after_max_retries() {
    let fake = FakeJmap::start().await;
    let state = fake.current_state();
    let client = connect(&fake).await;

    fake.fail_next(100);
    match client.changes(&state).await {
        Err(JmapError::Status(429)) => {}
        other => panic!("expected Status(429), got {other:?}"),
    }
}

#[tokio::test]
async fn query_page_walks_mailbox_in_received_order() {
    let fake = FakeJmap::start().await;
    for day in [3, 1, 5, 2, 4] {
        fake.add_message(&format!("day {day}"), "a@example.com", ts(day));
    }
    let client = connect(&fake).await;

    let mut collected = Vec::new();
    let mut anchor: Option<String> = None;
    loop {
        let page = client.query_page(anchor.as_deref(), 2, None).await.unwrap();
        if page.emails.is_empty() {
            break;
        }
        anchor = page.query.ids.last().cloned();
        collected.extend(page.emails);
        if page.query.ids.len() < 2 {
            break;
        }
    }
    assert_eq!(collected.len(), 5);
    let received: Vec<_> = collected.iter().map(|e| e.received_at).collect();
    let mut sorted = received.clone();
    sorted.sort();
    assert_eq!(received, sorted, "backfill pages oldest-first");
}

#[tokio::test]
async fn query_page_since_filter_limits_results() {
    let fake = FakeJmap::start().await;
    for day in 1..=5 {
        fake.add_message(&format!("day {day}"), "a@example.com", ts(day));
    }
    let client = connect(&fake).await;
    let page = client.query_page(None, 100, Some(ts(4))).await.unwrap();
    assert_eq!(
        page.emails.len(),
        2,
        "only messages received on/after day 4"
    );
}

#[tokio::test]
async fn download_blob_returns_raw_message() {
    let fake = FakeJmap::start().await;
    fake.add_message("raw test", "a@example.com", ts(1));
    let client = connect(&fake).await;

    let page = client.query_page(None, 10, None).await.unwrap();
    let blob_id = &page.emails[0].blob_id;
    let raw = client.download_blob(blob_id).await.unwrap();
    let text = String::from_utf8(raw).unwrap();
    assert!(text.contains("Subject: raw test"));
    assert!(text.contains("Message-ID:"));
}

#[tokio::test]
async fn query_all_ids_pages_through_everything() {
    let fake = FakeJmap::start().await;
    fake.set_max_objects_in_get(2);
    for day in 1..=5 {
        fake.add_message(&format!("m{day}"), "a@example.com", ts(day));
    }
    let client = connect(&fake).await;
    let ids = client.query_all_ids().await.unwrap();
    assert_eq!(ids.len(), 5);
}
