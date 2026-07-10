mod support;

use std::time::Duration;

use arkivo::gmail::{GmailClient, GmailError};
use arkivo::jmap::RetryPolicy;
use chrono::{TimeZone, Utc};
use support::fake_gmail::{EMAIL, FakeGmail, REFRESH_TOKEN};

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 5,
        base_delay: Duration::from_millis(5),
    }
}

fn client(fake: &FakeGmail) -> GmailClient {
    GmailClient::with_endpoints(
        &fake.google_config(),
        REFRESH_TOKEN.into(),
        fast_retry(),
        &fake.token_url(),
        &fake.api_base(),
    )
    .unwrap()
}

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

#[tokio::test]
async fn access_token_is_cached_across_calls() {
    let fake = FakeGmail::start().await;
    let client = client(&fake);

    let profile = client.get_profile().await.unwrap();
    assert_eq!(profile.email_address, EMAIL);
    client.get_profile().await.unwrap();

    assert_eq!(
        fake.token_refreshes(),
        1,
        "second API call must reuse the cached access token"
    );
}

#[tokio::test]
async fn expiring_token_is_refreshed() {
    let fake = FakeGmail::start().await;
    // Below the client's 60s expiry margin: every call refreshes.
    fake.set_expires_in(30);
    let client = client(&fake);

    client.get_profile().await.unwrap();
    client.get_profile().await.unwrap();
    assert_eq!(fake.token_refreshes(), 2);
}

#[tokio::test]
async fn revoked_access_token_is_replaced_transparently() {
    let fake = FakeGmail::start().await;
    let client = client(&fake);
    client.get_profile().await.unwrap();

    fake.revoke_access_tokens();
    let profile = client.get_profile().await.unwrap();
    assert_eq!(profile.email_address, EMAIL);
    assert_eq!(fake.token_refreshes(), 2, "401 must trigger a re-refresh");
}

#[tokio::test]
async fn revoked_grant_surfaces_auth_revoked() {
    let fake = FakeGmail::start().await;
    fake.revoke_grant();
    let client = client(&fake);
    let err = client.get_profile().await.unwrap_err();
    assert!(matches!(err, GmailError::AuthRevoked), "got {err:?}");
}

#[tokio::test]
async fn rate_limit_429_is_retried() {
    let fake = FakeGmail::start().await;
    fake.add_message("hello", "alice@example.com", ts(1), &["INBOX"]);
    let client = client(&fake);

    fake.fail_next(2);
    let page = client.list_messages(None, None, 100).await.unwrap();
    assert_eq!(page.messages.len(), 1);
}

#[tokio::test]
async fn quota_403_is_retried_but_plain_403_is_not() {
    let fake = FakeGmail::start().await;
    let client = client(&fake);
    // Prime the token so the scripted failure hits the API call itself.
    client.get_profile().await.unwrap();

    fake.fail_next_with(
        1,
        403,
        r#"{"error": {"errors": [{"reason": "userRateLimitExceeded"}]}}"#,
    );
    client.get_profile().await.unwrap();

    fake.fail_next_with(1, 403, r#"{"error": {"errors": [{"reason": "forbidden"}]}}"#);
    let err = client.get_profile().await.unwrap_err();
    assert!(matches!(err, GmailError::Status(403, _)), "got {err:?}");
}

#[tokio::test]
async fn raw_message_round_trips() {
    let fake = FakeGmail::start().await;
    let id = fake.add_message("hello", "alice@example.com", ts(1), &["INBOX", "UNREAD"]);
    let client = client(&fake);

    let m = client.get_message_raw(&id).await.unwrap();
    let raw = m.raw_bytes().unwrap();
    let text = String::from_utf8(raw).unwrap();
    assert!(text.contains("Subject: hello"));
    assert_eq!(m.label_ids, vec!["INBOX", "UNREAD"]);
    assert_eq!(m.internal_date, Some(ts(1).timestamp_millis()));

    let meta = client.get_message_metadata(&id).await.unwrap();
    assert!(meta.raw.is_none());
    assert!(meta.raw_bytes().is_err());
}

#[tokio::test]
async fn list_excludes_spam_and_trash_and_pages_newest_first() {
    let fake = FakeGmail::start().await;
    fake.add_message("oldest", "a@example.com", ts(1), &["INBOX"]);
    fake.add_message("spam", "s@example.com", ts(2), &["SPAM"]);
    fake.add_message("middle", "a@example.com", ts(3), &["INBOX"]);
    fake.add_message("trash", "t@example.com", ts(4), &["TRASH"]);
    fake.add_message("newest", "a@example.com", ts(5), &["INBOX"]);
    let client = client(&fake);

    let first = client.list_messages(None, None, 2).await.unwrap();
    assert_eq!(first.messages.len(), 2);
    let token = first.next_page_token.clone().expect("more pages");
    let second = client
        .list_messages(None, Some(&token), 2)
        .await
        .unwrap();
    assert_eq!(second.messages.len(), 1, "spam/trash never listed");
    assert!(second.next_page_token.is_none());
}

#[tokio::test]
async fn history_expired_maps_to_typed_error() {
    let fake = FakeGmail::start().await;
    let stale = fake.current_history_id();
    fake.invalidate_history();
    let client = client(&fake);

    let err = client.list_history(&stale, None).await.unwrap_err();
    assert!(matches!(err, GmailError::HistoryExpired), "got {err:?}");
}

#[tokio::test]
async fn history_reports_adds_deletes_and_label_changes() {
    let fake = FakeGmail::start().await;
    let m1 = fake.add_message("one", "a@example.com", ts(1), &["INBOX", "UNREAD"]);
    let start = fake.current_history_id();

    let m2 = fake.add_message("two", "a@example.com", ts(2), &["INBOX"]);
    fake.change_labels(&m1, &[], &["UNREAD"]);
    fake.destroy(&m2);

    let client = client(&fake);
    let page = client.list_history(&start, None).await.unwrap();
    assert!(page.next_page_token.is_none());
    assert_eq!(page.history_id.as_deref(), Some(fake.current_history_id().as_str()));

    let added: Vec<_> = page
        .history
        .iter()
        .flat_map(|r| &r.messages_added)
        .map(|m| m.message.id.as_str())
        .collect();
    let deleted: Vec<_> = page
        .history
        .iter()
        .flat_map(|r| &r.messages_deleted)
        .map(|m| m.message.id.as_str())
        .collect();
    let relabeled: Vec<_> = page
        .history
        .iter()
        .flat_map(|r| &r.labels_removed)
        .map(|l| l.message.id.as_str())
        .collect();
    assert_eq!(added, vec![m2.as_str()]);
    assert_eq!(deleted, vec![m2.as_str()]);
    assert_eq!(relabeled, vec![m1.as_str()]);
}
