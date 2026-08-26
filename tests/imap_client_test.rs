mod support;

use arkivo::imap::{ImapClient, ImapError, SpecialUse, TlsMode};
use chrono::{TimeZone, Utc};
use support::fake_imap::{FakeImap, PASSWORD, USERNAME};

async fn connect(fake: &FakeImap) -> ImapClient {
    ImapClient::connect(&fake.host(), fake.port(), TlsMode::None, USERNAME, PASSWORD)
        .await
        .unwrap()
}

#[tokio::test]
async fn login_failure_is_auth_error() {
    let fake = FakeImap::start().await;
    let err = ImapClient::connect(&fake.host(), fake.port(), TlsMode::None, USERNAME, "wrong")
        .await
        .unwrap_err();
    assert!(matches!(err, ImapError::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn connect_failure_is_connect_error() {
    // Port 1 on localhost is refused immediately.
    let err = ImapClient::connect("127.0.0.1", 1, TlsMode::None, USERNAME, PASSWORD)
        .await
        .unwrap_err();
    assert!(matches!(err, ImapError::Connect(_)), "got {err:?}");
}

#[tokio::test]
async fn list_folders_reports_special_use_and_quoting() {
    let fake = FakeImap::start().await;
    fake.add_folder("Rubbish Bin", Some("\\Trash"));
    fake.add_folder("Archive", None);
    let client = connect(&fake).await;

    let folders = client.list_folders().await.unwrap();
    let trash = folders.iter().find(|f| f.name == "Rubbish Bin").unwrap();
    assert_eq!(trash.special_use, Some(SpecialUse::Trash));
    assert!(trash.selectable);
    assert!(folders.iter().any(|f| f.name == "INBOX"));
    assert!(folders.iter().any(|f| f.name == "Archive"));
}

#[tokio::test]
async fn examine_and_uid_list_roundtrip() {
    let fake = FakeImap::start().await;
    let date = Utc.with_ymd_and_hms(2020, 1, 5, 12, 0, 0).unwrap();
    fake.add_message("INBOX", "hello", "a@example.com", date, &["\\Seen"]);
    fake.add_message("INBOX", "world", "a@example.com", date, &[]);
    let client = connect(&fake).await;

    let status = client.examine("INBOX").await.unwrap();
    assert_eq!(status.exists, 2);
    assert_eq!(status.uidvalidity, 1000);
    assert_eq!(status.uidnext, 3);

    let entries = client.uid_list(status.exists).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].uid, 1);
    assert_eq!(entries[0].flags, vec!["\\Seen".to_string()]);
    assert!(entries[1].flags.is_empty());
}

#[tokio::test]
async fn header_peek_and_raw_fetch() {
    let fake = FakeImap::start().await;
    let date = Utc.with_ymd_and_hms(2020, 1, 5, 12, 0, 0).unwrap();
    let uid1 = fake.add_message("INBOX", "with id", "a@example.com", date, &[]);
    let uid2 = fake.add_message_with_msgid("INBOX", "anon", "a@example.com", date, &[], None);
    let client = connect(&fake).await;
    client.examine("INBOX").await.unwrap();

    let headers = client
        .fetch_message_id_headers(&[uid1, uid2])
        .await
        .unwrap();
    let with_id = headers[&uid1].as_deref().unwrap();
    assert!(with_id.starts_with('<') && with_id.ends_with('>'));
    assert!(headers[&uid2].is_none(), "no Message-ID header");

    let raw = client.fetch_raw(uid1).await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&raw.raw).contains("Subject: with id"));
    assert_eq!(raw.internal_date.unwrap(), date);

    assert!(
        client.fetch_raw(999).await.unwrap().is_none(),
        "vanished uid"
    );
    assert_eq!(
        fake.body_fetch_count(),
        1,
        "header peek is not a body fetch"
    );
}

#[tokio::test]
async fn uid_search_since_filters_by_date() {
    let fake = FakeImap::start().await;
    let old = Utc.with_ymd_and_hms(2019, 6, 1, 12, 0, 0).unwrap();
    let new = Utc.with_ymd_and_hms(2020, 2, 1, 12, 0, 0).unwrap();
    fake.add_message("INBOX", "old", "a@example.com", old, &[]);
    let uid_new = fake.add_message("INBOX", "new", "a@example.com", new, &[]);
    let client = connect(&fake).await;
    client.examine("INBOX").await.unwrap();

    let since = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let uids = client.uid_search_since(since).await.unwrap();
    assert_eq!(uids, vec![uid_new]);
}
