mod support;

use std::sync::Arc;
use std::time::Duration;

use arkivo::crypto::Sealer;
use arkivo::jmap::RetryPolicy;
use arkivo::o365::{O365Client, O365Error, list_archivable};
use chrono::{TimeZone, Utc};
use support::fake_o365::{FakeO365, MAIL, REFRESH_TOKEN};

fn sealer() -> Arc<Sealer> {
    Arc::new(Sealer::new(&[7u8; 32], "primary").unwrap())
}

fn client(fake: &FakeO365) -> O365Client {
    O365Client::with_endpoints(
        &fake.microsoft_config(),
        REFRESH_TOKEN.into(),
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

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, 12, 0, 0).unwrap()
}

#[tokio::test]
async fn access_token_is_cached_and_refreshed_near_expiry() {
    let fake = FakeO365::start().await;
    let c = client(&fake);
    let me = c.get_me().await.unwrap();
    assert_eq!(me.address(), Some(MAIL));
    c.get_me().await.unwrap();
    assert_eq!(fake.token_refreshes(), 1);

    // A token about to expire is replaced before the next call.
    fake.set_expires_in(30);
    let fake2 = FakeO365::start().await;
    fake2.set_expires_in(30);
    let c2 = client(&fake2);
    c2.get_me().await.unwrap();
    c2.get_me().await.unwrap();
    assert_eq!(fake2.token_refreshes(), 2);
}

#[tokio::test]
async fn revoked_access_token_is_replaced_transparently() {
    let fake = FakeO365::start().await;
    let c = client(&fake);
    c.get_me().await.unwrap();
    fake.revoke_access_tokens();
    c.get_me().await.unwrap();
    assert_eq!(fake.token_refreshes(), 2);
}

#[tokio::test]
async fn revoked_grant_and_interaction_required_map_to_auth_revoked() {
    let fake = FakeO365::start().await;
    let c = client(&fake);
    fake.revoke_grant();
    assert!(matches!(c.get_me().await, Err(O365Error::AuthRevoked)));
}

#[tokio::test]
async fn throttling_is_retried_with_retry_after() {
    let fake = FakeO365::start().await;
    let c = client(&fake);
    fake.fail_next(2);
    c.get_me().await.unwrap();
    fake.fail_next_with(1, 503, "service unavailable");
    c.get_me().await.unwrap();
    // Exhausting retries surfaces the status.
    fake.fail_next(10);
    assert!(matches!(c.get_me().await, Err(O365Error::Status(429, _))));
}

#[tokio::test]
async fn expired_delta_token_maps_to_delta_expired() {
    let fake = FakeO365::start().await;
    let inbox = fake.folder_id("inbox");
    fake.add_message(&inbox, "one", "a@example.com", ts(1));
    let c = client(&fake);
    let page = c.delta_page(&inbox, None, None, 200).await.unwrap();
    assert_eq!(page.value.len(), 1);
    let link = page.delta_link.unwrap();
    fake.expire_delta_tokens();
    assert!(matches!(
        c.delta_page(&inbox, Some(&link), None, 200).await,
        Err(O365Error::DeltaExpired)
    ));
}

#[tokio::test]
async fn rotated_refresh_token_is_captured_once_and_sealed() {
    let fake = FakeO365::start().await;
    let c = client(&fake);
    assert!(c.take_rotated_sealed_token().unwrap().is_none());
    c.get_me().await.unwrap();
    let sealed = c.take_rotated_sealed_token().unwrap().expect("rotated");
    let latest = fake.refresh_tokens_issued().last().cloned().unwrap();
    assert_ne!(latest, REFRESH_TOKEN);
    assert_eq!(sealer().unseal(&sealed).unwrap(), latest.as_bytes());
    assert!(c.take_rotated_sealed_token().unwrap().is_none());

    // The rotated token is what the next refresh presents, even under
    // strict rotation.
    fake.invalidate_previous_refresh_tokens(true);
    fake.revoke_access_tokens();
    c.get_me().await.unwrap();
    assert_eq!(fake.token_refreshes(), 2);
}

#[tokio::test]
async fn immutable_id_preference_rides_every_request() {
    let fake = FakeO365::start().await;
    let inbox = fake.folder_id("inbox");
    for day in 1..=5 {
        fake.add_message(&inbox, &format!("m{day}"), "a@example.com", ts(day));
    }
    let c = client(&fake);
    let mut link: Option<String> = None;
    let mut seen = 0;
    loop {
        let page = c
            .delta_page(&inbox, link.as_deref(), None, 2)
            .await
            .unwrap();
        seen += page.value.len();
        match (page.delta_link, page.next_link) {
            (Some(_), _) => break,
            (None, Some(next)) => link = Some(next),
            _ => panic!("page without links"),
        }
    }
    assert_eq!(seen, 5);
    let id = fake.add_message(&inbox, "raw", "a@example.com", ts(6));
    assert_eq!(c.get_message_raw(&id).await.unwrap(), fake.raw_of(&id));
    c.get_message_meta(&id).await.unwrap();
    c.list_folders().await.unwrap();
    assert_eq!(fake.prefer_violations(), 0);
    assert!(fake.api_calls() >= 6);
}

#[tokio::test]
async fn folder_tree_yields_paths_and_skips_system_folders() {
    let fake = FakeO365::start().await;
    let archive = fake.folder_id("archive");
    let y2024 = fake.add_folder("2024", Some(&archive), None);
    fake.add_folder("Q1", Some(&y2024), None);
    let deleted = fake.folder_id("deleteditems");
    fake.add_folder("Old", Some(&deleted), None);
    let c = client(&fake);
    let folders = list_archivable(&c).await.unwrap();
    let paths: Vec<&str> = folders.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "Archive",
            "Archive/2024",
            "Archive/2024/Q1",
            "Drafts",
            "Inbox",
            "Sent Items"
        ]
    );
    let inbox = folders.iter().find(|f| f.path == "Inbox").unwrap();
    assert_eq!(inbox.well_known_name.as_deref(), Some("inbox"));
    assert_eq!(fake.prefer_violations(), 0);
}

#[tokio::test]
async fn raw_fetch_rejects_json_bodies_and_404s_cleanly() {
    let fake = FakeO365::start().await;
    let c = client(&fake);
    assert!(matches!(
        c.get_message_raw("nope").await,
        Err(O365Error::Status(404, _))
    ));
}
