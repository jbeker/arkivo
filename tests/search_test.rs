//! Integration tests against a real OpenSearch (env-gated: set
//! ARKIVO_TEST_OPENSEARCH_URL, e.g. via docker/compose.dev.yaml).
//! Each test uses process-unique user ids so runs don't collide.

use std::sync::atomic::{AtomicI64, Ordering};

use arkivo::config::OpenSearchConfig;
use arkivo::db::messages::Message;
use arkivo::embed::{EmbeddingProvider, FakeEmbedder};
use arkivo::extract::ExtractedEmail;
use arkivo::search::SearchClient;
use arkivo::search::client::SearchFilter;
use arkivo::search::hybrid::hybrid_search;
use arkivo::search::indexer::Indexer;
use chrono::{TimeZone, Utc};

static NEXT_USER: AtomicI64 = AtomicI64::new(0);

fn test_user_id() -> i64 {
    // Unique across concurrent runs: pid in the high bits, counter low.
    let base = 900_000_000 + (std::process::id() as i64 % 90_000) * 1_000;
    base + NEXT_USER.fetch_add(1, Ordering::SeqCst)
}

fn search_client() -> Option<SearchClient> {
    let url = std::env::var("ARKIVO_TEST_OPENSEARCH_URL").ok()?;
    Some(
        SearchClient::new(&OpenSearchConfig {
            url,
            username: None,
            password: None,
        })
        .unwrap(),
    )
}

macro_rules! require_opensearch {
    () => {
        match search_client() {
            Some(c) => c,
            None => {
                eprintln!("skipping: ARKIVO_TEST_OPENSEARCH_URL not set");
                return;
            }
        }
    };
}

fn message(id: i64, subject: &str) -> Message {
    Message {
        id,
        mail_account_id: 1,
        jmap_email_id: format!("m{id}"),
        blob_id: format!("blob-{id}"),
        message_id_hdr: Some(format!("<m{id}@example.com>")),
        thread_id: Some("t1".into()),
        received_at: Utc.with_ymd_and_hms(2019, 5, 1, 12, 0, 0).unwrap(),
        size: 1000,
        has_attachments: false,
        from_addr: Some("alice@example.com".into()),
        subject: Some(subject.into()),
        mailbox_ids: serde_json::json!(["inbox"]),
        keywords: serde_json::json!({}),
        maildir_path: Some(format!("new/m{id}")),
        fetched_at: None,
        indexed_at: None,
        index_status: "staged".into(),
        pipeline_version: None,
        error: None,
        deleted_at: None,
    }
}

fn email(subject: &str, body: &str) -> ExtractedEmail {
    ExtractedEmail {
        subject: Some(subject.into()),
        from: vec!["alice@example.com".into()],
        to: vec!["owner@example.com".into()],
        cc: vec![],
        body_text: body.into(),
    }
}

/// Like `email`, but with explicit sender/recipient addresses for
/// structured-filter tests.
fn email_addrs(subject: &str, body: &str, from: &str, to: &str, cc: &str) -> ExtractedEmail {
    ExtractedEmail {
        subject: Some(subject.into()),
        from: vec![from.into()],
        to: vec![to.into()],
        cc: if cc.is_empty() {
            vec![]
        } else {
            vec![cc.into()]
        },
        body_text: body.into(),
    }
}

#[tokio::test]
async fn search_on_missing_indices_returns_empty() {
    // A fresh account that has never been promoted has no indices; the
    // MCP tools must return empty rather than an index_not_found error.
    let search = require_opensearch!();
    let user_id = test_user_id(); // no ensure_user_indices — indices absent
    let embedder = FakeEmbedder::default();

    let page = search
        .bm25_search(user_id, "anything", 10, &SearchFilter::default())
        .await
        .unwrap();
    assert!(page.hits.is_empty());
    assert_eq!(page.total, 0);
    let vector = embedder.embed_query("anything").await.unwrap();
    assert!(
        search
            .knn_search(user_id, &vector, 10, &SearchFilter::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        search
            .facets(
                user_id,
                arkivo::search::client::FacetKind::Year,
                20,
                None,
                &SearchFilter::default(),
            )
            .await
            .unwrap()
            .is_empty()
    );
    let page = search
        .list_messages(user_id, None, &SearchFilter::default(), 10, 0, false)
        .await
        .unwrap();
    assert!(page.hits.is_empty());
    assert!(
        search
            .thread_messages(user_id, "t1", 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(search.get_msg_doc(user_id, 1).await.unwrap().is_none());
}

#[tokio::test]
async fn index_and_search_bm25_and_knn() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    let body_a = "The quarterly budget review covers travel reimbursement policy.";
    let body_b = "Recipe for sourdough bread with a long cold ferment.";
    indexer
        .index_message(
            user_id,
            &message(1, "budget review"),
            &email("budget review", body_a),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user_id,
            &message(2, "bread recipe"),
            &email("bread recipe", body_b),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    // BM25 finds the budget message by keyword, with an exact total and
    // a plain-text highlight fragment containing the query term.
    let page = search
        .bm25_search(user_id, "reimbursement", 10, &SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    let hits = page.hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, "1");
    let highlight = hits[0].highlight.as_deref().expect("highlight fragment");
    assert!(highlight.contains("reimbursement"));
    assert!(!highlight.contains("<em>"), "plain text, no tags");

    // kNN with the exact chunk text returns that chunk first
    // (FakeEmbedder: identical text = identical vector).
    let vector = embedder.embed_query(body_b).await.unwrap();
    let hits = search
        .knn_search(user_id, &vector, 5, &SearchFilter::default())
        .await
        .unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].message_id, "2");

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn hybrid_fuses_across_both_indices() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    let body = "Discussion about the kayak trip logistics and gear list.";
    indexer
        .index_message(
            user_id,
            &message(10, "kayak trip"),
            &email("kayak trip", body),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user_id,
            &message(11, "unrelated"),
            &email("unrelated", "Totally different content about taxes."),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    let results = hybrid_search(
        &search,
        &embedder,
        user_id,
        body,
        5,
        0,
        &SearchFilter::default(),
    )
    .await
    .unwrap()
    .results;
    assert!(!results.is_empty());
    assert_eq!(
        results[0].message_id, "10",
        "keyword+vector agreement must win"
    );
    assert!(
        results.iter().all(|r| r.msg_source.is_some()),
        "every fused result is hydrated with msg metadata"
    );

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn reindex_overwrite_is_idempotent_and_delete_removes_docs() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    let msg = message(20, "dup test");
    let content = email("dup test", "Same message indexed twice.");
    indexer
        .index_message(user_id, &msg, &content, true)
        .await
        .unwrap();
    indexer
        .index_message(user_id, &msg, &content, true)
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    let hits = search
        .bm25_search(user_id, "indexed twice", 10, &SearchFilter::default())
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 1, "doc-id upsert, not duplication");

    search.delete_message_docs(user_id, msg.id).await.unwrap();
    search.refresh_user_indices(user_id).await.unwrap();
    let hits = search
        .bm25_search(user_id, "indexed twice", 10, &SearchFilter::default())
        .await
        .unwrap()
        .hits;
    assert!(hits.is_empty());
    assert!(search.get_msg_doc(user_id, msg.id).await.unwrap().is_none());

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn user_indices_are_isolated() {
    let search = require_opensearch!();
    let user_a = test_user_id();
    let user_b = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_a, embedder.dimension())
        .await
        .unwrap();
    search
        .ensure_user_indices(user_b, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    indexer
        .index_message(
            user_a,
            &message(30, "secret alpha"),
            &email("secret alpha", "Alpha-only content."),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_a).await.unwrap();

    let hits = search
        .bm25_search(user_b, "alpha", 10, &SearchFilter::default())
        .await
        .unwrap()
        .hits;
    assert!(hits.is_empty(), "user B must never see user A's documents");
    assert!(
        search
            .thread_messages(user_b, "t1", 10)
            .await
            .unwrap()
            .is_empty(),
        "threads are user-scoped too"
    );

    search.delete_user_indices(user_a).await.unwrap();
    search.delete_user_indices(user_b).await.unwrap();
}

#[tokio::test]
async fn structured_filter_constrains_bm25_knn_and_hybrid() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    // Two messages with *identical* bodies (so both are equally strong
    // BM25 and kNN hits) but different senders — only the filter can tell
    // them apart. This proves the filter reaches the kNN half, not just BM25.
    let body = "The migration runbook covers cutover, rollback, and verification.";
    indexer
        .index_message(
            user_id,
            &message(40, "runbook"),
            &email_addrs(
                "runbook",
                body,
                "alice@example.com",
                "owner@example.com",
                "",
            ),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user_id,
            &message(41, "runbook"),
            &email_addrs("runbook", body, "bob@example.com", "owner@example.com", ""),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    let from_alice = SearchFilter {
        from: Some("alice@example.com".into()),
        ..Default::default()
    };

    // BM25: only alice's message survives the filter.
    let hits = search
        .bm25_search(user_id, "migration runbook", 10, &from_alice)
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, "40");

    // kNN: the exact chunk vector matches both, but the filter pins it to alice.
    let vector = embedder.embed_query(body).await.unwrap();
    let hits = search
        .knn_search(user_id, &vector, 10, &from_alice)
        .await
        .unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits.iter().all(|h| h.message_id == "40"),
        "filtered kNN must exclude bob's message"
    );

    // Hybrid: fused result set is constrained to alice.
    let results = hybrid_search(&search, &embedder, user_id, body, 5, 0, &from_alice)
        .await
        .unwrap()
        .results;
    assert!(!results.is_empty());
    assert!(results.iter().all(|r| r.message_id == "40"));

    // A non-matching sender yields nothing.
    let from_carol = SearchFilter {
        from: Some("carol@example.com".into()),
        ..Default::default()
    };
    assert!(
        search
            .bm25_search(user_id, "migration runbook", 10, &from_carol)
            .await
            .unwrap()
            .hits
            .is_empty()
    );

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn structured_filter_fields_combine_with_and() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    let body = "Invoice for the consulting engagement, net 30 terms.";
    indexer
        .index_message(
            user_id,
            &message(50, "invoice"),
            &email_addrs(
                "invoice",
                body,
                "alice@example.com",
                "owner@example.com",
                "",
            ),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    // from AND to both match → hit.
    let both_match = SearchFilter {
        from: Some("alice@example.com".into()),
        to: Some("owner@example.com".into()),
        ..Default::default()
    };
    let hits = search
        .bm25_search(user_id, "invoice", 10, &both_match)
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, "50");

    // from matches but to does not → AND excludes it.
    let to_mismatch = SearchFilter {
        from: Some("alice@example.com".into()),
        to: Some("someone-else@example.com".into()),
        ..Default::default()
    };
    assert!(
        search
            .bm25_search(user_id, "invoice", 10, &to_mismatch)
            .await
            .unwrap()
            .hits
            .is_empty(),
        "AND semantics: a single mismatched field excludes the message"
    );

    search.delete_user_indices(user_id).await.unwrap();
}

/// Message with a controllable received_at, for date filter/sort tests.
fn message_at(id: i64, subject: &str, year: i32, month: u32) -> Message {
    let mut msg = message(id, subject);
    msg.received_at = Utc.with_ymd_and_hms(year, month, 15, 12, 0, 0).unwrap();
    msg.thread_id = Some(format!("t{id}"));
    msg
}

#[tokio::test]
async fn date_filter_constrains_bm25_knn_and_listing() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    // Identical bodies in different years: only the date range can tell
    // them apart, proving the filter reaches both index halves.
    let body = "Annual insurance renewal notice with premium details.";
    indexer
        .index_message(
            user_id,
            &message_at(60, "renewal", 2018, 3),
            &email("renewal", body),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user_id,
            &message_at(61, "renewal", 2021, 3),
            &email("renewal", body),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    let only_2021 = SearchFilter {
        received_after: Some("2021-01-01T00:00:00Z".into()),
        received_before: Some("2021-12-31T23:59:59.999Z".into()),
        ..Default::default()
    };

    let page = search
        .bm25_search(user_id, "insurance renewal", 10, &only_2021)
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.hits[0].message_id, "61");

    let vector = embedder.embed_query(body).await.unwrap();
    let hits = search
        .knn_search(user_id, &vector, 10, &only_2021)
        .await
        .unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits.iter().all(|h| h.message_id == "61"),
        "date filter must reach the kNN half (chunk received_at, pipeline v3)"
    );

    // Query-less listing with the same filter, newest first.
    let page = search
        .list_messages(user_id, None, &only_2021, 10, 0, false)
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.hits[0].message_id, "61");

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn listing_sorts_by_date_and_paginates_with_totals() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    for (id, year) in [(70, 2015), (71, 2019), (72, 2023)] {
        indexer
            .index_message(
                user_id,
                &message_at(id, "status update", year, 6),
                &email("status update", "Monthly project status update."),
                true,
            )
            .await
            .unwrap();
    }
    search.refresh_user_indices(user_id).await.unwrap();

    // Newest first.
    let page = search
        .list_messages(user_id, None, &SearchFilter::default(), 2, 0, false)
        .await
        .unwrap();
    assert_eq!(page.total, 3);
    let ids: Vec<&str> = page.hits.iter().map(|h| h.message_id.as_str()).collect();
    assert_eq!(ids, vec!["72", "71"]);

    // Second page via offset.
    let page = search
        .list_messages(user_id, None, &SearchFilter::default(), 2, 2, false)
        .await
        .unwrap();
    assert_eq!(page.total, 3);
    let ids: Vec<&str> = page.hits.iter().map(|h| h.message_id.as_str()).collect();
    assert_eq!(ids, vec!["70"]);

    // Oldest first flips the order.
    let page = search
        .list_messages(user_id, None, &SearchFilter::default(), 3, 0, true)
        .await
        .unwrap();
    let ids: Vec<&str> = page.hits.iter().map(|h| h.message_id.as_str()).collect();
    assert_eq!(ids, vec!["70", "71", "72"]);

    // With a query, listing still date-sorts and highlights.
    let page = search
        .list_messages(
            user_id,
            Some("status"),
            &SearchFilter::default(),
            3,
            0,
            false,
        )
        .await
        .unwrap();
    assert_eq!(page.hits[0].message_id, "72");
    assert!(
        page.hits[0]
            .highlight
            .as_deref()
            .unwrap()
            .contains("status")
    );

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn thread_messages_returns_conversation_oldest_first() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    for (id, month, body) in [
        (80, 2, "First message in the trip thread."),
        (81, 3, "Reply with hotel options."),
        (82, 4, "Final itinerary confirmation."),
    ] {
        let mut msg = message_at(id, "trip planning", 2020, month);
        msg.thread_id = Some("trip-thread".into());
        indexer
            .index_message(user_id, &msg, &email("trip planning", body), true)
            .await
            .unwrap();
    }
    // An unrelated thread must not leak in.
    indexer
        .index_message(
            user_id,
            &message_at(83, "other", 2020, 3),
            &email("other", "Unrelated conversation."),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    let docs = search
        .thread_messages(user_id, "trip-thread", 50)
        .await
        .unwrap();
    let ids: Vec<&str> = docs
        .iter()
        .map(|d| d.get("message_id").unwrap().as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["80", "81", "82"], "date order, thread-scoped");

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn facets_scope_to_query_and_filters() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    indexer
        .index_message(
            user_id,
            &message_at(90, "invoice march", 2020, 3),
            &email_addrs(
                "invoice march",
                "Invoice attached for March.",
                "vendor@example.com",
                "owner@example.com",
                "",
            ),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user_id,
            &message_at(91, "newsletter", 2020, 4),
            &email_addrs(
                "newsletter",
                "Monthly community newsletter.",
                "news@example.com",
                "owner@example.com",
                "",
            ),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_id).await.unwrap();

    use arkivo::search::client::FacetKind;

    // Unscoped: both senders appear.
    let all = search
        .facets(user_id, FacetKind::From, 20, None, &SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 2);

    // Scoped to a query: only the invoice sender remains.
    let scoped = search
        .facets(
            user_id,
            FacetKind::From,
            20,
            Some("invoice"),
            &SearchFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].value, "vendor@example.com");

    // Month histogram with a date filter.
    let months = search
        .facets(
            user_id,
            FacetKind::Month,
            20,
            None,
            &SearchFilter {
                received_after: Some("2020-04-01T00:00:00Z".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(months.len(), 1);
    assert_eq!(months[0].value, "2020-04");

    search.delete_user_indices(user_id).await.unwrap();
}

#[tokio::test]
async fn hybrid_pagination_skips_and_reports_has_more() {
    let search = require_opensearch!();
    let user_id = test_user_id();
    let embedder = FakeEmbedder::default();
    search
        .ensure_user_indices(user_id, embedder.dimension())
        .await
        .unwrap();

    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    for id in 100..105 {
        indexer
            .index_message(
                user_id,
                &message_at(id, "gardening notes", 2020, 5),
                &email(
                    "gardening notes",
                    &format!("Gardening notes entry number {id} about tomatoes."),
                ),
                true,
            )
            .await
            .unwrap();
    }
    search.refresh_user_indices(user_id).await.unwrap();

    let first = hybrid_search(
        &search,
        &embedder,
        user_id,
        "gardening tomatoes",
        2,
        0,
        &SearchFilter::default(),
    )
    .await
    .unwrap();
    assert_eq!(first.results.len(), 2);
    assert!(first.has_more);
    assert_eq!(first.total, 5);

    let second = hybrid_search(
        &search,
        &embedder,
        user_id,
        "gardening tomatoes",
        2,
        2,
        &SearchFilter::default(),
    )
    .await
    .unwrap();
    assert_eq!(second.results.len(), 2);
    let first_ids: Vec<_> = first.results.iter().map(|r| &r.message_id).collect();
    assert!(
        second
            .results
            .iter()
            .all(|r| !first_ids.contains(&&r.message_id)),
        "pages must not overlap"
    );

    let last = hybrid_search(
        &search,
        &embedder,
        user_id,
        "gardening tomatoes",
        2,
        4,
        &SearchFilter::default(),
    )
    .await
    .unwrap();
    assert_eq!(last.results.len(), 1);
    assert!(!last.has_more);

    search.delete_user_indices(user_id).await.unwrap();
}
