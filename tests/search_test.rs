//! Integration tests against a real OpenSearch (env-gated: set
//! ARKIVO_TEST_OPENSEARCH_URL, e.g. via docker/compose.dev.yaml).
//! Each test uses process-unique user ids so runs don't collide.

use std::sync::atomic::{AtomicI64, Ordering};

use arkivo::config::OpenSearchConfig;
use arkivo::db::messages::Message;
use arkivo::embed::{EmbeddingProvider, FakeEmbedder};
use arkivo::extract::ExtractedEmail;
use arkivo::search::SearchClient;
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

#[tokio::test]
async fn search_on_missing_indices_returns_empty() {
    // A fresh account that has never been promoted has no indices; the
    // MCP tools must return empty rather than an index_not_found error.
    let search = require_opensearch!();
    let user_id = test_user_id(); // no ensure_user_indices — indices absent
    let embedder = FakeEmbedder::default();

    assert!(
        search
            .bm25_search(user_id, "anything", 10)
            .await
            .unwrap()
            .is_empty()
    );
    let vector = embedder.embed_query("anything").await.unwrap();
    assert!(
        search
            .knn_search(user_id, &vector, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        search
            .facets(user_id, arkivo::search::client::FacetKind::Year, 20)
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

    // BM25 finds the budget message by keyword.
    let hits = search
        .bm25_search(user_id, "reimbursement", 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, "1");

    // kNN with the exact chunk text returns that chunk first
    // (FakeEmbedder: identical text = identical vector).
    let vector = embedder.embed_query(body_b).await.unwrap();
    let hits = search.knn_search(user_id, &vector, 5).await.unwrap();
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

    let results = hybrid_search(&search, &embedder, user_id, body, 5)
        .await
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(
        results[0].message_id, "10",
        "keyword+vector agreement must win"
    );
    assert!(results[0].chunk_snippet.is_some() || results[0].msg_source.is_some());

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
        .bm25_search(user_id, "indexed twice", 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "doc-id upsert, not duplication");

    search.delete_message_docs(user_id, msg.id).await.unwrap();
    search.refresh_user_indices(user_id).await.unwrap();
    let hits = search
        .bm25_search(user_id, "indexed twice", 10)
        .await
        .unwrap();
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

    let hits = search.bm25_search(user_b, "alpha", 10).await.unwrap();
    assert!(hits.is_empty(), "user B must never see user A's documents");

    search.delete_user_indices(user_a).await.unwrap();
    search.delete_user_indices(user_b).await.unwrap();
}
