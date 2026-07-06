//! MCP server tests: the tenancy contract (token A can never see user
//! B's data), token lifecycle, and audit logging — exercised over real
//! HTTP with the JSON-RPC framing an MCP client uses. OpenSearch is
//! env-gated as in search_test.rs.

use std::sync::Arc;

use arkivo::config::OpenSearchConfig;
use arkivo::db::{audit, messages::Message, tokens, users};
use arkivo::embed::{EmbeddingProvider, FakeEmbedder};
use arkivo::extract::ExtractedEmail;
use arkivo::search::SearchClient;
use arkivo::search::indexer::Indexer;
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;

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

async fn unique_user(pool: &PgPool, handle: &str) -> i64 {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let unique_id = 700_000_000
        + (std::process::id() as i64 % 90_000) * 1_000
        + NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "alter table users alter column id restart with {unique_id}"
    )))
    .execute(pool)
    .await
    .unwrap();
    users::create(pool, handle, "user").await.unwrap().id
}

fn msg(id: i64, subject: &str) -> Message {
    Message {
        id,
        mail_account_id: 1,
        jmap_email_id: format!("m{id}"),
        blob_id: format!("b{id}"),
        message_id_hdr: None,
        thread_id: None,
        received_at: Utc.with_ymd_and_hms(2018, 3, 1, 0, 0, 0).unwrap(),
        size: 100,
        has_attachments: false,
        from_addr: Some("sender@example.com".into()),
        subject: Some(subject.into()),
        mailbox_ids: json!(["inbox"]),
        keywords: json!({}),
        maildir_path: Some("new/x".into()),
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
        from: vec!["sender@example.com".into()],
        to: vec!["owner@example.com".into()],
        cc: vec![],
        body_text: body.into(),
    }
}

fn email_from(subject: &str, body: &str, from: &str) -> ExtractedEmail {
    ExtractedEmail {
        from: vec![from.into()],
        ..email(subject, body)
    }
}

struct McpHarness {
    base: String,
    http: reqwest::Client,
}

impl McpHarness {
    async fn start(pool: PgPool, search: SearchClient) -> Self {
        let app = arkivo::mcp::app(
            pool,
            Arc::new(search),
            Arc::new(FakeEmbedder::default()),
            vec![],
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            base: format!("http://{addr}"),
            http: reqwest::Client::new(),
        }
    }

    async fn rpc(&self, token: Option<&str>, body: Value) -> (u16, Value) {
        let mut request = self
            .http
            .post(format!("{}/mcp", self.base))
            .header("accept", "application/json, text/event-stream")
            .json(&body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let value = response.json().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn call_tool(&self, token: &str, name: &str, args: Value) -> (u16, Value) {
        self.rpc(
            Some(token),
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": args},
            }),
        )
        .await
    }
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn tools_list_requires_auth_and_shows_three_tools(pool: PgPool) {
    let search = require_opensearch!();
    let user = unique_user(&pool, "alice").await;
    let generated = arkivo::crypto::generate_token("mcp");
    tokens::mint(&pool, user, &generated.hash, "test")
        .await
        .unwrap();

    let h = McpHarness::start(pool, search).await;

    // No token: rejected before any protocol handling.
    let (status, _) = h
        .rpc(
            None,
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .await;
    assert_eq!(status, 401);

    let (status, value) = h
        .rpc(
            Some(&generated.token),
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .await;
    assert_eq!(status, 200, "response: {value}");
    let tools: Vec<&str> = value
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .filter_map(|t| t.get("name").and_then(Value::as_str))
        .collect();
    assert_eq!(tools.len(), 3);
    for expected in ["search", "get_message", "list_facets"] {
        assert!(tools.contains(&expected), "missing tool {expected}");
    }
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn cross_user_isolation_and_audit(pool: PgPool) {
    let search = require_opensearch!();
    let embedder = FakeEmbedder::default();

    let user_a = unique_user(&pool, "alice").await;
    let user_b = unique_user(&pool, "bob").await;

    // Seed both users' indices directly.
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
            &msg(101, "alpha secret plans"),
            &email("alpha secret plans", "the alpha content"),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user_b,
            &msg(202, "bravo private notes"),
            &email("bravo private notes", "the bravo content"),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user_a).await.unwrap();
    search.refresh_user_indices(user_b).await.unwrap();

    let token_a = arkivo::crypto::generate_token("mcp");
    let minted_a = tokens::mint(&pool, user_a, &token_a.hash, "a")
        .await
        .unwrap();

    let h = McpHarness::start(pool.clone(), search_client().unwrap()).await;

    // A searches for B's content: nothing.
    let (status, value) = h
        .call_tool(
            &token_a.token,
            "search",
            json!({"query": "bravo private notes"}),
        )
        .await;
    assert_eq!(status, 200);
    let results = value
        .pointer("/result/structuredContent/results")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("unexpected response shape: {value}"));
    assert!(
        results
            .iter()
            .all(|r| r.get("message_id").and_then(Value::as_str) != Some("202")),
        "user A must never see user B's message: {value}"
    );

    // A searches own content: finds it.
    let (_, value) = h
        .call_tool(
            &token_a.token,
            "search",
            json!({"query": "alpha secret plans"}),
        )
        .await;
    let results = value
        .pointer("/result/structuredContent/results")
        .and_then(Value::as_array)
        .unwrap();
    assert!(
        results
            .iter()
            .any(|r| r.get("message_id").and_then(Value::as_str) == Some("101")),
        "own content must be findable: {value}"
    );

    // A fetches B's message id: not found (it isn't in A's index).
    let (_, value) = h
        .call_tool(&token_a.token, "get_message", json!({"message_id": "202"}))
        .await;
    let err = value.pointer("/result/isError").and_then(Value::as_bool) == Some(true)
        || value.get("error").is_some();
    assert!(err, "cross-user get_message must fail: {value}");

    // A fetches own message: full sanitized detail.
    let (_, value) = h
        .call_tool(&token_a.token, "get_message", json!({"message_id": "101"}))
        .await;
    assert_eq!(
        value
            .pointer("/result/structuredContent/body_text")
            .and_then(Value::as_str),
        Some("the alpha content"),
        "own message fetch: {value}"
    );

    // Every access audit-logged with the token as actor.
    let entries = audit::recent_for_user(&pool, user_a, 20).await.unwrap();
    let actor = format!("mcp:{}", minted_a.id);
    assert!(
        entries
            .iter()
            .filter(|e| e.actor == actor && e.action == "search")
            .count()
            >= 2
    );
    assert!(
        entries.iter().any(|e| e.actor == actor
            && e.action == "get_message"
            && e.message_ref.as_deref() == Some("202")),
        "the probing attempt must leave an audit trail"
    );

    search.delete_user_indices(user_a).await.unwrap();
    search.delete_user_indices(user_b).await.unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn search_from_filter_constrains_results(pool: PgPool) {
    let search = require_opensearch!();
    let embedder = FakeEmbedder::default();
    let user = unique_user(&pool, "alice").await;
    search
        .ensure_user_indices(user, embedder.dimension())
        .await
        .unwrap();

    // Two messages with identical bodies but different senders.
    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    let body = "the shared project status content";
    indexer
        .index_message(
            user,
            &msg(301, "status"),
            &email_from("status", body, "alice@example.com"),
            true,
        )
        .await
        .unwrap();
    indexer
        .index_message(
            user,
            &msg(302, "status"),
            &email_from("status", body, "bob@example.com"),
            true,
        )
        .await
        .unwrap();
    search.refresh_user_indices(user).await.unwrap();

    let generated = arkivo::crypto::generate_token("mcp");
    tokens::mint(&pool, user, &generated.hash, "test")
        .await
        .unwrap();
    let h = McpHarness::start(pool.clone(), search_client().unwrap()).await;

    let (status, value) = h
        .call_tool(
            &generated.token,
            "search",
            json!({"query": "shared project status", "from": "alice@example.com"}),
        )
        .await;
    assert_eq!(status, 200);
    let results = value
        .pointer("/result/structuredContent/results")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("unexpected response shape: {value}"));
    assert!(!results.is_empty(), "alice's message must match: {value}");
    assert!(
        results
            .iter()
            .all(|r| r.get("message_id").and_then(Value::as_str) != Some("302")),
        "the from filter must exclude bob's message: {value}"
    );

    search.delete_user_indices(user).await.unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn revoked_token_is_rejected(pool: PgPool) {
    let search = require_opensearch!();
    let user = unique_user(&pool, "alice").await;
    let generated = arkivo::crypto::generate_token("mcp");
    let minted = tokens::mint(&pool, user, &generated.hash, "t")
        .await
        .unwrap();

    let h = McpHarness::start(pool.clone(), search).await;

    let (status, _) = h
        .call_tool(&generated.token, "list_facets", json!({"facet": "year"}))
        .await;
    assert_ne!(status, 401, "active token accepted");

    tokens::revoke(&pool, user, minted.id).await.unwrap();
    let (status, _) = h
        .call_tool(&generated.token, "list_facets", json!({"facet": "year"}))
        .await;
    assert_eq!(status, 401, "revoked token must be rejected");
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn facets_aggregate_own_corpus(pool: PgPool) {
    let search = require_opensearch!();
    let embedder = FakeEmbedder::default();
    let user = unique_user(&pool, "alice").await;
    search
        .ensure_user_indices(user, embedder.dimension())
        .await
        .unwrap();
    let indexer = Indexer {
        search: &search,
        embedder: &embedder,
    };
    for (id, subject) in [(1, "one"), (2, "two"), (3, "three")] {
        indexer
            .index_message(user, &msg(id, subject), &email(subject, "body text"), true)
            .await
            .unwrap();
    }
    search.refresh_user_indices(user).await.unwrap();

    let generated = arkivo::crypto::generate_token("mcp");
    tokens::mint(&pool, user, &generated.hash, "t")
        .await
        .unwrap();
    let h = McpHarness::start(pool, search_client().unwrap()).await;

    let (status, value) = h
        .call_tool(&generated.token, "list_facets", json!({"facet": "from"}))
        .await;
    assert_eq!(status, 200);
    let facets = value
        .pointer("/result/structuredContent/facets")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("unexpected shape: {value}"));
    assert_eq!(facets.len(), 1);
    assert_eq!(
        facets[0].get("value").and_then(Value::as_str),
        Some("sender@example.com")
    );
    assert_eq!(facets[0].get("count").and_then(Value::as_i64), Some(3));

    let (_, value) = h
        .call_tool(&generated.token, "list_facets", json!({"facet": "year"}))
        .await;
    let facets = value
        .pointer("/result/structuredContent/facets")
        .and_then(Value::as_array)
        .unwrap();
    assert_eq!(facets[0].get("value").and_then(Value::as_str), Some("2018"));

    search.delete_user_indices(user).await.unwrap();
}
