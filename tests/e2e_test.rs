//! End-to-end pipeline test: FakeJmap → backfill → promote (cutoff
//! crossed via FakeClock) → MCP search over HTTP returns the archived
//! message, with the audit trail and metrics in place. Exercises every
//! stage the production stack runs, minus real Fastmail credentials.

mod support;

use std::sync::Arc;

use arkivo::clock::FakeClock;
use arkivo::config::{DeletionPolicy, OpenSearchConfig};
use arkivo::db::{accounts, audit, tokens, users};
use arkivo::embed::FakeEmbedder;
use arkivo::jmap::backfill::{BackfillOptions, backfill_account};
use arkivo::jmap::sync::poll_account;
use arkivo::jmap::{JmapClient, RetryPolicy};
use arkivo::maildir::Maildir;
use arkivo::promote::promote_account;
use arkivo::search::SearchClient;
use chrono::{Duration, TimeZone, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use support::fake_jmap::FakeJmap;

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

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn full_pipeline_backfill_promote_mcp_search(pool: PgPool) {
    let Some(search) = search_client() else {
        eprintln!("skipping: ARKIVO_TEST_OPENSEARCH_URL not set");
        return;
    };

    // Unique user id so OpenSearch indices don't collide between runs.
    let unique_id = 600_000_000 + (std::process::id() as i64 % 900_000);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "alter table users alter column id restart with {unique_id}"
    )))
    .execute(&pool)
    .await
    .unwrap();

    // --- Setup: user, account, live-ish mailbox -------------------------
    let user = users::create(&pool, "owner", "user").await.unwrap();
    let fake = FakeJmap::start().await;
    let old = Utc.with_ymd_and_hms(2015, 6, 1, 9, 0, 0).unwrap();
    fake.add_message("kayak trip planning", "friend@example.com", old);
    fake.add_message(
        "Password reset for ExampleBank",
        "noreply@examplebank.com",
        old + Duration::days(1),
    );

    let account = accounts::create(&pool, user.id, &fake.session_url(), b"sealed", "primary")
        .await
        .unwrap();
    let client = JmapClient::connect(&fake.session_url(), fake.token(), RetryPolicy::default())
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let maildir = Maildir::open_or_create(dir.path()).unwrap();

    // --- Ingest: backfill then a poll for the fresh message -------------
    let stats = backfill_account(
        &pool,
        &client,
        &maildir,
        &account,
        &BackfillOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.fetched, 2);

    // A brand-new message arrives (attacker-window simulation).
    fake.add_message("fresh reset email", "noreply@examplebank.com", Utc::now());
    let stats = poll_account(
        &pool,
        &client,
        &maildir,
        Some(&search),
        &account,
        DeletionPolicy::Retain,
    )
    .await
    .unwrap();
    assert_eq!(stats.fetched, 1);

    // --- Promote: only messages past the 7-day cutoff index -------------
    let embedder = FakeEmbedder::default();
    let clock = FakeClock::at(Utc::now());
    let stats = promote_account(&pool, &maildir, &search, &embedder, &clock, &account, None)
        .await
        .unwrap();
    assert_eq!(stats.promoted, 2, "the two old messages");
    search.refresh_user_indices(user.id).await.unwrap();

    // --- MCP: search through the real HTTP surface ----------------------
    let generated = arkivo::crypto::generate_token("mcp");
    tokens::mint(&pool, user.id, &generated.hash, "e2e")
        .await
        .unwrap();
    let app = arkivo::mcp::app(
        pool.clone(),
        Arc::new(search_client().unwrap()),
        Arc::new(FakeEmbedder::default()),
        vec![],
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let http = reqwest::Client::new();
    let call = |name: &'static str, args: Value| {
        let http = http.clone();
        let token = generated.token.clone();
        async move {
            http.post(format!("http://{addr}/mcp"))
                .bearer_auth(&token)
                .header("accept", "application/json, text/event-stream")
                .json(&json!({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": name, "arguments": args},
                }))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };

    // The old message is findable...
    let value = call("search", json!({"query": "kayak trip planning"})).await;
    let results = value
        .pointer("/result/structuredContent/results")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("unexpected: {value}"));
    assert!(!results.is_empty(), "archived message must be searchable");
    let hit_id = results[0]["message_id"].as_str().unwrap().to_string();

    // ...with full sanitized detail...
    let value = call("get_message", json!({"message_id": hit_id})).await;
    let body = value
        .pointer("/result/structuredContent/body_text")
        .and_then(Value::as_str)
        .unwrap();
    assert!(body.contains("kayak trip planning"));

    // ...while the fresh (recency-gated) message is invisible.
    let value = call("search", json!({"query": "fresh reset email"})).await;
    let results = value
        .pointer("/result/structuredContent/results")
        .and_then(Value::as_array)
        .unwrap();
    assert!(
        results
            .iter()
            .all(|r| r["subject"].as_str() != Some("fresh reset email")),
        "post-cutoff message must not be searchable: {value}"
    );

    // --- Audit + metrics --------------------------------------------------
    let entries = audit::recent_for_user(&pool, user.id, 20).await.unwrap();
    assert!(entries.iter().any(|e| e.action == "search"));
    assert!(entries.iter().any(|e| e.action == "get_message"));

    let metrics = arkivo::metrics::gather(&pool).await.unwrap();
    assert_eq!(
        metrics.pointer("/totals/messages").and_then(Value::as_i64),
        Some(3)
    );
    assert_eq!(
        metrics.pointer("/totals/indexed").and_then(Value::as_i64),
        Some(2)
    );

    search.delete_user_indices(user.id).await.unwrap();
}
