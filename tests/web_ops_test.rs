//! Web-driven operations: backfill/poll/promote/cancel through the HTTP
//! API against FakeJmap — the "no CLI required" contract. The promote
//! path additionally needs OpenSearch (env-gated) and a fake Ollama
//! embed endpoint.

mod support;

use std::sync::Arc;
use std::time::Duration;

use arkivo::config::{AppConfig, EmbeddingConfig, OpenSearchConfig, UserDefaults};
use arkivo::crypto::Sealer;
use arkivo::db::{jobs, users};
use arkivo::ops::{self, JobKind};
use arkivo::search::SearchClient;
use arkivo::web::{WebState, app};
use axum::routing::post;
use axum::{Json as AxumJson, Router};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use support::fake_jmap::FakeJmap;
use webauthn_authenticator_rs::WebauthnAuthenticator;
use webauthn_authenticator_rs::softtoken::SoftToken;
use webauthn_rs::prelude::{CreationChallengeResponse, Url, WebauthnBuilder};

const EMBED_DIM: usize = 8;

fn ts(day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2019, 1, day, 12, 0, 0).unwrap()
}

/// The ephemeral sqlx::test database's URL, with credentials taken from
/// DATABASE_URL (connect_options redacts the password).
fn test_db_url(pool: &PgPool) -> String {
    use sqlx::ConnectOptions;
    let base = std::env::var("DATABASE_URL").expect("DATABASE_URL set for tests");
    let dbname = pool.connect_options().to_url_lossy().path().to_string();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&dbname);
    url.to_string()
}

/// Minimal Ollama-compatible /api/embed: unit vectors of EMBED_DIM.
async fn fake_ollama() -> String {
    async fn embed(AxumJson(body): AxumJson<Value>) -> AxumJson<Value> {
        let count = body["input"].as_array().map(Vec::len).unwrap_or(1);
        let mut vector = vec![0.0f32; EMBED_DIM];
        vector[0] = 1.0;
        AxumJson(json!({"embeddings": vec![vector; count]}))
    }
    let router = Router::new().route("/api/embed", post(embed));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

struct OpsHarness {
    base: String,
    config: AppConfig,
    fake: FakeJmap,
    account_id: i64,
    user_id: i64,
    client: reqwest::Client,
    _maildir_dir: tempfile::TempDir,
    _key_dir: tempfile::TempDir,
}

impl OpsHarness {
    /// Full stack: sealed FakeJmap credential, web app with a working
    /// AppConfig (jobs really run), one registered admin with a session.
    async fn start(pool: PgPool) -> Self {
        // Unique user id: OpenSearch index names derive from it.
        static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let unique = 500_000_000
            + (std::process::id() as i64 % 90_000) * 1_000
            + NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "alter table users alter column id restart with {unique}"
        )))
        .execute(&pool)
        .await
        .unwrap();

        let fake = FakeJmap::start().await;
        let key_dir = tempfile::tempdir().unwrap();
        let key_path = key_dir.path().join("master.key");
        let key = [9u8; 32];
        std::fs::write(&key_path, hex::encode(key)).unwrap();
        let sealer = Sealer::new(&key, "primary").unwrap();

        let maildir_dir = tempfile::tempdir().unwrap();
        let opensearch = OpenSearchConfig {
            url: std::env::var("ARKIVO_TEST_OPENSEARCH_URL")
                .unwrap_or_else(|_| "http://localhost:1".into()),
            username: None,
            password: None,
        };
        let config = AppConfig {
            database_url: test_db_url(&pool),
            maildir_root: maildir_dir.path().to_path_buf(),
            master_key_path: key_path,
            opensearch: opensearch.clone(),
            embedding: EmbeddingConfig {
                url: fake_ollama().await,
                model: "fake".into(),
                dimension: EMBED_DIM,
                num_ctx: 4096,
            },
            web: Default::default(),
            mcp: Default::default(),
            defaults: UserDefaults::default(),
            google: None,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let origin = Url::parse(&format!("http://localhost:{port}")).unwrap();
        let webauthn = WebauthnBuilder::new("localhost", &origin)
            .unwrap()
            .rp_name("Arkivo Ops Test")
            .build()
            .unwrap();
        let state = WebState {
            pool: pool.clone(),
            webauthn: Arc::new(webauthn),
            sealer: Arc::new(sealer.clone()),
            search: Arc::new(SearchClient::new(&opensearch).unwrap()),
            embedding_url: config.embedding.url.clone(),
            defaults: UserDefaults::default(),
            config: config.clone(),
        };
        tokio::spawn(async move {
            axum::serve(listener, app(state)).await.unwrap();
        });
        let base = format!("http://localhost:{port}");

        // First admin via setup mode; session cookie retained.
        let client = reqwest::Client::builder()
            .cookie_store(true)
            .build()
            .unwrap();
        let mut authenticator = WebauthnAuthenticator::new(SoftToken::new(true).unwrap().0);
        let ccr: CreationChallengeResponse = client
            .post(format!("{base}/auth/setup/start"))
            .json(&json!({"handle": "owner"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let credential = authenticator.do_registration(origin.clone(), ccr).unwrap();
        let response = client
            .post(format!("{base}/auth/setup/finish"))
            .json(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200, "setup must succeed");
        let user_id = users::get_by_handle(&pool, "owner")
            .await
            .unwrap()
            .unwrap()
            .id;

        // Add the FakeJmap account through the API (validates the token).
        let response = client
            .post(format!("{base}/api/accounts"))
            .json(&json!({"jmap_session_url": fake.session_url(), "token": fake.token()}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 201);
        let account_id = response.json::<Value>().await.unwrap()["id"]
            .as_i64()
            .unwrap();

        Self {
            base,
            config,
            fake,
            account_id,
            user_id,
            client,
            _maildir_dir: maildir_dir,
            _key_dir: key_dir,
        }
    }

    async fn api(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut request = self.client.request(method, format!("{}{path}", self.base));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let value = response.json().await.unwrap_or(Value::Null);
        (status, value)
    }

    /// Poll /api/status until no jobs are running (or timeout).
    async fn wait_idle(&self) -> Value {
        for _ in 0..100 {
            let (_, status) = self.api(reqwest::Method::GET, "/api/status", None).await;
            if status["running_jobs"]
                .as_array()
                .map(Vec::is_empty)
                .unwrap_or(false)
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jobs did not finish in time");
    }
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn backfill_and_poll_run_entirely_through_the_api(pool: PgPool) {
    let h = OpsHarness::start(pool.clone()).await;
    for day in 1..=5 {
        h.fake
            .add_message(&format!("old {day}"), "a@example.com", ts(day));
    }

    // Smoke-test limit first, exactly as the UI offers.
    let (status, body) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({"limit": 2})),
        )
        .await;
    assert_eq!(status, 202, "{body}");
    let final_status = h.wait_idle().await;
    assert_eq!(final_status["accounts"][0]["counts"]["total"], 2);

    // Full run completes the archive.
    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202);
    let final_status = h.wait_idle().await;
    assert_eq!(final_status["accounts"][0]["counts"]["total"], 5);
    assert_eq!(final_status["accounts"][0]["backfill_done"], true);

    // New mail arrives; poll via API picks it up.
    h.fake.add_message("fresh", "b@example.com", ts(20));
    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/poll", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202);
    let final_status = h.wait_idle().await;
    assert_eq!(final_status["accounts"][0]["counts"]["total"], 6);

    // Job history recorded.
    let recent = jobs::recent(&pool, h.account_id, 10).await.unwrap();
    assert!(recent.iter().filter(|j| j.status == "succeeded").count() >= 3);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn conflicting_job_gets_409(pool: PgPool) {
    let h = OpsHarness::start(pool).await;
    h.fake.add_message("m", "a@example.com", ts(1));

    // Hold the account's sync lock as the worker cron would.
    let lock =
        arkivo::db::locks::AdvisoryLock::try_acquire(&h.config.database_url, "sync", h.account_id)
            .await
            .unwrap()
            .unwrap();

    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 409, "busy account must refuse a second sync job");

    lock.release().await.unwrap();
    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202, "freed account accepts the job");
    h.wait_idle().await;
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn cancelled_backfill_stops_at_page_boundary_and_resumes(pool: PgPool) {
    let h = OpsHarness::start(pool.clone()).await;
    h.fake.set_max_objects_in_get(2); // many small pages
    for day in 1..=9 {
        h.fake
            .add_message(&format!("m{day}"), "a@example.com", ts(day));
    }

    // Library-level determinism: start the job, flag cancellation before
    // running, so the first page boundary observes it.
    let started = ops::try_start(
        &h.config,
        &pool,
        h.account_id,
        JobKind::Backfill {
            limit: None,
            since: None,
        },
    )
    .await
    .unwrap()
    .unwrap();
    let job_id = started.job_id;
    sqlx::query!(
        "update jobs set cancel_requested_at = now() where id = $1",
        job_id
    )
    .execute(&pool)
    .await
    .unwrap();
    ops::run(&h.config, &pool, started).await.unwrap();

    let job = jobs::recent(&pool, h.account_id, 10)
        .await
        .unwrap()
        .into_iter()
        .find(|j| j.id == job_id)
        .unwrap();
    assert_eq!(job.status, "cancelled");
    let fetched_before = job.stats.as_ref().unwrap()["fetched"].as_u64().unwrap();
    assert!(fetched_before < 9, "cancel must stop before the end");

    // Cancel via the API surface too (ownership check), on a fresh job.
    let (status, body) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202);
    let new_job = body["job_id"].as_i64().unwrap();
    // Cancel request succeeds while running or may race completion; both fine.
    let _ = h
        .api(
            reqwest::Method::POST,
            &format!("/api/jobs/{new_job}/cancel"),
            Some(json!({})),
        )
        .await;
    h.wait_idle().await;

    // Keep resuming until done: the anchor guarantees forward progress.
    loop {
        let (status, _) = h
            .api(
                reqwest::Method::POST,
                &format!("/api/accounts/{}/backfill", h.account_id),
                Some(json!({})),
            )
            .await;
        assert_eq!(status, 202);
        let final_status = h.wait_idle().await;
        if final_status["accounts"][0]["backfill_done"] == true {
            assert_eq!(final_status["accounts"][0]["counts"]["total"], 9);
            break;
        }
    }
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn promote_runs_through_the_api(pool: PgPool) {
    if std::env::var("ARKIVO_TEST_OPENSEARCH_URL").is_err() {
        eprintln!("skipping: ARKIVO_TEST_OPENSEARCH_URL not set");
        return;
    }
    let h = OpsHarness::start(pool.clone()).await;
    h.fake.add_message("ancient", "a@example.com", ts(1));

    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202);
    h.wait_idle().await;

    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/promote", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202);
    let final_status = h.wait_idle().await;
    assert_eq!(
        final_status["accounts"][0]["counts"]["indexed"], 1,
        "message promoted into the index via the API: {final_status}"
    );

    let search = SearchClient::new(&h.config.opensearch).unwrap();
    search.delete_user_indices(h.user_id).await.unwrap();
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn failed_messages_list_and_retry_through_the_api(pool: PgPool) {
    if std::env::var("ARKIVO_TEST_OPENSEARCH_URL").is_err() {
        eprintln!("skipping: ARKIVO_TEST_OPENSEARCH_URL not set");
        return;
    }
    let h = OpsHarness::start(pool.clone()).await;
    h.fake.add_message("first", "a@example.com", ts(1));
    h.fake.add_message("second", "a@example.com", ts(2));

    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/backfill", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 202);
    h.wait_idle().await;

    // Simulate an indexing outage: both messages failed with errors that
    // differ only by an embedded id, so they must group as one cause.
    sqlx::query(
        "update messages
         set index_status = 'failed', indexed_at = null,
             error = 'OpenSearch /msg/_doc/' || id || ' returned 429'
         where mail_account_id = $1",
    )
    .bind(h.account_id)
    .execute(&pool)
    .await
    .unwrap();

    let (status, problems) = h
        .api(
            reqwest::Method::GET,
            &format!("/api/accounts/{}/problems", h.account_id),
            None,
        )
        .await;
    assert_eq!(status, 200);
    let groups = problems["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 1, "digit-normalized errors group as one: {problems}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[0]["status"], "failed");
    let error_key = groups[0]["error_key"].as_str().unwrap().to_string();

    let (status, samples) = h
        .api(
            reqwest::Method::GET,
            &format!(
                "/api/accounts/{}/problems/messages?status=failed&error_key={}",
                h.account_id,
                url::form_urlencoded::byte_serialize(error_key.as_bytes()).collect::<String>(),
            ),
            None,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(samples["messages"].as_array().unwrap().len(), 2);
    assert!(samples["messages"][0]["retryable"].as_bool().unwrap());

    // Ownership guard: a foreign account id is a 404.
    let (status, _) = h
        .api(
            reqwest::Method::GET,
            &format!("/api/accounts/{}/problems", h.account_id + 999),
            None,
        )
        .await;
    assert_eq!(status, 404);

    // Retry all failed: re-stages both and spawns a promote job.
    let (status, retry) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/problems/retry", h.account_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(retry["requeued"], 2, "both failures re-staged: {retry}");
    assert!(retry["job_id"].is_i64(), "promote spawned: {retry}");

    let final_status = h.wait_idle().await;
    assert_eq!(final_status["accounts"][0]["counts"]["failed"], 0);
    assert_eq!(
        final_status["accounts"][0]["counts"]["indexed"], 2,
        "retried messages made it into the index: {final_status}"
    );

    let (status, problems) = h
        .api(
            reqwest::Method::GET,
            &format!("/api/accounts/{}/problems", h.account_id),
            None,
        )
        .await;
    assert_eq!(status, 200);
    assert!(problems["groups"].as_array().unwrap().is_empty());

    // A healthy (indexed) message refuses a per-message retry.
    let msg_id: i64 = sqlx::query_scalar("select id from messages where mail_account_id = $1 limit 1")
        .bind(h.account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let (status, _) = h
        .api(
            reqwest::Method::POST,
            &format!("/api/accounts/{}/messages/{}/retry", h.account_id, msg_id),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 409);

    let search = SearchClient::new(&h.config.opensearch).unwrap();
    search.delete_user_indices(h.user_id).await.unwrap();
}
