//! Web service tests: full WebAuthn ceremonies driven in-process by the
//! softtoken authenticator (no browser), first-run setup mode,
//! invite/recovery lifecycles, role enforcement, and MCP token
//! management via the API.

mod support;

use std::sync::Arc;

use arkivo::config::{OpenSearchConfig, UserDefaults};
use arkivo::crypto::{Sealer, generate_token};
use arkivo::db::{auth, users};
use arkivo::search::SearchClient;
use arkivo::web::{WebState, app};
use serde_json::{Value, json};
use sqlx::PgPool;
use webauthn_authenticator_rs::WebauthnAuthenticator;
use webauthn_authenticator_rs::softtoken::SoftToken;
use webauthn_rs::prelude::{
    CreationChallengeResponse, RequestChallengeResponse, Url, WebauthnBuilder,
};

struct WebHarness {
    base: String,
    origin: Url,
}

impl WebHarness {
    async fn start(pool: PgPool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let origin = Url::parse(&format!("http://localhost:{port}")).unwrap();

        let webauthn = WebauthnBuilder::new("localhost", &origin)
            .unwrap()
            .rp_name("Arkivo Test")
            .build()
            .unwrap();
        let opensearch = OpenSearchConfig {
            url: "http://localhost:1".into(), // health checks may fail; fine
            username: None,
            password: None,
        };
        let config = arkivo::config::AppConfig {
            database_url: String::new(), // job spawning not exercised here
            maildir_root: std::env::temp_dir().join("arkivo-web-test"),
            master_key_path: std::env::temp_dir().join("arkivo-web-test.key"),
            opensearch: opensearch.clone(),
            embedding: arkivo::config::EmbeddingConfig {
                url: "http://localhost:1".into(),
                model: "nomic-embed-text".into(),
                dimension: 768,
                num_ctx: 4096,
            },
            web: Default::default(),
            mcp: Default::default(),
            defaults: UserDefaults::default(),
        };
        let state = WebState {
            pool,
            webauthn: Arc::new(webauthn),
            sealer: Arc::new(Sealer::new(&[7u8; 32], "primary").unwrap()),
            search: Arc::new(SearchClient::new(&opensearch).unwrap()),
            embedding_url: "http://localhost:1".into(),
            defaults: UserDefaults::default(),
            config,
        };
        tokio::spawn(async move {
            axum::serve(listener, app(state)).await.unwrap();
        });
        Self {
            base: format!("http://localhost:{port}"),
            origin,
        }
    }

    fn client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .cookie_store(true)
            .build()
            .unwrap()
    }

    /// Full registration ceremony through HTTP + softtoken.
    async fn register(
        &self,
        client: &reqwest::Client,
        authenticator: &mut WebauthnAuthenticator<SoftToken>,
        invite_code: &str,
        handle: &str,
    ) -> u16 {
        let response = client
            .post(format!("{}/auth/register/start", self.base))
            .json(&json!({"invite_code": invite_code, "handle": handle}))
            .send()
            .await
            .unwrap();
        if !response.status().is_success() {
            return response.status().as_u16();
        }
        let ccr: CreationChallengeResponse = response.json().await.unwrap();
        let credential = authenticator
            .do_registration(self.origin.clone(), ccr)
            .unwrap();
        let response = client
            .post(format!("{}/auth/register/finish", self.base))
            .json(&credential)
            .send()
            .await
            .unwrap();
        response.status().as_u16()
    }

    async fn login(
        &self,
        client: &reqwest::Client,
        authenticator: &mut WebauthnAuthenticator<SoftToken>,
        handle: &str,
    ) -> u16 {
        let response = client
            .post(format!("{}/auth/login/start", self.base))
            .json(&json!({"handle": handle}))
            .send()
            .await
            .unwrap();
        if !response.status().is_success() {
            return response.status().as_u16();
        }
        let rcr: RequestChallengeResponse = response.json().await.unwrap();
        let credential = authenticator
            .do_authentication(self.origin.clone(), rcr)
            .unwrap();
        let response = client
            .post(format!("{}/auth/login/finish", self.base))
            .json(&credential)
            .send()
            .await
            .unwrap();
        response.status().as_u16()
    }
}

fn softtoken() -> WebauthnAuthenticator<SoftToken> {
    let (token, _cert) = SoftToken::new(true).unwrap();
    WebauthnAuthenticator::new(token)
}

async fn make_invite(pool: &PgPool, role: &str) -> String {
    let generated = generate_token("inv");
    auth::create_invite(pool, &generated.hash, None, role, 7)
        .await
        .unwrap();
    generated.token
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn invite_registers_passkey_creates_user_and_session(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();
    let mut authenticator = softtoken();

    let invite = make_invite(&pool, "admin").await;
    assert_eq!(
        h.register(&client, &mut authenticator, &invite, "alice")
            .await,
        200
    );

    // Session established: authed API works.
    let status = client
        .get(format!("{}/api/status", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status().as_u16(), 200);
    let body: Value = status.json().await.unwrap();
    assert_eq!(
        body.pointer("/user/handle").and_then(Value::as_str),
        Some("alice")
    );
    assert_eq!(
        body.pointer("/user/role").and_then(Value::as_str),
        Some("admin")
    );

    let user = users::get_by_handle(&pool, "alice").await.unwrap().unwrap();
    assert!(user.is_admin());
    let passkeys = auth::passkeys_for_user(&pool, user.id).await.unwrap();
    assert_eq!(passkeys.len(), 1);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn invite_is_single_use_and_bad_invite_rejected(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let invite = make_invite(&pool, "user").await;

    let client1 = h.client();
    assert_eq!(
        h.register(&client1, &mut softtoken(), &invite, "alice")
            .await,
        200
    );

    // Same code again: rejected at start.
    let client2 = h.client();
    assert_eq!(
        h.register(&client2, &mut softtoken(), &invite, "bob").await,
        400
    );

    // Nonsense code.
    let client3 = h.client();
    assert_eq!(
        h.register(&client3, &mut softtoken(), "inv_bogus", "carol")
            .await,
        400
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn login_with_registered_passkey(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();
    let mut authenticator = softtoken();
    let invite = make_invite(&pool, "user").await;
    h.register(&client, &mut authenticator, &invite, "alice")
        .await;

    // Fresh client = fresh session.
    let client = h.client();
    assert_eq!(h.login(&client, &mut authenticator, "alice").await, 200);
    let status = client
        .get(format!("{}/api/status", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status().as_u16(), 200);

    // A different authenticator cannot log in as alice.
    let client = h.client();
    let response = client
        .post(format!("{}/auth/login/start", h.base))
        .json(&json!({"handle": "alice"}))
        .send()
        .await
        .unwrap();
    let rcr: RequestChallengeResponse = response.json().await.unwrap();
    // The wrong softtoken has no credential matching allowCredentials.
    assert!(
        softtoken()
            .do_authentication(h.origin.clone(), rcr)
            .is_err()
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn unknown_handle_login_denied_uniformly(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();
    let response = client
        .post(format!("{}/auth/login/start", h.base))
        .json(&json!({"handle": "ghost"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn recovery_code_enrolls_new_passkey_single_use(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();
    let mut old_authenticator = softtoken();
    let invite = make_invite(&pool, "user").await;
    h.register(&client, &mut old_authenticator, &invite, "alice")
        .await;
    let user = users::get_by_handle(&pool, "alice").await.unwrap().unwrap();

    // Admin issues a recovery code (device lost).
    let code = generate_token("rec");
    auth::create_recovery_code(&pool, user.id, &code.hash, 3)
        .await
        .unwrap();

    // New device enrolls through recovery.
    let client = h.client();
    let mut new_authenticator = softtoken();
    let response = client
        .post(format!("{}/auth/recover/start", h.base))
        .json(&json!({"handle": "alice", "recovery_code": code.token}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let ccr: CreationChallengeResponse = response.json().await.unwrap();
    let credential = new_authenticator
        .do_registration(h.origin.clone(), ccr)
        .unwrap();
    let response = client
        .post(format!("{}/auth/recover/finish", h.base))
        .json(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);

    // New passkey logs in.
    let client = h.client();
    assert_eq!(h.login(&client, &mut new_authenticator, "alice").await, 200);

    // Code is spent.
    let client = h.client();
    let response = client
        .post(format!("{}/auth/recover/start", h.base))
        .json(&json!({"handle": "alice", "recovery_code": code.token}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status().as_u16(),
        400,
        "recovery code must be single-use"
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn role_middleware_blocks_non_admin(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;

    let admin_client = h.client();
    let invite = make_invite(&pool, "admin").await;
    h.register(&admin_client, &mut softtoken(), &invite, "root")
        .await;

    let user_client = h.client();
    let invite = make_invite(&pool, "user").await;
    h.register(&user_client, &mut softtoken(), &invite, "plain")
        .await;

    let response = user_client
        .get(format!("{}/api/admin/users", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 403, "user role must be blocked");

    let response = admin_client
        .get(format!("{}/api/admin/users", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["users"].as_array().unwrap().len(), 2);
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn unauthenticated_api_gets_401_pages_redirect(pool: PgPool) {
    let h = WebHarness::start(pool).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let response = client
        .get(format!("{}/api/status", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 401);

    let response = client.get(format!("{}/", h.base)).send().await.unwrap();
    assert_eq!(
        response.status().as_u16(),
        303,
        "page should redirect to /login"
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn token_mint_and_revoke_via_api(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();
    let invite = make_invite(&pool, "user").await;
    h.register(&client, &mut softtoken(), &invite, "alice")
        .await;

    let response = client
        .post(format!("{}/api/tokens", h.base))
        .json(&json!({"label": "laptop"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 201);
    let body: Value = response.json().await.unwrap();
    let token = body["token"].as_str().unwrap();
    assert!(token.starts_with("mcp_"));
    let token_id = body["id"].as_i64().unwrap();

    // The minted token resolves for MCP use.
    let resolved = arkivo::db::tokens::resolve_active(&pool, &arkivo::crypto::hash_token(token))
        .await
        .unwrap();
    assert!(resolved.is_some());

    // Revoke and confirm.
    let response = client
        .delete(format!("{}/api/tokens/{token_id}", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let resolved = arkivo::db::tokens::resolve_active(&pool, &arkivo::crypto::hash_token(token))
        .await
        .unwrap();
    assert!(resolved.is_none());
}

/// Setup ceremony: same two legs as registration, no invite.
async fn run_setup(
    h: &WebHarness,
    client: &reqwest::Client,
    authenticator: &mut WebauthnAuthenticator<SoftToken>,
    handle: &str,
) -> u16 {
    let response = client
        .post(format!("{}/auth/setup/start", h.base))
        .json(&json!({"handle": handle}))
        .send()
        .await
        .unwrap();
    if !response.status().is_success() {
        return response.status().as_u16();
    }
    let ccr: CreationChallengeResponse = response.json().await.unwrap();
    let credential = authenticator
        .do_registration(h.origin.clone(), ccr)
        .unwrap();
    let response = client
        .post(format!("{}/auth/setup/finish", h.base))
        .json(&credential)
        .send()
        .await
        .unwrap();
    response.status().as_u16()
}

async fn setup_needed(h: &WebHarness, client: &reqwest::Client) -> bool {
    let v: Value = client
        .get(format!("{}/auth/setup-needed", h.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["needed"].as_bool().unwrap()
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn first_run_setup_creates_admin_then_closes(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();

    // Fresh install: setup offered.
    assert!(setup_needed(&h, &client).await);

    // First admin created via ceremony, session established.
    let mut authenticator = softtoken();
    assert_eq!(
        run_setup(&h, &client, &mut authenticator, "root").await,
        200
    );
    let user = users::get_by_handle(&pool, "root").await.unwrap().unwrap();
    assert!(user.is_admin());
    let response = client
        .get(format!("{}/api/admin/users", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status().as_u16(),
        200,
        "setup admin has an admin session"
    );

    // The window is closed permanently.
    let fresh = h.client();
    assert!(!setup_needed(&h, &fresh).await);
    assert_eq!(
        run_setup(&h, &fresh, &mut softtoken(), "intruder").await,
        400
    );

    // Normal invite-gated registration still works afterwards.
    let invite = make_invite(&pool, "user").await;
    let client2 = h.client();
    assert_eq!(
        h.register(&client2, &mut softtoken(), &invite, "alice")
            .await,
        200
    );
}

#[sqlx::test(migrator = "arkivo::db::MIGRATOR")]
async fn add_account_validates_token_before_sealing(pool: PgPool) {
    let h = WebHarness::start(pool.clone()).await;
    let client = h.client();
    let invite = make_invite(&pool, "user").await;
    h.register(&client, &mut softtoken(), &invite, "alice")
        .await;

    let fake = support::fake_jmap::FakeJmap::start().await;

    // Wrong token: rejected with a clear message, nothing stored.
    let response = client
        .post(format!("{}/api/accounts", h.base))
        .json(&json!({"jmap_session_url": fake.session_url(), "token": "wrong-token"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);

    // Correct token: accepted, JMAP account id captured.
    let response = client
        .post(format!("{}/api/accounts", h.base))
        .json(&json!({"jmap_session_url": fake.session_url(), "token": fake.token()}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 201);
    let body: Value = response.json().await.unwrap();
    let account = arkivo::db::accounts::get(&pool, body["id"].as_i64().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        account.account_id.as_deref(),
        Some(support::fake_jmap::ACCOUNT_ID)
    );
}
