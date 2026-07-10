//! In-process Gmail API fixture: an in-memory mailbox with a scriptable
//! history log, serving /token, /profile, /messages, and /history —
//! enough surface to exercise the real client and the sync engine
//! without Google credentials.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, RawForm, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

pub const CLIENT_ID: &str = "fake-client-id.apps.googleusercontent.com";
pub const CLIENT_SECRET: &str = "fake-client-secret";
pub const REFRESH_TOKEN: &str = "fake-refresh-token";
pub const EMAIL: &str = "owner@example.com";

#[derive(Debug, Clone)]
pub struct FakeGmailMessage {
    pub id: String,
    pub thread_id: String,
    pub label_ids: Vec<String>,
    pub internal_date: i64,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone)]
enum Change {
    Added(String),
    Deleted(String),
    /// (message id, labels added, labels removed)
    Labels(String, Vec<String>, Vec<String>),
}

struct Inner {
    next_id: u64,
    /// History sequence; every mutation bumps it.
    seq: u64,
    /// History ids older than this are gone: history.list 404s.
    oldest_history: u64,
    messages: HashMap<String, FakeGmailMessage>,
    history: Vec<(u64, Change)>,
    /// (count, status, body) — fail the next N API requests this way.
    fail_next: Option<(u32, u16, String)>,
    /// Reject every pageToken (simulates server-side token expiry).
    reject_page_tokens: bool,
    /// Access tokens currently accepted by the API.
    valid_tokens: Vec<String>,
    token_seq: u64,
    expires_in: u64,
    grant_revoked: bool,
    /// Authorization codes the token endpoint will exchange (single-use).
    pending_codes: Vec<String>,
    omit_refresh_token: bool,
}

#[derive(Clone)]
pub struct FakeGmail {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
    api_calls: Arc<AtomicU64>,
    token_refreshes: Arc<AtomicU64>,
}

impl FakeGmail {
    pub async fn start() -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            next_id: 1,
            seq: 1,
            oldest_history: 0,
            messages: HashMap::new(),
            history: Vec::new(),
            fail_next: None,
            reject_page_tokens: false,
            valid_tokens: Vec::new(),
            token_seq: 0,
            expires_in: 3600,
            grant_revoked: false,
            pending_codes: Vec::new(),
            omit_refresh_token: false,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fake = Self {
            addr,
            inner,
            api_calls: Arc::new(AtomicU64::new(0)),
            token_refreshes: Arc::new(AtomicU64::new(0)),
        };
        let app = Router::new()
            .route("/token", post(token_handler))
            .route("/gmail/users/me/profile", get(profile_handler))
            .route("/gmail/users/me/messages", get(list_handler))
            .route("/gmail/users/me/messages/{id}", get(get_handler))
            .route("/gmail/users/me/history", get(history_handler))
            .with_state(fake.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        fake
    }

    pub fn token_url(&self) -> String {
        format!("http://{}/token", self.addr)
    }

    pub fn api_base(&self) -> String {
        format!("http://{}/gmail/users/me", self.addr)
    }

    pub fn google_config(&self) -> arkivo::config::GoogleConfig {
        arkivo::config::GoogleConfig {
            client_id: CLIENT_ID.into(),
            client_secret: CLIENT_SECRET.into(),
            // The consent page is never fetched by the app (the browser
            // is redirected there), so any URL will do for tests.
            auth_url: Some(format!("http://{}/o/oauth2/auth", self.addr)),
            token_url: Some(self.token_url()),
            api_base: Some(self.api_base()),
        }
    }

    pub fn api_calls(&self) -> u64 {
        self.api_calls.load(Ordering::SeqCst)
    }

    pub fn token_refreshes(&self) -> u64 {
        self.token_refreshes.load(Ordering::SeqCst)
    }

    /// Current history id as the server would report it.
    pub fn current_history_id(&self) -> String {
        self.inner.lock().unwrap().seq.to_string()
    }

    /// Fail the next `n` API requests with 429.
    pub fn fail_next(&self, n: u32) {
        self.inner.lock().unwrap().fail_next = Some((n, 429, "slow down".into()));
    }

    /// Fail the next `n` API requests with an arbitrary status and body
    /// (e.g. 403 + rateLimitExceeded for the quota-signal path).
    pub fn fail_next_with(&self, n: u32, status: u16, body: &str) {
        self.inner.lock().unwrap().fail_next = Some((n, status, body.to_string()));
    }

    /// Invalidate all issued access tokens; the next API call gets 401.
    pub fn revoke_access_tokens(&self) {
        self.inner.lock().unwrap().valid_tokens.clear();
    }

    /// Refuse the refresh grant itself (invalid_grant).
    pub fn revoke_grant(&self) {
        self.inner.lock().unwrap().grant_revoked = true;
    }

    pub fn set_expires_in(&self, secs: u64) {
        self.inner.lock().unwrap().expires_in = secs;
    }

    /// Make every previously issued history id too old, forcing a 404
    /// from history.list.
    pub fn invalidate_history(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        inner.oldest_history = inner.seq;
    }

    /// Reject any pageToken with 400 (simulates expiry between runs).
    pub fn reject_page_tokens(&self, reject: bool) {
        self.inner.lock().unwrap().reject_page_tokens = reject;
    }

    /// Register an authorization code the token endpoint will accept
    /// (single-use), as if a user had just completed consent.
    pub fn expect_auth_code(&self, code: &str) {
        self.inner
            .lock()
            .unwrap()
            .pending_codes
            .push(code.to_string());
    }

    /// Make code exchanges omit the refresh_token (Google does this on
    /// re-consent without prompt=consent).
    pub fn omit_refresh_token(&self, omit: bool) {
        self.inner.lock().unwrap().omit_refresh_token = omit;
    }

    pub fn add_message(
        &self,
        subject: &str,
        from: &str,
        received_at: DateTime<Utc>,
        labels: &[&str],
    ) -> String {
        let mut inner = self.inner.lock().unwrap();
        let id = format!("g{}", inner.next_id);
        inner.next_id += 1;
        inner.seq += 1;
        let seq = inner.seq;
        let raw = format!(
            "From: {from}\r\nTo: {EMAIL}\r\nSubject: {subject}\r\n\
             Message-ID: <{id}@fake.gmail>\r\nDate: {}\r\n\r\nBody of {subject}.\r\n",
            received_at.to_rfc2822()
        )
        .into_bytes();
        let message = FakeGmailMessage {
            id: id.clone(),
            thread_id: format!("t-{id}"),
            label_ids: labels.iter().map(|s| s.to_string()).collect(),
            internal_date: received_at.timestamp_millis(),
            raw,
        };
        inner.messages.insert(id.clone(), message);
        inner.history.push((seq, Change::Added(id.clone())));
        id
    }

    pub fn destroy(&self, id: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        inner.messages.remove(id);
        inner.history.push((seq, Change::Deleted(id.to_string())));
    }

    pub fn change_labels(&self, id: &str, add: &[&str], remove: &[&str]) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        if let Some(m) = inner.messages.get_mut(id) {
            m.label_ids.retain(|l| !remove.contains(&l.as_str()));
            for l in add {
                if !m.label_ids.iter().any(|x| x == l) {
                    m.label_ids.push(l.to_string());
                }
            }
        }
        inner.history.push((
            seq,
            Change::Labels(
                id.to_string(),
                add.iter().map(|s| s.to_string()).collect(),
                remove.iter().map(|s| s.to_string()).collect(),
            ),
        ));
    }

    pub fn message_count(&self) -> usize {
        self.inner.lock().unwrap().messages.len()
    }
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string)
}

/// Shared API-request gate: scripted failures, then auth.
fn gate(fake: &FakeGmail, headers: &HeaderMap) -> Option<Response> {
    {
        let mut inner = fake.inner.lock().unwrap();
        if let Some((n, status, body)) = inner.fail_next.take() {
            let response = (
                StatusCode::from_u16(status).unwrap(),
                [("retry-after", "0")],
                body.clone(),
            )
                .into_response();
            if n > 1 {
                inner.fail_next = Some((n - 1, status, body));
            }
            return Some(response);
        }
        let authorized = bearer(headers)
            .map(|t| inner.valid_tokens.contains(&t))
            .unwrap_or(false);
        if !authorized {
            return Some(StatusCode::UNAUTHORIZED.into_response());
        }
    }
    fake.api_calls.fetch_add(1, Ordering::SeqCst);
    None
}

async fn token_handler(State(fake): State<FakeGmail>, RawForm(body): RawForm) -> Response {
    let params: HashMap<String, String> = url::form_urlencoded::parse(&body).into_owned().collect();
    let invalid_grant = || {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_grant"})),
        )
            .into_response()
    };
    let mut inner = fake.inner.lock().unwrap();
    if params.get("client_id").map(String::as_str) != Some(CLIENT_ID)
        || params.get("client_secret").map(String::as_str) != Some(CLIENT_SECRET)
    {
        return invalid_grant();
    }
    match params.get("grant_type").map(String::as_str) {
        Some("refresh_token") => {
            if inner.grant_revoked
                || params.get("refresh_token").map(String::as_str) != Some(REFRESH_TOKEN)
            {
                return invalid_grant();
            }
            inner.token_seq += 1;
            let token = format!("at-{}", inner.token_seq);
            inner.valid_tokens.push(token.clone());
            let expires_in = inner.expires_in;
            drop(inner);
            fake.token_refreshes.fetch_add(1, Ordering::SeqCst);
            Json(json!({
                "access_token": token,
                "expires_in": expires_in,
                "token_type": "Bearer",
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
            }))
            .into_response()
        }
        Some("authorization_code") => {
            let code = params.get("code").map(String::as_str).unwrap_or("");
            let position = inner.pending_codes.iter().position(|c| c == code);
            let Some(position) = position else {
                return invalid_grant();
            };
            if params.get("code_verifier").map(String::is_empty) != Some(false)
                || params.get("redirect_uri").map(String::is_empty) != Some(false)
            {
                return invalid_grant();
            }
            inner.pending_codes.remove(position); // single-use
            inner.token_seq += 1;
            let token = format!("at-{}", inner.token_seq);
            inner.valid_tokens.push(token.clone());
            let mut body = json!({
                "access_token": token,
                "expires_in": inner.expires_in,
                "token_type": "Bearer",
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
            });
            if !inner.omit_refresh_token {
                body["refresh_token"] = json!(REFRESH_TOKEN);
            }
            Json(body).into_response()
        }
        _ => invalid_grant(),
    }
}

async fn profile_handler(State(fake): State<FakeGmail>, headers: HeaderMap) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    let inner = fake.inner.lock().unwrap();
    Json(json!({
        "emailAddress": EMAIL,
        "messagesTotal": inner.messages.len(),
        // Number on purpose: exercises tolerant deserialization.
        "historyId": inner.seq,
    }))
    .into_response()
}

async fn list_handler(
    State(fake): State<FakeGmail>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    let inner = fake.inner.lock().unwrap();

    let offset = match params.get("pageToken") {
        Some(token) => {
            if inner.reject_page_tokens {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": {"code": 400, "message": "Invalid pageToken"}})),
                )
                    .into_response();
            }
            match token
                .strip_prefix("pt-")
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(o) => o,
                None => return StatusCode::BAD_REQUEST.into_response(),
            }
        }
        None => 0,
    };
    let max_results = params
        .get("maxResults")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);

    // Gmail search-operator subset: "after:<secs>", "before:<secs>".
    let (mut after, mut before) = (None, None);
    if let Some(q) = params.get("q") {
        for term in q.split_whitespace() {
            if let Some(v) = term.strip_prefix("after:") {
                after = v.parse::<i64>().ok();
            } else if let Some(v) = term.strip_prefix("before:") {
                before = v.parse::<i64>().ok();
            }
        }
    }

    let include_spam_trash = params.get("includeSpamTrash").map(String::as_str) == Some("true");
    let mut messages: Vec<&FakeGmailMessage> = inner
        .messages
        .values()
        .filter(|m| {
            (include_spam_trash || !m.label_ids.iter().any(|l| l == "SPAM" || l == "TRASH"))
                && after.is_none_or(|a| m.internal_date >= a * 1000)
                && before.is_none_or(|b| m.internal_date < b * 1000)
        })
        .collect();
    // Newest first, like the real API.
    messages.sort_by(|a, b| {
        b.internal_date
            .cmp(&a.internal_date)
            .then_with(|| b.id.cmp(&a.id))
    });

    let total = messages.len();
    let page: Vec<Value> = messages
        .iter()
        .skip(offset)
        .take(max_results)
        .map(|m| json!({"id": m.id, "threadId": m.thread_id}))
        .collect();
    let next = (offset + page.len() < total && !page.is_empty())
        .then(|| format!("pt-{}", offset + page.len()));

    let mut body = json!({"resultSizeEstimate": total});
    if !page.is_empty() {
        body["messages"] = json!(page);
    }
    if let Some(next) = next {
        body["nextPageToken"] = json!(next);
    }
    Json(body).into_response()
}

async fn get_handler(
    State(fake): State<FakeGmail>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    let inner = fake.inner.lock().unwrap();
    let Some(m) = inner.messages.get(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut body = json!({
        "id": m.id,
        "threadId": m.thread_id,
        "labelIds": m.label_ids,
        // String on purpose: matches the real API.
        "internalDate": m.internal_date.to_string(),
        "sizeEstimate": m.raw.len(),
        "historyId": inner.seq.to_string(),
    });
    if params.get("format").map(String::as_str) == Some("raw") {
        body["raw"] = json!(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&m.raw));
    }
    Json(body).into_response()
}

async fn history_handler(
    State(fake): State<FakeGmail>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    let inner = fake.inner.lock().unwrap();
    let Some(start) = params
        .get("startHistoryId")
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if start < inner.oldest_history {
        return StatusCode::NOT_FOUND.into_response();
    }
    let max_results = params
        .get("maxResults")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    let offset = match params.get("pageToken") {
        Some(t) => match t.strip_prefix("hpt-").and_then(|s| s.parse::<usize>().ok()) {
            Some(o) => o,
            None => return StatusCode::BAD_REQUEST.into_response(),
        },
        None => 0,
    };

    let labels_of = |id: &str| -> Vec<String> {
        inner
            .messages
            .get(id)
            .map(|m| m.label_ids.clone())
            .unwrap_or_default()
    };
    let records: Vec<Value> = inner
        .history
        .iter()
        .filter(|(seq, _)| *seq > start)
        .map(|(seq, change)| match change {
            Change::Added(id) => json!({
                "id": seq.to_string(),
                "messagesAdded": [{"message": {"id": id, "labelIds": labels_of(id)}}],
            }),
            Change::Deleted(id) => json!({
                "id": seq.to_string(),
                "messagesDeleted": [{"message": {"id": id}}],
            }),
            Change::Labels(id, added, removed) => {
                let mut record = json!({"id": seq.to_string()});
                if !added.is_empty() {
                    record["labelsAdded"] = json!([{
                        "message": {"id": id, "labelIds": labels_of(id)},
                        "labelIds": added,
                    }]);
                }
                if !removed.is_empty() {
                    record["labelsRemoved"] = json!([{
                        "message": {"id": id, "labelIds": labels_of(id)},
                        "labelIds": removed,
                    }]);
                }
                record
            }
        })
        .collect();

    let total = records.len();
    let page: Vec<Value> = records
        .iter()
        .skip(offset)
        .take(max_results)
        .cloned()
        .collect();
    let mut body = json!({
        // Number on purpose (the real history.list sends a string; the
        // client must accept both).
        "historyId": inner.seq,
    });
    if !page.is_empty() {
        body["history"] = json!(page);
    }
    if offset + page.len() < total {
        body["nextPageToken"] = json!(format!("hpt-{}", offset + page.len()));
    }
    Json(body).into_response()
}
