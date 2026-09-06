//! In-process Microsoft Graph fixture: an in-memory mailbox with a
//! folder tree and a per-folder change log, serving the token endpoint,
//! /me, the mailFolders tree, per-folder message delta, and message
//! metadata + $value — enough surface to exercise the real client and
//! the sync engine without Microsoft credentials.
//!
//! Refresh tokens rotate on every redemption like the real endpoint;
//! every API handler checks for the ImmutableId preference the client
//! must send and counts violations.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, RawForm, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

pub const CLIENT_ID: &str = "00000000-0000-0000-0000-00000000fake";
pub const CLIENT_SECRET: &str = "fake-microsoft-secret";
/// The refresh token a code exchange hands out first; rotation issues
/// rt-1, rt-2, ... after it.
pub const REFRESH_TOKEN: &str = "rt-0";
pub const UPN: &str = "owner@contoso.onmicrosoft.com";
pub const MAIL: &str = "owner@contoso.com";

#[derive(Debug, Clone)]
pub struct FakeFolder {
    pub id: String,
    pub display_name: String,
    pub parent_id: Option<String>,
    pub well_known: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FakeMessage {
    pub id: String,
    pub folder_id: String,
    pub is_read: bool,
    pub flagged: bool,
    pub is_draft: bool,
    pub received: DateTime<Utc>,
    pub raw: Vec<u8>,
}

type DeltaHook = Box<dyn Fn(&str, &FakeO365) + Send + Sync>;

struct Inner {
    next_id: u64,
    /// Change sequence; every mutation bumps it.
    seq: u64,
    /// Delta tokens older than this answer 410.
    expired_before: u64,
    folders: HashMap<String, FakeFolder>,
    messages: HashMap<String, FakeMessage>,
    /// (seq, folder id, message id): the folder(s) a change touched.
    log: Vec<(u64, String, String)>,
    /// (count, status, body) — fail the next N API requests this way.
    fail_next: Option<(u32, u16, String)>,
    reject_filter: bool,
    /// Access tokens currently accepted by the API.
    valid_tokens: Vec<String>,
    token_seq: u64,
    expires_in: u64,
    grant_revoked: bool,
    /// Every refresh token issued so far, oldest first.
    refresh_tokens: Vec<String>,
    invalidate_previous_refresh: bool,
    pending_codes: Vec<String>,
    omit_refresh_token: bool,
    /// Called (folder id) right before a delta page is computed; lets a
    /// test mutate the mailbox between two folders' reads.
    on_delta: Option<DeltaHook>,
}

#[derive(Clone)]
pub struct FakeO365 {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
    api_calls: Arc<AtomicU64>,
    token_refreshes: Arc<AtomicU64>,
    value_fetches: Arc<AtomicU64>,
    meta_fetches: Arc<AtomicU64>,
    prefer_violations: Arc<AtomicU64>,
}

impl FakeO365 {
    /// Start with the standard Exchange folder set: Inbox, Sent Items,
    /// Drafts, Archive, Deleted Items, Junk Email.
    pub async fn start() -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            next_id: 1,
            seq: 1,
            expired_before: 0,
            folders: HashMap::new(),
            messages: HashMap::new(),
            log: Vec::new(),
            fail_next: None,
            reject_filter: false,
            valid_tokens: Vec::new(),
            token_seq: 0,
            expires_in: 3600,
            grant_revoked: false,
            refresh_tokens: vec![REFRESH_TOKEN.to_string()],
            invalidate_previous_refresh: false,
            pending_codes: Vec::new(),
            omit_refresh_token: false,
            on_delta: None,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fake = Self {
            addr,
            inner,
            api_calls: Arc::new(AtomicU64::new(0)),
            token_refreshes: Arc::new(AtomicU64::new(0)),
            value_fetches: Arc::new(AtomicU64::new(0)),
            meta_fetches: Arc::new(AtomicU64::new(0)),
            prefer_violations: Arc::new(AtomicU64::new(0)),
        };
        for (name, well_known) in [
            ("Inbox", "inbox"),
            ("Sent Items", "sentitems"),
            ("Drafts", "drafts"),
            ("Archive", "archive"),
            ("Deleted Items", "deleteditems"),
            ("Junk Email", "junkemail"),
        ] {
            fake.add_folder(name, None, Some(well_known));
        }
        let app = Router::new()
            .route("/token", post(token_handler))
            .route("/v1.0/me", get(me_handler))
            .route("/v1.0/me/mailFolders", get(folders_handler))
            .route("/v1.0/me/mailFolders/{id}", get(folder_handler))
            .route(
                "/v1.0/me/mailFolders/{id}/childFolders",
                get(child_folders_handler),
            )
            .route(
                "/v1.0/me/mailFolders/{id}/messages/delta",
                get(delta_handler),
            )
            .route("/v1.0/me/messages/{id}", get(message_handler))
            .route("/v1.0/me/messages/{id}/$value", get(value_handler))
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
        format!("http://{}/v1.0", self.addr)
    }

    pub fn microsoft_config(&self) -> arkivo::config::MicrosoftConfig {
        arkivo::config::MicrosoftConfig {
            client_id: CLIENT_ID.into(),
            client_secret: CLIENT_SECRET.into(),
            tenant: "common".into(),
            // The consent page is never fetched by the app (the browser
            // is redirected there), so any URL will do for tests.
            auth_url: Some(format!("http://{}/oauth2/v2.0/authorize", self.addr)),
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

    pub fn value_fetches(&self) -> u64 {
        self.value_fetches.load(Ordering::SeqCst)
    }

    pub fn meta_fetches(&self) -> u64 {
        self.meta_fetches.load(Ordering::SeqCst)
    }

    pub fn prefer_violations(&self) -> u64 {
        self.prefer_violations.load(Ordering::SeqCst)
    }

    /// Every refresh token issued so far, oldest first.
    pub fn refresh_tokens_issued(&self) -> Vec<String> {
        self.inner.lock().unwrap().refresh_tokens.clone()
    }

    /// Fail the next `n` API requests with 429.
    pub fn fail_next(&self, n: u32) {
        self.inner.lock().unwrap().fail_next = Some((n, 429, "slow down".into()));
    }

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

    /// When set, a refresh token stops working the moment a newer one
    /// is issued (the strictest rotation policy).
    pub fn invalidate_previous_refresh_tokens(&self, strict: bool) {
        self.inner.lock().unwrap().invalidate_previous_refresh = strict;
    }

    /// Make every delta/skip token issued so far answer 410.
    pub fn expire_delta_tokens(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        inner.expired_before = inner.seq;
    }

    /// Answer 400 to any delta request carrying a $filter.
    pub fn reject_filter(&self, reject: bool) {
        self.inner.lock().unwrap().reject_filter = reject;
    }

    pub fn expect_auth_code(&self, code: &str) {
        self.inner
            .lock()
            .unwrap()
            .pending_codes
            .push(code.to_string());
    }

    pub fn omit_refresh_token(&self, omit: bool) {
        self.inner.lock().unwrap().omit_refresh_token = omit;
    }

    /// Run `f(folder_id)` right before each delta page is computed.
    pub fn on_delta(&self, f: impl Fn(&str, &FakeO365) + Send + Sync + 'static) {
        self.inner.lock().unwrap().on_delta = Some(Box::new(f));
    }

    pub fn folder_id(&self, well_known: &str) -> String {
        self.inner
            .lock()
            .unwrap()
            .folders
            .values()
            .find(|f| f.well_known.as_deref() == Some(well_known))
            .map(|f| f.id.clone())
            .expect("well-known folder")
    }

    pub fn add_folder(&self, name: &str, parent: Option<&str>, well_known: Option<&str>) -> String {
        let mut inner = self.inner.lock().unwrap();
        let id = format!("f{}", inner.next_id);
        inner.next_id += 1;
        inner.seq += 1;
        inner.folders.insert(
            id.clone(),
            FakeFolder {
                id: id.clone(),
                display_name: name.to_string(),
                parent_id: parent.map(String::from),
                well_known: well_known.map(String::from),
            },
        );
        id
    }

    pub fn rename_folder(&self, id: &str, name: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        if let Some(f) = inner.folders.get_mut(id) {
            f.display_name = name.to_string();
        }
    }

    /// Delete a folder: `purge` removes it and its messages outright;
    /// otherwise it is re-parented under Deleted Items (what Outlook
    /// does), keeping its id.
    pub fn delete_folder(&self, id: &str, purge: bool) {
        let deleted_items = self.folder_id("deleteditems");
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        if purge {
            inner.folders.remove(id);
            let doomed: Vec<String> = inner
                .messages
                .values()
                .filter(|m| m.folder_id == id)
                .map(|m| m.id.clone())
                .collect();
            for mid in doomed {
                inner.messages.remove(&mid);
                inner.log.push((seq, id.to_string(), mid));
            }
        } else if let Some(f) = inner.folders.get_mut(id) {
            f.parent_id = Some(deleted_items);
        }
    }

    pub fn add_message(
        &self,
        folder_id: &str,
        subject: &str,
        from: &str,
        received: DateTime<Utc>,
    ) -> String {
        let mut inner = self.inner.lock().unwrap();
        let id = format!("AAMk-{}=", inner.next_id);
        inner.next_id += 1;
        inner.seq += 1;
        let seq = inner.seq;
        let raw = format!(
            "From: {from}\r\nTo: {MAIL}\r\nSubject: {subject}\r\n\
             Message-ID: <{}@fake.o365>\r\nDate: {}\r\n\r\nBody of {subject}.\r\n",
            inner.next_id,
            received.to_rfc2822()
        )
        .into_bytes();
        inner.messages.insert(
            id.clone(),
            FakeMessage {
                id: id.clone(),
                folder_id: folder_id.to_string(),
                is_read: false,
                flagged: false,
                is_draft: false,
                received,
                raw,
            },
        );
        inner.log.push((seq, folder_id.to_string(), id.clone()));
        id
    }

    fn touch(&self, id: &str, f: impl FnOnce(&mut FakeMessage)) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        let Some(m) = inner.messages.get_mut(id) else {
            return;
        };
        f(m);
        let folder = m.folder_id.clone();
        inner.log.push((seq, folder, id.to_string()));
    }

    pub fn set_read(&self, id: &str, read: bool) {
        self.touch(id, |m| m.is_read = read);
    }

    pub fn set_flagged(&self, id: &str, flagged: bool) {
        self.touch(id, |m| m.flagged = flagged);
    }

    pub fn move_message(&self, id: &str, folder_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        let Some(m) = inner.messages.get_mut(id) else {
            return;
        };
        let from = std::mem::replace(&mut m.folder_id, folder_id.to_string());
        inner.log.push((seq, from, id.to_string()));
        inner.log.push((seq, folder_id.to_string(), id.to_string()));
    }

    pub fn destroy(&self, id: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        if let Some(m) = inner.messages.remove(id) {
            inner.log.push((seq, m.folder_id, id.to_string()));
        }
    }

    pub fn message_count(&self) -> usize {
        self.inner.lock().unwrap().messages.len()
    }

    pub fn raw_of(&self, id: &str) -> Vec<u8> {
        self.inner.lock().unwrap().messages[id].raw.clone()
    }
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string)
}

/// Shared API-request gate: scripted failures, then auth, then the
/// ImmutableId preference check.
fn gate(fake: &FakeO365, headers: &HeaderMap) -> Option<Response> {
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
    let prefers_immutable = headers
        .get_all("prefer")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.contains("IdType=\"ImmutableId\""));
    if !prefers_immutable {
        fake.prefer_violations.fetch_add(1, Ordering::SeqCst);
    }
    fake.api_calls.fetch_add(1, Ordering::SeqCst);
    None
}

fn max_page_size(headers: &HeaderMap, default: usize) -> usize {
    headers
        .get_all("prefer")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|part| part.trim().strip_prefix("odata.maxpagesize="))
        .filter_map(|n| n.parse::<usize>().ok())
        .next()
        .unwrap_or(default)
}

async fn token_handler(State(fake): State<FakeO365>, RawForm(body): RawForm) -> Response {
    let params: HashMap<String, String> = url::form_urlencoded::parse(&body).into_owned().collect();
    let invalid_grant = || {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_grant",
                "error_description": "AADSTS70008: The refresh token has expired.",
                "error_codes": [70008],
            })),
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
            let presented = params.get("refresh_token").cloned().unwrap_or_default();
            let accepted = if inner.invalidate_previous_refresh {
                inner.refresh_tokens.last() == Some(&presented)
            } else {
                inner.refresh_tokens.contains(&presented)
            };
            if inner.grant_revoked || !accepted {
                return invalid_grant();
            }
            if params.get("scope").map(String::is_empty) != Some(false) {
                return (StatusCode::BAD_REQUEST, "scope required").into_response();
            }
            inner.token_seq += 1;
            let token = format!("at-{}", inner.token_seq);
            let refresh = format!("rt-{}", inner.token_seq);
            inner.valid_tokens.push(token.clone());
            inner.refresh_tokens.push(refresh.clone());
            let expires_in = inner.expires_in;
            drop(inner);
            fake.token_refreshes.fetch_add(1, Ordering::SeqCst);
            Json(json!({
                "access_token": token,
                "refresh_token": refresh,
                "expires_in": expires_in,
                "token_type": "Bearer",
                "scope": "Mail.Read User.Read",
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
                "scope": "Mail.Read User.Read",
            });
            if !inner.omit_refresh_token {
                body["refresh_token"] = json!(REFRESH_TOKEN);
            }
            Json(body).into_response()
        }
        _ => invalid_grant(),
    }
}

async fn me_handler(State(fake): State<FakeO365>, headers: HeaderMap) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    Json(json!({
        "id": "user-1",
        "userPrincipalName": UPN,
        "mail": MAIL,
    }))
    .into_response()
}

fn folder_json(f: &FakeFolder, inner: &Inner) -> Value {
    let child_count = inner
        .folders
        .values()
        .filter(|c| c.parent_id.as_deref() == Some(&f.id))
        .count();
    let total = inner
        .messages
        .values()
        .filter(|m| m.folder_id == f.id)
        .count();
    json!({
        "id": f.id,
        "displayName": f.display_name,
        "parentFolderId": f.parent_id.clone().unwrap_or_else(|| "root".into()),
        "childFolderCount": child_count,
        "totalItemCount": total,
        "unreadItemCount": 0,
    })
}

fn folder_page(
    fake: &FakeO365,
    parent: Option<&str>,
    path: &str,
    params: &HashMap<String, String>,
) -> Response {
    let inner = fake.inner.lock().unwrap();
    let mut folders: Vec<&FakeFolder> = inner
        .folders
        .values()
        .filter(|f| f.parent_id.as_deref() == parent)
        .collect();
    folders.sort_by(|a, b| a.id.cmp(&b.id));
    let top = params
        .get("$top")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);
    let skip = params
        .get("$skip")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let page: Vec<Value> = folders
        .iter()
        .skip(skip)
        .take(top)
        .map(|f| folder_json(f, &inner))
        .collect();
    let mut body = json!({"value": page});
    if skip + top < folders.len() {
        body["@odata.nextLink"] = json!(format!(
            "http://{}{}?$top={}&$skip={}",
            fake.addr,
            path,
            top,
            skip + top
        ));
    }
    Json(body).into_response()
}

async fn folders_handler(
    State(fake): State<FakeO365>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    folder_page(&fake, None, "/v1.0/me/mailFolders", &params)
}

async fn child_folders_handler(
    State(fake): State<FakeO365>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    let path = format!("/v1.0/me/mailFolders/{id}/childFolders");
    folder_page(&fake, Some(&id), &path, &params)
}

/// `/me/mailFolders/{id}` accepts a folder id or a well-known name.
async fn folder_handler(
    State(fake): State<FakeO365>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    let inner = fake.inner.lock().unwrap();
    let found = inner.folders.get(&id).or_else(|| {
        inner
            .folders
            .values()
            .find(|f| f.well_known.as_deref() == Some(id.as_str()))
    });
    match found {
        Some(f) => Json(folder_json(f, &inner)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "ErrorFolderNotFound", "message": "no such folder"}})),
        )
            .into_response(),
    }
}

fn message_json(m: &FakeMessage) -> Value {
    json!({
        "id": m.id,
        "isRead": m.is_read,
        "isDraft": m.is_draft,
        "flag": {"flagStatus": if m.flagged { "flagged" } else { "notFlagged" }},
        "parentFolderId": m.folder_id,
        "receivedDateTime": m.received.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "conversationId": format!("conv-{}", m.id),
        "internetMessageId": format!("<{}@fake.o365>", m.id),
    })
}

fn tombstone(id: &str) -> Value {
    json!({"id": id, "@removed": {"reason": "deleted"}})
}

/// Delta token wire format (opaque to the client):
///   $skiptoken = "init:{offset}:{snapshot_seq}:{since}"  initial walk paging
///   $skiptoken = "inc:{offset}:{from_seq}:{since}"       incremental paging
///   $deltatoken = "{from_seq}:{since}"                    end of a cycle
/// `since` is epoch seconds or "-".
async fn delta_handler(
    State(fake): State<FakeO365>,
    headers: HeaderMap,
    Path(folder_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    // Test hook: mutate the mailbox between two folders' reads.
    let hook = fake.inner.lock().unwrap().on_delta.take();
    if let Some(hook) = hook {
        hook(&folder_id, &fake);
        fake.inner.lock().unwrap().on_delta = Some(hook);
    }
    let page_size = max_page_size(&headers, 10);
    let inner = fake.inner.lock().unwrap();

    let parse_since = |s: &str| -> Option<DateTime<Utc>> {
        if s == "-" {
            None
        } else {
            s.parse::<i64>()
                .ok()
                .and_then(|t| DateTime::from_timestamp(t, 0))
        }
    };
    let fmt_since = |s: Option<DateTime<Utc>>| -> String {
        s.map(|d| d.timestamp().to_string())
            .unwrap_or_else(|| "-".into())
    };
    let expired = |seq: u64| -> Option<Response> {
        (seq < inner.expired_before).then(|| {
            (
                StatusCode::GONE,
                Json(json!({"error": {"code": "SyncStateNotFound",
                    "message": "The sync state is not found; resync required."}})),
            )
                .into_response()
        })
    };

    enum Mode {
        Initial { offset: usize, snapshot: u64 },
        Incremental { offset: usize, from: u64 },
    }
    let (mode, since) = if let Some(token) = params.get("$skiptoken") {
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() != 4 {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let offset = parts[1].parse::<usize>().unwrap_or(0);
        let seq = parts[2].parse::<u64>().unwrap_or(0);
        if let Some(r) = expired(seq) {
            return r;
        }
        let mode = match parts[0] {
            "init" => Mode::Initial {
                offset,
                snapshot: seq,
            },
            "inc" => Mode::Incremental { offset, from: seq },
            _ => return StatusCode::BAD_REQUEST.into_response(),
        };
        (mode, parse_since(parts[3]))
    } else if let Some(token) = params.get("$deltatoken") {
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() != 2 {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let from = parts[0].parse::<u64>().unwrap_or(0);
        if let Some(r) = expired(from) {
            return r;
        }
        (Mode::Incremental { offset: 0, from }, parse_since(parts[1]))
    } else {
        let since = match params.get("$filter") {
            Some(filter) => {
                if inner.reject_filter {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": {"code": "ErrorInvalidUrlQueryFilter",
                            "message": "The query filter contains one or more invalid nodes."}})),
                    )
                        .into_response();
                }
                let Some(ts) = filter.strip_prefix("receivedDateTime ge ") else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                match DateTime::parse_from_rfc3339(ts) {
                    Ok(d) => Some(d.with_timezone(&Utc)),
                    Err(_) => return StatusCode::BAD_REQUEST.into_response(),
                }
            }
            None => None,
        };
        (
            Mode::Initial {
                offset: 0,
                snapshot: inner.seq,
            },
            since,
        )
    };
    if !inner.folders.contains_key(&folder_id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let qualifies = |m: &FakeMessage| since.is_none_or(|floor| m.received >= floor);
    let (items, offset, end_seq, prefix): (Vec<Value>, usize, u64, &str) = match mode {
        Mode::Initial { offset, snapshot } => {
            let mut msgs: Vec<&FakeMessage> = inner
                .messages
                .values()
                .filter(|m| m.folder_id == folder_id && qualifies(m))
                .collect();
            msgs.sort_by(|a, b| a.id.cmp(&b.id));
            (
                msgs.into_iter().map(message_json).collect(),
                offset,
                snapshot,
                "init",
            )
        }
        Mode::Incremental { offset, from } => {
            let mut touched: Vec<String> = Vec::new();
            for (seq, folder, mid) in &inner.log {
                if *seq > from && folder == &folder_id && !touched.contains(mid) {
                    touched.push(mid.clone());
                }
            }
            let items = touched
                .iter()
                .filter_map(|mid| match inner.messages.get(mid) {
                    Some(m) if m.folder_id == folder_id => qualifies(m).then(|| message_json(m)),
                    _ => Some(tombstone(mid)),
                })
                .collect();
            (items, offset, from, "inc")
        }
    };

    let page: Vec<Value> = items.iter().skip(offset).take(page_size).cloned().collect();
    let base = format!(
        "http://{}/v1.0/me/mailFolders/{}/messages/delta",
        fake.addr, folder_id
    );
    let mut body = json!({"value": page});
    if offset + page.len() < items.len() {
        body["@odata.nextLink"] = json!(format!(
            "{base}?$skiptoken={prefix}:{}:{}:{}",
            offset + page.len(),
            end_seq,
            fmt_since(since)
        ));
    } else {
        // The next cycle starts from the snapshot (initial) or from now.
        let next_from = match prefix {
            "init" => end_seq,
            _ => inner.seq,
        };
        body["@odata.deltaLink"] = json!(format!(
            "{base}?$deltatoken={next_from}:{}",
            fmt_since(since)
        ));
    }
    Json(body).into_response()
}

async fn message_handler(
    State(fake): State<FakeO365>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    fake.meta_fetches.fetch_add(1, Ordering::SeqCst);
    let inner = fake.inner.lock().unwrap();
    match inner.messages.get(&id) {
        Some(m) => Json(message_json(m)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "ErrorItemNotFound", "message": "gone"}})),
        )
            .into_response(),
    }
}

async fn value_handler(
    State(fake): State<FakeO365>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = gate(&fake, &headers) {
        return response;
    }
    fake.value_fetches.fetch_add(1, Ordering::SeqCst);
    let inner = fake.inner.lock().unwrap();
    match inner.messages.get(&id) {
        Some(m) => ([(header::CONTENT_TYPE, "message/rfc822")], m.raw.clone()).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "ErrorItemNotFound", "message": "gone"}})),
        )
            .into_response(),
    }
}
