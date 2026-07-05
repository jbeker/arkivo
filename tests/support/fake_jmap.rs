//! In-process JMAP server fixture: an in-memory mailbox with a scriptable
//! change log, serving /session, /api (with back-reference resolution),
//! and /download — enough surface to exercise the real client and the
//! sync engine without Fastmail credentials.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

pub const TOKEN: &str = "fake-jmap-token";
pub const ACCOUNT_ID: &str = "acc1";

#[derive(Debug, Clone)]
pub struct FakeMessage {
    pub id: String,
    pub blob_id: String,
    pub subject: String,
    pub from: String,
    pub received_at: DateTime<Utc>,
    pub mailbox_ids: Vec<String>,
    pub keywords: Vec<String>,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ChangeKind {
    Created,
    Updated,
    Destroyed,
}

struct Inner {
    seq: u64,
    /// States older than this are "too old": Email/changes refuses them.
    oldest_state: u64,
    next_id: u64,
    messages: HashMap<String, FakeMessage>,
    change_log: Vec<(u64, ChangeKind, String)>,
    fail_next: u32,
    max_objects_in_get: u64,
}

#[derive(Clone)]
pub struct FakeJmap {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
    api_calls: Arc<AtomicU64>,
    download_calls: Arc<AtomicU64>,
}

impl FakeJmap {
    pub async fn start() -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            seq: 0,
            oldest_state: 0,
            next_id: 1,
            messages: HashMap::new(),
            change_log: Vec::new(),
            fail_next: 0,
            max_objects_in_get: 100,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fake = Self {
            addr,
            inner,
            api_calls: Arc::new(AtomicU64::new(0)),
            download_calls: Arc::new(AtomicU64::new(0)),
        };
        let app = Router::new()
            .route("/session", get(session_handler))
            .route("/api", post(api_handler))
            .route(
                "/download/{account_id}/{blob_id}/{name}",
                get(download_handler),
            )
            .with_state(fake.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        fake
    }

    pub fn session_url(&self) -> String {
        format!("http://{}/session", self.addr)
    }

    pub fn token(&self) -> &'static str {
        TOKEN
    }

    /// Current JMAP state string ("s<seq>").
    pub fn current_state(&self) -> String {
        format!("s{}", self.inner.lock().unwrap().seq)
    }

    /// Number of POST /api round trips served so far.
    pub fn api_calls(&self) -> u64 {
        self.api_calls.load(Ordering::SeqCst)
    }

    pub fn download_calls(&self) -> u64 {
        self.download_calls.load(Ordering::SeqCst)
    }

    /// Respond 429 to the next `n` /api or /download requests.
    pub fn fail_next(&self, n: u32) {
        self.inner.lock().unwrap().fail_next = n;
    }

    pub fn set_max_objects_in_get(&self, n: u64) {
        self.inner.lock().unwrap().max_objects_in_get = n;
    }

    /// Make every previously issued state too old to diff from, forcing
    /// cannotCalculateChanges on the next Email/changes.
    pub fn invalidate_state(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        inner.oldest_state = inner.seq;
    }

    pub fn add_message(&self, subject: &str, from: &str, received_at: DateTime<Utc>) -> String {
        let mut inner = self.inner.lock().unwrap();
        let id = format!("m{}", inner.next_id);
        inner.next_id += 1;
        inner.seq += 1;
        let seq = inner.seq;
        let raw = format!(
            "From: {from}\r\nTo: owner@example.com\r\nSubject: {subject}\r\n\
             Message-ID: <{id}@fake.example>\r\nDate: {}\r\n\r\nBody of {subject}.\r\n",
            received_at.to_rfc2822()
        )
        .into_bytes();
        let message = FakeMessage {
            id: id.clone(),
            blob_id: format!("blob-{id}"),
            subject: subject.to_string(),
            from: from.to_string(),
            received_at,
            mailbox_ids: vec!["inbox".to_string()],
            keywords: Vec::new(),
            raw,
        };
        inner.messages.insert(id.clone(), message);
        inner
            .change_log
            .push((seq, ChangeKind::Created, id.clone()));
        id
    }

    /// Flag change (e.g. marked seen): records an `updated` entry.
    pub fn update_message(&self, id: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        if let Some(m) = inner.messages.get_mut(id) {
            m.keywords.push("$seen".to_string());
        }
        inner
            .change_log
            .push((seq, ChangeKind::Updated, id.to_string()));
    }

    pub fn destroy(&self, id: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        inner.messages.remove(id);
        inner
            .change_log
            .push((seq, ChangeKind::Destroyed, id.to_string()));
    }

    pub fn message_count(&self) -> usize {
        self.inner.lock().unwrap().messages.len()
    }
}

fn authorized(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {TOKEN}"))
        .unwrap_or(false)
}

async fn session_handler(State(fake): State<FakeJmap>, headers: HeaderMap) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let max_get = fake.inner.lock().unwrap().max_objects_in_get;
    let base = format!("http://{}", fake.addr);
    Json(json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxObjectsInGet": max_get,
                "maxCallsInRequest": 16,
                "maxSizeRequest": 10_000_000,
            },
            "urn:ietf:params:jmap:mail": {},
        },
        "accounts": { ACCOUNT_ID: {"name": "owner@example.com"} },
        "primaryAccounts": { "urn:ietf:params:jmap:mail": ACCOUNT_ID },
        "apiUrl": format!("{base}/api"),
        "downloadUrl": format!("{base}/download/{{accountId}}/{{blobId}}/{{name}}?type={{type}}"),
        "state": fake.current_state(),
    }))
    .into_response()
}

async fn download_handler(
    State(fake): State<FakeJmap>,
    headers: HeaderMap,
    Path((_account_id, blob_id, _name)): Path<(String, String, String)>,
) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    {
        let mut inner = fake.inner.lock().unwrap();
        if inner.fail_next > 0 {
            inner.fail_next -= 1;
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "0")],
                "slow down",
            )
                .into_response();
        }
    }
    fake.download_calls.fetch_add(1, Ordering::SeqCst);
    let inner = fake.inner.lock().unwrap();
    match inner.messages.values().find(|m| m.blob_id == blob_id) {
        Some(m) => m.raw.clone().into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn api_handler(
    State(fake): State<FakeJmap>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    {
        let mut inner = fake.inner.lock().unwrap();
        if inner.fail_next > 0 {
            inner.fail_next -= 1;
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "0")],
                "slow down",
            )
                .into_response();
        }
    }
    fake.api_calls.fetch_add(1, Ordering::SeqCst);

    let empty = Vec::new();
    let calls = body
        .get("methodCalls")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    let mut responses: Vec<Value> = Vec::new();
    let mut by_call_id: HashMap<String, Value> = HashMap::new();

    for call in calls {
        let (Some(name), Some(args), Some(call_id)) = (
            call.get(0).and_then(Value::as_str),
            call.get(1),
            call.get(2).and_then(Value::as_str),
        ) else {
            continue;
        };
        let args = match resolve_backrefs(args, &by_call_id) {
            Ok(args) => args,
            Err(err_type) => {
                responses.push(json!(["error", {"type": err_type}, call_id]));
                continue;
            }
        };
        let inner = fake.inner.lock().unwrap();
        let (response_name, response_args) = match name {
            "Email/changes" => email_changes(&inner, &args),
            "Email/get" => email_get(&inner, &args),
            "Email/query" => email_query(&inner, &args),
            _ => ("error".into(), json!({"type": "unknownMethod"})),
        };
        drop(inner);
        if response_name != "error" {
            by_call_id.insert(call_id.to_string(), response_args.clone());
        }
        responses.push(json!([response_name, response_args, call_id]));
    }

    Json(json!({
        "methodResponses": responses,
        "sessionState": fake.current_state(),
    }))
    .into_response()
}

/// Resolve `#ids` result references against earlier responses in the same
/// request (RFC 8620 §3.7).
fn resolve_backrefs(
    args: &Value,
    by_call_id: &HashMap<String, Value>,
) -> Result<Value, &'static str> {
    let mut resolved = args.clone();
    let Some(obj) = resolved.as_object_mut() else {
        return Ok(resolved);
    };
    let backref_keys: Vec<String> = obj.keys().filter(|k| k.starts_with('#')).cloned().collect();
    for key in backref_keys {
        let reference = obj.remove(&key).unwrap();
        let (Some(result_of), Some(path)) = (
            reference.get("resultOf").and_then(Value::as_str),
            reference.get("path").and_then(Value::as_str),
        ) else {
            return Err("invalidResultReference");
        };
        let source = by_call_id.get(result_of).ok_or("invalidResultReference")?;
        let value = source
            .pointer(path)
            .cloned()
            .ok_or("invalidResultReference")?;
        obj.insert(key[1..].to_string(), value);
    }
    Ok(resolved)
}

fn email_to_json(m: &FakeMessage) -> Value {
    let mailbox_ids: HashMap<&str, bool> =
        m.mailbox_ids.iter().map(|id| (id.as_str(), true)).collect();
    let keywords: HashMap<&str, bool> = m.keywords.iter().map(|k| (k.as_str(), true)).collect();
    json!({
        "id": m.id,
        "blobId": m.blob_id,
        "threadId": format!("t-{}", m.id),
        "messageId": [format!("{}@fake.example", m.id)],
        "mailboxIds": mailbox_ids,
        "keywords": keywords,
        "receivedAt": m.received_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "size": m.raw.len(),
        "hasAttachment": false,
        "from": [{"name": null, "email": m.from}],
        "subject": m.subject,
    })
}

fn email_changes(inner: &Inner, args: &Value) -> (String, Value) {
    let since = args
        .get("sinceState")
        .and_then(Value::as_str)
        .and_then(|s| s.strip_prefix('s'))
        .and_then(|s| s.parse::<u64>().ok());
    let Some(since) = since else {
        return ("error".into(), json!({"type": "invalidArguments"}));
    };
    if since < inner.oldest_state {
        return ("error".into(), json!({"type": "cannotCalculateChanges"}));
    }
    let max_changes = args
        .get("maxChanges")
        .and_then(Value::as_u64)
        .unwrap_or(100)
        .max(1) as usize;

    // Merge log entries per message in sequence order, capped at
    // max_changes distinct ids; newState reflects the last consumed seq.
    let mut merged: Vec<(String, ChangeKind)> = Vec::new();
    let mut last_seq = since;
    let mut has_more = false;
    for (seq, kind, id) in inner.change_log.iter().filter(|(s, _, _)| *s > since) {
        if let Some(entry) = merged.iter_mut().find(|(mid, _)| mid == id) {
            entry.1 = match (entry.1, *kind) {
                (ChangeKind::Created, ChangeKind::Updated) => ChangeKind::Created,
                (_, new) => new,
            };
        } else {
            if merged.len() >= max_changes {
                has_more = true;
                break;
            }
            merged.push((id.clone(), *kind));
        }
        last_seq = *seq;
    }

    let collect = |wanted: ChangeKind| -> Vec<String> {
        merged
            .iter()
            .filter(|(_, k)| *k == wanted)
            .map(|(id, _)| id.clone())
            .collect()
    };
    (
        "Email/changes".into(),
        json!({
            "accountId": ACCOUNT_ID,
            "oldState": format!("s{since}"),
            "newState": format!("s{last_seq}"),
            "hasMoreChanges": has_more,
            "created": collect(ChangeKind::Created),
            "updated": collect(ChangeKind::Updated),
            "destroyed": collect(ChangeKind::Destroyed),
        }),
    )
}

fn email_get(inner: &Inner, args: &Value) -> (String, Value) {
    let empty = Vec::new();
    let ids = args.get("ids").and_then(Value::as_array).unwrap_or(&empty);
    if ids.len() > inner.max_objects_in_get as usize {
        return ("error".into(), json!({"type": "requestTooLarge"}));
    }
    let mut list = Vec::new();
    let mut not_found = Vec::new();
    for id in ids.iter().filter_map(Value::as_str) {
        match inner.messages.get(id) {
            Some(m) => list.push(email_to_json(m)),
            None => not_found.push(id.to_string()),
        }
    }
    (
        "Email/get".into(),
        json!({
            "accountId": ACCOUNT_ID,
            "state": format!("s{}", inner.seq),
            "list": list,
            "notFound": not_found,
        }),
    )
}

fn email_query(inner: &Inner, args: &Value) -> (String, Value) {
    let mut messages: Vec<&FakeMessage> = inner.messages.values().collect();
    if let Some(after) = args
        .get("filter")
        .and_then(|f| f.get("after"))
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
    {
        messages.retain(|m| m.received_at >= after);
    }
    messages.sort_by(|a, b| {
        a.received_at
            .cmp(&b.received_at)
            .then_with(|| a.id.cmp(&b.id))
    });

    let position = if let Some(anchor) = args.get("anchor").and_then(Value::as_str) {
        let offset = args
            .get("anchorOffset")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        match messages.iter().position(|m| m.id == anchor) {
            Some(idx) => (idx as i64 + offset).max(0) as usize,
            None => return ("error".into(), json!({"type": "anchorNotFound"})),
        }
    } else {
        args.get("position").and_then(Value::as_u64).unwrap_or(0) as usize
    };
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(inner.max_objects_in_get) as usize;

    let ids: Vec<String> = messages
        .iter()
        .skip(position)
        .take(limit)
        .map(|m| m.id.clone())
        .collect();
    (
        "Email/query".into(),
        json!({
            "accountId": ACCOUNT_ID,
            "queryState": format!("s{}", inner.seq),
            "ids": ids,
            "position": position,
            "total": messages.len(),
        }),
    )
}
