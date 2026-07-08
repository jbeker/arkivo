use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::types::*;

#[derive(Debug, thiserror::Error)]
pub enum JmapError {
    /// The stored state is too old for the server to compute a delta;
    /// the caller must fall back to a full Email/query resync (spec §7.3).
    #[error("server cannot calculate changes from the stored state")]
    CannotCalculateChanges,
    #[error("JMAP method error {error_type}: {description:?}")]
    Method {
        error_type: String,
        description: Option<String>,
    },
    #[error("HTTP status {0}")]
    Status(u16),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Blob downloads get a tighter budget than JMAP API calls. A message body
/// that hasn't finished arriving in this window is almost always a wedged
/// Fastmail blob, not a merely slow one — the global 120s client timeout
/// made each such blob cost minutes. Fail fast and defer to
/// `fetch_missing_blobs`, which keeps the sweep moving and cancellable.
const BLOB_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(45);
/// Retries (after the first attempt) for a single blob within a sweep.
/// A blob failing persistently won't recover this run; a full retry pass
/// happens later in `fetch_missing_blobs`.
const BLOB_MAX_RETRIES: u32 = 2;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay: Duration::from_secs(2),
        }
    }
}

/// Minimal JMAP client over reqwest. Holds the session fetched at connect
/// time; respects server-advertised limits; retries 429/503 with backoff.
pub struct JmapClient {
    http: reqwest::Client,
    token: String,
    session: Session,
    account_id: String,
    retry: RetryPolicy,
}

/// One page of backfill: the query slice plus fetched metadata.
#[derive(Debug)]
pub struct QueryPage {
    pub query: EmailQueryResponse,
    pub emails: Vec<Email>,
}

/// One poll cycle: the change lists plus metadata for created and updated,
/// fetched in the same round trip via result references.
#[derive(Debug)]
pub struct ChangesPage {
    pub changes: EmailChangesResponse,
    pub created: Vec<Email>,
    pub updated: Vec<Email>,
}

impl JmapClient {
    pub async fn connect(
        session_url: &str,
        token: &str,
        retry: RetryPolicy,
    ) -> Result<Self, JmapError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        let response = http
            .get(session_url)
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JmapError::Status(e.status().map(|s| s.as_u16()).unwrap_or(0)))?;
        let session: Session = response.json().await?;
        let account_id = session
            .mail_account_id()
            .ok_or_else(|| JmapError::Protocol("session has no primary mail account".into()))?
            .to_string();
        Ok(Self {
            http,
            token: token.to_string(),
            session,
            account_id,
            retry,
        })
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// Effective per-batch ceiling for Email/get and chained changes→get.
    pub fn batch_limit(&self) -> u64 {
        self.session.max_objects_in_get()
    }

    /// POST a JMAP request, retrying 429/503 with exponential backoff and
    /// honoring Retry-After. Returns the raw methodResponses array.
    async fn call(&self, method_calls: Value) -> Result<Vec<Value>, JmapError> {
        let body = json!({
            "using": [CAP_CORE, CAP_MAIL],
            "methodCalls": method_calls,
        });
        let mut attempt = 0u32;
        loop {
            let response = self
                .http
                .post(&self.session.api_url)
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await?;
            let status = response.status();
            if status.as_u16() == 429 || status.as_u16() == 503 {
                if attempt >= self.retry.max_retries {
                    return Err(JmapError::Status(status.as_u16()));
                }
                let delay = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs)
                    .unwrap_or_else(|| self.retry.base_delay * 2u32.pow(attempt));
                tracing::warn!(
                    status = status.as_u16(),
                    ?delay,
                    "rate limited, backing off"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
                continue;
            }
            if !status.is_success() {
                return Err(JmapError::Status(status.as_u16()));
            }
            let envelope: Value = response.json().await?;
            let responses = envelope
                .get("methodResponses")
                .and_then(Value::as_array)
                .ok_or_else(|| JmapError::Protocol("missing methodResponses".into()))?;
            return Ok(responses.clone());
        }
    }

    /// Extract the response for `call_id`, mapping `["error", ...]` to a
    /// typed error (cannotCalculateChanges gets its own variant).
    fn response_args(responses: &[Value], call_id: &str) -> Result<Value, JmapError> {
        for entry in responses {
            let (Some(name), Some(args), Some(id)) = (
                entry.get(0).and_then(Value::as_str),
                entry.get(1),
                entry.get(2).and_then(Value::as_str),
            ) else {
                continue;
            };
            if id != call_id {
                continue;
            }
            if name == "error" {
                let err: MethodError = serde_json::from_value(args.clone())
                    .map_err(|e| JmapError::Protocol(e.to_string()))?;
                if err.error_type == "cannotCalculateChanges" {
                    return Err(JmapError::CannotCalculateChanges);
                }
                return Err(JmapError::Method {
                    error_type: err.error_type,
                    description: err.description,
                });
            }
            return Ok(args.clone());
        }
        Err(JmapError::Protocol(format!(
            "no response for call {call_id}"
        )))
    }

    /// Email/changes chained with Email/get for created and updated IDs —
    /// one round trip per cycle (spec §7.1).
    pub async fn changes(&self, since_state: &str) -> Result<ChangesPage, JmapError> {
        let max_changes = self.batch_limit();
        let get_args = |path: &str| {
            json!({
                "accountId": self.account_id,
                "#ids": {"resultOf": "c0", "name": "Email/changes", "path": path},
                "properties": EMAIL_PROPERTIES,
            })
        };
        let responses = self
            .call(json!([
                ["Email/changes", {
                    "accountId": self.account_id,
                    "sinceState": since_state,
                    "maxChanges": max_changes,
                }, "c0"],
                ["Email/get", get_args("/created"), "c1"],
                ["Email/get", get_args("/updated"), "c2"],
            ]))
            .await?;

        let changes: EmailChangesResponse =
            serde_json::from_value(Self::response_args(&responses, "c0")?)
                .map_err(|e| JmapError::Protocol(e.to_string()))?;
        let created: EmailGetResponse =
            serde_json::from_value(Self::response_args(&responses, "c1")?)
                .map_err(|e| JmapError::Protocol(e.to_string()))?;
        let updated: EmailGetResponse =
            serde_json::from_value(Self::response_args(&responses, "c2")?)
                .map_err(|e| JmapError::Protocol(e.to_string()))?;
        Ok(ChangesPage {
            changes,
            created: created.list,
            updated: updated.list,
        })
    }

    /// One backfill page: Email/query (receivedAt ascending, anchor
    /// pagination) chained with Email/get, one round trip (spec §7.2).
    pub async fn query_page(
        &self,
        anchor: Option<&str>,
        limit: u64,
        since: Option<DateTime<Utc>>,
    ) -> Result<QueryPage, JmapError> {
        let limit = limit.min(self.batch_limit());
        let mut query_args = json!({
            "accountId": self.account_id,
            "sort": [{"property": "receivedAt", "isAscending": true}],
            "limit": limit,
        });
        match anchor {
            Some(anchor) => {
                query_args["anchor"] = json!(anchor);
                query_args["anchorOffset"] = json!(1);
            }
            None => {
                query_args["position"] = json!(0);
            }
        }
        if let Some(since) = since {
            query_args["filter"] =
                json!({"after": since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)});
        }
        let responses = self
            .call(json!([
                ["Email/query", query_args, "q0"],
                ["Email/get", {
                    "accountId": self.account_id,
                    "#ids": {"resultOf": "q0", "name": "Email/query", "path": "/ids"},
                    "properties": EMAIL_PROPERTIES,
                }, "q1"],
            ]))
            .await?;

        let query: EmailQueryResponse =
            serde_json::from_value(Self::response_args(&responses, "q0")?)
                .map_err(|e| JmapError::Protocol(e.to_string()))?;
        let emails: EmailGetResponse =
            serde_json::from_value(Self::response_args(&responses, "q1")?)
                .map_err(|e| JmapError::Protocol(e.to_string()))?;
        Ok(QueryPage {
            query,
            emails: emails.list,
        })
    }

    /// The server's current Email state, via an empty Email/get — used to
    /// bracket a backfill or resync sweep before it starts.
    pub async fn email_state_now(&self) -> Result<String, JmapError> {
        let responses = self
            .call(json!([
                ["Email/get", {
                    "accountId": self.account_id,
                    "ids": [],
                }, "s0"],
            ]))
            .await?;
        let got: EmailGetResponse = serde_json::from_value(Self::response_args(&responses, "s0")?)
            .map_err(|e| JmapError::Protocol(e.to_string()))?;
        Ok(got.state)
    }

    /// Fetch metadata for explicit IDs, batched by the server's
    /// maxObjectsInGet ceiling.
    pub async fn email_get(&self, ids: &[String]) -> Result<Vec<Email>, JmapError> {
        let mut all = Vec::with_capacity(ids.len());
        for batch in ids.chunks(self.batch_limit() as usize) {
            let responses = self
                .call(json!([
                    ["Email/get", {
                        "accountId": self.account_id,
                        "ids": batch,
                        "properties": EMAIL_PROPERTIES,
                    }, "g0"],
                ]))
                .await?;
            let got: EmailGetResponse =
                serde_json::from_value(Self::response_args(&responses, "g0")?)
                    .map_err(|e| JmapError::Protocol(e.to_string()))?;
            all.extend(got.list);
        }
        Ok(all)
    }

    /// All message IDs in the account, paged — the resync path when the
    /// server returns cannotCalculateChanges.
    pub async fn query_all_ids(&self) -> Result<Vec<String>, JmapError> {
        let mut ids = Vec::new();
        let mut anchor: Option<String> = None;
        loop {
            let limit = self.batch_limit();
            let mut query_args = json!({
                "accountId": self.account_id,
                "sort": [{"property": "receivedAt", "isAscending": true}],
                "limit": limit,
            });
            match &anchor {
                Some(a) => {
                    query_args["anchor"] = json!(a);
                    query_args["anchorOffset"] = json!(1);
                }
                None => query_args["position"] = json!(0),
            }
            let responses = self
                .call(json!([["Email/query", query_args, "q0"]]))
                .await?;
            let page: EmailQueryResponse =
                serde_json::from_value(Self::response_args(&responses, "q0")?)
                    .map_err(|e| JmapError::Protocol(e.to_string()))?;
            if page.ids.is_empty() {
                break;
            }
            anchor = page.ids.last().cloned();
            let got = page.ids.len() as u64;
            ids.extend(page.ids);
            if got < limit {
                break;
            }
        }
        Ok(ids)
    }

    /// Download the raw RFC822 blob for a message. Retries not only
    /// 429/503 but also transient network failures — a timeout or dropped
    /// connection on either the request or the body read. Backfill sweeps
    /// hundreds of thousands of blobs, so a single flaky download must not
    /// be fatal.
    ///
    /// Note: reqwest labels *any* body-read failure "error decoding response
    /// body" — including a read timeout. That is not content decoding (we
    /// build reqwest without compression and read via `.bytes()`); the true
    /// cause is in the error's source chain, which the retry log surfaces
    /// via [`error_chain`].
    pub async fn download_blob(&self, blob_id: &str) -> Result<Vec<u8>, JmapError> {
        let url = self
            .session
            .download_url
            .replace("{accountId}", &self.account_id)
            .replace("{blobId}", blob_id)
            .replace("{name}", "message.eml")
            .replace("{type}", "application%2Foctet-stream");
        let mut attempt = 0u32;
        loop {
            let result = self.download_once(&url).await;
            let retryable = match &result {
                Err(JmapError::Status(s)) => *s == 429 || *s == 503,
                Err(JmapError::Http(e)) => is_transient(e),
                _ => false,
            };
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) if !retryable || attempt >= BLOB_MAX_RETRIES => return Err(e),
                Err(e) => {
                    let delay = self.retry.base_delay * 2u32.pow(attempt);
                    tracing::warn!(?delay, attempt, error = %error_chain(&e), "blob download failed, retrying");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    /// One blob download attempt, on a tighter per-request timeout than the
    /// JMAP API calls. Any transport error (send or body read) surfaces as
    /// `JmapError::Http` for the caller's retry decision.
    async fn download_once(&self, url: &str) -> Result<Vec<u8>, JmapError> {
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .timeout(BLOB_DOWNLOAD_TIMEOUT)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(JmapError::Status(response.status().as_u16()));
        }
        Ok(response.bytes().await?.to_vec())
    }
}

/// A network error worth retrying: a timeout, a connection failure, or an
/// error while sending the request. A decode/protocol error is not.
fn is_transient(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}

/// Render an error with its full `source()` chain joined by ": ". reqwest's
/// top-level Display for a failed body read is the misleading "error
/// decoding response body for url (...)"; the real cause (e.g. "operation
/// timed out") lives in the source chain, and `{e:#}` doesn't expose it for
/// a `thiserror` type. This does.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut src = e.source();
    while let Some(s) = src {
        out.push_str(": ");
        out.push_str(&s.to_string());
        src = s.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::error_chain;
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct Layer {
        msg: &'static str,
        source: Option<Box<dyn Error>>,
    }
    impl fmt::Display for Layer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.msg)
        }
    }
    impl Error for Layer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source.as_deref()
        }
    }

    #[test]
    fn error_chain_joins_the_source_chain() {
        let outer = Layer {
            msg: "error decoding response body",
            source: Some(Box::new(Layer {
                msg: "operation timed out",
                source: None,
            })),
        };
        assert_eq!(
            error_chain(&outer),
            "error decoding response body: operation timed out"
        );
    }

    #[test]
    fn error_chain_single_error_has_no_separator() {
        let e = Layer {
            msg: "boom",
            source: None,
        };
        assert_eq!(error_chain(&e), "boom");
    }
}
