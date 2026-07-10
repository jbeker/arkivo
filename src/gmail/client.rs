//! Minimal Gmail REST client over reqwest, mirroring the JMAP client's
//! posture: hand-rolled protocol, typed errors for the conditions the
//! sync engine branches on, retry with backoff for rate limits.
//!
//! Auth is a long-lived OAuth refresh token (sealed at rest); short-lived
//! access tokens are minted lazily and cached in memory for the lifetime
//! of the client — one job run — never persisted.

use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;

use super::types::*;
use crate::config::GoogleConfig;
use crate::jmap::RetryPolicy;

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const API_BASE: &str = "https://gmail.googleapis.com/gmail/v1/users/me";
/// Refresh the access token when it has less than this left to live.
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum GmailError {
    /// startHistoryId is too old for the server to compute a delta
    /// (Google keeps roughly a week); the caller must fall back to a
    /// full listing resync — the analog of JMAP's cannotCalculateChanges.
    #[error("history id expired; full resync required")]
    HistoryExpired,
    /// The refresh token was revoked or the grant expired; re-consent via
    /// the web UI is the only fix, so surface it clearly in job errors.
    #[error("Google OAuth grant is no longer valid (invalid_grant); reconnect the account")]
    AuthRevoked,
    #[error("HTTP status {0}: {1}")]
    Status(u16, String),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

pub struct GmailClient {
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    token_url: String,
    api_base: String,
    access: tokio::sync::Mutex<Option<CachedToken>>,
    retry: RetryPolicy,
}

impl GmailClient {
    pub fn new(
        google: &GoogleConfig,
        refresh_token: String,
        retry: RetryPolicy,
    ) -> Result<Self, GmailError> {
        Self::with_endpoints(
            google,
            refresh_token,
            retry,
            google.token_url.as_deref().unwrap_or(TOKEN_URL),
            google.api_base.as_deref().unwrap_or(API_BASE),
        )
    }

    /// Endpoint-injectable constructor for tests against a fake server.
    pub fn with_endpoints(
        google: &GoogleConfig,
        refresh_token: String,
        retry: RetryPolicy,
        token_url: &str,
        api_base: &str,
    ) -> Result<Self, GmailError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            http,
            client_id: google.client_id.clone(),
            client_secret: google.client_secret.clone(),
            refresh_token,
            token_url: token_url.to_string(),
            api_base: api_base.trim_end_matches('/').to_string(),
            access: tokio::sync::Mutex::new(None),
            retry,
        })
    }

    /// The cached access token, or a fresh one from the refresh grant.
    /// Holding the lock across the refresh serializes concurrent callers
    /// onto a single token request.
    pub async fn access_token(&self) -> Result<String, GmailError> {
        let mut guard = self.access.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at > Instant::now() + TOKEN_EXPIRY_MARGIN
        {
            return Ok(cached.token.clone());
        }
        // reqwest is built without the form feature; encode by hand.
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "refresh_token")
            .append_pair("refresh_token", &self.refresh_token)
            .append_pair("client_id", &self.client_id)
            .append_pair("client_secret", &self.client_secret)
            .finish();
        let response = self
            .http
            .post(&self.token_url)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            if text.contains("invalid_grant") {
                return Err(GmailError::AuthRevoked);
            }
            return Err(GmailError::Status(status.as_u16(), truncate(&text)));
        }
        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
            expires_in: u64,
        }
        let tr: TokenResponse = serde_json::from_str(&text)
            .map_err(|e| GmailError::Protocol(format!("bad token response: {e}")))?;
        let token = tr.access_token.clone();
        *guard = Some(CachedToken {
            token: tr.access_token,
            expires_at: Instant::now() + Duration::from_secs(tr.expires_in),
        });
        Ok(token)
    }

    /// GET an API path, retrying rate limits (429, 500, 503, and 403 whose
    /// body reason is a per-user quota signal — Gmail reports quota that
    /// way) with exponential backoff honoring Retry-After. A 401 drops the
    /// cached access token and retries once with a fresh one.
    async fn api_get(&self, path_and_query: &str) -> Result<Value, GmailError> {
        let url = format!("{}{}", self.api_base, path_and_query);
        let mut attempt = 0u32;
        loop {
            let token = self.access_token().await?;
            let response = self.http.get(&url).bearer_auth(&token).send().await?;
            let status = response.status().as_u16();
            if status == 401 {
                self.access.lock().await.take();
                if attempt >= self.retry.max_retries {
                    return Err(GmailError::Status(401, "unauthorized".into()));
                }
                attempt += 1;
                continue;
            }
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs);
            let text = response.text().await?;
            if status_ok(status) {
                return serde_json::from_str(&text)
                    .map_err(|e| GmailError::Protocol(format!("bad response body: {e}")));
            }
            let rate_limited = status == 429
                || status == 500
                || status == 503
                || (status == 403
                    && (text.contains("rateLimitExceeded")
                        || text.contains("userRateLimitExceeded")));
            if !rate_limited || attempt >= self.retry.max_retries {
                return Err(GmailError::Status(status, truncate(&text)));
            }
            let delay = retry_after.unwrap_or_else(|| self.retry.base_delay * 2u32.pow(attempt));
            tracing::warn!(status, ?delay, "gmail rate limited, backing off");
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    /// The granted address and current history id — also the cheapest
    /// credential check, used to fail fast when a context opens.
    pub async fn get_profile(&self) -> Result<Profile, GmailError> {
        let v = self.api_get("/profile").await?;
        serde_json::from_value(v).map_err(|e| GmailError::Protocol(e.to_string()))
    }

    /// One page of message ids, newest first. With `include_spam_trash`
    /// false (the API default) SPAM and TRASH never appear — the ingest
    /// policy for backfill. The resync diff passes true so a message
    /// merely moved to trash is not mistaken for a deleted one. `q` takes
    /// Gmail search syntax; `after:`/`before:` accept epoch seconds.
    pub async fn list_messages(
        &self,
        q: Option<&str>,
        page_token: Option<&str>,
        max_results: u32,
        include_spam_trash: bool,
    ) -> Result<MessageListPage, GmailError> {
        // Scoped: the Serializer is !Send and must drop before the await.
        let query = {
            let mut qs = url::form_urlencoded::Serializer::new(String::new());
            qs.append_pair("maxResults", &max_results.to_string());
            if include_spam_trash {
                qs.append_pair("includeSpamTrash", "true");
            }
            if let Some(q) = q {
                qs.append_pair("q", q);
            }
            if let Some(token) = page_token {
                qs.append_pair("pageToken", token);
            }
            qs.finish()
        };
        let v = self.api_get(&format!("/messages?{query}")).await?;
        serde_json::from_value(v).map_err(|e| GmailError::Protocol(e.to_string()))
    }

    /// Full message: metadata plus the raw RFC822 body, one call.
    pub async fn get_message_raw(&self, id: &str) -> Result<GmailMessage, GmailError> {
        let v = self.api_get(&format!("/messages/{id}?format=raw")).await?;
        serde_json::from_value(v).map_err(|e| GmailError::Protocol(e.to_string()))
    }

    /// Metadata only (labels, thread, internalDate) — for refreshing a
    /// message we already store, without re-downloading the body.
    pub async fn get_message_metadata(&self, id: &str) -> Result<GmailMessage, GmailError> {
        let v = self
            .api_get(&format!("/messages/{id}?format=minimal"))
            .await?;
        serde_json::from_value(v).map_err(|e| GmailError::Protocol(e.to_string()))
    }

    /// One page of history records since `start_history_id`. A 404 means
    /// the id is too old to diff from and maps to [`GmailError::HistoryExpired`].
    pub async fn list_history(
        &self,
        start_history_id: &str,
        page_token: Option<&str>,
    ) -> Result<HistoryPage, GmailError> {
        // Scoped: the Serializer is !Send and must drop before the await.
        let query = {
            let mut qs = url::form_urlencoded::Serializer::new(String::new());
            qs.append_pair("startHistoryId", start_history_id);
            qs.append_pair("maxResults", "500");
            if let Some(token) = page_token {
                qs.append_pair("pageToken", token);
            }
            qs.finish()
        };
        let result = self.api_get(&format!("/history?{query}")).await;
        match result {
            Err(GmailError::Status(404, _)) => Err(GmailError::HistoryExpired),
            other => other.and_then(|v| {
                serde_json::from_value(v).map_err(|e| GmailError::Protocol(e.to_string()))
            }),
        }
    }
}

fn status_ok(status: u16) -> bool {
    (200..300).contains(&status)
}

fn truncate(text: &str) -> String {
    let mut s = text.chars().take(200).collect::<String>();
    if s.len() < text.len() {
        s.push('…');
    }
    s
}
