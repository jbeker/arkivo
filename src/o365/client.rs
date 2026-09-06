//! Minimal Microsoft Graph client over reqwest, mirroring the Gmail
//! client's posture: hand-rolled protocol, typed errors for the
//! conditions the sync engine branches on, retry with backoff for
//! throttling.
//!
//! Auth is a long-lived OAuth refresh token (sealed at rest). Access
//! tokens are minted lazily and cached in memory for one job run. Unlike
//! Google, Microsoft rotates the refresh token on every redemption, so
//! the client keeps the newest one and hands it back sealed via
//! [`O365Client::take_rotated_sealed_token`] for the caller to persist.
//!
//! Every request carries `Prefer: IdType="ImmutableId"` so message ids
//! survive folder moves — the identity contract the sync engine relies
//! on.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::types::*;
use crate::config::MicrosoftConfig;
use crate::crypto::Sealer;
use crate::jmap::RetryPolicy;

/// Delegated permissions requested at consent and on every refresh.
pub const SCOPE: &str = "offline_access Mail.Read User.Read";
/// Refresh the access token when it has less than this left to live.
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
/// `odata.maxpagesize` asked for on delta requests. The server may
/// return fewer; callers always follow `nextLink`.
pub const DELTA_PAGE_SIZE: u32 = 200;
const MESSAGE_SELECT: &str =
    "id,isRead,flag,isDraft,parentFolderId,receivedDateTime,conversationId,internetMessageId";
const FOLDER_SELECT: &str = "id,displayName,parentFolderId,childFolderCount,totalItemCount";

#[derive(Debug, thiserror::Error)]
pub enum O365Error {
    /// The refresh token was revoked, expired from inactivity, or the
    /// tenant now requires interaction; re-consent via the web UI is the
    /// only fix, so surface it clearly in job errors.
    #[error("Microsoft OAuth grant is no longer valid (invalid_grant); reconnect the account")]
    AuthRevoked,
    /// A stored delta/next link is no longer valid on the server (HTTP
    /// 410); the folder must be re-walked from scratch.
    #[error("delta token expired; folder resync required")]
    DeltaExpired,
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

pub struct O365Client {
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    token_url: String,
    api_base: String,
    /// Current refresh token; replaced whenever the token endpoint
    /// rotates it.
    refresh_token: StdMutex<String>,
    /// A rotated refresh token the caller has not yet persisted.
    rotated: StdMutex<Option<String>>,
    sealer: Arc<Sealer>,
    access: tokio::sync::Mutex<Option<CachedToken>>,
    retry: RetryPolicy,
}

impl O365Client {
    pub fn new(
        microsoft: &MicrosoftConfig,
        refresh_token: String,
        retry: RetryPolicy,
        sealer: Arc<Sealer>,
    ) -> Result<Self, O365Error> {
        Self::with_endpoints(
            microsoft,
            refresh_token,
            retry,
            sealer,
            &microsoft.token_url(),
            &microsoft.api_base(),
        )
    }

    /// Endpoint-injectable constructor for tests against a fake server.
    pub fn with_endpoints(
        microsoft: &MicrosoftConfig,
        refresh_token: String,
        retry: RetryPolicy,
        sealer: Arc<Sealer>,
        token_url: &str,
        api_base: &str,
    ) -> Result<Self, O365Error> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            http,
            client_id: microsoft.client_id.clone(),
            client_secret: microsoft.client_secret.clone(),
            token_url: token_url.to_string(),
            api_base: api_base.trim_end_matches('/').to_string(),
            refresh_token: StdMutex::new(refresh_token),
            rotated: StdMutex::new(None),
            sealer,
            access: tokio::sync::Mutex::new(None),
            retry,
        })
    }

    /// The cached access token, or a fresh one from the refresh grant.
    /// Holding the lock across the refresh serializes concurrent callers
    /// onto a single token request. A rotated refresh token in the
    /// response is adopted immediately and queued for persistence.
    pub async fn access_token(&self) -> Result<String, O365Error> {
        let mut guard = self.access.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at > Instant::now() + TOKEN_EXPIRY_MARGIN
        {
            return Ok(cached.token.clone());
        }
        let current = self.refresh_token.lock().unwrap().clone();
        // reqwest is built without the form feature; encode by hand.
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "refresh_token")
            .append_pair("refresh_token", &current)
            .append_pair("client_id", &self.client_id)
            .append_pair("client_secret", &self.client_secret)
            .append_pair("scope", SCOPE)
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
            let code = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or_default();
            if code == "invalid_grant" || code == "interaction_required" {
                return Err(O365Error::AuthRevoked);
            }
            return Err(O365Error::Status(status.as_u16(), truncate(&text)));
        }
        let tr: TokenResponse = serde_json::from_str(&text)
            .map_err(|e| O365Error::Protocol(format!("bad token response: {e}")))?;
        if let Some(new) = tr.refresh_token
            && new != current
        {
            *self.refresh_token.lock().unwrap() = new.clone();
            *self.rotated.lock().unwrap() = Some(new);
        }
        let token = tr.access_token.clone();
        *guard = Some(CachedToken {
            token: tr.access_token,
            expires_at: Instant::now() + Duration::from_secs(tr.expires_in),
        });
        Ok(token)
    }

    /// The sealed form of a refresh token that rotated since the last
    /// call, if any. Callers persist it to `mail_accounts.sealed_token`.
    pub fn take_rotated_sealed_token(&self) -> anyhow::Result<Option<Vec<u8>>> {
        let rotated = self.rotated.lock().unwrap().take();
        match rotated {
            Some(token) => Ok(Some(self.sealer.seal(token.as_bytes())?)),
            None => Ok(None),
        }
    }

    fn resolve(&self, path_or_url: &str) -> String {
        if path_or_url.starts_with("http://") || path_or_url.starts_with("https://") {
            path_or_url.to_string()
        } else {
            format!("{}{}", self.api_base, path_or_url)
        }
    }

    /// GET with the retry loop shared by every call: 429/5xx back off
    /// honoring Retry-After; a 401 drops the cached access token and
    /// retries once with a fresh one. `prefer` is appended to the
    /// ImmutableId preference every request carries.
    async fn get_raw(
        &self,
        path_or_url: &str,
        prefer_extra: Option<&str>,
    ) -> Result<(u16, Vec<u8>), O365Error> {
        let url = self.resolve(path_or_url);
        let prefer = match prefer_extra {
            Some(extra) => format!("IdType=\"ImmutableId\", {extra}"),
            None => "IdType=\"ImmutableId\"".to_string(),
        };
        let mut attempt = 0u32;
        loop {
            let token = self.access_token().await?;
            let response = self
                .http
                .get(&url)
                .bearer_auth(&token)
                .header("prefer", &prefer)
                .send()
                .await?;
            let status = response.status().as_u16();
            if status == 401 {
                self.access.lock().await.take();
                if attempt >= self.retry.max_retries {
                    return Err(O365Error::Status(401, "unauthorized".into()));
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
            let body = response.bytes().await?;
            if (200..300).contains(&status) {
                return Ok((status, body.to_vec()));
            }
            let throttled = matches!(status, 429 | 500 | 502 | 503 | 504);
            if !throttled || attempt >= self.retry.max_retries {
                let text = String::from_utf8_lossy(&body);
                return Err(O365Error::Status(status, truncate(&text)));
            }
            let delay = retry_after.unwrap_or_else(|| self.retry.base_delay * 2u32.pow(attempt));
            tracing::warn!(status, ?delay, "graph throttled, backing off");
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    async fn api_get(
        &self,
        path_or_url: &str,
        prefer_extra: Option<&str>,
    ) -> Result<Value, O365Error> {
        let (_, body) = self.get_raw(path_or_url, prefer_extra).await?;
        serde_json::from_slice(&body)
            .map_err(|e| O365Error::Protocol(format!("bad response body: {e}")))
    }

    fn parse<T: serde::de::DeserializeOwned>(v: Value) -> Result<T, O365Error> {
        serde_json::from_value(v).map_err(|e| O365Error::Protocol(e.to_string()))
    }

    /// The signed-in mailbox — also the cheapest credential check, used
    /// to fail fast when a context opens and to learn the address at
    /// connect time.
    pub async fn get_me(&self) -> Result<Me, O365Error> {
        let v = self
            .api_get("/me?$select=userPrincipalName,mail", None)
            .await?;
        Self::parse(v)
    }

    /// The whole visible folder tree, flattened; parent ids let the
    /// caller rebuild display paths. Hidden folders are not returned by
    /// Graph unless asked for, and we never ask.
    pub async fn list_folders(&self) -> Result<Vec<FolderNode>, O365Error> {
        let mut out = Vec::new();
        let mut pending: Vec<String> =
            vec![format!("/me/mailFolders?$top=100&$select={FOLDER_SELECT}")];
        while let Some(path) = pending.pop() {
            let mut link: Option<String> = Some(path);
            while let Some(url) = link.take() {
                let page: FolderPage = Self::parse(self.api_get(&url, None).await?)?;
                for node in page.value {
                    if node.child_folder_count > 0 {
                        pending.push(format!(
                            "/me/mailFolders/{}/childFolders?$top=100&$select={FOLDER_SELECT}",
                            seg(&node.id)
                        ));
                    }
                    out.push(node);
                }
                link = page.next_link;
            }
        }
        Ok(out)
    }

    /// The id of a well-known folder (`inbox`, `deleteditems`, ...), or
    /// None when the mailbox has no such folder.
    pub async fn resolve_well_known(&self, name: &str) -> Result<Option<String>, O365Error> {
        match self
            .api_get(&format!("/me/mailFolders/{name}?$select=id"), None)
            .await
        {
            Ok(v) => Ok(v.get("id").and_then(|i| i.as_str()).map(String::from)),
            Err(O365Error::Status(404, _)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// One delta page. `link` continues a cycle (a stored nextLink or
    /// deltaLink); otherwise a new cycle starts for `folder_id`, with an
    /// optional `receivedDateTime` floor. A 410 means the link is dead
    /// and maps to [`O365Error::DeltaExpired`].
    pub async fn delta_page(
        &self,
        folder_id: &str,
        link: Option<&str>,
        since: Option<chrono::DateTime<chrono::Utc>>,
        page_size: u32,
    ) -> Result<DeltaPage, O365Error> {
        let url = match link {
            Some(link) => link.to_string(),
            None => {
                let mut url =
                    format!("/me/mailFolders/{folder_id}/messages/delta?$select={MESSAGE_SELECT}");
                if let Some(since) = since {
                    let ts = since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    url.push_str(&format!("&$filter=receivedDateTime%20ge%20{ts}"));
                }
                url
            }
        };
        let prefer = format!("odata.maxpagesize={}", page_size.clamp(1, DELTA_PAGE_SIZE));
        match self.api_get(&url, Some(&prefer)).await {
            Err(O365Error::Status(410, _)) => Err(O365Error::DeltaExpired),
            other => other.and_then(Self::parse),
        }
    }

    /// Metadata only — the same properties a delta item carries.
    pub async fn get_message_meta(&self, id: &str) -> Result<DeltaItem, O365Error> {
        let v = self
            .api_get(
                &format!("/me/messages/{}?$select={MESSAGE_SELECT}", seg(id)),
                None,
            )
            .await?;
        Self::parse(v)
    }

    /// The raw RFC822 bytes of a message.
    pub async fn get_message_raw(&self, id: &str) -> Result<Vec<u8>, O365Error> {
        let (_, body) = self
            .get_raw(&format!("/me/messages/{}/$value", seg(id)), None)
            .await?;
        if body.first() == Some(&b'{') {
            return Err(O365Error::Protocol(
                "expected MIME bytes from $value, got JSON".into(),
            ));
        }
        Ok(body)
    }
}

/// Percent-encode a Graph id for use as a URL path segment. Ids are
/// base64-ish and may carry `=`, `+`, or `/`.
fn seg(id: &str) -> String {
    let mut out = String::with_capacity(id.len());
    for b in id.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn truncate(text: &str) -> String {
    let mut s = text.chars().take(200).collect::<String>();
    if s.len() < text.len() {
        s.push('…');
    }
    s
}
