//! Google OAuth consent flow for connecting a Gmail account.
//!
//! `/oauth/google/start` lives in the authed router: navigating there
//! from the dashboard is same-site, so the Strict session cookie is
//! present and the flow binds to the signed-in user. The callback is
//! public: the redirect back from accounts.google.com is cross-site and
//! carries no cookie, so it authenticates via the single-use
//! `oauth_states` row created at start (a 256-bit nonce, hash-stored)
//! and PKCE binds the authorization code to that same start request.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Extension, Router};
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config::GoogleConfig;
use crate::crypto::{generate_token, hash_token};
use crate::db::{accounts, audit, oauth_states};
use crate::gmail::GmailClient;
use crate::jmap::RetryPolicy;
use crate::web::{CurrentUser, WebState};

const SCOPE: &str = "https://www.googleapis.com/auth/gmail.readonly";
const STATE_TTL_SECS: i64 = 600;

pub fn authed_router() -> Router<WebState> {
    Router::new().route("/oauth/google/start", get(start))
}

pub fn public_router() -> Router<WebState> {
    Router::new().route("/oauth/google/callback", get(callback))
}

fn pkce_challenge(verifier: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

async fn start(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    let Some(google) = &state.config.google else {
        return (
            StatusCode::NOT_FOUND,
            "Gmail is not configured on this server ([google] section missing)",
        )
            .into_response();
    };

    // The state nonce goes to Google verbatim; only its hash is stored,
    // so a database read can't forge a live callback.
    let nonce = generate_token("gs");
    let verifier = hex::encode(rand::random::<[u8; 32]>());
    if let Err(e) =
        oauth_states::create(&state.pool, user.id, &nonce.hash, &verifier, STATE_TTL_SECS).await
    {
        tracing::error!(error = %format!("{e:#}"), "oauth state create failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
    }

    let query = {
        let mut qs = url::form_urlencoded::Serializer::new(String::new());
        qs.append_pair("client_id", &google.client_id);
        qs.append_pair(
            "redirect_uri",
            &GoogleConfig::redirect_uri(&state.config.web.rp_origin),
        );
        qs.append_pair("response_type", "code");
        qs.append_pair("scope", SCOPE);
        // offline + consent guarantees a refresh_token even on re-consent.
        qs.append_pair("access_type", "offline");
        qs.append_pair("prompt", "consent");
        qs.append_pair("state", &nonce.token);
        qs.append_pair("code_challenge", &pkce_challenge(&verifier));
        qs.append_pair("code_challenge_method", "S256");
        qs.finish()
    };
    Redirect::to(&format!("{}?{}", google.auth_url(), query)).into_response()
}

#[derive(Deserialize)]
struct CallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct ExchangeResponse {
    #[allow(dead_code)]
    access_token: String,
    refresh_token: Option<String>,
}

async fn callback(State(state): State<WebState>, Query(params): Query<CallbackParams>) -> Response {
    let Some(google) = state.config.google.clone() else {
        return (StatusCode::NOT_FOUND, "Gmail is not configured").into_response();
    };
    if params.error.is_some() {
        // User declined at the consent screen.
        return Redirect::to("/?gmail=denied").into_response();
    }
    let (Some(code), Some(state_param)) = (&params.code, &params.state) else {
        return (StatusCode::BAD_REQUEST, "missing code or state").into_response();
    };

    // Single-use, expiring, user-bound: this row is the authentication.
    let consumed = match oauth_states::consume_by_hash(&state.pool, &hash_token(state_param)).await
    {
        Ok(Some(consumed)) => consumed,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                "unknown, expired, or already-used OAuth state; start again from the dashboard",
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "oauth state consume failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };

    let refresh_token = match exchange_code(
        &google,
        &state.config.web.rp_origin,
        code,
        &consumed.pkce_verifier,
    )
    .await
    {
        Ok(ExchangeResponse {
            refresh_token: Some(token),
            ..
        }) => token,
        Ok(_) => {
            tracing::warn!("google token exchange returned no refresh_token");
            return Redirect::to("/?gmail=error").into_response();
        }
        Err(e) => {
            tracing::warn!(error = %format!("{e:#}"), "google code exchange failed");
            return Redirect::to("/?gmail=error").into_response();
        }
    };

    // Validate the grant end-to-end (mirrors add_account's live check)
    // and learn the granted address.
    let profile = {
        let client = match GmailClient::new(
            &google,
            refresh_token.clone(),
            RetryPolicy {
                max_retries: 1,
                base_delay: std::time::Duration::from_millis(500),
            },
        ) {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!(error = %e, "gmail client build failed");
                return Redirect::to("/?gmail=error").into_response();
            }
        };
        match client.get_profile().await {
            Ok(profile) => profile,
            Err(e) => {
                tracing::warn!(error = %e, "gmail profile fetch failed after consent");
                return Redirect::to("/?gmail=error").into_response();
            }
        }
    };

    let existing = accounts::list_for_user(&state.pool, consumed.user_id)
        .await
        .unwrap_or_default();
    if existing
        .iter()
        .any(|a| a.provider == "gmail" && a.account_id.as_deref() == Some(&profile.email_address))
    {
        return Redirect::to("/?gmail=exists").into_response();
    }

    let sealed = match state.sealer.seal(refresh_token.as_bytes()) {
        Ok(sealed) => sealed,
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "sealing refresh token failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    match accounts::create_gmail(
        &state.pool,
        consumed.user_id,
        &profile.email_address,
        &sealed,
        state.sealer.key_id(),
    )
    .await
    {
        Ok(account) => {
            let _ = audit::record(
                &state.pool,
                Some(consumed.user_id),
                &format!("session:{}", consumed.user_id),
                "gmail_account_connected",
                None,
                Some(&json!({"account_id": account.id, "email": profile.email_address})),
            )
            .await;
            Redirect::to("/?gmail=connected").into_response()
        }
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "gmail account create failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn exchange_code(
    google: &GoogleConfig,
    rp_origin: &str,
    code: &str,
    pkce_verifier: &str,
) -> anyhow::Result<ExchangeResponse> {
    let body = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("grant_type", "authorization_code")
        .append_pair("code", code)
        .append_pair("client_id", &google.client_id)
        .append_pair("client_secret", &google.client_secret)
        .append_pair("redirect_uri", &GoogleConfig::redirect_uri(rp_origin))
        .append_pair("code_verifier", pkce_verifier)
        .finish();
    let token_url = google
        .token_url
        .as_deref()
        .unwrap_or("https://oauth2.googleapis.com/token");
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let response = http
        .post(token_url)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        anyhow::bail!("token endpoint returned {status}: {text}");
    }
    Ok(serde_json::from_str(&text)?)
}
