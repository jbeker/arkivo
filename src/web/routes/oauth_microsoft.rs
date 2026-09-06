//! Microsoft identity platform consent flow for connecting an Office 365
//! (or personal Microsoft) mailbox over Graph. Same shape and security
//! posture as the Google flow in `oauth.rs`: `/oauth/microsoft/start` is
//! authed (same-site navigation carries the Strict session cookie), the
//! callback is public and authenticated by the single-use `oauth_states`
//! row, and PKCE binds the code to that start request.
//!
//! One difference matters for storage: Microsoft rotates the refresh
//! token on every redemption. The token returned by the code exchange is
//! the one sealed here; the first job run rotates and re-persists it.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Extension, Router};
use serde_json::json;

use super::oauth::{CallbackParams, ExchangeResponse, begin_state, exchange_code, pkce_challenge};
use crate::config::MicrosoftConfig;
use crate::crypto::hash_token;
use crate::db::{accounts, audit, oauth_states};
use crate::jmap::RetryPolicy;
use crate::o365::{O365Client, SCOPE};
use crate::web::{CurrentUser, WebState};

pub fn authed_router() -> Router<WebState> {
    Router::new().route("/oauth/microsoft/start", get(start))
}

pub fn public_router() -> Router<WebState> {
    Router::new().route("/oauth/microsoft/callback", get(callback))
}

async fn start(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    let Some(microsoft) = &state.config.microsoft else {
        return (
            StatusCode::NOT_FOUND,
            "Microsoft 365 is not configured on this server ([microsoft] section missing)",
        )
            .into_response();
    };

    let started = match begin_state(&state.pool, user.id).await {
        Ok(started) => started,
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "oauth state create failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };

    let query = {
        let mut qs = url::form_urlencoded::Serializer::new(String::new());
        qs.append_pair("client_id", &microsoft.client_id);
        qs.append_pair(
            "redirect_uri",
            &MicrosoftConfig::redirect_uri(&state.config.web.rp_origin),
        );
        qs.append_pair("response_type", "code");
        qs.append_pair("response_mode", "query");
        // offline_access in the scope is what yields a refresh token.
        qs.append_pair("scope", SCOPE);
        // Let a user with several Microsoft identities pick the mailbox.
        qs.append_pair("prompt", "select_account");
        qs.append_pair("state", &started.nonce);
        qs.append_pair("code_challenge", &pkce_challenge(&started.verifier));
        qs.append_pair("code_challenge_method", "S256");
        qs.finish()
    };
    Redirect::to(&format!("{}?{}", microsoft.auth_url(), query)).into_response()
}

async fn callback(State(state): State<WebState>, Query(params): Query<CallbackParams>) -> Response {
    let Some(microsoft) = state.config.microsoft.clone() else {
        return (StatusCode::NOT_FOUND, "Microsoft 365 is not configured").into_response();
    };
    if params.error.is_some() {
        // User declined at the consent screen.
        return Redirect::to("/?o365=denied").into_response();
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
        &microsoft.token_url(),
        &microsoft.client_id,
        &microsoft.client_secret,
        &MicrosoftConfig::redirect_uri(&state.config.web.rp_origin),
        code,
        &consumed.pkce_verifier,
        &[("scope", SCOPE)],
    )
    .await
    {
        Ok(ExchangeResponse {
            refresh_token: Some(token),
            ..
        }) => token,
        Ok(_) => {
            tracing::warn!("microsoft token exchange returned no refresh_token");
            return Redirect::to("/?o365=error").into_response();
        }
        Err(e) => {
            tracing::warn!(error = %format!("{e:#}"), "microsoft code exchange failed");
            return Redirect::to("/?o365=error").into_response();
        }
    };

    // Validate the grant end-to-end (mirrors add_account's live check)
    // and learn the mailbox address.
    let client = match O365Client::new(
        &microsoft,
        refresh_token.clone(),
        RetryPolicy {
            max_retries: 1,
            base_delay: std::time::Duration::from_millis(500),
        },
        state.sealer.clone(),
    ) {
        Ok(client) => client,
        Err(e) => {
            tracing::warn!(error = %e, "o365 client build failed");
            return Redirect::to("/?o365=error").into_response();
        }
    };
    let address = match client.get_me().await {
        Ok(me) => match me.address() {
            Some(address) => address.to_string(),
            None => {
                tracing::warn!("microsoft /me returned neither mail nor userPrincipalName");
                return Redirect::to("/?o365=error").into_response();
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "microsoft /me fetch failed after consent");
            return Redirect::to("/?o365=error").into_response();
        }
    };

    let existing = accounts::list_for_user(&state.pool, consumed.user_id)
        .await
        .unwrap_or_default();
    if existing
        .iter()
        .any(|a| a.provider == "o365" && a.account_id.as_deref() == Some(&address))
    {
        return Redirect::to("/?o365=exists").into_response();
    }

    // The /me call above redeemed the refresh grant, which rotates the
    // token; seal whichever one is current.
    let sealed = match client.take_rotated_sealed_token() {
        Ok(Some(sealed)) => Ok(sealed),
        Ok(None) => state.sealer.seal(refresh_token.as_bytes()),
        Err(e) => Err(e),
    };
    let sealed = match sealed {
        Ok(sealed) => sealed,
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "sealing refresh token failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    match accounts::create_o365(
        &state.pool,
        consumed.user_id,
        &address,
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
                "o365_account_connected",
                None,
                Some(&json!({"account_id": account.id, "email": address})),
            )
            .await;
            Redirect::to("/?o365=connected").into_response()
        }
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "o365 account create failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}
