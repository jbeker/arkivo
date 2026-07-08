//! Web administration service (spec §12): passkey-only authentication,
//! per-user management of accounts/tokens/config, admin user management.

pub mod pages;
pub mod passkeys;
pub mod ratelimit;
pub mod routes;
pub mod store;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use sqlx::PgPool;
use time::Duration as TimeDuration;
use tower_sessions::{Expiry, Session, SessionManagerLayer};
use webauthn_rs::prelude::Url;
use webauthn_rs::{Webauthn, WebauthnBuilder};

use crate::config::AppConfig;
use crate::crypto::Sealer;
use crate::db::users::User;
use crate::search::SearchClient;
use store::PgSessionStore;

pub const SESSION_USER_KEY: &str = "user_id";
/// Unix timestamp of the authentication that created the session; the
/// absolute-lifetime check in [`require_session`] reads it.
pub const SESSION_AUTH_AT_KEY: &str = "auth_at";

#[derive(Clone)]
pub struct WebState {
    pub pool: PgPool,
    pub webauthn: Arc<Webauthn>,
    pub sealer: Arc<Sealer>,
    pub search: Arc<SearchClient>,
    pub embedding_url: String,
    pub defaults: crate::config::UserDefaults,
    /// Full config: web-spawned jobs (backfill/poll/promote) need the
    /// maildir root, master key path, and embedding settings.
    pub config: AppConfig,
}

/// The session-authenticated user, injected by [`require_session`].
#[derive(Clone)]
pub struct CurrentUser(pub User);

pub fn build_webauthn(config: &AppConfig) -> Result<Webauthn> {
    let origin = Url::parse(&config.web.rp_origin).context("web.rp_origin must be a URL")?;
    Ok(WebauthnBuilder::new(&config.web.rp_id, &origin)
        .context("invalid rp_id/rp_origin")?
        .rp_name("Arkivo")
        .build()?)
}

pub async fn require_session(
    State(state): State<WebState>,
    session: Session,
    mut request: Request,
    next: Next,
) -> Response {
    let mut user_id: Option<i64> = session.get(SESSION_USER_KEY).await.ok().flatten();
    // Absolute lifetime on top of the idle timeout: a session (however
    // active) dies session_max_age_days after the login that minted it.
    // Missing auth_at (pre-upgrade session) counts as expired.
    if user_id.is_some() {
        let auth_at: Option<i64> = session.get(SESSION_AUTH_AT_KEY).await.ok().flatten();
        let max_age_secs = i64::from(state.config.web.session_max_age_days) * 86_400;
        let expired =
            auth_at.is_none_or(|t| chrono::Utc::now().timestamp().saturating_sub(t) > max_age_secs);
        if expired {
            let _ = session.flush().await;
            user_id = None;
        }
    }
    let user = match user_id {
        Some(id) => crate::db::users::get(&state.pool, id).await.ok().flatten(),
        None => None,
    };
    match user {
        Some(user) if user.is_active() => {
            request.extensions_mut().insert(CurrentUser(user));
            next.run(request).await
        }
        _ => {
            if request.uri().path().starts_with("/api/") {
                (StatusCode::UNAUTHORIZED, "not signed in").into_response()
            } else {
                Redirect::to("/login").into_response()
            }
        }
    }
}

pub async fn require_admin(request: Request, next: Next) -> Response {
    let is_admin = request
        .extensions()
        .get::<CurrentUser>()
        .map(|u| u.0.is_admin())
        .unwrap_or(false);
    if is_admin {
        next.run(request).await
    } else {
        (StatusCode::FORBIDDEN, "admin role required").into_response()
    }
}

pub fn app(state: WebState) -> Router {
    // Secure follows the deployed origin: an https rp_origin means the
    // browser only ever sees the cookie over TLS (the terminator sits in
    // front per spec §16); plain-http localhost dev keeps working.
    let secure_cookie = state.config.web.rp_origin.starts_with("https://");
    let session_layer = SessionManagerLayer::new(PgSessionStore::new(state.pool.clone()))
        .with_secure(secure_cookie)
        .with_same_site(tower_sessions::cookie::SameSite::Strict)
        .with_expiry(Expiry::OnInactivity(TimeDuration::hours(12)));

    let authed = Router::new()
        .merge(routes::user::router())
        .merge(passkeys::authed_router())
        .merge(pages::authed_router())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ));

    let admin = Router::new()
        .merge(routes::admin::router())
        .layer(axum::middleware::from_fn(require_admin))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ));

    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get({
                let pool = state.pool.clone();
                move || {
                    let pool = pool.clone();
                    async move {
                        match sqlx::query("select 1").execute(&pool).await {
                            Ok(_) => (StatusCode::OK, "ready"),
                            Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "db down"),
                        }
                    }
                }
            }),
        )
        .route(
            // JSON counters for Zabbix HTTP-agent items (spec §15). With
            // web.metrics_token set, requires a matching bearer token.
            "/metrics",
            get({
                let pool = state.pool.clone();
                let required = state.config.web.metrics_token.clone();
                move |headers: axum::http::HeaderMap| {
                    let pool = pool.clone();
                    let required = required.clone();
                    async move {
                        if let Some(required) = required {
                            let presented = headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.strip_prefix("Bearer "));
                            if presented != Some(required.as_str()) {
                                return (StatusCode::UNAUTHORIZED, "metrics token required")
                                    .into_response();
                            }
                        }
                        match crate::metrics::gather(&pool).await {
                            Ok(value) => axum::Json(value).into_response(),
                            Err(e) => {
                                tracing::error!(error = %format!("{e:#}"), "metrics failed");
                                (StatusCode::INTERNAL_SERVER_ERROR, "metrics error").into_response()
                            }
                        }
                    }
                }
            }),
        )
        .merge(
            passkeys::router().layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(ratelimit::RateLimiter::new()),
                ratelimit::limit,
            )),
        )
        .merge(pages::public_router())
        .merge(authed)
        .merge(admin)
        .layer(session_layer)
        .with_state(state)
}
