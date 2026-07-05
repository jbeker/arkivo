//! Web administration service (spec §12): passkey-only authentication,
//! per-user management of accounts/tokens/config, admin user management.

pub mod pages;
pub mod passkeys;
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

#[derive(Clone)]
pub struct WebState {
    pub pool: PgPool,
    pub webauthn: Arc<Webauthn>,
    pub sealer: Arc<Sealer>,
    pub search: Arc<SearchClient>,
    pub embedding_url: String,
    pub defaults: crate::config::UserDefaults,
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
    let user_id: Option<i64> = session.get(SESSION_USER_KEY).await.ok().flatten();
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
    let session_layer = SessionManagerLayer::new(PgSessionStore::new(state.pool.clone()))
        .with_secure(false) // TLS terminates in front (spec §16)
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
        .merge(passkeys::router())
        .merge(pages::public_router())
        .merge(authed)
        .merge(admin)
        .layer(session_layer)
        .with_state(state)
}
