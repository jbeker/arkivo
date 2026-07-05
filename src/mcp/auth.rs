//! Bearer-token authentication for the MCP surface (spec §11.2): per-user
//! tokens minted in the web UI, stored as SHA-256 hashes, revocable.
//! The middleware resolves the token and injects [`AuthedUser`] into the
//! request extensions; rmcp forwards those into the tool-call context.

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sqlx::PgPool;

use crate::crypto::hash_token;
use crate::db::tokens;

/// The identity every tool call runs as. Index names and audit entries
/// derive from this — never from tool arguments.
#[derive(Debug, Clone, Copy)]
pub struct AuthedUser {
    pub user_id: i64,
    pub token_id: i64,
}

impl AuthedUser {
    pub fn actor(&self) -> String {
        format!("mcp:{}", self.token_id)
    }
}

pub async fn require_token(
    State(pool): State<PgPool>,
    mut request: Request,
    next: Next,
) -> Response {
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let Some(token) = bearer else {
        return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
    };

    match tokens::resolve_active(&pool, &hash_token(token)).await {
        Ok(Some(resolved)) => {
            request.extensions_mut().insert(AuthedUser {
                user_id: resolved.user_id,
                token_id: resolved.id,
            });
            next.run(request).await
        }
        Ok(None) => (StatusCode::UNAUTHORIZED, "invalid or revoked token").into_response(),
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "token resolution failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "auth backend error").into_response()
        }
    }
}
