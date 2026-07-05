//! Admin API (spec §12): user management, invites, recovery codes,
//! system health. Mounted behind require_session + require_admin.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::crypto::generate_token;
use crate::db::{audit, auth, users};
use crate::web::{CurrentUser, WebState};

pub fn router() -> Router<WebState> {
    Router::new()
        .route("/api/admin/users", get(list_users))
        .route("/api/admin/invites", post(create_invite))
        .route("/api/admin/users/{id}/disabled", post(set_disabled))
        .route("/api/admin/users/{id}/recovery", post(issue_recovery))
        .route("/api/admin/health", get(health))
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, "admin route error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

async fn list_users(State(state): State<WebState>) -> Response {
    match users::list(&state.pool).await {
        Ok(list) => Json(json!({
            "users": list.iter().map(|u| json!({
                "id": u.id, "handle": u.handle, "role": u.role,
                "created_at": u.created_at, "disabled_at": u.disabled_at,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct CreateInvite {
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "user".into()
}

async fn create_invite(
    State(state): State<WebState>,
    Extension(CurrentUser(admin)): Extension<CurrentUser>,
    Json(body): Json<CreateInvite>,
) -> Response {
    if body.role != "user" && body.role != "admin" {
        return (StatusCode::BAD_REQUEST, "role: user|admin").into_response();
    }
    let generated = generate_token("inv");
    match auth::create_invite(&state.pool, &generated.hash, Some(admin.id), &body.role, 7).await {
        Ok(id) => {
            let _ = audit::record(
                &state.pool,
                Some(admin.id),
                &format!("session:{}", admin.id),
                "invite_created",
                None,
                Some(&json!({"invite_id": id, "role": body.role})),
            )
            .await;
            // Shown once; only the hash is stored.
            (
                StatusCode::CREATED,
                Json(json!({"id": id, "code": generated.token})),
            )
                .into_response()
        }
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct SetDisabled {
    disabled: bool,
}

async fn set_disabled(
    State(state): State<WebState>,
    Extension(CurrentUser(admin)): Extension<CurrentUser>,
    Path(id): Path<i64>,
    Json(body): Json<SetDisabled>,
) -> Response {
    if id == admin.id {
        return (StatusCode::BAD_REQUEST, "cannot disable yourself").into_response();
    }
    match users::set_disabled(&state.pool, id, body.disabled).await {
        Ok(()) => {
            let _ = audit::record(
                &state.pool,
                Some(admin.id),
                &format!("session:{}", admin.id),
                if body.disabled {
                    "user_disabled"
                } else {
                    "user_enabled"
                },
                None,
                Some(&json!({"target_user": id})),
            )
            .await;
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => internal(e),
    }
}

async fn issue_recovery(
    State(state): State<WebState>,
    Extension(CurrentUser(admin)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    let generated = generate_token("rec");
    match auth::create_recovery_code(&state.pool, id, &generated.hash, 3).await {
        Ok(rec_id) => {
            let _ = audit::record(
                &state.pool,
                Some(admin.id),
                &format!("session:{}", admin.id),
                "recovery_code_issued",
                None,
                Some(&json!({"target_user": id, "recovery_id": rec_id})),
            )
            .await;
            (StatusCode::CREATED, Json(json!({"code": generated.token}))).into_response()
        }
        Err(e) => internal(e),
    }
}

/// System health across the stack (spec §12 admin view).
async fn health(State(state): State<WebState>) -> Response {
    let postgres = sqlx::query("select 1").execute(&state.pool).await.is_ok();
    let opensearch = state.search.health().await.is_ok();
    let embedding = reqwest::Client::new()
        .get(format!(
            "{}/api/version",
            state.embedding_url.trim_end_matches('/')
        ))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);
    Json(json!({
        "postgres": postgres,
        "opensearch": opensearch,
        "embedding": embedding,
    }))
    .into_response()
}
