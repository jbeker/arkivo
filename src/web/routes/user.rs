//! User-scoped API (spec §12): mail accounts, ingestion status, config,
//! MCP tokens, reindex trigger, audit log, passkey management. Every
//! query is constrained to the session user; ownership checks guard all
//! path-parameter resources.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::crypto::generate_token;
use crate::db::{accounts, audit, auth, jobs, messages, tokens};
use crate::web::{CurrentUser, WebState};

pub fn router() -> Router<WebState> {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/accounts", post(add_account))
        .route(
            "/api/accounts/{id}",
            delete(remove_account).patch(update_account),
        )
        .route("/api/accounts/{id}/backfill", post(start_backfill))
        .route("/api/accounts/{id}/poll", post(start_poll))
        .route("/api/accounts/{id}/promote", post(start_promote))
        .route("/api/accounts/{id}/reindex", post(start_reindex))
        .route("/api/jobs/{id}/cancel", post(cancel_job))
        .route("/api/tokens", post(mint_token).get(list_tokens))
        .route("/api/tokens/{id}", delete(revoke_token))
        .route("/api/passkeys", get(list_passkeys))
        .route("/api/passkeys/{id}", delete(remove_passkey))
        .route("/api/audit", get(recent_audit))
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, "user route error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

/// Load an account only if the session user owns it.
async fn owned_account(
    state: &WebState,
    user_id: i64,
    account_id: i64,
) -> Result<Option<accounts::MailAccount>, Response> {
    match accounts::get(&state.pool, account_id).await {
        Ok(Some(account)) if account.user_id == user_id => Ok(Some(account)),
        Ok(_) => Ok(None),
        Err(e) => Err(internal(e)),
    }
}

async fn status(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    let accounts = match accounts::list_for_user(&state.pool, user.id).await {
        Ok(list) => list,
        Err(e) => return internal(e),
    };
    let mut out = Vec::new();
    for account in accounts {
        let counts = messages::counts(&state.pool, account.id)
            .await
            .unwrap_or_default();
        let state_row = accounts::get_jmap_state(&state.pool, account.id)
            .await
            .ok()
            .flatten();
        let recent_jobs = jobs::recent(&state.pool, account.id, 5)
            .await
            .unwrap_or_default();
        out.push(json!({
            "id": account.id,
            "jmap_session_url": account.jmap_session_url,
            "recency_cutoff_days": account.recency_cutoff_days,
            "deletion_policy": account.deletion_policy,
            "poll_interval_secs": account.poll_interval_secs,
            "backfill_done": state_row.as_ref().map(|s| s.backfill_done),
            "last_state_update": state_row.as_ref().map(|s| s.updated_at),
            "counts": {
                "total": counts.total,
                "staged": counts.staged,
                "indexed": counts.indexed,
                "quarantined": counts.quarantined,
                "failed": counts.failed,
                "unfetched": counts.unfetched,
            },
            "recent_jobs": recent_jobs.iter().map(|j| json!({
                "kind": j.kind, "status": j.status,
                "started_at": j.started_at, "finished_at": j.finished_at,
                "error": j.error,
            })).collect::<Vec<_>>(),
        }));
    }
    let running_jobs = jobs::running_for_user(&state.pool, user.id)
        .await
        .unwrap_or_default();
    Json(json!({
        "user": {"handle": user.handle, "role": user.role},
        "accounts": out,
        "running_jobs": running_jobs.iter().map(|j| json!({
            "job_id": j.id, "kind": j.kind, "account_id": j.mail_account_id,
            "started_at": j.started_at, "stats": j.stats,
        })).collect::<Vec<_>>(),
    }))
    .into_response()
}

/// Validate ownership, then take the lock + jobs row synchronously and
/// spawn the long-running work. 202 with the job id, 409 when a job of
/// the same family already holds the account.
async fn spawn_job(
    state: &WebState,
    user_id: i64,
    account_id: i64,
    kind: crate::ops::JobKind,
) -> Response {
    let Some(account) = (match owned_account(state, user_id, account_id).await {
        Ok(a) => a,
        Err(r) => return r,
    }) else {
        return not_found();
    };
    match crate::ops::try_start(&state.config, &state.pool, account.id, kind).await {
        Ok(Some(started)) => {
            let job_id = started.job_id;
            crate::ops::spawn_detached(state.config.clone(), state.pool.clone(), started);
            (StatusCode::ACCEPTED, Json(json!({"job_id": job_id}))).into_response()
        }
        Ok(None) => (
            StatusCode::CONFLICT,
            "a conflicting job is already running for this account",
        )
            .into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize, Default)]
struct StartBackfill {
    /// Optional cap for smoke-test runs.
    limit: Option<u64>,
}

async fn start_backfill(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
    body: Option<Json<StartBackfill>>,
) -> Response {
    let limit = body.and_then(|Json(b)| b.limit);
    spawn_job(
        &state,
        user.id,
        id,
        crate::ops::JobKind::Backfill { limit, since: None },
    )
    .await
}

async fn start_poll(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    spawn_job(&state, user.id, id, crate::ops::JobKind::Poll).await
}

async fn start_promote(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    spawn_job(&state, user.id, id, crate::ops::JobKind::Promote).await
}

#[derive(Deserialize, Default)]
struct StartReindex {
    #[serde(default)]
    recreate: bool,
}

async fn start_reindex(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
    body: Option<Json<StartReindex>>,
) -> Response {
    let recreate = body.map(|Json(b)| b.recreate).unwrap_or(false);
    spawn_job(
        &state,
        user.id,
        id,
        crate::ops::JobKind::Reindex { recreate },
    )
    .await
}

async fn cancel_job(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    match jobs::request_cancel(&state.pool, id, user.id).await {
        Ok(true) => Json(json!({"ok": true})).into_response(),
        Ok(false) => not_found(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct AddAccount {
    jmap_session_url: String,
    /// Read-only Fastmail API token; sealed before storage, never echoed.
    token: String,
}

async fn add_account(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Json(body): Json<AddAccount>,
) -> Response {
    // Homelab dev setups may use plain http; production sits behind TLS.
    if !body.jmap_session_url.starts_with("https://")
        && !body.jmap_session_url.starts_with("http://")
    {
        return (StatusCode::BAD_REQUEST, "session URL must be http(s)").into_response();
    }

    // Validate the credential against the server before sealing: a bad
    // token should fail here with a clear message, not at first poll.
    let client = match crate::jmap::JmapClient::connect(
        &body.jmap_session_url,
        &body.token,
        crate::jmap::RetryPolicy {
            max_retries: 1,
            base_delay: std::time::Duration::from_millis(500),
        },
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::warn!(error = %e, "account validation failed");
            return (
                StatusCode::BAD_REQUEST,
                "could not connect: the server rejected the token or the URL is unreachable",
            )
                .into_response();
        }
    };

    let sealed = match state.sealer.seal(body.token.as_bytes()) {
        Ok(sealed) => sealed,
        Err(e) => return internal(e),
    };
    match accounts::create(
        &state.pool,
        user.id,
        &body.jmap_session_url,
        &sealed,
        state.sealer.key_id(),
    )
    .await
    {
        Ok(account) => {
            let _ =
                accounts::set_jmap_account_id(&state.pool, account.id, client.account_id()).await;
            (StatusCode::CREATED, Json(json!({"id": account.id}))).into_response()
        }
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct UpdateAccount {
    recency_cutoff_days: Option<i32>,
    deletion_policy: Option<String>,
    poll_interval_secs: Option<i32>,
    sanitize_policy: Option<serde_json::Value>,
}

async fn update_account(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
    Json(body): Json<UpdateAccount>,
) -> Response {
    let Some(account) = (match owned_account(&state, user.id, id).await {
        Ok(a) => a,
        Err(r) => return r,
    }) else {
        return not_found();
    };
    if let Some(days) = body.recency_cutoff_days
        && !(0..=365).contains(&days)
    {
        return (StatusCode::BAD_REQUEST, "cutoff must be 0-365 days").into_response();
    }
    if let Some(policy) = &body.deletion_policy
        && policy != "retain"
        && policy != "mirror"
    {
        return (StatusCode::BAD_REQUEST, "deletion_policy: retain|mirror").into_response();
    }
    let result = sqlx::query!(
        r#"update mail_accounts set
               recency_cutoff_days = coalesce($2, recency_cutoff_days),
               deletion_policy = coalesce($3, deletion_policy),
               poll_interval_secs = coalesce($4, poll_interval_secs),
               sanitize_policy = coalesce($5, sanitize_policy)
           where id = $1"#,
        account.id,
        body.recency_cutoff_days,
        body.deletion_policy,
        body.poll_interval_secs,
        body.sanitize_policy,
    )
    .execute(&state.pool)
    .await;
    match result {
        Ok(_) => Json(json!({"ok": true})).into_response(),
        Err(e) => internal(e),
    }
}

async fn remove_account(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    let Some(account) = (match owned_account(&state, user.id, id).await {
        Ok(a) => a,
        Err(r) => return r,
    }) else {
        return not_found();
    };
    match accounts::delete(&state.pool, account.id).await {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct MintToken {
    label: String,
}

async fn mint_token(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Json(body): Json<MintToken>,
) -> Response {
    let generated = generate_token("mcp");
    match tokens::mint(&state.pool, user.id, &generated.hash, &body.label).await {
        Ok(row) => {
            let _ = audit::record(
                &state.pool,
                Some(user.id),
                &format!("session:{}", user.id),
                "mcp_token_minted",
                None,
                Some(&json!({"token_id": row.id, "label": body.label})),
            )
            .await;
            // The secret appears exactly once, in this response.
            (
                StatusCode::CREATED,
                Json(json!({"id": row.id, "token": generated.token})),
            )
                .into_response()
        }
        Err(e) => internal(e),
    }
}

async fn list_tokens(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    match tokens::list_for_user(&state.pool, user.id).await {
        Ok(list) => Json(json!({
            "tokens": list.iter().map(|t| json!({
                "id": t.id, "label": t.label, "created_at": t.created_at,
                "last_used_at": t.last_used_at, "revoked_at": t.revoked_at,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => internal(e),
    }
}

async fn revoke_token(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    match tokens::revoke(&state.pool, user.id, id).await {
        Ok(true) => {
            let _ = audit::record(
                &state.pool,
                Some(user.id),
                &format!("session:{}", user.id),
                "mcp_token_revoked",
                None,
                Some(&json!({"token_id": id})),
            )
            .await;
            Json(json!({"ok": true})).into_response()
        }
        Ok(false) => not_found(),
        Err(e) => internal(e),
    }
}

async fn list_passkeys(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    match auth::passkeys_for_user(&state.pool, user.id).await {
        Ok(rows) => Json(json!({
            "passkeys": rows.iter().map(|p| json!({
                "id": p.id, "label": p.label,
                "created_at": p.created_at, "last_used_at": p.last_used_at,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => internal(e),
    }
}

async fn remove_passkey(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
    Path(id): Path<i64>,
) -> Response {
    match auth::delete_passkey(&state.pool, user.id, id).await {
        Ok(true) => Json(json!({"ok": true})).into_response(),
        Ok(false) => (
            StatusCode::BAD_REQUEST,
            "cannot remove: not yours or it is your last passkey",
        )
            .into_response(),
        Err(e) => internal(e),
    }
}

async fn recent_audit(
    State(state): State<WebState>,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    match audit::recent_for_user(&state.pool, user.id, 200).await {
        Ok(entries) => Json(json!({
            "entries": entries.iter().map(|e| json!({
                "actor": e.actor, "action": e.action,
                "message_ref": e.message_ref, "detail": e.detail, "at": e.at,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => internal(e),
    }
}
