//! WebAuthn ceremonies (spec §11.1). Every flow is two legs with the
//! serialized ceremony state parked in the server-side session between
//! them: registration (invite-gated), login, add-passkey (session-
//! gated), and recovery (admin-issued single-use code).

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use tower_sessions::Session;
use uuid::Uuid;
use webauthn_rs::prelude::{
    CreationChallengeResponse, PasskeyAuthentication, PasskeyRegistration, PublicKeyCredential,
    RegisterPublicKeyCredential, RequestChallengeResponse,
};

use super::{CurrentUser, SESSION_USER_KEY, WebState};
use crate::crypto::hash_token;
use crate::db::{audit, auth, users};

const REG_STATE_KEY: &str = "reg_state";
const LOGIN_STATE_KEY: &str = "login_state";

pub fn router() -> Router<WebState> {
    Router::new()
        .route("/auth/setup-needed", axum::routing::get(setup_needed))
        .route("/auth/setup/start", post(setup_start))
        .route("/auth/setup/finish", post(register_finish))
        .route("/auth/register/start", post(register_start))
        .route("/auth/register/finish", post(register_finish))
        .route("/auth/login/start", post(login_start))
        .route("/auth/login/finish", post(login_finish))
        .route("/auth/recover/start", post(recover_start))
        .route("/auth/recover/finish", post(recover_finish))
        .route("/auth/logout", post(logout))
}

/// Add-passkey needs a signed-in session; mounted under the authed router.
pub fn authed_router() -> Router<WebState> {
    Router::new()
        .route("/auth/add-passkey/start", post(add_passkey_start))
        .route("/auth/add-passkey/finish", post(add_passkey_finish))
}

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, msg.to_string()).into_response()
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, "auth handler error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// Ceremony context parked in the session between the two legs.
#[derive(serde::Serialize, serde::Deserialize)]
struct RegContext {
    state: PasskeyRegistration,
    handle: String,
    webauthn_uuid: Uuid,
    /// Invite being consumed (new user) — or None for add-passkey /
    /// recovery flows targeting an existing user.
    invite_id: Option<i64>,
    existing_user_id: Option<i64>,
    recovery_id: Option<i64>,
    /// First-run setup: creates the first admin without an invite,
    /// guarded transactionally on "no users exist yet".
    #[serde(default)]
    setup: bool,
}

async fn no_users_yet(state: &WebState) -> anyhow::Result<bool> {
    let count: i64 = sqlx::query_scalar!(r#"select count(*) as "n!" from users"#)
        .fetch_one(&state.pool)
        .await?;
    Ok(count == 0)
}

/// First-run probe: the login page shows the setup card iff this is true.
async fn setup_needed(State(state): State<WebState>) -> Response {
    match no_users_yet(&state).await {
        Ok(needed) => Json(serde_json::json!({"needed": needed})).into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct SetupStart {
    handle: String,
}

/// First-run setup (replaces the CLI bootstrap): while no user exists,
/// anyone reaching the service may create the first admin. The finish
/// leg re-checks emptiness under a table lock, so the window closes
/// atomically with the first successful registration.
async fn setup_start(
    State(state): State<WebState>,
    session: Session,
    Json(body): Json<SetupStart>,
) -> Response {
    let handle = body.handle.trim().to_string();
    if handle.is_empty() || handle.len() > 64 {
        return bad_request("handle must be 1-64 characters");
    }
    match no_users_yet(&state).await {
        Ok(true) => {}
        Ok(false) => return bad_request("setup already completed"),
        Err(e) => return internal(e),
    }

    let webauthn_uuid = Uuid::new_v4();
    let (ccr, reg_state) =
        match state
            .webauthn
            .start_passkey_registration(webauthn_uuid, &handle, &handle, None)
        {
            Ok(pair) => pair,
            Err(e) => return internal(e),
        };
    let ctx = RegContext {
        state: reg_state,
        handle,
        webauthn_uuid,
        invite_id: None,
        existing_user_id: None,
        recovery_id: None,
        setup: true,
    };
    if let Err(e) = session.insert(REG_STATE_KEY, &ctx).await {
        return internal(e);
    }
    Json(ccr).into_response()
}

#[derive(Deserialize)]
struct RegisterStart {
    invite_code: String,
    handle: String,
}

async fn register_start(
    State(state): State<WebState>,
    session: Session,
    Json(body): Json<RegisterStart>,
) -> Response {
    let handle = body.handle.trim().to_string();
    if handle.is_empty() || handle.len() > 64 {
        return bad_request("handle must be 1-64 characters");
    }
    let invite = match auth::find_live_invite(&state.pool, &hash_token(&body.invite_code)).await {
        Ok(Some(invite)) => invite,
        Ok(None) => return bad_request("invalid or expired invite"),
        Err(e) => return internal(e),
    };
    match users::get_by_handle(&state.pool, &handle).await {
        Ok(None) => {}
        Ok(Some(_)) => return bad_request("handle already taken"),
        Err(e) => return internal(e),
    }

    let webauthn_uuid = Uuid::new_v4();
    let (ccr, reg_state): (CreationChallengeResponse, PasskeyRegistration) = match state
        .webauthn
        .start_passkey_registration(webauthn_uuid, &handle, &handle, None)
    {
        Ok(pair) => pair,
        Err(e) => return internal(e),
    };
    let ctx = RegContext {
        state: reg_state,
        handle,
        webauthn_uuid,
        invite_id: Some(invite.id),
        existing_user_id: None,
        recovery_id: None,
        setup: false,
    };
    if let Err(e) = session.insert(REG_STATE_KEY, &ctx).await {
        return internal(e);
    }
    Json(ccr).into_response()
}

async fn register_finish(
    State(state): State<WebState>,
    session: Session,
    Json(credential): Json<RegisterPublicKeyCredential>,
) -> Response {
    let ctx: RegContext = match session.remove(REG_STATE_KEY).await {
        Ok(Some(ctx)) => ctx,
        _ => return bad_request("no registration in progress"),
    };
    let passkey = match state
        .webauthn
        .finish_passkey_registration(&credential, &ctx.state)
    {
        Ok(passkey) => passkey,
        Err(e) => {
            tracing::warn!(error = %e, "passkey registration failed verification");
            return bad_request("credential verification failed");
        }
    };
    let passkey_json = match serde_json::to_value(&passkey) {
        Ok(v) => v,
        Err(e) => return internal(e),
    };

    let result: anyhow::Result<i64> = async {
        let mut tx = state.pool.begin().await?;
        let user_id = if ctx.setup {
            // First-run setup: serialize against concurrent attempts and
            // re-check emptiness inside the transaction. Once any user
            // exists this path is permanently closed.
            sqlx::query("lock table users in exclusive mode")
                .execute(&mut *tx)
                .await?;
            let existing: i64 = sqlx::query_scalar!(r#"select count(*) as "n!" from users"#)
                .fetch_one(&mut *tx)
                .await?;
            anyhow::ensure!(existing == 0, "setup already completed");
            auth::create_user_with_uuid(&mut tx, &ctx.handle, "admin", ctx.webauthn_uuid).await?
        } else {
            match (ctx.invite_id, ctx.existing_user_id, ctx.recovery_id) {
                // New user via invite.
                (Some(invite_id), None, None) => {
                    let role =
                        sqlx::query_scalar!("select role from invites where id = $1", invite_id)
                            .fetch_one(&mut *tx)
                            .await?;
                    let user_id =
                        auth::create_user_with_uuid(&mut tx, &ctx.handle, &role, ctx.webauthn_uuid)
                            .await?;
                    anyhow::ensure!(
                        auth::consume_invite(&mut tx, invite_id, user_id).await?,
                        "invite already consumed"
                    );
                    user_id
                }
                // Recovery: new passkey for an existing user.
                (None, Some(user_id), Some(recovery_id)) => {
                    anyhow::ensure!(
                        auth::consume_recovery(&mut tx, recovery_id).await?,
                        "recovery code already consumed"
                    );
                    user_id
                }
                // Add-passkey while signed in.
                (None, Some(user_id), None) => user_id,
                _ => anyhow::bail!("inconsistent registration context"),
            }
        };
        sqlx::query!(
            r#"insert into passkeys (user_id, credential_id, passkey, label)
               values ($1, $2, $3, $4)"#,
            user_id,
            passkey.cred_id().as_slice(),
            passkey_json,
            "",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(user_id)
    }
    .await;

    match result {
        Ok(user_id) => {
            let _ = session.cycle_id().await;
            if let Err(e) = session.insert(SESSION_USER_KEY, user_id).await {
                return internal(e);
            }
            let _ = audit::record(
                &state.pool,
                Some(user_id),
                &format!("session:{user_id}"),
                "passkey_registered",
                None,
                None,
            )
            .await;
            Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %format!("{e:#}"), "registration finish failed");
            bad_request("registration failed")
        }
    }
}

#[derive(Deserialize)]
struct LoginStart {
    handle: String,
}

/// Parked between login legs: ceremony state + the claimed user.
#[derive(serde::Serialize, serde::Deserialize)]
struct LoginContext {
    state: PasskeyAuthentication,
    user_id: i64,
}

async fn login_start(
    State(state): State<WebState>,
    session: Session,
    Json(body): Json<LoginStart>,
) -> Response {
    // Uniform error for unknown handle vs no passkeys: don't leak which.
    let denied = || bad_request("login unavailable for this handle");

    let user = match users::get_by_handle(&state.pool, body.handle.trim()).await {
        Ok(Some(user)) if user.is_active() => user,
        Ok(_) => return denied(),
        Err(e) => return internal(e),
    };
    let rows = match auth::passkeys_for_user(&state.pool, user.id).await {
        Ok(rows) if !rows.is_empty() => rows,
        Ok(_) => return denied(),
        Err(e) => return internal(e),
    };
    let passkeys: Vec<webauthn_rs::prelude::Passkey> = rows
        .iter()
        .filter_map(|r| serde_json::from_value(r.passkey.clone()).ok())
        .collect();
    let (rcr, auth_state): (RequestChallengeResponse, PasskeyAuthentication) =
        match state.webauthn.start_passkey_authentication(&passkeys) {
            Ok(pair) => pair,
            Err(e) => return internal(e),
        };
    let ctx = LoginContext {
        state: auth_state,
        user_id: user.id,
    };
    if let Err(e) = session.insert(LOGIN_STATE_KEY, &ctx).await {
        return internal(e);
    }
    Json(rcr).into_response()
}

async fn login_finish(
    State(state): State<WebState>,
    session: Session,
    Json(credential): Json<PublicKeyCredential>,
) -> Response {
    let ctx: LoginContext = match session.remove(LOGIN_STATE_KEY).await {
        Ok(Some(ctx)) => ctx,
        _ => return bad_request("no login in progress"),
    };
    let result = match state
        .webauthn
        .finish_passkey_authentication(&credential, &ctx.state)
    {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!(error = %e, "passkey assertion failed");
            return bad_request("assertion failed");
        }
    };

    // Persist the updated sign counter on whichever passkey matched.
    let rows = match auth::passkeys_for_user(&state.pool, ctx.user_id).await {
        Ok(rows) => rows,
        Err(e) => return internal(e),
    };
    for row in rows {
        let Ok(mut passkey) =
            serde_json::from_value::<webauthn_rs::prelude::Passkey>(row.passkey.clone())
        else {
            continue;
        };
        if let Some(true) = passkey.update_credential(&result) {
            let Ok(updated) = serde_json::to_value(&passkey) else {
                continue;
            };
            let _ = auth::update_passkey(&state.pool, row.id, &updated).await;
        }
    }

    let _ = session.cycle_id().await;
    if let Err(e) = session.insert(SESSION_USER_KEY, ctx.user_id).await {
        return internal(e);
    }
    let _ = audit::record(
        &state.pool,
        Some(ctx.user_id),
        &format!("session:{}", ctx.user_id),
        "login",
        None,
        None,
    )
    .await;
    Json(serde_json::json!({"ok": true})).into_response()
}

#[derive(Deserialize)]
struct RecoverStart {
    handle: String,
    recovery_code: String,
}

async fn recover_start(
    State(state): State<WebState>,
    session: Session,
    Json(body): Json<RecoverStart>,
) -> Response {
    let denied = || bad_request("recovery unavailable");
    let user = match users::get_by_handle(&state.pool, body.handle.trim()).await {
        Ok(Some(user)) if user.is_active() => user,
        Ok(_) => return denied(),
        Err(e) => return internal(e),
    };
    let recovery_id = match auth::find_live_recovery(
        &state.pool,
        user.id,
        &hash_token(&body.recovery_code),
    )
    .await
    {
        Ok(Some(id)) => id,
        Ok(None) => return denied(),
        Err(e) => return internal(e),
    };
    let webauthn_uuid = match auth::user_webauthn_uuid(&state.pool, user.id).await {
        Ok(uuid) => uuid,
        Err(e) => return internal(e),
    };
    let (ccr, reg_state) = match state.webauthn.start_passkey_registration(
        webauthn_uuid,
        &user.handle,
        &user.handle,
        None,
    ) {
        Ok(pair) => pair,
        Err(e) => return internal(e),
    };
    let ctx = RegContext {
        state: reg_state,
        handle: user.handle.clone(),
        webauthn_uuid,
        invite_id: None,
        existing_user_id: Some(user.id),
        recovery_id: Some(recovery_id),
        setup: false,
    };
    if let Err(e) = session.insert(REG_STATE_KEY, &ctx).await {
        return internal(e);
    }
    Json(ccr).into_response()
}

/// Recovery finish shares the registration finish handler's logic.
async fn recover_finish(
    state: State<WebState>,
    session: Session,
    credential: Json<RegisterPublicKeyCredential>,
) -> Response {
    register_finish(state, session, credential).await
}

async fn add_passkey_start(
    State(state): State<WebState>,
    session: Session,
    Extension(CurrentUser(user)): Extension<CurrentUser>,
) -> Response {
    let webauthn_uuid = match auth::user_webauthn_uuid(&state.pool, user.id).await {
        Ok(uuid) => uuid,
        Err(e) => return internal(e),
    };
    let (ccr, reg_state) = match state.webauthn.start_passkey_registration(
        webauthn_uuid,
        &user.handle,
        &user.handle,
        None,
    ) {
        Ok(pair) => pair,
        Err(e) => return internal(e),
    };
    let ctx = RegContext {
        state: reg_state,
        handle: user.handle.clone(),
        webauthn_uuid,
        invite_id: None,
        existing_user_id: Some(user.id),
        recovery_id: None,
        setup: false,
    };
    if let Err(e) = session.insert(REG_STATE_KEY, &ctx).await {
        return internal(e);
    }
    Json(ccr).into_response()
}

async fn add_passkey_finish(
    state: State<WebState>,
    session: Session,
    credential: Json<RegisterPublicKeyCredential>,
) -> Response {
    register_finish(state, session, credential).await
}

async fn logout(session: Session) -> Response {
    let _ = session.flush().await;
    Json(serde_json::json!({"ok": true})).into_response()
}
