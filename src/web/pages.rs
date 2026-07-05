//! Server-rendered shell pages (askama) plus the static JS that drives
//! the WebAuthn ceremonies and JSON API. Deliberately small: the spec
//! mandates no frontend framework.

use askama::Template;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};

use super::{CurrentUser, WebState};

#[derive(Template)]
#[template(path = "login.html")]
struct LoginPage;

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardPage {
    handle: String,
    is_admin: bool,
}

fn render<T: Template>(template: T) -> Response {
    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "template render failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "render error").into_response()
        }
    }
}

pub fn public_router() -> Router<WebState> {
    Router::new()
        .route("/login", get(|| async { render(LoginPage) }))
        .route(
            "/static/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/javascript")],
                    include_str!("../../static/app.js"),
                )
            }),
        )
}

pub fn authed_router() -> Router<WebState> {
    Router::new().route(
        "/",
        get(
            |Extension(CurrentUser(user)): Extension<CurrentUser>| async move {
                render(DashboardPage {
                    handle: user.handle.clone(),
                    is_admin: user.is_admin(),
                })
            },
        ),
    )
}
