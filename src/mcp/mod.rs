//! Read-only MCP server (spec §9). Capability separation is structural:
//! this process holds no Fastmail credential, no send path, and no write
//! path to any store; tools only read the user's own OpenSearch indices,
//! which contain exclusively post-cutoff, sanitized content. Every
//! message access is audit-logged.

pub mod auth;
pub mod tools;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use sqlx::PgPool;

use crate::embed::EmbeddingProvider;
use crate::search::SearchClient;
use tools::ArkivoMcp;

/// Build the full axum app: bearer-authenticated /mcp plus unauthenticated
/// health probes. Stateless JSON mode: each POST is independent, no SSE.
pub fn app(
    pool: PgPool,
    search: Arc<SearchClient>,
    embedder: Arc<dyn EmbeddingProvider>,
    allowed_hosts: Vec<String>,
) -> Router {
    let config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_allowed_hosts(allowed_hosts);
    let mcp_pool = pool.clone();
    let mcp_service = StreamableHttpService::new(
        move || {
            Ok(ArkivoMcp::new(
                mcp_pool.clone(),
                search.clone(),
                embedder.clone(),
            ))
        },
        Arc::new(NeverSessionManager::default()),
        config,
    );

    let protected = Router::new().nest_service("/mcp", mcp_service).layer(
        axum::middleware::from_fn_with_state(pool.clone(), auth::require_token),
    );

    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get({
                let pool = pool.clone();
                move || {
                    let pool = pool.clone();
                    async move {
                        match sqlx::query("select 1").execute(&pool).await {
                            Ok(_) => (axum::http::StatusCode::OK, "ready"),
                            Err(_) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "db down"),
                        }
                    }
                }
            }),
        )
        .merge(protected)
}
