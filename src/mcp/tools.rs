//! The three read-only MCP tools (spec §9): search, get_message,
//! list_facets. Identity comes exclusively from the bearer-token
//! middleware via request extensions — tool arguments can never widen
//! scope past the token's user.

use std::sync::Arc;

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::db::audit;
use crate::embed::EmbeddingProvider;
use crate::mcp::auth::AuthedUser;
use crate::search::SearchClient;
use crate::search::client::FacetKind;
use crate::search::hybrid::hybrid_search;

#[derive(Clone)]
pub struct ArkivoMcp {
    pool: PgPool,
    search: Arc<SearchClient>,
    embedder: Arc<dyn EmbeddingProvider>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchParams {
    /// Search query: matched by keyword (BM25) and semantically (vector
    /// similarity), fused into one ranking.
    pub query: String,
    /// Maximum results to return (default 10, max 50).
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SearchResultItem {
    /// Stable reference for get_message.
    pub message_id: String,
    pub score: f32,
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub received_at: Option<String>,
    /// Best-matching passage (semantic hit) or leading body text.
    pub snippet: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SearchResults {
    pub results: Vec<SearchResultItem>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetMessageParams {
    /// A message_id returned by search.
    pub message_id: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct MessageDetail {
    pub message_id: String,
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub received_at: Option<String>,
    pub mailbox_ids: Vec<String>,
    pub has_attachments: bool,
    /// Sanitized body text as indexed.
    pub body_text: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListFacetsParams {
    /// Facet dimension: "from" (top senders), "mailbox", or "year".
    pub facet: String,
    /// Maximum buckets (default 20).
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct FacetItem {
    pub value: String,
    pub count: i64,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct FacetResults {
    pub facets: Vec<FacetItem>,
}

fn authed(ctx: &RequestContext<RoleServer>) -> Result<AuthedUser, ErrorData> {
    ctx.extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<AuthedUser>())
        .copied()
        .ok_or_else(|| ErrorData::internal_error("request missing authenticated user", None))
}

fn internal(e: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error(e.to_string(), None)
}

fn str_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[tool_router(server_handler)]
impl ArkivoMcp {
    pub fn new(
        pool: PgPool,
        search: Arc<SearchClient>,
        embedder: Arc<dyn EmbeddingProvider>,
    ) -> Self {
        Self {
            pool,
            search,
            embedder,
        }
    }

    #[tool(
        name = "search",
        description = "Hybrid keyword + semantic search over the user's email archive. \
                       Returns ranked message references with snippets. The archive \
                       contains messages older than the configured recency cutoff only."
    )]
    async fn search(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<Json<SearchResults>, ErrorData> {
        let user = authed(&ctx)?;
        let limit = params.limit.unwrap_or(10).clamp(1, 50);
        let ranked = hybrid_search(
            &self.search,
            self.embedder.as_ref(),
            user.user_id,
            &params.query,
            limit,
        )
        .await
        .map_err(internal)?;

        let results: Vec<SearchResultItem> = ranked
            .into_iter()
            .map(|r| {
                let src = r.msg_source.as_ref();
                let snippet = r.chunk_snippet.clone().or_else(|| {
                    src.and_then(|s| s.get("body_text"))
                        .and_then(|v| v.as_str())
                        .map(|t| t.chars().take(400).collect())
                });
                SearchResultItem {
                    message_id: r.message_id,
                    score: r.score,
                    subject: src
                        .and_then(|s| s.get("subject"))
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    from: str_list(src.and_then(|s| s.get("from"))),
                    received_at: src
                        .and_then(|s| s.get("received_at"))
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    snippet: snippet.map(|s| s.chars().take(400).collect()),
                }
            })
            .collect();

        audit::record(
            &self.pool,
            Some(user.user_id),
            &user.actor(),
            "search",
            None,
            Some(&serde_json::json!({"query": params.query, "results": results.len()})),
        )
        .await
        .map_err(internal)?;

        Ok(Json(SearchResults { results }))
    }

    #[tool(
        name = "get_message",
        description = "Fetch sanitized metadata and body text for one archived message \
                       by the message_id returned from search."
    )]
    async fn get_message(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<GetMessageParams>,
    ) -> Result<Json<MessageDetail>, ErrorData> {
        let user = authed(&ctx)?;
        let message_id: i64 = params
            .message_id
            .parse()
            .map_err(|_| ErrorData::invalid_params("message_id must be numeric", None))?;

        // The lookup runs against the token owner's index only: a foreign
        // id simply isn't there. Audit before the not-found return so
        // probing attempts leave a trail too.
        let doc = self
            .search
            .get_msg_doc(user.user_id, message_id)
            .await
            .map_err(internal)?;

        audit::record(
            &self.pool,
            Some(user.user_id),
            &user.actor(),
            "get_message",
            Some(&params.message_id),
            Some(&serde_json::json!({"found": doc.is_some()})),
        )
        .await
        .map_err(internal)?;

        let Some(doc) = doc else {
            return Err(ErrorData::resource_not_found("message not found", None));
        };
        let get_str = |k: &str| doc.get(k).and_then(|v| v.as_str()).map(String::from);
        Ok(Json(MessageDetail {
            message_id: params.message_id,
            subject: get_str("subject"),
            from: str_list(doc.get("from")),
            to: str_list(doc.get("to")),
            cc: str_list(doc.get("cc")),
            received_at: get_str("received_at"),
            mailbox_ids: str_list(doc.get("mailbox_ids")),
            has_attachments: doc
                .get("has_attachments")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            body_text: get_str("body_text").unwrap_or_default(),
        }))
    }

    #[tool(
        name = "list_facets",
        description = "Aggregate the archive for query refinement: facet by \"from\" \
                       (top senders), \"mailbox\", or \"year\" (message counts per year)."
    )]
    async fn list_facets(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<ListFacetsParams>,
    ) -> Result<Json<FacetResults>, ErrorData> {
        let user = authed(&ctx)?;
        let kind = match params.facet.as_str() {
            "from" => FacetKind::From,
            "mailbox" => FacetKind::Mailbox,
            "year" => FacetKind::Year,
            other => {
                return Err(ErrorData::invalid_params(
                    format!("unknown facet {other:?}; expected from|mailbox|year"),
                    None,
                ));
            }
        };
        let buckets = self
            .search
            .facets(user.user_id, kind, params.limit.unwrap_or(20).clamp(1, 100))
            .await
            .map_err(internal)?;

        audit::record(
            &self.pool,
            Some(user.user_id),
            &user.actor(),
            "list_facets",
            None,
            Some(&serde_json::json!({"facet": params.facet})),
        )
        .await
        .map_err(internal)?;

        Ok(Json(FacetResults {
            facets: buckets
                .into_iter()
                .map(|b| FacetItem {
                    value: b.value,
                    count: b.count,
                })
                .collect(),
        }))
    }
}
