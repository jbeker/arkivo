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
use crate::search::client::{FacetKind, SearchFilter};
use crate::search::hybrid::hybrid_search;

#[derive(Clone)]
pub struct ArkivoMcp {
    pool: PgPool,
    search: Arc<SearchClient>,
    embedder: Arc<dyn EmbeddingProvider>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchParams {
    /// Search text, matched by keyword (BM25) and semantically (vector
    /// similarity), fused into one ranking. Optional: omit it to list
    /// messages purely by the filters below (date-sorted).
    pub query: Option<String>,
    /// Maximum results per page (default 10, max 50).
    pub limit: Option<usize>,
    /// Results to skip, for pagination (default 0; offset+limit ≤ 1000).
    /// Combine with has_more/total in the response to walk all matches.
    pub offset: Option<usize>,
    /// Exact-match filter: keep only messages whose From is exactly this
    /// address (a bare address, e.g. "alice@example.com" — use a value
    /// from list_facets "from"). Combined with to/cc via AND.
    pub from: Option<String>,
    /// Exact-match filter on a To recipient address (bare address).
    pub to: Option<String>,
    /// Exact-match filter on a Cc recipient address (bare address).
    pub cc: Option<String>,
    /// Only messages received at/after this time, as "YYYY-MM-DD" or
    /// RFC3339 ("2019-03-01T00:00:00Z"). Inclusive.
    pub received_after: Option<String>,
    /// Only messages received at/before this time, as "YYYY-MM-DD"
    /// (whole day included) or RFC3339. Inclusive.
    pub received_before: Option<String>,
    /// Result order: "relevance" (default when query is set; hybrid
    /// ranking), "date_desc" (newest first; default without a query), or
    /// "date_asc". Date sorts search by keyword only — best for
    /// "most recent messages from X" questions.
    pub sort: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SearchResultItem {
    /// Stable reference for get_message.
    pub message_id: String,
    /// Conversation reference for get_thread, when known.
    pub thread_id: Option<String>,
    pub score: f32,
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub received_at: Option<String>,
    /// Query-term highlight, best semantic passage, or leading body text.
    pub snippet: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SearchResults {
    pub results: Vec<SearchResultItem>,
    /// Total matching messages. Exact for date sorts; for relevance
    /// (hybrid) ranking it counts the keyword side only, so treat it as
    /// a floor.
    pub total: i64,
    /// True when another page exists beyond offset+limit.
    pub has_more: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetMessageParams {
    /// A message_id returned by search.
    pub message_id: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct MessageDetail {
    pub message_id: String,
    /// Conversation reference for get_thread, when known.
    pub thread_id: Option<String>,
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
pub struct GetThreadParams {
    /// A thread_id returned by search or get_message.
    pub thread_id: String,
    /// Maximum messages to return (default 50, max 100), oldest first.
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ThreadResults {
    pub thread_id: String,
    /// The thread's messages in date order (oldest first).
    pub messages: Vec<MessageDetail>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListFacetsParams {
    /// Facet dimension: "from" (top senders), "mailbox", "year", or
    /// "month" (message counts per period).
    pub facet: String,
    /// Maximum buckets (default 20).
    pub limit: Option<usize>,
    /// Optional keyword query scoping the aggregation, e.g. top senders
    /// among messages matching "invoice".
    pub query: Option<String>,
    /// Exact-match From filter (bare address), AND-combined.
    pub from: Option<String>,
    /// Exact-match To filter (bare address).
    pub to: Option<String>,
    /// Exact-match Cc filter (bare address).
    pub cc: Option<String>,
    /// Only messages received at/after ("YYYY-MM-DD" or RFC3339).
    pub received_after: Option<String>,
    /// Only messages received at/before ("YYYY-MM-DD" or RFC3339).
    pub received_before: Option<String>,
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

/// Log the detail server-side, return a generic error: backend error
/// strings (OpenSearch, embedding, DB) must not reach MCP clients.
fn internal(e: impl std::fmt::Display) -> ErrorData {
    tracing::error!(error = %e, "mcp tool error");
    ErrorData::internal_error("internal error", None)
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

/// Accepts "YYYY-MM-DD" or RFC3339 and yields an RFC3339 instant. A bare
/// end-bound date means "through that whole day".
fn normalize_date(value: &str, end_of_day: bool) -> Result<String, ErrorData> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(dt.to_rfc3339());
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let time = if end_of_day {
            date.and_hms_milli_opt(23, 59, 59, 999)
        } else {
            date.and_hms_opt(0, 0, 0)
        }
        .expect("in-range time components");
        return Ok(format!("{}Z", time.format("%Y-%m-%dT%H:%M:%S%.3f")));
    }
    Err(ErrorData::invalid_params(
        format!("invalid date {value:?}; expected YYYY-MM-DD or RFC3339"),
        None,
    ))
}

/// Shared address+date filter assembly for search and list_facets.
fn build_filter(
    from: Option<String>,
    to: Option<String>,
    cc: Option<String>,
    received_after: Option<&str>,
    received_before: Option<&str>,
) -> Result<SearchFilter, ErrorData> {
    Ok(SearchFilter {
        from,
        to,
        cc,
        received_after: received_after
            .map(|v| normalize_date(v, false))
            .transpose()?,
        received_before: received_before
            .map(|v| normalize_date(v, true))
            .transpose()?,
    })
}

/// Build a MessageDetail from a msg-index _source document.
fn message_detail(message_id: String, doc: &serde_json::Value) -> MessageDetail {
    let get_str = |k: &str| doc.get(k).and_then(|v| v.as_str()).map(String::from);
    MessageDetail {
        message_id,
        thread_id: get_str("thread_id"),
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
    }
}

/// Snippet preference: query-term highlight (keyword grounding), then
/// the best semantic passage, then leading body text. Always ≤400 chars.
fn choose_snippet(
    highlight: Option<String>,
    chunk_snippet: Option<String>,
    source: Option<&serde_json::Value>,
) -> Option<String> {
    highlight
        .or(chunk_snippet)
        .or_else(|| {
            source
                .and_then(|s| s.get("body_text"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .map(|s| s.chars().take(400).collect())
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
        description = "Search the user's email archive: hybrid keyword + semantic \
                       ranking, or date-ordered listing with sort=date_desc/date_asc \
                       (query optional for date sorts). Filters: from/to/cc exact \
                       addresses, received_after/received_before dates. Paginate with \
                       offset + has_more. The archive contains messages older than \
                       the configured recency cutoff only."
    )]
    async fn search(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<Json<SearchResults>, ErrorData> {
        let user = authed(&ctx)?;
        let limit = params.limit.unwrap_or(10).clamp(1, 50);
        let offset = params.offset.unwrap_or(0);
        if offset + limit > 1000 {
            return Err(ErrorData::invalid_params(
                "offset+limit must not exceed 1000",
                None,
            ));
        }
        let filter = build_filter(
            params.from.clone(),
            params.to.clone(),
            params.cc.clone(),
            params.received_after.as_deref(),
            params.received_before.as_deref(),
        )?;
        let query = params
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty());

        // Relevance needs a query; without one we fall back to newest-first.
        let sort = match (params.sort.as_deref(), query) {
            (None, Some(_)) | (Some("relevance"), Some(_)) => "relevance",
            (Some("relevance"), None) => {
                return Err(ErrorData::invalid_params(
                    "sort=relevance requires a query; use date_desc/date_asc to browse",
                    None,
                ));
            }
            (None, None) | (Some("date_desc"), _) => "date_desc",
            (Some("date_asc"), _) => "date_asc",
            (Some(other), _) => {
                return Err(ErrorData::invalid_params(
                    format!("unknown sort {other:?}; expected relevance|date_desc|date_asc"),
                    None,
                ));
            }
        };

        let (results, total, has_more) = if sort == "relevance" {
            let page = hybrid_search(
                &self.search,
                self.embedder.as_ref(),
                user.user_id,
                query.expect("relevance implies query"),
                limit,
                offset,
                &filter,
            )
            .await
            .map_err(internal)?;
            let results: Vec<SearchResultItem> = page
                .results
                .into_iter()
                .map(|r| {
                    let src = r.msg_source.as_ref();
                    let get_str = |k: &str| {
                        src.and_then(|s| s.get(k))
                            .and_then(|v| v.as_str())
                            .map(String::from)
                    };
                    SearchResultItem {
                        thread_id: get_str("thread_id"),
                        score: r.score,
                        subject: get_str("subject"),
                        from: str_list(src.and_then(|s| s.get("from"))),
                        received_at: get_str("received_at"),
                        snippet: choose_snippet(r.highlight, r.chunk_snippet, src),
                        message_id: r.message_id,
                    }
                })
                .collect();
            (results, page.total, page.has_more)
        } else {
            let page = self
                .search
                .list_messages(
                    user.user_id,
                    query,
                    &filter,
                    limit,
                    offset,
                    sort == "date_asc",
                )
                .await
                .map_err(internal)?;
            let has_more = (offset + page.hits.len()) < page.total as usize;
            let results: Vec<SearchResultItem> = page
                .hits
                .into_iter()
                .map(|hit| {
                    let src = Some(&hit.source);
                    let get_str =
                        |k: &str| hit.source.get(k).and_then(|v| v.as_str()).map(String::from);
                    SearchResultItem {
                        thread_id: get_str("thread_id"),
                        score: hit.score,
                        subject: get_str("subject"),
                        from: str_list(hit.source.get("from")),
                        received_at: get_str("received_at"),
                        snippet: choose_snippet(hit.highlight.clone(), None, src),
                        message_id: hit.message_id,
                    }
                })
                .collect();
            (results, page.total, has_more)
        };

        audit::record(
            &self.pool,
            Some(user.user_id),
            &user.actor(),
            "search",
            None,
            Some(&serde_json::json!({
                "query": params.query,
                "from": params.from,
                "to": params.to,
                "cc": params.cc,
                "received_after": params.received_after,
                "received_before": params.received_before,
                "sort": sort,
                "offset": offset,
                "results": results.len(),
                "total": total,
            })),
        )
        .await
        .map_err(internal)?;

        Ok(Json(SearchResults {
            results,
            total,
            has_more,
        }))
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
        Ok(Json(message_detail(params.message_id, &doc)))
    }

    #[tool(
        name = "get_thread",
        description = "Fetch a whole conversation: every archived message sharing the \
                       thread_id returned by search or get_message, oldest first, with \
                       sanitized metadata and body text."
    )]
    async fn get_thread(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<GetThreadParams>,
    ) -> Result<Json<ThreadResults>, ErrorData> {
        let user = authed(&ctx)?;
        let limit = params.limit.unwrap_or(50).clamp(1, 100);

        // Like get_message: the term query runs against the token
        // owner's index only, and probes are audited before returning.
        let docs = self
            .search
            .thread_messages(user.user_id, &params.thread_id, limit)
            .await
            .map_err(internal)?;

        audit::record(
            &self.pool,
            Some(user.user_id),
            &user.actor(),
            "get_thread",
            Some(&params.thread_id),
            Some(&serde_json::json!({"messages": docs.len()})),
        )
        .await
        .map_err(internal)?;

        if docs.is_empty() {
            return Err(ErrorData::resource_not_found("thread not found", None));
        }
        let messages = docs
            .iter()
            .map(|doc| {
                let id = doc
                    .get("message_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                message_detail(id, doc)
            })
            .collect();
        Ok(Json(ThreadResults {
            thread_id: params.thread_id,
            messages,
        }))
    }

    #[tool(
        name = "list_facets",
        description = "Aggregate the archive for query refinement: facet by \"from\" \
                       (top senders), \"mailbox\", \"year\", or \"month\". Optionally \
                       scope with a keyword query and from/to/cc/date filters, e.g. \
                       top senders among messages matching \"invoice\" in 2020."
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
            "month" => FacetKind::Month,
            other => {
                return Err(ErrorData::invalid_params(
                    format!("unknown facet {other:?}; expected from|mailbox|year|month"),
                    None,
                ));
            }
        };
        let filter = build_filter(
            params.from.clone(),
            params.to.clone(),
            params.cc.clone(),
            params.received_after.as_deref(),
            params.received_before.as_deref(),
        )?;
        let buckets = self
            .search
            .facets(
                user.user_id,
                kind,
                params.limit.unwrap_or(20).clamp(1, 100),
                params.query.as_deref(),
                &filter,
            )
            .await
            .map_err(internal)?;

        audit::record(
            &self.pool,
            Some(user.user_id),
            &user.actor(),
            "list_facets",
            None,
            Some(&serde_json::json!({
                "facet": params.facet,
                "query": params.query,
                "from": params.from,
                "to": params.to,
                "cc": params.cc,
                "received_after": params.received_after,
                "received_before": params.received_before,
            })),
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
