//! Thin OpenSearch REST wrapper over reqwest. The surface is small
//! (index management, doc puts, _bulk, _search, _delete_by_query), and
//! staying at the HTTP level keeps kNN/pipeline features available the
//! moment the server supports them.

use anyhow::{Context, Result};
use reqwest::{Method, RequestBuilder};
use serde_json::{Value, json};

use crate::config::OpenSearchConfig;
use crate::search::mappings;

pub struct SearchClient {
    http: reqwest::Client,
    base: String,
    username: Option<String>,
    password: Option<String>,
}

/// One search hit, collapsed to what ranking needs.
#[derive(Debug, Clone)]
pub struct Hit {
    pub message_id: String,
    pub score: f32,
    pub source: Value,
    /// Query-term-centered fragments (plain text, " … "-joined) when the
    /// request asked for highlighting.
    pub highlight: Option<String>,
}

/// One page of hits plus the index-side total match count.
#[derive(Debug, Clone)]
pub struct SearchPage {
    pub hits: Vec<Hit>,
    pub total: i64,
}

#[derive(Debug, Clone, Copy)]
pub enum FacetKind {
    From,
    Mailbox,
    Year,
    Month,
}

/// Optional constraints applied alongside the query: exact-match
/// sender/recipient addresses (AND-combined) and a received_at range
/// (RFC3339 instants, inclusive). Empty means "no constraint".
#[derive(Debug, Clone, Default)]
pub struct SearchFilter {
    pub from: Option<String>,
    pub to: Option<String>,
    pub cc: Option<String>,
    pub received_after: Option<String>,
    pub received_before: Option<String>,
}

impl SearchFilter {
    pub fn is_empty(&self) -> bool {
        self.from.is_none()
            && self.to.is_none()
            && self.cc.is_none()
            && self.received_after.is_none()
            && self.received_before.is_none()
    }

    /// AND-combined `term` clauses in from/to/cc order. `suffix` is ".raw"
    /// for the msg index (from/to/cc are text with a .raw keyword
    /// sub-field) and "" for the chunk index (from/to/cc are plain keyword).
    fn term_clauses(&self, suffix: &str) -> Vec<Value> {
        [("from", &self.from), ("to", &self.to), ("cc", &self.cc)]
            .into_iter()
            .filter_map(|(field, value)| {
                value
                    .as_ref()
                    .map(|value| json!({"term": {(format!("{field}{suffix}")): value}}))
            })
            .collect()
    }

    /// Inclusive range clause on received_at, when either bound is set.
    /// Both indices map received_at as `date` (chunk docs since pipeline v3).
    fn range_clause(&self) -> Option<Value> {
        if self.received_after.is_none() && self.received_before.is_none() {
            return None;
        }
        let mut range = serde_json::Map::new();
        if let Some(after) = &self.received_after {
            range.insert("gte".into(), json!(after));
        }
        if let Some(before) = &self.received_before {
            range.insert("lte".into(), json!(before));
        }
        Some(json!({"range": {"received_at": range}}))
    }

    /// All filter clauses for one index flavor (see `term_clauses`).
    fn clauses(&self, suffix: &str) -> Vec<Value> {
        let mut clauses = self.term_clauses(suffix);
        clauses.extend(self.range_clause());
        clauses
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FacetBucket {
    pub value: String,
    pub count: i64,
}

impl SearchClient {
    pub fn new(config: &OpenSearchConfig) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()?,
            base: config.url.trim_end_matches('/').to_string(),
            username: config.username.clone(),
            password: config.password.clone(),
        })
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let builder = self.http.request(method, format!("{}{path}", self.base));
        match &self.username {
            Some(user) => builder.basic_auth(user, self.password.as_deref()),
            None => builder,
        }
    }

    async fn send_json(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value> {
        let mut builder = self.request(method, path);
        if let Some(body) = body {
            builder = builder.json(body);
        }
        let response = builder.send().await?;
        let status = response.status();
        let value: Value = response.json().await.unwrap_or(Value::Null);
        anyhow::ensure!(
            status.is_success(),
            "OpenSearch {path} returned {status}: {value}"
        );
        Ok(value)
    }

    /// POST a `_search` body, treating a not-yet-created index as empty
    /// results. A fresh account that has never been promoted has no
    /// indices, so an agent searching it should get nothing back rather
    /// than an index_not_found error.
    async fn search(&self, index: &str, body: &Value) -> Result<Value> {
        let response = self
            .request(Method::POST, &format!("/{index}/_search"))
            .json(body)
            .send()
            .await?;
        let status = response.status();
        if status.as_u16() == 404 {
            return Ok(json!({"hits": {"hits": [], "total": {"value": 0}}}));
        }
        let value: Value = response.json().await.unwrap_or(Value::Null);
        anyhow::ensure!(
            status.is_success(),
            "OpenSearch /{index}/_search returned {status}: {value}"
        );
        Ok(value)
    }

    pub async fn health(&self) -> Result<Value> {
        self.send_json(Method::GET, "/_cluster/health", None).await
    }

    async fn index_exists(&self, name: &str) -> Result<bool> {
        let response = self
            .request(Method::HEAD, &format!("/{name}"))
            .send()
            .await?;
        Ok(response.status().is_success())
    }

    /// Create the per-user msg and chunk indices if absent.
    pub async fn ensure_user_indices(&self, user_id: i64, dimension: usize) -> Result<()> {
        let msg = mappings::msg_index_name(user_id);
        if !self.index_exists(&msg).await? {
            self.send_json(
                Method::PUT,
                &format!("/{msg}"),
                Some(&mappings::msg_index_body()),
            )
            .await
            .context("creating msg index")?;
        }
        let chunk = mappings::chunk_index_name(user_id);
        if !self.index_exists(&chunk).await? {
            self.send_json(
                Method::PUT,
                &format!("/{chunk}"),
                Some(&mappings::chunk_index_body(dimension)),
            )
            .await
            .context("creating chunk index")?;
        }
        Ok(())
    }

    pub async fn delete_user_indices(&self, user_id: i64) -> Result<()> {
        for name in [
            mappings::msg_index_name(user_id),
            mappings::chunk_index_name(user_id),
        ] {
            if self.index_exists(&name).await? {
                self.send_json(Method::DELETE, &format!("/{name}"), None)
                    .await?;
            }
        }
        Ok(())
    }

    /// Upsert the message document (id = ledger row id).
    pub async fn put_msg_doc(&self, user_id: i64, message_id: i64, doc: &Value) -> Result<()> {
        let index = mappings::msg_index_name(user_id);
        self.send_json(
            Method::PUT,
            &format!("/{index}/_doc/{message_id}"),
            Some(doc),
        )
        .await?;
        Ok(())
    }

    /// Bulk-upsert chunk documents, keyed `{message_id}:{chunk_index}` so
    /// a reindex overwrite is idempotent.
    #[allow(clippy::too_many_arguments)]
    pub async fn bulk_chunks(
        &self,
        user_id: i64,
        message_id: i64,
        chunks: &[(usize, String, Vec<f32>)],
        from: &[String],
        to: &[String],
        cc: &[String],
        received_at: &str,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let index = mappings::chunk_index_name(user_id);
        let mut body = String::new();
        for (chunk_index, text, vector) in chunks {
            body.push_str(
                &json!({"index": {"_index": index, "_id": format!("{message_id}:{chunk_index}")}})
                    .to_string(),
            );
            body.push('\n');
            body.push_str(
                &json!({
                    "message_id": message_id.to_string(),
                    "chunk_index": chunk_index,
                    "chunk_text": text,
                    "embedding": vector,
                    "from": from,
                    "to": to,
                    "cc": cc,
                    "received_at": received_at,
                })
                .to_string(),
            );
            body.push('\n');
        }
        let response = self
            .request(Method::POST, "/_bulk")
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let value: Value = response.json().await.unwrap_or(Value::Null);
        anyhow::ensure!(status.is_success(), "bulk failed: {status}");
        anyhow::ensure!(
            value.get("errors").and_then(Value::as_bool) != Some(true),
            "bulk reported item errors: {value}"
        );
        Ok(())
    }

    /// Remove every trace of a message from both indices (mirror policy,
    /// quarantine reversal).
    pub async fn delete_message_docs(&self, user_id: i64, message_id: i64) -> Result<()> {
        let msg = mappings::msg_index_name(user_id);
        if self.index_exists(&msg).await? {
            let response = self
                .request(Method::DELETE, &format!("/{msg}/_doc/{message_id}"))
                .send()
                .await?;
            // 404 = never indexed; fine.
            anyhow::ensure!(
                response.status().is_success() || response.status().as_u16() == 404,
                "deleting msg doc failed: {}",
                response.status()
            );
        }
        let chunk = mappings::chunk_index_name(user_id);
        if self.index_exists(&chunk).await? {
            self.send_json(
                Method::POST,
                &format!("/{chunk}/_delete_by_query"),
                Some(&json!({"query": {"term": {"message_id": message_id.to_string()}}})),
            )
            .await?;
        }
        Ok(())
    }

    /// Make recent writes visible to search (tests and interactive use).
    pub async fn refresh_user_indices(&self, user_id: i64) -> Result<()> {
        for name in [
            mappings::msg_index_name(user_id),
            mappings::chunk_index_name(user_id),
        ] {
            self.send_json(Method::POST, &format!("/{name}/_refresh"), None)
                .await?;
        }
        Ok(())
    }

    fn parse_hits(value: &Value, id_field: &str) -> Vec<Hit> {
        value
            .pointer("/hits/hits")
            .and_then(Value::as_array)
            .map(|hits| {
                hits.iter()
                    .filter_map(|h| {
                        let source = h.get("_source")?.clone();
                        let message_id = source
                            .get(id_field)
                            .and_then(Value::as_str)
                            .map(String::from)
                            .or_else(|| h.get("_id").and_then(Value::as_str).map(String::from))?;
                        let highlight = h
                            .pointer("/highlight/body_text")
                            .and_then(Value::as_array)
                            .map(|frags| {
                                frags
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .collect::<Vec<_>>()
                                    .join(" … ")
                            })
                            .filter(|s| !s.is_empty());
                        Some(Hit {
                            message_id,
                            score: h.get("_score").and_then(Value::as_f64).unwrap_or(0.0) as f32,
                            source,
                            highlight,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn parse_total(value: &Value) -> i64 {
        value
            .pointer("/hits/total/value")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    }

    /// Plain-text fragment highlighting on body_text: no tags — agents
    /// consume the fragments as-is.
    fn highlight_block() -> Value {
        json!({
            "fields": {"body_text": {}},
            "fragment_size": 200,
            "number_of_fragments": 2,
            "pre_tags": [""],
            "post_tags": [""],
        })
    }

    /// BM25 over subject, body, and participants in the message index,
    /// optionally constrained by address/date filters. Returns hits with
    /// plain-text highlight fragments plus the exact total match count.
    pub async fn bm25_search(
        &self,
        user_id: i64,
        query: &str,
        size: usize,
        filter: &SearchFilter,
    ) -> Result<SearchPage> {
        let index = mappings::msg_index_name(user_id);
        let value = self
            .search(
                &index,
                &json!({
                    "size": size,
                    "track_total_hits": true,
                    "highlight": Self::highlight_block(),
                    "query": {
                        "bool": {
                            "must": {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["subject^2", "body_text", "from", "to", "cc"],
                                }
                            },
                            "filter": filter.clauses(".raw"),
                        }
                    }
                }),
            )
            .await?;
        Ok(SearchPage {
            hits: Self::parse_hits(&value, "message_id"),
            total: Self::parse_total(&value),
        })
    }

    /// Date-ordered listing over the message index: optional keyword
    /// query, address/date filters, and offset pagination. This is the
    /// non-hybrid path behind `sort: date_desc | date_asc` and behind
    /// query-less filter browsing.
    pub async fn list_messages(
        &self,
        user_id: i64,
        query: Option<&str>,
        filter: &SearchFilter,
        size: usize,
        offset: usize,
        ascending: bool,
    ) -> Result<SearchPage> {
        let index = mappings::msg_index_name(user_id);
        let must = match query {
            Some(q) if !q.trim().is_empty() => json!({
                "multi_match": {
                    "query": q,
                    "fields": ["subject^2", "body_text", "from", "to", "cc"],
                }
            }),
            _ => json!({"match_all": {}}),
        };
        let order = if ascending { "asc" } else { "desc" };
        let mut body = json!({
            "size": size,
            "from": offset,
            "track_total_hits": true,
            "sort": [
                {"received_at": {"order": order}},
                {"message_id": {"order": "asc"}},
            ],
            "query": {"bool": {"must": must, "filter": filter.clauses(".raw")}},
        });
        if query.is_some_and(|q| !q.trim().is_empty()) {
            body["highlight"] = Self::highlight_block();
        }
        let value = self.search(&index, &body).await?;
        Ok(SearchPage {
            hits: Self::parse_hits(&value, "message_id"),
            total: Self::parse_total(&value),
        })
    }

    /// Every message of one thread, oldest first. Returns the raw msg
    /// documents (same shape get_msg_doc yields).
    pub async fn thread_messages(
        &self,
        user_id: i64,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let index = mappings::msg_index_name(user_id);
        let value = self
            .search(
                &index,
                &json!({
                    "size": limit,
                    "sort": [
                        {"received_at": {"order": "asc"}},
                        {"message_id": {"order": "asc"}},
                    ],
                    "query": {"term": {"thread_id": thread_id}},
                }),
            )
            .await?;
        Ok(value
            .pointer("/hits/hits")
            .and_then(Value::as_array)
            .map(|hits| {
                hits.iter()
                    .filter_map(|h| h.get("_source").cloned())
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Fetch several msg documents by id in one round trip, keyed by id.
    /// Used to hydrate kNN-only hits with full message metadata.
    pub async fn mget_msg_docs(
        &self,
        user_id: i64,
        ids: &[&str],
    ) -> Result<std::collections::HashMap<String, Value>> {
        if ids.is_empty() {
            return Ok(Default::default());
        }
        let index = mappings::msg_index_name(user_id);
        let response = self
            .request(Method::POST, &format!("/{index}/_mget"))
            .json(&json!({"ids": ids}))
            .send()
            .await?;
        let status = response.status();
        if status.as_u16() == 404 {
            return Ok(Default::default());
        }
        let value: Value = response.json().await.unwrap_or(Value::Null);
        anyhow::ensure!(status.is_success(), "_mget returned {status}: {value}");
        Ok(value
            .get("docs")
            .and_then(Value::as_array)
            .map(|docs| {
                docs.iter()
                    .filter_map(|d| {
                        let id = d.get("_id").and_then(Value::as_str)?;
                        Some((id.to_string(), d.get("_source")?.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// kNN over chunk vectors; hits carry their chunk text for snippets.
    /// A non-empty filter is applied natively via the Lucene engine's
    /// `knn` `filter` clause (on the chunk index's plain-keyword address
    /// fields and its received_at date, pipeline v3), keeping the
    /// semantic half consistent with BM25.
    pub async fn knn_search(
        &self,
        user_id: i64,
        vector: &[f32],
        k: usize,
        filter: &SearchFilter,
    ) -> Result<Vec<Hit>> {
        let index = mappings::chunk_index_name(user_id);
        let mut embedding = json!({"vector": vector, "k": k});
        let clauses = filter.clauses("");
        if !clauses.is_empty() {
            embedding["filter"] = json!({"bool": {"filter": clauses}});
        }
        let value = self
            .search(
                &index,
                &json!({
                    "size": k,
                    "query": {"knn": {"embedding": embedding}},
                }),
            )
            .await?;
        Ok(Self::parse_hits(&value, "message_id"))
    }

    /// Facet aggregation over the message index (spec §9 list_facets):
    /// top senders, mailbox distribution, or per-year/month counts —
    /// optionally scoped to a keyword query and address/date filters
    /// ("top senders among messages matching X").
    pub async fn facets(
        &self,
        user_id: i64,
        kind: FacetKind,
        size: usize,
        query: Option<&str>,
        filter: &SearchFilter,
    ) -> Result<Vec<FacetBucket>> {
        let index = mappings::msg_index_name(user_id);
        let aggs = match kind {
            FacetKind::From => json!({"f": {"terms": {"field": "from.raw", "size": size}}}),
            FacetKind::Mailbox => json!({"f": {"terms": {"field": "mailbox_ids", "size": size}}}),
            FacetKind::Year => json!({"f": {
                "date_histogram": {
                    "field": "received_at",
                    "calendar_interval": "year",
                    "format": "yyyy",
                    "min_doc_count": 1,
                }
            }}),
            FacetKind::Month => json!({"f": {
                "date_histogram": {
                    "field": "received_at",
                    "calendar_interval": "month",
                    "format": "yyyy-MM",
                    "min_doc_count": 1,
                }
            }}),
        };
        let must = match query {
            Some(q) if !q.trim().is_empty() => json!({
                "multi_match": {
                    "query": q,
                    "fields": ["subject^2", "body_text", "from", "to", "cc"],
                }
            }),
            _ => json!({"match_all": {}}),
        };
        let value = self
            .search(
                &index,
                &json!({
                    "size": 0,
                    "query": {"bool": {"must": must, "filter": filter.clauses(".raw")}},
                    "aggs": aggs,
                }),
            )
            .await?;
        let buckets = value
            .pointer("/aggregations/f/buckets")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(buckets
            .iter()
            .filter_map(|b| {
                let key = b
                    .get("key_as_string")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .or_else(|| b.get("key").and_then(Value::as_str).map(String::from))?;
                Some(FacetBucket {
                    value: key,
                    count: b.get("doc_count").and_then(Value::as_i64).unwrap_or(0),
                })
            })
            .collect())
    }

    pub async fn get_msg_doc(&self, user_id: i64, message_id: i64) -> Result<Option<Value>> {
        let index = mappings::msg_index_name(user_id);
        let response = self
            .request(Method::GET, &format!("/{index}/_doc/{message_id}"))
            .send()
            .await?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        anyhow::ensure!(response.status().is_success(), "get doc failed");
        let value: Value = response.json().await?;
        Ok(value.get("_source").cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_yields_no_clauses() {
        let f = SearchFilter::default();
        assert!(f.is_empty());
        assert!(f.term_clauses(".raw").is_empty());
        assert!(f.term_clauses("").is_empty());
    }

    #[test]
    fn single_field_targets_raw_subfield_on_msg_index() {
        let f = SearchFilter {
            from: Some("alice@example.com".into()),
            ..Default::default()
        };
        assert!(!f.is_empty());
        assert_eq!(
            f.term_clauses(".raw"),
            vec![json!({"term": {"from.raw": "alice@example.com"}})]
        );
        // Chunk index uses the bare keyword field.
        assert_eq!(
            f.term_clauses(""),
            vec![json!({"term": {"from": "alice@example.com"}})]
        );
    }

    #[test]
    fn multiple_fields_are_and_combined_in_from_to_cc_order() {
        let f = SearchFilter {
            from: Some("a@x.com".into()),
            to: Some("b@x.com".into()),
            cc: Some("c@x.com".into()),
            ..Default::default()
        };
        assert_eq!(
            f.term_clauses(".raw"),
            vec![
                json!({"term": {"from.raw": "a@x.com"}}),
                json!({"term": {"to.raw": "b@x.com"}}),
                json!({"term": {"cc.raw": "c@x.com"}}),
            ]
        );
    }

    #[test]
    fn date_bounds_become_an_inclusive_range_clause() {
        let f = SearchFilter {
            received_after: Some("2019-01-01T00:00:00+00:00".into()),
            received_before: Some("2019-12-31T23:59:59.999Z".into()),
            ..Default::default()
        };
        assert!(!f.is_empty());
        assert_eq!(
            f.clauses(".raw"),
            vec![json!({"range": {"received_at": {
                "gte": "2019-01-01T00:00:00+00:00",
                "lte": "2019-12-31T23:59:59.999Z",
            }}})]
        );
        // Filter shape is identical on the chunk index.
        assert_eq!(f.clauses(""), f.clauses(".raw"));
    }

    #[test]
    fn terms_and_range_combine() {
        let f = SearchFilter {
            from: Some("a@x.com".into()),
            received_after: Some("2020-06-01T00:00:00Z".into()),
            ..Default::default()
        };
        let clauses = f.clauses(".raw");
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0], json!({"term": {"from.raw": "a@x.com"}}));
        assert_eq!(
            clauses[1],
            json!({"range": {"received_at": {"gte": "2020-06-01T00:00:00Z"}}})
        );
    }
}
