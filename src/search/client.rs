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
}

#[derive(Debug, Clone, Copy)]
pub enum FacetKind {
    From,
    Mailbox,
    Year,
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
    pub async fn bulk_chunks(
        &self,
        user_id: i64,
        message_id: i64,
        chunks: &[(usize, String, Vec<f32>)],
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
                        Some(Hit {
                            message_id,
                            score: h.get("_score").and_then(Value::as_f64).unwrap_or(0.0) as f32,
                            source,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// BM25 over subject and body in the message index.
    pub async fn bm25_search(&self, user_id: i64, query: &str, size: usize) -> Result<Vec<Hit>> {
        let index = mappings::msg_index_name(user_id);
        let value = self
            .send_json(
                Method::POST,
                &format!("/{index}/_search"),
                Some(&json!({
                    "size": size,
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": ["subject^2", "body_text", "from", "to"],
                        }
                    }
                })),
            )
            .await?;
        Ok(Self::parse_hits(&value, "message_id"))
    }

    /// kNN over chunk vectors; hits carry their chunk text for snippets.
    pub async fn knn_search(&self, user_id: i64, vector: &[f32], k: usize) -> Result<Vec<Hit>> {
        let index = mappings::chunk_index_name(user_id);
        let value = self
            .send_json(
                Method::POST,
                &format!("/{index}/_search"),
                Some(&json!({
                    "size": k,
                    "query": {"knn": {"embedding": {"vector": vector, "k": k}}},
                })),
            )
            .await?;
        Ok(Self::parse_hits(&value, "message_id"))
    }

    /// Facet aggregation over the message index (spec §9 list_facets):
    /// top senders, mailbox distribution, or per-year message counts.
    pub async fn facets(
        &self,
        user_id: i64,
        kind: FacetKind,
        size: usize,
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
        };
        let value = self
            .send_json(
                Method::POST,
                &format!("/{index}/_search"),
                Some(&json!({"size": 0, "aggs": aggs})),
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
