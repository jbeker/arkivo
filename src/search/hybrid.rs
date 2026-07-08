//! Hybrid retrieval (spec §6.2/§9): BM25 over the message index and kNN
//! over the chunk index run in parallel from here, then fuse app-side
//! with reciprocal rank fusion. (OpenSearch search pipelines only fuse
//! sub-queries within ONE index; our two-index layout rules them out —
//! deliberate deviation recorded in the plan.)

use anyhow::Result;
use futures::try_join;
use serde_json::Value;
use std::collections::HashMap;

use crate::embed::EmbeddingProvider;
use crate::search::client::{Hit, SearchClient, SearchFilter};

/// Standard RRF constant: softens the head of each ranking.
pub const RRF_K: f32 = 60.0;

#[derive(Debug, Clone)]
pub struct RankedResult {
    pub message_id: String,
    pub score: f32,
    /// Best-ranked chunk text (semantic snippet), if kNN surfaced one.
    pub chunk_snippet: Option<String>,
    /// Query-term highlight fragments, if BM25 surfaced the message.
    pub highlight: Option<String>,
    /// Message-index source document; hybrid_search hydrates kNN-only
    /// hits so this is present on every returned result.
    pub msg_source: Option<Value>,
}

/// One page of fused results plus pagination metadata.
#[derive(Debug, Clone)]
pub struct HybridResults {
    pub results: Vec<RankedResult>,
    /// Total keyword-side matches: exact for BM25, a floor for the fused
    /// ranking (semantic-only hits aren't counted by the index).
    pub total: i64,
    pub has_more: bool,
}

/// Reciprocal rank fusion over two ranked lists keyed by message_id.
/// Pure function: trivially testable, deterministic. kNN hits are
/// collapsed to their best-ranked chunk per message first.
pub fn rrf_fuse(bm25: &[Hit], knn: &[Hit], limit: usize) -> Vec<RankedResult> {
    let mut results: HashMap<String, RankedResult> = HashMap::new();

    for (rank, hit) in bm25.iter().enumerate() {
        let entry = results
            .entry(hit.message_id.clone())
            .or_insert_with(|| RankedResult {
                message_id: hit.message_id.clone(),
                score: 0.0,
                chunk_snippet: None,
                highlight: None,
                msg_source: None,
            });
        entry.score += 1.0 / (RRF_K + rank as f32 + 1.0);
        entry.highlight = hit.highlight.clone();
        entry.msg_source = Some(hit.source.clone());
    }

    // Collapse chunk hits to best-per-message (first occurrence = best
    // rank), then contribute that rank's reciprocal.
    let mut seen_chunk_msg: HashMap<&str, usize> = HashMap::new();
    for (rank, hit) in knn.iter().enumerate() {
        if seen_chunk_msg.contains_key(hit.message_id.as_str()) {
            continue;
        }
        seen_chunk_msg.insert(&hit.message_id, rank);
        let entry = results
            .entry(hit.message_id.clone())
            .or_insert_with(|| RankedResult {
                message_id: hit.message_id.clone(),
                score: 0.0,
                chunk_snippet: None,
                highlight: None,
                msg_source: None,
            });
        entry.score += 1.0 / (RRF_K + rank as f32 + 1.0);
        if entry.chunk_snippet.is_none() {
            entry.chunk_snippet = hit
                .source
                .get("chunk_text")
                .and_then(Value::as_str)
                .map(String::from);
        }
    }

    let mut ranked: Vec<RankedResult> = results.into_values().collect();
    ranked.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.message_id.cmp(&b.message_id))
    });
    ranked.truncate(limit);
    ranked
}

/// Run both retrievals in parallel, fuse, page, and hydrate. `offset`
/// skips fused results for pagination; fetch depth scales with the page
/// end so later pages stay stable-ish.
pub async fn hybrid_search(
    search: &SearchClient,
    embedder: &dyn EmbeddingProvider,
    user_id: i64,
    query: &str,
    limit: usize,
    offset: usize,
    filter: &SearchFilter,
) -> Result<HybridResults> {
    let depth = ((offset + limit) * 4).max(20);
    let vector = embedder.embed_query(query).await?;
    let (bm25, knn) = try_join!(
        search.bm25_search(user_id, query, depth, filter),
        search.knn_search(user_id, &vector, depth, filter),
    )?;
    // Fuse one past the page end: an exact has-more signal for the page
    // without trusting the (BM25-only) total.
    let mut fused = rrf_fuse(&bm25.hits, &knn, offset + limit + 1);
    let has_more = fused.len() > offset + limit;
    fused.truncate(offset + limit);
    let mut results: Vec<RankedResult> = fused.into_iter().skip(offset).collect();

    // Hydrate kNN-only hits so every result carries full msg metadata.
    let missing: Vec<&str> = results
        .iter()
        .filter(|r| r.msg_source.is_none())
        .map(|r| r.message_id.as_str())
        .collect();
    if !missing.is_empty() {
        let docs = search.mget_msg_docs(user_id, &missing).await?;
        for result in &mut results {
            if result.msg_source.is_none() {
                result.msg_source = docs.get(&result.message_id).cloned();
            }
        }
    }

    Ok(HybridResults {
        results,
        total: bm25.total,
        has_more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hit(message_id: &str, source: Value) -> Hit {
        Hit {
            message_id: message_id.into(),
            score: 1.0,
            source,
            highlight: None,
        }
    }

    #[test]
    fn doc_present_in_both_lists_ranks_first() {
        let bm25 = vec![hit("a", json!({})), hit("b", json!({}))];
        let knn = vec![
            hit("c", json!({"chunk_text": "c1"})),
            hit("b", json!({"chunk_text": "b1"})),
        ];
        let fused = rrf_fuse(&bm25, &knn, 10);
        assert_eq!(fused[0].message_id, "b", "b appears in both lists");
        assert_eq!(fused.len(), 3);
    }

    #[test]
    fn chunk_hits_collapse_to_best_per_message() {
        let knn = vec![
            hit("a", json!({"chunk_text": "best chunk"})),
            hit("a", json!({"chunk_text": "worse chunk"})),
            hit("b", json!({"chunk_text": "b chunk"})),
        ];
        let fused = rrf_fuse(&[], &knn, 10);
        assert_eq!(fused.len(), 2);
        let a = fused.iter().find(|r| r.message_id == "a").unwrap();
        assert_eq!(a.chunk_snippet.as_deref(), Some("best chunk"));
        // a's second chunk must not double-count: b at rank 2 vs a at rank 0.
        let b = fused.iter().find(|r| r.message_id == "b").unwrap();
        assert!(a.score > b.score);
        assert!((a.score - 1.0 / (RRF_K + 1.0)).abs() < 1e-6);
    }

    #[test]
    fn limit_truncates_but_orders_first() {
        let bm25: Vec<Hit> = (0..10).map(|i| hit(&format!("m{i}"), json!({}))).collect();
        let fused = rrf_fuse(&bm25, &[], 3);
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].message_id, "m0");
    }

    #[test]
    fn deterministic_tie_break_by_message_id() {
        let bm25 = vec![hit("z", json!({}))];
        let knn = vec![hit("a", json!({}))];
        let fused = rrf_fuse(&bm25, &knn, 10);
        // Equal scores: lexicographic id order for determinism.
        assert_eq!(fused[0].message_id, "a");
        assert_eq!(fused[1].message_id, "z");
    }
}
