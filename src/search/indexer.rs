//! Index one promoted message (spec §8): the caller supplies sanitized
//! body text; this module quote-strips (embedding path only), chunks,
//! embeds, and writes the message doc + chunk vectors.

use anyhow::Result;
use serde_json::json;

use crate::db::messages::Message;
use crate::embed::{EmbeddingProvider, truncate_chars};
use crate::extract::{ExtractedEmail, chunk, chunk::chunk_text, quotes::strip_quotes};
use crate::search::client::SearchClient;

/// Bumped whenever extraction/sanitization behavior changes, so a
/// reindex can tell stale documents from current ones. v2 denormalizes
/// from/to/cc into chunk docs for filtered kNN; v3 adds received_at to
/// chunk docs for date-filtered kNN; v4 caps indexed/embedded text for
/// oversized messages.
pub const PIPELINE_VERSION: i32 = 4;

/// BM25 sees at most this much body text (chars). The raw message in the
/// Maildir is always complete; this only bounds the search document — a
/// 70 MB body would otherwise land verbatim in the msg doc.
pub const MAX_BODY_INDEX_CHARS: usize = 1_000_000;

/// Embedding budget per message. Unbounded chunking turns one giant
/// message into thousands of sequential embed calls (hours of wall
/// clock) that monopolize the promote pipeline.
pub const MAX_EMBED_CHUNKS: usize = 256;

pub struct Indexer<'a> {
    pub search: &'a SearchClient,
    pub embedder: &'a dyn EmbeddingProvider,
}

#[derive(Debug, Default)]
pub struct IndexOutcome {
    pub chunks: usize,
}

impl Indexer<'_> {
    /// Write `msg` into the user's indices. `email` carries the already
    /// sanitized body text; `body_for_index` is what BM25 sees (full,
    /// un-quote-stripped), while embeddings see the quote-stripped form.
    pub async fn index_message(
        &self,
        user_id: i64,
        msg: &Message,
        email: &ExtractedEmail,
        sanitized: bool,
    ) -> Result<IndexOutcome> {
        // Truncate before quote-strip/chunk as well: the chunker
        // materializes per-paragraph char vecs, so feeding it 70 MB is
        // itself a resource problem.
        let body = truncate_chars(&email.body_text, MAX_BODY_INDEX_CHARS);
        if body.len() < email.body_text.len() {
            tracing::warn!(
                message = msg.id,
                chars = email.body_text.chars().count(),
                "indexing truncated oversized message"
            );
        }
        let doc = json!({
            "message_id": msg.id.to_string(),
            "thread_id": msg.thread_id,
            "mailbox_ids": msg.mailbox_ids,
            "from": email.from,
            "to": email.to,
            "cc": email.cc,
            "subject": email.subject.as_deref().or(msg.subject.as_deref()),
            "received_at": msg.received_at.to_rfc3339(),
            "size": msg.size,
            "body_text": body,
            "has_attachments": msg.has_attachments,
            "sanitized": sanitized,
        });
        self.search.put_msg_doc(user_id, msg.id, &doc).await?;

        let stripped = strip_quotes(body);
        let embed_budget = truncate_chars(&stripped, MAX_EMBED_CHUNKS * chunk::TARGET_CHARS);
        let mut chunk_texts = chunk_text(embed_budget);
        // Overlap prepending can push the count past the budget slightly.
        chunk_texts.truncate(MAX_EMBED_CHUNKS);
        if chunk_texts.is_empty() {
            return Ok(IndexOutcome { chunks: 0 });
        }
        let vectors = self.embedder.embed_documents(&chunk_texts).await?;
        let chunks: Vec<(usize, String, Vec<f32>)> = chunk_texts
            .into_iter()
            .zip(vectors)
            .enumerate()
            .map(|(i, (text, vector))| (i, text, vector))
            .collect();
        let count = chunks.len();
        self.search
            .bulk_chunks(
                user_id,
                msg.id,
                &chunks,
                &email.from,
                &email.to,
                &email.cc,
                &msg.received_at.to_rfc3339(),
            )
            .await?;
        Ok(IndexOutcome { chunks: count })
    }
}
