//! Index one promoted message (spec §8): the caller supplies sanitized
//! body text; this module quote-strips (embedding path only), chunks,
//! embeds, and writes the message doc + chunk vectors.

use anyhow::Result;
use serde_json::json;

use crate::db::messages::Message;
use crate::embed::EmbeddingProvider;
use crate::extract::{ExtractedEmail, chunk::chunk_text, quotes::strip_quotes};
use crate::search::client::SearchClient;

/// Bumped whenever extraction/sanitization behavior changes, so a
/// reindex can tell stale documents from current ones. v2 denormalizes
/// from/to/cc into chunk docs for filtered kNN; v3 adds received_at to
/// chunk docs for date-filtered kNN.
pub const PIPELINE_VERSION: i32 = 3;

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
            "body_text": email.body_text,
            "has_attachments": msg.has_attachments,
            "sanitized": sanitized,
        });
        self.search.put_msg_doc(user_id, msg.id, &doc).await?;

        let stripped = strip_quotes(&email.body_text);
        let chunk_texts = chunk_text(&stripped);
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
