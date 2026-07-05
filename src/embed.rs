//! Embedding provider. Two nomic-embed-text gotchas are encoded here so
//! call sites can't forget them: the task prefixes (search_document /
//! search_query) that the model requires for retrieval quality, and an
//! explicit num_ctx (Ollama's 2048 default silently truncates).

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::config::EmbeddingConfig;

#[async_trait::async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed_documents(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
    async fn embed_query(&self, text: &str) -> Result<Vec<f32>>;
    fn dimension(&self) -> usize;
}

/// Batch size per /api/embed call (plan: 32-64 keeps Ollama efficient
/// without huge request bodies).
const EMBED_BATCH: usize = 32;

pub struct OllamaEmbedder {
    http: reqwest::Client,
    url: String,
    model: String,
    dimension: usize,
    num_ctx: usize,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

impl OllamaEmbedder {
    pub fn new(config: &EmbeddingConfig) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()?,
            url: config.url.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            dimension: config.dimension,
            num_ctx: config.num_ctx,
        })
    }

    async fn embed_batch(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        let response = self
            .http
            .post(format!("{}/api/embed", self.url))
            .json(&serde_json::json!({
                "model": self.model,
                "input": inputs,
                "options": {"num_ctx": self.num_ctx},
            }))
            .send()
            .await?
            .error_for_status()
            .context("embedding request failed")?;
        let parsed: EmbedResponse = response.json().await?;
        anyhow::ensure!(
            parsed.embeddings.len() == inputs.len(),
            "embedding count mismatch: sent {}, got {}",
            inputs.len(),
            parsed.embeddings.len()
        );
        for e in &parsed.embeddings {
            anyhow::ensure!(
                e.len() == self.dimension,
                "embedding dimension {} does not match configured {}",
                e.len(),
                self.dimension
            );
        }
        Ok(parsed.embeddings)
    }
}

#[async_trait::async_trait]
impl EmbeddingProvider for OllamaEmbedder {
    async fn embed_documents(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut all = Vec::with_capacity(texts.len());
        for batch in texts.chunks(EMBED_BATCH) {
            let prefixed: Vec<String> = batch
                .iter()
                .map(|t| format!("search_document: {t}"))
                .collect();
            all.extend(self.embed_batch(&prefixed).await?);
        }
        Ok(all)
    }

    async fn embed_query(&self, text: &str) -> Result<Vec<f32>> {
        let embeddings = self.embed_batch(&[format!("search_query: {text}")]).await?;
        embeddings
            .into_iter()
            .next()
            .context("empty embedding response")
    }

    fn dimension(&self) -> usize {
        self.dimension
    }
}

/// Deterministic fake for tests: the vector is derived from the text's
/// hash, so identical text ⇒ identical vector (exact-match kNN works)
/// without any model dependency. Lives in the lib so integration tests
/// and future fixtures can share it.
pub struct FakeEmbedder {
    pub dimension: usize,
}

impl Default for FakeEmbedder {
    fn default() -> Self {
        Self { dimension: 768 }
    }
}

fn hash_vector(text: &str, dimension: usize) -> Vec<f32> {
    use sha2::{Digest, Sha256};
    let mut out = Vec::with_capacity(dimension);
    let mut counter = 0u32;
    while out.len() < dimension {
        let digest = Sha256::digest(format!("{counter}:{text}").as_bytes());
        for pair in digest.chunks(2) {
            if out.len() >= dimension {
                break;
            }
            let v = u16::from_le_bytes([pair[0], pair[1]]) as f32 / u16::MAX as f32;
            out.push(v * 2.0 - 1.0);
        }
        counter += 1;
    }
    // Normalize for cosine similarity.
    let norm: f32 = out.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut out {
            *v /= norm;
        }
    }
    out
}

#[async_trait::async_trait]
impl EmbeddingProvider for FakeEmbedder {
    async fn embed_documents(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|t| hash_vector(t, self.dimension))
            .collect())
    }

    async fn embed_query(&self, text: &str) -> Result<Vec<f32>> {
        Ok(hash_vector(text, self.dimension))
    }

    fn dimension(&self) -> usize {
        self.dimension
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_embedder_is_deterministic_and_normalized() {
        let fake = FakeEmbedder { dimension: 16 };
        let a = fake.embed_query("hello").await.unwrap();
        let b = fake.embed_query("hello").await.unwrap();
        let c = fake.embed_query("different").await.unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        let norm: f32 = a.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4);
    }
}
