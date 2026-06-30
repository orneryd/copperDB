//! Text embedding generation for copperdb.
//!
//! Equivalent to Go's `pkg/embed` in NornicDB.
//! Supports:
//! - **Local GGUF models** via llama.cpp (Metal/CUDA GPU acceleration)
//! - **LRU caching** for repeated queries
//! - **Crash resilience** with panic recovery for FFI faults
//! - **Model warmup** to prevent GPU memory eviction

pub mod cached;
pub mod local_gguf;

pub use cached::CachedEmbedder;
pub use local_gguf::{EmbedStats, LocalGgufEmbedder};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EmbedError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("API error: {0}")]
    Api(String),
    #[error("local model error: {0}")]
    LocalModel(String),
    #[error("provider not configured: {0}")]
    NotConfigured(String),
}

/// Supported embedding providers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Provider {
    /// Local GGUF model via llama.cpp with Metal/CUDA.
    LocalGGUF { model_name: String, dimensions: usize },
    /// LRU-cached wrapper around any provider.
    Cached { inner: Box<Provider>, max_size: usize },
    /// Mock provider for testing.
    Mock { dimensions: usize },
}

/// A single text embedding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Embedding {
    pub text: String,
    pub vector: Vec<f32>,
    pub model: String,
}

/// Embedder trait — async + sync embedding.
#[async_trait::async_trait]
pub trait Embedder: Send + Sync {
    /// Generate embeddings asynchronously.
    async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError>;
    /// Generate embeddings synchronously (for blocking contexts and cache).
    fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError>;
    fn dimensions(&self) -> usize;
}

// ── Mock embedder ──────────────────────────────────────────────────────────────

pub struct MockEmbedder {
    dims: usize,
}

impl MockEmbedder {
    pub fn new(dims: usize) -> Self { Self { dims } }
}

#[async_trait::async_trait]
impl Embedder for MockEmbedder {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
        Ok(texts.iter().map(|t| Embedding {
            text: t.clone(),
            vector: vec![0.0; self.dims],
            model: "mock".into(),
        }).collect())
    }
    fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
        Ok(texts.iter().map(|t| Embedding {
            text: t.clone(),
            vector: vec![0.0; self.dims],
            model: "mock".into(),
        }).collect())
    }
    fn dimensions(&self) -> usize { self.dims }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_embedder() {
        let e = MockEmbedder::new(128);
        assert_eq!(e.dimensions(), 128);
    }
}

#[cfg(test)]
mod e2e_tests;
