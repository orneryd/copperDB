//! Text embedding generation for copperdb.
//!
//! Equivalent to Go's `pkg/embed` in NornicDB.
//! Supports:
//! - **OpenAI API**: `text-embedding-3-small` / `text-embedding-3-large`
//! - **Local GGUF models**: via llama.cpp FFI (`libloading`)
//!
//! NornicDB uses `github.com/hybridgroup/yzma` (WASM) for local models.
//! This crate uses `libloading` to call a native llama.cpp shared library instead.
//! See `copperdb-localllm` for the llama.cpp wrapper.

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
    /// OpenAI embeddings API.
    OpenAI { api_key: String, model: String },
    /// Local GGUF model via llama.cpp.
    LocalGGUF { model_path: String },
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

/// Embedder trait — asynchronously embeds a batch of texts.
#[async_trait::async_trait]
pub trait Embedder: Send + Sync {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError>;
    fn dimensions(&self) -> usize;
}

/// Mock embedder for testing (returns random unit vectors).
pub struct MockEmbedder {
    dims: usize,
}

impl MockEmbedder {
    pub fn new(dims: usize) -> Self {
        Self { dims }
    }
}

#[async_trait::async_trait]
impl Embedder for MockEmbedder {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
        use std::hash::{Hash, Hasher};
        texts.iter().map(|text| {
            // Deterministic pseudo-random vector from text hash
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            text.hash(&mut hasher);
            let seed = hasher.finish();
            let vector: Vec<f32> = (0..self.dims)
                .map(|i| {
                    let v = ((seed.wrapping_add(i as u64).wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407)) >> 33) as f32
                        / u32::MAX as f32
                        * 2.0
                        - 1.0;
                    v
                })
                .collect();
            let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
            let vector = if norm > 0.0 {
                vector.iter().map(|x| x / norm).collect()
            } else {
                vector
            };
            Ok(Embedding {
                text: text.clone(),
                vector,
                model: "mock".into(),
            })
        }).collect()
    }

    fn dimensions(&self) -> usize {
        self.dims
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_embedder() {
        let embedder = MockEmbedder::new(128);
        let texts = vec!["hello world".to_string(), "foo bar".to_string()];
        let embeddings = embedder.embed(&texts).await.unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].vector.len(), 128);
    }

    #[tokio::test]
    async fn test_mock_embedder_deterministic() {
        let embedder = MockEmbedder::new(64);
        let texts = vec!["test".to_string()];
        let e1 = embedder.embed(&texts).await.unwrap();
        let e2 = embedder.embed(&texts).await.unwrap();
        assert_eq!(e1[0].vector, e2[0].vector);
    }
}
