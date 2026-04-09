//! ML model inference pipeline for magnetDB.
//!
//! Equivalent to Go's `pkg/inference` in NornicDB.
//! Orchestrates the embedding + inference pipeline:
//! 1. Chunk text (magnetdb-textchunk)
//! 2. Embed chunks (magnetdb-embed)
//! 3. Run optional classification or generation (magnetdb-localllm)

use thiserror::Error;

#[derive(Debug, Error)]
pub enum InferenceError {
    #[error("embed error: {0}")]
    EmbedError(String),
    #[error("model error: {0}")]
    ModelError(String),
    #[error("pipeline not configured")]
    NotConfigured,
}

/// Result of an inference pipeline run.
#[derive(Debug, Clone)]
pub struct InferenceResult {
    pub text: String,
    pub embeddings: Vec<Vec<f32>>,
    pub labels: Vec<String>,
    pub confidence: Vec<f32>,
}

// TODO: Implement inference pipeline wiring magnetdb-embed + magnetdb-localllm.
