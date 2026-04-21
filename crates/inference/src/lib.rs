//! ML model inference pipeline for copperdb.
//!
//! Equivalent to Go's `pkg/inference` in NornicDB.
//! Orchestrates the embedding + inference pipeline:
//! 1. Chunk text (copperdb-textchunk)
//! 2. Embed chunks (copperdb-embed)
//! 3. Run optional classification or generation (copperdb-localllm)

use thiserror::Error;

#[derive(Debug, Error)]
pub enum InferenceError {
    #[error("embed error: {0}")]
    EmbedError(String),
    #[error("model error: {0}")]
    ModelError(String),
    #[error("pipeline not configured")]
    NotConfigured,
    #[error("unsupported model type")]
    UnsupportedModelType,
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Supported model backends.
#[derive(Debug, Clone)]
pub enum ModelType {
    /// ONNX Runtime inference.
    Onnx,
    /// GGUF/llama.cpp style local model.
    Gguf,
    /// OpenAI API (remote).
    OpenAI,
    /// Custom model backend identified by name.
    Custom(String),
}

/// Configuration for an inference pipeline.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub model_type: ModelType,
    pub model_path: String,
    pub batch_size: usize,
}

impl InferenceConfig {
    pub fn new(model_type: ModelType, model_path: impl Into<String>) -> Self {
        Self {
            model_type,
            model_path: model_path.into(),
            batch_size: 32,
        }
    }
}

/// Result of an inference pipeline run.
#[derive(Debug, Clone)]
pub struct InferenceResult {
    pub text: String,
    pub embeddings: Vec<Vec<f32>>,
    pub labels: Vec<String>,
    pub confidence: Vec<f32>,
}

/// An inference pipeline that wraps a model backend.
pub struct InferencePipeline {
    config: InferenceConfig,
}

impl InferencePipeline {
    pub fn new(config: InferenceConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &InferenceConfig {
        &self.config
    }

    /// Run numeric inference — returns a passthrough/identity transform as a stub.
    /// Real implementations would load the model and run forward pass.
    pub fn predict(&self, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        if input.is_empty() {
            return Err(InferenceError::InvalidInput("input must not be empty".into()));
        }
        match &self.config.model_type {
            ModelType::OpenAI => Err(InferenceError::UnsupportedModelType),
            _ => {
                // Stub: return normalized input vector
                let magnitude: f32 = input.iter().map(|x| x * x).sum::<f32>().sqrt();
                if magnitude == 0.0 {
                    return Ok(vec![0.0; input.len()]);
                }
                Ok(input.iter().map(|x| x / magnitude).collect())
            }
        }
    }

    /// Run text-to-text inference — returns a stub response.
    pub fn predict_text(&self, text: &str) -> Result<String, InferenceError> {
        if text.is_empty() {
            return Err(InferenceError::InvalidInput("text must not be empty".into()));
        }
        match &self.config.model_type {
            ModelType::OpenAI => Err(InferenceError::UnsupportedModelType),
            ModelType::Custom(name) => Ok(format!("[{name}]: {text}")),
            _ => Ok(format!("[inference:{}]: {text}", self.config.model_path)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inference_config() {
        let cfg = InferenceConfig::new(ModelType::Onnx, "/models/embedding.onnx");
        assert_eq!(cfg.batch_size, 32);
        assert_eq!(cfg.model_path, "/models/embedding.onnx");
    }

    #[test]
    fn test_predict_normalizes() {
        let pipeline = InferencePipeline::new(
            InferenceConfig::new(ModelType::Gguf, "/models/llm.gguf")
        );
        let result = pipeline.predict(&[3.0, 4.0]).unwrap();
        assert_eq!(result.len(), 2);
        let magnitude: f32 = result.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_predict_empty_fails() {
        let pipeline = InferencePipeline::new(InferenceConfig::new(ModelType::Onnx, "m"));
        assert!(pipeline.predict(&[]).is_err());
    }

    #[test]
    fn test_predict_text() {
        let pipeline = InferencePipeline::new(
            InferenceConfig::new(ModelType::Gguf, "/models/llm.gguf")
        );
        let result = pipeline.predict_text("hello world").unwrap();
        assert!(result.contains("hello world"));
    }

    #[test]
    fn test_predict_text_empty_fails() {
        let pipeline = InferencePipeline::new(InferenceConfig::new(ModelType::Onnx, "m"));
        assert!(pipeline.predict_text("").is_err());
    }

    #[test]
    fn test_openai_unsupported() {
        let pipeline = InferencePipeline::new(InferenceConfig::new(ModelType::OpenAI, "gpt-4"));
        assert!(pipeline.predict(&[1.0]).is_err());
        assert!(pipeline.predict_text("test").is_err());
    }

    #[test]
    fn test_custom_model_type() {
        let pipeline = InferencePipeline::new(
            InferenceConfig::new(ModelType::Custom("my-model".into()), "path")
        );
        let result = pipeline.predict_text("hi").unwrap();
        assert!(result.contains("my-model"));
    }
}
