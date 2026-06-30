//! Local GGUF model inference via llama.cpp.
//! Uses `llama-cpp-2` (v0.1.150) — matches NornicDB's `llama.go`.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocalLlmError {
    #[error("model file not found: {0}")]
    ModelNotFound(PathBuf),
    #[error("model load error: {0}")]
    ModelLoad(String),
    #[error("embedding error: {0}")]
    EmbeddingError(String),
}

#[derive(Debug, Clone)]
pub struct GgufConfig {
    pub model_path: PathBuf,
    pub context_size: u32,
    pub batch_size: u32,
    pub threads: i32,
    pub gpu_layers: i32,
}

impl GgufConfig {
    pub fn with_model(model_path: impl Into<PathBuf>) -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get() as i32 / 2).unwrap_or(4).clamp(4, 8);
        Self { model_path: model_path.into(), context_size: 0, batch_size: 0, threads, gpu_layers: -1 }
    }
}

// ── Model ────────────────────────────────────────────────────────────────────

pub struct LocalModel {
    config: GgufConfig,
    backend: llama_cpp_2::llama_backend::LlamaBackend,
    model: Arc<llama_cpp_2::model::LlamaModel>,
    dimensions: usize,
    context_size: u32,
    batch_size: u32,
    threads: i32,
}

unsafe impl Send for LocalModel {}
unsafe impl Sync for LocalModel {}

impl LocalModel {
    pub fn load(config: GgufConfig) -> Result<Self, LocalLlmError> {
        // Check path before model params consume the config
        if !config.model_path.exists() {
            return Err(LocalLlmError::ModelNotFound(config.model_path.clone()));
        }

        let path_clone = config.model_path.clone(); // for use after config moves
        let backend = llama_cpp_2::llama_backend::LlamaBackend::init()
            .map_err(|e| LocalLlmError::ModelLoad(format!("backend: {e}")))?;

        let path_str = path_clone.to_string_lossy();
        tracing::info!(path = %path_str, "loading llama model");

        let gpu_layers = if config.gpu_layers < 0 { u32::MAX } else { config.gpu_layers as u32 };

        // Try GPU-first; fall back to CPU if loading fails.
        // Matches NornicDB's implicit fallback: llama.cpp silently uses CPU
        // when no GPU backend is available. We make this explicit and logged.
        let mut model_params = llama_cpp_2::model::params::LlamaModelParams::default()
            .with_n_gpu_layers(gpu_layers)
            .with_use_mmap(true)
            .with_use_mlock(false);

        let model = match llama_cpp_2::model::LlamaModel::load_from_file(
            &backend, &path_clone, &model_params,
        ) {
            Ok(m) => m,
            Err(e) if gpu_layers > 0 => {
                // GPU load failed — retry with CPU only
                tracing::warn!(
                    error = %e,
                    "GPU model load failed, falling back to CPU-only"
                );
                model_params = llama_cpp_2::model::params::LlamaModelParams::default()
                    .with_n_gpu_layers(0)
                    .with_use_mmap(true)
                    .with_use_mlock(false);
                llama_cpp_2::model::LlamaModel::load_from_file(
                    &backend, &path_clone, &model_params,
                ).map_err(|e| LocalLlmError::ModelLoad(format!(
                    "GPU load failed, CPU fallback also failed: {e}"
                )))?
            }
            Err(e) => return Err(LocalLlmError::ModelLoad(format!("{e}"))),
        };

        let model_ctx_train = model.n_ctx_train();
        let cap: u32 = 2048;
        let context_size = if config.context_size > 0 { config.context_size }
            else if model_ctx_train > 0 { model_ctx_train.min(cap).max(1) }
            else { cap };
        let batch_size = if config.batch_size > 0 { config.batch_size }
            else { context_size }.max(1).min(context_size);
        let dimensions = model.n_embd() as usize;

        let actual_gpu_layers = model_params.n_gpu_layers();
        let backend_label = if actual_gpu_layers > 0 { "gpu" } else { "cpu" };
        tracing::info!(dimensions, context_size, batch_size, threads = config.threads,
            gpu_layers = actual_gpu_layers, backend = backend_label, "llama model loaded");

        let dims = dimensions;
        let ctx = context_size;
        let bat = batch_size;
        let thr = config.threads;
        Ok(Self { config, backend, model: Arc::new(model), dimensions: dims, context_size: ctx, batch_size: bat, threads: thr })
    }

    pub fn dimensions(&self) -> usize { self.dimensions }
    pub fn config(&self) -> &GgufConfig { &self.config }
    pub fn model_path(&self) -> &Path { &self.config.model_path }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>, LocalLlmError> {
        if text.is_empty() { return Ok(Vec::new()); }

        let n_ctx = NonZeroU32::new(self.context_size).unwrap_or(NonZeroU32::new(1).unwrap());

        let ctx_params = llama_cpp_2::context::params::LlamaContextParams::default()
            .with_n_ctx(Some(n_ctx))
            .with_n_batch(self.batch_size)
            .with_n_threads(self.threads)
            .with_n_threads_batch(self.threads)
            .with_embeddings(true)
            .with_pooling_type(llama_cpp_2::context::params::LlamaPoolingType::Mean);

        let mut context = self.model.new_context(&self.backend, ctx_params)
            .map_err(|e| LocalLlmError::ModelLoad(format!("context: {e}")))?;

        // Tokenize text
        let tokens = self.model.str_to_token(text, llama_cpp_2::model::AddBos::Always)
            .map_err(|e| LocalLlmError::EmbeddingError(format!("tokenize: {e}")))?;
        let tokens: Vec<_> = tokens.into_iter().take(self.context_size as usize).collect();
        if tokens.is_empty() { return Err(LocalLlmError::EmbeddingError("no tokens".into())); }

        // Build batch
        let n_tokens = tokens.len();
        let mut batch = llama_cpp_2::llama_batch::LlamaBatch::new(n_tokens, 1);
        for (pos, &token) in tokens.iter().enumerate() {
            batch.add(token, pos as i32, &[0], pos == n_tokens - 1)
                .map_err(|e| LocalLlmError::EmbeddingError(format!("batch: {e}")))?;
        }

        // Encode + extract
        context.clear_kv_cache();
        context.encode(&mut batch)
            .map_err(|e| LocalLlmError::EmbeddingError(format!("encode: {e}")))?;

        let embd = context.embeddings_ith(0)
            .map_err(|e| LocalLlmError::EmbeddingError(format!("embeddings: {e}")))?;

        let n = self.dimensions.min(embd.len());
        if n == 0 { return Err(LocalLlmError::EmbeddingError("empty embedding".into())); }
        let mut vec = embd[..n].to_vec();
        l2_normalize(&mut vec);
        Ok(vec)
    }

    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, LocalLlmError> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

pub fn l2_normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 { for x in v.iter_mut() { *x /= norm; } }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_config() { let c = GgufConfig::with_model("/tmp/x.gguf"); assert_eq!(c.gpu_layers, -1); }
    #[test]
    fn test_missing() {
        assert!(matches!(LocalModel::load(GgufConfig::with_model("/nope/x.gguf")), Err(LocalLlmError::ModelNotFound(_))));
    }
    #[test]
    fn test_normalize() { let mut v = vec![3.0, 4.0]; l2_normalize(&mut v); assert!((v[0] - 0.6).abs() < 1e-6); }
}
