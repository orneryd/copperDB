//! Local LLM inference via llama.cpp FFI.
//!
//! Equivalent to Go's `pkg/embed/local_gguf.go` in NornicDB.
//! NornicDB uses `github.com/hybridgroup/yzma` (WASM runtime) to run GGUF
//! models. This crate instead uses `libloading` to call a native llama.cpp
//! shared library at runtime (no WASM overhead).
//!
//! ⚠️ **Requires**: `libllama.so` (Linux), `libllama.dylib` (macOS), or
//! `llama.dll` (Windows) built from https://github.com/ggerganov/llama.cpp
//!
//! See README.md for build instructions.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocalLlmError {
    #[error("model not loaded")]
    NotLoaded,
    #[error("library load error: {0}")]
    LibraryLoad(String),
    #[error("inference error: {0}")]
    InferenceError(String),
    #[error("model file not found: {0}")]
    ModelNotFound(String),
}

/// Configuration for a local GGUF model.
#[derive(Debug, Clone)]
pub struct GgufConfig {
    /// Path to the .gguf model file.
    pub model_path: String,
    /// Number of context tokens.
    pub context_size: usize,
    /// Number of GPU layers to offload (0 = CPU only).
    pub gpu_layers: i32,
    /// Number of threads.
    pub threads: i32,
}

impl Default for GgufConfig {
    fn default() -> Self {
        Self {
            model_path: String::new(),
            context_size: 2048,
            gpu_layers: 0,
            threads: 4,
        }
    }
}

/// Local GGUF model handle (FFI wrapper over llama.cpp).
///
/// ⚠️ **Not yet implemented.** The FFI bindings to llama.cpp need to be
/// completed. Consider using the `llama_cpp` crate from crates.io once
/// it stabilizes, or generate bindings with `bindgen`.
pub struct LocalModel {
    config: GgufConfig,
    // _lib: Option<libloading::Library>,
    // _ctx: *mut llama_context,
}

impl LocalModel {
    /// Load a GGUF model from disk.
    pub fn load(config: GgufConfig) -> Result<Self, LocalLlmError> {
        if !std::path::Path::new(&config.model_path).exists() {
            return Err(LocalLlmError::ModelNotFound(config.model_path.clone()));
        }
        // TODO: Load libllama via libloading and initialize the model.
        Ok(Self { config })
    }

    pub fn config(&self) -> &GgufConfig {
        &self.config
    }

    pub fn model_path(&self) -> &str {
        &self.config.model_path
    }

    /// Generate an embedding vector for the given text.
    pub fn embed(&self, _text: &str) -> Result<Vec<f32>, LocalLlmError> {
        Err(LocalLlmError::InferenceError(
            format!(
                "llama.cpp FFI not yet implemented for model {}. Build libllama and complete FFI bindings.",
                self.config.model_path
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_missing_model() {
        let config = GgufConfig {
            model_path: "/nonexistent/model.gguf".into(),
            ..Default::default()
        };
        assert!(matches!(
            LocalModel::load(config),
            Err(LocalLlmError::ModelNotFound(_))
        ));
    }
}
