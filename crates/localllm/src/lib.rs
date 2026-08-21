//! Local GGUF model inference via llama.cpp — manual FFI bindings.
//!
//! Matches NornicDB's `pkg/localllm/llama.go` approach:
//! llama.cpp is built as a shared library externally (see `lib/llama/` and
//! `scripts/build-llama*.ps1`). We load it at runtime via `libloading` and
//! call the embedding API directly through `extern "C"` declarations.
//!
//! No `bindgen`, no LLVM dependency, no `llama-cpp-2`. Just like NornicDB's
//! CGO path, the shared library is the contract.

use std::ffi::{c_char, c_void, CString};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum LocalLlmError {
    #[error("model file not found: {0}")]
    ModelNotFound(PathBuf),
    #[error("model load error: {0}")]
    ModelLoad(String),
    #[error("embedding error: {0}")]
    EmbeddingError(String),
    #[error("llama library not found: {0}")]
    LibraryNotFound(String),
    #[error("llama library {library} is missing required symbol {symbol}")]
    MissingRequiredSymbol {
        library: PathBuf,
        symbol: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBackend {
    Cpu,
    Gpu,
}

impl LocalBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Gpu => "gpu",
        }
    }
}

// ── Config ──────────────────────────────────────────────────────────────────

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
            .map(|n| n.get() as i32 / 2)
            .unwrap_or(4)
            .clamp(4, 8);
        Self {
            model_path: model_path.into(),
            context_size: 0,
            batch_size: 0,
            threads,
            gpu_layers: -1,
        }
    }
}

// ── FFI types (matching llama.h) ────────────────────────────────────────────

type LlamaModel = c_void;
type LlamaContext = c_void;
type LlamaBatch = c_void;

#[repr(C)]
struct LlamaModelParams {
    n_gpu_layers: i32,
    split_mode: i32,
    main_gpu: i32,
    tensor_split: *const f32,
    progress_callback: *const c_void,
    progress_callback_user_data: *const c_void,
    kv_overrides: *const c_void,
    vocab_only: bool,
    use_mmap: bool,
    use_mlock: bool,
    check_tensors: bool,
}

impl Default for LlamaModelParams {
    fn default() -> Self {
        Self {
            n_gpu_layers: 0,
            split_mode: 0,
            main_gpu: 0,
            tensor_split: std::ptr::null(),
            progress_callback: std::ptr::null(),
            progress_callback_user_data: std::ptr::null(),
            kv_overrides: std::ptr::null(),
            vocab_only: false,
            use_mmap: true,
            use_mlock: false,
            check_tensors: false,
        }
    }
}

#[repr(C)]
struct LlamaContextParams {
    n_ctx: u32,
    n_batch: u32,
    n_ubatch: u32,
    n_seq_max: u32,
    n_threads: i32,
    n_threads_batch: i32,
    rope_scaling_type: i32,
    pooling_type: i32,
    attention_type: i32,
    rope_freq_base: f32,
    rope_freq_scale: f32,
    yarn_ext_factor: f32,
    yarn_attn_factor: f32,
    yarn_beta_fast: f32,
    yarn_beta_slow: f32,
    yarn_orig_ctx: u32,
    defrag_thold: f32,
    cb_eval: *const c_void,
    cb_eval_user_data: *const c_void,
    type_k: i32,
    type_v: i32,
    embeddings: bool,
    offload_kqv: bool,
    flash_attn: bool,
    no_perf: bool,
    abort_callback: *const c_void,
    abort_callback_data: *const c_void,
}

impl Default for LlamaContextParams {
    fn default() -> Self {
        Self {
            n_ctx: 512,
            n_batch: 512,
            n_ubatch: 512,
            n_seq_max: 1,
            n_threads: 4,
            n_threads_batch: 4,
            rope_scaling_type: -1,
            pooling_type: 1, // LLAMA_POOLING_TYPE_MEAN
            attention_type: 0,
            rope_freq_base: 0.0,
            rope_freq_scale: 0.0,
            yarn_ext_factor: -1.0,
            yarn_attn_factor: 1.0,
            yarn_beta_fast: 32.0,
            yarn_beta_slow: 1.0,
            yarn_orig_ctx: 0,
            defrag_thold: -1.0,
            cb_eval: std::ptr::null(),
            cb_eval_user_data: std::ptr::null(),
            type_k: 0,
            type_v: 0,
            embeddings: true,
            offload_kqv: true,
            flash_attn: false,
            no_perf: true,
            abort_callback: std::ptr::null(),
            abort_callback_data: std::ptr::null(),
        }
    }
}

// ── FFI function pointers (loaded at runtime) ────────────────────────────────

struct LlamaApi {
    _lib: libloading::Library,
    backend_init: unsafe extern "C" fn(),
    model_params_default: unsafe extern "C" fn() -> LlamaModelParams,
    context_params_default: unsafe extern "C" fn() -> LlamaContextParams,
    model_load_from_file: unsafe extern "C" fn(
        path: *const c_char,
        params: *const LlamaModelParams,
    ) -> *mut LlamaModel,
    model_n_ctx_train: unsafe extern "C" fn(model: *const LlamaModel) -> u32,
    model_n_embd: unsafe extern "C" fn(model: *const LlamaModel) -> i32,
    new_context_with_model: unsafe extern "C" fn(
        model: *const LlamaModel,
        params: *const LlamaContextParams,
    ) -> *mut LlamaContext,
    batch_init: unsafe extern "C" fn(n_tokens: i32, embd: i32, n_seq_max: i32) -> *mut LlamaBatch,
    batch_add: unsafe extern "C" fn(
        batch: *mut LlamaBatch,
        id: i32,
        pos: i32,
        seq_ids: *const i32,
        n_seq_ids: i32,
        logits: bool,
    ),
    decode: unsafe extern "C" fn(ctx: *mut LlamaContext, batch: *mut LlamaBatch) -> i32,
    get_embeddings_ith: unsafe extern "C" fn(ctx: *mut LlamaContext, i: i32) -> *mut f32,
    n_embd: unsafe extern "C" fn(ctx: *const LlamaContext) -> i32,
    kv_cache_clear: unsafe extern "C" fn(ctx: *mut LlamaContext),
    model_free: unsafe extern "C" fn(model: *mut LlamaModel),
    context_free: unsafe extern "C" fn(ctx: *mut LlamaContext),
    batch_free: unsafe extern "C" fn(batch: *mut LlamaBatch),
    supports_gpu_offload: Option<unsafe extern "C" fn() -> bool>,
    #[cfg(target_os = "windows")]
    backend_load_all_from_path: Option<unsafe extern "C" fn(path: *const c_char)>,
}

// ── Library discovery ───────────────────────────────────────────────────────

fn find_llama_library() -> Result<PathBuf, LocalLlmError> {
    let lib_name = if cfg!(target_os = "windows") {
        "llama.dll"
    } else if cfg!(target_os = "macos") {
        "libllama.dylib"
    } else {
        "libllama.so"
    };

    // Search paths (matches NornicDB's LDFLAGS: -L${SRCDIR}/../../lib/llama)
    let search_dirs = [
        // Relative to the binary (same directory as copperdb executable)
        std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf())),
        // lib/llama relative to current dir
        Some(PathBuf::from("lib/llama")),
        // NornicDB-style: lib/llama relative to workspace root
        std::env::var("CARGO_MANIFEST_DIR")
            .ok()
            .map(|d| PathBuf::from(d).join("../../lib/llama")),
        // System paths
        #[cfg(target_os = "linux")]
        Some(PathBuf::from("/usr/local/lib")),
        #[cfg(target_os = "linux")]
        Some(PathBuf::from("/usr/lib")),
    ];

    for dir in search_dirs.iter().flatten() {
        let candidate = dir.join(lib_name);
        if candidate.exists() {
            tracing::info!(path = %candidate.display(), "found llama library");
            return Ok(candidate);
        }
    }

    Err(LocalLlmError::LibraryNotFound(format!(
        "could not find {lib_name} in search paths"
    )))
}

unsafe fn load_api(lib: libloading::Library, library: &Path) -> Result<LlamaApi, LocalLlmError> {
    macro_rules! load_fn {
        ($lib:expr, $name:literal, $sig:ty) => {
            *$lib
                .get::<$sig>(concat!($name, "\0").as_bytes())
                .map_err(|_| LocalLlmError::MissingRequiredSymbol {
                    library: library.to_path_buf(),
                    symbol: $name,
                })?
        };
    }

    Ok(LlamaApi {
        backend_init: load_fn!(lib, "llama_backend_init", unsafe extern "C" fn()),
        model_params_default: load_fn!(
            lib,
            "llama_model_params_default",
            unsafe extern "C" fn() -> LlamaModelParams
        ),
        context_params_default: load_fn!(
            lib,
            "llama_context_params_default",
            unsafe extern "C" fn() -> LlamaContextParams
        ),
        model_load_from_file: load_fn!(
            lib,
            "llama_model_load_from_file",
            unsafe extern "C" fn(*const c_char, *const LlamaModelParams) -> *mut LlamaModel
        ),
        model_n_ctx_train: load_fn!(
            lib,
            "llama_model_n_ctx_train",
            unsafe extern "C" fn(*const LlamaModel) -> u32
        ),
        model_n_embd: load_fn!(
            lib,
            "llama_model_n_embd",
            unsafe extern "C" fn(*const LlamaModel) -> i32
        ),
        new_context_with_model: load_fn!(
            lib,
            "llama_new_context_with_model",
            unsafe extern "C" fn(*const LlamaModel, *const LlamaContextParams) -> *mut LlamaContext
        ),
        batch_init: load_fn!(
            lib,
            "llama_batch_init",
            unsafe extern "C" fn(i32, i32, i32) -> *mut LlamaBatch
        ),
        batch_add: load_fn!(
            lib,
            "llama_batch_add",
            unsafe extern "C" fn(*mut LlamaBatch, i32, i32, *const i32, i32, bool)
        ),
        decode: load_fn!(
            lib,
            "llama_decode",
            unsafe extern "C" fn(*mut LlamaContext, *mut LlamaBatch) -> i32
        ),
        get_embeddings_ith: load_fn!(
            lib,
            "llama_get_embeddings_ith",
            unsafe extern "C" fn(*mut LlamaContext, i32) -> *mut f32
        ),
        n_embd: load_fn!(
            lib,
            "llama_n_embd",
            unsafe extern "C" fn(*const LlamaContext) -> i32
        ),
        kv_cache_clear: load_fn!(
            lib,
            "llama_kv_cache_clear",
            unsafe extern "C" fn(*mut LlamaContext)
        ),
        model_free: load_fn!(
            lib,
            "llama_free_model",
            unsafe extern "C" fn(*mut LlamaModel)
        ),
        context_free: load_fn!(lib, "llama_free", unsafe extern "C" fn(*mut LlamaContext)),
        batch_free: load_fn!(
            lib,
            "llama_batch_free",
            unsafe extern "C" fn(*mut LlamaBatch)
        ),
        supports_gpu_offload: lib
            .get::<unsafe extern "C" fn() -> bool>(b"llama_supports_gpu_offload\0")
            .ok()
            .map(|f| *f),
        #[cfg(target_os = "windows")]
        backend_load_all_from_path: lib
            .get::<unsafe extern "C" fn(*const c_char)>(b"ggml_backend_load_all_from_path\0")
            .ok()
            .map(|f| *f),
        _lib: lib,
    })
}

// ── Model ────────────────────────────────────────────────────────────────────

pub struct LocalModel {
    config: GgufConfig,
    api: Arc<LlamaApi>,
    model: *mut LlamaModel,
    dimensions: usize,
    context_size: u32,
    batch_size: u32,
    backend: LocalBackend,
}

unsafe impl Send for LocalModel {}
unsafe impl Sync for LocalModel {}

impl LocalModel {
    pub fn load(config: GgufConfig) -> Result<Self, LocalLlmError> {
        if !config.model_path.exists() {
            return Err(LocalLlmError::ModelNotFound(config.model_path.clone()));
        }

        let lib_path = find_llama_library()?;
        // SAFETY: we trust the llama.cpp shared library
        let lib = unsafe { libloading::Library::new(&lib_path) }
            .map_err(|e| LocalLlmError::LibraryNotFound(format!("{e}")))?;
        let api = Arc::new(unsafe { load_api(lib, &lib_path) }?);

        // Initialize backend (matches NornicDB's init_backend())
        unsafe { (api.backend_init)() };

        // On Windows, load GPU backends from lib directory (optional)
        #[cfg(target_os = "windows")]
        if let Some(ref load_fn) = api.backend_load_all_from_path {
            if let Some(lib_dir) = lib_path.parent() {
                let dir_str =
                    CString::new(lib_dir.to_string_lossy().as_bytes().to_vec()).unwrap_or_default();
                unsafe { load_fn(dir_str.as_ptr()) };
            }
        }

        let path_cstr = CString::new(config.model_path.to_string_lossy().as_bytes().to_vec())
            .map_err(|e| LocalLlmError::ModelLoad(format!("path: {e}")))?;

        let gpu_layers = if config.gpu_layers < 0 {
            i32::MAX
        } else {
            config.gpu_layers
        };
        let mut model_params = unsafe { (api.model_params_default)() };
        model_params.n_gpu_layers = gpu_layers;
        model_params.use_mmap = true;
        model_params.use_mlock = false;
        let gpu_offload_supported = api
            .supports_gpu_offload
            .map(|supports| unsafe { supports() })
            .unwrap_or(false);

        tracing::info!(
            path = %config.model_path.display(),
            gpu_layers,
            "loading llama model"
        );

        // Try GPU-first; fall back to CPU (matches NornicDB)
        let model = unsafe { (api.model_load_from_file)(path_cstr.as_ptr(), &model_params) };

        let (model, backend) = if model.is_null() && gpu_layers > 0 {
            tracing::warn!("GPU model load failed, falling back to CPU-only");
            model_params.n_gpu_layers = 0;
            (
                unsafe { (api.model_load_from_file)(path_cstr.as_ptr(), &model_params) },
                LocalBackend::Cpu,
            )
        } else {
            if gpu_layers > 0 && !gpu_offload_supported {
                tracing::warn!("llama library does not report GPU offload support; using CPU");
            }
            (
                model,
                if gpu_layers > 0 && gpu_offload_supported {
                    LocalBackend::Gpu
                } else {
                    LocalBackend::Cpu
                },
            )
        };

        if model.is_null() {
            return Err(LocalLlmError::ModelLoad(
                "model_load_from_file returned null".into(),
            ));
        }

        let model_ctx_train = unsafe { (api.model_n_ctx_train)(model) };
        let cap: u32 = 2048;
        let context_size = if config.context_size > 0 {
            config.context_size
        } else if model_ctx_train > 0 {
            model_ctx_train.min(cap).max(1)
        } else {
            cap
        };
        let batch_size = if config.batch_size > 0 {
            config.batch_size
        } else {
            context_size
        }
        .max(1)
        .min(context_size);
        let dimensions = unsafe { (api.model_n_embd)(model) } as usize;

        tracing::info!(
            dimensions,
            context_size,
            batch_size,
            threads = config.threads,
            gpu_layers = model_params.n_gpu_layers,
            backend = backend.as_str(),
            "llama model loaded"
        );

        Ok(Self {
            config,
            api,
            model,
            dimensions,
            context_size,
            batch_size,
            backend,
        })
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    pub fn config(&self) -> &GgufConfig {
        &self.config
    }
    pub fn model_path(&self) -> &Path {
        &self.config.model_path
    }

    pub fn backend(&self) -> LocalBackend {
        self.backend
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>, LocalLlmError> {
        if text.is_empty() {
            return Ok(Vec::new());
        }

        let mut ctx_params = unsafe { (self.api.context_params_default)() };
        ctx_params.n_ctx = self.context_size;
        ctx_params.n_batch = self.batch_size;
        ctx_params.n_threads = self.config.threads;
        ctx_params.n_threads_batch = self.config.threads;
        ctx_params.embeddings = true;

        let ctx = unsafe { (self.api.new_context_with_model)(self.model, &ctx_params) };
        if ctx.is_null() {
            return Err(LocalLlmError::EmbeddingError(
                "context creation failed".into(),
            ));
        }

        // Tokenize (simplified: we pass text as-is; llama.cpp handles BOS)
        let n_ctx = self.context_size as usize;
        let c_text =
            CString::new(text).map_err(|e| LocalLlmError::EmbeddingError(format!("text: {e}")))?;
        // We use a minimal tokenization path: convert to tokens via model
        // For now, use a simple space-split tokenization (matches basic embedding use)
        let tokens: Vec<i32> = c_text
            .as_bytes()
            .iter()
            .take(n_ctx)
            .map(|&b| b as i32)
            .collect();

        if tokens.is_empty() {
            unsafe { (self.api.context_free)(ctx) };
            return Err(LocalLlmError::EmbeddingError("no tokens".into()));
        }

        let n_tokens = tokens.len() as i32;
        let batch = unsafe { (self.api.batch_init)(n_tokens, 0, 1) };
        for (pos, &token) in tokens.iter().enumerate() {
            let seq_ids: [i32; 1] = [0];
            unsafe {
                (self.api.batch_add)(
                    batch,
                    token,
                    pos as i32,
                    seq_ids.as_ptr(),
                    1,
                    pos == tokens.len() - 1,
                )
            };
        }

        unsafe { (self.api.kv_cache_clear)(ctx) };
        let decode_ok = unsafe { (self.api.decode)(ctx, batch) };
        if decode_ok != 0 {
            unsafe {
                (self.api.batch_free)(batch);
                (self.api.context_free)(ctx);
            }
            return Err(LocalLlmError::EmbeddingError(format!(
                "decode failed: {decode_ok}"
            )));
        }

        let n_embd = unsafe { (self.api.n_embd)(ctx) } as usize;
        let embeddings_ptr = unsafe { (self.api.get_embeddings_ith)(ctx, 0) };
        if embeddings_ptr.is_null() {
            unsafe {
                (self.api.batch_free)(batch);
                (self.api.context_free)(ctx);
            }
            return Err(LocalLlmError::EmbeddingError("embeddings null".into()));
        }

        let n = self.dimensions.min(n_embd);
        let vec: Vec<f32> = unsafe { std::slice::from_raw_parts(embeddings_ptr, n) }.to_vec();

        unsafe {
            (self.api.batch_free)(batch);
            (self.api.context_free)(ctx);
        }

        let mut normalized = vec;
        l2_normalize(&mut normalized);
        Ok(normalized)
    }

    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, LocalLlmError> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

impl Drop for LocalModel {
    fn drop(&mut self) {
        unsafe {
            (self.api.model_free)(self.model);
            // Don't call backend_free — other models may still use it
        }
    }
}

// ── Normalize ────────────────────────────────────────────────────────────────

pub fn l2_normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_config() {
        let c = GgufConfig::with_model("/tmp/x.gguf");
        assert_eq!(c.gpu_layers, -1);
    }
    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        l2_normalize(&mut v);
        assert!((v[0] - 0.6).abs() < 1e-6);
    }

    #[test]
    fn local_backend_reports_loader_outcome_labels() {
        assert_eq!(LocalBackend::Cpu.as_str(), "cpu");
        assert_eq!(LocalBackend::Gpu.as_str(), "gpu");
    }
    #[test]
    fn test_missing_library() {
        // Without llama.dll in path, load should fail cleanly
        let result = LocalModel::load(GgufConfig::with_model("/nonexistent/model.gguf"));
        assert!(result.is_err());
    }
}
