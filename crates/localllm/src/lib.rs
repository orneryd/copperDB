//! Local GGUF model inference via llama.cpp — manual FFI bindings.
//!
//! Matches NornicDB's `pkg/localllm/llama.go` approach:
//! llama.cpp is built as a shared library externally (see `lib/llama/` and
//! `scripts/build-llama*.ps1`). We load it at runtime via `libloading` and
//! call the embedding API directly through `extern "C"` declarations.
//!
//! No `bindgen`, no LLVM dependency, no `llama-cpp-2`. Just like NornicDB's
//! CGO path, the shared library is the contract.

use std::ffi::{CStr, CString, c_char, c_void};
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
type LlamaVocab = c_void;

#[repr(C)]
#[derive(Clone, Copy)]
struct LlamaModelParams {
    devices: *mut *mut c_void,
    tensor_buft_overrides: *const c_void,
    n_gpu_layers: i32,
    split_mode: i32,
    main_gpu: i32,
    tensor_split: *const f32,
    progress_callback: *const c_void,
    progress_callback_user_data: *const c_void,
    kv_overrides: *const c_void,
    vocab_only: bool,
    use_mmap: bool,
    use_direct_io: bool,
    use_mlock: bool,
    check_tensors: bool,
    use_extra_bufts: bool,
    no_host: bool,
    no_alloc: bool,
}

impl Default for LlamaModelParams {
    fn default() -> Self {
        Self {
            devices: std::ptr::null_mut(),
            tensor_buft_overrides: std::ptr::null(),
            n_gpu_layers: 0,
            split_mode: 0,
            main_gpu: 0,
            tensor_split: std::ptr::null(),
            progress_callback: std::ptr::null(),
            progress_callback_user_data: std::ptr::null(),
            kv_overrides: std::ptr::null(),
            vocab_only: false,
            use_mmap: true,
            use_direct_io: false,
            use_mlock: false,
            check_tensors: false,
            use_extra_bufts: false,
            no_host: false,
            no_alloc: false,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct LlamaContextParams {
    n_ctx: u32,
    n_batch: u32,
    n_ubatch: u32,
    n_seq_max: u32,
    n_rs_seq: u32,
    n_outputs_max: u32,
    n_threads: i32,
    n_threads_batch: i32,
    context_type: i32,
    rope_scaling_type: i32,
    pooling_type: i32,
    attention_type: i32,
    flash_attn_type: i32,
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
    abort_callback: *const c_void,
    abort_callback_data: *const c_void,
    embeddings: bool,
    offload_kqv: bool,
    no_perf: bool,
    op_offload: bool,
    swa_full: bool,
    kv_unified: bool,
    samplers: *mut c_void,
    n_samplers: usize,
    context_other: *mut LlamaContext,
}

impl Default for LlamaContextParams {
    fn default() -> Self {
        Self {
            n_ctx: 512,
            n_batch: 512,
            n_ubatch: 512,
            n_seq_max: 1,
            n_rs_seq: 0,
            n_outputs_max: 0,
            n_threads: 4,
            n_threads_batch: 4,
            context_type: 0,
            rope_scaling_type: -1,
            pooling_type: 1, // LLAMA_POOLING_TYPE_MEAN
            attention_type: 1,
            flash_attn_type: 0,
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
            abort_callback: std::ptr::null(),
            abort_callback_data: std::ptr::null(),
            embeddings: true,
            offload_kqv: true,
            no_perf: true,
            op_offload: true,
            swa_full: true,
            kv_unified: false,
            samplers: std::ptr::null_mut(),
            n_samplers: 0,
            context_other: std::ptr::null_mut(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct LlamaBatch {
    n_tokens: i32,
    token: *mut i32,
    embd: *mut f32,
    pos: *mut i32,
    n_seq_id: *mut i32,
    seq_id: *mut *mut i32,
    logits: *mut i8,
}

// ── FFI function pointers (loaded at runtime) ────────────────────────────────

struct LlamaApi {
    _lib: libloading::Library,
    backend_init: unsafe extern "C" fn(),
    model_params_default: unsafe extern "C" fn() -> LlamaModelParams,
    context_params_default: unsafe extern "C" fn() -> LlamaContextParams,
    model_load_from_file:
        unsafe extern "C" fn(path: *const c_char, params: LlamaModelParams) -> *mut LlamaModel,
    model_n_ctx_train: unsafe extern "C" fn(model: *const LlamaModel) -> i32,
    model_n_embd: unsafe extern "C" fn(model: *const LlamaModel) -> i32,
    model_n_cls_out: unsafe extern "C" fn(model: *const LlamaModel) -> u32,
    model_get_vocab: unsafe extern "C" fn(model: *const LlamaModel) -> *const LlamaVocab,
    model_chat_template:
        unsafe extern "C" fn(model: *const LlamaModel, name: *const c_char) -> *const c_char,
    new_context_with_model: unsafe extern "C" fn(
        model: *const LlamaModel,
        params: LlamaContextParams,
    ) -> *mut LlamaContext,
    tokenize: unsafe extern "C" fn(
        vocab: *const LlamaVocab,
        text: *const c_char,
        text_len: i32,
        tokens: *mut i32,
        token_capacity: i32,
        add_special: bool,
        parse_special: bool,
    ) -> i32,
    vocab_bos: unsafe extern "C" fn(vocab: *const LlamaVocab) -> i32,
    vocab_eos: unsafe extern "C" fn(vocab: *const LlamaVocab) -> i32,
    vocab_sep: unsafe extern "C" fn(vocab: *const LlamaVocab) -> i32,
    vocab_get_add_bos: unsafe extern "C" fn(vocab: *const LlamaVocab) -> bool,
    vocab_get_add_eos: unsafe extern "C" fn(vocab: *const LlamaVocab) -> bool,
    vocab_get_add_sep: unsafe extern "C" fn(vocab: *const LlamaVocab) -> bool,
    batch_init: unsafe extern "C" fn(n_tokens: i32, embd: i32, n_seq_max: i32) -> LlamaBatch,
    encode: unsafe extern "C" fn(ctx: *mut LlamaContext, batch: LlamaBatch) -> i32,
    get_embeddings_seq: unsafe extern "C" fn(ctx: *mut LlamaContext, seq_id: i32) -> *mut f32,
    model_free: unsafe extern "C" fn(model: *mut LlamaModel),
    context_free: unsafe extern "C" fn(ctx: *mut LlamaContext),
    batch_free: unsafe extern "C" fn(batch: LlamaBatch),
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
            // SAFETY: Each signature matches the llama C API, and LlamaApi retains the library.
            unsafe {
                *$lib
                    .get::<$sig>(concat!($name, "\0").as_bytes())
                    .map_err(|_| LocalLlmError::MissingRequiredSymbol {
                        library: library.to_path_buf(),
                        symbol: $name,
                    })?
            }
        };
    }

    Ok(LlamaApi {
        backend_init: load_fn!(lib, "llama_backend_init", unsafe extern "C" fn()),
        model_params_default: load_fn!(
            lib,
            "llama_model_default_params",
            unsafe extern "C" fn() -> LlamaModelParams
        ),
        context_params_default: load_fn!(
            lib,
            "llama_context_default_params",
            unsafe extern "C" fn() -> LlamaContextParams
        ),
        model_load_from_file: load_fn!(
            lib,
            "llama_model_load_from_file",
            unsafe extern "C" fn(*const c_char, LlamaModelParams) -> *mut LlamaModel
        ),
        model_n_ctx_train: load_fn!(
            lib,
            "llama_model_n_ctx_train",
            unsafe extern "C" fn(*const LlamaModel) -> i32
        ),
        model_n_embd: load_fn!(
            lib,
            "llama_model_n_embd",
            unsafe extern "C" fn(*const LlamaModel) -> i32
        ),
        model_n_cls_out: load_fn!(
            lib,
            "llama_model_n_cls_out",
            unsafe extern "C" fn(*const LlamaModel) -> u32
        ),
        model_get_vocab: load_fn!(
            lib,
            "llama_model_get_vocab",
            unsafe extern "C" fn(*const LlamaModel) -> *const LlamaVocab
        ),
        model_chat_template: load_fn!(
            lib,
            "llama_model_chat_template",
            unsafe extern "C" fn(*const LlamaModel, *const c_char) -> *const c_char
        ),
        new_context_with_model: load_fn!(
            lib,
            "llama_init_from_model",
            unsafe extern "C" fn(*const LlamaModel, LlamaContextParams) -> *mut LlamaContext
        ),
        tokenize: load_fn!(
            lib,
            "llama_tokenize",
            unsafe extern "C" fn(
                *const LlamaVocab,
                *const c_char,
                i32,
                *mut i32,
                i32,
                bool,
                bool,
            ) -> i32
        ),
        vocab_bos: load_fn!(
            lib,
            "llama_vocab_bos",
            unsafe extern "C" fn(*const LlamaVocab) -> i32
        ),
        vocab_eos: load_fn!(
            lib,
            "llama_vocab_eos",
            unsafe extern "C" fn(*const LlamaVocab) -> i32
        ),
        vocab_sep: load_fn!(
            lib,
            "llama_vocab_sep",
            unsafe extern "C" fn(*const LlamaVocab) -> i32
        ),
        vocab_get_add_bos: load_fn!(
            lib,
            "llama_vocab_get_add_bos",
            unsafe extern "C" fn(*const LlamaVocab) -> bool
        ),
        vocab_get_add_eos: load_fn!(
            lib,
            "llama_vocab_get_add_eos",
            unsafe extern "C" fn(*const LlamaVocab) -> bool
        ),
        vocab_get_add_sep: load_fn!(
            lib,
            "llama_vocab_get_add_sep",
            unsafe extern "C" fn(*const LlamaVocab) -> bool
        ),
        batch_init: load_fn!(
            lib,
            "llama_batch_init",
            unsafe extern "C" fn(i32, i32, i32) -> LlamaBatch
        ),
        encode: load_fn!(
            lib,
            "llama_encode",
            unsafe extern "C" fn(*mut LlamaContext, LlamaBatch) -> i32
        ),
        get_embeddings_seq: load_fn!(
            lib,
            "llama_get_embeddings_seq",
            unsafe extern "C" fn(*mut LlamaContext, i32) -> *mut f32
        ),
        model_free: load_fn!(
            lib,
            "llama_model_free",
            unsafe extern "C" fn(*mut LlamaModel)
        ),
        context_free: load_fn!(lib, "llama_free", unsafe extern "C" fn(*mut LlamaContext)),
        batch_free: load_fn!(lib, "llama_batch_free", unsafe extern "C" fn(LlamaBatch)),
        supports_gpu_offload: unsafe {
            lib.get::<unsafe extern "C" fn() -> bool>(b"llama_supports_gpu_offload\0")
                .ok()
                .map(|function| *function)
        },
        #[cfg(target_os = "windows")]
        backend_load_all_from_path: unsafe {
            lib.get::<unsafe extern "C" fn(*const c_char)>(b"ggml_backend_load_all_from_path\0")
                .ok()
                .map(|function| *function)
        },
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
    pooling_type: i32,
    rerank_template: Option<String>,
}

unsafe impl Send for LocalModel {}
unsafe impl Sync for LocalModel {}

impl LocalModel {
    pub fn load(config: GgufConfig) -> Result<Self, LocalLlmError> {
        Self::load_with_pooling(config, 1)
    }

    fn load_with_pooling(config: GgufConfig, pooling_type: i32) -> Result<Self, LocalLlmError> {
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
        let model = unsafe { (api.model_load_from_file)(path_cstr.as_ptr(), model_params) };

        let (model, backend) = if model.is_null() && gpu_layers > 0 {
            tracing::warn!("GPU model load failed, falling back to CPU-only");
            model_params.n_gpu_layers = 0;
            (
                unsafe { (api.model_load_from_file)(path_cstr.as_ptr(), model_params) },
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
            (model_ctx_train as u32).min(cap).max(1)
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
        let dimensions = if pooling_type == 4 {
            unsafe { (api.model_n_cls_out)(model) as usize }
        } else {
            unsafe { (api.model_n_embd)(model).max(0) as usize }
        };
        let rerank_template = if pooling_type == 4 {
            let template = unsafe { (api.model_chat_template)(model, c"rerank".as_ptr()) };
            if template.is_null() {
                None
            } else {
                Some(
                    unsafe { CStr::from_ptr(template) }
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        } else {
            None
        };

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
            pooling_type,
            rerank_template,
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

        let tokens = self.tokenize(text, true)?;
        let mut embedding = self.infer_tokens(&tokens)?;
        l2_normalize(&mut embedding);
        Ok(embedding)
    }

    fn tokenize(&self, text: &str, add_special: bool) -> Result<Vec<i32>, LocalLlmError> {
        let text = CString::new(text)
            .map_err(|error| LocalLlmError::EmbeddingError(format!("text: {error}")))?;
        let text_len = i32::try_from(text.as_bytes().len())
            .map_err(|_| LocalLlmError::EmbeddingError("text exceeds tokenizer limit".into()))?;
        let vocab = unsafe { (self.api.model_get_vocab)(self.model) };
        if vocab.is_null() {
            return Err(LocalLlmError::EmbeddingError(
                "model vocabulary is null".into(),
            ));
        }
        let count = unsafe {
            (self.api.tokenize)(
                vocab,
                text.as_ptr(),
                text_len,
                std::ptr::null_mut(),
                0,
                add_special,
                true,
            )
        };
        let required = count
            .checked_abs()
            .ok_or_else(|| LocalLlmError::EmbeddingError("token count overflow".into()))?;
        if required == 0 {
            return Ok(Vec::new());
        }
        if required > self.context_size as i32 {
            return Err(LocalLlmError::EmbeddingError(format!(
                "tokenization overflow: requires {required} tokens, limit {}",
                self.context_size
            )));
        }
        let mut tokens = vec![0; required as usize];
        let written = unsafe {
            (self.api.tokenize)(
                vocab,
                text.as_ptr(),
                text_len,
                tokens.as_mut_ptr(),
                required,
                add_special,
                true,
            )
        };
        if written < 0 {
            return Err(LocalLlmError::EmbeddingError(format!(
                "tokenization overflow: requires {} tokens, limit {required}",
                written.checked_abs().unwrap_or(i32::MAX)
            )));
        }
        tokens.truncate(written as usize);
        Ok(tokens)
    }

    fn tokenize_rerank_pair(&self, query: &str, document: &str) -> Result<Vec<i32>, LocalLlmError> {
        let query_tokens = self.tokenize(query, false)?;
        let document_tokens = self.tokenize(document, false)?;
        let vocab = unsafe { (self.api.model_get_vocab)(self.model) };
        let add_bos = unsafe { (self.api.vocab_get_add_bos)(vocab) };
        let add_eos = unsafe { (self.api.vocab_get_add_eos)(vocab) };
        let add_sep = unsafe { (self.api.vocab_get_add_sep)(vocab) };
        let required = query_tokens.len()
            + document_tokens.len()
            + usize::from(add_bos)
            + 2 * usize::from(add_eos)
            + usize::from(add_sep);
        if required > self.context_size as usize {
            return Err(LocalLlmError::EmbeddingError(format!(
                "reranker tokenization overflow: requires {required} tokens, limit {}",
                self.context_size
            )));
        }

        let mut tokens = Vec::with_capacity(required);
        if add_bos {
            tokens.push(unsafe { (self.api.vocab_bos)(vocab) });
        }
        tokens.extend(query_tokens);
        let mut eos = unsafe { (self.api.vocab_eos)(vocab) };
        if eos == -1 {
            eos = unsafe { (self.api.vocab_sep)(vocab) };
        }
        if add_eos {
            tokens.push(eos);
        }
        if add_sep {
            tokens.push(unsafe { (self.api.vocab_sep)(vocab) });
        }
        tokens.extend(document_tokens);
        if add_eos {
            tokens.push(eos);
        }
        Ok(tokens)
    }

    fn infer_tokens(&self, tokens: &[i32]) -> Result<Vec<f32>, LocalLlmError> {
        if tokens.is_empty() {
            return Err(LocalLlmError::EmbeddingError("no tokens".into()));
        }

        let mut ctx_params = unsafe { (self.api.context_params_default)() };
        ctx_params.n_ctx = self.context_size;
        ctx_params.n_batch = self.batch_size;
        ctx_params.n_threads = self.config.threads;
        ctx_params.n_threads_batch = self.config.threads;
        ctx_params.embeddings = true;
        ctx_params.pooling_type = self.pooling_type;
        ctx_params.attention_type = 1;
        ctx_params.flash_attn_type = 0;

        let ctx = unsafe { (self.api.new_context_with_model)(self.model, ctx_params) };
        if ctx.is_null() {
            return Err(LocalLlmError::EmbeddingError(
                "context creation failed".into(),
            ));
        }

        let n_tokens = i32::try_from(tokens.len())
            .map_err(|_| LocalLlmError::EmbeddingError("token count exceeds batch limit".into()))?;
        let mut batch = unsafe { (self.api.batch_init)(n_tokens, 0, 1) };
        if batch.token.is_null()
            || batch.pos.is_null()
            || batch.n_seq_id.is_null()
            || batch.seq_id.is_null()
            || batch.logits.is_null()
        {
            unsafe {
                (self.api.batch_free)(batch);
                (self.api.context_free)(ctx);
            }
            return Err(LocalLlmError::EmbeddingError(
                "batch allocation failed".into(),
            ));
        }
        for (pos, &token) in tokens.iter().enumerate() {
            unsafe {
                *batch.token.add(pos) = token;
                *batch.pos.add(pos) = pos as i32;
                *batch.n_seq_id.add(pos) = 1;
                *(*batch.seq_id.add(pos)) = 0;
                *batch.logits.add(pos) = 1;
            }
        }
        batch.n_tokens = n_tokens;

        let encode_result = unsafe { (self.api.encode)(ctx, batch) };
        if encode_result != 0 {
            unsafe {
                (self.api.batch_free)(batch);
                (self.api.context_free)(ctx);
            }
            return Err(LocalLlmError::EmbeddingError(format!(
                "encode failed: {encode_result}"
            )));
        }

        let embeddings_ptr = unsafe { (self.api.get_embeddings_seq)(ctx, 0) };
        if embeddings_ptr.is_null() {
            unsafe {
                (self.api.batch_free)(batch);
                (self.api.context_free)(ctx);
            }
            return Err(LocalLlmError::EmbeddingError("embeddings null".into()));
        }

        let output =
            unsafe { std::slice::from_raw_parts(embeddings_ptr, self.dimensions) }.to_vec();

        unsafe {
            (self.api.batch_free)(batch);
            (self.api.context_free)(ctx);
        }

        Ok(output)
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

pub struct LocalRerankerModel {
    model: LocalModel,
}

impl LocalRerankerModel {
    pub fn load(config: GgufConfig) -> Result<Self, LocalLlmError> {
        let model = LocalModel::load_with_pooling(config, 4)?;
        if model.dimensions != 1 {
            return Err(LocalLlmError::ModelLoad(format!(
                "reranker requires a single classifier output, got {} dimensions",
                model.dimensions
            )));
        }
        Ok(Self { model })
    }

    pub fn score(&self, query: &str, document: &str) -> Result<f32, LocalLlmError> {
        let tokens = if let Some(template) = self.model.rerank_template.as_deref() {
            self.model
                .tokenize(&format_rerank_prompt(template, query, document), true)?
        } else {
            self.model.tokenize_rerank_pair(query, document)?
        };
        reranker_score_from_output(&self.model.infer_tokens(&tokens)?)
    }

    pub fn backend(&self) -> LocalBackend {
        self.model.backend()
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

pub fn format_rerank_prompt(template: &str, query: &str, document: &str) -> String {
    template
        .replace("{query}", query)
        .replace("{document}", document)
}

pub fn reranker_score_from_output(output: &[f32]) -> Result<f32, LocalLlmError> {
    if output.len() != 1 {
        return Err(LocalLlmError::EmbeddingError(format!(
            "reranker must return exactly one relevance logit, got {} dimensions",
            output.len()
        )));
    }
    Ok(sigmoid(output[0]))
}

pub fn sigmoid(value: f32) -> f32 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exp = value.exp();
        exp / (1.0 + exp)
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
    fn rerank_prompt_replaces_all_placeholders() {
        assert_eq!(
            format_rerank_prompt("Q:{query}|D:{document}|Q:{query}", "why", "because"),
            "Q:why|D:because|Q:why"
        );
    }

    #[test]
    fn reranker_output_requires_one_logit_and_uses_stable_sigmoid() {
        assert!(reranker_score_from_output(&[]).is_err());
        assert!(reranker_score_from_output(&[0.1, 0.2]).is_err());
        assert_eq!(reranker_score_from_output(&[0.0]).unwrap(), 0.5);
        assert!((0.5..1.0).contains(&sigmoid(2.0)));
        assert!((0.0..0.5).contains(&sigmoid(-2.0)));
        assert_eq!(sigmoid(1000.0), 1.0);
        assert_eq!(sigmoid(-1000.0), 0.0);
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
