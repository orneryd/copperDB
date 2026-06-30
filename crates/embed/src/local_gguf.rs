//! Local GGUF embedder matching NornicDB's `local_gguf.go`.
//!
//! Features:
//! - llama.cpp model loading with Metal/CUDA GPU acceleration
//! - Crash resilience: panic recovery for FFI faults
//! - Model warmup to prevent GPU memory eviction
//! - Thread-safe embedding generation
//! - Model file resolution from config

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use copperdb_localllm::{GgufConfig, LocalModel};
use super::{Embedder, EmbedError, Embedding};

/// Local GGUF embedder matching NornicDB's `LocalGGUFEmbedder`.
pub struct LocalGgufEmbedder {
    model: Arc<LocalModel>,
    model_name: String,
    model_path: PathBuf,

    // Crash resilience
    closed: AtomicBool,
    stop_warmup: Mutex<Option<crossbeam_channel::Sender<()>>>,

    // Stats
    embed_count: AtomicU64,
    error_count: AtomicU64,
    panic_count: AtomicU64,
    last_embed_time: AtomicI64,
}

impl LocalGgufEmbedder {
    /// Create a local GGUF embedder from config.
    ///
    /// Model resolution: `config.model` → `{models_dir}/{model}.gguf`
    pub fn new(
        model_name: &str,
        model_path: PathBuf,
        dimensions: usize,
        warmup_interval: Option<Duration>,
    ) -> Result<Self, EmbedError> {
        if !model_path.exists() {
            return Err(EmbedError::LocalModel(format!(
                "model not found: {} (expected at {})\n  → Download a GGUF embedding model (e.g. bge-m3) and place it in the models directory",
                model_name,
                model_path.display()
            )));
        }

        let config = GgufConfig::with_model(&model_path);
        let model = LocalModel::load(config)
            .map_err(|e| EmbedError::LocalModel(format!("failed to load model: {e}")))?;

        // Verify dimensions
        let model_dims = model.dimensions();
        if dimensions > 0 && model_dims != dimensions {
            return Err(EmbedError::LocalModel(format!(
                "dimension mismatch: model has {model_dims}, config expects {dimensions}"
            )));
        }

        tracing::info!(
            model = %model_name,
            path = %model_path.display(),
            dimensions = model_dims,
            "local GGUF embedding model loaded"
        );

        let embedder = Self {
            model: Arc::new(model),
            model_name: model_name.to_string(),
            model_path,
            closed: AtomicBool::new(false),
            stop_warmup: Mutex::new(None),
            embed_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            panic_count: AtomicU64::new(0),
            last_embed_time: AtomicI64::new(0),
        };

        // Start warmup goroutine (matching NornicDB's warmupLoop)
        if let Some(interval) = warmup_interval {
            if interval > Duration::ZERO {
                let (tx, rx) = crossbeam_channel::bounded(1);
                *embedder.stop_warmup.lock().unwrap() = Some(tx);
                let model = Arc::clone(&embedder.model);
                let last_embed = embedder.last_embed_time.load(Ordering::Relaxed);
                thread::spawn(move || {
                    loop {
                        // Check stop signal
                        if rx.try_recv().is_ok() { break; }
                        thread::sleep(interval.min(Duration::from_secs(60)));

                        // Skip warmup if recently used
                        let last = AtomicI64::new(last_embed).load(Ordering::Relaxed);
                        let elapsed = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64
                            - last;
                        if elapsed < interval.as_secs() as i64 / 2 {
                            continue;
                        }

                        // Dummy embedding to keep GPU memory warm
                        match model.embed("warmup") {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!(error = %e, "model warmup failed");
                            }
                        }
                    }
                });
                tracing::info!(interval = ?interval, "model warmup enabled");
            }
        }

        Ok(embedder)
    }

    /// Backend string matching NornicDB's Backend() method.
    pub fn backend(&self) -> &str {
        #[cfg(target_os = "macos")]
        { "metal" }
        #[cfg(all(target_os = "linux", feature = "cuda"))]
        { "cuda" }
        #[cfg(not(any(target_os = "macos", all(target_os = "linux", feature = "cuda"))))]
        { "cpu" }
    }

    /// Embed a single text with panic recovery (matching NornicDB's embedWithRecovery).
    pub fn embed_with_recovery(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        // Panic recovery for FFI faults (Plan 04-05-03 / D-09)
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if self.closed.load(Ordering::Relaxed) {
                return Err(EmbedError::LocalModel("embedder is closed".into()));
            }
            self.model.embed(text)
                .map_err(|e| EmbedError::LocalModel(format!("embedding failed: {e}")))
        }));

        match result {
            Ok(Ok(embedding)) => {
                self.embed_count.fetch_add(1, Ordering::Relaxed);
                self.last_embed_time.store(
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64,
                    Ordering::Relaxed,
                );
                Ok(embedding)
            }
            Ok(Err(e)) => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
            Err(panic_info) => {
                self.panic_count.fetch_add(1, Ordering::Relaxed);
                let msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                tracing::error!(
                    panic_count = self.panic_count.load(Ordering::Relaxed),
                    model = %self.model_name,
                    text_len = text.len(),
                    "EMBEDDING PANIC RECOVERED: {msg}"
                );
                Err(EmbedError::LocalModel(format!("PANIC in llama.cpp (recovered): {msg}")))
            }
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        // Signal warmup to stop
        if let Some(tx) = self.stop_warmup.lock().unwrap().take() {
            drop(tx);
        }
    }

    pub fn stats(&self) -> EmbedStats {
        EmbedStats {
            embed_count: self.embed_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            panic_count: self.panic_count.load(Ordering::Relaxed),
            dimensions: self.model.dimensions(),
            backend: self.backend().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmbedStats {
    pub embed_count: u64,
    pub error_count: u64,
    pub panic_count: u64,
    pub dimensions: usize,
    pub backend: String,
}

impl Drop for LocalGgufEmbedder {
    fn drop(&mut self) { self.close(); }
}
