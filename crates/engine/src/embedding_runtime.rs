use crate::CopperDbError;
use copperdb_config::EffectiveDatabaseConfig;
use copperdb_embed::{CachedEmbedder, Embedder, LocalGgufEmbedder};
use copperdb_storage::{NodeRecord, StorageEngine};
use copperdb_util::RequestContext;
#[cfg(test)]
use serde_json::json;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingRuntimeState {
    Disabled,
    Cold,
    Warming,
    Ready,
    Degraded,
    Failed,
    Stopping,
}

impl EmbeddingRuntimeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Cold => "cold",
            Self::Warming => "warming",
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingOperationalStatus {
    pub state: EmbeddingRuntimeState,
    pub backend: Option<String>,
    pub worker_count: usize,
    pub pending: u64,
    pub dead_lettered: u64,
    pub completed: u64,
    pub failed: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingRuntimeStatus {
    pub state: EmbeddingRuntimeState,
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub backend: Option<String>,
    pub model_load_duration_ms: Option<u64>,
    pub worker_count: usize,
    pub pending: u64,
    pub queue_age_ms: Option<u64>,
    pub dead_lettered: u64,
    pub completed: u64,
    pub failed: u64,
    pub batch_count: u64,
    pub last_batch_latency_ms: Option<u64>,
    pub average_batch_latency_ms: Option<u64>,
    pub cache_hits: Option<u64>,
    pub cache_misses: Option<u64>,
    pub cache_hit_ratio: Option<f64>,
    pub last_error: Option<String>,
}

/// Per-database embedding lifecycle owner.
pub(crate) struct EmbeddingRuntime {
    storage: Arc<StorageEngine>,
    provider: String,
    model: String,
    dimensions: usize,
    max_attempts: u32,
    retry_backoff: Duration,
    shutdown_timeout: Duration,
    provider_config: EffectiveDatabaseConfig,
    backend: Arc<Mutex<Option<String>>>,
    cache: Arc<Mutex<Option<Arc<CachedEmbedder>>>>,
    model_load_duration_ms: Arc<Mutex<Option<u64>>>,
    embedder: Arc<Mutex<Option<Arc<dyn Embedder>>>>,
    provider_init_lock: Arc<Mutex<()>>,
    state: Arc<Mutex<EmbeddingRuntimeState>>,
    completed: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    batch_count: Arc<AtomicU64>,
    total_batch_latency_ms: Arc<AtomicU64>,
    last_batch_latency_ms: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    generation_reconciled: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    active_workers: Arc<AtomicUsize>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct EmbeddingProviderContext<'a> {
    config: &'a EffectiveDatabaseConfig,
    embedder: &'a Mutex<Option<Arc<dyn Embedder>>>,
    backend: &'a Mutex<Option<String>>,
    cache: &'a Mutex<Option<Arc<CachedEmbedder>>>,
    model_load_duration_ms: &'a Mutex<Option<u64>>,
    provider_init_lock: &'a Mutex<()>,
    state: &'a Mutex<EmbeddingRuntimeState>,
    last_error: &'a Mutex<Option<String>>,
}

struct EmbeddingDrainContext<'a> {
    storage: &'a StorageEngine,
    config: &'a EffectiveDatabaseConfig,
    dimensions: usize,
    max_attempts: u32,
    configured_generation: &'a str,
    state: &'a Mutex<EmbeddingRuntimeState>,
    completed: &'a AtomicU64,
    failed: &'a AtomicU64,
    batch_count: &'a AtomicU64,
    total_batch_latency_ms: &'a AtomicU64,
    last_batch_latency_ms: &'a AtomicU64,
    last_error: &'a Mutex<Option<String>>,
    provider: &'a str,
    model: &'a str,
    backend: &'a Mutex<Option<String>>,
}

struct EmbeddingObservation {
    provider: String,
    model: String,
    mode: String,
    started: Instant,
    result: &'static str,
}

impl EmbeddingObservation {
    fn new(provider: &str, model: &str, backend: Option<&str>) -> Self {
        Self {
            provider: metric_provider(provider).into(),
            model: metric_model(model),
            mode: metric_backend(backend).into(),
            started: Instant::now(),
            result: "failure",
        }
    }

    fn succeed(&mut self) {
        self.result = "success";
    }
}

impl Drop for EmbeddingObservation {
    fn drop(&mut self) {
        if let Some(telemetry) = copperdb_otel::global_telemetry() {
            let labels = [
                ("provider", self.provider.as_str()),
                ("model", self.model.as_str()),
                ("mode", self.mode.as_str()),
            ];
            let _ = telemetry.observe_histogram(
                "nornicdb_embed_duration_seconds",
                &labels,
                self.started.elapsed().as_secs_f64(),
            );
            let _ = telemetry.record_counter(
                "nornicdb_embed_processed_total",
                &[
                    ("provider", self.provider.as_str()),
                    ("model", self.model.as_str()),
                    ("result", self.result),
                    ("mode", self.mode.as_str()),
                ],
            );
        }
    }
}

type BuiltEmbedder = (Arc<dyn Embedder>, String, Option<Arc<CachedEmbedder>>);

impl EmbeddingRuntime {
    pub(crate) fn from_config(
        storage: Arc<StorageEngine>,
        config: &EffectiveDatabaseConfig,
    ) -> Self {
        let (embedder, backend, cache, state, last_error, model_load_duration_ms) =
            if !config.embedding_enabled {
                (
                    None,
                    None,
                    None,
                    EmbeddingRuntimeState::Disabled,
                    None,
                    None,
                )
            } else if config.embedding_warming == "lazy" {
                (None, None, None, EmbeddingRuntimeState::Cold, None, None)
            } else {
                let started_at = Instant::now();
                match build_embedder(config) {
                    Ok((embedder, backend, cache)) => (
                        Some(embedder),
                        Some(backend),
                        cache,
                        EmbeddingRuntimeState::Ready,
                        None,
                        Some(started_at.elapsed().as_millis() as u64),
                    ),
                    Err(error) => (
                        None,
                        None,
                        None,
                        EmbeddingRuntimeState::Failed,
                        Some(error),
                        None,
                    ),
                }
            };
        Self {
            storage,
            provider: config.embedding_provider.clone(),
            model: config.embedding_model.clone(),
            dimensions: config.embedding_dimensions,
            max_attempts: config.embedding_max_attempts,
            retry_backoff: Duration::from_millis(config.embedding_retry_backoff_ms),
            shutdown_timeout: Duration::from_millis(config.embedding_shutdown_timeout_ms),
            provider_config: config.clone(),
            backend: Arc::new(Mutex::new(backend)),
            cache: Arc::new(Mutex::new(cache)),
            model_load_duration_ms: Arc::new(Mutex::new(model_load_duration_ms)),
            embedder: Arc::new(Mutex::new(embedder)),
            provider_init_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(state)),
            completed: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
            batch_count: Arc::new(AtomicU64::new(0)),
            total_batch_latency_ms: Arc::new(AtomicU64::new(0)),
            last_batch_latency_ms: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(last_error)),
            generation_reconciled: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            active_workers: Arc::new(AtomicUsize::new(0)),
            workers: Mutex::new(Vec::new()),
        }
    }

    #[cfg(test)]
    fn ready(storage: Arc<StorageEngine>, dimensions: usize, embedder: Arc<dyn Embedder>) -> Self {
        let mut runtime = Self::ready_with_max_attempts(storage, dimensions, 3, embedder);
        runtime.shutdown_timeout = Duration::from_secs(1);
        runtime
    }

    #[cfg(test)]
    fn ready_with_max_attempts(
        storage: Arc<StorageEngine>,
        dimensions: usize,
        max_attempts: u32,
        embedder: Arc<dyn Embedder>,
    ) -> Self {
        Self {
            storage,
            provider: "test".to_string(),
            model: "test-model".to_string(),
            dimensions,
            max_attempts,
            retry_backoff: Duration::ZERO,
            shutdown_timeout: Duration::ZERO,
            provider_config: copperdb_config::resolve_per_database_config(
                &copperdb_config::Config::default(),
                &std::collections::BTreeMap::new(),
            )
            .expect("default embedding config should resolve"),
            backend: Arc::new(Mutex::new(Some("test".to_string()))),
            cache: Arc::new(Mutex::new(None)),
            model_load_duration_ms: Arc::new(Mutex::new(None)),
            embedder: Arc::new(Mutex::new(Some(embedder))),
            provider_init_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(EmbeddingRuntimeState::Ready)),
            completed: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
            batch_count: Arc::new(AtomicU64::new(0)),
            total_batch_latency_ms: Arc::new(AtomicU64::new(0)),
            last_batch_latency_ms: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            generation_reconciled: Arc::new(AtomicBool::new(true)),
            stop: Arc::new(AtomicBool::new(false)),
            active_workers: Arc::new(AtomicUsize::new(0)),
            workers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn operational_status(&self) -> EmbeddingOperationalStatus {
        EmbeddingOperationalStatus {
            state: *self.state.lock().expect("embedding runtime state lock"),
            backend: self.backend.lock().expect("embedding backend lock").clone(),
            worker_count: self.active_workers.load(Ordering::Relaxed),
            pending: self.storage.pending_embeddings_count_snapshot(),
            dead_lettered: self.storage.embedding_dead_letter_count_snapshot(),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .expect("embedding runtime error lock")
                .clone(),
        }
    }

    pub(crate) fn status(&self) -> Result<EmbeddingRuntimeStatus, CopperDbError> {
        let batch_count = self.batch_count.load(Ordering::Relaxed);
        let total_batch_latency_ms = self.total_batch_latency_ms.load(Ordering::Relaxed);
        let cache_stats = self
            .cache
            .lock()
            .expect("embedding cache lock")
            .as_ref()
            .map(|cache| cache.stats());
        Ok(EmbeddingRuntimeStatus {
            state: *self.state.lock().expect("embedding runtime state lock"),
            provider: self.provider.clone(),
            model: self.model.clone(),
            dimensions: self.dimensions,
            backend: self.backend.lock().expect("embedding backend lock").clone(),
            model_load_duration_ms: *self
                .model_load_duration_ms
                .lock()
                .expect("embedding model load duration lock"),
            worker_count: self.active_workers.load(Ordering::Relaxed),
            pending: self.storage.pending_embeddings_count()? as u64,
            queue_age_ms: self.storage.pending_embedding_oldest_age_ms()?,
            dead_lettered: self.storage.embedding_dead_letter_count()? as u64,
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            batch_count,
            last_batch_latency_ms: (batch_count > 0)
                .then(|| self.last_batch_latency_ms.load(Ordering::Relaxed)),
            average_batch_latency_ms: (batch_count > 0)
                .then(|| total_batch_latency_ms / batch_count),
            cache_hits: cache_stats.map(|stats| stats.hits),
            cache_misses: cache_stats.map(|stats| stats.misses),
            cache_hit_ratio: cache_stats.and_then(|stats| {
                let requests = stats.hits + stats.misses;
                (requests > 0).then(|| stats.hits as f64 / requests as f64)
            }),
            last_error: self
                .last_error
                .lock()
                .expect("embedding runtime error lock")
                .clone(),
        })
    }

    pub(crate) fn embed_query_with_context(
        &self,
        request_context: &RequestContext,
        text: &str,
    ) -> Result<Option<Vec<f32>>, CopperDbError> {
        request_context.check_active()?;
        if text.trim().is_empty()
            || *self.state.lock().expect("embedding runtime state lock")
                == EmbeddingRuntimeState::Disabled
        {
            return Ok(None);
        }
        let embedder = self.ensure_embedder()?;
        let backend = self.backend.lock().expect("embedding backend lock").clone();
        let mut observation =
            EmbeddingObservation::new(&self.provider, &self.model, backend.as_deref());
        let embedding_result = embedder.embed_batch_blocking(&[text.to_string()]);
        request_context.check_active()?;
        let embedding = embedding_result
            .map_err(|error| CopperDbError::Init(format!("query embedding failed: {error}")))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                CopperDbError::Init("query embedding provider returned no embedding".into())
            })?;
        if self.dimensions > 0 && embedding.vector.len() != self.dimensions {
            return Err(CopperDbError::Config(format!(
                "query embedding dimensions mismatch: expected {}, got {}",
                self.dimensions,
                embedding.vector.len()
            )));
        }
        observation.succeed();
        Ok(Some(embedding.vector))
    }

    pub(crate) fn embed_queries_with_context(
        &self,
        request_context: &RequestContext,
        texts: &[String],
    ) -> Result<Option<Vec<Vec<f32>>>, CopperDbError> {
        request_context.check_active()?;
        if texts.is_empty()
            || *self.state.lock().expect("embedding runtime state lock")
                == EmbeddingRuntimeState::Disabled
        {
            return Ok(None);
        }
        let embedder = self.ensure_embedder()?;
        let backend = self.backend.lock().expect("embedding backend lock").clone();
        let mut observation =
            EmbeddingObservation::new(&self.provider, &self.model, backend.as_deref());
        let embeddings = embedder
            .embed_batch_blocking(texts)
            .map_err(|error| CopperDbError::Init(format!("query embedding failed: {error}")))?;
        request_context.check_active()?;
        if embeddings.len() != texts.len() {
            return Err(CopperDbError::Init(format!(
                "query embedding provider returned {} embeddings for {} chunks",
                embeddings.len(),
                texts.len()
            )));
        }
        let vectors = embeddings
            .into_iter()
            .map(|embedding| {
                if self.dimensions > 0 && embedding.vector.len() != self.dimensions {
                    return Err(CopperDbError::Config(format!(
                        "query embedding dimensions mismatch: expected {}, got {}",
                        self.dimensions,
                        embedding.vector.len()
                    )));
                }
                Ok(embedding.vector)
            })
            .collect::<Result<Vec<_>, _>>()?;
        observation.succeed();
        Ok(Some(vectors))
    }

    pub(crate) fn drain_one(&self) -> Result<bool, CopperDbError> {
        record_embedding_runtime_gauges(
            self.storage.pending_embeddings_count_snapshot(),
            self.active_workers.load(Ordering::Relaxed),
        );
        if *self.state.lock().expect("embedding runtime state lock")
            == EmbeddingRuntimeState::Disabled
        {
            return Ok(false);
        }
        let embedder = self.ensure_embedder()?;
        reconcile_embedding_generation(
            &self.storage,
            &self.provider_config,
            &self.generation_reconciled,
        )?;
        let context = EmbeddingDrainContext {
            storage: &self.storage,
            config: &self.provider_config,
            dimensions: self.dimensions,
            max_attempts: self.max_attempts,
            configured_generation: &self.provider_config.embedding_model,
            state: &self.state,
            completed: &self.completed,
            failed: &self.failed,
            batch_count: &self.batch_count,
            total_batch_latency_ms: &self.total_batch_latency_ms,
            last_batch_latency_ms: &self.last_batch_latency_ms,
            last_error: &self.last_error,
            provider: &self.provider,
            model: &self.model,
            backend: &self.backend,
        };
        drain_one_with(&context, embedder)
    }

    pub(crate) fn start_workers(self: &Arc<Self>, worker_count: usize) {
        if !matches!(
            *self.state.lock().expect("embedding runtime state lock"),
            EmbeddingRuntimeState::Ready | EmbeddingRuntimeState::Cold
        ) {
            return;
        }
        let mut workers = self.workers.lock().expect("embedding worker handles lock");
        if !workers.is_empty() {
            return;
        }
        for _ in 0..worker_count.max(1) {
            let storage = Arc::clone(&self.storage);
            let provider_config = self.provider_config.clone();
            let embedder = Arc::clone(&self.embedder);
            let backend = Arc::clone(&self.backend);
            let cache = Arc::clone(&self.cache);
            let model_load_duration_ms = Arc::clone(&self.model_load_duration_ms);
            let provider_init_lock = Arc::clone(&self.provider_init_lock);
            let state = Arc::clone(&self.state);
            let completed = Arc::clone(&self.completed);
            let failed = Arc::clone(&self.failed);
            let batch_count = Arc::clone(&self.batch_count);
            let total_batch_latency_ms = Arc::clone(&self.total_batch_latency_ms);
            let last_batch_latency_ms = Arc::clone(&self.last_batch_latency_ms);
            let last_error = Arc::clone(&self.last_error);
            let generation_reconciled = Arc::clone(&self.generation_reconciled);
            let stop = Arc::clone(&self.stop);
            let active_workers = Arc::clone(&self.active_workers);
            let dimensions = self.dimensions;
            let max_attempts = self.max_attempts;
            let retry_backoff = self.retry_backoff;
            workers.push(thread::spawn(move || {
                active_workers.fetch_add(1, Ordering::Relaxed);
                let provider_context = EmbeddingProviderContext {
                    config: &provider_config,
                    embedder: &embedder,
                    backend: &backend,
                    cache: &cache,
                    model_load_duration_ms: &model_load_duration_ms,
                    provider_init_lock: &provider_init_lock,
                    state: &state,
                    last_error: &last_error,
                };
                let drain_context = EmbeddingDrainContext {
                    storage: &storage,
                    config: &provider_config,
                    dimensions,
                    max_attempts,
                    configured_generation: &provider_config.embedding_model,
                    state: &state,
                    completed: &completed,
                    failed: &failed,
                    batch_count: &batch_count,
                    total_batch_latency_ms: &total_batch_latency_ms,
                    last_batch_latency_ms: &last_batch_latency_ms,
                    last_error: &last_error,
                    provider: &provider_config.embedding_provider,
                    model: &provider_config.embedding_model,
                    backend: &backend,
                };
                record_embedding_runtime_gauges(
                    storage.pending_embeddings_count_snapshot(),
                    active_workers.load(Ordering::Relaxed),
                );
                while !stop.load(Ordering::Acquire) {
                    let loaded_embedder = match ensure_embedder_with(&provider_context) {
                        Ok(embedder) => embedder,
                        Err(_) => break,
                    };
                    if reconcile_embedding_generation(
                        &storage,
                        &provider_config,
                        &generation_reconciled,
                    )
                    .is_err()
                    {
                        thread::sleep(retry_backoff.max(Duration::from_millis(1)));
                        continue;
                    }
                    match drain_one_with(&drain_context, loaded_embedder) {
                        Ok(true) => {}
                        Ok(false) => thread::sleep(Duration::from_millis(25)),
                        Err(_) => thread::sleep(retry_backoff.max(Duration::from_millis(1))),
                    }
                }
                active_workers.fetch_sub(1, Ordering::Relaxed);
                record_embedding_runtime_gauges(
                    storage.pending_embeddings_count_snapshot(),
                    active_workers.load(Ordering::Relaxed),
                );
            }));
        }
    }

    fn ensure_embedder(&self) -> Result<Arc<dyn Embedder>, CopperDbError> {
        ensure_embedder_with(&EmbeddingProviderContext {
            config: &self.provider_config,
            embedder: &self.embedder,
            backend: &self.backend,
            cache: &self.cache,
            model_load_duration_ms: &self.model_load_duration_ms,
            provider_init_lock: &self.provider_init_lock,
            state: &self.state,
            last_error: &self.last_error,
        })
    }

    pub(crate) fn shutdown_workers(&self) -> bool {
        self.stop.store(true, Ordering::Release);
        {
            let mut state = self.state.lock().expect("embedding runtime state lock");
            if matches!(
                *state,
                EmbeddingRuntimeState::Cold
                    | EmbeddingRuntimeState::Ready
                    | EmbeddingRuntimeState::Degraded
            ) {
                *state = EmbeddingRuntimeState::Stopping;
            }
        }
        let deadline = Instant::now() + self.shutdown_timeout;
        while self.active_workers.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let workers =
            std::mem::take(&mut *self.workers.lock().expect("embedding worker handles lock"));
        if self.active_workers.load(Ordering::Acquire) > 0 {
            drop(workers);
            return false;
        }
        for worker in workers {
            let _ = worker.join();
        }
        true
    }
}

impl Drop for EmbeddingRuntime {
    fn drop(&mut self) {
        self.shutdown_workers();
    }
}

fn ensure_embedder_with(
    context: &EmbeddingProviderContext<'_>,
) -> Result<Arc<dyn Embedder>, CopperDbError> {
    if let Some(embedder) = context
        .embedder
        .lock()
        .expect("embedding provider lock")
        .as_ref()
    {
        return Ok(Arc::clone(embedder));
    }
    let _initializing = context
        .provider_init_lock
        .lock()
        .expect("embedding provider init lock");
    if let Some(embedder) = context
        .embedder
        .lock()
        .expect("embedding provider lock")
        .as_ref()
    {
        return Ok(Arc::clone(embedder));
    }
    {
        let mut runtime_state = context.state.lock().expect("embedding runtime state lock");
        if *runtime_state == EmbeddingRuntimeState::Stopping {
            return Err(CopperDbError::Config(
                "embedding runtime is stopping".to_string(),
            ));
        }
        *runtime_state = EmbeddingRuntimeState::Warming;
    }
    let started_at = Instant::now();
    match build_embedder(context.config) {
        Ok((loaded_embedder, loaded_backend, loaded_cache)) => {
            *context.backend.lock().expect("embedding backend lock") = Some(loaded_backend);
            *context.cache.lock().expect("embedding cache lock") = loaded_cache;
            *context
                .model_load_duration_ms
                .lock()
                .expect("embedding model load duration lock") =
                Some(started_at.elapsed().as_millis() as u64);
            *context.embedder.lock().expect("embedding provider lock") =
                Some(Arc::clone(&loaded_embedder));
            let mut runtime_state = context.state.lock().expect("embedding runtime state lock");
            if *runtime_state != EmbeddingRuntimeState::Stopping {
                *runtime_state = EmbeddingRuntimeState::Ready;
            }
            Ok(loaded_embedder)
        }
        Err(error) => {
            *context
                .last_error
                .lock()
                .expect("embedding runtime error lock") = Some(error.clone());
            *context.state.lock().expect("embedding runtime state lock") =
                EmbeddingRuntimeState::Failed;
            Err(CopperDbError::Init(
                "embedding provider initialization failed".to_string(),
            ))
        }
    }
}

fn reconcile_embedding_generation(
    storage: &StorageEngine,
    config: &EffectiveDatabaseConfig,
    reconciled: &AtomicBool,
) -> Result<(), CopperDbError> {
    if reconciled
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(());
    }
    if let Err(error) = storage
        .request_reembedding_for_generation(&config.embedding_model, config.embedding_dimensions)
    {
        reconciled.store(false, Ordering::Release);
        return Err(error.into());
    }
    Ok(())
}

fn drain_one_with(
    context: &EmbeddingDrainContext<'_>,
    embedder: Arc<dyn Embedder>,
) -> Result<bool, CopperDbError> {
    let Some(node) = context.storage.claim_node_needing_embedding()? else {
        return Ok(false);
    };
    let node_id = node.id.clone();
    let result = embed_claimed_node(context, node, embedder);
    context.storage.release_embedding_claim(&node_id);
    result
}

fn embed_claimed_node(
    context: &EmbeddingDrainContext<'_>,
    node: NodeRecord,
    embedder: Arc<dyn Embedder>,
) -> Result<bool, CopperDbError> {
    let backend = context
        .backend
        .lock()
        .expect("embedding backend lock")
        .clone();
    let mut observation =
        EmbeddingObservation::new(context.provider, context.model, backend.as_deref());
    let batch_started_at = Instant::now();
    let embeddings = embedder.embed_batch_blocking(&[canonical_input(&node, context.config)]);
    let batch_latency_ms = batch_started_at.elapsed().as_millis() as u64;
    context.batch_count.fetch_add(1, Ordering::Relaxed);
    context
        .total_batch_latency_ms
        .fetch_add(batch_latency_ms, Ordering::Relaxed);
    context
        .last_batch_latency_ms
        .store(batch_latency_ms, Ordering::Relaxed);
    let embeddings = match embeddings {
        Ok(embeddings) => embeddings,
        Err(error) => {
            return Err(record_failure(
                context.storage,
                &node.id,
                context.max_attempts,
                context.state,
                context.failed,
                context.last_error,
                error.to_string(),
            ))
        }
    };
    let Some(embedding) = embeddings.into_iter().next() else {
        return Err(record_failure(
            context.storage,
            &node.id,
            context.max_attempts,
            context.state,
            context.failed,
            context.last_error,
            "embedding provider returned no embedding".to_string(),
        ));
    };
    if context.dimensions > 0 && embedding.vector.len() != context.dimensions {
        return Err(record_failure(
            context.storage,
            &node.id,
            context.max_attempts,
            context.state,
            context.failed,
            context.last_error,
            format!(
                "embedding dimensions mismatch: expected {}, got {}",
                context.dimensions,
                embedding.vector.len()
            ),
        ));
    }
    let mut update = node;
    update.set_managed_chunk_embeddings(
        vec![embedding.vector],
        Some(embedding.model),
        Some(current_unix_seconds().to_string()),
    );
    update.embed_meta.embedding_generation = Some(context.configured_generation.to_string());
    context.storage.update_node_embedding(&update)?;
    context.completed.fetch_add(1, Ordering::Relaxed);
    let mut runtime_state = context.state.lock().expect("embedding runtime state lock");
    if *runtime_state != EmbeddingRuntimeState::Stopping {
        *runtime_state = EmbeddingRuntimeState::Ready;
    }
    *context
        .last_error
        .lock()
        .expect("embedding runtime error lock") = None;
    observation.succeed();
    Ok(true)
}

fn record_embedding_runtime_gauges(pending: u64, workers: usize) {
    if let Some(telemetry) = copperdb_otel::global_telemetry() {
        let _ = telemetry.set_gauge("nornicdb_embed_queue_depth", &[], pending as f64);
        let _ = telemetry.set_gauge("nornicdb_embed_worker_running", &[], workers as f64);
    }
}

fn metric_provider(provider: &str) -> &'static str {
    match provider {
        "ollama" => "ollama",
        "openai" => "openai",
        "local" | "local_gguf" => "local",
        _ => "other",
    }
}

fn metric_backend(backend: Option<&str>) -> &'static str {
    match backend {
        Some("gpu") => "gpu",
        Some("cuda") => "cuda",
        Some("metal") => "metal",
        Some("vulkan") => "vulkan",
        _ => "cpu",
    }
}

fn metric_model(model: &str) -> String {
    Path::new(model)
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("unknown")
        .chars()
        .take(64)
        .collect()
}

fn record_failure(
    storage: &StorageEngine,
    node_id: &str,
    max_attempts: u32,
    state: &Mutex<EmbeddingRuntimeState>,
    failed: &AtomicU64,
    last_error: &Mutex<Option<String>>,
    error: String,
) -> CopperDbError {
    let dead_lettered = storage
        .record_embedding_failure(node_id, &error, max_attempts, current_unix_seconds())
        .map(|disposition| {
            matches!(
                disposition,
                copperdb_storage::EmbeddingFailureDisposition::DeadLettered
            )
        });
    failed.fetch_add(1, Ordering::Relaxed);
    let mut runtime_state = state.lock().expect("embedding runtime state lock");
    if *runtime_state != EmbeddingRuntimeState::Stopping {
        *runtime_state = EmbeddingRuntimeState::Degraded;
    }
    *last_error.lock().expect("embedding runtime error lock") =
        Some(error.chars().take(256).collect());
    match dead_lettered {
        Ok(true) => {
            CopperDbError::Init("embedding generation failed and was dead-lettered".to_string())
        }
        Ok(false) => CopperDbError::Init("embedding generation failed".to_string()),
        Err(storage_error) => CopperDbError::from(storage_error),
    }
}

fn canonical_input(node: &NodeRecord, config: &EffectiveDatabaseConfig) -> String {
    copperdb_embeddingutil::build_text(
        &node.labels,
        &node.properties,
        &copperdb_embeddingutil::EmbedTextOptions {
            include_labels: config.embedding_include_labels,
            include_properties: config
                .embedding_properties_include
                .iter()
                .cloned()
                .collect(),
            exclude_properties: config
                .embedding_properties_exclude
                .iter()
                .cloned()
                .collect(),
        },
    )
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn build_embedder(config: &EffectiveDatabaseConfig) -> Result<BuiltEmbedder, String> {
    if config.embedding_provider != "local_gguf" {
        return Err(format!(
            "unsupported embedding provider {:?}; supported provider: local_gguf",
            config.embedding_provider
        ));
    }
    if config.embedding_model.trim().is_empty() {
        return Err("local_gguf embedding provider requires a model path".to_string());
    }
    let model_path = Path::new(&config.embedding_model);
    let model_name = model_path
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "local_gguf model path must name a file".to_string())?;
    let embedder = LocalGgufEmbedder::new(
        model_name,
        model_path.to_path_buf(),
        config.embedding_dimensions,
        (config.embedding_warmup_interval_ms > 0)
            .then(|| Duration::from_millis(config.embedding_warmup_interval_ms)),
    )
    .map_err(|error| error.to_string())?;
    let backend = embedder.backend().to_string();
    let base: Arc<dyn Embedder> = Arc::new(embedder);
    if config.embedding_cache_capacity == 0 {
        return Ok((base, backend, None));
    }
    let cache = Arc::new(CachedEmbedder::from_arc(
        base,
        config.embedding_cache_capacity,
    ));
    let embedder: Arc<dyn Embedder> = Arc::clone(&cache) as Arc<dyn Embedder>;
    Ok((embedder, backend, Some(cache)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_identity_is_bounded_and_does_not_expose_model_paths() {
        assert_eq!(metric_provider("local_gguf"), "local");
        assert_eq!(metric_provider("tenant-provider"), "other");
        assert_eq!(metric_backend(Some("metal")), "metal");
        assert_eq!(metric_backend(Some("tenant-backend")), "cpu");
        assert_eq!(
            metric_model("/private/models/bge-m3.gguf"),
            "bge-m3".to_string()
        );
        assert!(!metric_model("/private/models/bge-m3.gguf").contains("private"));
        assert!(metric_model(&format!("{}.gguf", "x".repeat(100))).len() <= 64);
    }
    use copperdb_embed::{EmbedError, Embedding};
    use copperdb_storage::{NodeEmbeddingMetadata, NodeRecord};
    use std::collections::BTreeMap;
    use std::sync::mpsc::{self, Receiver, Sender};

    struct TestEmbedder(Result<Vec<f32>, EmbedError>);

    struct BlockingEmbedder {
        started: Sender<()>,
        release: Mutex<Receiver<()>>,
    }

    struct UnexpectedEmbedder;

    #[async_trait::async_trait]
    impl Embedder for TestEmbedder {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.embed_batch_blocking(texts)
        }

        fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            let vector = self.0.clone()?;
            Ok(texts
                .iter()
                .map(|text| Embedding {
                    text: text.clone(),
                    vector: vector.clone(),
                    model: "test-model".to_string(),
                })
                .collect())
        }

        fn dimensions(&self) -> usize {
            2
        }
    }

    #[async_trait::async_trait]
    impl Embedder for BlockingEmbedder {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.embed_batch_blocking(texts)
        }

        fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.started.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(texts
                .iter()
                .map(|text| Embedding {
                    text: text.clone(),
                    vector: vec![0.25, 0.75],
                    model: "test-model".to_string(),
                })
                .collect())
        }

        fn dimensions(&self) -> usize {
            2
        }
    }

    #[async_trait::async_trait]
    impl Embedder for UnexpectedEmbedder {
        async fn embed(&self, _texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            panic!("cancelled query must not invoke the embedding provider")
        }

        fn embed_batch_blocking(&self, _texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            panic!("cancelled query must not invoke the embedding provider")
        }

        fn dimensions(&self) -> usize {
            2
        }
    }

    fn node() -> NodeRecord {
        NodeRecord {
            id: "node-1".to_string(),
            labels: vec!["Document".to_string()],
            properties: BTreeMap::from([("body".to_string(), json!("hello"))]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: NodeEmbeddingMetadata::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn disabled_runtime_starts_no_workers() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let config = copperdb_config::resolve_per_database_config(
            &copperdb_config::Config::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        let runtime = Arc::new(EmbeddingRuntime::from_config(storage, &config));
        runtime.start_workers(4);
        assert_eq!(
            runtime.status().unwrap().state,
            EmbeddingRuntimeState::Disabled
        );
        assert_eq!(runtime.status().unwrap().worker_count, 0);
        assert!(runtime.status().unwrap().model_load_duration_ms.is_none());
        assert_eq!(
            runtime.operational_status(),
            EmbeddingOperationalStatus {
                state: EmbeddingRuntimeState::Disabled,
                backend: None,
                worker_count: 0,
                pending: 0,
                dead_lettered: 0,
                completed: 0,
                failed: 0,
                last_error: None,
            }
        );
        assert!(!runtime.drain_one().unwrap());
        assert_eq!(
            runtime
                .embed_query_with_context(&RequestContext::detached(), "graph database")
                .unwrap(),
            None
        );
    }

    #[test]
    fn ready_runtime_embeds_search_queries() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let runtime =
            EmbeddingRuntime::ready(storage, 2, Arc::new(TestEmbedder(Ok(vec![0.25, 0.75]))));

        assert_eq!(
            runtime
                .embed_query_with_context(&RequestContext::detached(), "graph database")
                .unwrap(),
            Some(vec![0.25, 0.75])
        );
        assert_eq!(
            runtime
                .embed_query_with_context(&RequestContext::detached(), "   ")
                .unwrap(),
            None
        );
        assert_eq!(
            runtime
                .embed_queries_with_context(
                    &RequestContext::detached(),
                    &["first chunk".into(), "second chunk".into()],
                )
                .unwrap(),
            Some(vec![vec![0.25, 0.75], vec![0.25, 0.75]])
        );
    }

    #[test]
    fn query_embedding_rejects_pre_cancelled_context_before_provider_call() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let runtime = EmbeddingRuntime::ready(storage, 2, Arc::new(UnexpectedEmbedder));
        let request_context = RequestContext::detached();
        request_context.cancel();

        let error = runtime
            .embed_query_with_context(&request_context, "graph database")
            .unwrap_err();

        assert!(matches!(error, CopperDbError::RequestCancelled(_)));
    }

    #[test]
    fn query_embedding_discards_result_cancelled_during_provider_call() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let runtime = Arc::new(EmbeddingRuntime::ready(
            storage,
            2,
            Arc::new(BlockingEmbedder {
                started: started_tx,
                release: Mutex::new(release_rx),
            }),
        ));
        let request_context = RequestContext::detached();
        let worker_context = request_context.clone();
        let worker_runtime = Arc::clone(&runtime);
        let worker = thread::spawn(move || {
            worker_runtime.embed_query_with_context(&worker_context, "graph database")
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        request_context.cancel();
        release_tx.send(()).unwrap();
        let error = worker.join().unwrap().unwrap_err();

        assert!(matches!(error, CopperDbError::RequestCancelled(_)));
    }

    #[test]
    fn unsupported_enabled_provider_is_failed_without_a_fallback() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let mut config = copperdb_config::resolve_per_database_config(
            &copperdb_config::Config::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        config.embedding_enabled = true;
        config.embedding_provider = "mock".to_string();
        let runtime = EmbeddingRuntime::from_config(storage, &config);

        let status = runtime.status().unwrap();
        assert_eq!(status.state, EmbeddingRuntimeState::Failed);
        assert_eq!(status.worker_count, 0);
        assert!(status.backend.is_none());
        assert!(status.model_load_duration_ms.is_none());
        assert!(status
            .last_error
            .unwrap()
            .contains("unsupported embedding provider"));
    }

    #[test]
    fn lazy_provider_defers_initialization_until_work_is_requested() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let mut config = copperdb_config::resolve_per_database_config(
            &copperdb_config::Config::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        config.embedding_enabled = true;
        config.embedding_provider = "mock".to_string();
        config.embedding_warming = "lazy".to_string();
        let runtime = EmbeddingRuntime::from_config(storage, &config);

        assert_eq!(runtime.status().unwrap().state, EmbeddingRuntimeState::Cold);
        assert!(runtime.status().unwrap().backend.is_none());
        assert!(runtime.drain_one().is_err());
        let status = runtime.status().unwrap();
        assert_eq!(status.state, EmbeddingRuntimeState::Failed);
        assert!(status
            .last_error
            .unwrap()
            .contains("unsupported embedding provider"));
    }

    #[test]
    fn ready_runtime_drains_and_failure_preserves_the_queue() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        storage.put_node_record(&node()).unwrap();
        let runtime = EmbeddingRuntime::ready(
            Arc::clone(&storage),
            2,
            Arc::new(TestEmbedder(Ok(vec![0.25, 0.75]))),
        );
        assert_eq!(runtime.operational_status().pending, 1);
        assert!(runtime.drain_one().unwrap());
        assert_eq!(storage.pending_embeddings_count().unwrap(), 0);
        assert_eq!(runtime.operational_status().pending, 0);
        let status = runtime.status().unwrap();
        assert_eq!(status.completed, 1);
        assert_eq!(status.batch_count, 1);
        assert!(status.last_batch_latency_ms.is_some());
        assert!(status.average_batch_latency_ms.is_some());

        let mut retry = node();
        retry.id = "node-2".to_string();
        storage.put_node_record(&retry).unwrap();
        let failing = EmbeddingRuntime::ready(
            Arc::clone(&storage),
            2,
            Arc::new(TestEmbedder(Err(EmbedError::LocalModel(
                "unavailable".to_string(),
            )))),
        );
        assert!(failing.drain_one().is_err());
        assert_eq!(storage.pending_embeddings_count().unwrap(), 1);
        assert_eq!(failing.operational_status().pending, 1);
        assert_eq!(
            failing.status().unwrap().state,
            EmbeddingRuntimeState::Degraded
        );
    }

    #[test]
    fn failed_embeddings_are_dead_lettered_after_the_configured_attempt_limit() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        storage.put_node_record(&node()).unwrap();
        let runtime = EmbeddingRuntime::ready_with_max_attempts(
            Arc::clone(&storage),
            2,
            2,
            Arc::new(TestEmbedder(Err(EmbedError::LocalModel(
                "unavailable".to_string(),
            )))),
        );

        assert!(runtime.drain_one().is_err());
        assert_eq!(runtime.status().unwrap().pending, 1);
        assert!(runtime.drain_one().is_err());
        let status = runtime.status().unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.dead_lettered, 1);
        assert_eq!(runtime.operational_status().dead_lettered, 1);
        assert_eq!(
            storage
                .embedding_dead_letter("node-1")
                .unwrap()
                .unwrap()
                .attempts,
            2
        );
    }

    #[test]
    fn ready_runtime_worker_drains_and_stops_cleanly() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        storage.put_node_record(&node()).unwrap();
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let runtime = Arc::new(EmbeddingRuntime::ready(
            Arc::clone(&storage),
            2,
            Arc::new(BlockingEmbedder {
                started: started_sender,
                release: Mutex::new(release_receiver),
            }),
        ));

        runtime.start_workers(1);
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("embedding worker should start");
        assert_eq!(runtime.status().unwrap().worker_count, 1);
        release_sender.send(()).unwrap();
        drop(runtime);

        assert_eq!(storage.pending_embeddings_count().unwrap(), 0);
    }

    #[test]
    fn worker_shutdown_is_bounded_while_inference_is_blocked() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        storage.put_node_record(&node()).unwrap();
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let mut runtime = EmbeddingRuntime::ready(
            Arc::clone(&storage),
            2,
            Arc::new(BlockingEmbedder {
                started: started_sender,
                release: Mutex::new(release_receiver),
            }),
        );
        runtime.shutdown_timeout = Duration::ZERO;
        let runtime = Arc::new(runtime);
        runtime.start_workers(1);
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("embedding worker should start");

        let started_at = Instant::now();
        assert!(!runtime.shutdown_workers());
        assert!(started_at.elapsed() < Duration::from_millis(100));
        assert_eq!(
            runtime.status().unwrap().state,
            EmbeddingRuntimeState::Stopping
        );

        release_sender.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.status().unwrap().worker_count > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.status().unwrap().worker_count, 0);
    }
}
