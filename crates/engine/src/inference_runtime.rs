use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use copperdb_inference::{
    Evidence, InferenceError, InferenceNotification, Provenance, ProviderRegistryBuilder,
    ProviderReview, ReviewProvider, SchedulerConfig, SignalConfig, SignalEngine, SimilarityResult,
    SimilaritySearch, Suggestion, SuggestionMethod, SuggestionRepository, SuggestionScheduler,
    SuggestionStatus,
};
use copperdb_storage::NodeRecord;
use copperdb_util::RequestContext;
use sha2::{Digest, Sha256};

struct StorageSimilaritySearch {
    vector_indexes: Arc<crate::VectorIndexManager>,
}

impl SimilaritySearch for StorageSimilaritySearch {
    fn search(
        &self,
        request_context: &RequestContext,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SimilarityResult>, InferenceError> {
        self.vector_indexes
            .inference_query(request_context.cancellation(), embedding, limit)
            .map(|results| {
                results
                    .into_iter()
                    .map(|(id, score)| SimilarityResult {
                        id,
                        score: f64::from(score),
                    })
                    .collect()
            })
            .map_err(|error| InferenceError::Storage(error.to_string()))
    }
}

struct FailClosedReviewProvider;

impl ReviewProvider for FailClosedReviewProvider {
    fn review(
        &self,
        _request_context: &RequestContext,
        _suggestions: &[Suggestion],
    ) -> Result<Vec<ProviderReview>, InferenceError> {
        Err(InferenceError::ProviderFailure(
            "no Heimdall review model is configured".into(),
        ))
    }
}

pub(crate) struct InferenceRuntime {
    database: String,
    embedding_identity: String,
    signal_engine: SignalEngine,
    repository: Arc<SuggestionRepository>,
    scheduler: SuggestionScheduler,
}

pub(crate) struct InferenceDispatcher {
    sender: mpsc::SyncSender<NodeRecord>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl InferenceDispatcher {
    pub(crate) fn new(runtime: Arc<InferenceRuntime>, capacity: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel(capacity.max(1));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            runtime.recover_and_drain(&RequestContext::detached());
            loop {
                if worker_stop.load(Ordering::Acquire) {
                    while let Ok(node) = receiver.try_recv() {
                        let _ = runtime.on_embedding_stored(&node);
                    }
                    break;
                }
                match receiver.recv_timeout(Duration::from_millis(10)) {
                    Ok(node) => {
                        if let Err(error) = runtime.on_embedding_stored(&node) {
                            tracing::warn!(error = %error, node_id = %node.id, "inference signal processing failed");
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        runtime.recover_and_drain(&RequestContext::detached());
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });
        Self {
            sender,
            stop,
            worker: Some(worker),
        }
    }

    pub(crate) fn callback(&self) -> copperdb_storage::NodeEventCallback {
        let sender = self.sender.clone();
        let stop = Arc::clone(&self.stop);
        Arc::new(move |node| {
            if node.has_materialized_embedding()
                && !stop.load(Ordering::Acquire)
                && sender.send(node).is_err()
            {
                tracing::warn!("inference embedding event worker stopped");
            }
        })
    }
}

impl Drop for InferenceDispatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl InferenceRuntime {
    pub(crate) fn new(
        database: String,
        embedding_identity: String,
        vector_indexes: Arc<crate::VectorIndexManager>,
        repository: Arc<SuggestionRepository>,
        review_provider: Option<Arc<dyn ReviewProvider>>,
    ) -> Result<Self, InferenceError> {
        let search = Arc::new(StorageSimilaritySearch { vector_indexes });
        let signal_engine =
            SignalEngine::new(SignalConfig::default()).with_similarity_search(search);
        let mut providers = ProviderRegistryBuilder::default();
        providers.register(
            "heimdall",
            review_provider.unwrap_or_else(|| Arc::new(FailClosedReviewProvider)),
        )?;
        let scheduler = SuggestionScheduler::new(
            Arc::clone(&repository),
            Arc::new(providers.build()),
            SchedulerConfig::default(),
        );
        scheduler.recover_pending()?;
        Ok(Self {
            database,
            embedding_identity,
            signal_engine,
            repository,
            scheduler,
        })
    }

    pub(crate) fn on_embedding_stored(&self, node: &NodeRecord) -> Result<(), InferenceError> {
        if !node.has_materialized_embedding() {
            return Ok(());
        }
        let request_context = RequestContext::detached();
        let suggestions =
            self.signal_engine
                .on_store(&request_context, &node.id, &node.chunk_embeddings)?;
        self.persist_and_schedule(&request_context, suggestions, Some(&node.chunk_embeddings))
    }

    pub(crate) fn on_access(
        &self,
        request_context: &RequestContext,
        node_id: &str,
        observed_at_unix_ms: u64,
    ) -> Result<(), InferenceError> {
        let suggestions =
            self.signal_engine
                .on_access_at(request_context, node_id, observed_at_unix_ms)?;
        self.persist_and_schedule(request_context, suggestions, None)
    }

    pub(crate) fn drain_notifications(&self) -> Vec<InferenceNotification> {
        self.scheduler.drain_notifications()
    }

    fn persist_and_schedule(
        &self,
        request_context: &RequestContext,
        suggestions: Vec<copperdb_inference::EdgeSuggestion>,
        source_embeddings: Option<&[Vec<f32>]>,
    ) -> Result<(), InferenceError> {
        for candidate in suggestions {
            request_context
                .check_active()
                .map_err(|_| InferenceError::RequestCancelled)?;
            let observed_at_unix_ms = now_unix_ms();
            let input_digest = digest_input(&candidate, source_embeddings)?;
            let suggestion = self.repository.record_evidence(Evidence {
                id: String::new(),
                database: self.database.clone(),
                source_id: candidate.source_id,
                target_id: candidate.target_id,
                relationship_type: candidate.relationship_type,
                signal: method_name(candidate.method).into(),
                score: candidate.confidence,
                session_id: request_context.request_id().into(),
                request_id: Some(request_context.request_id().into()),
                observed_at_unix_ms,
                reason: candidate.reason,
                provenance: Provenance {
                    algorithm: method_name(candidate.method).into(),
                    algorithm_version: "1".into(),
                    embedding_identity: Some(self.embedding_identity.clone()),
                    policy_id: Some("auto-links".into()),
                    policy_version: Some("1".into()),
                    input_digest,
                    ..Provenance::default()
                },
                metadata: BTreeMap::new(),
            })?;
            if suggestion.status == SuggestionStatus::PendingReview {
                self.scheduler.enqueue(suggestion.id)?;
            }
        }
        self.drain_scheduler(request_context);
        Ok(())
    }

    fn drain_scheduler(&self, request_context: &RequestContext) {
        let config = SchedulerConfig::default();
        let max_runs = config.queue_capacity * (config.retry_limit as usize + 1);
        for _ in 0..max_runs {
            if let Ok(None) = self.scheduler.run_next(request_context) {
                break;
            }
        }
    }

    fn recover_and_drain(&self, request_context: &RequestContext) {
        loop {
            match self.scheduler.recover_pending() {
                Ok(0) | Err(_) => break,
                Ok(_) => self.drain_scheduler(request_context),
            }
        }
    }
}

fn method_name(method: SuggestionMethod) -> &'static str {
    match method {
        SuggestionMethod::Similarity => "similarity",
        SuggestionMethod::CoAccess => "coaccess",
        SuggestionMethod::Temporal => "temporal",
        SuggestionMethod::Transitive => "transitive",
    }
}

fn digest_input(
    candidate: &copperdb_inference::EdgeSuggestion,
    source_embeddings: Option<&[Vec<f32>]>,
) -> Result<String, InferenceError> {
    let bytes = serde_json::to_vec(&(candidate, source_embeddings))
        .map_err(|error| InferenceError::Storage(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
