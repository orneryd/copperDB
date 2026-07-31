use crate::CopperDbError;
use copperdb_config::EffectiveDatabaseConfig;
use copperdb_embed::Embedder;
use copperdb_storage::{NodeRecord, StorageEngine};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingRuntimeState {
    Disabled,
    Cold,
    Ready,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingRuntimeStatus {
    pub state: EmbeddingRuntimeState,
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub worker_count: usize,
    pub pending: u64,
    pub completed: u64,
    pub failed: u64,
    pub last_error: Option<String>,
}

/// Per-database embedding lifecycle owner. This initial runtime performs at
/// most one synchronous drain at a time; background workers come later.
pub(crate) struct EmbeddingRuntime {
    storage: Arc<StorageEngine>,
    provider: String,
    model: String,
    dimensions: usize,
    embedder: Option<Arc<dyn Embedder>>,
    state: Mutex<EmbeddingRuntimeState>,
    process_lock: Mutex<()>,
    completed: AtomicU64,
    failed: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl EmbeddingRuntime {
    pub(crate) fn from_config(
        storage: Arc<StorageEngine>,
        config: &EffectiveDatabaseConfig,
    ) -> Self {
        Self {
            storage,
            provider: config.embedding_provider.clone(),
            model: config.embedding_model.clone(),
            dimensions: config.embedding_dimensions,
            embedder: None,
            state: Mutex::new(if config.embedding_enabled {
                EmbeddingRuntimeState::Cold
            } else {
                EmbeddingRuntimeState::Disabled
            }),
            process_lock: Mutex::new(()),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn ready(
        storage: Arc<StorageEngine>,
        dimensions: usize,
        embedder: Arc<dyn Embedder>,
    ) -> Self {
        Self {
            storage,
            provider: "test".to_string(),
            model: "test-model".to_string(),
            dimensions,
            embedder: Some(embedder),
            state: Mutex::new(EmbeddingRuntimeState::Ready),
            process_lock: Mutex::new(()),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    pub(crate) fn status(&self) -> Result<EmbeddingRuntimeStatus, CopperDbError> {
        Ok(EmbeddingRuntimeStatus {
            state: *self.state.lock().expect("embedding runtime state lock"),
            provider: self.provider.clone(),
            model: self.model.clone(),
            dimensions: self.dimensions,
            worker_count: 0,
            pending: self.storage.pending_embeddings_count()? as u64,
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .expect("embedding runtime error lock")
                .clone(),
        })
    }

    pub(crate) fn drain_one(&self) -> Result<bool, CopperDbError> {
        let _guard = self.process_lock.lock().expect("embedding process lock");
        let Some(embedder) = self.embedder.as_ref().map(Arc::clone) else {
            if *self.state.lock().expect("embedding runtime state lock")
                == EmbeddingRuntimeState::Disabled
            {
                return Ok(false);
            }
            return Err(CopperDbError::Config(
                "embedding runtime is enabled but no provider is loaded".to_string(),
            ));
        };
        let Some(node) = self.storage.find_node_needing_embedding()? else {
            return Ok(false);
        };
        let embeddings = match embedder.embed_batch_blocking(&[canonical_input(&node)]) {
            Ok(embeddings) => embeddings,
            Err(error) => return Err(self.record_failure(error.to_string())),
        };
        let Some(embedding) = embeddings.into_iter().next() else {
            return Err(self.record_failure("embedding provider returned no embedding".to_string()));
        };
        if self.dimensions > 0 && embedding.vector.len() != self.dimensions {
            return Err(self.record_failure(format!(
                "embedding dimensions mismatch: expected {}, got {}",
                self.dimensions,
                embedding.vector.len()
            )));
        }
        let mut update = node;
        update.set_managed_chunk_embeddings(
            vec![embedding.vector],
            Some(embedding.model),
            Some(current_unix_seconds().to_string()),
        );
        self.storage.update_node_embedding(&update)?;
        self.completed.fetch_add(1, Ordering::Relaxed);
        *self.state.lock().expect("embedding runtime state lock") = EmbeddingRuntimeState::Ready;
        *self.last_error.lock().expect("embedding runtime error lock") = None;
        Ok(true)
    }

    fn record_failure(&self, error: String) -> CopperDbError {
        self.failed.fetch_add(1, Ordering::Relaxed);
        *self.state.lock().expect("embedding runtime state lock") = EmbeddingRuntimeState::Degraded;
        *self.last_error.lock().expect("embedding runtime error lock") = Some(error.chars().take(256).collect());
        CopperDbError::Init("embedding generation failed".to_string())
    }
}

fn canonical_input(node: &NodeRecord) -> String {
    json!({ "labels": node.labels, "properties": node.properties }).to_string()
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_embed::{EmbedError, Embedding};
    use copperdb_storage::{NodeEmbeddingMetadata, NodeRecord};
    use std::collections::BTreeMap;

    struct TestEmbedder(Result<Vec<f32>, EmbedError>);

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
        let runtime = EmbeddingRuntime::from_config(storage, &config);
        assert_eq!(runtime.status().unwrap().state, EmbeddingRuntimeState::Disabled);
        assert_eq!(runtime.status().unwrap().worker_count, 0);
        assert!(!runtime.drain_one().unwrap());
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
        assert!(runtime.drain_one().unwrap());
        assert_eq!(storage.pending_embeddings_count().unwrap(), 0);
        assert_eq!(runtime.status().unwrap().completed, 1);

        let mut retry = node();
        retry.id = "node-2".to_string();
        storage.put_node_record(&retry).unwrap();
        let failing = EmbeddingRuntime::ready(
            Arc::clone(&storage),
            2,
            Arc::new(TestEmbedder(Err(EmbedError::LocalModel("unavailable".to_string())))),
        );
        assert!(failing.drain_one().is_err());
        assert_eq!(storage.pending_embeddings_count().unwrap(), 1);
        assert_eq!(failing.status().unwrap().state, EmbeddingRuntimeState::Degraded);
    }
}