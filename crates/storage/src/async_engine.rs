use crate::{
    CommitEventCallback, EdgeAdjacencyDirection, EdgeDeleteCallback, EdgeEventCallback, EdgeRecord,
    MvccLifecycleDebtKey, MvccLifecycleStatus, MvccPruneOptions, MvccSnapshot, MvccSnapshotLease,
    NodeDeleteCallback, NodeEventCallback, NodeRecord, StorageEngine, StorageError,
    StorageEventNotifier,
};
use copperdb_util::RequestCancellation;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEFAULT_ASYNC_FLUSH_INTERVAL_MS: u64 = 50;
const DEFAULT_ASYNC_MIN_FLUSH_INTERVAL_MS: u64 = 10;
const DEFAULT_ASYNC_MAX_FLUSH_INTERVAL_MS: u64 = 200;
const DEFAULT_ASYNC_TARGET_FLUSH_SIZE: usize = 1000;

type PendingNodeOps = Vec<(String, Option<NodeRecord>)>;
type PendingEdgeOps = Vec<(String, Option<EdgeRecord>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncStorageConfig {
    pub flush_interval_ms: u64,
    pub adaptive_flush: bool,
    pub min_flush_interval_ms: u64,
    pub max_flush_interval_ms: u64,
    pub target_flush_size: usize,
    pub max_node_cache_size: usize,
    pub max_edge_cache_size: usize,
}

impl Default for AsyncStorageConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: DEFAULT_ASYNC_FLUSH_INTERVAL_MS,
            adaptive_flush: false,
            min_flush_interval_ms: DEFAULT_ASYNC_MIN_FLUSH_INTERVAL_MS,
            max_flush_interval_ms: DEFAULT_ASYNC_MAX_FLUSH_INTERVAL_MS,
            target_flush_size: DEFAULT_ASYNC_TARGET_FLUSH_SIZE,
            max_node_cache_size: 0,
            max_edge_cache_size: 0,
        }
    }
}

#[derive(Default)]
struct AsyncStorageCallbacks {
    node_created: RwLock<Option<NodeEventCallback>>,
    node_updated: RwLock<Option<NodeEventCallback>>,
    node_deleted: RwLock<Option<NodeDeleteCallback>>,
    edge_created: RwLock<Option<EdgeEventCallback>>,
    edge_updated: RwLock<Option<EdgeEventCallback>>,
    edge_deleted: RwLock<Option<EdgeDeleteCallback>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AsyncFlushResult {
    pub nodes_written: u64,
    pub nodes_deleted: u64,
    pub edges_written: u64,
    pub edges_deleted: u64,
}

impl AsyncFlushResult {
    pub fn is_empty(&self) -> bool {
        self.nodes_written == 0
            && self.nodes_deleted == 0
            && self.edges_written == 0
            && self.edges_deleted == 0
    }
}

#[derive(Debug)]
pub struct AsyncFlushGuard<'a> {
    _guard: RwLockReadGuard<'a, ()>,
}

#[derive(Debug, Default)]
struct PendingState {
    nodes: BTreeMap<String, Option<NodeRecord>>,
    edges: BTreeMap<String, Option<EdgeRecord>>,
    node_label_index: BTreeMap<String, BTreeSet<String>>,
    edge_type_index: BTreeMap<String, BTreeSet<String>>,
    edge_start_index: BTreeMap<String, BTreeSet<String>>,
    edge_end_index: BTreeMap<String, BTreeSet<String>>,
}

impl PendingState {
    fn put_node(&mut self, node: NodeRecord) {
        self.remove_node_index_entry(&node.id);
        self.index_node(&node);
        self.nodes.insert(node.id.clone(), Some(node));
    }

    fn delete_node(&mut self, id: String) {
        self.remove_node_index_entry(&id);
        self.nodes.insert(id, None);
    }

    fn put_edge(&mut self, edge: EdgeRecord) {
        self.remove_edge_index_entry(&edge.id);
        self.index_edge(&edge);
        self.edges.insert(edge.id.clone(), Some(edge));
    }

    fn delete_edge(&mut self, id: String) {
        self.remove_edge_index_entry(&id);
        self.edges.insert(id, None);
    }

    fn take_ops(&mut self) -> (PendingNodeOps, PendingEdgeOps) {
        let nodes = mem::take(&mut self.nodes).into_iter().collect();
        let edges = mem::take(&mut self.edges).into_iter().collect();
        self.node_label_index.clear();
        self.edge_type_index.clear();
        self.edge_start_index.clear();
        self.edge_end_index.clear();
        (nodes, edges)
    }

    fn requeue_node_ops(&mut self, ops: PendingNodeOps) {
        for (id, pending) in ops {
            match pending {
                Some(node) => self.put_node(node),
                None => self.delete_node(id),
            }
        }
    }

    fn requeue_edge_ops(&mut self, ops: PendingEdgeOps) {
        for (id, pending) in ops {
            match pending {
                Some(edge) => self.put_edge(edge),
                None => self.delete_edge(id),
            }
        }
    }

    fn pending_node(&self, id: &str) -> Option<Option<NodeRecord>> {
        self.nodes.get(id).cloned()
    }

    fn pending_edge(&self, id: &str) -> Option<Option<EdgeRecord>> {
        self.edges.get(id).cloned()
    }

    fn node_ids_for_label(&self, label: &str) -> Vec<String> {
        self.node_label_index
            .get(label)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn edge_ids_for_type(&self, edge_type: &str) -> Vec<String> {
        self.edge_type_index
            .get(edge_type)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn pending_nodes_iter(&self) -> impl Iterator<Item = (&String, &Option<NodeRecord>)> {
        self.nodes.iter()
    }

    fn pending_edges_iter(&self) -> impl Iterator<Item = (&String, &Option<EdgeRecord>)> {
        self.edges.iter()
    }

    fn edge_ids_from_start(&self, node_id: &str) -> Vec<String> {
        self.edge_start_index
            .get(node_id)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn edge_ids_to_end(&self, node_id: &str) -> Vec<String> {
        self.edge_end_index
            .get(node_id)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn index_node(&mut self, node: &NodeRecord) {
        for label in &node.labels {
            self.node_label_index
                .entry(label.clone())
                .or_default()
                .insert(node.id.clone());
        }
    }

    fn remove_node_index_entry(&mut self, id: &str) {
        let labels = self
            .nodes
            .get(id)
            .and_then(|pending| pending.as_ref())
            .map(|node| node.labels.clone())
            .unwrap_or_default();
        for label in labels {
            let remove_label = if let Some(ids) = self.node_label_index.get_mut(&label) {
                ids.remove(id);
                ids.is_empty()
            } else {
                false
            };
            if remove_label {
                self.node_label_index.remove(&label);
            }
        }
    }

    fn index_edge(&mut self, edge: &EdgeRecord) {
        self.edge_type_index
            .entry(edge.edge_type.clone())
            .or_default()
            .insert(edge.id.clone());
        self.edge_start_index
            .entry(edge.start_node.clone())
            .or_default()
            .insert(edge.id.clone());
        self.edge_end_index
            .entry(edge.end_node.clone())
            .or_default()
            .insert(edge.id.clone());
    }

    fn remove_edge_index_entry(&mut self, id: &str) {
        let edge = self
            .edges
            .get(id)
            .and_then(|pending| pending.as_ref())
            .cloned();
        if let Some(edge) = edge {
            let remove_type = if let Some(ids) = self.edge_type_index.get_mut(&edge.edge_type) {
                ids.remove(id);
                ids.is_empty()
            } else {
                false
            };
            if remove_type {
                self.edge_type_index.remove(&edge.edge_type);
            }

            let remove_start = if let Some(ids) = self.edge_start_index.get_mut(&edge.start_node) {
                ids.remove(id);
                ids.is_empty()
            } else {
                false
            };
            if remove_start {
                self.edge_start_index.remove(&edge.start_node);
            }

            let remove_end = if let Some(ids) = self.edge_end_index.get_mut(&edge.end_node) {
                ids.remove(id);
                ids.is_empty()
            } else {
                false
            };
            if remove_end {
                self.edge_end_index.remove(&edge.end_node);
            }
        }
    }
}

#[derive(Debug)]
struct AsyncStorageShared {
    pending: Mutex<PendingState>,
    pending_embeddings: Mutex<BTreeSet<String>>,
    flush_lock: RwLock<()>,
    last_flush_at: Mutex<Instant>,
}

impl Default for AsyncStorageShared {
    fn default() -> Self {
        Self {
            pending: Mutex::new(PendingState::default()),
            pending_embeddings: Mutex::new(BTreeSet::new()),
            flush_lock: RwLock::new(()),
            last_flush_at: Mutex::new(Instant::now()),
        }
    }
}

impl AsyncStorageShared {
    fn pending_node_count(&self) -> usize {
        self.pending.lock().nodes.len()
    }

    fn pending_edge_count(&self) -> usize {
        self.pending.lock().edges.len()
    }

    fn pending_write_count(&self) -> usize {
        let pending = self.pending.lock();
        pending.nodes.len() + pending.edges.len()
    }

    fn pending_embeddings_count(&self) -> usize {
        self.pending_embeddings.lock().len()
    }

    fn first_pending_embedding(&self) -> Option<String> {
        self.pending_embeddings.lock().iter().next().cloned()
    }

    fn remove_pending_embedding(&self, id: &str) {
        self.pending_embeddings.lock().remove(id);
    }

    fn update_pending_embedding_for_node(&self, node: &NodeRecord) {
        let mut pending_embeddings = self.pending_embeddings.lock();
        if node.needs_embedding() {
            pending_embeddings.insert(node.id.clone());
        } else {
            pending_embeddings.remove(&node.id);
        }
    }

    fn replace_pending_embeddings(&self, ids: BTreeSet<String>) -> usize {
        let mut pending_embeddings = self.pending_embeddings.lock();
        *pending_embeddings = ids;
        pending_embeddings.len()
    }

    fn last_flush_at(&self) -> Instant {
        *self.last_flush_at.lock()
    }

    fn record_flush_at(&self, when: Instant) {
        *self.last_flush_at.lock() = when;
    }

    fn flush_pending(&self, engine: &StorageEngine) -> Result<AsyncFlushResult, StorageError> {
        let _flush_guard = self.flush_lock.write();
        let result = self.flush_pending_locked(engine);
        if result.is_ok() {
            self.record_flush_at(Instant::now());
        }
        result
    }

    fn try_flush_pending(
        &self,
        engine: &StorageEngine,
    ) -> Result<Option<AsyncFlushResult>, StorageError> {
        let Some(_flush_guard) = self.flush_lock.try_write() else {
            return Ok(None);
        };
        let result = self.flush_pending_locked(engine);
        if result.is_ok() {
            self.record_flush_at(Instant::now());
        }
        result.map(Some)
    }

    fn flush_pending_locked(
        &self,
        engine: &StorageEngine,
    ) -> Result<AsyncFlushResult, StorageError> {
        let (node_ops, edge_ops) = self.pending.lock().take_ops();
        let result = AsyncFlushResult {
            nodes_written: node_ops
                .iter()
                .filter(|(_, pending)| pending.is_some())
                .count() as u64,
            nodes_deleted: node_ops
                .iter()
                .filter(|(_, pending)| pending.is_none())
                .count() as u64,
            edges_written: edge_ops
                .iter()
                .filter(|(_, pending)| pending.is_some())
                .count() as u64,
            edges_deleted: edge_ops
                .iter()
                .filter(|(_, pending)| pending.is_none())
                .count() as u64,
        };

        if let Err(error) = engine.batch_write(|batch| {
            for (id, pending) in &node_ops {
                match pending {
                    Some(node) => batch.put_node_record(node),
                    None => batch.delete_node_record(id),
                }
            }
            for (id, pending) in &edge_ops {
                match pending {
                    Some(edge) => batch.put_edge_record(edge),
                    None => batch.delete_edge_record(id),
                }
            }
            Ok::<_, StorageError>(())
        }) {
            self.requeue_node_ops(node_ops);
            self.requeue_edge_ops(edge_ops);
            return Err(error);
        }

        engine.flush()?;
        Ok(result)
    }

    fn requeue_node_ops(&self, ops: PendingNodeOps) {
        if ops.is_empty() {
            return;
        }
        self.pending.lock().requeue_node_ops(ops);
    }

    fn requeue_edge_ops(&self, ops: PendingEdgeOps) {
        if ops.is_empty() {
            return;
        }
        self.pending.lock().requeue_edge_ops(ops);
    }
}

enum WorkerRequest {
    GetNodeRecord {
        id: String,
        reply: Sender<Result<Option<NodeRecord>, StorageError>>,
    },
    GetNodesByLabel {
        label: String,
        reply: Sender<Result<Vec<NodeRecord>, StorageError>>,
    },
    GetNodeRecordsByPrefix {
        prefix: String,
        reply: Sender<Result<Vec<NodeRecord>, StorageError>>,
    },
    AllNodeRecords {
        reply: Sender<Result<Vec<NodeRecord>, StorageError>>,
    },
    NodeCountByPrefix {
        prefix: String,
        reply: Sender<Result<u64, StorageError>>,
    },
    NodeCountByLabelInNamespace {
        namespace: String,
        label: String,
        reply: Sender<Result<u64, StorageError>>,
    },
    GetEdgeRecord {
        id: String,
        reply: Sender<Result<Option<EdgeRecord>, StorageError>>,
    },
    GetEdgesByType {
        edge_type: String,
        reply: Sender<Result<Vec<EdgeRecord>, StorageError>>,
    },
    AllEdges {
        reply: Sender<Result<Vec<EdgeRecord>, StorageError>>,
    },
    GetAdjacentEdges {
        node_id: String,
        direction: EdgeAdjacencyDirection,
        edge_type: Option<String>,
        reply: Sender<Result<Vec<EdgeRecord>, StorageError>>,
    },
    EdgeCountByPrefix {
        prefix: String,
        reply: Sender<Result<u64, StorageError>>,
    },
    BeginMvccSnapshot {
        reply: Sender<MvccSnapshot>,
    },
    BeginRegisteredMvccSnapshot {
        reply: Sender<MvccSnapshotLease>,
    },
    GetNodeRecordVisibleAt {
        snapshot: MvccSnapshot,
        id: String,
        reply: Sender<Result<Option<NodeRecord>, StorageError>>,
    },
    GetNodesByLabelVisibleAt {
        snapshot: MvccSnapshot,
        label: String,
        reply: Sender<Result<Vec<NodeRecord>, StorageError>>,
    },
    GetEdgeRecordVisibleAt {
        snapshot: MvccSnapshot,
        id: String,
        reply: Sender<Result<Option<EdgeRecord>, StorageError>>,
    },
    GetEdgesByTypeVisibleAt {
        snapshot: MvccSnapshot,
        edge_type: String,
        reply: Sender<Result<Vec<EdgeRecord>, StorageError>>,
    },
    RebuildMvccFromCurrentState {
        reply: Sender<Result<(), StorageError>>,
    },
    PruneMvccVersions {
        opts: MvccPruneOptions,
        reply: Sender<usize>,
    },
    LifecycleStatus {
        reply: Sender<MvccLifecycleStatus>,
    },
    TriggerPruneNow {
        retain_last_n_versions: u64,
        reply: Sender<usize>,
    },
    PauseLifecycle,
    ResumeLifecycle,
    SetLifecycleScheduleMs {
        interval_ms: u64,
    },
    TopLifecycleDebtKeys {
        limit: usize,
        reply: Sender<Vec<MvccLifecycleDebtKey>>,
    },
    Flush {
        reply: Sender<Result<AsyncFlushResult, StorageError>>,
    },
    TryFlush {
        reply: Sender<Result<Option<AsyncFlushResult>, StorageError>>,
    },
    Close {
        reply: Sender<Result<AsyncFlushResult, StorageError>>,
    },
    EnqueueDeindex {
        id: String,
        reply: Sender<Result<(), StorageError>>,
    },
    DrainDeindex {
        reply: Sender<Result<usize, StorageError>>,
    },
    PendingDeindexCount {
        reply: Sender<Result<usize, StorageError>>,
    },
    RegisterCommitCompleted {
        callback: CommitEventCallback,
    },
}

pub struct AsyncStorageEngine {
    shared: Arc<AsyncStorageShared>,
    callbacks: AsyncStorageCallbacks,
    worker_tx: Mutex<Option<Sender<WorkerRequest>>>,
    worker_handle: Mutex<Option<JoinHandle<()>>>,
    config: AsyncStorageConfig,
}

impl AsyncStorageEngine {
    pub fn new(engine: StorageEngine, config: Option<AsyncStorageConfig>) -> Self {
        let config = config.unwrap_or_default();
        let initial_pending_embeddings = engine
            .refresh_pending_embeddings_index()
            .and_then(|_| {
                engine
                    .pending_embedding_ids()
                    .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .unwrap_or_default();
        let shared = Arc::new(AsyncStorageShared {
            pending: Mutex::new(PendingState::default()),
            pending_embeddings: Mutex::new(initial_pending_embeddings),
            flush_lock: RwLock::new(()),
            last_flush_at: Mutex::new(Instant::now()),
        });
        let (worker_tx, worker_rx) = mpsc::channel();
        let worker_shared = Arc::clone(&shared);
        let worker_handle =
            thread::spawn(move || worker_loop(engine, worker_shared, worker_rx, config));

        Self {
            shared,
            callbacks: AsyncStorageCallbacks::default(),
            worker_tx: Mutex::new(Some(worker_tx)),
            worker_handle: Mutex::new(Some(worker_handle)),
            config,
        }
    }

    pub fn config(&self) -> AsyncStorageConfig {
        self.config
    }

    pub fn hold_flush(&self) -> AsyncFlushGuard<'_> {
        AsyncFlushGuard {
            _guard: self.shared.flush_lock.read(),
        }
    }

    pub fn flush(&self) -> Result<AsyncFlushResult, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::Flush { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn try_flush(&self) -> Result<Option<AsyncFlushResult>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::TryFlush { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn close(&self) -> Result<AsyncFlushResult, StorageError> {
        let Some(worker_tx) = self.worker_tx.lock().take() else {
            return Ok(AsyncFlushResult::default());
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        worker_tx
            .send(WorkerRequest::Close { reply: reply_tx })
            .map_err(|_| StorageError::AsyncEngineClosed)?;
        let result = self.recv_result(reply_rx);
        self.join_worker();
        result
    }

    fn flush_if_node_cache_full(&self) -> Result<(), StorageError> {
        if self.config.max_node_cache_size == 0 {
            return Ok(());
        }
        if self.shared.pending_node_count() >= self.config.max_node_cache_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush_if_edge_cache_full(&self) -> Result<(), StorageError> {
        if self.config.max_edge_cache_size == 0 {
            return Ok(());
        }
        if self.shared.pending_edge_count() >= self.config.max_edge_cache_size {
            self.flush()?;
        }
        Ok(())
    }

    fn notify_node_deleted(&self, id: &str) {
        if let Some(callback) = self.callbacks.node_deleted.read().clone() {
            callback(id.to_string());
        }
    }

    fn notify_edge_deleted(&self, id: &str) {
        if let Some(callback) = self.callbacks.edge_deleted.read().clone() {
            callback(id.to_string());
        }
    }

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<(), StorageError> {
        self.flush_if_node_cache_full()?;
        self.shared.pending.lock().put_node(node.clone());
        self.shared.update_pending_embedding_for_node(node);
        Ok(())
    }

    pub fn delete_node_record(&self, id: &str) -> Result<(), StorageError> {
        self.flush_if_node_cache_full()?;
        self.shared.remove_pending_embedding(id);
        let should_notify = {
            let _flush_guard = self.shared.flush_lock.read();
            let had_pending_node = {
                let mut pending = self.shared.pending.lock();
                let present = matches!(pending.pending_node(id), Some(Some(_)));
                pending.delete_node(id.to_string());
                present
            };
            had_pending_node && self.get_persisted_node_record(id)?.is_none()
        };
        if should_notify {
            self.notify_node_deleted(id);
        }
        Ok(())
    }

    pub fn update_node_embedding(&self, node: &NodeRecord) -> Result<(), StorageError> {
        let mut existing = self
            .get_node_record(&node.id)?
            .ok_or_else(|| StorageError::NotFound(node.id.clone()))?;
        existing.chunk_embeddings = node.chunk_embeddings.clone();
        existing.embed_meta = node.embed_meta.clone();
        existing.updated_at_unix_ms = existing.updated_at_unix_ms.max(node.updated_at_unix_ms);
        self.put_node_record(&existing)
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        if let Some(pending) = self.shared.pending.lock().pending_node(id) {
            return Ok(pending);
        }
        self.get_persisted_node_record(id)
    }

    pub fn get_node_record_latest_effective(
        &self,
        id: &str,
    ) -> Result<Option<NodeRecord>, StorageError> {
        self.get_node_record(id)
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut nodes = self
            .get_persisted_nodes_by_label(label)?
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect::<BTreeMap<_, _>>();
        let pending = self.shared.pending.lock();
        let matching_ids = pending
            .node_ids_for_label(label)
            .into_iter()
            .collect::<BTreeSet<_>>();
        for id in pending.node_ids_for_label(label) {
            if let Some(Some(node)) = pending.pending_node(&id) {
                nodes.insert(id, node);
            }
        }
        for (id, pending_node) in pending.pending_nodes_iter() {
            if pending_node.is_none() || !matching_ids.contains(id) {
                nodes.remove(id);
            }
        }
        Ok(nodes.into_values().collect())
    }

    pub fn all_nodes(&self) -> Result<Vec<NodeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut nodes = self
            .get_persisted_all_node_records()?
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect::<BTreeMap<_, _>>();
        let pending = self.shared.pending.lock();
        for (id, pending_node) in pending.pending_nodes_iter() {
            match pending_node {
                Some(node) => {
                    nodes.insert(id.clone(), node.clone());
                }
                None => {
                    nodes.remove(id);
                }
            }
        }
        Ok(nodes.into_values().collect())
    }

    pub fn find_node_needing_embedding(&self) -> Result<Option<NodeRecord>, StorageError> {
        loop {
            let Some(id) = self.shared.first_pending_embedding() else {
                return Ok(None);
            };
            let Some(node) = self.get_node_record(&id)? else {
                self.shared.remove_pending_embedding(&id);
                continue;
            };
            if node.needs_embedding() {
                return Ok(Some(node));
            }
            self.shared.remove_pending_embedding(&id);
        }
    }

    pub fn refresh_pending_embeddings_index(&self) -> Result<usize, StorageError> {
        let ids = self
            .all_nodes()?
            .into_iter()
            .filter(|node| node.needs_embedding())
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        Ok(self.shared.replace_pending_embeddings(ids))
    }

    pub fn mark_node_embedded(&self, id: &str) {
        self.shared.remove_pending_embedding(id);
    }

    pub fn add_to_pending_embeddings(&self, id: &str) -> Result<(), StorageError> {
        let Some(node) = self.get_node_record(id)? else {
            return Ok(());
        };
        if node.needs_embedding() {
            self.shared.update_pending_embedding_for_node(&node);
        }
        Ok(())
    }

    pub fn pending_embeddings_count(&self) -> usize {
        self.shared.pending_embeddings_count()
    }

    pub fn enqueue_deindex_work(&self, entity_id: &str) -> Result<(), StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::EnqueueDeindex {
            id: entity_id.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn drain_deindex_work(&self) -> Result<usize, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::DrainDeindex { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn pending_deindex_count(&self) -> Result<usize, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::PendingDeindexCount { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn stream_node_records<F>(&self, visit: F) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        self.stream_node_records_with_cancellation(&RequestCancellation::new(), visit)
    }

    pub fn stream_node_records_with_cancellation<F>(
        &self,
        cancel: &RequestCancellation,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        let mut streamed = 0;
        for node in self.all_nodes()? {
            cancel.check_cancelled()?;
            match visit(node) {
                Ok(()) => streamed += 1,
                Err(StorageError::IterationStopped) => {
                    streamed += 1;
                    return Ok(streamed);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(streamed)
    }

    pub fn stream_node_records_by_prefix<F>(
        &self,
        prefix: &str,
        visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        self.stream_node_records_by_prefix_with_cancellation(
            prefix,
            &RequestCancellation::new(),
            visit,
        )
    }

    pub fn stream_node_records_by_prefix_with_cancellation<F>(
        &self,
        prefix: &str,
        cancel: &RequestCancellation,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        let _flush_guard = self.shared.flush_lock.read();
        let mut nodes = self
            .get_persisted_node_records_by_prefix(prefix)?
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect::<BTreeMap<_, _>>();
        let pending = self.shared.pending.lock();
        for (id, pending_node) in pending.pending_nodes_iter() {
            if !id.starts_with(prefix) {
                continue;
            }
            match pending_node {
                Some(node) => {
                    nodes.insert(id.clone(), node.clone());
                }
                None => {
                    nodes.remove(id);
                }
            }
        }

        let mut streamed = 0;
        for node in nodes.into_values() {
            cancel.check_cancelled()?;
            match visit(node) {
                Ok(()) => streamed += 1,
                Err(StorageError::IterationStopped) => {
                    streamed += 1;
                    return Ok(streamed);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(streamed)
    }

    pub fn stream_node_record_chunks<F>(
        &self,
        chunk_size: usize,
        visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(&[NodeRecord]) -> Result<(), StorageError>,
    {
        self.stream_node_record_chunks_with_cancellation(
            chunk_size,
            &RequestCancellation::new(),
            visit,
        )
    }

    pub fn stream_node_record_chunks_with_cancellation<F>(
        &self,
        chunk_size: usize,
        cancel: &RequestCancellation,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(&[NodeRecord]) -> Result<(), StorageError>,
    {
        if chunk_size == 0 {
            return Err(StorageError::InvalidChunkSize(0));
        }

        let mut chunk = Vec::with_capacity(chunk_size);
        let mut stop_requested = false;
        let mut streamed = 0;
        let result = self.stream_node_records_with_cancellation(cancel, |node| {
            chunk.push(node);
            streamed += 1;
            if chunk.len() == chunk_size {
                cancel.check_cancelled()?;
                match visit(&chunk) {
                    Ok(()) => {
                        chunk.clear();
                    }
                    Err(StorageError::IterationStopped) => {
                        stop_requested = true;
                        return Err(StorageError::IterationStopped);
                    }
                    Err(err) => return Err(err),
                }
            }
            Ok(())
        });

        match result {
            Ok(_) => {}
            Err(StorageError::IterationStopped) => {}
            Err(err) => return Err(err),
        }

        if !stop_requested && !chunk.is_empty() {
            cancel.check_cancelled()?;
            match visit(&chunk) {
                Ok(()) | Err(StorageError::IterationStopped) => {}
                Err(err) => return Err(err),
            }
        }

        Ok(streamed)
    }

    pub fn node_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut count = self.get_persisted_node_count_by_prefix(prefix)? as i64;
        for (id, pending) in self.shared.pending.lock().pending_nodes_iter() {
            let persisted = self.get_persisted_node_record(id)?;
            let persisted_matches = persisted
                .as_ref()
                .is_some_and(|node| node.id.starts_with(prefix));
            let pending_matches = pending
                .as_ref()
                .is_some_and(|node| node.id.starts_with(prefix));
            count += match (persisted_matches, pending_matches) {
                (false, true) => 1,
                (true, false) => -1,
                _ => 0,
            };
        }
        Ok(count.max(0) as u64)
    }

    pub fn node_count_by_label_in_namespace(
        &self,
        namespace: &str,
        label: &str,
    ) -> Result<u64, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut count =
            self.get_persisted_node_count_by_label_in_namespace(namespace, label)? as i64;
        for (id, pending) in self.shared.pending.lock().pending_nodes_iter() {
            let persisted = self.get_persisted_node_record(id)?;
            let persisted_matches = persisted.as_ref().is_some_and(|node| {
                namespace_from_id(&node.id) == Some(namespace)
                    && node.labels.iter().any(|existing| existing == label)
            });
            let pending_matches = pending.as_ref().is_some_and(|node| {
                namespace_from_id(&node.id) == Some(namespace)
                    && node.labels.iter().any(|existing| existing == label)
            });
            count += match (persisted_matches, pending_matches) {
                (false, true) => 1,
                (true, false) => -1,
                _ => 0,
            };
        }
        Ok(count.max(0) as u64)
    }

    pub fn put_edge_record(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.flush_if_edge_cache_full()?;
        self.shared.pending.lock().put_edge(edge.clone());
        Ok(())
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<(), StorageError> {
        self.flush_if_edge_cache_full()?;
        let should_notify = {
            let _flush_guard = self.shared.flush_lock.read();
            let had_pending_edge = {
                let mut pending = self.shared.pending.lock();
                let present = matches!(pending.pending_edge(id), Some(Some(_)));
                pending.delete_edge(id.to_string());
                present
            };
            had_pending_edge && self.get_persisted_edge_record(id)?.is_none()
        };
        if should_notify {
            self.notify_edge_deleted(id);
        }
        Ok(())
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        if let Some(pending) = self.shared.pending.lock().pending_edge(id) {
            return Ok(pending);
        }
        self.get_persisted_edge_record(id)
    }

    pub fn get_edge_record_latest_effective(
        &self,
        id: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        self.get_edge_record(id)
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut edges = self
            .get_persisted_edges_by_type(edge_type)?
            .into_iter()
            .map(|edge| (edge.id.clone(), edge))
            .collect::<BTreeMap<_, _>>();
        let pending = self.shared.pending.lock();
        let matching_ids = pending
            .edge_ids_for_type(edge_type)
            .into_iter()
            .collect::<BTreeSet<_>>();
        for id in pending.edge_ids_for_type(edge_type) {
            if let Some(Some(edge)) = pending.pending_edge(&id) {
                edges.insert(id, edge);
            }
        }
        for (id, pending_edge) in pending.pending_edges_iter() {
            if pending_edge.is_none() || !matching_ids.contains(id) {
                edges.remove(id);
            }
        }
        Ok(edges.into_values().collect())
    }

    pub fn all_edges(&self) -> Result<Vec<EdgeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut edges = self
            .get_persisted_all_edges()?
            .into_iter()
            .map(|edge| (edge.id.clone(), edge))
            .collect::<BTreeMap<_, _>>();
        let pending = self.shared.pending.lock();
        for (id, pending_edge) in pending.pending_edges_iter() {
            match pending_edge {
                Some(edge) => {
                    edges.insert(id.clone(), edge.clone());
                }
                None => {
                    edges.remove(id);
                }
            }
        }
        Ok(edges.into_values().collect())
    }

    pub fn stream_edge_records<F>(&self, visit: F) -> Result<u64, StorageError>
    where
        F: FnMut(EdgeRecord) -> Result<(), StorageError>,
    {
        self.stream_edge_records_with_cancellation(&RequestCancellation::new(), visit)
    }

    pub fn stream_edge_records_with_cancellation<F>(
        &self,
        cancel: &RequestCancellation,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(EdgeRecord) -> Result<(), StorageError>,
    {
        let mut streamed = 0;
        for edge in self.all_edges()? {
            cancel.check_cancelled()?;
            match visit(edge) {
                Ok(()) => streamed += 1,
                Err(StorageError::IterationStopped) => {
                    streamed += 1;
                    return Ok(streamed);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(streamed)
    }

    pub fn get_edges_from_node(&self, node_id: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_adjacent_edges(node_id, EdgeAdjacencyDirection::Outgoing, None)
    }

    pub fn get_edges_from_node_by_type(
        &self,
        node_id: &str,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_adjacent_edges(node_id, EdgeAdjacencyDirection::Outgoing, Some(edge_type))
    }

    pub fn get_edges_to_node(&self, node_id: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_adjacent_edges(node_id, EdgeAdjacencyDirection::Incoming, None)
    }

    pub fn get_edges_to_node_by_type(
        &self,
        node_id: &str,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_adjacent_edges(node_id, EdgeAdjacencyDirection::Incoming, Some(edge_type))
    }

    pub fn get_adjacent_edges(
        &self,
        node_id: &str,
        direction: EdgeAdjacencyDirection,
        edge_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut edges = self
            .get_persisted_adjacent_edges(node_id, direction, edge_type)?
            .into_iter()
            .map(|edge| (edge.id.clone(), edge))
            .collect::<BTreeMap<_, _>>();
        let pending = self.shared.pending.lock();
        let matching_ids = pending_adjacent_edge_ids(&pending, node_id, direction, edge_type)
            .into_iter()
            .collect::<BTreeSet<_>>();

        for id in &matching_ids {
            if let Some(Some(edge)) = pending.pending_edge(id) {
                edges.insert(id.clone(), edge);
            }
        }

        let persisted_ids = edges.keys().cloned().collect::<Vec<_>>();
        for id in persisted_ids {
            if let Some(pending_edge) = pending.pending_edge(&id) {
                match pending_edge {
                    Some(edge) if edge_matches_adjacency(&edge, node_id, direction, edge_type) => {
                        edges.insert(id, edge);
                    }
                    Some(_) | None => {
                        edges.remove(&id);
                    }
                }
            }
        }

        Ok(edges.into_values().collect())
    }

    pub fn edge_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let _flush_guard = self.shared.flush_lock.read();
        let mut count = self.get_persisted_edge_count_by_prefix(prefix)? as i64;
        for (id, pending) in self.shared.pending.lock().pending_edges_iter() {
            let persisted = self.get_persisted_edge_record(id)?;
            let persisted_matches = persisted
                .as_ref()
                .is_some_and(|edge| edge.id.starts_with(prefix));
            let pending_matches = pending
                .as_ref()
                .is_some_and(|edge| edge.id.starts_with(prefix));
            count += match (persisted_matches, pending_matches) {
                (false, true) => 1,
                (true, false) => -1,
                _ => 0,
            };
        }
        Ok(count.max(0) as u64)
    }

    pub fn begin_mvcc_snapshot(&self) -> MvccSnapshot {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::BeginMvccSnapshot { reply: reply_tx })
            .expect("async storage worker closed while beginning snapshot");
        reply_rx
            .recv()
            .expect("async storage worker closed while receiving snapshot")
    }

    pub fn begin_registered_mvcc_snapshot(&self) -> MvccSnapshotLease {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::BeginRegisteredMvccSnapshot { reply: reply_tx })
            .expect("async storage worker closed while beginning registered snapshot");
        reply_rx
            .recv()
            .expect("async storage worker closed while receiving registered snapshot")
    }

    pub fn get_node_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<NodeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetNodeRecordVisibleAt {
            snapshot: snapshot.clone(),
            id: id.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_nodes_by_label_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        label: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetNodesByLabelVisibleAt {
            snapshot: snapshot.clone(),
            label: label.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_edge_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetEdgeRecordVisibleAt {
            snapshot: snapshot.clone(),
            id: id.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_edges_by_type_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetEdgesByTypeVisibleAt {
            snapshot: snapshot.clone(),
            edge_type: edge_type.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn rebuild_mvcc_from_current_state(&self) -> Result<(), StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::RebuildMvccFromCurrentState { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn prune_mvcc_versions(&self, opts: MvccPruneOptions) -> usize {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::PruneMvccVersions {
            opts,
            reply: reply_tx,
        })
        .expect("async storage worker closed while pruning mvcc versions");
        reply_rx
            .recv()
            .expect("async storage worker closed while receiving prune result")
    }

    pub fn lifecycle_status(&self) -> MvccLifecycleStatus {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::LifecycleStatus { reply: reply_tx })
            .expect("async storage worker closed while loading lifecycle status");
        reply_rx
            .recv()
            .expect("async storage worker closed while receiving lifecycle status")
    }

    pub fn trigger_prune_now(&self, retain_last_n_versions: u64) -> usize {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::TriggerPruneNow {
            retain_last_n_versions,
            reply: reply_tx,
        })
        .expect("async storage worker closed while triggering prune");
        reply_rx
            .recv()
            .expect("async storage worker closed while receiving prune result")
    }

    pub fn pause_lifecycle(&self) {
        let _ = self.send_request(WorkerRequest::PauseLifecycle);
    }

    pub fn resume_lifecycle(&self) {
        let _ = self.send_request(WorkerRequest::ResumeLifecycle);
    }

    pub fn set_lifecycle_schedule_ms(&self, interval_ms: u64) {
        let _ = self.send_request(WorkerRequest::SetLifecycleScheduleMs { interval_ms });
    }

    pub fn top_lifecycle_debt_keys(&self, limit: usize) -> Vec<MvccLifecycleDebtKey> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::TopLifecycleDebtKeys {
            limit,
            reply: reply_tx,
        })
        .expect("async storage worker closed while loading lifecycle debt");
        reply_rx
            .recv()
            .expect("async storage worker closed while receiving lifecycle debt")
    }

    pub fn get_persisted_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetNodeRecord {
            id: id.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_nodes_by_label(
        &self,
        label: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetNodesByLabel {
            label: label.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_node_records_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetNodeRecordsByPrefix {
            prefix: prefix.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_all_node_records(&self) -> Result<Vec<NodeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::AllNodeRecords { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_node_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::NodeCountByPrefix {
            prefix: prefix.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_node_count_by_label_in_namespace(
        &self,
        namespace: &str,
        label: &str,
    ) -> Result<u64, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::NodeCountByLabelInNamespace {
            namespace: namespace.to_string(),
            label: label.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetEdgeRecord {
            id: id.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_edges_by_type(
        &self,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetEdgesByType {
            edge_type: edge_type.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_all_edges(&self) -> Result<Vec<EdgeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::AllEdges { reply: reply_tx })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_adjacent_edges(
        &self,
        node_id: &str,
        direction: EdgeAdjacencyDirection,
        edge_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::GetAdjacentEdges {
            node_id: node_id.to_string(),
            direction,
            edge_type: edge_type.map(ToOwned::to_owned),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    pub fn get_persisted_edge_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_request(WorkerRequest::EdgeCountByPrefix {
            prefix: prefix.to_string(),
            reply: reply_tx,
        })?;
        self.recv_result(reply_rx)
    }

    fn send_request(&self, request: WorkerRequest) -> Result<(), StorageError> {
        let worker_tx = self
            .worker_tx
            .lock()
            .as_ref()
            .cloned()
            .ok_or(StorageError::AsyncEngineClosed)?;
        worker_tx
            .send(request)
            .map_err(|_| StorageError::AsyncEngineClosed)
    }

    fn recv_result<T>(
        &self,
        reply_rx: Receiver<Result<T, StorageError>>,
    ) -> Result<T, StorageError> {
        reply_rx
            .recv()
            .map_err(|_| StorageError::AsyncEngineClosed)?
    }

    fn join_worker(&self) {
        if let Some(handle) = self.worker_handle.lock().take() {
            let _ = handle.join();
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_node_ids_for_label(&self, label: &str) -> Vec<String> {
        self.shared.pending.lock().node_ids_for_label(label)
    }

    #[cfg(test)]
    pub(crate) fn pending_edge_ids_for_type(&self, edge_type: &str) -> Vec<String> {
        self.shared.pending.lock().edge_ids_for_type(edge_type)
    }

    #[cfg(test)]
    pub(crate) fn pending_edge_ids_from_start(&self, node_id: &str) -> Vec<String> {
        self.shared.pending.lock().edge_ids_from_start(node_id)
    }

    #[cfg(test)]
    pub(crate) fn pending_edge_ids_to_end(&self, node_id: &str) -> Vec<String> {
        self.shared.pending.lock().edge_ids_to_end(node_id)
    }
}

impl StorageEventNotifier for AsyncStorageEngine {
    fn on_node_created(&self, callback: NodeEventCallback) {
        *self.callbacks.node_created.write() = Some(callback);
    }

    fn on_node_updated(&self, callback: NodeEventCallback) {
        *self.callbacks.node_updated.write() = Some(callback);
    }

    fn on_node_deleted(&self, callback: NodeDeleteCallback) {
        *self.callbacks.node_deleted.write() = Some(callback);
    }

    fn on_edge_created(&self, callback: EdgeEventCallback) {
        *self.callbacks.edge_created.write() = Some(callback);
    }

    fn on_edge_updated(&self, callback: EdgeEventCallback) {
        *self.callbacks.edge_updated.write() = Some(callback);
    }

    fn on_edge_deleted(&self, callback: EdgeDeleteCallback) {
        *self.callbacks.edge_deleted.write() = Some(callback);
    }

    fn on_commit_completed(&self, callback: CommitEventCallback) {
        let _ = self.send_request(WorkerRequest::RegisterCommitCompleted { callback });
    }
}

impl Drop for AsyncStorageEngine {
    fn drop(&mut self) {
        let Some(worker_tx) = self.worker_tx.get_mut().take() else {
            return;
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        let _ = worker_tx.send(WorkerRequest::Close { reply: reply_tx });
        let _ = reply_rx.recv();
        if let Some(handle) = self.worker_handle.get_mut().take() {
            let _ = handle.join();
        }
    }
}

fn worker_loop(
    engine: StorageEngine,
    shared: Arc<AsyncStorageShared>,
    worker_rx: Receiver<WorkerRequest>,
    config: AsyncStorageConfig,
) {
    let tick_interval = auto_flush_tick_interval(config);
    let mut next_auto_flush = Instant::now() + tick_interval;
    let mut last_lifecycle_prune = Instant::now();
    loop {
        let timeout = next_auto_flush.saturating_duration_since(Instant::now());
        match worker_rx.recv_timeout(timeout) {
            Ok(request) => {
                if handle_request(&engine, &shared, request) {
                    break;
                }
                maybe_run_lifecycle_prune(&engine, &mut last_lifecycle_prune);
                if Instant::now() >= next_auto_flush {
                    maybe_auto_flush(&engine, &shared, config);
                    next_auto_flush = Instant::now() + tick_interval;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                maybe_auto_flush(&engine, &shared, config);
                maybe_run_lifecycle_prune(&engine, &mut last_lifecycle_prune);
                next_auto_flush = Instant::now() + tick_interval;
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = shared.flush_pending(&engine);
                break;
            }
        }
    }
}

fn maybe_run_lifecycle_prune(engine: &StorageEngine, last_prune: &mut Instant) {
    let status = engine.lifecycle_status();
    if status.paused || status.schedule_interval_ms == 0 {
        *last_prune = Instant::now();
        return;
    }
    if last_prune.elapsed() < Duration::from_millis(status.schedule_interval_ms) {
        return;
    }
    engine.trigger_prune_now(0);
    *last_prune = Instant::now();
}

fn auto_flush_tick_interval(config: AsyncStorageConfig) -> Duration {
    if config.adaptive_flush {
        Duration::from_millis(config.min_flush_interval_ms.max(1))
    } else {
        Duration::from_millis(config.flush_interval_ms.max(1))
    }
}

fn adaptive_flush_interval(config: AsyncStorageConfig, pending: usize) -> Duration {
    if pending == 0 || config.target_flush_size == 0 {
        return Duration::from_millis(config.max_flush_interval_ms.max(1));
    }
    let min_interval = Duration::from_millis(config.min_flush_interval_ms.max(1));
    let max_interval = Duration::from_millis(config.max_flush_interval_ms.max(1));
    if max_interval <= min_interval {
        return min_interval;
    }
    let ratio = (pending as f64 / config.target_flush_size as f64).min(1.0);
    let span = max_interval - min_interval;
    min_interval + Duration::from_secs_f64(span.as_secs_f64() * ratio)
}

fn maybe_auto_flush(
    engine: &StorageEngine,
    shared: &AsyncStorageShared,
    config: AsyncStorageConfig,
) {
    if config.adaptive_flush {
        let pending = shared.pending_write_count();
        if pending == 0 {
            return;
        }
        let interval = adaptive_flush_interval(config, pending);
        if shared.last_flush_at().elapsed() < interval {
            return;
        }
    }
    let _ = shared.try_flush_pending(engine);
    // Drain any pending deindex work after each flush tick
    let _ = engine.drain_deindex_work();
    let _ = engine.sync_wal_if_due();
}

fn handle_request(
    engine: &StorageEngine,
    shared: &AsyncStorageShared,
    request: WorkerRequest,
) -> bool {
    match request {
        WorkerRequest::GetNodeRecord { id, reply } => {
            let _ = reply.send(engine.get_node_record(&id));
        }
        WorkerRequest::GetNodesByLabel { label, reply } => {
            let _ = reply.send(engine.get_nodes_by_label(&label));
        }
        WorkerRequest::GetNodeRecordsByPrefix { prefix, reply } => {
            let mut nodes = Vec::new();
            let result = engine.stream_node_records_by_prefix(&prefix, |node| {
                nodes.push(node);
                Ok(())
            });
            let _ = reply.send(result.map(|_| nodes));
        }
        WorkerRequest::AllNodeRecords { reply } => {
            let _ = reply.send(engine.all_node_records());
        }
        WorkerRequest::NodeCountByPrefix { prefix, reply } => {
            let _ = reply.send(engine.node_count_by_prefix(&prefix));
        }
        WorkerRequest::NodeCountByLabelInNamespace {
            namespace,
            label,
            reply,
        } => {
            let _ = reply.send(engine.node_count_by_label_in_namespace(&namespace, &label));
        }
        WorkerRequest::GetEdgeRecord { id, reply } => {
            let _ = reply.send(engine.get_edge_record(&id));
        }
        WorkerRequest::GetEdgesByType { edge_type, reply } => {
            let _ = reply.send(engine.get_edges_by_type(&edge_type));
        }
        WorkerRequest::AllEdges { reply } => {
            let _ = reply.send(engine.all_edges());
        }
        WorkerRequest::GetAdjacentEdges {
            node_id,
            direction,
            edge_type,
            reply,
        } => {
            let _ =
                reply.send(engine.get_adjacent_edges(&node_id, direction, edge_type.as_deref()));
        }
        WorkerRequest::EdgeCountByPrefix { prefix, reply } => {
            let _ = reply.send(engine.edge_count_by_prefix(&prefix));
        }
        WorkerRequest::BeginMvccSnapshot { reply } => {
            let _ = reply.send(engine.begin_mvcc_snapshot());
        }
        WorkerRequest::BeginRegisteredMvccSnapshot { reply } => {
            let _ = reply.send(engine.begin_registered_mvcc_snapshot());
        }
        WorkerRequest::GetNodeRecordVisibleAt {
            snapshot,
            id,
            reply,
        } => {
            let _ = reply.send(engine.get_node_record_visible_at(&snapshot, &id));
        }
        WorkerRequest::GetNodesByLabelVisibleAt {
            snapshot,
            label,
            reply,
        } => {
            let _ = reply.send(engine.get_nodes_by_label_visible_at(&snapshot, &label));
        }
        WorkerRequest::GetEdgeRecordVisibleAt {
            snapshot,
            id,
            reply,
        } => {
            let _ = reply.send(engine.get_edge_record_visible_at(&snapshot, &id));
        }
        WorkerRequest::GetEdgesByTypeVisibleAt {
            snapshot,
            edge_type,
            reply,
        } => {
            let _ = reply.send(engine.get_edges_by_type_visible_at(&snapshot, &edge_type));
        }
        WorkerRequest::RebuildMvccFromCurrentState { reply } => {
            let _ = reply.send(engine.rebuild_mvcc_from_current_state());
        }
        WorkerRequest::PruneMvccVersions { opts, reply } => {
            let _ = reply.send(engine.prune_mvcc_versions(opts));
        }
        WorkerRequest::LifecycleStatus { reply } => {
            let _ = reply.send(engine.lifecycle_status());
        }
        WorkerRequest::TriggerPruneNow {
            retain_last_n_versions,
            reply,
        } => {
            let _ = reply.send(engine.trigger_prune_now(retain_last_n_versions));
        }
        WorkerRequest::PauseLifecycle => {
            engine.pause_lifecycle();
        }
        WorkerRequest::ResumeLifecycle => {
            engine.resume_lifecycle();
        }
        WorkerRequest::SetLifecycleScheduleMs { interval_ms } => {
            engine.set_lifecycle_schedule_ms(interval_ms);
        }
        WorkerRequest::TopLifecycleDebtKeys { limit, reply } => {
            let _ = reply.send(engine.top_lifecycle_debt_keys(limit));
        }
        WorkerRequest::Flush { reply } => {
            let _ = reply.send(shared.flush_pending(engine));
        }
        WorkerRequest::TryFlush { reply } => {
            let _ = reply.send(shared.try_flush_pending(engine));
        }
        WorkerRequest::Close { reply } => {
            let _ = reply.send(shared.flush_pending(engine));
            return true;
        }
        WorkerRequest::EnqueueDeindex { id, reply } => {
            let _ = reply.send(engine.enqueue_deindex_work(&id));
        }
        WorkerRequest::DrainDeindex { reply } => {
            let _ = reply.send(engine.drain_deindex_work());
        }
        WorkerRequest::PendingDeindexCount { reply } => {
            let _ = reply.send(engine.pending_deindex_count());
        }
        WorkerRequest::RegisterCommitCompleted { callback } => {
            engine.on_commit_completed(callback);
        }
    }
    false
}

fn namespace_from_id(id: &str) -> Option<&str> {
    crate::parse_database_prefix(id).map(|(namespace, _)| namespace)
}

fn pending_adjacent_edge_ids(
    pending: &PendingState,
    node_id: &str,
    direction: EdgeAdjacencyDirection,
    edge_type: Option<&str>,
) -> Vec<String> {
    let mut ids = BTreeSet::new();
    match direction {
        EdgeAdjacencyDirection::Outgoing => {
            ids.extend(pending.edge_ids_from_start(node_id));
        }
        EdgeAdjacencyDirection::Incoming => {
            ids.extend(pending.edge_ids_to_end(node_id));
        }
        EdgeAdjacencyDirection::Both => {
            ids.extend(pending.edge_ids_from_start(node_id));
            ids.extend(pending.edge_ids_to_end(node_id));
        }
    }
    ids.into_iter()
        .filter(|id| {
            pending
                .pending_edge(id)
                .and_then(|pending_edge| pending_edge)
                .is_some_and(|edge| edge_matches_adjacency(&edge, node_id, direction, edge_type))
        })
        .collect()
}

fn edge_matches_adjacency(
    edge: &EdgeRecord,
    node_id: &str,
    direction: EdgeAdjacencyDirection,
    edge_type: Option<&str>,
) -> bool {
    let direction_matches = match direction {
        EdgeAdjacencyDirection::Outgoing => edge.start_node == node_id,
        EdgeAdjacencyDirection::Incoming => edge.end_node == node_id,
        EdgeAdjacencyDirection::Both => edge.start_node == node_id || edge.end_node == node_id,
    };
    direction_matches && edge_type.is_none_or(|expected| edge.edge_type == expected)
}
