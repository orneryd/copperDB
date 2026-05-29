use crate::{
    EdgeRecord, MvccLifecycleDebtKey, MvccLifecycleStatus, MvccPruneOptions, MvccSnapshot,
    MvccSnapshotLease, NodeRecord, StorageEngine, StorageError,
};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEFAULT_ASYNC_FLUSH_INTERVAL_MS: u64 = 50;

type PendingNodeOps = Vec<(String, Option<NodeRecord>)>;
type PendingEdgeOps = Vec<(String, Option<EdgeRecord>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncStorageConfig {
    pub flush_interval_ms: u64,
}

impl Default for AsyncStorageConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: DEFAULT_ASYNC_FLUSH_INTERVAL_MS,
        }
    }
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
    }

    fn remove_edge_index_entry(&mut self, id: &str) {
        let edge_type = self
            .edges
            .get(id)
            .and_then(|pending| pending.as_ref())
            .map(|edge| edge.edge_type.clone());
        if let Some(edge_type) = edge_type {
            let remove_type = if let Some(ids) = self.edge_type_index.get_mut(&edge_type) {
                ids.remove(id);
                ids.is_empty()
            } else {
                false
            };
            if remove_type {
                self.edge_type_index.remove(&edge_type);
            }
        }
    }
}

#[derive(Debug, Default)]
struct AsyncStorageShared {
    pending: Mutex<PendingState>,
    flush_lock: RwLock<()>,
}

impl AsyncStorageShared {
    fn flush_pending(&self, engine: &StorageEngine) -> Result<AsyncFlushResult, StorageError> {
        let _flush_guard = self.flush_lock.write();
        self.flush_pending_locked(engine)
    }

    fn try_flush_pending(
        &self,
        engine: &StorageEngine,
    ) -> Result<Option<AsyncFlushResult>, StorageError> {
        let Some(_flush_guard) = self.flush_lock.try_write() else {
            return Ok(None);
        };
        self.flush_pending_locked(engine).map(Some)
    }

    fn flush_pending_locked(
        &self,
        engine: &StorageEngine,
    ) -> Result<AsyncFlushResult, StorageError> {
        let (mut node_ops, mut edge_ops) = self.pending.lock().take_ops();

        let mut result = AsyncFlushResult::default();

        for (index, (id, pending)) in node_ops.iter().enumerate() {
            let apply = match pending {
                Some(node) => engine.put_node_record(node).map(|_| {
                    result.nodes_written += 1;
                }),
                None => engine.delete_node_record(id).map(|_| {
                    result.nodes_deleted += 1;
                }),
            };
            if let Err(err) = apply {
                self.requeue_node_ops(node_ops.split_off(index));
                self.requeue_edge_ops(edge_ops);
                return Err(err);
            }
        }

        for (index, (id, pending)) in edge_ops.iter().enumerate() {
            let apply = match pending {
                Some(edge) => engine.put_edge_record(edge).map(|_| {
                    result.edges_written += 1;
                }),
                None => engine.delete_edge_record(id).map(|_| {
                    result.edges_deleted += 1;
                }),
            };
            if let Err(err) = apply {
                self.requeue_edge_ops(edge_ops.split_off(index));
                return Err(err);
            }
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
}

#[derive(Debug)]
pub struct AsyncStorageEngine {
    shared: Arc<AsyncStorageShared>,
    worker_tx: Mutex<Option<Sender<WorkerRequest>>>,
    worker_handle: Mutex<Option<JoinHandle<()>>>,
    config: AsyncStorageConfig,
}

impl AsyncStorageEngine {
    pub fn new(engine: StorageEngine, config: Option<AsyncStorageConfig>) -> Self {
        let config = config.unwrap_or_default();
        let shared = Arc::new(AsyncStorageShared {
            pending: Mutex::new(PendingState::default()),
            flush_lock: RwLock::new(()),
        });
        let (worker_tx, worker_rx) = mpsc::channel();
        let worker_shared = Arc::clone(&shared);
        let flush_interval = Duration::from_millis(config.flush_interval_ms.max(1));
        let worker_handle =
            thread::spawn(move || worker_loop(engine, worker_shared, worker_rx, flush_interval));

        Self {
            shared,
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

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<(), StorageError> {
        self.shared.pending.lock().put_node(node.clone());
        Ok(())
    }

    pub fn delete_node_record(&self, id: &str) -> Result<(), StorageError> {
        self.shared.pending.lock().delete_node(id.to_string());
        Ok(())
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
        self.shared.pending.lock().put_edge(edge.clone());
        Ok(())
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<(), StorageError> {
        self.shared.pending.lock().delete_edge(id.to_string());
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
    flush_interval: Duration,
) {
    let mut next_auto_flush = Instant::now() + flush_interval;
    loop {
        let timeout = next_auto_flush.saturating_duration_since(Instant::now());
        match worker_rx.recv_timeout(timeout) {
            Ok(request) => {
                if handle_request(&engine, &shared, request) {
                    break;
                }
                if Instant::now() >= next_auto_flush {
                    let _ = shared.try_flush_pending(&engine);
                    next_auto_flush = Instant::now() + flush_interval;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = shared.try_flush_pending(&engine);
                next_auto_flush = Instant::now() + flush_interval;
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = shared.flush_pending(&engine);
                break;
            }
        }
    }
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
    }
    false
}

fn namespace_from_id(id: &str) -> Option<&str> {
    id.split_once(':').map(|(namespace, _)| namespace)
}
