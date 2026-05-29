use crate::{EdgeRecord, NodeRecord, StorageError};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DEFAULT_LIFECYCLE_SCHEDULE_MS: u64 = 60_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccSnapshot {
    pub id: u64,
    pub read_ts: u64,
}

#[derive(Debug)]
pub struct MvccSnapshotLease {
    snapshot: MvccSnapshot,
    active_readers: Arc<Mutex<BTreeMap<u64, u64>>>,
}

impl MvccSnapshotLease {
    pub fn snapshot(&self) -> &MvccSnapshot {
        &self.snapshot
    }
}

impl Drop for MvccSnapshotLease {
    fn drop(&mut self) {
        let mut readers = self.active_readers.lock();
        if let Some(count) = readers.get_mut(&self.snapshot.read_ts) {
            *count -= 1;
            if *count == 0 {
                readers.remove(&self.snapshot.read_ts);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccVersion {
    pub version: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccHead {
    pub floor: u64,
    pub head: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccLifecycleStatus {
    pub enabled: bool,
    pub paused: bool,
    pub schedule_interval_ms: u64,
    pub floor: u64,
    pub head: u64,
    pub oldest_active_reader: Option<u64>,
    pub active_reader_count: u64,
    pub retained_versions: usize,
    pub prune_debt: usize,
    pub suggested_prune_floor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccLifecycleDebtKey {
    pub logical_key: String,
    pub retained_versions: usize,
    pub prune_debt: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccLogicalHead {
    pub floor: u64,
    pub head: u64,
    pub tombstoned: bool,
}

#[derive(Debug, Clone, Default)]
struct MvccKeyState {
    head: Option<MvccLogicalStateHead>,
    archived: BTreeMap<u64, Option<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MvccLogicalStateHead {
    floor: u64,
    head: u64,
    tombstoned: bool,
    current_value: Option<Vec<u8>>,
}

impl MvccKeyState {
    fn append_version(&mut self, version: u64, value: Option<Vec<u8>>) {
        if let Some(previous_head) = self.head.take() {
            self.archived
                .insert(previous_head.head, previous_head.current_value);
            self.head = Some(MvccLogicalStateHead {
                floor: previous_head.floor,
                head: version,
                tombstoned: value.is_none(),
                current_value: value,
            });
            return;
        }

        self.head = Some(MvccLogicalStateHead {
            floor: version,
            head: version,
            tombstoned: value.is_none(),
            current_value: value,
        });
    }

    fn read_at(&self, read_ts: u64) -> Option<Vec<u8>> {
        let head = self.head.as_ref()?;
        if read_ts < head.floor {
            return None;
        }
        if read_ts >= head.head {
            return if head.tombstoned {
                None
            } else {
                head.current_value.clone()
            };
        }
        self.archived
            .range(..=read_ts)
            .next_back()
            .and_then(|(_, value)| value.clone())
    }

    fn retained_versions(&self) -> usize {
        self.archived.len() + usize::from(self.head.is_some())
    }

    fn compute_floor_for_request(&self, requested_floor: u64) -> Option<u64> {
        let head = self.head.as_ref()?;
        let requested_floor = requested_floor.min(head.head);
        if requested_floor >= head.head {
            return Some(head.head);
        }
        Some(
            self.archived
                .range(requested_floor..)
                .next()
                .map(|(version, _)| *version)
                .unwrap_or(head.head),
        )
    }

    fn prune_to_floor(&mut self, requested_floor: u64) -> usize {
        let Some(new_floor) = self.compute_floor_for_request(requested_floor) else {
            return 0;
        };
        let removed_keys = self
            .archived
            .range(..new_floor)
            .map(|(version, _)| *version)
            .collect::<Vec<_>>();
        let removed_count = removed_keys.len();
        for version in removed_keys {
            self.archived.remove(&version);
        }
        if let Some(head) = self.head.as_mut() {
            head.floor = new_floor;
        }
        removed_count
    }

    fn prune_debt(&self, candidate_floor: u64) -> usize {
        let Some(new_floor) = self.compute_floor_for_request(candidate_floor) else {
            return 0;
        };
        self.archived.range(..new_floor).count()
    }

    fn logical_head(&self) -> Option<MvccLogicalHead> {
        self.head.as_ref().map(|head| MvccLogicalHead {
            floor: head.floor,
            head: head.head,
            tombstoned: head.tombstoned,
        })
    }
}

pub struct NamespacedMvccStore<'a> {
    inner: &'a MvccStore,
    namespace: String,
    prefix: String,
}

#[derive(Debug)]
pub struct MvccStore {
    current_version: AtomicU64,
    floor: AtomicU64,
    values: RwLock<BTreeMap<String, MvccKeyState>>,
    current_node_labels: RwLock<BTreeMap<String, BTreeSet<String>>>,
    node_label_history: RwLock<BTreeMap<String, BTreeSet<String>>>,
    current_edge_types: RwLock<BTreeMap<String, BTreeSet<String>>>,
    edge_type_history: RwLock<BTreeMap<String, BTreeSet<String>>>,
    active_readers: Arc<Mutex<BTreeMap<u64, u64>>>,
    lifecycle_schedule_ms: AtomicU64,
    lifecycle_paused: AtomicU64,
}

impl Default for MvccStore {
    fn default() -> Self {
        Self {
            current_version: AtomicU64::new(0),
            floor: AtomicU64::new(0),
            values: RwLock::new(BTreeMap::new()),
            current_node_labels: RwLock::new(BTreeMap::new()),
            node_label_history: RwLock::new(BTreeMap::new()),
            current_edge_types: RwLock::new(BTreeMap::new()),
            edge_type_history: RwLock::new(BTreeMap::new()),
            active_readers: Arc::new(Mutex::new(BTreeMap::new())),
            lifecycle_schedule_ms: AtomicU64::new(DEFAULT_LIFECYCLE_SCHEDULE_MS),
            lifecycle_paused: AtomicU64::new(0),
        }
    }
}

impl MvccStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn for_namespace(&self, namespace: impl Into<String>) -> NamespacedMvccStore<'_> {
        NamespacedMvccStore::new(self, namespace)
    }

    pub fn begin_snapshot(&self) -> MvccSnapshot {
        let read_ts = self.current_version.load(Ordering::SeqCst);
        MvccSnapshot {
            id: read_ts,
            read_ts,
        }
    }

    pub fn begin_registered_snapshot(&self) -> MvccSnapshotLease {
        let snapshot = self.begin_snapshot();
        let mut readers = self.active_readers.lock();
        *readers.entry(snapshot.read_ts).or_insert(0) += 1;
        drop(readers);

        MvccSnapshotLease {
            snapshot,
            active_readers: Arc::clone(&self.active_readers),
        }
    }

    pub fn commit_batch<I>(&self, writes: I) -> u64
    where
        I: IntoIterator<Item = (String, Option<Vec<u8>>)>,
    {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        let mut guard = self.values.write();
        for (key, value) in writes {
            guard.entry(key).or_default().append_version(version, value);
        }
        version
    }

    pub fn read(&self, snapshot: &MvccSnapshot, key: &str) -> Option<Vec<u8>> {
        let guard = self.values.read();
        guard
            .get(key)
            .and_then(|state| state.read_at(snapshot.read_ts))
    }

    pub fn prune_versions_older_than(&self, min_version: u64) {
        let effective_min_version = self
            .oldest_active_reader()
            .map(|oldest_reader| min_version.min(oldest_reader))
            .unwrap_or(min_version);

        self.floor.store(effective_min_version, Ordering::SeqCst);
        let mut guard = self.values.write();
        for state in guard.values_mut() {
            state.prune_to_floor(effective_min_version);
        }
    }

    pub fn trigger_prune_now(&self, retain_last_n_versions: u64) -> usize {
        let head = self.current_version.load(Ordering::SeqCst);
        let requested_floor = head.saturating_sub(retain_last_n_versions);
        let effective_min_version = self.safe_prune_floor(requested_floor);

        self.floor.store(effective_min_version, Ordering::SeqCst);
        let mut removed_versions = 0usize;
        let mut guard = self.values.write();
        for state in guard.values_mut() {
            removed_versions += state.prune_to_floor(effective_min_version);
        }

        removed_versions
    }

    pub fn current_head_for_key(&self, key: &str) -> Option<MvccLogicalHead> {
        let guard = self.values.read();
        guard.get(key).and_then(MvccKeyState::logical_head)
    }

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<u64, StorageError> {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        let logical_key = node_logical_key(&node.id);
        let encoded = encode_node_record(node)?;
        let mut values = self.values.write();
        let previous = values
            .get(&logical_key)
            .and_then(current_live_node_from_state)
            .transpose()?;
        let mut current_labels = self.current_node_labels.write();
        let mut label_history = self.node_label_history.write();

        if let Some(previous) = previous.as_ref() {
            remove_node_from_current_labels(&mut current_labels, previous);
        }

        for label in &node.labels {
            current_labels
                .entry(label.clone())
                .or_default()
                .insert(node.id.clone());
            label_history
                .entry(label.clone())
                .or_default()
                .insert(node.id.clone());
        }

        values
            .entry(logical_key)
            .or_default()
            .append_version(version, Some(encoded));
        Ok(version)
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        let snapshot = self.begin_snapshot();
        self.get_node_record_visible_at(&snapshot, id)
    }

    pub fn get_node_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<NodeRecord>, StorageError> {
        self.read(snapshot, &node_logical_key(id))
            .map(|raw| decode_node_record(raw.as_slice()))
            .transpose()
    }

    pub fn delete_node_record(&self, id: &str) -> Result<u64, StorageError> {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        let logical_key = node_logical_key(id);
        let mut values = self.values.write();
        let previous = values
            .get(&logical_key)
            .and_then(current_live_node_from_state)
            .transpose()?;
        if let Some(previous) = previous.as_ref() {
            let mut current_labels = self.current_node_labels.write();
            remove_node_from_current_labels(&mut current_labels, previous);
        }
        values
            .entry(logical_key)
            .or_default()
            .append_version(version, None);
        Ok(version)
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeRecord>, StorageError> {
        let ids = self
            .current_node_labels
            .read()
            .get(label)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut nodes = Vec::new();
        for id in ids {
            if let Some(node) = self.get_node_record(&id)? {
                nodes.push(node);
            }
        }
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(nodes)
    }

    pub fn get_nodes_by_label_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        label: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        let ids = self
            .node_label_history
            .read()
            .get(label)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut nodes = Vec::new();
        for id in ids {
            let Some(node) = self.get_node_record_visible_at(snapshot, &id)? else {
                continue;
            };
            if node.labels.iter().any(|existing| existing == label) {
                nodes.push(node);
            }
        }
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        nodes.dedup_by(|left, right| left.id == right.id);
        Ok(nodes)
    }

    pub fn put_edge_record(&self, edge: &EdgeRecord) -> Result<u64, StorageError> {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        let logical_key = edge_logical_key(&edge.id);
        let encoded = encode_edge_record(edge)?;
        let mut values = self.values.write();
        let previous = values
            .get(&logical_key)
            .and_then(current_live_edge_from_state)
            .transpose()?;
        let mut current_edge_types = self.current_edge_types.write();
        let mut edge_type_history = self.edge_type_history.write();

        if let Some(previous) = previous.as_ref() {
            remove_edge_from_current_types(&mut current_edge_types, previous);
        }

        current_edge_types
            .entry(edge.edge_type.clone())
            .or_default()
            .insert(edge.id.clone());
        edge_type_history
            .entry(edge.edge_type.clone())
            .or_default()
            .insert(edge.id.clone());

        values
            .entry(logical_key)
            .or_default()
            .append_version(version, Some(encoded));
        Ok(version)
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        let snapshot = self.begin_snapshot();
        self.get_edge_record_visible_at(&snapshot, id)
    }

    pub fn get_edge_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        self.read(snapshot, &edge_logical_key(id))
            .map(|raw| decode_edge_record(raw.as_slice()))
            .transpose()
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<u64, StorageError> {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        let logical_key = edge_logical_key(id);
        let mut values = self.values.write();
        let previous = values
            .get(&logical_key)
            .and_then(current_live_edge_from_state)
            .transpose()?;
        if let Some(previous) = previous.as_ref() {
            let mut current_edge_types = self.current_edge_types.write();
            remove_edge_from_current_types(&mut current_edge_types, previous);
        }
        values
            .entry(logical_key)
            .or_default()
            .append_version(version, None);
        Ok(version)
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        let ids = self
            .current_edge_types
            .read()
            .get(edge_type)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut edges = Vec::new();
        for id in ids {
            if let Some(edge) = self.get_edge_record(&id)? {
                edges.push(edge);
            }
        }
        edges.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(edges)
    }

    pub fn get_edges_by_type_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        let ids = self
            .edge_type_history
            .read()
            .get(edge_type)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut edges = Vec::new();
        for id in ids {
            let Some(edge) = self.get_edge_record_visible_at(snapshot, &id)? else {
                continue;
            };
            if edge.edge_type == edge_type {
                edges.push(edge);
            }
        }
        edges.sort_by(|left, right| left.id.cmp(&right.id));
        edges.dedup_by(|left, right| left.id == right.id);
        Ok(edges)
    }

    pub fn pause_lifecycle(&self) {
        self.lifecycle_paused.store(1, Ordering::SeqCst);
    }

    pub fn resume_lifecycle(&self) {
        self.lifecycle_paused.store(0, Ordering::SeqCst);
    }

    pub fn set_lifecycle_schedule_ms(&self, interval_ms: u64) {
        self.lifecycle_schedule_ms
            .store(interval_ms, Ordering::SeqCst);
    }

    pub fn top_lifecycle_debt_keys(&self, limit: usize) -> Vec<MvccLifecycleDebtKey> {
        if limit == 0 {
            return Vec::new();
        }
        let candidate_floor = self
            .oldest_active_reader()
            .unwrap_or_else(|| self.current_version.load(Ordering::SeqCst));
        let guard = self.values.read();
        let mut keys = guard
            .iter()
            .filter_map(|(logical_key, state)| {
                let prune_debt = state.prune_debt(candidate_floor);
                if prune_debt == 0 {
                    return None;
                }
                Some(MvccLifecycleDebtKey {
                    logical_key: logical_key.clone(),
                    retained_versions: state.retained_versions(),
                    prune_debt,
                })
            })
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            right
                .prune_debt
                .cmp(&left.prune_debt)
                .then_with(|| left.logical_key.cmp(&right.logical_key))
        });
        keys.truncate(limit);
        keys
    }

    pub fn oldest_active_reader(&self) -> Option<u64> {
        self.active_readers.lock().keys().next().copied()
    }

    pub fn lifecycle_status(&self) -> MvccLifecycleStatus {
        let head = self.head();
        let oldest_active_reader = self.oldest_active_reader();
        let active_reader_count = self.active_reader_count();
        let retained_versions = self.retained_version_count();
        let suggested_prune_floor = oldest_active_reader.unwrap_or(head.head);
        let prune_debt = self.compute_prune_debt(suggested_prune_floor);

        MvccLifecycleStatus {
            enabled: true,
            paused: self.lifecycle_paused.load(Ordering::SeqCst) != 0,
            schedule_interval_ms: self.lifecycle_schedule_ms.load(Ordering::SeqCst),
            floor: head.floor,
            head: head.head,
            oldest_active_reader,
            active_reader_count,
            retained_versions,
            prune_debt,
            suggested_prune_floor,
        }
    }

    pub fn active_reader_count(&self) -> u64 {
        self.active_readers.lock().values().copied().sum()
    }

    pub fn retained_version_count(&self) -> usize {
        let guard = self.values.read();
        guard.values().map(MvccKeyState::retained_versions).sum()
    }

    pub(crate) fn reset_for_rebuild(&self) {
        self.current_version.store(0, Ordering::SeqCst);
        self.floor.store(0, Ordering::SeqCst);
        self.values.write().clear();
        self.current_node_labels.write().clear();
        self.node_label_history.write().clear();
        self.current_edge_types.write().clear();
        self.edge_type_history.write().clear();
        self.active_readers.lock().clear();
    }

    fn safe_prune_floor(&self, requested_floor: u64) -> u64 {
        self.oldest_active_reader()
            .map(|oldest_reader| requested_floor.min(oldest_reader))
            .unwrap_or(requested_floor)
    }

    fn compute_prune_debt(&self, candidate_floor: u64) -> usize {
        let guard = self.values.read();
        guard
            .values()
            .map(|state| state.prune_debt(candidate_floor))
            .sum()
    }

    pub fn head(&self) -> MvccHead {
        MvccHead {
            floor: self.floor.load(Ordering::SeqCst),
            head: self.current_version.load(Ordering::SeqCst),
        }
    }

    pub fn encode_head(head: &MvccHead) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&head.floor.to_be_bytes());
        out[8..].copy_from_slice(&head.head.to_be_bytes());
        out
    }

    pub fn decode_head(raw: &[u8]) -> Result<MvccHead, StorageError> {
        if raw.len() < 8 {
            return Err(StorageError::MvccHeadTruncated(raw.len()));
        }
        if raw.len() < 16 {
            return Err(StorageError::MvccHeadMissingFloor(raw.len()));
        }
        let floor = u64::from_be_bytes(raw[..8].try_into().expect("slice length checked"));
        let head = u64::from_be_bytes(raw[8..16].try_into().expect("slice length checked"));
        Ok(MvccHead { floor, head })
    }
}

impl<'a> NamespacedMvccStore<'a> {
    fn new(inner: &'a MvccStore, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let prefix = format!("{namespace}:");
        Self {
            inner,
            namespace,
            prefix,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn inner(&self) -> &MvccStore {
        self.inner
    }

    pub fn begin_snapshot(&self) -> MvccSnapshot {
        self.inner.begin_snapshot()
    }

    pub fn begin_registered_snapshot(&self) -> MvccSnapshotLease {
        self.inner.begin_registered_snapshot()
    }

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<u64, StorageError> {
        let mut namespaced = node.clone();
        namespaced.id = self.prefix_id(&namespaced.id);
        self.inner.put_node_record(&namespaced)
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        self.inner
            .get_node_record(&self.prefix_id(id))
            .map(|node| node.map(|node| self.to_user_node(node)))
    }

    pub fn get_node_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<NodeRecord>, StorageError> {
        self.inner
            .get_node_record_visible_at(snapshot, &self.prefix_id(id))
            .map(|node| node.map(|node| self.to_user_node(node)))
    }

    pub fn delete_node_record(&self, id: &str) -> Result<u64, StorageError> {
        self.inner.delete_node_record(&self.prefix_id(id))
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self
            .inner
            .get_nodes_by_label(label)?
            .into_iter()
            .filter(|node| self.in_namespace(&node.id))
            .map(|node| self.to_user_node(node))
            .collect())
    }

    pub fn get_nodes_by_label_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        label: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self
            .inner
            .get_nodes_by_label_visible_at(snapshot, label)?
            .into_iter()
            .filter(|node| self.in_namespace(&node.id))
            .map(|node| self.to_user_node(node))
            .collect())
    }

    pub fn put_edge_record(&self, edge: &EdgeRecord) -> Result<u64, StorageError> {
        let mut namespaced = edge.clone();
        namespaced.id = self.prefix_id(&namespaced.id);
        namespaced.start_node = self.prefix_id(&namespaced.start_node);
        namespaced.end_node = self.prefix_id(&namespaced.end_node);
        self.inner.put_edge_record(&namespaced)
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        self.inner
            .get_edge_record(&self.prefix_id(id))
            .map(|edge| edge.map(|edge| self.to_user_edge(edge)))
    }

    pub fn get_edge_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        self.inner
            .get_edge_record_visible_at(snapshot, &self.prefix_id(id))
            .map(|edge| edge.map(|edge| self.to_user_edge(edge)))
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<u64, StorageError> {
        self.inner.delete_edge_record(&self.prefix_id(id))
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_edges_by_type(edge_type)?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn get_edges_by_type_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_edges_by_type_visible_at(snapshot, edge_type)?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn lifecycle_status(&self) -> MvccLifecycleStatus {
        self.inner.lifecycle_status()
    }

    pub fn trigger_prune_now(&self, retain_last_n_versions: u64) -> usize {
        self.inner.trigger_prune_now(retain_last_n_versions)
    }

    pub fn pause_lifecycle(&self) {
        self.inner.pause_lifecycle();
    }

    pub fn resume_lifecycle(&self) {
        self.inner.resume_lifecycle();
    }

    pub fn set_lifecycle_schedule_ms(&self, interval_ms: u64) {
        self.inner.set_lifecycle_schedule_ms(interval_ms);
    }

    pub fn top_lifecycle_debt_keys(&self, limit: usize) -> Vec<MvccLifecycleDebtKey> {
        self.inner
            .top_lifecycle_debt_keys(limit.saturating_mul(4).max(limit))
            .into_iter()
            .filter_map(|debt| {
                if !self.logical_key_in_namespace(&debt.logical_key) {
                    return None;
                }
                Some(MvccLifecycleDebtKey {
                    logical_key: self.strip_namespace_from_logical_key(&debt.logical_key),
                    retained_versions: debt.retained_versions,
                    prune_debt: debt.prune_debt,
                })
            })
            .take(limit)
            .collect()
    }

    fn prefix_id(&self, id: &str) -> String {
        if self.in_namespace(id) {
            id.to_string()
        } else {
            format!("{}{id}", self.prefix)
        }
    }

    fn unprefix_id(&self, id: &str) -> String {
        id.strip_prefix(&self.prefix).unwrap_or(id).to_string()
    }

    fn in_namespace(&self, id: &str) -> bool {
        id.starts_with(&self.prefix)
    }

    fn to_user_node(&self, mut node: NodeRecord) -> NodeRecord {
        node.id = self.unprefix_id(&node.id);
        node
    }

    fn to_user_edge(&self, mut edge: EdgeRecord) -> EdgeRecord {
        edge.id = self.unprefix_id(&edge.id);
        edge.start_node = self.unprefix_id(&edge.start_node);
        edge.end_node = self.unprefix_id(&edge.end_node);
        edge
    }

    fn logical_key_in_namespace(&self, logical_key: &str) -> bool {
        let Some((_, id)) = logical_key.split_once(':') else {
            return false;
        };
        id.starts_with(&self.prefix)
    }

    fn strip_namespace_from_logical_key(&self, logical_key: &str) -> String {
        let Some((kind, id)) = logical_key.split_once(':') else {
            return logical_key.to_string();
        };
        format!("{kind}:{}", self.unprefix_id(id))
    }
}

fn node_logical_key(id: &str) -> String {
    format!("node:{id}")
}

fn edge_logical_key(id: &str) -> String {
    format!("edge:{id}")
}

fn encode_node_record(node: &NodeRecord) -> Result<Vec<u8>, StorageError> {
    Ok(rmp_serde::to_vec(node)?)
}

fn decode_node_record(raw: &[u8]) -> Result<NodeRecord, StorageError> {
    Ok(rmp_serde::from_slice(raw)?)
}

fn encode_edge_record(edge: &EdgeRecord) -> Result<Vec<u8>, StorageError> {
    Ok(rmp_serde::to_vec(edge)?)
}

fn decode_edge_record(raw: &[u8]) -> Result<EdgeRecord, StorageError> {
    Ok(rmp_serde::from_slice(raw)?)
}

fn current_live_node_from_state(state: &MvccKeyState) -> Option<Result<NodeRecord, StorageError>> {
    let head = state.head.as_ref()?;
    if head.tombstoned {
        return None;
    }
    Some(
        head.current_value
            .as_deref()
            .ok_or_else(|| StorageError::NotFound("missing node head payload".to_string()))
            .and_then(decode_node_record),
    )
}

fn current_live_edge_from_state(state: &MvccKeyState) -> Option<Result<EdgeRecord, StorageError>> {
    let head = state.head.as_ref()?;
    if head.tombstoned {
        return None;
    }
    Some(
        head.current_value
            .as_deref()
            .ok_or_else(|| StorageError::NotFound("missing edge head payload".to_string()))
            .and_then(decode_edge_record),
    )
}

fn remove_node_from_current_labels(
    current_labels: &mut BTreeMap<String, BTreeSet<String>>,
    node: &NodeRecord,
) {
    for label in &node.labels {
        if let Some(ids) = current_labels.get_mut(label) {
            ids.remove(&node.id);
            if ids.is_empty() {
                current_labels.remove(label);
            }
        }
    }
}

fn remove_edge_from_current_types(
    current_edge_types: &mut BTreeMap<String, BTreeSet<String>>,
    edge: &EdgeRecord,
) {
    if let Some(ids) = current_edge_types.get_mut(&edge.edge_type) {
        ids.remove(&edge.id);
        if ids.is_empty() {
            current_edge_types.remove(&edge.edge_type);
        }
    }
}
