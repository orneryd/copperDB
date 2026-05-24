//! Embedded key-value storage engine for copperdb.
//!
//! Storage layout policy for copper: **version 0 only**.
//! This crate intentionally avoids legacy migration arms and only supports
//! opening databases whose manifest declares layout version 0.

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const STORAGE_LAYOUT_VERSION: u8 = 0;
const META_LAYOUT_MANIFEST_KEY: &[u8] = b"layout_manifest";
const META_SEARCH_PEER_PREFIX: &[u8] = b"search_peer/";
const META_HYPERSCALER_PROFILE_PREFIX: &[u8] = b"hyperscaler_profile/";
const META_SCHEMA_CONSTRAINT_PREFIX: &[u8] = b"schema_constraint/";
const META_SCHEMA_INDEX_PREFIX: &[u8] = b"schema_index/";
const META_KP_DECAY_PROFILE_PREFIX: &[u8] = b"kp_decay_profile/";
const META_KP_PROMOTION_PROFILE_PREFIX: &[u8] = b"kp_promotion_profile/";
const META_KP_PROMOTION_POLICY_PREFIX: &[u8] = b"kp_promotion_policy/";
const IDX_LABEL_PREFIX: &str = "label_nodes";
const IDX_EDGE_TYPE_PREFIX: &str = "edge_type";

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io/sled error: {0}")]
    Sled(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),
    #[error("unsupported storage layout version: expected {expected}, got {actual}")]
    UnsupportedLayoutVersion { expected: u8, actual: u8 },
    #[error("key not found: {0}")]
    NotFound(String),
    #[error("invalid utf8 in key")]
    InvalidUtf8,
    #[error("mvcc head truncated: {0} bytes")]
    MvccHeadTruncated(usize),
    #[error("mvcc head missing floor: {0} bytes")]
    MvccHeadMissingFloor(usize),
    #[error("wal: closed")]
    WalClosed,
    #[error("wal: corrupted entry")]
    WalCorruptedEntry,
    #[error("wal: partial write detected")]
    WalPartialWriteDetected,
    #[error("wal: checksum verification failed")]
    WalChecksumVerificationFailed,
    #[error("wal: missing or invalid trailer (incomplete write)")]
    WalMissingOrInvalidTrailer,
    #[error("constraint \"{0}\" already exists")]
    ConstraintAlreadyExists(String),
    #[error("constraint \"{0}\" not found")]
    ConstraintNotFound(String),
    #[error("constraint \"{constraint}\" violated: missing required property \"{property}\"")]
    ConstraintMissingProperty {
        constraint: String,
        property: String,
    },
    #[error("Node({label}) already exists with {property} = {value}")]
    UniqueConstraintViolation {
        label: String,
        property: String,
        value: String,
    },
    #[error("knowledge policy already exists: {0}")]
    KnowledgePolicyAlreadyExists(String),
    #[error("knowledge policy not found: {0}")]
    KnowledgePolicyNotFound(String),
    #[error("knowledge policy invalid: {0}")]
    KnowledgePolicyInvalid(String),
    #[error("knowledge policy in use: {0}")]
    KnowledgePolicyInUse(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccSnapshot {
    pub id: u64,
    pub read_ts: u64,
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

#[derive(Debug, Default)]
pub struct MvccStore {
    current_version: AtomicU64,
    floor: AtomicU64,
    values: RwLock<BTreeMap<String, Vec<MvccVersion>>>,
}

impl MvccStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_snapshot(&self) -> MvccSnapshot {
        let read_ts = self.current_version.load(Ordering::SeqCst);
        MvccSnapshot {
            id: read_ts,
            read_ts,
        }
    }

    pub fn commit_batch<I>(&self, writes: I) -> u64
    where
        I: IntoIterator<Item = (String, Option<Vec<u8>>)>,
    {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        let mut guard = self.values.write();
        for (key, value) in writes {
            guard
                .entry(key)
                .or_default()
                .push(MvccVersion { version, value });
        }
        version
    }

    pub fn read(&self, snapshot: &MvccSnapshot, key: &str) -> Option<Vec<u8>> {
        let guard = self.values.read();
        guard.get(key).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|v| v.version <= snapshot.read_ts)
                .and_then(|v| v.value.clone())
        })
    }

    pub fn prune_versions_older_than(&self, min_version: u64) {
        self.floor.store(min_version, Ordering::SeqCst);
        let mut guard = self.values.write();
        for versions in guard.values_mut() {
            if versions.len() <= 1 {
                continue;
            }
            let keep_from = versions
                .iter()
                .position(|v| v.version >= min_version)
                .unwrap_or(versions.len().saturating_sub(1));
            if keep_from > 0 {
                versions.drain(0..keep_from);
            }
        }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALEntry {
    pub seq: u64,
    pub op: String,
    pub key: String,
    pub payload: Vec<u8>,
    pub checksum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALSegment {
    pub segment_id: u64,
    pub start_seq: u64,
    pub end_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALConfig {
    pub enabled: bool,
    pub max_entries_per_segment: usize,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries_per_segment: 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALStats {
    pub entries: usize,
    pub segments: usize,
    pub degraded: bool,
}

#[derive(Debug)]
pub struct WAL {
    config: WALConfig,
    next_seq: AtomicU64,
    closed: AtomicBool,
    degraded: AtomicBool,
    entries: Mutex<Vec<WALEntry>>,
    segments: Mutex<Vec<WALSegment>>,
}

impl WAL {
    pub fn new(config: WALConfig) -> Self {
        Self {
            config,
            next_seq: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            entries: Mutex::new(Vec::new()),
            segments: Mutex::new(Vec::new()),
        }
    }

    pub fn append(&self, op: &str, key: &str, payload: &[u8]) -> Result<WALEntry, StorageError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::WalClosed);
        }
        if !self.config.enabled {
            return Ok(WALEntry {
                seq: 0,
                op: op.to_string(),
                key: key.to_string(),
                payload: payload.to_vec(),
                checksum: wal_checksum(op, key, payload),
            });
        }
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let entry = WALEntry {
            seq,
            op: op.to_string(),
            key: key.to_string(),
            payload: payload.to_vec(),
            checksum: wal_checksum(op, key, payload),
        };
        {
            let mut entries = self.entries.lock();
            entries.push(entry.clone());
            self.recompute_segments(entries.len());
        }
        Ok(entry)
    }

    pub fn append_batch(
        &self,
        records: Vec<(String, String, Vec<u8>)>,
    ) -> Result<(u64, u64), StorageError> {
        if records.is_empty() {
            return Ok((0, 0));
        }
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::WalClosed);
        }
        let mut staged = Vec::with_capacity(records.len());
        for (op, key, payload) in records {
            let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
            staged.push(WALEntry {
                seq,
                checksum: wal_checksum(&op, &key, &payload),
                op,
                key,
                payload,
            });
        }
        let first = staged.first().map(|e| e.seq).unwrap_or(0);
        let last = staged.last().map(|e| e.seq).unwrap_or(0);
        let mut entries = self.entries.lock();
        entries.extend(staged);
        self.recompute_segments(entries.len());
        Ok((first, last))
    }

    pub fn replay_after(&self, after_seq: u64) -> Result<Vec<WALEntry>, StorageError> {
        let entries = self.entries.lock();
        let mut out = Vec::new();
        for entry in entries.iter().filter(|e| e.seq > after_seq) {
            let expected = wal_checksum(&entry.op, &entry.key, &entry.payload);
            if entry.checksum != expected {
                self.degraded.store(true, Ordering::SeqCst);
                return Err(StorageError::WalChecksumVerificationFailed);
            }
            out.push(entry.clone());
        }
        Ok(out)
    }

    pub fn inject_corruption_for_test(&self, seq: u64) -> Result<(), StorageError> {
        let mut entries = self.entries.lock();
        let target = entries
            .iter_mut()
            .find(|entry| entry.seq == seq)
            .ok_or(StorageError::WalCorruptedEntry)?;
        target.checksum ^= 0xFFFF_FFFF;
        Ok(())
    }

    pub fn mark_partial_write_detected(&self) -> Result<(), StorageError> {
        self.degraded.store(true, Ordering::SeqCst);
        Err(StorageError::WalPartialWriteDetected)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> WALStats {
        WALStats {
            entries: self.entries.lock().len(),
            segments: self.segments.lock().len(),
            degraded: self.is_degraded(),
        }
    }

    fn recompute_segments(&self, total_entries: usize) {
        if !self.config.enabled || self.config.max_entries_per_segment == 0 {
            return;
        }
        let segment_len = self.config.max_entries_per_segment as u64;
        let mut segments = self.segments.lock();
        segments.clear();
        let total_entries = total_entries as u64;
        if total_entries == 0 {
            return;
        }
        let mut start = 1u64;
        let mut id = 1u64;
        while start <= total_entries {
            let end = (start + segment_len - 1).min(total_entries);
            segments.push(WALSegment {
                segment_id: id,
                start_seq: start,
                end_seq: end,
            });
            id += 1;
            start = end + 1;
        }
    }
}

fn wal_checksum(op: &str, key: &str, payload: &[u8]) -> u32 {
    let mut checksum = 2166136261u32;
    for b in op.bytes().chain(key.bytes()).chain(payload.iter().copied()) {
        checksum ^= b as u32;
        checksum = checksum.wrapping_mul(16777619);
    }
    checksum
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConstraintType {
    Unique,
    Exists,
    NodeKey,
    Type,
    Relationship,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexEntityType {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConstraintEntityType {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Constraint {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub entity_type: ConstraintEntityType,
    pub label: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub entity_type: IndexEntityType,
    pub label: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecayProfileSchema {
    pub name: String,
    pub half_life_seconds: i64,
    pub visibility_threshold: f64,
    pub score_floor: f64,
    pub function: String,
    pub scope: String,
    pub decay_enabled: bool,
    pub score_from: String,
    pub score_from_property: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromotionProfileSchema {
    pub name: String,
    pub scope: String,
    pub multiplier: f64,
    pub score_floor: f64,
    pub score_cap: f64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionWhenClauseSchema {
    pub profile_ref: String,
    pub predicate: String,
    pub order: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionPolicySchema {
    pub name: String,
    pub target_labels: Vec<String>,
    pub enabled: bool,
    pub when_clauses: Vec<PromotionWhenClauseSchema>,
}

#[derive(Debug, Default)]
pub struct SchemaManager {
    constraints: RwLock<BTreeMap<String, Constraint>>,
    unique_values: RwLock<BTreeMap<(String, String, String), String>>,
}

impl SchemaManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_constraint(&self, constraint: Constraint) -> Result<(), StorageError> {
        let mut guard = self.constraints.write();
        if guard.contains_key(&constraint.name) {
            return Err(StorageError::ConstraintAlreadyExists(constraint.name));
        }
        guard.insert(constraint.name.clone(), constraint);
        Ok(())
    }

    pub fn remove_constraint(&self, name: &str) -> Result<(), StorageError> {
        let mut guard = self.constraints.write();
        guard
            .remove(name)
            .ok_or_else(|| StorageError::ConstraintNotFound(name.to_string()))?;
        Ok(())
    }

    pub fn list_constraints(&self) -> Vec<Constraint> {
        self.constraints.read().values().cloned().collect()
    }

    pub fn validate_node(
        &self,
        node_id: &str,
        label: &str,
        properties: &BTreeMap<String, serde_json::Value>,
    ) -> Result<(), StorageError> {
        let constraints = self.constraints.read();
        for constraint in constraints.values().filter(|c| c.label == label) {
            match constraint.constraint_type {
                ConstraintType::Exists | ConstraintType::NodeKey => {
                    for property in &constraint.properties {
                        if !properties.contains_key(property) {
                            return Err(StorageError::ConstraintMissingProperty {
                                constraint: constraint.name.clone(),
                                property: property.clone(),
                            });
                        }
                    }
                }
                ConstraintType::Unique => {
                    for property in &constraint.properties {
                        if let Some(value) = properties.get(property) {
                            let value_key = value.to_string();
                            let key = (label.to_string(), property.clone(), value_key.clone());
                            let mut unique = self.unique_values.write();
                            cleanup_stale_unique_values_for_node(
                                &mut unique,
                                label,
                                property,
                                &value_key,
                                node_id,
                            );
                            if let Some(existing) = unique.get(&key) {
                                if existing != node_id {
                                    return Err(StorageError::UniqueConstraintViolation {
                                        label: label.to_string(),
                                        property: property.clone(),
                                        value: value_key,
                                    });
                                }
                            } else {
                                unique.insert(key, node_id.to_string());
                            }
                        }
                    }
                }
                ConstraintType::Type | ConstraintType::Relationship => {}
            }
        }
        Ok(())
    }
}

fn cleanup_stale_unique_values_for_node(
    unique: &mut BTreeMap<(String, String, String), String>,
    label: &str,
    property: &str,
    new_value: &str,
    node_id: &str,
) {
    unique.retain(
        |(existing_label, existing_property, existing_value), existing_node| {
            !(existing_label == label
                && existing_property == property
                && existing_value != new_value
                && existing_node == node_id)
        },
    );
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageLayoutManifest {
    pub version: u8,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeRecord {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, serde_json::Value>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EdgeRecord {
    pub id: String,
    pub start_node: String,
    pub end_node: String,
    pub edge_type: String,
    pub properties: BTreeMap<String, serde_json::Value>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchPeerRecord {
    pub peer_id: String,
    pub endpoint: String,
    pub region: String,
    pub capacity_class: String,
    pub last_heartbeat_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HyperscalerProfile {
    pub profile_id: String,
    pub provider: String,
    pub region: String,
    pub tier: String,
    pub enabled: bool,
}

/// A single opened copperdb storage instance.
#[derive(Debug)]
pub struct StorageEngine {
    db: Db,
    meta: Tree,
    nodes: Tree,
    edges: Tree,
    indexes: Tree,
    temp_dir: Option<tempfile::TempDir>,
}

impl StorageEngine {
    /// Open (or create) a storage engine at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = sled::open(path)?;
        Self::open_with_db(db)
    }

    /// Open an in-memory (temporary) storage engine for testing.
    pub fn open_temporary() -> Result<Self, StorageError> {
        let temp_dir = tempfile::tempdir()?;
        let db = sled::open(temp_dir.path())?;
        let meta = db.open_tree("meta")?;
        let nodes = db.open_tree("nodes")?;
        let edges = db.open_tree("edges")?;
        let indexes = db.open_tree("indexes")?;
        let engine = Self {
            db,
            meta,
            nodes,
            edges,
            indexes,
            temp_dir: Some(temp_dir),
        };
        engine.ensure_layout_manifest()?;
        Ok(engine)
    }

    fn open_with_db(db: Db) -> Result<Self, StorageError> {
        let meta = db.open_tree("meta")?;
        let nodes = db.open_tree("nodes")?;
        let edges = db.open_tree("edges")?;
        let indexes = db.open_tree("indexes")?;
        let engine = Self {
            db,
            meta,
            nodes,
            edges,
            indexes,
            temp_dir: None,
        };
        engine.ensure_layout_manifest()?;
        Ok(engine)
    }

    fn ensure_layout_manifest(&self) -> Result<(), StorageError> {
        if let Some(raw) = self.meta.get(META_LAYOUT_MANIFEST_KEY)? {
            let manifest: StorageLayoutManifest = rmp_serde::from_slice(raw.as_ref())?;
            if manifest.version != STORAGE_LAYOUT_VERSION {
                return Err(StorageError::UnsupportedLayoutVersion {
                    expected: STORAGE_LAYOUT_VERSION,
                    actual: manifest.version,
                });
            }
            return Ok(());
        }

        let manifest = StorageLayoutManifest {
            version: STORAGE_LAYOUT_VERSION,
            created_at_unix_ms: now_unix_ms(),
        };
        self.meta
            .insert(META_LAYOUT_MANIFEST_KEY, rmp_serde::to_vec(&manifest)?)?;
        Ok(())
    }

    pub fn layout_manifest(&self) -> Result<StorageLayoutManifest, StorageError> {
        let raw = self
            .meta
            .get(META_LAYOUT_MANIFEST_KEY)?
            .ok_or_else(|| StorageError::NotFound("layout_manifest".to_string()))?;
        Ok(rmp_serde::from_slice(raw.as_ref())?)
    }

    pub fn storage_layout_version(&self) -> Result<u8, StorageError> {
        Ok(self.layout_manifest()?.version)
    }

    pub fn is_temporary(&self) -> bool {
        self.temp_dir.is_some()
    }

    // --- Compatibility node operations (raw bytes) ---

    /// Store a node's serialized properties.
    pub fn put_node(&self, id: &str, value: &[u8]) -> Result<(), StorageError> {
        self.nodes.insert(id.as_bytes(), value)?;
        Ok(())
    }

    /// Retrieve a node's serialized properties.
    pub fn get_node(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.nodes.get(id.as_bytes())?.map(|v| v.to_vec()))
    }

    /// Delete a node.
    pub fn delete_node(&self, id: &str) -> Result<(), StorageError> {
        self.nodes.remove(id.as_bytes())?;
        Ok(())
    }

    /// Iterate over all nodes with a given prefix.
    pub fn scan_nodes_with_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(Bytes, Bytes), StorageError>> + '_ {
        self.nodes.scan_prefix(prefix.as_bytes()).map(|res| {
            res.map(|(k, v)| (Bytes::from(k.to_vec()), Bytes::from(v.to_vec())))
                .map_err(StorageError::from)
        })
    }

    // --- Compatibility edge operations (raw bytes) ---

    /// Store an edge's serialized properties.
    pub fn put_edge(&self, id: &str, value: &[u8]) -> Result<(), StorageError> {
        self.edges.insert(id.as_bytes(), value)?;
        Ok(())
    }

    /// Retrieve an edge's serialized properties.
    pub fn get_edge(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.edges.get(id.as_bytes())?.map(|v| v.to_vec()))
    }

    /// Delete an edge.
    pub fn delete_edge(&self, id: &str) -> Result<(), StorageError> {
        self.edges.remove(id.as_bytes())?;
        Ok(())
    }

    // --- Structured node/edge APIs (storage v0 baseline) ---

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<(), StorageError> {
        if let Some(old) = self.get_node_record(&node.id)? {
            self.unindex_node_labels(&old)?;
        }
        self.nodes
            .insert(node.id.as_bytes(), rmp_serde::to_vec(node)?)?;
        self.index_node_labels(node)?;
        Ok(())
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        match self.nodes.get(id.as_bytes())? {
            Some(v) => Ok(Some(rmp_serde::from_slice(v.as_ref())?)),
            None => Ok(None),
        }
    }

    pub fn delete_node_record(&self, id: &str) -> Result<(), StorageError> {
        if let Some(existing) = self.get_node_record(id)? {
            self.unindex_node_labels(&existing)?;
            self.nodes.remove(id.as_bytes())?;
        }
        Ok(())
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeRecord>, StorageError> {
        let prefix = label_index_prefix(label);
        let mut out = Vec::new();

        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if let Some(node_id) = key_str.rsplit('/').next() {
                if let Some(node) = self.get_node_record(node_id)? {
                    out.push(node);
                }
            }
        }

        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub fn node_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        Ok(self.nodes.scan_prefix(prefix.as_bytes()).count() as u64)
    }

    pub fn put_edge_record(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        if let Some(old) = self.get_edge_record(&edge.id)? {
            self.unindex_edge_type(&old)?;
        }
        self.edges
            .insert(edge.id.as_bytes(), rmp_serde::to_vec(edge)?)?;
        self.index_edge_type(edge)?;
        Ok(())
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        match self.edges.get(id.as_bytes())? {
            Some(v) => Ok(Some(rmp_serde::from_slice(v.as_ref())?)),
            None => Ok(None),
        }
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<(), StorageError> {
        if let Some(existing) = self.get_edge_record(id)? {
            self.unindex_edge_type(&existing)?;
            self.edges.remove(id.as_bytes())?;
        }
        Ok(())
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        let prefix = edge_type_index_prefix(edge_type);
        let mut out = Vec::new();

        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if let Some(edge_id) = key_str.rsplit('/').next() {
                if let Some(edge) = self.get_edge_record(edge_id)? {
                    out.push(edge);
                }
            }
        }

        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub fn edge_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        Ok(self.edges.scan_prefix(prefix.as_bytes()).count() as u64)
    }

    pub fn list_namespaces(&self) -> Result<Vec<String>, StorageError> {
        let mut out = BTreeSet::new();

        for kv in self.nodes.iter() {
            let (k, _) = kv?;
            if let Some(ns) = namespace_from_id(k.as_ref()) {
                out.insert(ns);
            }
        }

        for kv in self.edges.iter() {
            let (k, _) = kv?;
            if let Some(ns) = namespace_from_id(k.as_ref()) {
                out.insert(ns);
            }
        }

        Ok(out.into_iter().collect())
    }

    // --- Distributed search mesh / hyperscaler metadata baselines ---

    pub fn register_search_peer(&self, peer: &SearchPeerRecord) -> Result<(), StorageError> {
        let key = [META_SEARCH_PEER_PREFIX, peer.peer_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(peer)?)?;
        Ok(())
    }

    pub fn list_search_peers(&self) -> Result<Vec<SearchPeerRecord>, StorageError> {
        let mut peers: Vec<SearchPeerRecord> = Vec::new();
        for entry in self.meta.scan_prefix(META_SEARCH_PEER_PREFIX) {
            let (_, v) = entry?;
            peers.push(rmp_serde::from_slice(v.as_ref())?);
        }
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        Ok(peers)
    }

    pub fn register_hyperscaler_profile(
        &self,
        profile: &HyperscalerProfile,
    ) -> Result<(), StorageError> {
        let key = [
            META_HYPERSCALER_PROFILE_PREFIX,
            profile.profile_id.as_bytes(),
        ]
        .concat();
        self.meta.insert(key, rmp_serde::to_vec(profile)?)?;
        Ok(())
    }

    pub fn list_hyperscaler_profiles(&self) -> Result<Vec<HyperscalerProfile>, StorageError> {
        let mut profiles: Vec<HyperscalerProfile> = Vec::new();
        for entry in self.meta.scan_prefix(META_HYPERSCALER_PROFILE_PREFIX) {
            let (_, v) = entry?;
            profiles.push(rmp_serde::from_slice(v.as_ref())?);
        }
        profiles.sort_by(|a, b| a.profile_id.cmp(&b.profile_id));
        Ok(profiles)
    }

    pub fn persist_constraint(&self, constraint: &Constraint) -> Result<(), StorageError> {
        let key = [META_SCHEMA_CONSTRAINT_PREFIX, constraint.name.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(constraint)?)?;
        Ok(())
    }

    pub fn load_constraints(&self) -> Result<Vec<Constraint>, StorageError> {
        let mut constraints: Vec<Constraint> = Vec::new();
        for entry in self.meta.scan_prefix(META_SCHEMA_CONSTRAINT_PREFIX) {
            let (_, value) = entry?;
            constraints.push(rmp_serde::from_slice(value.as_ref())?);
        }
        constraints.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(constraints)
    }

    pub fn delete_constraint(&self, name: &str) -> Result<bool, StorageError> {
        let key = [META_SCHEMA_CONSTRAINT_PREFIX, name.as_bytes()].concat();
        Ok(self.meta.remove(key)?.is_some())
    }

    pub fn persist_index_definition(&self, index: &IndexDefinition) -> Result<(), StorageError> {
        let key = [META_SCHEMA_INDEX_PREFIX, index.name.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(index)?)?;
        Ok(())
    }

    pub fn load_index_definitions(&self) -> Result<Vec<IndexDefinition>, StorageError> {
        let mut indexes: Vec<IndexDefinition> = Vec::new();
        for entry in self.meta.scan_prefix(META_SCHEMA_INDEX_PREFIX) {
            let (_, value) = entry?;
            indexes.push(rmp_serde::from_slice(value.as_ref())?);
        }
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(indexes)
    }

    pub fn delete_index_definition(&self, name: &str) -> Result<bool, StorageError> {
        let key = [META_SCHEMA_INDEX_PREFIX, name.as_bytes()].concat();
        Ok(self.meta.remove(key)?.is_some())
    }

    pub fn persist_decay_profile_schema(
        &self,
        profile: &DecayProfileSchema,
    ) -> Result<(), StorageError> {
        validate_decay_profile(profile)?;
        let key = [META_KP_DECAY_PROFILE_PREFIX, profile.name.as_bytes()].concat();
        if self.meta.get(&key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "decay profile {}",
                profile.name
            )));
        }
        self.meta.insert(key, rmp_serde::to_vec(profile)?)?;
        Ok(())
    }

    pub fn load_decay_profile_schemas(&self) -> Result<Vec<DecayProfileSchema>, StorageError> {
        let mut profiles: Vec<DecayProfileSchema> = Vec::new();
        for entry in self.meta.scan_prefix(META_KP_DECAY_PROFILE_PREFIX) {
            let (_, value) = entry?;
            profiles.push(rmp_serde::from_slice(value.as_ref())?);
        }
        profiles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(profiles)
    }

    pub fn alter_decay_profile_schema(
        &self,
        name: &str,
        updates: &BTreeMap<String, serde_json::Value>,
    ) -> Result<(), StorageError> {
        let key = [META_KP_DECAY_PROFILE_PREFIX, name.as_bytes()].concat();
        let raw = self
            .meta
            .get(&key)?
            .ok_or_else(|| StorageError::KnowledgePolicyNotFound(format!("decay profile {}", name)))?;
        let mut profile: DecayProfileSchema = rmp_serde::from_slice(raw.as_ref())?;
        for (k, v) in updates {
            match k.as_str() {
                "halfLifeSeconds" => {
                    profile.half_life_seconds = value_as_i64(v, "halfLifeSeconds")?;
                }
                "visibilityThreshold" => {
                    profile.visibility_threshold = value_as_f64(v, "visibilityThreshold")?;
                }
                "scoreFloor" => {
                    profile.score_floor = value_as_f64(v, "scoreFloor")?;
                }
                "function" => {
                    profile.function = value_as_string(v, "function")?;
                }
                "scope" => {
                    profile.scope = value_as_string(v, "scope")?;
                }
                "decayEnabled" => {
                    profile.decay_enabled = value_as_bool(v, "decayEnabled")?;
                }
                "scoreFrom" => {
                    profile.score_from = value_as_string(v, "scoreFrom")?;
                }
                "scoreFromProperty" => {
                    profile.score_from_property = Some(value_as_string(v, "scoreFromProperty")?);
                }
                "enabled" => {
                    profile.enabled = value_as_bool(v, "enabled")?;
                }
                other => {
                    return Err(StorageError::KnowledgePolicyInvalid(format!(
                        "unknown option '{}'",
                        other
                    )));
                }
            }
        }
        validate_decay_profile(&profile)?;
        self.meta.insert(key, rmp_serde::to_vec(&profile)?)?;
        Ok(())
    }

    pub fn delete_decay_profile_schema(
        &self,
        name: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        for policy in self.load_promotion_policy_schemas()? {
            for clause in policy.when_clauses {
                if clause.profile_ref == name {
                    return Err(StorageError::KnowledgePolicyInUse(format!(
                        "decay profile {} referenced by promotion policy {}",
                        name, policy.name
                    )));
                }
            }
        }
        let key = [META_KP_DECAY_PROFILE_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.remove(key)?.is_some();
        if !deleted && !if_exists {
            return Err(StorageError::KnowledgePolicyNotFound(format!(
                "decay profile {}",
                name
            )));
        }
        Ok(())
    }

    pub fn persist_promotion_profile_schema(
        &self,
        profile: &PromotionProfileSchema,
    ) -> Result<(), StorageError> {
        validate_promotion_profile(profile)?;
        let key = [META_KP_PROMOTION_PROFILE_PREFIX, profile.name.as_bytes()].concat();
        if self.meta.get(&key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "promotion profile {}",
                profile.name
            )));
        }
        self.meta.insert(key, rmp_serde::to_vec(profile)?)?;
        Ok(())
    }

    pub fn load_promotion_profile_schemas(
        &self,
    ) -> Result<Vec<PromotionProfileSchema>, StorageError> {
        let mut profiles: Vec<PromotionProfileSchema> = Vec::new();
        for entry in self.meta.scan_prefix(META_KP_PROMOTION_PROFILE_PREFIX) {
            let (_, value) = entry?;
            profiles.push(rmp_serde::from_slice(value.as_ref())?);
        }
        profiles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(profiles)
    }

    pub fn alter_promotion_profile_schema(
        &self,
        name: &str,
        updates: &BTreeMap<String, serde_json::Value>,
    ) -> Result<(), StorageError> {
        let key = [META_KP_PROMOTION_PROFILE_PREFIX, name.as_bytes()].concat();
        let raw = self.meta.get(&key)?.ok_or_else(|| {
            StorageError::KnowledgePolicyNotFound(format!("promotion profile {}", name))
        })?;
        let mut profile: PromotionProfileSchema = rmp_serde::from_slice(raw.as_ref())?;
        for (k, v) in updates {
            match k.as_str() {
                "multiplier" => profile.multiplier = value_as_f64(v, "multiplier")?,
                "scoreFloor" => profile.score_floor = value_as_f64(v, "scoreFloor")?,
                "scoreCap" => profile.score_cap = value_as_f64(v, "scoreCap")?,
                "scope" => profile.scope = value_as_string(v, "scope")?,
                "enabled" => profile.enabled = value_as_bool(v, "enabled")?,
                other => {
                    return Err(StorageError::KnowledgePolicyInvalid(format!(
                        "unknown option '{}'",
                        other
                    )));
                }
            }
        }
        validate_promotion_profile(&profile)?;
        self.meta.insert(key, rmp_serde::to_vec(&profile)?)?;
        Ok(())
    }

    pub fn delete_promotion_profile_schema(
        &self,
        name: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        for policy in self.load_promotion_policy_schemas()? {
            for clause in policy.when_clauses {
                if clause.profile_ref == name {
                    return Err(StorageError::KnowledgePolicyInUse(format!(
                        "promotion profile {} referenced by promotion policy {}",
                        name, policy.name
                    )));
                }
            }
        }
        let key = [META_KP_PROMOTION_PROFILE_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.remove(key)?.is_some();
        if !deleted && !if_exists {
            return Err(StorageError::KnowledgePolicyNotFound(format!(
                "promotion profile {}",
                name
            )));
        }
        Ok(())
    }

    pub fn persist_promotion_policy_schema(
        &self,
        policy: &PromotionPolicySchema,
    ) -> Result<(), StorageError> {
        validate_promotion_policy(policy, &self.load_promotion_profile_schemas()?)?;
        let key = [META_KP_PROMOTION_POLICY_PREFIX, policy.name.as_bytes()].concat();
        if self.meta.get(&key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "promotion policy {}",
                policy.name
            )));
        }
        self.meta.insert(key, rmp_serde::to_vec(policy)?)?;
        Ok(())
    }

    pub fn load_promotion_policy_schemas(&self) -> Result<Vec<PromotionPolicySchema>, StorageError> {
        let mut policies: Vec<PromotionPolicySchema> = Vec::new();
        for entry in self.meta.scan_prefix(META_KP_PROMOTION_POLICY_PREFIX) {
            let (_, value) = entry?;
            policies.push(rmp_serde::from_slice(value.as_ref())?);
        }
        policies.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(policies)
    }

    pub fn alter_promotion_policy_schema(
        &self,
        name: &str,
        updates: &BTreeMap<String, serde_json::Value>,
    ) -> Result<(), StorageError> {
        let key = [META_KP_PROMOTION_POLICY_PREFIX, name.as_bytes()].concat();
        let raw = self.meta.get(&key)?.ok_or_else(|| {
            StorageError::KnowledgePolicyNotFound(format!("promotion policy {}", name))
        })?;
        let mut policy: PromotionPolicySchema = rmp_serde::from_slice(raw.as_ref())?;
        for (k, v) in updates {
            match k.as_str() {
                "enabled" => policy.enabled = value_as_bool(v, "enabled")?,
                other => {
                    return Err(StorageError::KnowledgePolicyInvalid(format!(
                        "unknown option '{}'",
                        other
                    )));
                }
            }
        }
        validate_promotion_policy(&policy, &self.load_promotion_profile_schemas()?)?;
        self.meta.insert(key, rmp_serde::to_vec(&policy)?)?;
        Ok(())
    }

    pub fn delete_promotion_policy_schema(
        &self,
        name: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        let key = [META_KP_PROMOTION_POLICY_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.remove(key)?.is_some();
        if !deleted && !if_exists {
            return Err(StorageError::KnowledgePolicyNotFound(format!(
                "promotion policy {}",
                name
            )));
        }
        Ok(())
    }

    // --- Generic index operations ---

    /// Store an index entry.
    pub fn put_index(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.indexes.insert(key, value)?;
        Ok(())
    }

    /// Get an index entry.
    pub fn get_index(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.indexes.get(key)?.map(|v| v.to_vec()))
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush()?;
        Ok(())
    }

    /// Acquire a flush guard for call-site parity with NornicDB's async engine.
    pub fn hold_flush(&self) -> FlushGuard {
        FlushGuard
    }

    /// Return the on-disk size in bytes.
    pub fn size_on_disk(&self) -> u64 {
        self.db.size_on_disk().unwrap_or(0)
    }

    fn index_node_labels(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for label in &node.labels {
            self.indexes
                .insert(label_index_key(label, &node.id).as_bytes(), &[])?;
        }
        Ok(())
    }

    fn unindex_node_labels(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for label in &node.labels {
            self.indexes
                .remove(label_index_key(label, &node.id).as_bytes())?;
        }
        Ok(())
    }

    fn index_edge_type(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes.insert(
            edge_type_index_key(&edge.edge_type, &edge.id).as_bytes(),
            &[],
        )?;
        Ok(())
    }

    fn unindex_edge_type(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes
            .remove(edge_type_index_key(&edge.edge_type, &edge.id).as_bytes())?;
        Ok(())
    }
}

/// A RAII guard that signals "no flush should occur while I am alive".
pub struct FlushGuard;

impl Drop for FlushGuard {
    fn drop(&mut self) {
        // No-op: sled handles durability internally.
    }
}

fn label_index_prefix(label: &str) -> String {
    format!("{IDX_LABEL_PREFIX}/{label}/")
}

fn label_index_key(label: &str, node_id: &str) -> String {
    format!("{}{}", label_index_prefix(label), node_id)
}

fn edge_type_index_prefix(edge_type: &str) -> String {
    format!("{IDX_EDGE_TYPE_PREFIX}/{edge_type}/")
}

fn edge_type_index_key(edge_type: &str, edge_id: &str) -> String {
    format!("{}{}", edge_type_index_prefix(edge_type), edge_id)
}

fn value_as_f64(value: &serde_json::Value, field: &str) -> Result<f64, StorageError> {
    value
        .as_f64()
        .ok_or_else(|| StorageError::KnowledgePolicyInvalid(format!("{} must be a number", field)))
}

fn value_as_i64(value: &serde_json::Value, field: &str) -> Result<i64, StorageError> {
    value
        .as_i64()
        .ok_or_else(|| StorageError::KnowledgePolicyInvalid(format!("{} must be an integer", field)))
}

fn value_as_bool(value: &serde_json::Value, field: &str) -> Result<bool, StorageError> {
    value
        .as_bool()
        .ok_or_else(|| StorageError::KnowledgePolicyInvalid(format!("{} must be a boolean", field)))
}

fn value_as_string(value: &serde_json::Value, field: &str) -> Result<String, StorageError> {
    value
        .as_str()
        .map(|v| v.to_string())
        .ok_or_else(|| StorageError::KnowledgePolicyInvalid(format!("{} must be a string", field)))
}

fn validate_scope(scope: &str) -> bool {
    matches!(scope.to_ascii_uppercase().as_str(), "NODE" | "EDGE")
}

fn validate_decay_function(function: &str) -> bool {
    matches!(
        function.to_ascii_lowercase().as_str(),
        "exponential" | "linear" | "step" | "none"
    )
}

fn validate_score_from(mode: &str) -> bool {
    matches!(
        mode.to_ascii_uppercase().as_str(),
        "CREATED" | "LAST_ACCESSED" | "VERSION" | "CUSTOM"
    )
}

fn validate_decay_profile(profile: &DecayProfileSchema) -> Result<(), StorageError> {
    if profile.name.trim().is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "decay profile name is required".into(),
        ));
    }
    if !validate_decay_function(&profile.function) {
        return Err(StorageError::KnowledgePolicyInvalid(format!(
            "invalid decay function '{}'",
            profile.function
        )));
    }
    if !validate_scope(&profile.scope) {
        return Err(StorageError::KnowledgePolicyInvalid(format!(
            "invalid scope '{}'",
            profile.scope
        )));
    }
    if !validate_score_from(&profile.score_from) {
        return Err(StorageError::KnowledgePolicyInvalid(format!(
            "invalid scoreFrom '{}'",
            profile.score_from
        )));
    }
    if profile.visibility_threshold < 0.0 || profile.visibility_threshold > 1.0 {
        return Err(StorageError::KnowledgePolicyInvalid(
            "visibilityThreshold must be between 0 and 1".into(),
        ));
    }
    if profile.score_floor < 0.0 || profile.score_floor > 1.0 {
        return Err(StorageError::KnowledgePolicyInvalid(
            "scoreFloor must be between 0 and 1".into(),
        ));
    }
    if profile.score_from.eq_ignore_ascii_case("CUSTOM")
        && profile
            .score_from_property
            .as_ref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
    {
        return Err(StorageError::KnowledgePolicyInvalid(
            "scoreFromProperty is required when scoreFrom is CUSTOM".into(),
        ));
    }
    Ok(())
}

fn validate_promotion_profile(profile: &PromotionProfileSchema) -> Result<(), StorageError> {
    if profile.name.trim().is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "promotion profile name is required".into(),
        ));
    }
    if !validate_scope(&profile.scope) {
        return Err(StorageError::KnowledgePolicyInvalid(format!(
            "invalid scope '{}'",
            profile.scope
        )));
    }
    if profile.multiplier < 0.0 {
        return Err(StorageError::KnowledgePolicyInvalid(
            "multiplier must be non-negative".into(),
        ));
    }
    if profile.score_floor < 0.0 || profile.score_floor > 1.0 {
        return Err(StorageError::KnowledgePolicyInvalid(
            "scoreFloor must be between 0 and 1".into(),
        ));
    }
    if profile.score_cap < 0.0 || profile.score_cap > 1.0 {
        return Err(StorageError::KnowledgePolicyInvalid(
            "scoreCap must be between 0 and 1".into(),
        ));
    }
    Ok(())
}

fn validate_promotion_policy(
    policy: &PromotionPolicySchema,
    profiles: &[PromotionProfileSchema],
) -> Result<(), StorageError> {
    if policy.name.trim().is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "promotion policy name is required".into(),
        ));
    }
    if policy.target_labels.is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "promotion policy target labels are required".into(),
        ));
    }
    let profile_names: BTreeSet<&str> = profiles.iter().map(|p| p.name.as_str()).collect();
    for clause in &policy.when_clauses {
        if clause.profile_ref.trim().is_empty() {
            return Err(StorageError::KnowledgePolicyInvalid(
                "promotion WHEN clause requires profileRef".into(),
            ));
        }
        if !profile_names.contains(clause.profile_ref.as_str()) {
            return Err(StorageError::KnowledgePolicyInvalid(format!(
                "promotion profile '{}' not found",
                clause.profile_ref
            )));
        }
    }
    Ok(())
}

fn namespace_from_id(id: &[u8]) -> Option<String> {
    let id = std::str::from_utf8(id).ok()?;
    id.split_once(':').map(|(ns, _)| ns.to_string())
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    fn sample_node(id: &str, labels: &[&str]) -> NodeRecord {
        NodeRecord {
            id: id.to_string(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            properties: BTreeMap::from([
                ("name".to_string(), json!("alice")),
                ("score".to_string(), json!(42)),
            ]),
            created_at_unix_ms: 1000,
            updated_at_unix_ms: 2000,
        }
    }

    fn sample_edge(id: &str, t: &str, start: &str, end: &str) -> EdgeRecord {
        EdgeRecord {
            id: id.to_string(),
            start_node: start.to_string(),
            end_node: end.to_string(),
            edge_type: t.to_string(),
            properties: BTreeMap::from([("weight".to_string(), json!(0.9))]),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 456,
        }
    }

    #[test]
    fn creates_and_reads_layout_manifest_v0() {
        let engine = StorageEngine::open_temporary().unwrap();
        assert!(engine.is_temporary());
        let manifest = engine.layout_manifest().unwrap();
        assert_eq!(manifest.version, STORAGE_LAYOUT_VERSION);
        assert!(manifest.created_at_unix_ms > 0);
        assert_eq!(engine.storage_layout_version().unwrap(), 0);
    }

    #[test]
    fn rejects_non_v0_layout_manifest() {
        let test_dir = std::env::temp_dir().join(format!(
            "copperdb-storage-layout-version-rejection-test-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&test_dir).unwrap();
        let db = sled::open(&test_dir).unwrap();
        let meta = db.open_tree("meta").unwrap();

        let bad_manifest = StorageLayoutManifest {
            version: 1,
            created_at_unix_ms: 1,
        };
        meta.insert(
            META_LAYOUT_MANIFEST_KEY,
            rmp_serde::to_vec(&bad_manifest).unwrap(),
        )
        .unwrap();
        db.flush().unwrap();
        drop(meta);
        drop(db);

        let err = StorageEngine::open(&test_dir).err().unwrap();
        match err {
            StorageError::UnsupportedLayoutVersion { expected, actual } => {
                assert_eq!(expected, 0);
                assert_eq!(actual, 1);
            }
            _ => panic!("expected UnsupportedLayoutVersion"),
        }
        let _ = fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn compatibility_raw_node_edge_round_trip() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine.put_node("node:1", b"node_data").unwrap();
        engine.put_edge("edge:1", b"edge_data").unwrap();

        assert_eq!(
            engine.get_node("node:1").unwrap(),
            Some(b"node_data".to_vec())
        );
        assert_eq!(
            engine.get_edge("edge:1").unwrap(),
            Some(b"edge_data".to_vec())
        );

        engine.delete_node("node:1").unwrap();
        engine.delete_edge("edge:1").unwrap();

        assert!(engine.get_node("node:1").unwrap().is_none());
        assert!(engine.get_edge("edge:1").unwrap().is_none());
    }

    #[test]
    fn node_record_indexes_are_maintained_and_updated() {
        let engine = StorageEngine::open_temporary().unwrap();

        let mut node = sample_node("db1:n1", &["Person", "Employee"]);
        engine.put_node_record(&node).unwrap();

        let person_nodes = engine.get_nodes_by_label("Person").unwrap();
        assert_eq!(person_nodes.len(), 1);
        assert_eq!(person_nodes[0], node);

        let employee_nodes = engine.get_nodes_by_label("Employee").unwrap();
        assert_eq!(employee_nodes.len(), 1);
        assert_eq!(employee_nodes[0].id, "db1:n1");

        node.labels = vec!["Person".to_string(), "Founder".to_string()];
        node.updated_at_unix_ms = 3000;
        node.properties.insert("rank".to_string(), json!("A"));
        engine.put_node_record(&node).unwrap();

        let founder_nodes = engine.get_nodes_by_label("Founder").unwrap();
        assert_eq!(founder_nodes.len(), 1);
        assert_eq!(founder_nodes[0].properties.get("rank"), Some(&json!("A")));

        let stale_employee_nodes = engine.get_nodes_by_label("Employee").unwrap();
        assert!(stale_employee_nodes.is_empty());

        engine.delete_node_record("db1:n1").unwrap();
        assert!(engine.get_node_record("db1:n1").unwrap().is_none());
        assert!(engine.get_nodes_by_label("Person").unwrap().is_empty());
        assert!(engine.get_nodes_by_label("Founder").unwrap().is_empty());
    }

    #[test]
    fn edge_record_indexes_are_maintained_and_updated() {
        let engine = StorageEngine::open_temporary().unwrap();

        let mut edge = sample_edge("db1:e1", "KNOWS", "db1:n1", "db1:n2");
        engine.put_edge_record(&edge).unwrap();

        let knows = engine.get_edges_by_type("KNOWS").unwrap();
        assert_eq!(knows.len(), 1);
        assert_eq!(knows[0], edge);

        edge.edge_type = "MENTORS".to_string();
        edge.properties.insert("years".to_string(), json!(5));
        engine.put_edge_record(&edge).unwrap();

        assert!(engine.get_edges_by_type("KNOWS").unwrap().is_empty());
        let mentors = engine.get_edges_by_type("MENTORS").unwrap();
        assert_eq!(mentors.len(), 1);
        assert_eq!(mentors[0].properties.get("years"), Some(&json!(5)));

        engine.delete_edge_record("db1:e1").unwrap();
        assert!(engine.get_edge_record("db1:e1").unwrap().is_none());
        assert!(engine.get_edges_by_type("MENTORS").unwrap().is_empty());
    }

    #[test]
    fn prefix_scan_counts_and_namespace_listing_are_deterministic() {
        let engine = StorageEngine::open_temporary().unwrap();

        engine
            .put_node_record(&sample_node("alpha:n1", &["Person"]))
            .unwrap();
        engine
            .put_node_record(&sample_node("alpha:n2", &["Person"]))
            .unwrap();
        engine
            .put_node_record(&sample_node("beta:n1", &["Robot"]))
            .unwrap();

        engine
            .put_edge_record(&sample_edge("alpha:e1", "LINKS", "alpha:n1", "alpha:n2"))
            .unwrap();
        engine
            .put_edge_record(&sample_edge("beta:e1", "LINKS", "beta:n1", "beta:n2"))
            .unwrap();

        assert_eq!(engine.node_count_by_prefix("alpha:").unwrap(), 2);
        assert_eq!(engine.node_count_by_prefix("beta:").unwrap(), 1);
        assert_eq!(engine.edge_count_by_prefix("alpha:").unwrap(), 1);
        assert_eq!(engine.edge_count_by_prefix("beta:").unwrap(), 1);

        let namespaces = engine.list_namespaces().unwrap();
        assert_eq!(namespaces, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn distributed_mesh_and_hyperscaler_metadata_round_trip() {
        let engine = StorageEngine::open_temporary().unwrap();

        engine
            .register_search_peer(&SearchPeerRecord {
                peer_id: "peer-b".to_string(),
                endpoint: "https://b.mesh.local".to_string(),
                region: "us-west-2".to_string(),
                capacity_class: "medium".to_string(),
                last_heartbeat_unix_ms: 200,
            })
            .unwrap();
        engine
            .register_search_peer(&SearchPeerRecord {
                peer_id: "peer-a".to_string(),
                endpoint: "https://a.mesh.local".to_string(),
                region: "us-east-1".to_string(),
                capacity_class: "large".to_string(),
                last_heartbeat_unix_ms: 100,
            })
            .unwrap();

        let peers = engine.list_search_peers().unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].peer_id, "peer-a");
        assert_eq!(peers[1].peer_id, "peer-b");

        engine
            .register_hyperscaler_profile(&HyperscalerProfile {
                profile_id: "aws-primary".to_string(),
                provider: "aws".to_string(),
                region: "us-east-1".to_string(),
                tier: "prod".to_string(),
                enabled: true,
            })
            .unwrap();
        engine
            .register_hyperscaler_profile(&HyperscalerProfile {
                profile_id: "gcp-burst".to_string(),
                provider: "gcp".to_string(),
                region: "us-central1".to_string(),
                tier: "burst".to_string(),
                enabled: false,
            })
            .unwrap();

        let profiles = engine.list_hyperscaler_profiles().unwrap();
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].profile_id, "aws-primary");
        assert_eq!(profiles[1].profile_id, "gcp-burst");
        assert!(!profiles[1].enabled);
    }

    #[test]
    fn index_and_prefix_scan_apis_work() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine.put_index(b"idx:key", b"idx:value").unwrap();
        assert_eq!(
            engine.get_index(b"idx:key").unwrap(),
            Some(b"idx:value".to_vec())
        );

        engine.put_node("Person:1", b"alice").unwrap();
        engine.put_node("Person:2", b"bob").unwrap();
        engine.put_node("Movie:1", b"matrix").unwrap();

        let rows: Vec<_> = engine.scan_nodes_with_prefix("Person:").collect();
        assert_eq!(rows.len(), 2);
        let mut keys = rows
            .into_iter()
            .map(|r| String::from_utf8(r.unwrap().0.to_vec()).unwrap())
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, vec!["Person:1", "Person:2"]);
    }

    #[test]
    fn flush_guard_and_size_api_are_stable() {
        let engine = StorageEngine::open_temporary().unwrap();
        {
            let _guard = engine.hold_flush();
            engine.put_node("n1", b"v1").unwrap();
        }
        engine.flush().unwrap();
        assert!(engine.size_on_disk() <= u64::MAX);
    }

    #[test]
    fn mvcc_snapshot_isolation_and_pruning_work() {
        let mvcc = MvccStore::new();
        let snapshot0 = mvcc.begin_snapshot();
        assert_eq!(snapshot0.read_ts, 0);

        let v1 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"alice".to_vec()))]);
        assert_eq!(v1, 1);
        let snapshot1 = mvcc.begin_snapshot();

        let v2 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"bob".to_vec()))]);
        assert_eq!(v2, 2);
        let snapshot2 = mvcc.begin_snapshot();

        assert_eq!(mvcc.read(&snapshot0, "node:1"), None);
        assert_eq!(mvcc.read(&snapshot1, "node:1"), Some(b"alice".to_vec()));
        assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));

        mvcc.prune_versions_older_than(2);
        assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));
        let head = mvcc.head();
        assert_eq!(head.floor, 2);
        assert_eq!(head.head, 2);
    }

    #[test]
    fn mvcc_head_decode_errors_match_contract() {
        let err = MvccStore::decode_head(&[1, 2, 3]).unwrap_err();
        assert!(matches!(err, StorageError::MvccHeadTruncated(3)));
        assert_eq!(err.to_string(), "mvcc head truncated: 3 bytes");

        let err = MvccStore::decode_head(&[0; 10]).unwrap_err();
        assert!(matches!(err, StorageError::MvccHeadMissingFloor(10)));
        assert_eq!(err.to_string(), "mvcc head missing floor: 10 bytes");
    }

    #[test]
    fn wal_batch_replay_and_checksum_error_paths_work() {
        let wal = WAL::new(WALConfig {
            enabled: true,
            max_entries_per_segment: 2,
        });
        let (start, end) = wal
            .append_batch(vec![
                ("put".to_string(), "node:1".to_string(), b"a".to_vec()),
                ("put".to_string(), "node:2".to_string(), b"b".to_vec()),
                ("delete".to_string(), "node:1".to_string(), Vec::new()),
            ])
            .unwrap();
        assert_eq!((start, end), (1, 3));
        assert_eq!(wal.stats().segments, 2);

        let replay = wal.replay_after(1).unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].key, "node:2");
        assert_eq!(replay[1].op, "delete");

        wal.inject_corruption_for_test(2).unwrap();
        let err = wal.replay_after(0).unwrap_err();
        assert!(matches!(err, StorageError::WalChecksumVerificationFailed));
        assert!(wal.is_degraded());
    }

    #[test]
    fn wal_close_and_partial_write_errors_match_contract() {
        let wal = WAL::new(WALConfig::default());
        let err = wal.mark_partial_write_detected().unwrap_err();
        assert!(matches!(err, StorageError::WalPartialWriteDetected));
        assert_eq!(err.to_string(), "wal: partial write detected");

        wal.close();
        let err = wal.append("put", "node:1", b"x").unwrap_err();
        assert!(matches!(err, StorageError::WalClosed));
        assert_eq!(err.to_string(), "wal: closed");
    }

    #[test]
    fn schema_constraints_validate_and_persist() {
        let schema = SchemaManager::new();
        schema
            .add_constraint(Constraint {
                name: "person_email_unique".to_string(),
                constraint_type: ConstraintType::Unique,
                entity_type: ConstraintEntityType::Node,
                label: "Person".to_string(),
                properties: vec!["email".to_string()],
            })
            .unwrap();
        schema
            .add_constraint(Constraint {
                name: "person_email_exists".to_string(),
                constraint_type: ConstraintType::Exists,
                entity_type: ConstraintEntityType::Node,
                label: "Person".to_string(),
                properties: vec!["email".to_string()],
            })
            .unwrap();

        let missing = BTreeMap::new();
        let err = schema.validate_node("n1", "Person", &missing).unwrap_err();
        assert!(matches!(
            err,
            StorageError::ConstraintMissingProperty { .. }
        ));

        let mut alice = BTreeMap::new();
        alice.insert("email".to_string(), json!("alice@example.com"));
        schema.validate_node("n1", "Person", &alice).unwrap();

        let mut duplicate = BTreeMap::new();
        duplicate.insert("email".to_string(), json!("alice@example.com"));
        let err = schema
            .validate_node("n2", "Person", &duplicate)
            .unwrap_err();
        assert!(matches!(
            err,
            StorageError::UniqueConstraintViolation { .. }
        ));
        assert_eq!(
            err.to_string(),
            "Node(Person) already exists with email = \"alice@example.com\""
        );

        let mut updated = BTreeMap::new();
        updated.insert("email".to_string(), json!("alice+new@example.com"));
        schema.validate_node("n1", "Person", &updated).unwrap();
        schema.validate_node("n2", "Person", &duplicate).unwrap();

        let engine = StorageEngine::open_temporary().unwrap();
        for constraint in schema.list_constraints() {
            engine.persist_constraint(&constraint).unwrap();
        }
        let loaded = engine.load_constraints().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "person_email_exists");
        assert_eq!(loaded[1].name, "person_email_unique");
    }

    #[test]
    fn schema_index_definitions_roundtrip() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine
            .persist_index_definition(&IndexDefinition {
                name: "person_email_idx".to_string(),
                entity_type: IndexEntityType::Node,
                label: "Person".to_string(),
                properties: vec!["email".to_string()],
            })
            .unwrap();

        let indexes = engine.load_index_definitions().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "person_email_idx");

        let deleted = engine.delete_index_definition("person_email_idx").unwrap();
        assert!(deleted);
        assert!(engine.load_index_definitions().unwrap().is_empty());
    }

    #[test]
    fn knowledge_policy_decay_profile_roundtrip_and_update() {
        let engine = StorageEngine::open_temporary().unwrap();
        let profile = DecayProfileSchema {
            name: "slow_decay".to_string(),
            half_life_seconds: 604_800,
            visibility_threshold: 0.1,
            score_floor: 0.0,
            function: "exponential".to_string(),
            scope: "NODE".to_string(),
            decay_enabled: true,
            score_from: "CREATED".to_string(),
            score_from_property: None,
            enabled: true,
        };
        engine.persist_decay_profile_schema(&profile).unwrap();

        let mut updates = BTreeMap::new();
        updates.insert("visibilityThreshold".to_string(), json!(0.2));
        updates.insert("scoreFloor".to_string(), json!(0.05));
        engine
            .alter_decay_profile_schema("slow_decay", &updates)
            .unwrap();

        let loaded = engine.load_decay_profile_schemas().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "slow_decay");
        assert_eq!(loaded[0].visibility_threshold, 0.2);
        assert_eq!(loaded[0].score_floor, 0.05);
    }

    #[test]
    fn knowledge_policy_promotion_schema_roundtrip_and_reference_guards() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine
            .persist_promotion_profile_schema(&PromotionProfileSchema {
                name: "boost_profile".to_string(),
                scope: "NODE".to_string(),
                multiplier: 1.5,
                score_floor: 0.0,
                score_cap: 1.0,
                enabled: true,
            })
            .unwrap();

        engine
            .persist_promotion_policy_schema(&PromotionPolicySchema {
                name: "fact_policy".to_string(),
                target_labels: vec!["KnowledgeFact".to_string()],
                enabled: true,
                when_clauses: vec![PromotionWhenClauseSchema {
                    profile_ref: "boost_profile".to_string(),
                    predicate: "n.evidence >= 3".to_string(),
                    order: 1,
                }],
            })
            .unwrap();

        let err = engine
            .delete_promotion_profile_schema("boost_profile", false)
            .unwrap_err();
        assert!(matches!(err, StorageError::KnowledgePolicyInUse(_)));

        let mut updates = BTreeMap::new();
        updates.insert("enabled".to_string(), json!(false));
        engine
            .alter_promotion_policy_schema("fact_policy", &updates)
            .unwrap();
        let policies = engine.load_promotion_policy_schemas().unwrap();
        assert_eq!(policies.len(), 1);
        assert!(!policies[0].enabled);

        engine
            .delete_promotion_policy_schema("fact_policy", false)
            .unwrap();
        engine
            .delete_promotion_profile_schema("boost_profile", false)
            .unwrap();
        assert!(engine.load_promotion_policy_schemas().unwrap().is_empty());
        assert!(engine.load_promotion_profile_schemas().unwrap().is_empty());
    }
}
