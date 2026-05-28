//! Embedded key-value storage engine for copperdb.
//!
//! Storage layout policy for copper: **version 0 only**.
//! This crate intentionally avoids alternate layout migration arms and only supports
//! opening databases whose manifest declares layout version 0.

use bytes::Bytes;
use copperdb_encryption::{EnvelopeConfig, EnvelopeEncryptor};
use copperdb_kms::KeyProvider;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const STORAGE_LAYOUT_VERSION: u8 = 0;
const META_LAYOUT_MANIFEST_KEY: &[u8] = b"layout_manifest";
const META_ENCRYPTION_MANIFEST_KEY: &[u8] = b"encryption_manifest";
const META_TOPOLOGY_PEER_PREFIX: &[u8] = b"topology_peer/";
const META_TOPOLOGY_PROFILE_PREFIX: &[u8] = b"topology_profile/";
const META_TOPOLOGY_PLACEMENT_PREFIX: &[u8] = b"topology_placement/";
const META_FABRIC_DATABASE_PREFIX: &[u8] = b"fabric_database/";
const META_SCHEMA_CONSTRAINT_PREFIX: &[u8] = b"schema_constraint/";
const META_SCHEMA_INDEX_PREFIX: &[u8] = b"schema_index/";
const META_KP_DECAY_PROFILE_PREFIX: &[u8] = b"kp_decay_profile/";
const META_KP_DECAY_BINDING_PREFIX: &[u8] = b"kp_decay_binding/";
const META_KP_PROMOTION_PROFILE_PREFIX: &[u8] = b"kp_promotion_profile/";
const META_KP_PROMOTION_POLICY_PREFIX: &[u8] = b"kp_promotion_policy/";
const META_KP_ACCESS_METADATA_PREFIX: &[u8] = b"kp_access_metadata/";
const IDX_LABEL_PREFIX: &str = "label_nodes";
const IDX_EDGE_TYPE_PREFIX: &str = "edge_type";
const IDX_EDGE_START_PREFIX: &str = "edge_start";
const IDX_EDGE_END_PREFIX: &str = "edge_end";
const IDX_NODE_PROPERTY_PREFIX: &str = "node_property";

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
    #[error("topology invalid: {0}")]
    TopologyInvalid(String),
    #[error("storage encryption error: {0}")]
    Encryption(String),
    #[error("storage encryption is required for this database")]
    EncryptionRequired,
    #[error("storage encryption metadata mismatch: {0}")]
    EncryptionMismatch(String),
}

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
    pub floor: u64,
    pub head: u64,
    pub oldest_active_reader: Option<u64>,
    pub active_reader_count: u64,
    pub retained_versions: usize,
    pub prune_debt: usize,
    pub suggested_prune_floor: u64,
}

#[derive(Debug, Default)]
pub struct MvccStore {
    current_version: AtomicU64,
    floor: AtomicU64,
    values: RwLock<BTreeMap<String, Vec<MvccVersion>>>,
    active_readers: Arc<Mutex<BTreeMap<u64, u64>>>,
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
        let effective_min_version = self
            .oldest_active_reader()
            .map(|oldest_reader| min_version.min(oldest_reader))
            .unwrap_or(min_version);

        self.floor.store(effective_min_version, Ordering::SeqCst);
        let mut guard = self.values.write();
        for versions in guard.values_mut() {
            if versions.len() <= 1 {
                continue;
            }
            let keep_from = versions
                .iter()
                .position(|v| v.version >= effective_min_version)
                .unwrap_or(versions.len().saturating_sub(1));
            if keep_from > 0 {
                versions.drain(0..keep_from);
            }
        }
    }

    pub fn trigger_prune_now(&self, retain_last_n_versions: u64) -> usize {
        let head = self.current_version.load(Ordering::SeqCst);
        let requested_floor = head.saturating_sub(retain_last_n_versions);
        let effective_min_version = self.safe_prune_floor(requested_floor);

        self.floor.store(effective_min_version, Ordering::SeqCst);
        let mut removed_versions = 0usize;
        let mut guard = self.values.write();
        for versions in guard.values_mut() {
            if versions.len() <= 1 {
                continue;
            }
            let keep_from = versions
                .iter()
                .position(|v| v.version >= effective_min_version)
                .unwrap_or(versions.len().saturating_sub(1));
            if keep_from > 0 {
                removed_versions += keep_from;
                versions.drain(0..keep_from);
            }
        }

        removed_versions
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
        guard.values().map(Vec::len).sum()
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
            .map(|versions| {
                if versions.len() <= 1 {
                    return 0;
                }
                versions
                    .iter()
                    .position(|v| v.version >= candidate_floor)
                    .unwrap_or(versions.len().saturating_sub(1))
            })
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
    pub compacted_through: u64,
    pub next_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct WALDiskState {
    next_seq: u64,
    compacted_through: u64,
    entries: Vec<WALEntry>,
}

#[derive(Debug)]
pub struct WAL {
    config: WALConfig,
    path: Option<PathBuf>,
    next_seq: AtomicU64,
    compacted_through: AtomicU64,
    closed: AtomicBool,
    degraded: AtomicBool,
    entries: Mutex<Vec<WALEntry>>,
    segments: Mutex<Vec<WALSegment>>,
}

impl WAL {
    pub fn new(config: WALConfig) -> Self {
        Self {
            config,
            path: None,
            next_seq: AtomicU64::new(0),
            compacted_through: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            entries: Mutex::new(Vec::new()),
            segments: Mutex::new(Vec::new()),
        }
    }

    pub fn open(path: impl AsRef<Path>, config: WALConfig) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        let (entries, next_seq, compacted_through) = if path.exists() {
            let raw = fs::read(&path)?;
            if raw.is_empty() {
                (Vec::new(), 0, 0)
            } else {
                match rmp_serde::from_slice::<WALDiskState>(&raw) {
                    Ok(state) => (state.entries, state.next_seq, state.compacted_through),
                    Err(_) => {
                        let entries = rmp_serde::from_slice::<Vec<WALEntry>>(&raw)
                            .map_err(|_| StorageError::WalMissingOrInvalidTrailer)?;
                        let next_seq = entries.iter().map(|entry| entry.seq).max().unwrap_or(0);
                        (entries, next_seq, 0)
                    }
                }
            }
        } else {
            (Vec::new(), 0, 0)
        };
        let wal = Self {
            config,
            path: Some(path),
            next_seq: AtomicU64::new(next_seq),
            compacted_through: AtomicU64::new(compacted_through),
            closed: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            entries: Mutex::new(entries),
            segments: Mutex::new(Vec::new()),
        };
        wal.verify_entries()?;
        wal.recompute_segments(wal.entries.lock().len());
        Ok(wal)
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
            self.persist_entries(&entries)?;
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
        if !self.config.enabled {
            return Ok((0, 0));
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
        self.persist_entries(&entries)?;
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

    pub fn compacted_through(&self) -> u64 {
        self.compacted_through.load(Ordering::SeqCst)
    }

    pub fn compact_up_to(&self, last_included_seq: u64) -> Result<usize, StorageError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::WalClosed);
        }
        if !self.config.enabled {
            let prior = self.compacted_through.load(Ordering::SeqCst);
            self.compacted_through
                .store(prior.max(last_included_seq), Ordering::SeqCst);
            return Ok(0);
        }

        let next_seq = self.next_seq.load(Ordering::SeqCst);
        let effective_seq = last_included_seq.min(next_seq);
        let prior = self.compacted_through.load(Ordering::SeqCst);
        self.compacted_through
            .store(prior.max(effective_seq), Ordering::SeqCst);
        let removed = {
            let mut entries = self.entries.lock();
            let remove_until = entries.partition_point(|entry| entry.seq <= effective_seq);
            if remove_until > 0 {
                entries.drain(0..remove_until);
            }
            self.persist_entries(&entries)?;
            self.recompute_segments(entries.len());
            remove_until
        };
        Ok(removed)
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> WALStats {
        WALStats {
            entries: self.entries.lock().len(),
            segments: self.segments.lock().len(),
            degraded: self.is_degraded(),
            compacted_through: self.compacted_through(),
            next_seq: self.next_seq.load(Ordering::SeqCst),
        }
    }

    fn verify_entries(&self) -> Result<(), StorageError> {
        let entries = self.entries.lock();
        for entry in entries.iter() {
            let expected = wal_checksum(&entry.op, &entry.key, &entry.payload);
            if entry.checksum != expected {
                self.degraded.store(true, Ordering::SeqCst);
                return Err(StorageError::WalChecksumVerificationFailed);
            }
        }
        Ok(())
    }

    fn persist_entries(&self, entries: &[WALEntry]) -> Result<(), StorageError> {
        if !self.config.enabled {
            return Ok(());
        }
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("tmp");
        let state = WALDiskState {
            next_seq: self.next_seq.load(Ordering::SeqCst),
            compacted_through: self.compacted_through(),
            entries: entries.to_vec(),
        };
        fs::write(&tmp_path, rmp_serde::to_vec(&state)?)?;
        fs::rename(tmp_path, path)?;
        Ok(())
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
pub struct DecayProfileBindingSchema {
    pub name: String,
    pub target_labels: Vec<String>,
    pub target_edge_type: Option<String>,
    pub is_wildcard: bool,
    pub is_edge: bool,
    pub profile_ref: Option<String>,
    pub no_decay: bool,
    pub visibility_threshold: Option<f64>,
    pub order: i64,
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
pub enum PromotionOnAccessMutationKindSchema {
    SetLastAccessedNow,
    IncrementAccessCount,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionOnAccessMutationSchema {
    pub kind: PromotionOnAccessMutationKindSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionPolicySchema {
    pub name: String,
    pub target_labels: Vec<String>,
    pub target_edge_type: Option<String>,
    pub is_wildcard: bool,
    pub is_edge: bool,
    pub enabled: bool,
    pub on_access_mutations: Vec<PromotionOnAccessMutationSchema>,
    pub when_clauses: Vec<PromotionWhenClauseSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct KnowledgePolicyAccessMetadata {
    pub last_accessed_at_unix_ms: Option<i64>,
    pub access_count: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageEncryptionManifest {
    pub version: u8,
    pub algorithm: String,
    pub key_uri: String,
    pub created_at_unix_ms: i64,
}

pub const STORAGE_ENCRYPTION_MANIFEST_VERSION: u8 = 1;

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

/// A single opened copperdb storage instance.
#[derive(Debug)]
pub struct StorageEngine {
    db: Db,
    meta: Tree,
    nodes: Tree,
    edges: Tree,
    indexes: Tree,
    encryption: Option<StorageEncryption>,
    temp_dir: Option<tempfile::TempDir>,
}

struct StorageEncryption {
    encryptor: EnvelopeEncryptor,
    runtime: tokio::runtime::Runtime,
    key_uri: String,
}

impl fmt::Debug for StorageEncryption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageEncryption")
            .field("key_uri", &self.key_uri)
            .finish_non_exhaustive()
    }
}

impl StorageEncryption {
    fn new(provider: Arc<dyn KeyProvider>, key_uri: String) -> Result<Self, StorageError> {
        let encryptor = EnvelopeEncryptor::new(
            provider,
            EnvelopeConfig {
                associated_data: b"copperdb-storage-v0".to_vec(),
                ..Default::default()
            },
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| StorageError::Encryption(err.to_string()))?;
        Ok(Self {
            encryptor,
            runtime,
            key_uri,
        })
    }

    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, StorageError> {
        self.runtime
            .block_on(self.encryptor.encrypt(plaintext))
            .map_err(|err| StorageError::Encryption(err.to_string()))
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, StorageError> {
        self.runtime
            .block_on(self.encryptor.decrypt(ciphertext))
            .map_err(|err| StorageError::Encryption(err.to_string()))
    }
}

impl StorageEngine {
    /// Open (or create) a storage engine at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = sled::open(path)?;
        Self::open_with_db(db, None)
    }

    /// Open (or create) a storage engine whose graph records are encrypted using
    /// provider-backed envelope encryption.
    pub fn open_encrypted(
        path: impl AsRef<Path>,
        provider: Arc<dyn KeyProvider>,
        key_uri: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let db = sled::open(path)?;
        let encryption = StorageEncryption::new(provider, key_uri.into())?;
        Self::open_with_db(db, Some(encryption))
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
            encryption: None,
            temp_dir: Some(temp_dir),
        };
        engine.ensure_layout_manifest()?;
        engine.ensure_encryption_manifest()?;
        Ok(engine)
    }

    fn open_with_db(db: Db, encryption: Option<StorageEncryption>) -> Result<Self, StorageError> {
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
            encryption,
            temp_dir: None,
        };
        engine.ensure_layout_manifest()?;
        engine.ensure_encryption_manifest()?;
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

    fn ensure_encryption_manifest(&self) -> Result<(), StorageError> {
        match (
            self.meta.get(META_ENCRYPTION_MANIFEST_KEY)?,
            &self.encryption,
        ) {
            (Some(raw), Some(encryption)) => {
                let manifest: StorageEncryptionManifest = rmp_serde::from_slice(raw.as_ref())?;
                if manifest.version != STORAGE_ENCRYPTION_MANIFEST_VERSION {
                    return Err(StorageError::EncryptionMismatch(format!(
                        "expected manifest version {STORAGE_ENCRYPTION_MANIFEST_VERSION}, got {}",
                        manifest.version
                    )));
                }
                if manifest.algorithm != copperdb_encryption::ALGORITHM_AES_256_GCM {
                    return Err(StorageError::EncryptionMismatch(format!(
                        "unsupported algorithm {}",
                        manifest.algorithm
                    )));
                }
                if manifest.key_uri != encryption.key_uri {
                    return Err(StorageError::EncryptionMismatch(format!(
                        "expected key URI {}, got {}",
                        manifest.key_uri, encryption.key_uri
                    )));
                }
                Ok(())
            }
            (Some(_), None) => Err(StorageError::EncryptionRequired),
            (None, Some(encryption)) => {
                let manifest = StorageEncryptionManifest {
                    version: STORAGE_ENCRYPTION_MANIFEST_VERSION,
                    algorithm: copperdb_encryption::ALGORITHM_AES_256_GCM.into(),
                    key_uri: encryption.key_uri.clone(),
                    created_at_unix_ms: now_unix_ms(),
                };
                self.meta
                    .insert(META_ENCRYPTION_MANIFEST_KEY, rmp_serde::to_vec(&manifest)?)?;
                Ok(())
            }
            (None, None) => Ok(()),
        }
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

    pub fn is_encrypted(&self) -> bool {
        self.encryption.is_some()
    }

    pub fn encryption_manifest(&self) -> Result<Option<StorageEncryptionManifest>, StorageError> {
        match self.meta.get(META_ENCRYPTION_MANIFEST_KEY)? {
            Some(raw) => Ok(Some(rmp_serde::from_slice(raw.as_ref())?)),
            None => Ok(None),
        }
    }

    fn encode_record_bytes(&self, plaintext: Vec<u8>) -> Result<Vec<u8>, StorageError> {
        match &self.encryption {
            Some(encryption) => encryption.encrypt(&plaintext),
            None => Ok(plaintext),
        }
    }

    fn decode_record_bytes(&self, stored: &[u8]) -> Result<Vec<u8>, StorageError> {
        match &self.encryption {
            Some(encryption) => encryption.decrypt(stored),
            None => Ok(stored.to_vec()),
        }
    }

    // --- Raw node operations ---

    /// Store a node's serialized properties.
    pub fn put_node(&self, id: &str, value: &[u8]) -> Result<(), StorageError> {
        if let Some(node) = compat_node_record_from_bytes(id, value)? {
            return self.put_node_record(&node);
        }

        self.nodes
            .insert(id.as_bytes(), self.encode_record_bytes(value.to_vec())?)?;
        Ok(())
    }

    /// Retrieve a node's serialized properties.
    pub fn get_node(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match self.nodes.get(id.as_bytes())? {
            Some(v) => {
                let raw = self.decode_record_bytes(v.as_ref())?;
                if let Some(node) = compat_node_record_from_bytes(id, &raw)? {
                    return Ok(Some(rmp_serde::to_vec(&node_record_to_legacy_props(&node))?));
                }
                Ok(Some(raw))
            }
            None => Ok(None),
        }
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
            let (k, v) = res.map_err(StorageError::from)?;
            let key = std::str::from_utf8(k.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            let raw = self.decode_record_bytes(v.as_ref())?;
            let value = if let Some(node) = compat_node_record_from_bytes(key, &raw)? {
                rmp_serde::to_vec(&node_record_to_legacy_props(&node))?
            } else {
                raw
            };
            Ok((
                Bytes::from(k.to_vec()),
                Bytes::from(value),
            ))
        })
    }

    // --- Raw edge operations ---

    /// Store an edge's serialized properties.
    pub fn put_edge(&self, id: &str, value: &[u8]) -> Result<(), StorageError> {
        self.edges
            .insert(id.as_bytes(), self.encode_record_bytes(value.to_vec())?)?;
        Ok(())
    }

    /// Retrieve an edge's serialized properties.
    pub fn get_edge(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.edges
            .get(id.as_bytes())?
            .map(|v| self.decode_record_bytes(v.as_ref()))
            .transpose()
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
            self.unindex_node_properties(&old)?;
        }
        self.nodes.insert(
            node.id.as_bytes(),
            self.encode_record_bytes(rmp_serde::to_vec(node)?)?,
        )?;
        self.index_node_labels(node)?;
        self.index_node_properties(node)?;
        Ok(())
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        match self.nodes.get(id.as_bytes())? {
            Some(v) => compat_node_record_from_bytes(id, self.decode_record_bytes(v.as_ref())?.as_slice()),
            None => Ok(None),
        }
    }

    pub fn delete_node_record(&self, id: &str) -> Result<(), StorageError> {
        if let Some(existing) = self.get_node_record(id)? {
            self.unindex_node_labels(&existing)?;
            self.unindex_node_properties(&existing)?;
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

        if out.is_empty() {
            for entry in self.nodes.iter() {
                let (key, value) = entry?;
                let key_str = std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
                let raw = self.decode_record_bytes(value.as_ref())?;
                let Some(node) = compat_node_record_from_bytes(key_str, &raw)? else {
                    continue;
                };
                if node.labels.iter().any(|node_label| node_label == label) {
                    out.push(node);
                }
            }
        }

        out.sort_by(|a, b| a.id.cmp(&b.id));
        out.dedup_by(|a, b| a.id == b.id);
        Ok(out)
    }

    pub fn all_node_records(&self) -> Result<Vec<NodeRecord>, StorageError> {
        let mut out: Vec<NodeRecord> = Vec::new();
        for entry in self.nodes.iter() {
            let (key, value) = entry?;
            let key_str = std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            let raw = self.decode_record_bytes(value.as_ref())?;
            if let Some(node) = compat_node_record_from_bytes(key_str, &raw)? {
                out.push(node);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub fn get_nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        if !self.has_node_property_index(label, property)? {
            return Ok(Vec::new());
        }

        let prefix = node_property_index_value_prefix(label, property, value);
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
            self.unindex_edge(&old)?;
        }
        self.edges.insert(
            edge.id.as_bytes(),
            self.encode_record_bytes(rmp_serde::to_vec(edge)?)?,
        )?;
        self.index_edge(edge)?;
        Ok(())
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        match self.edges.get(id.as_bytes())? {
            Some(v) => Ok(Some(rmp_serde::from_slice(
                self.decode_record_bytes(v.as_ref())?.as_slice(),
            )?)),
            None => Ok(None),
        }
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<(), StorageError> {
        if let Some(existing) = self.get_edge_record(id)? {
            self.unindex_edge(&existing)?;
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

    pub fn all_edges(&self) -> Result<Vec<EdgeRecord>, StorageError> {
        let mut out = Vec::new();
        for entry in self.edges.iter() {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if let Some(edge) = self.get_edge_record(key_str)? {
                out.push(edge);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub fn get_edges_from_node(&self, node_id: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_edges_by_adjacency_prefix(&edge_start_index_prefix(node_id))
    }

    pub fn get_edges_to_node(&self, node_id: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_edges_by_adjacency_prefix(&edge_end_index_prefix(node_id))
    }

    pub fn get_edges_from_node_by_type(
        &self,
        node_id: &str,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_edges_by_adjacency_prefix(&edge_start_type_index_prefix(node_id, edge_type))
    }

    pub fn get_edges_to_node_by_type(
        &self,
        node_id: &str,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        self.get_edges_by_adjacency_prefix(&edge_end_type_index_prefix(node_id, edge_type))
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

    // --- Topology-native distributed search / hyperscaler metadata ---

    pub fn register_topology_peer(
        &self,
        peer: &copperdb_topology::MeshPeer,
    ) -> Result<(), StorageError> {
        let key = [META_TOPOLOGY_PEER_PREFIX, peer.node_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(peer)?)?;
        Ok(())
    }

    pub fn list_topology_peers(&self) -> Result<Vec<copperdb_topology::MeshPeer>, StorageError> {
        let mut peers: Vec<copperdb_topology::MeshPeer> = Vec::new();
        for entry in self.meta.scan_prefix(META_TOPOLOGY_PEER_PREFIX) {
            let (_, v) = entry?;
            peers.push(rmp_serde::from_slice(v.as_ref())?);
        }
        peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(peers)
    }

    pub fn register_topology_hyperscaler_profile(
        &self,
        profile: &copperdb_topology::HyperscalerProfile,
    ) -> Result<(), StorageError> {
        let key = [META_TOPOLOGY_PROFILE_PREFIX, profile.profile_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(profile)?)?;
        Ok(())
    }

    pub fn list_topology_hyperscaler_profiles(
        &self,
    ) -> Result<Vec<copperdb_topology::HyperscalerProfile>, StorageError> {
        let mut profiles: Vec<copperdb_topology::HyperscalerProfile> = Vec::new();
        for entry in self.meta.scan_prefix(META_TOPOLOGY_PROFILE_PREFIX) {
            let (_, v) = entry?;
            profiles.push(rmp_serde::from_slice(v.as_ref())?);
        }
        profiles.sort_by(|a, b| a.profile_id.cmp(&b.profile_id));
        Ok(profiles)
    }

    pub fn register_topology_placement(
        &self,
        placement: &copperdb_topology::PlacementRecord,
    ) -> Result<(), StorageError> {
        let stable_id = placement.key.stable_id();
        let key = [META_TOPOLOGY_PLACEMENT_PREFIX, stable_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(placement)?)?;
        Ok(())
    }

    pub fn list_topology_placements(
        &self,
    ) -> Result<Vec<copperdb_topology::PlacementRecord>, StorageError> {
        let mut placements: Vec<copperdb_topology::PlacementRecord> = Vec::new();
        for entry in self.meta.scan_prefix(META_TOPOLOGY_PLACEMENT_PREFIX) {
            let (_, v) = entry?;
            placements.push(rmp_serde::from_slice(v.as_ref())?);
        }
        placements.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(placements)
    }

    pub fn load_topology_registry(
        &self,
    ) -> Result<copperdb_topology::TopologyRegistry, StorageError> {
        let mut registry = copperdb_topology::TopologyRegistry::new();
        for profile in self.list_topology_hyperscaler_profiles()? {
            registry
                .register_hyperscaler_profile(profile)
                .map_err(|err| StorageError::TopologyInvalid(err.to_string()))?;
        }
        for peer in self.list_topology_peers()? {
            registry
                .register_peer(peer)
                .map_err(|err| StorageError::TopologyInvalid(err.to_string()))?;
        }
        for placement in self.list_topology_placements()? {
            registry
                .register_placement(placement)
                .map_err(|err| StorageError::TopologyInvalid(err.to_string()))?;
        }
        Ok(registry)
    }

    pub fn register_fabric_database(
        &self,
        database: &copperdb_topology::FabricDatabase,
    ) -> Result<(), StorageError> {
        database
            .validate()
            .map_err(|err| StorageError::TopologyInvalid(err.to_string()))?;
        let stable_id = database.stable_id();
        let key = [META_FABRIC_DATABASE_PREFIX, stable_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(database)?)?;
        Ok(())
    }

    pub fn list_fabric_databases(
        &self,
    ) -> Result<Vec<copperdb_topology::FabricDatabase>, StorageError> {
        let mut databases: Vec<copperdb_topology::FabricDatabase> = Vec::new();
        for entry in self.meta.scan_prefix(META_FABRIC_DATABASE_PREFIX) {
            let (_, value) = entry?;
            let database: copperdb_topology::FabricDatabase =
                rmp_serde::from_slice(value.as_ref())?;
            database
                .validate()
                .map_err(|err| StorageError::TopologyInvalid(err.to_string()))?;
            databases.push(database);
        }
        databases.sort_by_key(|database| database.stable_id());
        Ok(databases)
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
        if is_single_property_node_index(index) {
            self.rebuild_node_property_index(index)?;
        }
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
        let existing = self
            .load_index_definitions()?
            .into_iter()
            .find(|index| index.name == name);
        let key = [META_SCHEMA_INDEX_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.remove(key)?.is_some();
        if deleted {
            if let Some(index) = existing.filter(is_single_property_node_index) {
                self.delete_node_property_index_entries(&index.label, &index.properties[0])?;
            }
        }
        Ok(deleted)
    }

    pub fn persist_decay_profile_schema(
        &self,
        profile: &DecayProfileSchema,
    ) -> Result<(), StorageError> {
        validate_decay_profile(profile)?;
        let key = [META_KP_DECAY_PROFILE_PREFIX, profile.name.as_bytes()].concat();
        let binding_key = [META_KP_DECAY_BINDING_PREFIX, profile.name.as_bytes()].concat();
        if self.meta.get(&key)?.is_some() || self.meta.get(binding_key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "decay profile {}",
                profile.name
            )));
        }
        self.meta.insert(key, rmp_serde::to_vec(profile)?)?;
        Ok(())
    }

    pub fn persist_decay_profile_binding_schema(
        &self,
        binding: &DecayProfileBindingSchema,
    ) -> Result<(), StorageError> {
        validate_decay_profile_binding(binding)?;
        if let Some(profile_ref) = &binding.profile_ref {
            let profile_key = [META_KP_DECAY_PROFILE_PREFIX, profile_ref.as_bytes()].concat();
            if self.meta.get(profile_key)?.is_none() {
                return Err(StorageError::KnowledgePolicyNotFound(format!(
                    "decay profile {}",
                    profile_ref
                )));
            }
        }

        let key = [META_KP_DECAY_BINDING_PREFIX, binding.name.as_bytes()].concat();
        let profile_key = [META_KP_DECAY_PROFILE_PREFIX, binding.name.as_bytes()].concat();
        if self.meta.get(&key)?.is_some() || self.meta.get(profile_key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "decay profile {}",
                binding.name
            )));
        }

        let mut persisted = binding.clone();
        persisted.target_labels.sort();
        self.meta.insert(key, rmp_serde::to_vec(&persisted)?)?;
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

    pub fn load_decay_profile_binding_schemas(
        &self,
    ) -> Result<Vec<DecayProfileBindingSchema>, StorageError> {
        let mut bindings: Vec<DecayProfileBindingSchema> = Vec::new();
        for entry in self.meta.scan_prefix(META_KP_DECAY_BINDING_PREFIX) {
            let (_, value) = entry?;
            bindings.push(rmp_serde::from_slice(value.as_ref())?);
        }
        bindings.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(bindings)
    }

    pub fn alter_decay_profile_schema(
        &self,
        name: &str,
        updates: &BTreeMap<String, serde_json::Value>,
    ) -> Result<(), StorageError> {
        let key = [META_KP_DECAY_PROFILE_PREFIX, name.as_bytes()].concat();
        let raw = self.meta.get(&key)?.ok_or_else(|| {
            StorageError::KnowledgePolicyNotFound(format!("decay profile {}", name))
        })?;
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
        for binding in self.load_decay_profile_binding_schemas()? {
            if binding.profile_ref.as_deref() == Some(name) {
                return Err(StorageError::KnowledgePolicyInUse(format!(
                    "decay profile {} referenced by decay binding {}",
                    name, binding.name
                )));
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

    pub fn delete_decay_profile_binding_schema(
        &self,
        name: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        let key = [META_KP_DECAY_BINDING_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.remove(key)?.is_some();
        if !deleted && !if_exists {
            return Err(StorageError::KnowledgePolicyNotFound(format!(
                "decay profile {}",
                name
            )));
        }
        Ok(())
    }

    pub fn put_knowledge_policy_access_metadata(
        &self,
        entity_id: &str,
        metadata: &KnowledgePolicyAccessMetadata,
    ) -> Result<(), StorageError> {
        let key = [META_KP_ACCESS_METADATA_PREFIX, entity_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec(metadata)?)?;
        Ok(())
    }

    pub fn get_knowledge_policy_access_metadata(
        &self,
        entity_id: &str,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, StorageError> {
        let key = [META_KP_ACCESS_METADATA_PREFIX, entity_id.as_bytes()].concat();
        self.meta
            .get(key)?
            .map(|raw| rmp_serde::from_slice(raw.as_ref()))
            .transpose()
            .map_err(StorageError::from)
    }

    pub fn delete_knowledge_policy_access_metadata(
        &self,
        entity_id: &str,
    ) -> Result<(), StorageError> {
        let key = [META_KP_ACCESS_METADATA_PREFIX, entity_id.as_bytes()].concat();
        self.meta.remove(key)?;
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
        let profiles = self.load_promotion_profile_schemas()?;
        let existing_policies = self.load_promotion_policy_schemas()?;
        validate_promotion_policy(policy, &profiles, &existing_policies)?;
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

    pub fn load_promotion_policy_schemas(
        &self,
    ) -> Result<Vec<PromotionPolicySchema>, StorageError> {
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
        let profiles = self.load_promotion_profile_schemas()?;
        let existing_policies = self
            .load_promotion_policy_schemas()?
            .into_iter()
            .filter(|existing| existing.name != name)
            .collect::<Vec<_>>();
        validate_promotion_policy(&policy, &profiles, &existing_policies)?;
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

    fn index_node_properties(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for index in self.node_property_index_definitions()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            let property = &index.properties[0];
            if let Some(value) = node.properties.get(property) {
                self.indexes.insert(
                    node_property_index_key(&index.label, property, value, &node.id).as_bytes(),
                    &[],
                )?;
            }
        }
        Ok(())
    }

    fn unindex_node_properties(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for index in self.node_property_index_definitions()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            let property = &index.properties[0];
            if let Some(value) = node.properties.get(property) {
                self.indexes.remove(
                    node_property_index_key(&index.label, property, value, &node.id).as_bytes(),
                )?;
            }
        }
        Ok(())
    }

    fn has_node_property_index(&self, label: &str, property: &str) -> Result<bool, StorageError> {
        Ok(self
            .node_property_index_definitions()?
            .iter()
            .any(|index| index.label == label && index.properties[0] == property))
    }

    fn node_property_index_definitions(&self) -> Result<Vec<IndexDefinition>, StorageError> {
        Ok(self
            .load_index_definitions()?
            .into_iter()
            .filter(is_single_property_node_index)
            .collect())
    }

    fn rebuild_node_property_index(&self, index: &IndexDefinition) -> Result<(), StorageError> {
        self.delete_node_property_index_entries(&index.label, &index.properties[0])?;
        for node in self.get_nodes_by_label(&index.label)? {
            if let Some(value) = node.properties.get(&index.properties[0]) {
                self.indexes.insert(
                    node_property_index_key(&index.label, &index.properties[0], value, &node.id)
                        .as_bytes(),
                    &[],
                )?;
            }
        }
        Ok(())
    }

    fn delete_node_property_index_entries(
        &self,
        label: &str,
        property: &str,
    ) -> Result<(), StorageError> {
        let prefix = node_property_index_property_prefix(label, property);
        let keys = self
            .indexes
            .scan_prefix(prefix.as_bytes())
            .map(|entry| {
                entry
                    .map(|(key, _)| key.to_vec())
                    .map_err(StorageError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            self.indexes.remove(key)?;
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

    fn get_edges_by_adjacency_prefix(&self, prefix: &str) -> Result<Vec<EdgeRecord>, StorageError> {
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

    fn index_edge(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes.insert(
            edge_type_index_key(&edge.edge_type, &edge.id).as_bytes(),
            &[],
        )?;
        self.indexes.insert(
            edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id).as_bytes(),
            &[],
        )?;
        self.indexes.insert(
            edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id).as_bytes(),
            &[],
        )?;
        Ok(())
    }

    fn unindex_edge(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes
            .remove(edge_type_index_key(&edge.edge_type, &edge.id).as_bytes())?;
        self.indexes
            .remove(edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id).as_bytes())?;
        self.indexes
            .remove(edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id).as_bytes())?;
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

fn edge_start_index_prefix(node_id: &str) -> String {
    format!(
        "{IDX_EDGE_START_PREFIX}/{}/",
        escape_index_component(node_id)
    )
}

fn edge_start_type_index_prefix(node_id: &str, edge_type: &str) -> String {
    format!(
        "{}{}/",
        edge_start_index_prefix(node_id),
        escape_index_component(edge_type)
    )
}

fn edge_start_index_key(node_id: &str, edge_type: &str, edge_id: &str) -> String {
    format!(
        "{}{}",
        edge_start_type_index_prefix(node_id, edge_type),
        edge_id
    )
}

fn edge_end_index_prefix(node_id: &str) -> String {
    format!("{IDX_EDGE_END_PREFIX}/{}/", escape_index_component(node_id))
}

fn edge_end_type_index_prefix(node_id: &str, edge_type: &str) -> String {
    format!(
        "{}{}/",
        edge_end_index_prefix(node_id),
        escape_index_component(edge_type)
    )
}

fn edge_end_index_key(node_id: &str, edge_type: &str, edge_id: &str) -> String {
    format!(
        "{}{}",
        edge_end_type_index_prefix(node_id, edge_type),
        edge_id
    )
}

fn is_single_property_node_index(index: &IndexDefinition) -> bool {
    index.entity_type == IndexEntityType::Node && index.properties.len() == 1
}

fn node_property_index_property_prefix(label: &str, property: &str) -> String {
    format!(
        "{IDX_NODE_PROPERTY_PREFIX}/{}/{}/",
        escape_index_component(label),
        escape_index_component(property)
    )
}

fn node_property_index_value_prefix(
    label: &str,
    property: &str,
    value: &serde_json::Value,
) -> String {
    format!(
        "{}{}/",
        node_property_index_property_prefix(label, property),
        property_index_value_key(value)
    )
}

fn node_property_index_key(
    label: &str,
    property: &str,
    value: &serde_json::Value,
    node_id: &str,
) -> String {
    format!(
        "{}{}",
        node_property_index_value_prefix(label, property, value),
        node_id
    )
}

fn property_index_value_key(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_else(|_| value.to_string().into_bytes());
    hex::encode(bytes)
}

fn escape_index_component(value: &str) -> String {
    hex::encode(value.as_bytes())
}

fn value_as_f64(value: &serde_json::Value, field: &str) -> Result<f64, StorageError> {
    value
        .as_f64()
        .ok_or_else(|| StorageError::KnowledgePolicyInvalid(format!("{} must be a number", field)))
}

fn value_as_i64(value: &serde_json::Value, field: &str) -> Result<i64, StorageError> {
    value.as_i64().ok_or_else(|| {
        StorageError::KnowledgePolicyInvalid(format!("{} must be an integer", field))
    })
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

fn validate_decay_profile_binding(binding: &DecayProfileBindingSchema) -> Result<(), StorageError> {
    if binding.name.trim().is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "decay binding name is required".into(),
        ));
    }
    if binding.is_wildcard {
        if binding.is_edge
            || binding.target_edge_type.is_some()
            || !binding.target_labels.is_empty()
        {
            return Err(StorageError::KnowledgePolicyInvalid(
                "wildcard decay binding cannot target labels or edge types".into(),
            ));
        }
    } else if binding.is_edge {
        if binding.target_edge_type.as_deref().unwrap_or("").trim().is_empty() {
            return Err(StorageError::KnowledgePolicyInvalid(
                "edge decay binding requires a target edge type".into(),
            ));
        }
        if !binding.target_labels.is_empty() {
            return Err(StorageError::KnowledgePolicyInvalid(
                "edge decay binding cannot target node labels".into(),
            ));
        }
    } else if binding.target_labels.is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "node decay binding requires at least one target label".into(),
        ));
    }

    if !binding.no_decay
        && binding
            .profile_ref
            .as_ref()
            .map(|profile| profile.trim().is_empty())
            .unwrap_or(true)
    {
        return Err(StorageError::KnowledgePolicyInvalid(
            "decay binding requires profileRef or noDecay=true".into(),
        ));
    }

    if let Some(threshold) = binding.visibility_threshold {
        if !(0.0..=1.0).contains(&threshold) {
            return Err(StorageError::KnowledgePolicyInvalid(
                "visibilityThreshold must be between 0 and 1".into(),
            ));
        }
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
    existing_policies: &[PromotionPolicySchema],
) -> Result<(), StorageError> {
    if policy.name.trim().is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "promotion policy name is required".into(),
        ));
    }
    if policy.is_edge {
        if policy.target_edge_type.as_deref().unwrap_or_default().trim().is_empty() {
            return Err(StorageError::KnowledgePolicyInvalid(
                "promotion policy edge targets require an edge type".into(),
            ));
        }
        if !policy.target_labels.is_empty() || policy.is_wildcard {
            return Err(StorageError::KnowledgePolicyInvalid(
                "promotion policy edge targets cannot also declare node labels or wildcard"
                    .into(),
            ));
        }
    } else if policy.is_wildcard {
        if !policy.target_labels.is_empty() || policy.target_edge_type.is_some() {
            return Err(StorageError::KnowledgePolicyInvalid(
                "promotion policy wildcard targets cannot declare labels or edge type".into(),
            ));
        }
    } else if policy.target_labels.is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "promotion policy target labels are required".into(),
        ));
    }
    if policy.on_access_mutations.is_empty() && policy.when_clauses.is_empty() {
        return Err(StorageError::KnowledgePolicyInvalid(
            "promotion policy requires ON ACCESS mutations or WHEN clauses".into(),
        ));
    }
    let target_key = promotion_policy_target_key(policy);
    if existing_policies
        .iter()
        .any(|existing| promotion_policy_target_key(existing) == target_key)
    {
        return Err(StorageError::KnowledgePolicyInvalid(format!(
            "promotion policy target '{}' already has a policy",
            target_key
        )));
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

fn promotion_policy_target_key(policy: &PromotionPolicySchema) -> String {
    if policy.is_edge {
        return format!("edge:{}", policy.target_edge_type.clone().unwrap_or_default());
    }
    if policy.is_wildcard {
        return "wild:node".to_string();
    }
    let mut sorted = policy.target_labels.clone();
    sorted.sort();
    format!("node:{}", sorted.join("\0"))
}

fn compat_node_record_from_bytes(id: &str, raw: &[u8]) -> Result<Option<NodeRecord>, StorageError> {
    if let Ok(record) = rmp_serde::from_slice::<NodeRecord>(raw) {
        return Ok(Some(record));
    }

    let mut props = match rmp_serde::from_slice::<BTreeMap<String, serde_json::Value>>(raw) {
        Ok(props) => props,
        Err(_) => return Ok(None),
    };

    let labels = legacy_node_labels(id, props.get("_labels"));
    props.remove("_labels");
    props.remove("_id");

    Ok(Some(NodeRecord {
        id: id.to_string(),
        labels,
        properties: props,
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    }))
}

fn node_record_to_legacy_props(node: &NodeRecord) -> BTreeMap<String, serde_json::Value> {
    let mut props = node.properties.clone();
    props.insert("_id".to_string(), serde_json::Value::String(node.id.clone()));
    props.insert(
        "_labels".to_string(),
        serde_json::Value::Array(
            node.labels
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    props
}

fn legacy_node_labels(
    id: &str,
    stored_labels: Option<&serde_json::Value>,
) -> Vec<String> {
    if let Some(serde_json::Value::Array(labels)) = stored_labels {
        let parsed = labels
            .iter()
            .filter_map(|label| label.as_str().map(str::to_string))
            .collect::<Vec<_>>();
        if !parsed.is_empty() {
            return parsed;
        }
    }

    if let Some(serde_json::Value::String(label)) = stored_labels {
        if !label.is_empty() {
            return vec![label.clone()];
        }
    }

    derived_label_from_id(id).into_iter().collect()
}

fn derived_label_from_id(id: &str) -> Option<String> {
    let namespace = id.split_once(':').map(|(namespace, _)| namespace).unwrap_or(id);
    let mut chars = namespace.chars();
    let first = chars.next()?;
    let mut label = first.to_uppercase().collect::<String>();
    label.push_str(chars.as_str());
    Some(label)
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
    use copperdb_kms::{LocalKms, LocalKmsConfig};
    use serde_json::json;
    use std::fs;

    fn local_provider(byte: u8) -> Arc<dyn KeyProvider> {
        Arc::new(LocalKms::new(LocalKmsConfig::new(vec![byte; 32])).unwrap())
    }

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
    fn raw_node_edge_round_trip() {
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
    fn encrypted_storage_round_trips_records_and_rejects_plain_open() {
        let test_dir = std::env::temp_dir().join(format!(
            "copperdb-storage-encryption-test-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&test_dir).unwrap();

        let engine =
            StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/default")
                .unwrap();
        assert!(engine.is_encrypted());
        let manifest = engine.encryption_manifest().unwrap().unwrap();
        assert_eq!(manifest.key_uri, "kms://local/default");

        let node = sample_node("db1:n1", &["Secret"]);
        let edge = sample_edge("db1:e1", "SECRET_EDGE", "db1:n1", "db1:n2");
        engine.put_node_record(&node).unwrap();
        engine.put_edge_record(&edge).unwrap();
        engine.put_node("raw:1", b"classified").unwrap();
        engine.flush().unwrap();
        drop(engine);

        let raw_db = sled::open(&test_dir).unwrap();
        let raw_nodes = raw_db.open_tree("nodes").unwrap();
        let stored = raw_nodes.get("db1:n1").unwrap().unwrap();
        assert_ne!(
            stored.as_ref(),
            rmp_serde::to_vec(&node).unwrap().as_slice()
        );
        drop(raw_nodes);
        drop(raw_db);

        let reopened =
            StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/default")
                .unwrap();
        assert_eq!(reopened.get_node_record("db1:n1").unwrap(), Some(node));
        assert_eq!(reopened.get_edge_record("db1:e1").unwrap(), Some(edge));
        assert_eq!(
            reopened.get_node("raw:1").unwrap(),
            Some(b"classified".to_vec())
        );
        drop(reopened);
        assert!(matches!(
            StorageEngine::open(&test_dir),
            Err(StorageError::EncryptionRequired)
        ));
        let _ = fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn encrypted_storage_rejects_wrong_key_uri() {
        let test_dir = std::env::temp_dir().join(format!(
            "copperdb-storage-encryption-key-uri-test-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&test_dir).unwrap();

        let engine =
            StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/a")
                .unwrap();
        engine.flush().unwrap();
        drop(engine);

        let err = StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/b")
            .err()
            .unwrap();
        assert!(matches!(err, StorageError::EncryptionMismatch(_)));
        let _ = fs::remove_dir_all(&test_dir);
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
        assert_eq!(
            engine.get_edges_from_node("db1:n1").unwrap(),
            vec![edge.clone()]
        );
        assert_eq!(
            engine.get_edges_to_node("db1:n2").unwrap(),
            vec![edge.clone()]
        );
        assert_eq!(
            engine
                .get_edges_from_node_by_type("db1:n1", "KNOWS")
                .unwrap(),
            vec![edge.clone()]
        );
        assert!(engine
            .get_edges_from_node_by_type("db1:n1", "MENTORS")
            .unwrap()
            .is_empty());

        edge.edge_type = "MENTORS".to_string();
        edge.properties.insert("years".to_string(), json!(5));
        engine.put_edge_record(&edge).unwrap();

        assert!(engine.get_edges_by_type("KNOWS").unwrap().is_empty());
        assert!(engine
            .get_edges_from_node_by_type("db1:n1", "KNOWS")
            .unwrap()
            .is_empty());
        let mentors = engine.get_edges_by_type("MENTORS").unwrap();
        assert_eq!(mentors.len(), 1);
        assert_eq!(mentors[0].properties.get("years"), Some(&json!(5)));
        assert_eq!(
            engine
                .get_edges_to_node_by_type("db1:n2", "MENTORS")
                .unwrap(),
            mentors
        );

        engine.delete_edge_record("db1:e1").unwrap();
        assert!(engine.get_edge_record("db1:e1").unwrap().is_none());
        assert!(engine.get_edges_by_type("MENTORS").unwrap().is_empty());
        assert!(engine.get_edges_from_node("db1:n1").unwrap().is_empty());
        assert!(engine.get_edges_to_node("db1:n2").unwrap().is_empty());
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
    fn topology_metadata_round_trip_builds_valid_registry() {
        use copperdb_topology::{
            DistributedWriteMode, HyperscalerProfile as TopologyHyperscalerProfile, MeshPeer,
            NodeCapability, PlacementKey, PlacementRecord, SearchRoutingPolicy,
        };

        let engine = StorageEngine::open_temporary().unwrap();
        engine
            .register_topology_hyperscaler_profile(&TopologyHyperscalerProfile::local("local-prod"))
            .unwrap();
        engine
            .register_topology_peer(
                &MeshPeer::new("node-a", "node-a.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::WriteLeader)
                    .with_hyperscaler_profile("local-prod")
                    .with_region_zone("us-east-1", "us-east-1a")
                    .with_observed_rtt_micros(1_000),
            )
            .unwrap();
        engine
            .register_topology_peer(
                &MeshPeer::new("node-b", "node-b.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::WriteReplica)
                    .with_hyperscaler_profile("local-prod")
                    .with_region_zone("us-east-1", "us-east-1b")
                    .with_observed_rtt_micros(2_000),
            )
            .unwrap();
        engine
            .register_topology_placement(&PlacementRecord {
                key: PlacementKey::default_for_database("copper"),
                primary_node: "node-a".into(),
                replica_nodes: vec!["node-b".into()],
                search_nodes: vec!["node-a".into(), "node-b".into()],
                hyperscaler_profile: Some("local-prod".into()),
                min_write_replicas: 1,
                search_fanout: 2,
            })
            .unwrap();

        let registry = engine.load_topology_registry().unwrap();
        let placement = PlacementKey::default_for_database("copper");
        let search_plan = registry
            .plan_search_with_policy(&placement, SearchRoutingPolicy::low_latency("us-east-1", 2))
            .unwrap();
        let write_plan = registry
            .plan_write(&placement, DistributedWriteMode::LeaderLease)
            .unwrap();

        assert_eq!(search_plan.fanout.len(), 2);
        assert_eq!(search_plan.fanout[0].node_id, "node-a");
        assert_eq!(write_plan.required_acks, 2);
    }

    #[test]
    fn fabric_database_metadata_round_trip_lists_shard_map() {
        use copperdb_topology::{
            FabricDatabase, FabricPartitionPolicy, FabricShard, FabricShardKind, PlacementKey,
        };

        let engine = StorageEngine::open_temporary().unwrap();
        let fabric = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
            shards: vec![
                FabricShard::mixed(PlacementKey::default_for_database("copper")),
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "person-00"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec!["KNOWS".into()],
                    collections: vec![],
                },
            ],
        };

        engine.register_fabric_database(&fabric).unwrap();
        let databases = engine.list_fabric_databases().unwrap();

        assert_eq!(databases, vec![fabric]);
        assert_eq!(databases[0].placement_keys().len(), 2);
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
    fn mvcc_registered_snapshot_blocks_pruning_past_active_reader() {
        let mvcc = MvccStore::new();

        let v1 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"alice".to_vec()))]);
        assert_eq!(v1, 1);
        let snapshot1 = mvcc.begin_registered_snapshot();

        let v2 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"bob".to_vec()))]);
        assert_eq!(v2, 2);
        let snapshot2 = mvcc.begin_snapshot();

        mvcc.prune_versions_older_than(2);
        assert_eq!(mvcc.oldest_active_reader(), Some(1));
        assert_eq!(mvcc.read(snapshot1.snapshot(), "node:1"), Some(b"alice".to_vec()));
        assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));

        let head = mvcc.head();
        assert_eq!(head.floor, 1);
        assert_eq!(head.head, 2);

        drop(snapshot1);
        mvcc.prune_versions_older_than(2);

        assert_eq!(mvcc.oldest_active_reader(), None);
        let head = mvcc.head();
        assert_eq!(head.floor, 2);
        assert_eq!(head.head, 2);
        assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));
    }

    #[test]
    fn mvcc_lifecycle_status_reports_debt_and_reader_pressure() {
        let mvcc = MvccStore::new();
        mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v1".to_vec()))]);
        let reader = mvcc.begin_registered_snapshot();
        mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v2".to_vec()))]);
        mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v3".to_vec()))]);

        let status = mvcc.lifecycle_status();
        assert_eq!(status.floor, 0);
        assert_eq!(status.head, 3);
        assert_eq!(status.oldest_active_reader, Some(1));
        assert_eq!(status.active_reader_count, 1);
        assert_eq!(status.retained_versions, 3);
        assert_eq!(status.prune_debt, 0);
        assert_eq!(status.suggested_prune_floor, 1);

        drop(reader);
        let status = mvcc.lifecycle_status();
        assert_eq!(status.oldest_active_reader, None);
        assert_eq!(status.active_reader_count, 0);
        assert_eq!(status.suggested_prune_floor, 3);
        assert_eq!(status.prune_debt, 2);
    }

    #[test]
    fn mvcc_trigger_prune_now_respects_active_readers_and_reports_removed_versions() {
        let mvcc = MvccStore::new();
        mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v1".to_vec()))]);
        let reader = mvcc.begin_registered_snapshot();
        mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v2".to_vec()))]);
        mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v3".to_vec()))]);

        let removed = mvcc.trigger_prune_now(0);
        assert_eq!(removed, 0);
        assert_eq!(mvcc.head().floor, 1);
        assert_eq!(mvcc.read(reader.snapshot(), "node:1"), Some(b"v1".to_vec()));

        drop(reader);
        let removed = mvcc.trigger_prune_now(0);
        assert_eq!(removed, 2);
        assert_eq!(mvcc.head().floor, 3);
        let snapshot = mvcc.begin_snapshot();
        assert_eq!(mvcc.read(&snapshot, "node:1"), Some(b"v3".to_vec()));
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
    fn wal_persists_entries_and_reopens_next_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.rmp");
        let config = WALConfig {
            enabled: true,
            max_entries_per_segment: 2,
        };

        let wal = WAL::open(&wal_path, config.clone()).unwrap();
        let first = wal.append("put", "node:1", b"a").unwrap();
        assert_eq!(first.seq, 1);
        let (_start, end) = wal
            .append_batch(vec![
                ("put".to_string(), "node:2".to_string(), b"b".to_vec()),
                ("delete".to_string(), "node:1".to_string(), Vec::new()),
            ])
            .unwrap();
        assert_eq!(end, 3);
        assert_eq!(wal.stats().segments, 2);
        drop(wal);

        let reopened = WAL::open(&wal_path, config).unwrap();
        let replay = reopened.replay_after(0).unwrap();
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0].key, "node:1");
        assert_eq!(replay[2].op, "delete");
        let next = reopened.append("put", "node:3", b"c").unwrap();
        assert_eq!(next.seq, 4);
    }

    #[test]
    fn wal_compaction_truncates_replay_and_preserves_sequence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.rmp");
        let config = WALConfig {
            enabled: true,
            max_entries_per_segment: 2,
        };

        let wal = WAL::open(&wal_path, config.clone()).unwrap();
        let (_start, end) = wal
            .append_batch(vec![
                ("put".to_string(), "node:1".to_string(), b"a".to_vec()),
                ("put".to_string(), "node:2".to_string(), b"b".to_vec()),
                ("delete".to_string(), "node:1".to_string(), Vec::new()),
            ])
            .unwrap();
        assert_eq!(end, 3);

        let removed = wal.compact_up_to(2).unwrap();
        assert_eq!(removed, 2);
        let stats = wal.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.segments, 1);
        assert_eq!(stats.compacted_through, 2);
        assert_eq!(stats.next_seq, 3);

        let replay = wal.replay_after(0).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].seq, 3);
        drop(wal);

        let reopened = WAL::open(&wal_path, config).unwrap();
        let stats = reopened.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.compacted_through, 2);
        assert_eq!(stats.next_seq, 3);

        let replay = reopened.replay_after(0).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].seq, 3);
        let next = reopened.append("put", "node:3", b"c").unwrap();
        assert_eq!(next.seq, 4);
    }

    #[test]
    fn wal_rejects_invalid_persistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.rmp");
        fs::write(&wal_path, b"not-messagepack").unwrap();

        let err = WAL::open(&wal_path, WALConfig::default()).unwrap_err();
        assert!(matches!(err, StorageError::WalMissingOrInvalidTrailer));
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
    fn node_property_index_rebuilds_and_tracks_mutations() {
        let engine = StorageEngine::open_temporary().unwrap();
        let mut alice = sample_node("db:n1", &["Person"]);
        alice
            .properties
            .insert("email".into(), json!("alice@example.com"));
        let mut bob = sample_node("db:n2", &["Person"]);
        bob.properties
            .insert("email".into(), json!("bob@example.com"));
        engine.put_node_record(&alice).unwrap();
        engine.put_node_record(&bob).unwrap();

        assert!(engine
            .get_nodes_by_property("Person", "email", &json!("alice@example.com"))
            .unwrap()
            .is_empty());

        engine
            .persist_index_definition(&IndexDefinition {
                name: "person_email_idx".to_string(),
                entity_type: IndexEntityType::Node,
                label: "Person".to_string(),
                properties: vec!["email".to_string()],
            })
            .unwrap();

        let alice_hits = engine
            .get_nodes_by_property("Person", "email", &json!("alice@example.com"))
            .unwrap();
        assert_eq!(alice_hits.len(), 1);
        assert_eq!(alice_hits[0].id, "db:n1");

        alice
            .properties
            .insert("email".into(), json!("alice@new.test"));
        engine.put_node_record(&alice).unwrap();
        assert!(engine
            .get_nodes_by_property("Person", "email", &json!("alice@example.com"))
            .unwrap()
            .is_empty());
        assert_eq!(
            engine
                .get_nodes_by_property("Person", "email", &json!("alice@new.test"))
                .unwrap()[0]
                .id,
            "db:n1"
        );

        engine.delete_node_record("db:n1").unwrap();
        assert!(engine
            .get_nodes_by_property("Person", "email", &json!("alice@new.test"))
            .unwrap()
            .is_empty());

        engine.delete_index_definition("person_email_idx").unwrap();
        assert!(engine
            .get_nodes_by_property("Person", "email", &json!("bob@example.com"))
            .unwrap()
            .is_empty());
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
                target_edge_type: None,
                is_wildcard: false,
                is_edge: false,
                enabled: true,
                on_access_mutations: vec![PromotionOnAccessMutationSchema {
                    kind: PromotionOnAccessMutationKindSchema::SetLastAccessedNow,
                }],
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
        assert_eq!(policies[0].target_labels, vec!["KnowledgeFact".to_string()]);
        assert_eq!(policies[0].target_edge_type, None);
        assert!(!policies[0].is_edge);
        assert_eq!(policies[0].on_access_mutations.len(), 1);
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

    #[test]
    fn knowledge_policy_promotion_schema_rejects_duplicate_targets() {
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
                name: "fact_policy_a".to_string(),
                target_labels: vec!["KnowledgeFact".to_string()],
                target_edge_type: None,
                is_wildcard: false,
                is_edge: false,
                enabled: true,
                on_access_mutations: vec![PromotionOnAccessMutationSchema {
                    kind: PromotionOnAccessMutationKindSchema::SetLastAccessedNow,
                }],
                when_clauses: vec![PromotionWhenClauseSchema {
                    profile_ref: "boost_profile".to_string(),
                    predicate: "true".to_string(),
                    order: 1,
                }],
            })
            .unwrap();

        let err = engine
            .persist_promotion_policy_schema(&PromotionPolicySchema {
                name: "fact_policy_b".to_string(),
                target_labels: vec!["KnowledgeFact".to_string()],
                target_edge_type: None,
                is_wildcard: false,
                is_edge: false,
                enabled: true,
                on_access_mutations: vec![PromotionOnAccessMutationSchema {
                    kind: PromotionOnAccessMutationKindSchema::IncrementAccessCount,
                }],
                when_clauses: vec![],
            })
            .unwrap_err();
        assert!(matches!(err, StorageError::KnowledgePolicyInvalid(_)));
    }

    #[test]
    fn knowledge_policy_decay_binding_schema_roundtrip_and_reference_guards() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine
            .persist_decay_profile_schema(&DecayProfileSchema {
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
            })
            .unwrap();

        engine
            .persist_decay_profile_binding_schema(&DecayProfileBindingSchema {
                name: "memory_binding".to_string(),
                target_labels: vec!["MemoryEpisode".to_string()],
                target_edge_type: None,
                is_wildcard: false,
                is_edge: false,
                profile_ref: Some("slow_decay".to_string()),
                no_decay: false,
                visibility_threshold: Some(0.2),
                order: 10,
            })
            .unwrap();

        let bindings = engine.load_decay_profile_binding_schemas().unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].name, "memory_binding");
        assert_eq!(bindings[0].target_labels, vec!["MemoryEpisode".to_string()]);
        assert_eq!(bindings[0].profile_ref.as_deref(), Some("slow_decay"));

        let err = engine
            .delete_decay_profile_schema("slow_decay", false)
            .unwrap_err();
        assert!(matches!(err, StorageError::KnowledgePolicyInUse(_)));

        engine
            .delete_decay_profile_binding_schema("memory_binding", false)
            .unwrap();
        engine
            .delete_decay_profile_schema("slow_decay", false)
            .unwrap();

        assert!(engine.load_decay_profile_binding_schemas().unwrap().is_empty());
        assert!(engine.load_decay_profile_schemas().unwrap().is_empty());
    }

    #[test]
    fn knowledge_policy_access_metadata_roundtrip() {
        let engine = StorageEngine::open_temporary().unwrap();
        let metadata = KnowledgePolicyAccessMetadata {
            last_accessed_at_unix_ms: Some(1_717_171_717_000),
            access_count: 3,
        };

        engine
            .put_knowledge_policy_access_metadata("memory:1", &metadata)
            .unwrap();

        let loaded = engine
            .get_knowledge_policy_access_metadata("memory:1")
            .unwrap()
            .expect("metadata missing");
        assert_eq!(loaded, metadata);

        engine
            .delete_knowledge_policy_access_metadata("memory:1")
            .unwrap();
        assert!(engine
            .get_knowledge_policy_access_metadata("memory:1")
            .unwrap()
            .is_none());
    }
}
