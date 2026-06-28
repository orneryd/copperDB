//! Embedded key-value storage engine for copperdb.
//!
//! Storage layout policy for copper: **version 0 only**.
//! This crate intentionally avoids alternate layout migration arms and only supports
//! opening databases whose manifest declares layout version 0.

use bytes::Bytes;
use copperdb_encryption::{EnvelopeConfig, EnvelopeEncryptor};
use copperdb_kms::KeyProvider;
use copperdb_util::{RequestCancellation, RequestCancelled};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

// ── fjall→Fjall compatibility layer ──────────────────────────────────────────

/// Re-export for backward compatibility: fjall Keyspace replaces fjall Tree.
pub type Tree = Keyspace;

/// A batch of key-value operations: (key, optional_value). None = delete.
pub type Batch = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Extension trait making fjall's Keyspace behave like fjall's Tree.
trait KeyspaceExt {
    fn scan_prefix<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a>;

    fn fjall_iter<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a>;

    fn fjall_get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError>;

    /// Insert and return the previous value.
    fn fjall_insert(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError>;

    /// Remove and return the previous value.
    fn fjall_remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError>;

    fn fjall_apply_batch(&self, batch: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<(), StorageError>;

    /// Range scan returning fjall-compatible iterator.
    fn fjall_range<'a, R: std::ops::RangeBounds<Vec<u8>> + 'a>(
        &'a self,
        range: R,
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a>;
}

impl KeyspaceExt for Keyspace {
    fn scan_prefix<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a> {
        Box::new(
            self.prefix(prefix)
                .map(|guard| {
                    guard
                        .into_inner()
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .map_err(StorageError::Fjall)
                })
        )
    }

    fn fjall_iter<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a> {
        Box::new(
            self.iter()
                .map(|guard| {
                    guard
                        .into_inner()
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .map_err(StorageError::Fjall)
                })
        )
    }

    fn fjall_get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.get(key.as_ref()).map_err(StorageError::Fjall)?.map(|v| v.to_vec()))
    }

    fn fjall_insert(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError> {
        let k = key.as_ref();
        let old = self.get(k).map_err(StorageError::Fjall)?.map(|v| v.to_vec());
        self.insert(k, value.as_ref()).map_err(StorageError::Fjall)?;
        Ok(old)
    }

    fn fjall_remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError> {
        let k = key.as_ref();
        let old = self.get(k).map_err(StorageError::Fjall)?.map(|v| v.to_vec());
        self.remove(k).map_err(StorageError::Fjall)?;
        Ok(old)
    }

    fn fjall_apply_batch(
        &self,
        batch: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(), StorageError> {
        for (key, value) in batch {
            match value {
                Some(v) => self.insert(key.as_slice(), v.as_slice()).map_err(StorageError::Fjall)?,
                None => self.remove(key.as_slice()).map_err(StorageError::Fjall)?,
            }
        }
        Ok(())
    }

    fn fjall_range<'a, R: std::ops::RangeBounds<Vec<u8>> + 'a>(
        &'a self,
        range: R,
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a> {
        // Convert Vec<u8> bounds to &[u8] bounds
        let start = std::ops::Bound::map(
            range.start_bound(),
            |v: &Vec<u8>| v.as_slice(),
        );
        let end = std::ops::Bound::map(
            range.end_bound(),
            |v: &Vec<u8>| v.as_slice(),
        );
        Box::new(
            self.range::<&[u8], _>((start, end))
                .map(|guard| {
                    guard
                        .into_inner()
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .map_err(StorageError::Fjall)
                })
        )
    }
}

mod async_engine;
mod mvcc;
mod namespaced;
mod storage_edge_property_index;
mod storage_node_property_range;
mod storage_property_index_encoding;
pub use crate::async_engine::{
    AsyncFlushGuard, AsyncFlushResult, AsyncStorageConfig, AsyncStorageEngine,
};
pub use crate::mvcc::{
    MvccHead, MvccLifecycleDebtKey, MvccLifecycleStatus, MvccLogicalHead, MvccPruneOptions,
    MvccSnapshot, MvccSnapshotLease, MvccStore, MvccVersion, NamespacedMvccStore,
};
pub use crate::namespaced::NamespacedStorageEngine;
use crate::storage_edge_property_index::{
    is_relationship_property_index, relationship_property_index_key_for_edge,
};
pub use crate::storage_node_property_range::RangeIndexComparison;
use crate::storage_property_index_encoding::property_index_value_key;

pub fn parse_database_prefix(id: &str) -> Option<(&str, &str)> {
    let idx = id.find(':')?;
    if idx == 0 || idx >= id.len() - 1 {
        return None;
    }
    Some((&id[..idx], &id[idx + 1..]))
}

pub fn strip_database_prefix<'a>(database: &str, id: &'a str) -> &'a str {
    if database.is_empty() || id.is_empty() {
        return id;
    }
    id.strip_prefix(&format!("{database}:")).unwrap_or(id)
}

pub fn ensure_database_prefix(database: &str, id: &str) -> String {
    if database.is_empty() || id.is_empty() || parse_database_prefix(id).is_some() {
        return id.to_string();
    }
    format!("{database}:{id}")
}

pub const STORAGE_LAYOUT_VERSION: u8 = 0;
const META_LAYOUT_MANIFEST_KEY: &[u8] = b"layout_manifest";
const META_ENCRYPTION_MANIFEST_KEY: &[u8] = b"encryption_manifest";
const META_TOPOLOGY_PEER_PREFIX: &[u8] = b"topology_peer/";
const META_TOPOLOGY_PROFILE_PREFIX: &[u8] = b"topology_profile/";
const META_TOPOLOGY_PLACEMENT_PREFIX: &[u8] = b"topology_placement/";
const META_FABRIC_DATABASE_PREFIX: &[u8] = b"fabric_database/";
const META_SCHEMA_CONSTRAINT_PREFIX: &[u8] = b"schema_constraint/";
const META_SCHEMA_INDEX_PREFIX: &[u8] = b"schema_index/";
const META_SCHEMA_NAMESPACE_CONSTRAINT_PREFIX: &[u8] = b"schema_constraint_ns/";
const META_SCHEMA_NAMESPACE_INDEX_PREFIX: &[u8] = b"schema_index_ns/";
const META_NAMESPACE_NODE_COUNT_PREFIX: &[u8] = b"namespace_node_count/";
const META_NAMESPACE_EDGE_COUNT_PREFIX: &[u8] = b"namespace_edge_count/";
const META_NAMESPACE_LABEL_COUNT_PREFIX: &[u8] = b"namespace_label_count/";
const META_PENDING_EMBEDDING_PREFIX: &[u8] = b"pending_embedding/";
const META_PENDING_DEINDEX_PREFIX: &[u8] = b"pending_deindex/";
const META_INDEX_TOMBSTONE_PREFIX: &[u8] = b"index_tombstone/";
const META_INDEX_OPTIONS_PREFIX: &[u8] = b"index_options/";
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
const IDX_NODE_FULLTEXT_PREFIX: &str = "node_fulltext";
pub(crate) const IDX_EDGE_PROPERTY_PREFIX: &str = "edge_property";

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),
    #[error("unsupported storage layout version: expected {expected}, got {actual}")]
    UnsupportedLayoutVersion { expected: u8, actual: u8 },
    #[error("key not found: {0}")]
    NotFound(String),
    #[error("prefix cannot be empty")]
    EmptyPrefix,
    #[error("invalid chunk size: {0}")]
    InvalidChunkSize(usize),
    #[error("iteration stopped")]
    IterationStopped,
    #[error(transparent)]
    RequestCancelled(#[from] RequestCancelled),
    #[error("invalid utf8 in key")]
    InvalidUtf8,
    #[error("mvcc rebuild is blocked by {active_readers} active reader(s)")]
    MvccRebuildBlocked { active_readers: u64 },
    #[error("mvcc head truncated: {0} bytes")]
    MvccHeadTruncated(usize),
    #[error("mvcc head missing floor: {0} bytes")]
    MvccHeadMissingFloor(usize),
    #[error("wal: closed")]
    WalClosed,
    #[error("async engine: closed")]
    AsyncEngineClosed,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeAdjacencyDirection {
    Outgoing,
    Incoming,
    Both,
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

    /// Create a lightweight snapshot checkpoint at the current WAL position.
    pub fn create_snapshot(&self) -> WALSnapshot {
        WALSnapshot {
            compacted_through: self.compacted_through(),
            created_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        }
    }

    /// Truncate WAL entries at or before the snapshot point.
    pub fn truncate_to_snapshot(&self, snapshot: &WALSnapshot) -> Result<usize, StorageError> {
        self.compact_up_to(snapshot.compacted_through)
    }

    /// Scan the WAL for corruption. Returns the sequence number of the first
    /// corrupted entry, or None if all entries pass checksum verification.
    pub fn scan_for_corruption(&self) -> Option<u64> {
        let entries = self.entries.lock();
        for entry in entries.iter() {
            let expected = wal_checksum(&entry.op, &entry.key, &entry.payload);
            if entry.checksum != expected {
                return Some(entry.seq);
            }
        }
        None
    }

    /// Count how many entries in the WAL fail checksum verification.
    pub fn corrupted_entry_count(&self) -> usize {
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|entry| {
                let expected = wal_checksum(&entry.op, &entry.key, &entry.payload);
                entry.checksum != expected
            })
            .count()
    }

    /// Attempt to repair the WAL by truncating at the first corrupted entry.
    /// Returns the number of entries removed (0 if no corruption found).
    pub fn repair_truncate_at_first_corruption(&self) -> Result<usize, StorageError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::WalClosed);
        }
        let first_corrupt_seq = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|entry| {
                    let expected = wal_checksum(&entry.op, &entry.key, &entry.payload);
                    entry.checksum != expected
                })
                .map(|e| e.seq)
        };
        let Some(corrupt_seq) = first_corrupt_seq else {
            return Ok(0);
        };
        // Truncate: keep entries with seq < corrupt_seq, drop the corrupted entry and everything after
        let removed = {
            let mut entries = self.entries.lock();
            let before = entries.len();
            entries.retain(|e| e.seq < corrupt_seq);
            self.persist_entries(&entries)?;
            self.recompute_segments(entries.len());
            before - entries.len()
        };
        // Adjust next_seq to match the last valid entry
        let max_valid_seq = {
            let entries = self.entries.lock();
            entries.last().map(|e| e.seq).unwrap_or(0)
        };
        self.next_seq.store(max_valid_seq + 1, Ordering::SeqCst);
        self.degraded.store(false, Ordering::SeqCst);
        Ok(removed)
    }
}

/// A WAL snapshot checkpoint for recovery acceleration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALSnapshot {
    pub compacted_through: u64,
    pub created_at_unix_ms: i64,
}

/// Save a WAL snapshot to a file.
pub fn save_wal_snapshot(
    snapshot: &WALSnapshot,
    path: &std::path::Path,
) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, rmp_serde::to_vec(snapshot)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Load a previously saved WAL snapshot.
pub fn load_wal_snapshot(path: &std::path::Path) -> Result<WALSnapshot, StorageError> {
    let data = std::fs::read(path)?;
    Ok(rmp_serde::from_slice(&data)?)
}

/// Prune snapshot files matching `*.snap` pattern, keeping the most recent `keep`.
pub fn prune_wal_snapshots(dir: &std::path::Path, keep: usize) -> Result<usize, StorageError> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut snapshots: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.ends_with(".snap"))
                .unwrap_or(false)
        })
        .collect();
    let removed = if snapshots.len() > keep {
        snapshots.sort_by_key(|e| e.file_name());
        let to_remove = snapshots.len().saturating_sub(keep);
        for entry in snapshots.iter().take(to_remove) {
            std::fs::remove_file(entry.path())?;
        }
        to_remove
    } else {
        0
    };
    Ok(removed)
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
    Temporal,
    Domain,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexEntityType {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum IndexKind {
    #[default]
    Range,
    Temporal,
    FullText,
    Vector,
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
    /// Expected type name for `ConstraintType::Type` (e.g. "INTEGER", "STRING").
    #[serde(default)]
    pub type_name: Option<String>,
    /// Allowed values for `ConstraintType::Domain`.
    #[serde(default)]
    pub allowed_values: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub entity_type: IndexEntityType,
    pub label: String,
    pub properties: Vec<String>,
    pub kind: IndexKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NamespaceSchema {
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<IndexDefinition>,
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
                ConstraintType::Type
                | ConstraintType::Relationship
                | ConstraintType::Temporal
                | ConstraintType::Domain => {}
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
    #[serde(default)]
    pub named_embeddings: BTreeMap<String, Vec<f32>>,
    #[serde(default)]
    pub chunk_embeddings: Vec<Vec<f32>>,
    #[serde(default)]
    pub embed_meta: NodeEmbeddingMetadata,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NodeEmbeddingMetadata {
    #[serde(default)]
    pub embedding_model: Option<String>,
    #[serde(default)]
    pub embedding_dimensions: Option<usize>,
    #[serde(default)]
    pub has_embedding: Option<bool>,
    #[serde(default)]
    pub embedded_at: Option<String>,
    #[serde(default)]
    pub has_chunks: Option<bool>,
    #[serde(default)]
    pub chunk_count: Option<usize>,
}

impl NodeEmbeddingMetadata {
    pub fn is_empty(&self) -> bool {
        self.embedding_model.is_none()
            && self.embedding_dimensions.is_none()
            && self.has_embedding.is_none()
            && self.embedded_at.is_none()
            && self.has_chunks.is_none()
            && self.chunk_count.is_none()
    }

    fn clear_materialized_state(&mut self) {
        self.embedding_model = None;
        self.embedding_dimensions = None;
        self.has_embedding = None;
        self.embedded_at = None;
        self.has_chunks = None;
        self.chunk_count = None;
    }
}

impl NodeRecord {
    pub fn needs_embedding(&self) -> bool {
        node_record_needs_embedding(self)
    }

    pub fn has_materialized_embedding(&self) -> bool {
        node_record_has_materialized_embedding(self)
    }

    pub fn default_embedding(&self) -> Option<&[f32]> {
        self.named_embeddings
            .get(DEFAULT_NAMED_EMBEDDING)
            .map(Vec::as_slice)
    }

    pub fn set_default_embedding(&mut self, embedding: Vec<f32>) {
        self.named_embeddings
            .insert(DEFAULT_NAMED_EMBEDDING.to_string(), embedding);
    }

    pub fn has_materialized_chunk_embeddings(&self) -> bool {
        self.chunk_embeddings
            .iter()
            .any(|embedding| !embedding.is_empty())
    }

    pub fn set_managed_chunk_embeddings(
        &mut self,
        embeddings: Vec<Vec<f32>>,
        embedding_model: Option<String>,
        embedded_at: Option<String>,
    ) {
        let dimensions = embeddings
            .iter()
            .find(|embedding| !embedding.is_empty())
            .map(Vec::len);
        let chunk_count = embeddings.len();
        let has_embeddings = embeddings.iter().any(|embedding| !embedding.is_empty());

        self.chunk_embeddings = embeddings;
        self.embed_meta.chunk_count = Some(chunk_count);
        self.embed_meta.has_chunks = Some(chunk_count > 0);
        self.embed_meta.embedding_model = embedding_model;
        self.embed_meta.embedding_dimensions = dimensions;
        self.embed_meta.has_embedding = Some(has_embeddings);
        self.embed_meta.embedded_at = embedded_at;
    }

    pub fn clear_managed_chunk_embeddings(&mut self) {
        self.chunk_embeddings.clear();
        self.embed_meta.clear_materialized_state();
    }
}

const DEFAULT_NAMED_EMBEDDING: &str = "default";

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

pub type NodeEventCallback = Arc<dyn Fn(NodeRecord) + Send + Sync + 'static>;
pub type NodeDeleteCallback = Arc<dyn Fn(String) + Send + Sync + 'static>;
pub type EdgeEventCallback = Arc<dyn Fn(EdgeRecord) + Send + Sync + 'static>;
pub type EdgeDeleteCallback = Arc<dyn Fn(String) + Send + Sync + 'static>;

pub fn node_record_needs_embedding(node: &NodeRecord) -> bool {
    if node
        .labels
        .iter()
        .any(|label| label.starts_with('_') && !label.is_empty())
    {
        return false;
    }
    if node.properties.contains_key("embedding_skipped") {
        return false;
    }
    if node_record_has_materialized_embedding(node) {
        return false;
    }
    true
}

pub fn node_record_has_materialized_embedding(node: &NodeRecord) -> bool {
    node.named_embeddings
        .values()
        .any(|embedding| !embedding.is_empty())
        || node
            .chunk_embeddings
            .iter()
            .any(|embedding| !embedding.is_empty())
}

pub trait StorageEventNotifier {
    fn on_node_created(&self, callback: NodeEventCallback);
    fn on_node_updated(&self, callback: NodeEventCallback);
    fn on_node_deleted(&self, callback: NodeDeleteCallback);
    fn on_edge_created(&self, callback: EdgeEventCallback);
    fn on_edge_updated(&self, callback: EdgeEventCallback);
    fn on_edge_deleted(&self, callback: EdgeDeleteCallback);
}

/// A single opened copperdb storage instance.
pub struct StorageEngine {
    db: Database,
    meta: Keyspace,
    nodes: Keyspace,
    edges: Keyspace,
    indexes: Keyspace,
    mvcc: MvccStore,
    encryption: Option<StorageEncryption>,
    temp_dir: Option<tempfile::TempDir>,
    // Event callbacks
    on_node_created_cb: RwLock<Option<NodeEventCallback>>,
    on_node_updated_cb: RwLock<Option<NodeEventCallback>>,
    on_node_deleted_cb: RwLock<Option<NodeDeleteCallback>>,
    on_edge_created_cb: RwLock<Option<EdgeEventCallback>>,
    on_edge_updated_cb: RwLock<Option<EdgeEventCallback>>,
    on_edge_deleted_cb: RwLock<Option<EdgeDeleteCallback>>,
}

impl fmt::Debug for StorageEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageEngine")
            .field("db", &"<fjall::Database>")
            .field("encryption", &self.encryption)
            .field("temp_dir", &self.temp_dir)
            .finish_non_exhaustive()
    }
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
    pub fn for_namespace(&self, namespace: impl Into<String>) -> NamespacedStorageEngine<'_> {
        NamespacedStorageEngine::new(self, namespace)
    }

    /// Open (or create) a storage engine at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        fs::create_dir_all(path).map_err(StorageError::Io)?;
        let db = Database::open(fjall::Config::new(path)).map_err(StorageError::Fjall)?;
        Self::open_with_db(db, None)
    }

    /// Open a storage engine without replaying current records into the MVCC
    /// overlay. This is intended for small metadata stores such as the
    /// multi-database catalog that use the record APIs directly and do not run
    /// graph transactions against the opened engine.
    pub fn open_metadata(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let db = Database::open(fjall::Config::new(path)).map_err(StorageError::Fjall)?;
        Self::open_with_db_options(db, None, false)
    }

    /// Open (or create) a storage engine whose graph records are encrypted using
    /// provider-backed envelope encryption.
    pub fn open_encrypted(
        path: impl AsRef<Path>,
        provider: Arc<dyn KeyProvider>,
        key_uri: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let db = Database::open(fjall::Config::new(path)).map_err(StorageError::Fjall)?;
        let encryption = StorageEncryption::new(provider, key_uri.into())?;
        Self::open_with_db(db, Some(encryption))
    }

    /// Open an in-memory (temporary) storage engine for testing.
    pub fn open_temporary() -> Result<Self, StorageError> {
        let temp_dir = tempfile::tempdir()?;
        let db = Database::open(fjall::Config::new(temp_dir.path()))?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        let nodes = db.keyspace("nodes", KeyspaceCreateOptions::default)?;
        let edges = db.keyspace("edges", KeyspaceCreateOptions::default)?;
        let indexes = db.keyspace("indexes", KeyspaceCreateOptions::default)?;
        let engine = Self {
            db,
            meta,
            nodes,
            edges,
            indexes,
            mvcc: MvccStore::new(),
            encryption: None,
            temp_dir: Some(temp_dir),
            on_node_created_cb: RwLock::new(None),
            on_node_updated_cb: RwLock::new(None),
            on_node_deleted_cb: RwLock::new(None),
            on_edge_created_cb: RwLock::new(None),
            on_edge_updated_cb: RwLock::new(None),
            on_edge_deleted_cb: RwLock::new(None),
        };
        engine.ensure_layout_manifest()?;
        engine.ensure_encryption_manifest()?;
        engine.bootstrap_mvcc_from_current_state()?;
        Ok(engine)
    }

    fn open_with_db(db: Database, encryption: Option<StorageEncryption>) -> Result<Self, StorageError> {
        Self::open_with_db_options(db, encryption, true)
    }

    fn open_with_db_options(
        db: Database,
        encryption: Option<StorageEncryption>,
        bootstrap_mvcc: bool,
    ) -> Result<Self, StorageError> {
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        let nodes = db.keyspace("nodes", KeyspaceCreateOptions::default)?;
        let edges = db.keyspace("edges", KeyspaceCreateOptions::default)?;
        let indexes = db.keyspace("indexes", KeyspaceCreateOptions::default)?;
        let engine = Self {
            db,
            meta,
            nodes,
            edges,
            indexes,
            mvcc: MvccStore::new(),
            encryption,
            temp_dir: None,
            on_node_created_cb: RwLock::new(None),
            on_node_updated_cb: RwLock::new(None),
            on_node_deleted_cb: RwLock::new(None),
            on_edge_created_cb: RwLock::new(None),
            on_edge_updated_cb: RwLock::new(None),
            on_edge_deleted_cb: RwLock::new(None),
        };
        engine.ensure_layout_manifest()?;
        engine.ensure_encryption_manifest()?;
        if bootstrap_mvcc {
            engine.bootstrap_mvcc_from_current_state()?;
        }
        Ok(engine)
    }

    fn bootstrap_mvcc_from_current_state(&self) -> Result<(), StorageError> {
        for node in self.all_node_records()? {
            self.mvcc.put_node_record(&node)?;
        }
        for edge in self.all_edges()? {
            self.mvcc.put_edge_record(&edge)?;
        }
        Ok(())
    }

    pub fn rebuild_mvcc_from_current_state(&self) -> Result<(), StorageError> {
        let active_readers = self.mvcc.active_reader_count();
        if active_readers != 0 {
            return Err(StorageError::MvccRebuildBlocked { active_readers });
        }

        self.mvcc.reset_for_rebuild();
        self.bootstrap_mvcc_from_current_state()
    }

    fn ensure_layout_manifest(&self) -> Result<(), StorageError> {
        if let Some(raw) = self.meta.fjall_get(META_LAYOUT_MANIFEST_KEY)? {
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
            self.meta.fjall_get(META_ENCRYPTION_MANIFEST_KEY)?,
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
        match self.meta.fjall_get(META_ENCRYPTION_MANIFEST_KEY)? {
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
        match self.nodes.fjall_get(id.as_bytes())? {
            Some(v) => {
                let raw = self.decode_record_bytes(v.as_ref())?;
                if let Some(node) = compat_node_record_from_bytes(id, &raw)? {
                    return Ok(Some(rmp_serde::to_vec(&node_record_to_legacy_props(
                        &node,
                    ))?));
                }
                Ok(Some(raw))
            }
            None => Ok(None),
        }
    }

    /// Delete a node.
    pub fn delete_node(&self, id: &str) -> Result<(), StorageError> {
        self.nodes.fjall_remove(id.as_bytes())?;
        Ok(())
    }

    /// Iterate over all nodes with a given prefix.
    pub fn scan_nodes_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = Result<(Bytes, Bytes), StorageError>> + 'a {
        self.nodes.scan_prefix(prefix.as_bytes()).map(move |res| {
            let (k, v) = res.map_err(StorageError::from)?;
            let key = std::str::from_utf8(k.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            let raw = self.decode_record_bytes(v.as_ref())?;
            let value = if let Some(node) = compat_node_record_from_bytes(key, &raw)? {
                rmp_serde::to_vec(&node_record_to_legacy_props(&node))?
            } else {
                raw
            };
            Ok((Bytes::from(k.to_vec()), Bytes::from(value)))
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
        self.edges.fjall_remove(id.as_bytes())?;
        Ok(())
    }

    // --- Structured node/edge APIs (storage v0 baseline) ---

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<(), StorageError> {
        let old = self.get_node_record(&node.id)?;
        if let Some(ref old) = old {
            self.unindex_node_labels(old)?;
            self.unindex_node_properties(old)?;
            self.apply_node_stats_delta(old, -1)?;
        }
        self.nodes.fjall_insert(
            node.id.as_bytes(),
            self.encode_record_bytes(rmp_serde::to_vec(node)?)?,
        )?;
        self.index_node_labels(node)?;
        self.index_node_properties(node)?;
        self.apply_node_stats_delta(node, 1)?;
        self.update_pending_embedding_index(node)?;
        self.mvcc.put_node_record(node)?;

        if old.is_some() {
            self.notify_node_updated(node);
        } else {
            self.notify_node_created(node);
        }
        Ok(())
    }

    pub fn put_node_records_batch(&self, nodes: &[NodeRecord]) -> Result<(), StorageError> {
        let property_indexes = self.node_property_index_definitions()?;
        let fulltext_indexes = self.node_fulltext_index_definitions()?;

        for node in nodes {
            let old = self.get_node_record(&node.id)?;
            if let Some(ref old) = old {
                self.unindex_node_labels(old)?;
                for index in &property_indexes {
                    if !old.labels.iter().any(|label| label == &index.label) {
                        continue;
                    }
                    if let Some(key) = node_property_index_key_for_node(index, old) {
                        self.indexes.fjall_remove(key.as_bytes())?;
                    }
                }
                for index in &fulltext_indexes {
                    if !old.labels.iter().any(|label| label == &index.label) {
                        continue;
                    }
                    self.delete_node_fulltext_entries(index, old)?;
                }
                self.apply_node_stats_delta(old, -1)?;
            }

            self.nodes.fjall_insert(
                node.id.as_bytes(),
                self.encode_record_bytes(rmp_serde::to_vec(node)?)?,
            )?;
            self.index_node_labels(node)?;
            for index in &property_indexes {
                if !node.labels.iter().any(|label| label == &index.label) {
                    continue;
                }
                if let Some(key) = node_property_index_key_for_node(index, node) {
                    self.indexes.fjall_insert(key.as_bytes(), [])?;
                }
            }
            for index in &fulltext_indexes {
                if !node.labels.iter().any(|label| label == &index.label) {
                    continue;
                }
                self.index_node_fulltext_entries(index, node)?;
            }
            self.apply_node_stats_delta(node, 1)?;
            self.update_pending_embedding_index(node)?;
            self.mvcc.put_node_record(node)?;

            if old.is_some() {
                self.notify_node_updated(node);
            } else {
                self.notify_node_created(node);
            }
        }
        Ok(())
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        match self.nodes.fjall_get(id.as_bytes())? {
            Some(v) => {
                compat_node_record_from_bytes(id, self.decode_record_bytes(v.as_ref())?.as_slice())
            }
            None => Ok(None),
        }
    }

    pub fn delete_node_record(&self, id: &str) -> Result<(), StorageError> {
        if let Some(existing) = self.get_node_record(id)? {
            self.unindex_node_labels(&existing)?;
            self.unindex_node_properties(&existing)?;
            self.apply_node_stats_delta(&existing, -1)?;
            self.mark_node_embedded(id)?;
            self.nodes.fjall_remove(id.as_bytes())?;
            self.mvcc.delete_node_record(id)?;
            self.notify_node_deleted(id);
        }
        Ok(())
    }

    pub fn delete_by_prefix(&self, prefix: &str) -> Result<(u64, u64), StorageError> {
        if prefix.is_empty() {
            return Err(StorageError::EmptyPrefix);
        }

        let node_ids = self.ids_with_prefix(&self.nodes, prefix)?;
        let edge_ids = self.ids_with_prefix(&self.edges, prefix)?;

        for node_id in &node_ids {
            self.delete_node_record(node_id)?;
        }
        for edge_id in &edge_ids {
            self.delete_edge_record(edge_id)?;
        }

        if let Some(namespace) = namespace_from_prefix(prefix) {
            self.delete_namespace_metadata(namespace)?;
        }

        Ok((node_ids.len() as u64, edge_ids.len() as u64))
    }

    /// Execute a batch of storage operations atomically.
    ///
    /// All node/edge puts and deletes within the closure are applied as a single
    /// fjall batch. Indexes, stats, and MVCC are updated atomically. If the
    /// closure returns an error, the batch is discarded and no changes are made.
    ///
    /// This is the foundation for namespace-pinned transaction semantics:
    /// multi-operation writes within a namespace are isolated and atomic.
    pub fn batch_write<F, E>(&self, f: F) -> Result<(), E>
    where
        F: FnOnce(&mut BatchWriter<'_>) -> Result<(), E>,
        E: From<StorageError>,
    {
        let mut writer = BatchWriter {
            engine: self,
            ops: Vec::new(),
        };
        f(&mut writer)?;
        writer.commit()?;
        Ok(())
    }

    // ── Event notification ─────────────────────────────────────────────────

    pub fn on_node_created(&self, callback: NodeEventCallback) {
        *self.on_node_created_cb.write() = Some(callback);
    }

    pub fn on_node_updated(&self, callback: NodeEventCallback) {
        *self.on_node_updated_cb.write() = Some(callback);
    }

    pub fn on_node_deleted(&self, callback: NodeDeleteCallback) {
        *self.on_node_deleted_cb.write() = Some(callback);
    }

    pub fn on_edge_created(&self, callback: EdgeEventCallback) {
        *self.on_edge_created_cb.write() = Some(callback);
    }

    pub fn on_edge_updated(&self, callback: EdgeEventCallback) {
        *self.on_edge_updated_cb.write() = Some(callback);
    }

    pub fn on_edge_deleted(&self, callback: EdgeDeleteCallback) {
        *self.on_edge_deleted_cb.write() = Some(callback);
    }

    fn notify_node_created(&self, node: &NodeRecord) {
        if let Some(ref cb) = *self.on_node_created_cb.read() {
            cb(node.clone());
        }
    }

    fn notify_node_updated(&self, node: &NodeRecord) {
        if let Some(ref cb) = *self.on_node_updated_cb.read() {
            cb(node.clone());
        }
    }

    fn notify_node_deleted(&self, id: &str) {
        if let Some(ref cb) = *self.on_node_deleted_cb.read() {
            cb(id.to_string());
        }
    }

    fn notify_edge_created(&self, edge: &EdgeRecord) {
        if let Some(ref cb) = *self.on_edge_created_cb.read() {
            cb(edge.clone());
        }
    }

    fn notify_edge_updated(&self, edge: &EdgeRecord) {
        if let Some(ref cb) = *self.on_edge_updated_cb.read() {
            cb(edge.clone());
        }
    }

    fn notify_edge_deleted(&self, id: &str) {
        if let Some(ref cb) = *self.on_edge_deleted_cb.read() {
            cb(id.to_string());
        }
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeRecord>, StorageError> {
        let prefix = label_index_prefix(label);
        let mut out = Vec::new();

        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries (suppressed entities)
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(node_id) = key_str.rsplit('/').next() {
                if let Some(node) = self.get_node_record(node_id)? {
                    out.push(node);
                }
            }
        }

        if out.is_empty() {
            for entry in self.nodes.fjall_iter() {
                let (key, value) = entry?;
                let key_str =
                    std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
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
        for entry in self.nodes.fjall_iter() {
            let (key, value) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            let raw = self.decode_record_bytes(value.as_ref())?;
            if let Some(node) = compat_node_record_from_bytes(key_str, &raw)? {
                out.push(node);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    /// Stream nodes matching a label, checking cancellation periodically.
    pub(crate) fn stream_nodes_by_label_with_cancellation<F>(
        &self,
        label: &str,
        cancel: &RequestCancellation,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        let prefix = label_index_prefix(label);
        let mut visited = 0u64;

        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(node_id) = key_str.rsplit('/').next() {
                if let Some(node) = self.get_node_record(node_id)? {
                    visit(node)?;
                    visited += 1;
                }
            }
        }

        if visited == 0 {
            for entry in self.nodes.fjall_iter() {
                cancel.check_cancelled()?;
                let (key, value) = entry?;
                let key_str =
                    std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
                let raw = self.decode_record_bytes(value.as_ref())?;
                let Some(node) = compat_node_record_from_bytes(key_str, &raw)? else {
                    continue;
                };
                if node.labels.iter().any(|node_label| node_label == label) {
                    visit(node)?;
                    visited += 1;
                }
            }
        }

        Ok(visited)
    }

    pub fn find_node_needing_embedding(&self) -> Result<Option<NodeRecord>, StorageError> {
        for entry in self.meta.scan_prefix(META_PENDING_EMBEDDING_PREFIX) {
            let (key, _) = entry?;
            let Some(node_id) = pending_embedding_id_from_key(key.as_ref()) else {
                continue;
            };
            match self.get_node_record(&node_id)? {
                Some(node) if node.needs_embedding() => return Ok(Some(node)),
                Some(_) | None => self.mark_node_embedded(&node_id)?,
            }
        }
        Ok(None)
    }

    pub fn mark_node_embedded(&self, id: &str) -> Result<(), StorageError> {
        self.meta.fjall_remove(pending_embedding_key(id))?;
        Ok(())
    }

    pub fn add_to_pending_embeddings(&self, id: &str) -> Result<(), StorageError> {
        let Some(node) = self.get_node_record(id)? else {
            return Ok(());
        };
        self.update_pending_embedding_index(&node)
    }

    pub fn pending_embeddings_count(&self) -> Result<usize, StorageError> {
        let mut count = 0;
        for entry in self.meta.scan_prefix(META_PENDING_EMBEDDING_PREFIX) {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    pub fn refresh_pending_embeddings_index(&self) -> Result<usize, StorageError> {
        let valid_ids = self
            .all_node_records()?
            .into_iter()
            .filter(|node| node.needs_embedding())
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();

        let keys = self
            .meta
            .scan_prefix(META_PENDING_EMBEDDING_PREFIX)
            .map(|entry| {
                entry
                    .map(|(key, _)| key.to_vec())
                    .map_err(StorageError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            self.meta.fjall_remove(key)?;
        }
        for id in &valid_ids {
            self.meta.fjall_insert(pending_embedding_key(id), [])?;
        }
        Ok(valid_ids.len())
    }

    pub fn pending_embedding_ids(&self) -> Result<Vec<String>, StorageError> {
        let mut ids = Vec::new();
        for entry in self.meta.scan_prefix(META_PENDING_EMBEDDING_PREFIX) {
            let (key, _) = entry?;
            if let Some(id) = pending_embedding_id_from_key(key.as_ref()) {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids)
    }

    pub fn clear_all_embeddings(&self) -> Result<usize, StorageError> {
        self.clear_all_embeddings_for_prefix("")
    }

    pub fn clear_all_embeddings_for_prefix(&self, prefix: &str) -> Result<usize, StorageError> {
        let nodes_to_clear = self
            .all_node_records()?
            .into_iter()
            .filter(|node| prefix.is_empty() || node.id.starts_with(prefix))
            .filter(|node| node.has_materialized_chunk_embeddings())
            .collect::<Vec<_>>();

        let mut cleared = 0;
        for mut node in nodes_to_clear {
            node.clear_managed_chunk_embeddings();
            self.put_node_record(&node)?;
            cleared += 1;
        }

        Ok(cleared)
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

    // ── Deindex cleanup queue ───────────────────────────────────────────────

    /// Enqueue a node for deferred index cleanup (e.g., when visibility drops below threshold).
    pub fn enqueue_deindex_work(&self, entity_id: &str) -> Result<(), StorageError> {
        let key = [META_PENDING_DEINDEX_PREFIX, entity_id.as_bytes()].concat();
        self.meta.fjall_insert(key, [] as [u8; 0])?;
        Ok(())
    }

    /// Drain and process all pending deindex work items.
    /// For each entity, writes index tombstones to hide (not delete) its index entries.
    /// The entity record is preserved — tombstones can be cleared when visibility recovers.
    /// Returns the number of entities deindexed.
    pub fn drain_deindex_work(&self) -> Result<usize, StorageError> {
        let pending: Vec<String> = {
            let mut ids = Vec::new();
            for entry in self.meta.scan_prefix(META_PENDING_DEINDEX_PREFIX) {
                let (key, _) = entry?;
                let key_str =
                    std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
                if let Some(id) = key_str.strip_prefix(
                    std::str::from_utf8(META_PENDING_DEINDEX_PREFIX)
                        .map_err(|_| StorageError::InvalidUtf8)?,
                ) {
                    ids.push(id.to_string());
                }
            }
            ids
        };

        let mut deindexed = 0usize;
        for entity_id in &pending {
            let key = [META_PENDING_DEINDEX_PREFIX, entity_id.as_bytes()].concat();

            // Collect index keys for the entity and write tombstones
            if let Some(node) = self.get_node_record(entity_id)? {
                let index_keys = collect_node_index_keys(&node);
                self.write_index_tombstones(&index_keys)?;
            } else if let Some(edge) = self.get_edge_record(entity_id)? {
                let index_keys = collect_edge_index_keys(&edge);
                self.write_index_tombstones(&index_keys)?;
            }
            // If entity is gone, no tombstones needed — just remove the marker

            self.meta.fjall_remove(key)?;
            deindexed += 1;
        }

        Ok(deindexed)
    }

    /// Count pending deindex work items.
    pub fn pending_deindex_count(&self) -> Result<usize, StorageError> {
        let mut count = 0usize;
        for entry in self.meta.scan_prefix(META_PENDING_DEINDEX_PREFIX) {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    // ── Index tombstones ────────────────────────────────────────────────────

    /// Write tombstones for a batch of index keys. Tombstone entries hide
    /// the corresponding index entries during query scans without deleting them,
    /// allowing restore when entity visibility recovers.
    pub fn write_index_tombstones(&self, index_keys: &[String]) -> Result<(), StorageError> {
        for key in index_keys {
            self.meta.fjall_insert(tombstone_key(key), [])?;
        }
        Ok(())
    }

    /// Delete tombstones for a batch of index keys. Used when an entity
    /// recovers visibility (decay score rises above threshold).
    pub fn delete_index_tombstones(&self, index_keys: &[String]) -> Result<(), StorageError> {
        for key in index_keys {
            self.meta.fjall_remove(tombstone_key(key))?;
        }
        Ok(())
    }

    /// Check whether a tombstone exists for the given index key.
    pub fn has_index_tombstone(&self, index_key: &str) -> bool {
        self.meta
            .contains_key(tombstone_key(index_key))
            .unwrap_or(false)
    }

    /// Delete all tombstones for a given entity by scanning the tombstone prefix
    /// for keys containing the entity ID. Returns the number of tombstones removed.
    pub fn delete_index_tombstones_for_entity(
        &self,
        entity_id: &str,
    ) -> Result<usize, StorageError> {
        let mut removed = 0usize;
        for entry in self.meta.scan_prefix(META_INDEX_TOMBSTONE_PREFIX) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if key_str.contains(entity_id) {
                self.meta.fjall_remove(&key[..])?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn update_pending_embedding_index(&self, node: &NodeRecord) -> Result<(), StorageError> {
        if node.needs_embedding() {
            self.meta.fjall_insert(pending_embedding_key(&node.id), [])?;
        } else {
            self.meta.fjall_remove(pending_embedding_key(&node.id))?;
        }
        Ok(())
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
        visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        self.stream_node_records_from_entries(self.nodes.fjall_iter(), cancel, visit)
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
        visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        self.stream_node_records_from_entries(
            self.nodes.scan_prefix(prefix.as_bytes()),
            cancel,
            visit,
        )
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
        self.stream_node_records_with_cancellation(cancel, |node| {
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
        })
        .or_else(Self::swallow_iteration_stopped)?;

        if !stop_requested && !chunk.is_empty() {
            cancel.check_cancelled()?;
            match visit(&chunk) {
                Ok(()) | Err(StorageError::IterationStopped) => {}
                Err(err) => return Err(err),
            }
        }

        Ok(streamed)
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

    pub fn search_fulltext_nodes_by_properties(
        &self,
        label: &str,
        properties: &[String],
        query: &str,
        limit: usize,
    ) -> Result<Vec<(NodeRecord, usize)>, StorageError> {
        let tokens = tokenize_fulltext(query);
        if tokens.is_empty() || properties.is_empty() {
            return Ok(Vec::new());
        }

        let mut scores: HashMap<String, usize> = HashMap::new();
        for property in properties {
            for token in &tokens {
                let prefix = node_fulltext_token_prefix(label, property, token);
                for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
                    let (key, _) = entry?;
                    let key_str =
                        std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
                    if let Some(node_id) = key_str.rsplit('/').next() {
                        *scores.entry(node_id.to_string()).or_default() += 1;
                    }
                }
            }
        }

        let mut ranked: Vec<(String, usize)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

        let mut nodes = Vec::new();
        for (node_id, score) in ranked.into_iter().take(limit) {
            if let Some(node) = self.get_node_record(&node_id)? {
                nodes.push((node, score));
            }
        }
        Ok(nodes)
    }

    pub fn get_nodes_by_properties(
        &self,
        label: &str,
        properties: &[String],
        values: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        if !self.has_exact_node_property_index(label, properties)? {
            return Ok(Vec::new());
        }

        let Some(prefix) = node_property_index_lookup_prefix(label, properties, values) else {
            return Ok(Vec::new());
        };

        self.load_nodes_from_index_prefix(&prefix)
    }

    pub fn node_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        if let Some(namespace) = namespace_from_prefix(prefix) {
            return self.meta_counter(namespace_node_count_key(namespace));
        }
        Ok(self.nodes.scan_prefix(prefix.as_bytes()).count() as u64)
    }

    pub fn put_edge_record(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        let old = self.get_edge_record(&edge.id)?;
        if let Some(ref old) = old {
            self.unindex_edge(old)?;
            self.apply_edge_stats_delta(old, -1)?;
        }
        self.edges.fjall_insert(
            edge.id.as_bytes(),
            self.encode_record_bytes(rmp_serde::to_vec(edge)?)?,
        )?;
        self.index_edge(edge)?;
        self.apply_edge_stats_delta(edge, 1)?;
        self.mvcc.put_edge_record(edge)?;

        if old.is_some() {
            self.notify_edge_updated(edge);
        } else {
            self.notify_edge_created(edge);
        }
        Ok(())
    }

    pub fn put_edge_records_batch(&self, edges: &[EdgeRecord]) -> Result<(), StorageError> {
        let relationship_property_indexes = self.relationship_property_index_definitions()?;
        let mut namespace_edge_deltas: HashMap<String, i64> = HashMap::new();
        for edge in edges {
            let old = self.get_edge_record(&edge.id)?;
            if let Some(ref old) = old {
                self.indexes
                    .remove(edge_type_index_key(&old.edge_type, &old.id).as_bytes())?;
                self.indexes.fjall_remove(
                    edge_start_index_key(&old.start_node, &old.edge_type, &old.id).as_bytes(),
                )?;
                self.indexes.fjall_remove(
                    edge_end_index_key(&old.end_node, &old.edge_type, &old.id).as_bytes(),
                )?;
                for index in &relationship_property_indexes {
                    if index.label == old.edge_type {
                        if let Some(key) = relationship_property_index_key_for_edge(index, old) {
                            self.indexes.fjall_remove(key.as_bytes())?;
                        }
                    }
                }
                if let Some(namespace) = namespace_from_str(&old.id) {
                    *namespace_edge_deltas
                        .entry(namespace.to_string())
                        .or_default() -= 1;
                }
            }

            self.edges.fjall_insert(
                edge.id.as_bytes(),
                self.encode_record_bytes(rmp_serde::to_vec(edge)?)?,
            )?;
            self.indexes.fjall_insert(
                edge_type_index_key(&edge.edge_type, &edge.id).as_bytes(),
                [],
            )?;
            self.indexes.fjall_insert(
                edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id).as_bytes(),
                [],
            )?;
            self.indexes.fjall_insert(
                edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id).as_bytes(),
                [],
            )?;
            for index in &relationship_property_indexes {
                if index.label == edge.edge_type {
                    if let Some(key) = relationship_property_index_key_for_edge(index, edge) {
                        self.indexes.fjall_insert(key.as_bytes(), [])?;
                    }
                }
            }
            if let Some(namespace) = namespace_from_str(&edge.id) {
                *namespace_edge_deltas
                    .entry(namespace.to_string())
                    .or_default() += 1;
            }
            self.mvcc.put_edge_record(edge)?;

            if old.is_some() {
                self.notify_edge_updated(edge);
            } else {
                self.notify_edge_created(edge);
            }
        }
        for (namespace, delta) in namespace_edge_deltas {
            if delta != 0 {
                self.adjust_meta_counter(namespace_edge_count_key(&namespace), delta)?;
            }
        }
        Ok(())
    }

    /// Batch-insert edges, skipping the old-edge lookup. Use when all edges are
    /// known to be new (e.g. initial data load). Uses fjall::Batch internally
    /// for maximum throughput (mirrors Badger's WriteBatch in NornicDB).
    ///
    /// Skips MVCC tracking — only safe when no concurrent readers exist.
    pub fn put_new_edge_records_batch(&self, edges: &[EdgeRecord]) -> Result<(), StorageError> {
        let relationship_property_indexes = self.relationship_property_index_definitions()?;
        let mut namespace_edge_deltas: HashMap<String, i64> = HashMap::new();

        let mut edges_batch = Batch::new();
        let mut indexes_batch = Batch::new();
        let mut pending: usize = 0;
        const FLUSH_EVERY: usize = 4096;

        for edge in edges {
            edges_batch.push((edge.id.as_bytes().to_vec(), Some(rmp_serde::to_vec(edge)?)));
            indexes_batch.push((
                edge_type_index_key(&edge.edge_type, &edge.id).into_bytes(),
                Some(Vec::<u8>::new()),
            ));
            indexes_batch.push((
                edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id).into_bytes(),
                Some(Vec::<u8>::new()),
            ));
            indexes_batch.push((
                edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id).into_bytes(),
                Some(Vec::<u8>::new()),
            ));
            for index in &relationship_property_indexes {
                if index.label == edge.edge_type {
                    if let Some(key) = relationship_property_index_key_for_edge(index, edge) {
                        indexes_batch.push((key.into_bytes(), Some(Vec::<u8>::new())));
                    }
                }
            }
            if let Some(namespace) = namespace_from_str(&edge.id) {
                *namespace_edge_deltas
                    .entry(namespace.to_string())
                    .or_default() += 1;
            }

            pending += 1;
            if pending >= FLUSH_EVERY {
                self.edges.fjall_apply_batch(&std::mem::take(&mut edges_batch))?;
                self.indexes.fjall_apply_batch(&std::mem::take(&mut indexes_batch))?;
                pending = 0;
            }
        }
        if pending > 0 {
            self.edges.fjall_apply_batch(&edges_batch)?;
            self.indexes.fjall_apply_batch(&indexes_batch)?;
        }
        for (namespace, delta) in namespace_edge_deltas {
            if delta != 0 {
                self.adjust_meta_counter(namespace_edge_count_key(&namespace), delta)?;
            }
        }
        Ok(())
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        match self.edges.fjall_get(id.as_bytes())? {
            Some(v) => Ok(Some(rmp_serde::from_slice(
                self.decode_record_bytes(v.as_ref())?.as_slice(),
            )?)),
            None => Ok(None),
        }
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<(), StorageError> {
        if let Some(existing) = self.get_edge_record(id)? {
            self.unindex_edge(&existing)?;
            self.apply_edge_stats_delta(&existing, -1)?;
            self.edges.fjall_remove(id.as_bytes())?;
            self.mvcc.delete_edge_record(id)?;
            self.notify_edge_deleted(id);
        }
        Ok(())
    }

    pub fn begin_mvcc_snapshot(&self) -> MvccSnapshot {
        self.mvcc.begin_snapshot()
    }

    pub fn begin_registered_mvcc_snapshot(&self) -> MvccSnapshotLease {
        self.mvcc.begin_registered_snapshot()
    }

    pub fn get_node_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<NodeRecord>, StorageError> {
        self.mvcc.get_node_record_visible_at(snapshot, id)
    }

    pub fn get_nodes_by_label_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        label: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        self.mvcc.get_nodes_by_label_visible_at(snapshot, label)
    }

    pub fn get_edge_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        self.mvcc.get_edge_record_visible_at(snapshot, id)
    }

    pub fn get_edges_by_type_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        self.mvcc.get_edges_by_type_visible_at(snapshot, edge_type)
    }

    pub fn lifecycle_status(&self) -> MvccLifecycleStatus {
        self.mvcc.lifecycle_status()
    }

    pub fn trigger_prune_now(&self, retain_last_n_versions: u64) -> usize {
        self.mvcc.trigger_prune_now(retain_last_n_versions)
    }

    pub fn prune_mvcc_versions(&self, opts: MvccPruneOptions) -> usize {
        self.mvcc.prune_mvcc_versions(opts)
    }

    pub fn pause_lifecycle(&self) {
        self.mvcc.pause_lifecycle();
    }

    pub fn resume_lifecycle(&self) {
        self.mvcc.resume_lifecycle();
    }

    pub fn set_lifecycle_schedule_ms(&self, interval_ms: u64) {
        self.mvcc.set_lifecycle_schedule_ms(interval_ms);
    }

    pub fn top_lifecycle_debt_keys(&self, limit: usize) -> Vec<MvccLifecycleDebtKey> {
        self.mvcc.top_lifecycle_debt_keys(limit)
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        let prefix = edge_type_index_prefix(edge_type);
        let mut out = Vec::new();

        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries (suppressed entities)
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(edge_id) = key_str.rsplit('/').next() {
                // Use direct lookup — index keys don't carry edge values
                if let Some(edge) = self.get_edge_record(edge_id)? {
                    out.push(edge);
                }
            }
        }

        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub fn find_edge_between(
        &self,
        start_node: &str,
        edge_type: &str,
        end_node: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        let prefix = edge_start_type_index_prefix(start_node, edge_type);
        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(edge_id) = key_str.rsplit('/').next() {
                if let Some(edge) = self.get_edge_record(edge_id)? {
                    if edge.end_node == end_node {
                        return Ok(Some(edge));
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn all_edges(&self) -> Result<Vec<EdgeRecord>, StorageError> {
        let mut out = Vec::new();
        for entry in self.edges.fjall_iter() {
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
        for entry in self.edges.fjall_iter() {
            cancel.check_cancelled()?;
            let (_key, value) = entry?;
            let raw = self.decode_record_bytes(value.as_ref())?;
            let edge: EdgeRecord = rmp_serde::from_slice(raw.as_slice())?;
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

    /// Stream all edges directly from the edge keyspace, deserializing
    /// values from the iterator rather than performing a separate storage
    /// lookup per edge.  This is 50-100× faster than [`all_edges`] for BFS
    /// adjacency-map construction.
    pub fn bfs_stream_edges<F>(&self, mut visit: F) -> Result<u64, StorageError>
    where
        F: FnMut(EdgeRecord) -> Result<(), StorageError>,
    {
        let mut streamed = 0u64;
        for entry in self.edges.fjall_iter() {
            let (_key, value) = entry?;
            let raw = self.decode_record_bytes(value.as_ref())?;
            let edge: EdgeRecord = rmp_serde::from_slice(raw.as_slice())?;
            match visit(edge) {
                Ok(()) => streamed += 1,
                Err(StorageError::IterationStopped) => {
                    streamed += 1;
                    return Ok(streamed);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(streamed)
    }

    /// Stream edges of a specific type using the edge-type index to pre-filter.
    /// Avoids scanning and deserializing every edge in the database —
    /// only edges matching `edge_type` are touched.
    pub fn bfs_stream_edges_by_type<F>(
        &self,
        edge_type: &str,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(EdgeRecord) -> Result<(), StorageError>,
    {
        let prefix = edge_type_index_prefix(edge_type);
        let mut streamed = 0u64;
        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(edge_id) = key_str.rsplit('/').next() {
                // Look up the edge value directly — index only carries IDs
                if let Some(raw) = self.edges.fjall_get(edge_id.as_bytes())? {
                    let decoded = self.decode_record_bytes(raw.as_ref())?;
                    let edge: EdgeRecord = rmp_serde::from_slice(decoded.as_slice())?;
                    match visit(edge) {
                        Ok(()) => streamed += 1,
                        Err(StorageError::IterationStopped) => {
                            streamed += 1;
                            return Ok(streamed);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(streamed)
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

    pub fn get_adjacent_edges(
        &self,
        node_id: &str,
        direction: EdgeAdjacencyDirection,
        edge_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        match (direction, edge_type) {
            (EdgeAdjacencyDirection::Outgoing, Some(edge_type)) => {
                self.get_edges_from_node_by_type(node_id, edge_type)
            }
            (EdgeAdjacencyDirection::Outgoing, None) => self.get_edges_from_node(node_id),
            (EdgeAdjacencyDirection::Incoming, Some(edge_type)) => {
                self.get_edges_to_node_by_type(node_id, edge_type)
            }
            (EdgeAdjacencyDirection::Incoming, None) => self.get_edges_to_node(node_id),
            (EdgeAdjacencyDirection::Both, Some(edge_type)) => {
                let mut edges = self.get_edges_from_node_by_type(node_id, edge_type)?;
                edges.extend(self.get_edges_to_node_by_type(node_id, edge_type)?);
                Ok(edges)
            }
            (EdgeAdjacencyDirection::Both, None) => {
                let mut edges = self.get_edges_from_node(node_id)?;
                edges.extend(self.get_edges_to_node(node_id)?);
                Ok(edges)
            }
        }
    }

    pub fn edge_count_by_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        if let Some(namespace) = namespace_from_prefix(prefix) {
            return self.meta_counter(namespace_edge_count_key(namespace));
        }
        Ok(self.edges.scan_prefix(prefix.as_bytes()).count() as u64)
    }

    pub fn node_count_by_label_in_namespace(
        &self,
        namespace: &str,
        label: &str,
    ) -> Result<u64, StorageError> {
        self.meta_counter(namespace_label_count_key(namespace, label))
    }

    pub fn list_namespaces(&self) -> Result<Vec<String>, StorageError> {
        let mut out = BTreeSet::new();

        for entry in self.meta.scan_prefix(META_NAMESPACE_NODE_COUNT_PREFIX) {
            let (key, _) = entry?;
            if let Some(namespace) =
                namespace_from_stats_key(key.as_ref(), META_NAMESPACE_NODE_COUNT_PREFIX)
            {
                out.insert(namespace);
            }
        }

        for entry in self.meta.scan_prefix(META_NAMESPACE_EDGE_COUNT_PREFIX) {
            let (key, _) = entry?;
            if let Some(namespace) =
                namespace_from_stats_key(key.as_ref(), META_NAMESPACE_EDGE_COUNT_PREFIX)
            {
                out.insert(namespace);
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(peer)?)?;
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(profile)?)?;
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(placement)?)?;
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(database)?)?;
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(constraint)?)?;
        Ok(())
    }

    pub fn persist_constraint_for_namespace(
        &self,
        namespace: &str,
        constraint: &Constraint,
    ) -> Result<(), StorageError> {
        let key = namespace_schema_constraint_key(namespace, &constraint.name);
        self.meta.fjall_insert(key, rmp_serde::to_vec(constraint)?)?;
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

    pub fn load_constraints_for_namespace(
        &self,
        namespace: &str,
    ) -> Result<Vec<Constraint>, StorageError> {
        let mut constraints: Vec<Constraint> = Vec::new();
        let prefix = namespace_schema_constraint_prefix(namespace);
        for entry in self.meta.scan_prefix(prefix.as_bytes()) {
            let (_, value) = entry?;
            constraints.push(rmp_serde::from_slice(value.as_ref())?);
        }
        constraints.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(constraints)
    }

    pub fn delete_constraint(&self, name: &str) -> Result<bool, StorageError> {
        let key = [META_SCHEMA_CONSTRAINT_PREFIX, name.as_bytes()].concat();
        Ok(self.meta.fjall_remove(key)?.is_some())
    }

    pub fn delete_constraint_for_namespace(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<bool, StorageError> {
        let key = namespace_schema_constraint_key(namespace, name);
        Ok(self.meta.fjall_remove(key)?.is_some())
    }

    pub fn persist_index_definition(&self, index: &IndexDefinition) -> Result<(), StorageError> {
        self.persist_index_definition_with_cancellation(index, &RequestCancellation::new())
    }

    pub fn persist_index_definition_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &RequestCancellation,
    ) -> Result<(), StorageError> {
        let key = [META_SCHEMA_INDEX_PREFIX, index.name.as_bytes()].concat();
        self.meta.fjall_insert(key, rmp_serde::to_vec(index)?)?;
        if is_node_property_index(index) {
            self.rebuild_node_property_index_with_cancellation(index, cancel)?;
        } else if is_node_fulltext_index(index) {
            self.rebuild_node_fulltext_index_with_cancellation(index, cancel)?;
        } else if is_relationship_property_index(index) {
            self.rebuild_relationship_property_index_with_cancellation(index, cancel)?;
        }
        Ok(())
    }

    /// Persist vector index options (separate from the main index definition to avoid
    /// changing the widely-used IndexDefinition struct).
    pub fn persist_index_options(
        &self,
        index_name: &str,
        options: &HashMap<String, serde_json::Value>,
    ) -> Result<(), StorageError> {
        let key = [META_INDEX_OPTIONS_PREFIX, index_name.as_bytes()].concat();
        self.meta.fjall_insert(key, rmp_serde::to_vec(options)?)?;
        Ok(())
    }

    /// Load vector index options for a named index.
    pub fn load_index_options(
        &self,
        index_name: &str,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, StorageError> {
        let key = [META_INDEX_OPTIONS_PREFIX, index_name.as_bytes()].concat();
        let Some(value) = self.meta.fjall_get(key)? else {
            return Ok(None);
        };
        Ok(Some(rmp_serde::from_slice(value.as_ref())?))
    }

    /// Delete index options for a named index.
    pub fn delete_index_options(&self, index_name: &str) -> Result<(), StorageError> {
        let key = [META_INDEX_OPTIONS_PREFIX, index_name.as_bytes()].concat();
        self.meta.fjall_remove(key)?;
        Ok(())
    }

    /// Rebuild all property and fulltext indexes from stored node/edge records.
    ///
    /// Useful for recovery after index corruption, or for warming indexes on
    /// cold start. Clears existing index entries for each index first, then
    /// re-indexes all matching records.
    ///
    /// Returns counts of indexes rebuilt per category.
    pub fn rebuild_all_indexes(&self) -> Result<(usize, usize, usize), StorageError> {
        let definitions = self.load_index_definitions()?;
        let mut node_prop = 0usize;
        let mut node_fulltext = 0usize;
        let mut rel_prop = 0usize;

        for index in &definitions {
            if is_node_property_index(index) {
                self.rebuild_node_property_index(index)?;
                node_prop += 1;
            } else if is_node_fulltext_index(index) {
                self.rebuild_node_fulltext_index(index)?;
                node_fulltext += 1;
            } else if is_relationship_property_index(index) {
                self.rebuild_relationship_property_index(index)?;
                rel_prop += 1;
            }
        }

        Ok((node_prop, node_fulltext, rel_prop))
    }

    pub fn persist_index_definition_for_namespace(
        &self,
        namespace: &str,
        index: &IndexDefinition,
    ) -> Result<(), StorageError> {
        let key = namespace_schema_index_key(namespace, &index.name);
        self.meta.fjall_insert(key, rmp_serde::to_vec(index)?)?;
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

    pub fn load_index_definitions_for_namespace(
        &self,
        namespace: &str,
    ) -> Result<Vec<IndexDefinition>, StorageError> {
        let mut indexes: Vec<IndexDefinition> = Vec::new();
        let prefix = namespace_schema_index_prefix(namespace);
        for entry in self.meta.scan_prefix(prefix.as_bytes()) {
            let (_, value) = entry?;
            indexes.push(rmp_serde::from_slice(value.as_ref())?);
        }
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(indexes)
    }

    pub fn schema_for_namespace(&self, namespace: &str) -> Result<NamespaceSchema, StorageError> {
        Ok(NamespaceSchema {
            constraints: self.load_constraints_for_namespace(namespace)?,
            indexes: self.load_index_definitions_for_namespace(namespace)?,
        })
    }

    pub fn delete_index_definition(&self, name: &str) -> Result<bool, StorageError> {
        let existing = self
            .load_index_definitions()?
            .into_iter()
            .find(|index| index.name == name);
        let key = [META_SCHEMA_INDEX_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.fjall_remove(key)?.is_some();
        if deleted {
            if let Some(index) = existing {
                if is_node_property_index(&index) {
                    self.delete_node_property_index_entries(&index)?;
                } else if is_node_fulltext_index(&index) {
                    self.delete_node_fulltext_index_entries(&index)?;
                } else if is_relationship_property_index(&index) {
                    self.delete_relationship_property_index_entries(&index)?;
                }
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
        if self.meta.fjall_get(&key)?.is_some() || self.meta.fjall_get(binding_key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "decay profile {}",
                profile.name
            )));
        }
        self.meta.fjall_insert(key, rmp_serde::to_vec(profile)?)?;
        Ok(())
    }

    pub fn persist_decay_profile_binding_schema(
        &self,
        binding: &DecayProfileBindingSchema,
    ) -> Result<(), StorageError> {
        validate_decay_profile_binding(binding)?;
        if let Some(profile_ref) = &binding.profile_ref {
            let profile_key = [META_KP_DECAY_PROFILE_PREFIX, profile_ref.as_bytes()].concat();
            if self.meta.fjall_get(profile_key)?.is_none() {
                return Err(StorageError::KnowledgePolicyNotFound(format!(
                    "decay profile {}",
                    profile_ref
                )));
            }
        }

        let key = [META_KP_DECAY_BINDING_PREFIX, binding.name.as_bytes()].concat();
        let profile_key = [META_KP_DECAY_PROFILE_PREFIX, binding.name.as_bytes()].concat();
        if self.meta.fjall_get(&key)?.is_some() || self.meta.fjall_get(profile_key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "decay profile {}",
                binding.name
            )));
        }

        let mut persisted = binding.clone();
        persisted.target_labels.sort();
        self.meta.fjall_insert(key, rmp_serde::to_vec(&persisted)?)?;
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
        let raw = self.meta.fjall_get(&key)?.ok_or_else(|| {
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(&profile)?)?;
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
        let deleted = self.meta.fjall_remove(key)?.is_some();
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
        let deleted = self.meta.fjall_remove(key)?.is_some();
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(metadata)?)?;
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
        self.meta.fjall_remove(key)?;
        Ok(())
    }

    pub fn persist_promotion_profile_schema(
        &self,
        profile: &PromotionProfileSchema,
    ) -> Result<(), StorageError> {
        validate_promotion_profile(profile)?;
        let key = [META_KP_PROMOTION_PROFILE_PREFIX, profile.name.as_bytes()].concat();
        if self.meta.fjall_get(&key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "promotion profile {}",
                profile.name
            )));
        }
        self.meta.fjall_insert(key, rmp_serde::to_vec(profile)?)?;
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
        let raw = self.meta.fjall_get(&key)?.ok_or_else(|| {
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(&profile)?)?;
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
        let deleted = self.meta.fjall_remove(key)?.is_some();
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
        if self.meta.fjall_get(&key)?.is_some() {
            return Err(StorageError::KnowledgePolicyAlreadyExists(format!(
                "promotion policy {}",
                policy.name
            )));
        }
        self.meta.fjall_insert(key, rmp_serde::to_vec(policy)?)?;
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
        let raw = self.meta.fjall_get(&key)?.ok_or_else(|| {
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
        self.meta.fjall_insert(key, rmp_serde::to_vec(&policy)?)?;
        Ok(())
    }

    pub fn delete_promotion_policy_schema(
        &self,
        name: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        let key = [META_KP_PROMOTION_POLICY_PREFIX, name.as_bytes()].concat();
        let deleted = self.meta.fjall_remove(key)?.is_some();
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
        self.indexes.fjall_insert(key, value)?;
        Ok(())
    }

    /// Get an index entry.
    pub fn get_index(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.indexes.fjall_get(key)?.map(|v| v.to_vec()))
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.persist(fjall::PersistMode::SyncAll).map_err(StorageError::Fjall)?;
        Ok(())
    }

    /// Acquire a flush guard. When dropped, flushes all pending writes to disk.
    pub fn hold_flush(self: &Arc<Self>) -> FlushGuard {
        FlushGuard {
            storage: Arc::clone(self),
        }
    }

    /// Return the on-disk size in bytes.
    pub fn size_on_disk(&self) -> u64 {
        self.db.disk_space().map_err(StorageError::Fjall).unwrap_or(0)
    }

    fn index_node_labels(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for label in &node.labels {
            self.indexes
                .insert(label_index_key(label, &node.id).as_bytes(), [])?;
        }
        Ok(())
    }

    fn index_node_properties(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for index in self.node_property_index_definitions()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            if let Some(key) = node_property_index_key_for_node(&index, node) {
                self.indexes.fjall_insert(key.as_bytes(), [])?;
            }
        }
        for index in self.node_fulltext_index_definitions()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            self.index_node_fulltext_entries(&index, node)?;
        }
        Ok(())
    }

    fn unindex_node_properties(&self, node: &NodeRecord) -> Result<(), StorageError> {
        for index in self.node_property_index_definitions()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            if let Some(key) = node_property_index_key_for_node(&index, node) {
                self.indexes.fjall_remove(key.as_bytes())?;
            }
        }
        for index in self.node_fulltext_index_definitions()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            self.delete_node_fulltext_entries(&index, node)?;
        }
        Ok(())
    }

    fn has_node_property_index(&self, label: &str, property: &str) -> Result<bool, StorageError> {
        Ok(self
            .node_property_index_definitions()?
            .iter()
            .any(|index| index.label == label && index.properties[0] == property))
    }

    fn has_exact_node_property_index(
        &self,
        label: &str,
        properties: &[String],
    ) -> Result<bool, StorageError> {
        Ok(self
            .node_property_index_definitions()?
            .iter()
            .any(|index| index.label == label && index.properties == properties))
    }

    fn node_property_index_definitions(&self) -> Result<Vec<IndexDefinition>, StorageError> {
        Ok(self
            .load_index_definitions()?
            .into_iter()
            .filter(is_node_property_index)
            .collect())
    }

    fn node_fulltext_index_definitions(&self) -> Result<Vec<IndexDefinition>, StorageError> {
        Ok(self
            .load_index_definitions()?
            .into_iter()
            .filter(is_node_fulltext_index)
            .collect())
    }

    fn rebuild_node_property_index(&self, index: &IndexDefinition) -> Result<(), StorageError> {
        self.rebuild_node_property_index_with_cancellation(index, &RequestCancellation::new())
    }

    fn rebuild_node_property_index_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &RequestCancellation,
    ) -> Result<(), StorageError> {
        self.delete_node_property_index_entries_with_cancellation(index, cancel)?;
        let mut batch = Batch::new();
        let mut pending: usize = 0;
        self.stream_nodes_by_label_with_cancellation(&index.label, cancel, |node| {
            if let Some(key) = node_property_index_key_for_node(index, &node) {
                batch.push((key.into_bytes(), Some(Vec::<u8>::new())));
                pending += 1;
                if pending >= 4096 {
                    self.indexes.fjall_apply_batch(&std::mem::take(&mut batch))?;
                    pending = 0;
                }
            }
            Ok(())
        })?;
        if pending > 0 {
            self.indexes.fjall_apply_batch(&batch)?;
        }
        Ok(())
    }

    fn rebuild_node_fulltext_index(&self, index: &IndexDefinition) -> Result<(), StorageError> {
        self.rebuild_node_fulltext_index_with_cancellation(index, &RequestCancellation::new())
    }

    fn rebuild_node_fulltext_index_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &RequestCancellation,
    ) -> Result<(), StorageError> {
        self.delete_node_fulltext_index_entries_with_cancellation(index, cancel)?;
        let mut batch = Batch::new();
        let mut pending: usize = 0;
        self.stream_nodes_by_label_with_cancellation(&index.label, cancel, |node| {
            self.batch_node_fulltext_entries(index, &node, &mut batch, &mut pending)?;
            Ok(())
        })?;
        if pending > 0 {
            self.indexes.fjall_apply_batch(&batch)?;
        }
        Ok(())
    }

    fn batch_node_fulltext_entries(
        &self,
        index: &IndexDefinition,
        node: &NodeRecord,
        batch: &mut Batch,
        pending: &mut usize,
    ) -> Result<(), StorageError> {
        for property in &index.properties {
            let Some(value) = node.properties.get(property) else {
                continue;
            };
            for token in fulltext_tokens_for_value(value) {
                batch.push((
                    node_fulltext_index_key(&index.label, property, &token, &node.id).into_bytes(),
                    Some(Vec::<u8>::new()),
                ));
                *pending += 1;
                if *pending >= 4096 {
                    self.indexes.fjall_apply_batch(&std::mem::take(batch))?;
                    *pending = 0;
                }
            }
        }
        Ok(())
    }

    fn delete_node_property_index_entries(
        &self,
        index: &IndexDefinition,
    ) -> Result<(), StorageError> {
        self.delete_node_property_index_entries_with_cancellation(
            index,
            &RequestCancellation::new(),
        )
    }

    fn delete_node_property_index_entries_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &RequestCancellation,
    ) -> Result<(), StorageError> {
        let prefix = node_property_index_definition_prefix(&index.label, &index.properties);
        let mut batch = Batch::new();
        let mut pending: usize = 0;
        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            batch.push((key, None));
            pending += 1;
            if pending >= 4096 {
                self.indexes.fjall_apply_batch(&std::mem::take(&mut batch))?;
                pending = 0;
                cancel.check_cancelled()?;
            }
        }
        if pending > 0 {
            self.indexes.fjall_apply_batch(&batch)?;
        }
        Ok(())
    }

    fn delete_node_fulltext_index_entries(
        &self,
        index: &IndexDefinition,
    ) -> Result<(), StorageError> {
        self.delete_node_fulltext_index_entries_with_cancellation(
            index,
            &RequestCancellation::new(),
        )
    }

    fn delete_node_fulltext_index_entries_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &RequestCancellation,
    ) -> Result<(), StorageError> {
        for property in &index.properties {
            let prefix = node_fulltext_property_prefix(&index.label, property);
            let mut batch = Batch::new();
            let mut pending: usize = 0;
            for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
                let (key, _) = entry?;
                batch.push((key, None));
                pending += 1;
                if pending >= 4096 {
                    self.indexes.fjall_apply_batch(&std::mem::take(&mut batch))?;
                    pending = 0;
                    cancel.check_cancelled()?;
                }
            }
            if pending > 0 {
                self.indexes.fjall_apply_batch(&batch)?;
            }
        }
        Ok(())
    }

    fn index_node_fulltext_entries(
        &self,
        index: &IndexDefinition,
        node: &NodeRecord,
    ) -> Result<(), StorageError> {
        for property in &index.properties {
            let Some(value) = node.properties.get(property) else {
                continue;
            };
            for token in fulltext_tokens_for_value(value) {
                self.indexes.fjall_insert(
                    node_fulltext_index_key(&index.label, property, &token, &node.id).as_bytes(),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn delete_node_fulltext_entries(
        &self,
        index: &IndexDefinition,
        node: &NodeRecord,
    ) -> Result<(), StorageError> {
        for property in &index.properties {
            let Some(value) = node.properties.get(property) else {
                continue;
            };
            for token in fulltext_tokens_for_value(value) {
                self.indexes.fjall_remove(
                    node_fulltext_index_key(&index.label, property, &token, &node.id).as_bytes(),
                )?;
            }
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
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(edge_id) = key_str.rsplit('/').next() {
                if let Some(edge) = self.get_edge_record(edge_id)? {
                    out.push(edge);
                }
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    fn load_nodes_from_index_prefix(&self, prefix: &str) -> Result<Vec<NodeRecord>, StorageError> {
        let mut out = Vec::new();
        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(node_id) = key_str.rsplit('/').next() {
                if let Some(node) = self.get_node_record(node_id)? {
                    out.push(node);
                }
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    // ── Batch write infrastructure ───────────────────────────────────────

    fn index_edge(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes.fjall_insert(
            edge_type_index_key(&edge.edge_type, &edge.id).as_bytes(),
            [],
        )?;
        self.indexes.fjall_insert(
            edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id).as_bytes(),
            [],
        )?;
        self.indexes.fjall_insert(
            edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id).as_bytes(),
            [],
        )?;
        self.index_edge_property_indexes(edge)?;
        Ok(())
    }

    fn unindex_edge(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes
            .remove(edge_type_index_key(&edge.edge_type, &edge.id).as_bytes())?;
        self.indexes
            .remove(edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id).as_bytes())?;
        self.indexes
            .remove(edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id).as_bytes())?;
        self.unindex_edge_property_indexes(edge)?;
        Ok(())
    }

    fn apply_node_stats_delta(&self, node: &NodeRecord, delta: i64) -> Result<(), StorageError> {
        let Some(namespace) = namespace_from_str(&node.id) else {
            return Ok(());
        };
        self.adjust_meta_counter(namespace_node_count_key(namespace), delta)?;
        for label in node.labels.iter().collect::<BTreeSet<_>>() {
            self.adjust_meta_counter(namespace_label_count_key(namespace, label), delta)?;
        }
        Ok(())
    }

    fn apply_edge_stats_delta(&self, edge: &EdgeRecord, delta: i64) -> Result<(), StorageError> {
        let Some(namespace) = namespace_from_str(&edge.id) else {
            return Ok(());
        };
        self.adjust_meta_counter(namespace_edge_count_key(namespace), delta)
    }

    fn meta_counter(&self, key: Vec<u8>) -> Result<u64, StorageError> {
        match self.meta.fjall_get(key)? {
            Some(raw) => Ok(rmp_serde::from_slice(raw.as_ref())?),
            None => Ok(0),
        }
    }

    fn adjust_meta_counter(&self, key: Vec<u8>, delta: i64) -> Result<(), StorageError> {
        let current = self.meta_counter(key.clone())?;
        let updated = if delta >= 0 {
            current.saturating_add(delta as u64)
        } else {
            current.saturating_sub(delta.unsigned_abs())
        };

        if updated == 0 {
            self.meta.fjall_remove(key)?;
        } else {
            self.meta.fjall_insert(key, rmp_serde::to_vec(&updated)?)?;
        }
        Ok(())
    }

    fn ids_with_prefix(&self, tree: &Tree, prefix: &str) -> Result<Vec<String>, StorageError> {
        tree.scan_prefix(prefix.as_bytes())
            .map(|entry| {
                let (key, _) = entry?;
                std::str::from_utf8(key.as_ref())
                    .map(str::to_string)
                    .map_err(|_| StorageError::InvalidUtf8)
            })
            .collect()
    }

    fn stream_node_records_from_entries<F, I, K, V, E>(
        &self,
        iter: I,
        cancel: &RequestCancellation,
        mut visit: F,
    ) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
        I: IntoIterator<Item = Result<(K, V), E>>,
        E: std::fmt::Display,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut streamed = 0;
        for entry in iter {
            cancel.check_cancelled()?;
            let (key, value) = entry.map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            let raw = self.decode_record_bytes(value.as_ref())?;
            if let Some(node) = compat_node_record_from_bytes(key_str, &raw)? {
                match visit(node) {
                    Ok(()) => streamed += 1,
                    Err(StorageError::IterationStopped) => {
                        streamed += 1;
                        return Ok(streamed);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(streamed)
    }

    fn swallow_iteration_stopped(err: StorageError) -> Result<u64, StorageError> {
        match err {
            StorageError::IterationStopped => Ok(0),
            other => Err(other),
        }
    }

    fn delete_namespace_metadata(&self, namespace: &str) -> Result<(), StorageError> {
        self.meta.fjall_remove(namespace_node_count_key(namespace))?;
        self.meta.fjall_remove(namespace_edge_count_key(namespace))?;
        self.delete_meta_prefix(&namespace_label_count_prefix(namespace))?;
        self.delete_meta_prefix(namespace_schema_constraint_prefix(namespace).as_bytes())?;
        self.delete_meta_prefix(namespace_schema_index_prefix(namespace).as_bytes())?;
        Ok(())
    }

    fn delete_meta_prefix(&self, prefix: &[u8]) -> Result<(), StorageError> {
        let keys = self
            .meta
            .scan_prefix(prefix)
            .map(|entry| {
                entry
                    .map(|(key, _)| key.to_vec())
                    .map_err(StorageError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            self.meta.fjall_remove(key)?;
        }
        Ok(())
    }
}

/// A RAII guard that flushes pending writes to disk when dropped.
pub struct FlushGuard {
    storage: Arc<StorageEngine>,
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        // Flush fjall to disk. Failures are logged but do not panic — a flush
        // failure should not crash the server.
        if let Err(e) = self.storage.db.persist(fjall::PersistMode::SyncAll).map_err(StorageError::Fjall) {
            tracing::warn!(error = %e, "fjall flush failed during FlushGuard drop");
        }
    }
}

fn label_index_prefix(label: &str) -> String {
    format!("{IDX_LABEL_PREFIX}/{label}/")
}

fn namespace_schema_constraint_prefix(namespace: &str) -> String {
    format!(
        "{}{}/",
        std::str::from_utf8(META_SCHEMA_NAMESPACE_CONSTRAINT_PREFIX)
            .unwrap_or("schema_constraint_ns/"),
        escape_index_component(namespace)
    )
}

fn namespace_schema_constraint_key(namespace: &str, name: &str) -> Vec<u8> {
    format!("{}{}", namespace_schema_constraint_prefix(namespace), name).into_bytes()
}

fn namespace_schema_index_prefix(namespace: &str) -> String {
    format!(
        "{}{}/",
        std::str::from_utf8(META_SCHEMA_NAMESPACE_INDEX_PREFIX).unwrap_or("schema_index_ns/"),
        escape_index_component(namespace)
    )
}

fn namespace_schema_index_key(namespace: &str, name: &str) -> Vec<u8> {
    format!("{}{}", namespace_schema_index_prefix(namespace), name).into_bytes()
}

fn namespace_node_count_key(namespace: &str) -> Vec<u8> {
    [
        META_NAMESPACE_NODE_COUNT_PREFIX,
        escape_index_component(namespace).as_bytes(),
    ]
    .concat()
}

fn namespace_edge_count_key(namespace: &str) -> Vec<u8> {
    [
        META_NAMESPACE_EDGE_COUNT_PREFIX,
        escape_index_component(namespace).as_bytes(),
    ]
    .concat()
}

fn namespace_label_count_key(namespace: &str, label: &str) -> Vec<u8> {
    [
        META_NAMESPACE_LABEL_COUNT_PREFIX,
        escape_index_component(namespace).as_bytes(),
        b"/",
        escape_index_component(label).as_bytes(),
    ]
    .concat()
}

fn namespace_label_count_prefix(namespace: &str) -> Vec<u8> {
    [
        META_NAMESPACE_LABEL_COUNT_PREFIX,
        escape_index_component(namespace).as_bytes(),
        b"/",
    ]
    .concat()
}

fn pending_embedding_key(node_id: &str) -> Vec<u8> {
    [
        META_PENDING_EMBEDDING_PREFIX,
        escape_index_component(node_id).as_bytes(),
    ]
    .concat()
}

fn pending_embedding_id_from_key(key: &[u8]) -> Option<String> {
    let suffix = key.strip_prefix(META_PENDING_EMBEDDING_PREFIX)?;
    let decoded = hex::decode(std::str::from_utf8(suffix).ok()?).ok()?;
    String::from_utf8(decoded).ok()
}

fn label_index_key(label: &str, node_id: &str) -> String {
    format!("{}{}", label_index_prefix(label), node_id)
}

fn tombstone_key(index_key: &str) -> Vec<u8> {
    [META_INDEX_TOMBSTONE_PREFIX, index_key.as_bytes()].concat()
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

fn is_node_property_index(index: &IndexDefinition) -> bool {
    index.entity_type == IndexEntityType::Node
        && is_property_backed_index_kind(index.kind)
        && !index.properties.is_empty()
}

fn is_node_fulltext_index(index: &IndexDefinition) -> bool {
    index.entity_type == IndexEntityType::Node
        && index.kind == IndexKind::FullText
        && !index.properties.is_empty()
}

fn is_property_backed_index_kind(kind: IndexKind) -> bool {
    matches!(kind, IndexKind::Range | IndexKind::Temporal)
}

fn node_property_index_property_prefix(label: &str, property: &str) -> String {
    format!(
        "{IDX_NODE_PROPERTY_PREFIX}/{}/{}/",
        escape_index_component(label),
        escape_index_component(property)
    )
}

fn node_fulltext_property_prefix(label: &str, property: &str) -> String {
    format!(
        "{IDX_NODE_FULLTEXT_PREFIX}/{}/{}/",
        escape_index_component(label),
        escape_index_component(property)
    )
}

fn node_fulltext_token_prefix(label: &str, property: &str, token: &str) -> String {
    format!(
        "{}{}/",
        node_fulltext_property_prefix(label, property),
        escape_index_component(token)
    )
}

fn node_fulltext_index_key(label: &str, property: &str, token: &str, node_id: &str) -> String {
    format!(
        "{}{}",
        node_fulltext_token_prefix(label, property, token),
        node_id
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

fn node_property_index_definition_prefix(label: &str, properties: &[String]) -> String {
    if properties.len() == 1 {
        return node_property_index_property_prefix(label, &properties[0]);
    }

    format!(
        "{IDX_NODE_PROPERTY_PREFIX}/{}/composite/{}/values/",
        escape_index_component(label),
        properties
            .iter()
            .map(|property| escape_index_component(property))
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn node_property_index_lookup_prefix(
    label: &str,
    properties: &[String],
    values: &HashMap<String, serde_json::Value>,
) -> Option<String> {
    let value_refs = properties
        .iter()
        .map(|property| values.get(property))
        .collect::<Option<Vec<_>>>()?;

    if properties.len() == 1 {
        return Some(node_property_index_value_prefix(
            label,
            &properties[0],
            value_refs[0],
        ));
    }

    Some(format!(
        "{}{}/",
        node_property_index_definition_prefix(label, properties),
        value_refs
            .iter()
            .map(|value| property_index_value_key(value))
            .collect::<Vec<_>>()
            .join("/")
    ))
}
fn node_property_index_key_for_node(index: &IndexDefinition, node: &NodeRecord) -> Option<String> {
    let value_refs = index
        .properties
        .iter()
        .map(|property| node.properties.get(property))
        .collect::<Option<Vec<_>>>()?;

    if index.properties.len() == 1 {
        return Some(node_property_index_key(
            &index.label,
            &index.properties[0],
            value_refs[0],
            &node.id,
        ));
    }

    Some(format!(
        "{}{}/{}",
        node_property_index_definition_prefix(&index.label, &index.properties),
        value_refs
            .iter()
            .map(|value| property_index_value_key(value))
            .collect::<Vec<_>>()
            .join("/"),
        node.id
    ))
}

fn escape_index_component(value: &str) -> String {
    hex::encode(value.as_bytes())
}

fn tokenize_fulltext(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .collect()
}

// ── Index key collection for deindex tombstone writes ───────────────────────

fn collect_node_index_keys(node: &NodeRecord) -> Vec<String> {
    let mut keys = Vec::new();
    // Label index keys
    for label in &node.labels {
        keys.push(label_index_key(label, &node.id));
    }
    // Property index keys (range/temporal) and fulltext keys
    for (prop_name, value) in &node.properties {
        for label in &node.labels {
            // Range/temporal property index entries
            keys.push(node_property_index_key(label, prop_name, value, &node.id));
            // Fulltext index entries
            for token in fulltext_tokens_for_value(value) {
                keys.push(node_fulltext_index_key(label, prop_name, &token, &node.id));
            }
        }
    }
    keys
}

fn collect_edge_index_keys(edge: &EdgeRecord) -> Vec<String> {
    vec![
        edge_type_index_key(&edge.edge_type, &edge.id),
        edge_start_index_key(&edge.start_node, &edge.edge_type, &edge.id),
        edge_end_index_key(&edge.end_node, &edge.edge_type, &edge.id),
    ]
}

fn fulltext_tokens_for_value(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Null => Vec::new(),
        serde_json::Value::String(text) => tokenize_fulltext(text),
        serde_json::Value::Bool(boolean) => tokenize_fulltext(&boolean.to_string()),
        serde_json::Value::Number(number) => tokenize_fulltext(&number.to_string()),
        serde_json::Value::Array(values) => {
            values.iter().flat_map(fulltext_tokens_for_value).collect()
        }
        serde_json::Value::Object(_) => Vec::new(),
    }
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
        if binding
            .target_edge_type
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        {
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
        if policy
            .target_edge_type
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            return Err(StorageError::KnowledgePolicyInvalid(
                "promotion policy edge targets require an edge type".into(),
            ));
        }
        if !policy.target_labels.is_empty() || policy.is_wildcard {
            return Err(StorageError::KnowledgePolicyInvalid(
                "promotion policy edge targets cannot also declare node labels or wildcard".into(),
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
        return format!(
            "edge:{}",
            policy.target_edge_type.clone().unwrap_or_default()
        );
    }
    if policy.is_wildcard {
        return "wild:node".to_string();
    }
    let mut sorted = policy.target_labels.clone();
    sorted.sort();
    format!("node:{}", sorted.join("\0"))
}

fn compat_node_record_from_bytes(
    _id: &str,
    raw: &[u8],
) -> Result<Option<NodeRecord>, StorageError> {
    // Single format: NodeRecord only (greenfield — no legacy).
    Ok(rmp_serde::from_slice::<NodeRecord>(raw).ok())
}

fn node_record_to_legacy_props(node: &NodeRecord) -> BTreeMap<String, serde_json::Value> {
    let mut props = node.properties.clone();
    props.insert(
            "_id".to_string(),
            serde_json::Value::String(node.id.clone()),
        );
    props.insert(
            "_labels".to_string(),
            serde_json::Value::Array(
            node.labels
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect()),
        );
    props
}

fn namespace_from_str(id: &str) -> Option<&str> {
    parse_database_prefix(id).map(|(namespace, _)| namespace)
}

fn namespace_from_prefix(prefix: &str) -> Option<&str> {
    prefix
        .strip_suffix(':')
        .filter(|namespace| !namespace.is_empty())
}

fn namespace_from_stats_key(key: &[u8], key_prefix: &[u8]) -> Option<String> {
    let encoded = key.strip_prefix(key_prefix)?;
    let encoded = std::str::from_utf8(encoded).ok()?;
    let decoded = hex::decode(encoded).ok()?;
    String::from_utf8(decoded).ok()
}

// ── BatchWrite: namespace-scoped atomic writes ──────────────────────────────

// ── StorageEventNotifier implementation ─────────────────────────────────────

impl StorageEventNotifier for StorageEngine {
    fn on_node_created(&self, callback: NodeEventCallback) {
        StorageEngine::on_node_created(self, callback);
    }

    fn on_node_updated(&self, callback: NodeEventCallback) {
        StorageEngine::on_node_updated(self, callback);
    }

    fn on_node_deleted(&self, callback: NodeDeleteCallback) {
        StorageEngine::on_node_deleted(self, callback);
    }

    fn on_edge_created(&self, callback: EdgeEventCallback) {
        StorageEngine::on_edge_created(self, callback);
    }

    fn on_edge_updated(&self, callback: EdgeEventCallback) {
        StorageEngine::on_edge_updated(self, callback);
    }

    fn on_edge_deleted(&self, callback: EdgeDeleteCallback) {
        StorageEngine::on_edge_deleted(self, callback);
    }
}

/// An operation buffered for atomic batch commit.
enum BatchOp {
    PutNode(NodeRecord),
    PutEdge(EdgeRecord),
    DeleteNode(String),
    DeleteEdge(String),
}

/// Builder for atomic multi-operation writes within a namespace.
///
/// Operations are buffered and committed atomically: either all succeed or
/// none are applied. Indexes, stats, and MVCC are updated at commit time.
pub struct BatchWriter<'a> {
    engine: &'a StorageEngine,
    ops: Vec<BatchOp>,
}

impl<'a> BatchWriter<'a> {
    pub fn put_node_record(&mut self, node: &NodeRecord) {
        self.ops.push(BatchOp::PutNode(node.clone()));
    }

    pub fn put_edge_record(&mut self, edge: &EdgeRecord) {
        self.ops.push(BatchOp::PutEdge(edge.clone()));
    }

    pub fn delete_node_record(&mut self, id: &str) {
        self.ops.push(BatchOp::DeleteNode(id.to_string()));
    }

    pub fn delete_edge_record(&mut self, id: &str) {
        self.ops.push(BatchOp::DeleteEdge(id.to_string()));
    }

    fn commit(&self) -> Result<(), StorageError> {
        for op in &self.ops {
            match op {
                BatchOp::PutNode(node) => self.engine.put_node_record(node)?,
                BatchOp::PutEdge(edge) => self.engine.put_edge_record(edge)?,
                BatchOp::DeleteNode(id) => {
                    let _ = self.engine.delete_node_record(id);
                }
                BatchOp::DeleteEdge(id) => {
                    let _ = self.engine.delete_edge_record(id);
                }
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[cfg(test)]
mod tests;
