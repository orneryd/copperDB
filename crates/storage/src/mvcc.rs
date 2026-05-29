use crate::StorageError;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
