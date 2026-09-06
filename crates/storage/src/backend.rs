use crate::{StorageError, StorageIterator};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions,
    config::CompressionPolicy,
};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageKeyspaceId {
    Meta,
    Nodes,
    Edges,
    Indexes,
}

#[derive(Clone, Debug)]
pub struct StorageBackendOperation {
    pub keyspace: StorageKeyspaceId,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone)]
pub enum StorageKeyspace {
    Fjall {
        id: StorageKeyspaceId,
        inner: Keyspace,
    },
    Memory {
        id: StorageKeyspaceId,
        data: Arc<MemoryStorageData>,
    },
}

impl StorageKeyspace {
    pub(crate) fn id(&self) -> StorageKeyspaceId {
        match self {
            Self::Fjall { id, .. } | Self::Memory { id, .. } => *id,
        }
    }
    fn memory_map(&self) -> Option<&RwLock<BTreeMap<Vec<u8>, Vec<u8>>>> {
        let Self::Memory { id, data } = self else {
            return None;
        };
        Some(match id {
            StorageKeyspaceId::Meta => &data.meta,
            StorageKeyspaceId::Nodes => &data.nodes,
            StorageKeyspaceId::Edges => &data.edges,
            StorageKeyspaceId::Indexes => &data.indexes,
        })
    }

    pub(crate) fn scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> StorageIterator<'a> {
        match self {
            Self::Fjall { inner, .. } => Box::new(inner.prefix(prefix).map(|guard| {
                guard
                    .into_inner()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .map_err(StorageError::Fjall)
            })),
            Self::Memory { .. } => {
                let entries = self
                    .memory_map()
                    .expect("memory keyspace")
                    .read()
                    .range(prefix.to_vec()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .map(|(key, value)| Ok((key.clone(), value.clone())))
                    .collect::<Vec<_>>();
                Box::new(entries.into_iter())
            }
        }
    }

    pub(crate) fn iter<'a>(&'a self) -> StorageIterator<'a> {
        match self {
            Self::Fjall { inner, .. } => Box::new(inner.iter().map(|guard| {
                guard
                    .into_inner()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .map_err(StorageError::Fjall)
            })),
            Self::Memory { .. } => {
                let entries = self
                    .memory_map()
                    .expect("memory keyspace")
                    .read()
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), value.clone())))
                    .collect::<Vec<_>>();
                Box::new(entries.into_iter())
            }
        }
    }

    pub(crate) fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError> {
        match self {
            Self::Fjall { inner, .. } => Ok(inner.get(key.as_ref())?.map(|value| value.to_vec())),
            Self::Memory { .. } => Ok(self
                .memory_map()
                .expect("memory keyspace")
                .read()
                .get(key.as_ref())
                .cloned()),
        }
    }

    pub(crate) fn fjall_get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError> {
        self.get(key)
    }

    pub(crate) fn insert(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        match self {
            Self::Fjall { inner, .. } => {
                let key = key.as_ref();
                let old = inner.get(key)?.map(|value| value.to_vec());
                inner.insert(key, value.as_ref())?;
                Ok(old)
            }
            Self::Memory { .. } => Ok(self
                .memory_map()
                .expect("memory keyspace")
                .write()
                .insert(key.as_ref().to_vec(), value.as_ref().to_vec())),
        }
    }

    pub(crate) fn fjall_insert(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.insert(key, value)
    }

    pub(crate) fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, StorageError> {
        match self {
            Self::Fjall { inner, .. } => {
                let key = key.as_ref();
                let old = inner.get(key)?.map(|value| value.to_vec());
                inner.remove(key)?;
                Ok(old)
            }
            Self::Memory { .. } => Ok(self
                .memory_map()
                .expect("memory keyspace")
                .write()
                .remove(key.as_ref())),
        }
    }

    pub(crate) fn fjall_remove(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.remove(key)
    }

    pub(crate) fn apply_batch(
        &self,
        batch: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(), StorageError> {
        for (key, value) in batch {
            match value {
                Some(value) => self.insert(key, value)?,
                None => self.remove(key)?,
            };
        }
        Ok(())
    }

    pub(crate) fn fjall_apply_batch(
        &self,
        batch: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(), StorageError> {
        self.apply_batch(batch)
    }

    pub(crate) fn fjall_iter<'a>(&'a self) -> StorageIterator<'a> {
        self.iter()
    }

    pub(crate) fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool, StorageError> {
        Ok(self.get(key)?.is_some())
    }

    pub(crate) fn fjall_range<'a, R: std::ops::RangeBounds<Vec<u8>> + 'a>(
        &'a self,
        range: R,
    ) -> StorageIterator<'a> {
        match self {
            Self::Fjall { inner, .. } => {
                let start = std::ops::Bound::map(range.start_bound(), Vec::as_slice);
                let end = std::ops::Bound::map(range.end_bound(), Vec::as_slice);
                Box::new(inner.range::<&[u8], _>((start, end)).map(|guard| {
                    guard
                        .into_inner()
                        .map(|(key, value)| (key.to_vec(), value.to_vec()))
                        .map_err(StorageError::Fjall)
                }))
            }
            Self::Memory { .. } => {
                let entries = self
                    .memory_map()
                    .expect("memory keyspace")
                    .read()
                    .range(range)
                    .map(|(key, value)| Ok((key.clone(), value.clone())))
                    .collect::<Vec<_>>();
                Box::new(entries.into_iter())
            }
        }
    }
}

pub trait StorageBackend: Send + Sync {
    fn keyspace(&self, id: StorageKeyspaceId) -> StorageKeyspace;
    fn apply_batch(&self, operations: &[StorageBackendOperation]) -> Result<(), StorageError>;
    fn flush(&self) -> Result<(), StorageError>;
    fn checkpoint(&self) -> Result<(), StorageError>;
    fn size_on_disk(&self) -> u64;
    fn name(&self) -> &'static str;
}

pub struct FjallStorageBackend {
    db: Database,
    meta: Keyspace,
    nodes: Keyspace,
    edges: Keyspace,
    indexes: Keyspace,
}

impl FjallStorageBackend {
    pub(crate) fn open(path: &Path) -> Result<Self, StorageError> {
        fs::create_dir_all(path)?;
        Self::from_database(Database::open(fjall::Config::new(path))?)
    }

    pub(crate) fn from_database(db: Database) -> Result<Self, StorageError> {
        let graph_keyspace_options = || {
            KeyspaceCreateOptions::default()
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
                .index_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
        };
        Ok(Self {
            meta: db.keyspace("meta", graph_keyspace_options)?,
            nodes: db.keyspace("nodes", graph_keyspace_options)?,
            edges: db.keyspace("edges", graph_keyspace_options)?,
            indexes: db.keyspace("indexes", graph_keyspace_options)?,
            db,
        })
    }

    fn keyspace_ref(&self, id: StorageKeyspaceId) -> &Keyspace {
        match id {
            StorageKeyspaceId::Meta => &self.meta,
            StorageKeyspaceId::Nodes => &self.nodes,
            StorageKeyspaceId::Edges => &self.edges,
            StorageKeyspaceId::Indexes => &self.indexes,
        }
    }
}

impl StorageBackend for FjallStorageBackend {
    fn keyspace(&self, id: StorageKeyspaceId) -> StorageKeyspace {
        StorageKeyspace::Fjall {
            id,
            inner: self.keyspace_ref(id).clone(),
        }
    }

    fn apply_batch(&self, operations: &[StorageBackendOperation]) -> Result<(), StorageError> {
        let mut batch = self.db.batch();
        for operation in operations {
            match &operation.value {
                Some(value) => {
                    batch.insert(self.keyspace_ref(operation.keyspace), &operation.key, value)
                }
                None => batch.remove(self.keyspace_ref(operation.keyspace), &operation.key),
            }
        }
        batch.commit()?;
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush()?;
        for keyspace in [&self.meta, &self.nodes, &self.edges, &self.indexes] {
            keyspace.rotate_memtable_and_wait()?;
        }
        self.flush()
    }

    fn size_on_disk(&self) -> u64 {
        self.db.disk_space().unwrap_or(0)
    }

    fn name(&self) -> &'static str {
        "fjall"
    }
}

#[derive(Default)]
pub struct MemoryStorageData {
    meta: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    nodes: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    edges: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    indexes: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Default)]
pub struct MemoryStorageBackend {
    data: Arc<MemoryStorageData>,
}

impl MemoryStorageBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageBackend for MemoryStorageBackend {
    fn keyspace(&self, id: StorageKeyspaceId) -> StorageKeyspace {
        StorageKeyspace::Memory {
            id,
            data: Arc::clone(&self.data),
        }
    }

    fn apply_batch(&self, operations: &[StorageBackendOperation]) -> Result<(), StorageError> {
        let mut meta = self.data.meta.write();
        let mut nodes = self.data.nodes.write();
        let mut edges = self.data.edges.write();
        let mut indexes = self.data.indexes.write();
        for operation in operations {
            let map = match operation.keyspace {
                StorageKeyspaceId::Meta => &mut meta,
                StorageKeyspaceId::Nodes => &mut nodes,
                StorageKeyspaceId::Edges => &mut edges,
                StorageKeyspaceId::Indexes => &mut indexes,
            };
            match &operation.value {
                Some(value) => {
                    map.insert(operation.key.clone(), value.clone());
                }
                None => {
                    map.remove(&operation.key);
                }
            }
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn size_on_disk(&self) -> u64 {
        0
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}

pub(crate) struct StorageBackendBatch<'a> {
    backend: &'a dyn StorageBackend,
    operations: Vec<StorageBackendOperation>,
}

impl<'a> StorageBackendBatch<'a> {
    pub(crate) fn new(backend: &'a dyn StorageBackend) -> Self {
        Self {
            backend,
            operations: Vec::new(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        keyspace: &StorageKeyspace,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.operations.push(StorageBackendOperation {
            keyspace: keyspace.id(),
            key: key.as_ref().to_vec(),
            value: Some(value.as_ref().to_vec()),
        });
    }

    pub(crate) fn remove(&mut self, keyspace: &StorageKeyspace, key: impl AsRef<[u8]>) {
        self.operations.push(StorageBackendOperation {
            keyspace: keyspace.id(),
            key: key.as_ref().to_vec(),
            value: None,
        });
    }

    pub(crate) fn commit(self) -> Result<(), StorageError> {
        self.backend.apply_batch(&self.operations)
    }
}
