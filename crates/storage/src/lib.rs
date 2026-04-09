//! Embedded key-value storage engine for magnetDB.
//!
//! Equivalent to Go's `pkg/storage` in NornicDB (which uses BadgerDB).
//! Uses `sled` as the embedded key-value store, providing:
//! - ACID transactions
//! - Concurrent reads
//! - Crash recovery via write-ahead log
//! - Efficient iteration and prefix scanning
//!
//! # Architecture
//! The storage layer models graph data as three column families:
//! - `nodes`: node ID → serialized node properties
//! - `edges`: edge ID → serialized edge properties
//! - `indexes`: index key → set of node/edge IDs

use bytes::Bytes;
use sled::{Db, Tree};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),
    #[error("key not found: {0}")]
    NotFound(String),
    #[error("transaction conflict")]
    Conflict,
}

/// A single opened magnetDB storage instance.
pub struct StorageEngine {
    db: Db,
    nodes: Tree,
    edges: Tree,
    indexes: Tree,
}

impl StorageEngine {
    /// Open (or create) a storage engine at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = sled::open(path)?;
        let nodes = db.open_tree("nodes")?;
        let edges = db.open_tree("edges")?;
        let indexes = db.open_tree("indexes")?;
        Ok(Self {
            db,
            nodes,
            edges,
            indexes,
        })
    }

    /// Open an in-memory (temporary) storage engine for testing.
    pub fn open_temporary() -> Result<Self, StorageError> {
        let config = sled::Config::new().temporary(true);
        let db = config.open()?;
        let nodes = db.open_tree("nodes")?;
        let edges = db.open_tree("edges")?;
        let indexes = db.open_tree("indexes")?;
        Ok(Self {
            db,
            nodes,
            edges,
            indexes,
        })
    }

    // --- Node operations ---

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

    /// Iterate over all nodes with a given label prefix.
    pub fn scan_nodes_with_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(Bytes, Bytes), StorageError>> + '_ {
        self.nodes
            .scan_prefix(prefix.as_bytes())
            .map(|res| {
                res.map(|(k, v)| (Bytes::from(k.to_vec()), Bytes::from(v.to_vec())))
                    .map_err(StorageError::Sled)
            })
    }

    // --- Edge operations ---

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

    // --- Index operations ---

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

    /// Return the on-disk size in bytes.
    pub fn size_on_disk(&self) -> u64 {
        self.db.size_on_disk().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get_node() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine.put_node("node:1", b"data").unwrap();
        let result = engine.get_node("node:1").unwrap();
        assert_eq!(result, Some(b"data".to_vec()));
    }

    #[test]
    fn test_delete_node() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine.put_node("node:2", b"data").unwrap();
        engine.delete_node("node:2").unwrap();
        assert!(engine.get_node("node:2").unwrap().is_none());
    }

    #[test]
    fn test_put_get_edge() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine.put_edge("edge:1", b"edge_data").unwrap();
        let result = engine.get_edge("edge:1").unwrap();
        assert_eq!(result, Some(b"edge_data".to_vec()));
    }

    #[test]
    fn test_scan_nodes_with_prefix() {
        let engine = StorageEngine::open_temporary().unwrap();
        engine.put_node("Person:1", b"alice").unwrap();
        engine.put_node("Person:2", b"bob").unwrap();
        engine.put_node("Movie:1", b"matrix").unwrap();

        let persons: Vec<_> = engine.scan_nodes_with_prefix("Person:").collect();
        assert_eq!(persons.len(), 2);
    }
}
