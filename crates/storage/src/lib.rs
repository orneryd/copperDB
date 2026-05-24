//! Embedded key-value storage engine for copperdb.
//!
//! Storage layout policy for copper: **version 0 only**.
//! This crate intentionally avoids legacy migration arms and only supports
//! opening databases whose manifest declares layout version 0.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const STORAGE_LAYOUT_VERSION: u8 = 0;
const META_LAYOUT_MANIFEST_KEY: &[u8] = b"layout_manifest";
const META_SEARCH_PEER_PREFIX: &[u8] = b"search_peer/";
const META_HYPERSCALER_PROFILE_PREFIX: &[u8] = b"hyperscaler_profile/";
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
pub struct HyperScalerProfile {
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
}

impl StorageEngine {
    /// Open (or create) a storage engine at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = sled::open(path)?;
        Self::open_with_db(db)
    }

    /// Open an in-memory (temporary) storage engine for testing.
    pub fn open_temporary() -> Result<Self, StorageError> {
        let path = std::env::temp_dir().join(format!("copperdb-storage-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path)?;
        let db = sled::open(path)?;
        Self::open_with_db(db)
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
            .insert(node.id.as_bytes(), rmp_serde::to_vec_named(node)?)?;
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
            let key_str = std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
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
            .insert(edge.id.as_bytes(), rmp_serde::to_vec_named(edge)?)?;
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
            let key_str = std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
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
        self.meta.insert(key, rmp_serde::to_vec_named(peer)?)?;
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
        profile: &HyperScalerProfile,
    ) -> Result<(), StorageError> {
        let key = [META_HYPERSCALER_PROFILE_PREFIX, profile.profile_id.as_bytes()].concat();
        self.meta.insert(key, rmp_serde::to_vec_named(profile)?)?;
        Ok(())
    }

    pub fn list_hyperscaler_profiles(&self) -> Result<Vec<HyperScalerProfile>, StorageError> {
        let mut profiles: Vec<HyperScalerProfile> = Vec::new();
        for entry in self.meta.scan_prefix(META_HYPERSCALER_PROFILE_PREFIX) {
            let (_, v) = entry?;
            profiles.push(rmp_serde::from_slice(v.as_ref())?);
        }
        profiles.sort_by(|a, b| a.profile_id.cmp(&b.profile_id));
        Ok(profiles)
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
            self.indexes.remove(label_index_key(label, &node.id).as_bytes())?;
        }
        Ok(())
    }

    fn index_edge_type(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        self.indexes
            .insert(edge_type_index_key(&edge.edge_type, &edge.id).as_bytes(), &[])?;
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
        let manifest = engine.layout_manifest().unwrap();
        assert_eq!(manifest.version, STORAGE_LAYOUT_VERSION);
        assert!(manifest.created_at_unix_ms > 0);
        assert_eq!(engine.storage_layout_version().unwrap(), 0);
    }

    #[test]
    fn rejects_non_v0_layout_manifest() {
        let test_dir =
            std::env::temp_dir().join(format!("copperdb-storage-version-test-{}", uuid::Uuid::new_v4()));
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

        assert_eq!(engine.get_node("node:1").unwrap(), Some(b"node_data".to_vec()));
        assert_eq!(engine.get_edge("edge:1").unwrap(), Some(b"edge_data".to_vec()));

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

        engine.put_node_record(&sample_node("alpha:n1", &["Person"])).unwrap();
        engine.put_node_record(&sample_node("alpha:n2", &["Person"])).unwrap();
        engine.put_node_record(&sample_node("beta:n1", &["Robot"])).unwrap();

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
            .register_hyperscaler_profile(&HyperScalerProfile {
                profile_id: "aws-primary".to_string(),
                provider: "aws".to_string(),
                region: "us-east-1".to_string(),
                tier: "prod".to_string(),
                enabled: true,
            })
            .unwrap();
        engine
            .register_hyperscaler_profile(&HyperScalerProfile {
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
        assert_eq!(engine.get_index(b"idx:key").unwrap(), Some(b"idx:value".to_vec()));

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
}
