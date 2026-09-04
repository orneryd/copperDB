//! Replication primitives for copperdb.
//!
//! This crate mirrors the Go package split used in NornicDB:
//! - a storage abstraction for replicated commands
//! - a `Replicator` interface with standalone and clustered implementations
//! - a `ReplicatedEngine` wrapper that routes writes through the replicator
//! - a transport abstraction with an in-memory implementation for tests

use async_trait::async_trait;
use copperdb_localization::{Message, StableLocalizedDiagnostic};
use copperdb_storage::{
    EdgeAdjacencyDirection, EdgeRecord, KnowledgePolicyAccessMetadata, StorageEngine, StorageError,
};
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedWriteMode, DistributedWritePlan,
    LogicalTransactionId, PlacementKey, TopologyError, TopologyRegistry,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{Notify, oneshot};

const REPAIR_QUEUE_PREFIX: &str = "replication:repair:";

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("not a leader: redirect to {0}")]
    NotLeader(String),
    #[error("quorum not reached: required {required}, received {received}")]
    NoQuorum { required: usize, received: usize },
    #[error("transport error: {0}")]
    Transport(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("shutdown in progress")]
    Shutdown,
}

impl From<StorageError> for ReplicationError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl StableLocalizedDiagnostic for ReplicationError {
    fn diagnostic_id(&self) -> &'static str {
        match self {
            Self::NotLeader(_) => "replication.not_leader",
            Self::Timeout(_) => "replication.operation_timed_out",
            Self::Shutdown => "replication.transport.closed",
            Self::NoQuorum { .. } | Self::Transport(_) | Self::Storage(_) => {
                "replication.rpc.remote_apply_failed"
            }
        }
    }

    fn localized_message(&self) -> Message {
        let message = Message::from_catalog(self.diagnostic_id())
            .expect("generated replication catalog entry");
        match self {
            Self::NoQuorum { .. } | Self::Transport(_) | Self::Storage(_) => {
                message.with("Cause", self.to_string())
            }
            _ => message,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Standalone,
    Quorum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub mode: ReplicationMode,
    pub node_id: String,
    pub peers: Vec<String>,
    pub heartbeat_interval: Duration,
    pub election_timeout: Duration,
}

/// High-availability write planner.
///
/// This deliberately plans quorum/raft participation without performing network
/// replication yet. The execution layer can consume this once distributed writes
/// are enabled.
#[derive(Debug, Clone)]
pub struct HighAvailabilityWritePlanner {
    topology: TopologyRegistry,
    mode: DistributedWriteMode,
}

impl HighAvailabilityWritePlanner {
    pub fn new(topology: TopologyRegistry, mode: DistributedWriteMode) -> Self {
        Self { topology, mode }
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub fn mode(&self) -> DistributedWriteMode {
        self.mode
    }

    pub fn plan(&self, placement: &PlacementKey) -> Result<DistributedWritePlan, TopologyError> {
        self.topology.plan_write(placement, self.mode)
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Standalone,
            node_id: "node-1".into(),
            peers: Vec::new(),
            heartbeat_interval: Duration::from_millis(150),
            election_timeout: Duration::from_millis(750),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Noop,
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    CypherMutation {
        database: String,
        query: String,
        params: Value,
    },
    PutKnowledgePolicyAccessMetadata {
        entity_id: String,
        metadata: KnowledgePolicyAccessMetadata,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConfigAction {
    AddNode,
    RemoveNode,
    UpdateNode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogPayload {
    Command(Command),
    ConfigChange {
        action: ConfigAction,
        node_id: String,
        address: String,
    },
    SnapshotMarker {
        snapshot_id: String,
        checksum: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: LogPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub voter_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
    pub responder_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub mode: ReplicationMode,
    pub node_id: String,
    pub role: NodeRole,
    pub leader_id: Option<String>,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_len: usize,
    pub quorum_size: usize,
    pub peer_count: usize,
}

#[async_trait]
pub trait ReplicationStorage: Send + Sync {
    fn apply_command(&self, command: &Command) -> Result<(), ReplicationError>;
    fn read_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ReplicationError> {
        let _ = key;
        Ok(None)
    }
    fn graph_node(&self, node_id: &str) -> Result<Option<Vec<u8>>, ReplicationError> {
        let _ = node_id;
        Ok(None)
    }
    fn graph_edges_from_node(
        &self,
        node_id: &str,
        rel_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        let _ = (node_id, rel_type);
        Ok(Vec::new())
    }
    fn graph_edges_to_node(
        &self,
        node_id: &str,
        rel_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        let _ = (node_id, rel_type);
        Ok(Vec::new())
    }
    fn graph_nodes_by_label(&self, label: &str) -> Result<Vec<Vec<u8>>, ReplicationError> {
        let _ = label;
        Ok(Vec::new())
    }
    fn graph_nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Value,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        let _ = (label, property, value);
        Ok(Vec::new())
    }
    fn graph_access_metadata(
        &self,
        entity_id: &str,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, ReplicationError> {
        let _ = entity_id;
        Ok(None)
    }
    fn write_snapshot(&self) -> Result<Vec<u8>, ReplicationError>;
    fn restore_snapshot(&self, snapshot: &[u8]) -> Result<(), ReplicationError>;
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MemorySnapshot {
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
    cypher: Vec<(String, String, Value)>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MemorySnapshotWire {
    kv: Vec<(Vec<u8>, Vec<u8>)>,
    cypher: Vec<(String, String, Value)>,
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    state: RwLock<MemorySnapshot>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state.read().unwrap().kv.get(key).cloned()
    }

    pub fn cypher_log(&self) -> Vec<(String, String, Value)> {
        self.state.read().unwrap().cypher.clone()
    }
}

#[async_trait]
impl ReplicationStorage for MemoryStorage {
    fn apply_command(&self, command: &Command) -> Result<(), ReplicationError> {
        let mut state = self.state.write().unwrap();
        match command {
            Command::Noop => {}
            Command::Put { key, value } => {
                state.kv.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                state.kv.remove(key);
            }
            Command::CypherMutation {
                database,
                query,
                params,
            } => {
                state
                    .cypher
                    .push((database.clone(), query.clone(), params.clone()));
            }
            Command::PutKnowledgePolicyAccessMetadata { .. } => {}
        }
        Ok(())
    }

    fn read_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ReplicationError> {
        Ok(self.get(key))
    }

    fn write_snapshot(&self) -> Result<Vec<u8>, ReplicationError> {
        let snapshot = self.state.read().unwrap().clone();
        let wire = MemorySnapshotWire {
            kv: snapshot.kv.into_iter().collect(),
            cypher: snapshot.cypher,
        };
        serde_json::to_vec(&wire).map_err(|error| ReplicationError::Storage(error.to_string()))
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> Result<(), ReplicationError> {
        let restored: MemorySnapshotWire = serde_json::from_slice(snapshot)
            .map_err(|error| ReplicationError::Storage(error.to_string()))?;
        *self.state.write().unwrap() = MemorySnapshot {
            kv: restored.kv.into_iter().collect(),
            cypher: restored.cypher,
        };
        Ok(())
    }
}

pub struct StorageEngineAdapter {
    engine: Arc<StorageEngine>,
}

impl StorageEngineAdapter {
    pub fn new(engine: StorageEngine) -> Self {
        Self::from_shared(Arc::new(engine))
    }

    pub fn from_shared(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    fn data_key(key: &[u8]) -> String {
        format!("replication:{}", hex::encode(key))
    }

    fn cypher_key(database: &str, query: &str, params: &Value) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        database.hash(&mut hasher);
        query.hash(&mut hasher);
        params.to_string().hash(&mut hasher);
        format!("replication:cypher:{:x}", hasher.finish())
    }

    fn node_record_bytes(
        record: &copperdb_storage::NodeRecord,
    ) -> Result<Vec<u8>, ReplicationError> {
        let mut props = record.properties.clone();
        props.insert("_id".to_string(), Value::String(record.id.clone()));
        props.insert(
            "_labels".to_string(),
            Value::Array(record.labels.iter().cloned().map(Value::String).collect()),
        );
        props.insert(
            "_created_at_unix_ms".to_string(),
            Value::from(record.created_at_unix_ms),
        );
        props.insert(
            "_updated_at_unix_ms".to_string(),
            Value::from(record.updated_at_unix_ms),
        );
        rmp_serde::to_vec_named(&props)
            .map_err(|error| ReplicationError::Storage(error.to_string()))
    }
}

#[async_trait]
impl ReplicationStorage for StorageEngineAdapter {
    fn apply_command(&self, command: &Command) -> Result<(), ReplicationError> {
        let engine = &self.engine;
        match command {
            Command::Noop => Ok(()),
            Command::Put { key, value } => engine
                .put_node(&Self::data_key(key), value)
                .map_err(Into::into),
            Command::Delete { key } => engine.delete_node(&Self::data_key(key)).map_err(Into::into),
            Command::CypherMutation {
                database,
                query,
                params,
            } => {
                let payload = serde_json::to_vec(params)
                    .map_err(|error| ReplicationError::Storage(error.to_string()))?;
                engine
                    .put_node(&Self::cypher_key(database, query, params), &payload)
                    .map_err(Into::into)
            }
            Command::PutKnowledgePolicyAccessMetadata {
                entity_id,
                metadata,
            } => engine
                .put_knowledge_policy_access_metadata(entity_id, metadata)
                .map_err(Into::into),
        }
    }

    fn read_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ReplicationError> {
        let engine = &self.engine;
        engine.get_node(&Self::data_key(key)).map_err(Into::into)
    }

    fn graph_node(&self, node_id: &str) -> Result<Option<Vec<u8>>, ReplicationError> {
        let engine = &self.engine;
        match engine.get_node_record(node_id) {
            Ok(Some(record)) => Ok(Some(Self::node_record_bytes(&record)?)),
            Ok(None) => engine.get_node(node_id).map_err(Into::into),
            Err(_) => engine.get_node(node_id).map_err(Into::into),
        }
    }

    fn graph_edges_from_node(
        &self,
        node_id: &str,
        rel_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        let engine = &self.engine;
        engine
            .get_adjacent_edges(node_id, EdgeAdjacencyDirection::Outgoing, rel_type)
            .map_err(Into::into)
    }

    fn graph_edges_to_node(
        &self,
        node_id: &str,
        rel_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        let engine = &self.engine;
        engine
            .get_adjacent_edges(node_id, EdgeAdjacencyDirection::Incoming, rel_type)
            .map_err(Into::into)
    }

    fn graph_nodes_by_label(&self, label: &str) -> Result<Vec<Vec<u8>>, ReplicationError> {
        let engine = &self.engine;
        engine
            .get_nodes_by_label(label)?
            .iter()
            .map(Self::node_record_bytes)
            .collect()
    }

    fn graph_nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Value,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        let engine = &self.engine;
        engine
            .get_nodes_by_property(label, property, value)?
            .iter()
            .map(Self::node_record_bytes)
            .collect()
    }

    fn graph_access_metadata(
        &self,
        entity_id: &str,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, ReplicationError> {
        let engine = &self.engine;
        engine
            .get_knowledge_policy_access_metadata(entity_id)
            .map_err(Into::into)
    }

    fn write_snapshot(&self) -> Result<Vec<u8>, ReplicationError> {
        let engine = &self.engine;
        let mut snapshot = BTreeMap::new();
        for entry in engine.scan_nodes_with_prefix("replication:") {
            let (key, value) = entry?;
            snapshot.insert(key.to_vec(), value.to_vec());
        }
        serde_json::to_vec(&snapshot).map_err(|error| ReplicationError::Storage(error.to_string()))
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> Result<(), ReplicationError> {
        let engine = &self.engine;
        let restored: BTreeMap<Vec<u8>, Vec<u8>> = serde_json::from_slice(snapshot)
            .map_err(|error| ReplicationError::Storage(error.to_string()))?;

        let existing: Vec<Vec<u8>> = engine
            .scan_nodes_with_prefix("replication:")
            .map(|entry| entry.map(|(key, _)| key.to_vec()))
            .collect::<Result<_, _>>()?;

        for key in existing {
            let key = String::from_utf8(key)
                .map_err(|error| ReplicationError::Storage(error.to_string()))?;
            engine.delete_node(&key)?;
        }

        for (key, value) in restored {
            let key = String::from_utf8(key)
                .map_err(|error| ReplicationError::Storage(error.to_string()))?;
            engine.put_node(&key, &value)?;
        }

        Ok(())
    }
}

#[async_trait]
pub trait ReplicaTransport: Send + Sync {
    async fn apply_replica(&self, target: &str, command: Command) -> Result<(), ReplicationError>;
    async fn read_replica(
        &self,
        target: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ReplicationError>;
    async fn graph_node(
        &self,
        target: &str,
        node_id: &str,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Option<Vec<u8>>, ReplicationError>;
    async fn graph_edges_from_node(
        &self,
        target: &str,
        node_id: &str,
        rel_type: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError>;
    async fn graph_edges_to_node(
        &self,
        target: &str,
        node_id: &str,
        rel_type: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError>;
    async fn graph_nodes_by_label(
        &self,
        target: &str,
        label: &str,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<Vec<u8>>, ReplicationError>;
    async fn graph_nodes_by_property(
        &self,
        target: &str,
        label: &str,
        property: &str,
        value: &Value,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<Vec<u8>>, ReplicationError>;
    async fn graph_access_metadata(
        &self,
        target: &str,
        entity_id: &str,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, ReplicationError>;
}

#[derive(Default)]
pub struct InMemoryReplicaTransport {
    replicas: RwLock<HashMap<String, Arc<dyn ReplicationStorage>>>,
}

impl InMemoryReplicaTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, node_id: impl Into<String>, storage: Arc<dyn ReplicationStorage>) {
        self.replicas
            .write()
            .unwrap()
            .insert(node_id.into(), storage);
    }

    fn lookup(&self, target: &str) -> Result<Arc<dyn ReplicationStorage>, ReplicationError> {
        self.replicas
            .read()
            .unwrap()
            .get(target)
            .cloned()
            .ok_or_else(|| ReplicationError::Transport(format!("unknown replica {target}")))
    }
}

#[async_trait]
impl ReplicaTransport for InMemoryReplicaTransport {
    async fn apply_replica(&self, target: &str, command: Command) -> Result<(), ReplicationError> {
        self.lookup(target)?.apply_command(&command)
    }

    async fn read_replica(
        &self,
        target: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ReplicationError> {
        self.lookup(target)?.read_key(key)
    }

    async fn graph_node(
        &self,
        target: &str,
        node_id: &str,
        _read_fence: Option<LogicalTransactionId>,
    ) -> Result<Option<Vec<u8>>, ReplicationError> {
        self.lookup(target)?.graph_node(node_id)
    }

    async fn graph_edges_from_node(
        &self,
        target: &str,
        node_id: &str,
        rel_type: Option<&str>,
        _read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.lookup(target)?
            .graph_edges_from_node(node_id, rel_type)
    }

    async fn graph_edges_to_node(
        &self,
        target: &str,
        node_id: &str,
        rel_type: Option<&str>,
        _read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.lookup(target)?.graph_edges_to_node(node_id, rel_type)
    }

    async fn graph_nodes_by_label(
        &self,
        target: &str,
        label: &str,
        _read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.lookup(target)?.graph_nodes_by_label(label)
    }

    async fn graph_nodes_by_property(
        &self,
        target: &str,
        label: &str,
        property: &str,
        value: &Value,
        _read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.lookup(target)?
            .graph_nodes_by_property(label, property, value)
    }

    async fn graph_access_metadata(
        &self,
        target: &str,
        entity_id: &str,
        _read_fence: Option<LogicalTransactionId>,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, ReplicationError> {
        self.lookup(target)?.graph_access_metadata(entity_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedWriteOutcome {
    pub plan: DistributedWritePlan,
    pub acknowledged_by: Vec<String>,
    pub failed_replicas: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedReadOutcome {
    pub plan: DistributedReadPlan,
    pub responded_by: Vec<String>,
    pub failed_replicas: Vec<String>,
    pub value: Option<Vec<u8>>,
}

pub struct CassandraCoordinator {
    topology: TopologyRegistry,
    transport: Arc<dyn ReplicaTransport>,
    repair_queue: Option<Arc<DurableRepairQueue>>,
}

impl CassandraCoordinator {
    pub fn new(topology: TopologyRegistry, transport: Arc<dyn ReplicaTransport>) -> Self {
        Self {
            topology,
            transport,
            repair_queue: None,
        }
    }

    pub fn with_repair_queue(
        topology: TopologyRegistry,
        transport: Arc<dyn ReplicaTransport>,
        repair_queue: Arc<DurableRepairQueue>,
    ) -> Self {
        Self {
            topology,
            transport,
            repair_queue: Some(repair_queue),
        }
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub async fn write(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        command: Command,
        request_region: Option<&str>,
    ) -> Result<DistributedWriteOutcome, ReplicationError> {
        let plan = self
            .topology
            .plan_write_with_consistency(
                placement,
                DistributedWriteMode::DynamoQuorum,
                consistency,
                request_region,
            )
            .map_err(|error| ReplicationError::Storage(error.to_string()))?;
        let mut acknowledged_by = Vec::new();
        let mut failed_replicas = Vec::new();
        for replica in &plan.replicas {
            if self
                .transport
                .apply_replica(&replica.node_id, command.clone())
                .await
                .is_ok()
            {
                acknowledged_by.push(replica.node_id.clone());
            } else {
                failed_replicas.push(replica.node_id.clone());
            }
        }
        if acknowledged_by.len() < plan.required_acks {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_acks,
                received: acknowledged_by.len(),
            });
        }
        self.enqueue_hinted_handoff(&plan, &command, &failed_replicas)?;
        Ok(DistributedWriteOutcome {
            plan,
            acknowledged_by,
            failed_replicas,
        })
    }

    pub async fn read(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        key: &[u8],
        request_region: Option<&str>,
    ) -> Result<DistributedReadOutcome, ReplicationError> {
        let plan = self
            .topology
            .plan_read(placement, consistency, request_region)
            .map_err(|error| ReplicationError::Storage(error.to_string()))?;
        let mut responded_by = Vec::new();
        let mut failed_replicas = Vec::new();
        let mut value = None;
        for replica in &plan.replicas {
            match self.transport.read_replica(&replica.node_id, key).await {
                Ok(replica_value) => {
                    if value.is_none() && replica_value.is_some() {
                        value = replica_value;
                    }
                    responded_by.push(replica.node_id.clone());
                }
                Err(_) => {
                    failed_replicas.push(replica.node_id.clone());
                }
            }
            if responded_by.len() >= plan.required_responses && value.is_some() {
                break;
            }
        }
        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            });
        }
        self.enqueue_read_repairs(&plan, key, &failed_replicas)?;
        Ok(DistributedReadOutcome {
            plan,
            responded_by,
            failed_replicas,
            value,
        })
    }

    fn enqueue_hinted_handoff(
        &self,
        plan: &DistributedWritePlan,
        command: &Command,
        failed_replicas: &[String],
    ) -> Result<(), ReplicationError> {
        let Some(queue) = &self.repair_queue else {
            return Ok(());
        };
        for replica in failed_replicas {
            queue.enqueue(RepairRecord::hinted_handoff(
                plan.placement.clone(),
                replica.clone(),
                command.clone(),
            ))?;
        }
        Ok(())
    }

    fn enqueue_read_repairs(
        &self,
        plan: &DistributedReadPlan,
        key: &[u8],
        failed_replicas: &[String],
    ) -> Result<(), ReplicationError> {
        let Some(queue) = &self.repair_queue else {
            return Ok(());
        };
        for replica in failed_replicas {
            queue.enqueue(RepairRecord::read_repair_probe(
                plan.placement.clone(),
                replica.clone(),
                key.to_vec(),
            ))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RepairKind {
    HintedHandoff,
    ReadRepairProbe,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepairRecord {
    pub id: String,
    pub kind: RepairKind,
    pub placement: PlacementKey,
    pub target_node: String,
    pub command: Option<Command>,
    pub read_key: Option<Vec<u8>>,
    pub transaction_id: LogicalTransactionId,
    pub attempts: u32,
}

impl RepairRecord {
    pub fn hinted_handoff(placement: PlacementKey, target_node: String, command: Command) -> Self {
        Self::new(
            RepairKind::HintedHandoff,
            placement,
            target_node,
            Some(command),
            None,
        )
    }

    pub fn read_repair_probe(
        placement: PlacementKey,
        target_node: String,
        read_key: Vec<u8>,
    ) -> Self {
        Self::new(
            RepairKind::ReadRepairProbe,
            placement,
            target_node,
            None,
            Some(read_key),
        )
    }

    fn new(
        kind: RepairKind,
        placement: PlacementKey,
        target_node: String,
        command: Option<Command>,
        read_key: Option<Vec<u8>>,
    ) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        kind.hash(&mut hasher);
        placement.hash(&mut hasher);
        target_node.hash(&mut hasher);
        if let Some(command) = &command {
            serde_json::to_string(command)
                .unwrap_or_default()
                .hash(&mut hasher);
        }
        read_key.hash(&mut hasher);
        let id = format!(
            "{}:{}:{:x}",
            match kind {
                RepairKind::HintedHandoff => "hint",
                RepairKind::ReadRepairProbe => "read",
            },
            target_node,
            hasher.finish()
        );
        Self {
            id,
            kind,
            placement,
            target_node,
            command,
            read_key,
            transaction_id: LogicalTransactionId::ZERO,
            attempts: 0,
        }
    }

    fn increment_attempts(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairReplayReport {
    pub attempted: usize,
    pub completed: usize,
    pub retained: usize,
}

#[derive(Debug, Clone)]
pub struct DurableRepairQueue {
    path: PathBuf,
}

impl DurableRepairQueue {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplicationError> {
        let path = path.as_ref().to_path_buf();
        StorageEngine::open(&path)?;
        Ok(Self { path })
    }

    pub fn enqueue(&self, record: RepairRecord) -> Result<(), ReplicationError> {
        let storage = StorageEngine::open(&self.path)?;
        storage.put_node(
            &Self::record_key(&record.id),
            &serde_json::to_vec(&record)
                .map_err(|error| ReplicationError::Storage(error.to_string()))?,
        )?;
        Ok(())
    }

    pub fn pending(&self) -> Result<Vec<RepairRecord>, ReplicationError> {
        let storage = StorageEngine::open(&self.path)?;
        let mut records = Vec::new();
        for entry in storage.scan_nodes_with_prefix(REPAIR_QUEUE_PREFIX) {
            let (_, value) = entry?;
            records.push(
                serde_json::from_slice::<RepairRecord>(&value)
                    .map_err(|error| ReplicationError::Storage(error.to_string()))?,
            );
        }
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }

    pub fn delete(&self, record_id: &str) -> Result<(), ReplicationError> {
        let storage = StorageEngine::open(&self.path)?;
        storage.delete_node(&Self::record_key(record_id))?;
        Ok(())
    }

    pub async fn replay_batch(
        &self,
        transport: Arc<dyn ReplicaTransport>,
        max_records: usize,
    ) -> Result<RepairReplayReport, ReplicationError> {
        let mut report = RepairReplayReport::default();
        for mut record in self.pending()?.into_iter().take(max_records.max(1)) {
            report.attempted += 1;
            match Self::replay_record(Arc::clone(&transport), &record).await {
                Ok(()) => {
                    self.delete(&record.id)?;
                    report.completed += 1;
                }
                Err(_) => {
                    record.increment_attempts();
                    self.enqueue(record)?;
                    report.retained += 1;
                }
            }
        }
        Ok(report)
    }

    async fn replay_record(
        transport: Arc<dyn ReplicaTransport>,
        record: &RepairRecord,
    ) -> Result<(), ReplicationError> {
        match record.kind {
            RepairKind::HintedHandoff => {
                let command = record.command.clone().ok_or_else(|| {
                    ReplicationError::Storage("hinted handoff record missing command".into())
                })?;
                transport.apply_replica(&record.target_node, command).await
            }
            RepairKind::ReadRepairProbe => {
                let key = record.read_key.as_ref().ok_or_else(|| {
                    ReplicationError::Storage("read repair record missing key".into())
                })?;
                transport.read_replica(&record.target_node, key).await?;
                Ok(())
            }
        }
    }

    fn record_key(record_id: &str) -> String {
        format!("{REPAIR_QUEUE_PREFIX}{record_id}")
    }
}

#[derive(Debug, Clone)]
pub struct RepairWorkerConfig {
    pub interval: Duration,
    pub max_records_per_tick: usize,
}

impl Default for RepairWorkerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            max_records_per_tick: 100,
        }
    }
}

#[derive(Clone)]
pub struct ScheduledRepairWorker {
    queue: Arc<DurableRepairQueue>,
    transport: Arc<dyn ReplicaTransport>,
    config: RepairWorkerConfig,
}

impl ScheduledRepairWorker {
    pub fn new(
        queue: Arc<DurableRepairQueue>,
        transport: Arc<dyn ReplicaTransport>,
        config: RepairWorkerConfig,
    ) -> Self {
        let defaults = RepairWorkerConfig::default();
        Self {
            queue,
            transport,
            config: RepairWorkerConfig {
                interval: if config.interval.is_zero() {
                    defaults.interval
                } else {
                    config.interval
                },
                max_records_per_tick: config.max_records_per_tick.max(1),
            },
        }
    }

    pub async fn run_once(&self) -> Result<RepairReplayReport, ReplicationError> {
        self.queue
            .replay_batch(
                Arc::clone(&self.transport),
                self.config.max_records_per_tick,
            )
            .await
    }

    pub fn spawn(self) -> RepairWorkerHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join = tokio::spawn(async move { self.run_until_shutdown(shutdown_rx).await });
        RepairWorkerHandle {
            shutdown_tx: Some(shutdown_tx),
            join,
        }
    }

    async fn run_until_shutdown(
        self,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<RepairReplayReport, ReplicationError> {
        let mut interval = tokio::time::interval(self.config.interval);
        let mut total = RepairReplayReport::default();
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => return Ok(total),
                _ = interval.tick() => {
                    let report = self.run_once().await?;
                    total.attempted += report.attempted;
                    total.completed += report.completed;
                    total.retained += report.retained;
                }
            }
        }
    }
}

pub struct RepairWorkerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<Result<RepairReplayReport, ReplicationError>>,
}

impl RepairWorkerHandle {
    pub async fn shutdown(mut self) -> Result<RepairReplayReport, ReplicationError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.join.await.map_err(|error| {
            ReplicationError::Transport(format!("repair worker join error: {error}"))
        })?
    }
}

#[async_trait]
pub trait Replicator: Send + Sync {
    async fn start(&self) -> Result<(), ReplicationError>;
    async fn apply(&self, command: Command, timeout: Duration) -> Result<(), ReplicationError>;
    async fn apply_batch(
        &self,
        commands: Vec<Command>,
        timeout: Duration,
    ) -> Result<(), ReplicationError>;
    fn is_leader(&self) -> bool;
    fn leader_id(&self) -> Option<String>;
    fn health(&self) -> HealthStatus;
    async fn wait_for_leader(&self, timeout: Duration) -> Result<String, ReplicationError>;
    async fn shutdown(&self) -> Result<(), ReplicationError>;
    fn mode(&self) -> ReplicationMode;
    fn node_id(&self) -> String;
}

pub struct ReplicatedEngine {
    replicator: Arc<dyn Replicator>,
    apply_timeout: Duration,
}

impl ReplicatedEngine {
    pub fn new(replicator: Arc<dyn Replicator>, apply_timeout: Duration) -> Self {
        Self {
            replicator,
            apply_timeout,
        }
    }

    pub async fn apply_command(&self, command: Command) -> Result<(), ReplicationError> {
        self.replicator.apply(command, self.apply_timeout).await
    }

    pub async fn apply_batch(&self, commands: Vec<Command>) -> Result<(), ReplicationError> {
        self.replicator
            .apply_batch(commands, self.apply_timeout)
            .await
    }

    pub fn replicator(&self) -> &Arc<dyn Replicator> {
        &self.replicator
    }
}

pub struct StandaloneReplicator {
    config: ReplicationConfig,
    storage: Arc<dyn ReplicationStorage>,
    state: RwLock<(bool, u64)>,
    notify: Notify,
}

impl StandaloneReplicator {
    pub fn new(config: ReplicationConfig, storage: Arc<dyn ReplicationStorage>) -> Self {
        Self {
            config,
            storage,
            state: RwLock::new((false, 0)),
            notify: Notify::new(),
        }
    }
}

#[async_trait]
impl Replicator for StandaloneReplicator {
    async fn start(&self) -> Result<(), ReplicationError> {
        self.state.write().unwrap().0 = true;
        self.notify.notify_waiters();
        Ok(())
    }

    async fn apply(&self, command: Command, timeout: Duration) -> Result<(), ReplicationError> {
        self.apply_batch(vec![command], timeout).await
    }

    async fn apply_batch(
        &self,
        commands: Vec<Command>,
        timeout: Duration,
    ) -> Result<(), ReplicationError> {
        tokio::time::timeout(timeout, async {
            let running = self.state.read().unwrap().0;
            if !running {
                return Err(ReplicationError::Shutdown);
            }

            for command in &commands {
                self.storage.apply_command(command)?;
            }
            self.state.write().unwrap().1 += commands.len() as u64;
            self.notify.notify_waiters();
            Ok(())
        })
        .await
        .map_err(|_| ReplicationError::Timeout("standalone apply timed out".into()))?
    }

    fn is_leader(&self) -> bool {
        self.state.read().unwrap().0
    }

    fn leader_id(&self) -> Option<String> {
        Some(self.config.node_id.clone())
    }

    fn health(&self) -> HealthStatus {
        let (_, applied) = *self.state.read().unwrap();
        HealthStatus {
            mode: ReplicationMode::Standalone,
            node_id: self.config.node_id.clone(),
            role: NodeRole::Leader,
            leader_id: Some(self.config.node_id.clone()),
            term: 1,
            commit_index: applied,
            last_applied: applied,
            log_len: applied as usize,
            quorum_size: 1,
            peer_count: 0,
        }
    }

    async fn wait_for_leader(&self, _timeout: Duration) -> Result<String, ReplicationError> {
        Ok(self.config.node_id.clone())
    }

    async fn shutdown(&self) -> Result<(), ReplicationError> {
        self.state.write().unwrap().0 = false;
        self.notify.notify_waiters();
        Ok(())
    }

    fn mode(&self) -> ReplicationMode {
        ReplicationMode::Standalone
    }

    fn node_id(&self) -> String {
        self.config.node_id.clone()
    }
}

#[async_trait]
pub trait ReplicationRpc: Send + Sync {
    fn node_id(&self) -> String;
    async fn request_vote_rpc(
        &self,
        request: VoteRequest,
    ) -> Result<VoteResponse, ReplicationError>;
    async fn append_entries_rpc(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, ReplicationError>;
    async fn install_snapshot_rpc(&self, snapshot: Snapshot) -> Result<(), ReplicationError>;
}

#[async_trait]
pub trait ClusterTransport: Send + Sync {
    async fn request_vote(
        &self,
        target: &str,
        request: VoteRequest,
    ) -> Result<VoteResponse, ReplicationError>;
    async fn append_entries(
        &self,
        target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, ReplicationError>;
    async fn install_snapshot(
        &self,
        target: &str,
        snapshot: Snapshot,
    ) -> Result<(), ReplicationError>;
}

#[derive(Default)]
pub struct InMemoryTransport {
    peers: RwLock<HashMap<String, Arc<dyn ReplicationRpc>>>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, peer: Arc<dyn ReplicationRpc>) {
        self.peers.write().unwrap().insert(peer.node_id(), peer);
    }

    fn lookup(&self, target: &str) -> Result<Arc<dyn ReplicationRpc>, ReplicationError> {
        self.peers
            .read()
            .unwrap()
            .get(target)
            .cloned()
            .ok_or_else(|| ReplicationError::Transport(format!("unknown peer {target}")))
    }
}

#[async_trait]
impl ClusterTransport for InMemoryTransport {
    async fn request_vote(
        &self,
        target: &str,
        request: VoteRequest,
    ) -> Result<VoteResponse, ReplicationError> {
        self.lookup(target)?.request_vote_rpc(request).await
    }

    async fn append_entries(
        &self,
        target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, ReplicationError> {
        self.lookup(target)?.append_entries_rpc(request).await
    }

    async fn install_snapshot(
        &self,
        target: &str,
        snapshot: Snapshot,
    ) -> Result<(), ReplicationError> {
        self.lookup(target)?.install_snapshot_rpc(snapshot).await
    }
}

#[derive(Debug)]
struct ConsensusState {
    current_term: u64,
    role: NodeRole,
    leader_id: Option<String>,
    voted_for: Option<String>,
    commit_index: u64,
    last_applied: u64,
    running: bool,
    peer_next_index: HashMap<String, u64>,
    peer_match_index: HashMap<String, u64>,
}

impl ConsensusState {
    fn new(peers: &[String]) -> Self {
        Self {
            current_term: 0,
            role: NodeRole::Follower,
            leader_id: None,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            running: false,
            peer_next_index: peers.iter().cloned().map(|peer| (peer, 1)).collect(),
            peer_match_index: peers.iter().cloned().map(|peer| (peer, 0)).collect(),
        }
    }
}

pub struct QuorumReplicator {
    config: ReplicationConfig,
    storage: Arc<dyn ReplicationStorage>,
    transport: Arc<dyn ClusterTransport>,
    state: RwLock<ConsensusState>,
    log: RwLock<Vec<LogEntry>>,
    membership: RwLock<BTreeSet<String>>,
    notify: Notify,
}

impl QuorumReplicator {
    pub fn new(
        config: ReplicationConfig,
        storage: Arc<dyn ReplicationStorage>,
        transport: Arc<dyn ClusterTransport>,
    ) -> Self {
        let peers = config.peers.clone();
        Self {
            config,
            storage,
            transport,
            state: RwLock::new(ConsensusState::new(&peers)),
            log: RwLock::new(Vec::new()),
            membership: RwLock::new(peers.into_iter().collect()),
            notify: Notify::new(),
        }
    }

    pub async fn start_election(&self) -> Result<(), ReplicationError> {
        let request = {
            let mut state = self.state.write().unwrap();
            if !state.running {
                return Err(ReplicationError::Shutdown);
            }
            state.current_term += 1;
            state.role = NodeRole::Candidate;
            state.voted_for = Some(self.config.node_id.clone());
            state.leader_id = None;
            let (last_log_index, last_log_term) = self.last_log_position();
            VoteRequest {
                term: state.current_term,
                candidate_id: self.config.node_id.clone(),
                last_log_index,
                last_log_term,
            }
        };

        let peers: Vec<String> = self.membership.read().unwrap().iter().cloned().collect();
        let mut votes = 1;

        for peer in peers {
            if let Ok(response) = self.transport.request_vote(&peer, request.clone()).await {
                if response.term > request.term {
                    let mut state = self.state.write().unwrap();
                    state.current_term = response.term;
                    state.role = NodeRole::Follower;
                    state.voted_for = None;
                    return Err(ReplicationError::NotLeader(response.voter_id));
                }
                if response.vote_granted {
                    votes += 1;
                }
            }
        }

        if votes < self.quorum_size() {
            return Err(ReplicationError::NoQuorum {
                required: self.quorum_size(),
                received: votes,
            });
        }

        self.become_leader();
        self.broadcast_commit(self.commit_index()).await?;
        Ok(())
    }

    pub async fn create_snapshot(&self) -> Result<Snapshot, ReplicationError> {
        let state = self.state.read().unwrap();
        let last_included_index = state.last_applied;
        let last_included_term = if last_included_index == 0 {
            0
        } else {
            self.log.read().unwrap()[(last_included_index - 1) as usize].term
        };

        Ok(Snapshot {
            last_included_index,
            last_included_term,
            data: self.storage.write_snapshot()?,
        })
    }

    pub async fn install_snapshot(&self, snapshot: Snapshot) -> Result<(), ReplicationError> {
        self.storage.restore_snapshot(&snapshot.data)?;
        let mut state = self.state.write().unwrap();
        state.commit_index = snapshot.last_included_index;
        state.last_applied = snapshot.last_included_index;
        self.notify.notify_waiters();
        Ok(())
    }

    fn become_leader(&self) {
        let last_index = self.last_log_index() + 1;
        let peers: Vec<String> = self.membership.read().unwrap().iter().cloned().collect();
        let mut state = self.state.write().unwrap();
        state.role = NodeRole::Leader;
        state.leader_id = Some(self.config.node_id.clone());
        state.voted_for = Some(self.config.node_id.clone());
        for peer in peers {
            state.peer_next_index.insert(peer.clone(), last_index);
            state.peer_match_index.insert(peer, 0);
        }
        self.notify.notify_waiters();
    }

    fn last_log_index(&self) -> u64 {
        self.log
            .read()
            .unwrap()
            .last()
            .map(|entry| entry.index)
            .unwrap_or(0)
    }

    fn last_log_position(&self) -> (u64, u64) {
        self.log
            .read()
            .unwrap()
            .last()
            .map(|entry| (entry.index, entry.term))
            .unwrap_or((0, 0))
    }

    fn quorum_size(&self) -> usize {
        let total_nodes = self.membership.read().unwrap().len() + 1;
        (total_nodes / 2) + 1
    }

    fn commit_index(&self) -> u64 {
        self.state.read().unwrap().commit_index
    }

    fn leader_target(&self) -> String {
        self.leader_id()
            .unwrap_or_else(|| self.config.node_id.clone())
    }

    fn is_running(&self) -> bool {
        self.state.read().unwrap().running
    }

    async fn apply_inner(&self, commands: Vec<Command>) -> Result<(), ReplicationError> {
        if !self.is_running() {
            return Err(ReplicationError::Shutdown);
        }
        if !self.is_leader() {
            return Err(ReplicationError::NotLeader(self.leader_target()));
        }

        let term = self.state.read().unwrap().current_term;
        let last_index = {
            let mut log = self.log.write().unwrap();
            for command in commands {
                let index = log.len() as u64 + 1;
                log.push(LogEntry {
                    index,
                    term,
                    payload: LogPayload::Command(command),
                });
            }
            log.last().map(|entry| entry.index).unwrap_or(0)
        };

        let acks = self.replicate_to_quorum(last_index).await?;
        if acks < self.quorum_size() {
            return Err(ReplicationError::NoQuorum {
                required: self.quorum_size(),
                received: acks,
            });
        }

        {
            let mut state = self.state.write().unwrap();
            state.commit_index = last_index;
        }
        self.apply_committed_entries()?;
        self.broadcast_commit(last_index).await?;
        Ok(())
    }

    async fn replicate_to_quorum(&self, committed_through: u64) -> Result<usize, ReplicationError> {
        let peers: Vec<String> = self.membership.read().unwrap().iter().cloned().collect();
        let mut acks = 1;

        for peer in peers {
            if self.replicate_to_peer(&peer, committed_through).await? {
                acks += 1;
            }
        }

        Ok(acks)
    }

    async fn replicate_to_peer(
        &self,
        peer: &str,
        committed_through: u64,
    ) -> Result<bool, ReplicationError> {
        loop {
            let (next_index, current_term, current_commit) = {
                let state = self.state.read().unwrap();
                let next_index = *state.peer_next_index.get(peer).unwrap_or(&1);
                (next_index, state.current_term, state.commit_index)
            };

            let request = self.build_append_request(peer, next_index, current_term, current_commit);
            let response = self.transport.append_entries(peer, request).await?;

            if response.term > current_term {
                let mut state = self.state.write().unwrap();
                state.current_term = response.term;
                state.role = NodeRole::Follower;
                state.leader_id = Some(response.responder_id.clone());
                state.voted_for = None;
                self.notify.notify_waiters();
                return Err(ReplicationError::NotLeader(response.responder_id));
            }

            if response.success {
                let mut state = self.state.write().unwrap();
                state
                    .peer_match_index
                    .insert(peer.to_string(), response.match_index);
                state
                    .peer_next_index
                    .insert(peer.to_string(), response.match_index + 1);
                return Ok(response.match_index >= committed_through);
            }

            let mut state = self.state.write().unwrap();
            let next = state.peer_next_index.entry(peer.to_string()).or_insert(1);
            if *next <= 1 {
                return Ok(false);
            }
            *next -= 1;
        }
    }

    fn build_append_request(
        &self,
        peer: &str,
        next_index: u64,
        current_term: u64,
        leader_commit: u64,
    ) -> AppendEntriesRequest {
        let _ = peer;
        let log = self.log.read().unwrap();
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            log[(prev_log_index - 1) as usize].term
        };
        let entries = if next_index == 0 {
            log.clone()
        } else {
            log.iter()
                .skip(next_index.saturating_sub(1) as usize)
                .cloned()
                .collect()
        };

        AppendEntriesRequest {
            term: current_term,
            leader_id: self.config.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    async fn broadcast_commit(&self, leader_commit: u64) -> Result<(), ReplicationError> {
        let peers: Vec<String> = self.membership.read().unwrap().iter().cloned().collect();
        let term = self.state.read().unwrap().current_term;
        for peer in peers {
            let request = AppendEntriesRequest {
                term,
                leader_id: self.config.node_id.clone(),
                prev_log_index: self.last_log_index(),
                prev_log_term: self.last_log_position().1,
                entries: Vec::new(),
                leader_commit,
            };
            let _ = self.transport.append_entries(&peer, request).await;
        }
        Ok(())
    }

    fn apply_committed_entries(&self) -> Result<(), ReplicationError> {
        loop {
            let next_entry = {
                let state = self.state.read().unwrap();
                if state.last_applied >= state.commit_index {
                    None
                } else {
                    let next_index = state.last_applied + 1;
                    Some(next_index)
                }
            };

            let Some(next_index) = next_entry else {
                break;
            };

            let entry = self.log.read().unwrap()[(next_index - 1) as usize].clone();
            match entry.payload {
                LogPayload::Command(command) => self.storage.apply_command(&command)?,
                LogPayload::ConfigChange {
                    action, node_id, ..
                } => {
                    let mut membership = self.membership.write().unwrap();
                    match action {
                        ConfigAction::AddNode | ConfigAction::UpdateNode => {
                            if node_id != self.config.node_id {
                                membership.insert(node_id);
                            }
                        }
                        ConfigAction::RemoveNode => {
                            membership.remove(&node_id);
                        }
                    }
                }
                LogPayload::SnapshotMarker { .. } => {}
            }

            self.state.write().unwrap().last_applied = next_index;
            self.notify.notify_waiters();
        }

        Ok(())
    }

    fn is_up_to_date(&self, request: &VoteRequest) -> bool {
        let (last_index, last_term) = self.last_log_position();
        request.last_log_term > last_term
            || (request.last_log_term == last_term && request.last_log_index >= last_index)
    }

    async fn wait_for_leader_inner(
        &self,
        timeout_duration: Duration,
    ) -> Result<String, ReplicationError> {
        let deadline = Instant::now() + timeout_duration;

        loop {
            if let Some(leader_id) = self.leader_id() {
                return Ok(leader_id);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(ReplicationError::Timeout(
                    "leader election timed out".into(),
                ));
            }

            let remaining = deadline.saturating_duration_since(now);
            tokio::time::timeout(remaining, self.notify.notified())
                .await
                .map_err(|_| ReplicationError::Timeout("leader election timed out".into()))?;
        }
    }
}

#[async_trait]
impl ReplicationRpc for QuorumReplicator {
    fn node_id(&self) -> String {
        self.config.node_id.clone()
    }

    async fn request_vote_rpc(
        &self,
        request: VoteRequest,
    ) -> Result<VoteResponse, ReplicationError> {
        let mut state = self.state.write().unwrap();

        if request.term < state.current_term {
            return Ok(VoteResponse {
                term: state.current_term,
                vote_granted: false,
                voter_id: self.config.node_id.clone(),
            });
        }

        if request.term > state.current_term {
            state.current_term = request.term;
            state.role = NodeRole::Follower;
            state.voted_for = None;
            state.leader_id = None;
        }

        let can_vote = state
            .voted_for
            .as_ref()
            .map(|vote| vote == &request.candidate_id)
            .unwrap_or(true);
        let vote_granted = can_vote && self.is_up_to_date(&request);
        if vote_granted {
            state.voted_for = Some(request.candidate_id.clone());
        }

        Ok(VoteResponse {
            term: state.current_term,
            vote_granted,
            voter_id: self.config.node_id.clone(),
        })
    }

    async fn append_entries_rpc(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, ReplicationError> {
        {
            let mut state = self.state.write().unwrap();
            if request.term < state.current_term {
                return Ok(AppendEntriesResponse {
                    term: state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    responder_id: self.config.node_id.clone(),
                });
            }

            if request.term > state.current_term {
                state.current_term = request.term;
                state.voted_for = None;
            }

            state.role = NodeRole::Follower;
            state.leader_id = Some(request.leader_id.clone());
        }

        {
            let log = self.log.read().unwrap();
            if request.prev_log_index > 0 {
                let Some(entry) = log.get((request.prev_log_index - 1) as usize) else {
                    return Ok(AppendEntriesResponse {
                        term: self.state.read().unwrap().current_term,
                        success: false,
                        match_index: log.last().map(|entry| entry.index).unwrap_or(0),
                        responder_id: self.config.node_id.clone(),
                    });
                };
                if entry.term != request.prev_log_term {
                    return Ok(AppendEntriesResponse {
                        term: self.state.read().unwrap().current_term,
                        success: false,
                        match_index: request.prev_log_index.saturating_sub(1),
                        responder_id: self.config.node_id.clone(),
                    });
                }
            }
        }

        if !request.entries.is_empty() {
            let mut log = self.log.write().unwrap();
            for (next_slot, entry) in (request.prev_log_index as usize..).zip(request.entries) {
                if let Some(existing) = log.get(next_slot) {
                    if existing.term != entry.term || existing.payload != entry.payload {
                        log.truncate(next_slot);
                        log.push(entry);
                    }
                } else {
                    log.push(entry);
                }
            }
        }

        let last_index = self.last_log_index();
        {
            let mut state = self.state.write().unwrap();
            state.commit_index = state
                .commit_index
                .max(request.leader_commit.min(last_index));
        }
        self.apply_committed_entries()?;

        Ok(AppendEntriesResponse {
            term: self.state.read().unwrap().current_term,
            success: true,
            match_index: last_index,
            responder_id: self.config.node_id.clone(),
        })
    }

    async fn install_snapshot_rpc(&self, snapshot: Snapshot) -> Result<(), ReplicationError> {
        self.install_snapshot(snapshot).await
    }
}

#[async_trait]
impl Replicator for QuorumReplicator {
    async fn start(&self) -> Result<(), ReplicationError> {
        self.state.write().unwrap().running = true;
        self.notify.notify_waiters();
        Ok(())
    }

    async fn apply(&self, command: Command, timeout: Duration) -> Result<(), ReplicationError> {
        self.apply_batch(vec![command], timeout).await
    }

    async fn apply_batch(
        &self,
        commands: Vec<Command>,
        timeout: Duration,
    ) -> Result<(), ReplicationError> {
        tokio::time::timeout(timeout, self.apply_inner(commands))
            .await
            .map_err(|_| ReplicationError::Timeout("replication apply timed out".into()))?
    }

    fn is_leader(&self) -> bool {
        self.state.read().unwrap().role == NodeRole::Leader
    }

    fn leader_id(&self) -> Option<String> {
        let state = self.state.read().unwrap();
        match state.role {
            NodeRole::Leader => Some(self.config.node_id.clone()),
            _ => state.leader_id.clone(),
        }
    }

    fn health(&self) -> HealthStatus {
        let state = self.state.read().unwrap();
        HealthStatus {
            mode: ReplicationMode::Quorum,
            node_id: self.config.node_id.clone(),
            role: state.role,
            leader_id: self.leader_id(),
            term: state.current_term,
            commit_index: state.commit_index,
            last_applied: state.last_applied,
            log_len: self.log.read().unwrap().len(),
            quorum_size: self.quorum_size(),
            peer_count: self.membership.read().unwrap().len(),
        }
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Result<String, ReplicationError> {
        self.wait_for_leader_inner(timeout).await
    }

    async fn shutdown(&self) -> Result<(), ReplicationError> {
        self.state.write().unwrap().running = false;
        self.notify.notify_waiters();
        Ok(())
    }

    fn mode(&self) -> ReplicationMode {
        ReplicationMode::Quorum
    }

    fn node_id(&self) -> String {
        self.config.node_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_localization::{LanguageTag, Manager, StableLocalizedDiagnostic};

    #[test]
    fn replication_errors_expose_stable_localized_diagnostics() {
        let spanish = LanguageTag::parse("es-ES").unwrap().unwrap();
        let manager = Manager::new(std::slice::from_ref(&spanish));

        let not_leader = ReplicationError::NotLeader("node-1".into());
        assert_eq!(not_leader.diagnostic_id(), "replication.not_leader");
        assert_eq!(
            manager
                .render(
                    std::slice::from_ref(&spanish),
                    &not_leader.localized_message()
                )
                .unwrap()
                .text,
            "el nodo no es líder"
        );

        let transport = ReplicationError::Transport("peer reset".into());
        assert_eq!(
            transport.diagnostic_id(),
            "replication.rpc.remote_apply_failed"
        );
        assert_eq!(
            manager
                .render(&[spanish], &transport.localized_message())
                .unwrap()
                .text,
            "transport error: peer reset"
        );
    }

    fn cluster_config(node_id: &str, peers: &[&str]) -> ReplicationConfig {
        ReplicationConfig {
            mode: ReplicationMode::Quorum,
            node_id: node_id.into(),
            peers: peers.iter().map(|peer| (*peer).to_string()).collect(),
            heartbeat_interval: Duration::from_millis(50),
            election_timeout: Duration::from_millis(250),
        }
    }

    #[tokio::test]
    async fn standalone_replicator_applies_commands() {
        let storage = Arc::new(MemoryStorage::new());
        let replicator = Arc::new(StandaloneReplicator::new(
            ReplicationConfig::default(),
            storage.clone(),
        ));
        replicator.start().await.unwrap();

        replicator
            .apply(
                Command::Put {
                    key: b"hello".to_vec(),
                    value: b"world".to_vec(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(storage.get(b"hello"), Some(b"world".to_vec()));
        assert!(replicator.is_leader());
    }

    #[tokio::test]
    async fn quorum_replicator_elects_leader_and_replicates() {
        let transport = Arc::new(InMemoryTransport::new());

        let storage1 = Arc::new(MemoryStorage::new());
        let storage2 = Arc::new(MemoryStorage::new());
        let storage3 = Arc::new(MemoryStorage::new());

        let node1 = Arc::new(QuorumReplicator::new(
            cluster_config("node-1", &["node-2", "node-3"]),
            storage1.clone(),
            transport.clone(),
        ));
        let node2 = Arc::new(QuorumReplicator::new(
            cluster_config("node-2", &["node-1", "node-3"]),
            storage2.clone(),
            transport.clone(),
        ));
        let node3 = Arc::new(QuorumReplicator::new(
            cluster_config("node-3", &["node-1", "node-2"]),
            storage3.clone(),
            transport.clone(),
        ));

        transport.register(node1.clone());
        transport.register(node2.clone());
        transport.register(node3.clone());

        node1.start().await.unwrap();
        node2.start().await.unwrap();
        node3.start().await.unwrap();
        node1.start_election().await.unwrap();

        assert!(node1.is_leader());
        assert_eq!(
            node2.wait_for_leader(Duration::from_secs(1)).await.unwrap(),
            "node-1"
        );

        node1
            .apply(
                Command::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(storage1.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(storage2.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(storage3.get(b"k"), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn follower_rejects_write_requests() {
        let transport = Arc::new(InMemoryTransport::new());
        let follower = Arc::new(QuorumReplicator::new(
            cluster_config("node-2", &["node-1"]),
            Arc::new(MemoryStorage::new()),
            transport,
        ));
        follower.start().await.unwrap();

        let error = follower
            .apply(
                Command::Put {
                    key: b"blocked".to_vec(),
                    value: b"write".to_vec(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ReplicationError::NotLeader(_)));
    }

    #[tokio::test]
    async fn quorum_failure_returns_no_quorum() {
        let transport = Arc::new(InMemoryTransport::new());
        let leader = Arc::new(QuorumReplicator::new(
            cluster_config("node-1", &["node-2", "node-3", "node-4", "node-5"]),
            Arc::new(MemoryStorage::new()),
            transport.clone(),
        ));
        let follower = Arc::new(QuorumReplicator::new(
            cluster_config("node-2", &["node-1", "node-3", "node-4", "node-5"]),
            Arc::new(MemoryStorage::new()),
            transport.clone(),
        ));

        transport.register(leader.clone());
        transport.register(follower.clone());

        leader.start().await.unwrap();
        follower.start().await.unwrap();

        let error = leader.start_election().await.unwrap_err();
        assert!(matches!(error, ReplicationError::NoQuorum { .. }));
    }

    #[tokio::test]
    async fn snapshot_round_trip_restores_state() {
        let transport = Arc::new(InMemoryTransport::new());

        let storage1 = Arc::new(MemoryStorage::new());
        let storage2 = Arc::new(MemoryStorage::new());
        let restored_storage = Arc::new(MemoryStorage::new());
        let node1 = Arc::new(QuorumReplicator::new(
            cluster_config("node-1", &["node-2"]),
            storage1.clone(),
            transport.clone(),
        ));
        let node2 = Arc::new(QuorumReplicator::new(
            cluster_config("node-2", &["node-1"]),
            storage2.clone(),
            transport.clone(),
        ));

        transport.register(node1.clone());
        transport.register(node2.clone());

        node1.start().await.unwrap();
        node2.start().await.unwrap();
        node1.start_election().await.unwrap();
        node1
            .apply(
                Command::Put {
                    key: b"snap".to_vec(),
                    value: b"state".to_vec(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        let snapshot = node1.create_snapshot().await.unwrap();

        let restored = Arc::new(QuorumReplicator::new(
            cluster_config("node-3", &["node-1", "node-2"]),
            restored_storage.clone(),
            transport,
        ));
        restored.start().await.unwrap();
        restored.install_snapshot(snapshot).await.unwrap();

        assert_eq!(restored_storage.get(b"snap"), Some(b"state".to_vec()));
    }

    #[test]
    fn high_availability_planner_uses_topology_quorum() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("node-1", "node-1.mesh.local:9000")
                    .with_capability(NodeCapability::WriteLeader),
            )
            .unwrap();
        topology
            .register_peer(
                MeshPeer::new("node-2", "node-2.mesh.local:9000")
                    .with_capability(NodeCapability::WriteReplica),
            )
            .unwrap();
        topology
            .register_peer(
                MeshPeer::new("node-3", "node-3.mesh.local:9000")
                    .with_capability(NodeCapability::WriteReplica),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: PlacementKey::default_for_database("copper"),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let planner = HighAvailabilityWritePlanner::new(topology, DistributedWriteMode::RaftLog);
        let plan = planner
            .plan(&PlacementKey::default_for_database("copper"))
            .unwrap();
        assert_eq!(plan.leader.node_id, "node-1");
        assert_eq!(plan.required_acks, 2);
        assert_eq!(plan.replicas.len(), 2);
    }

    #[tokio::test]
    async fn cassandra_coordinator_writes_and_reads_at_quorum() {
        use copperdb_topology::{
            ConsistencyLevel, MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["node-1", "node-2", "node-3"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        let storage1 = Arc::new(MemoryStorage::new());
        let storage2 = Arc::new(MemoryStorage::new());
        let storage3 = Arc::new(MemoryStorage::new());
        transport.register("node-1", storage1.clone());
        transport.register("node-2", storage2.clone());
        transport.register("node-3", storage3.clone());
        let coordinator = CassandraCoordinator::new(topology, transport);

        let write = coordinator
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"distributed".to_vec(),
                    value: b"write".to_vec(),
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(write.plan.required_acks, 2);
        assert_eq!(write.acknowledged_by.len(), 3);
        assert!(write.failed_replicas.is_empty());
        assert_eq!(storage1.get(b"distributed"), Some(b"write".to_vec()));
        assert_eq!(storage2.get(b"distributed"), Some(b"write".to_vec()));
        assert_eq!(storage3.get(b"distributed"), Some(b"write".to_vec()));

        let read = coordinator
            .read(&placement, ConsistencyLevel::Quorum, b"distributed", None)
            .await
            .unwrap();
        assert_eq!(read.plan.required_responses, 2);
        assert!(read.failed_replicas.is_empty());
        assert_eq!(read.value, Some(b"write".to_vec()));
    }

    #[tokio::test]
    async fn cassandra_coordinator_reports_late_repair_candidates_after_quorum() {
        use copperdb_topology::{
            ConsistencyLevel, MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["node-1", "node-2", "node-3"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        let storage1 = Arc::new(MemoryStorage::new());
        let storage2 = Arc::new(MemoryStorage::new());
        transport.register("node-1", storage1.clone());
        transport.register("node-2", storage2.clone());
        let coordinator = CassandraCoordinator::new(topology, transport);

        let write = coordinator
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"handoff".to_vec(),
                    value: b"candidate".to_vec(),
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(write.acknowledged_by, vec!["node-1", "node-2"]);
        assert_eq!(write.failed_replicas, vec!["node-3"]);
    }

    #[tokio::test]
    async fn cassandra_coordinator_persists_hinted_handoff_after_quorum() {
        use copperdb_topology::{
            ConsistencyLevel, MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["node-1", "node-2", "node-3"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(MemoryStorage::new()));
        transport.register("node-2", Arc::new(MemoryStorage::new()));
        let dir = tempfile::tempdir().unwrap();
        let repair_path = dir.path().join("repair");
        let queue = Arc::new(DurableRepairQueue::open(&repair_path).unwrap());
        let coordinator =
            CassandraCoordinator::with_repair_queue(topology, transport, queue.clone());

        coordinator
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"handoff".to_vec(),
                    value: b"durable".to_vec(),
                },
                None,
            )
            .await
            .unwrap();

        let reopened = DurableRepairQueue::open(&repair_path).unwrap();
        let pending = reopened.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, RepairKind::HintedHandoff);
        assert_eq!(pending[0].target_node, "node-3");
        assert!(matches!(pending[0].command, Some(Command::Put { .. })));
    }

    #[tokio::test]
    async fn cassandra_coordinator_persists_read_repair_probe_after_quorum() {
        use copperdb_topology::{
            ConsistencyLevel, MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["node-1", "node-2", "node-3"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        let storage1 = Arc::new(MemoryStorage::new());
        let storage3 = Arc::new(MemoryStorage::new());
        storage1
            .apply_command(&Command::Put {
                key: b"repair-read".to_vec(),
                value: b"value".to_vec(),
            })
            .unwrap();
        storage3
            .apply_command(&Command::Put {
                key: b"repair-read".to_vec(),
                value: b"value".to_vec(),
            })
            .unwrap();
        transport.register("node-1", storage1);
        transport.register("node-3", storage3);
        let dir = tempfile::tempdir().unwrap();
        let repair_path = dir.path().join("repair");
        let queue = Arc::new(DurableRepairQueue::open(&repair_path).unwrap());
        let coordinator =
            CassandraCoordinator::with_repair_queue(topology, transport, queue.clone());

        let read = coordinator
            .read(&placement, ConsistencyLevel::Quorum, b"repair-read", None)
            .await
            .unwrap();
        assert_eq!(read.failed_replicas, vec!["node-2"]);
        assert_eq!(read.value, Some(b"value".to_vec()));

        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, RepairKind::ReadRepairProbe);
        assert_eq!(pending[0].target_node, "node-2");
        assert_eq!(pending[0].read_key, Some(b"repair-read".to_vec()));
    }

    #[test]
    fn durable_repair_queue_persists_and_deletes_records() {
        let dir = tempfile::tempdir().unwrap();
        let repair_path = dir.path().join("repair");
        let queue = DurableRepairQueue::open(&repair_path).unwrap();
        let record = RepairRecord::read_repair_probe(
            PlacementKey::default_for_database("copper"),
            "node-2".into(),
            b"key".to_vec(),
        );
        let record_id = record.id.clone();
        queue.enqueue(record).unwrap();

        let reopened = DurableRepairQueue::open(&repair_path).unwrap();
        let pending = reopened.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, RepairKind::ReadRepairProbe);
        assert_eq!(pending[0].read_key, Some(b"key".to_vec()));

        reopened.delete(&record_id).unwrap();
        assert!(reopened.pending().unwrap().is_empty());
    }

    #[tokio::test]
    async fn durable_repair_queue_replays_hinted_handoff_and_deletes_success() {
        let dir = tempfile::tempdir().unwrap();
        let repair_path = dir.path().join("repair");
        let queue = DurableRepairQueue::open(&repair_path).unwrap();
        queue
            .enqueue(RepairRecord::hinted_handoff(
                PlacementKey::default_for_database("copper"),
                "node-2".into(),
                Command::Put {
                    key: b"replay".to_vec(),
                    value: b"handoff".to_vec(),
                },
            ))
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        let storage = Arc::new(MemoryStorage::new());
        transport.register("node-2", storage.clone());

        let report = queue.replay_batch(transport, 10).await.unwrap();
        assert_eq!(
            report,
            RepairReplayReport {
                attempted: 1,
                completed: 1,
                retained: 0,
            }
        );
        assert_eq!(storage.get(b"replay"), Some(b"handoff".to_vec()));
        assert!(queue.pending().unwrap().is_empty());
    }

    #[tokio::test]
    async fn durable_repair_queue_retains_failed_replay_with_attempt_count() {
        let dir = tempfile::tempdir().unwrap();
        let repair_path = dir.path().join("repair");
        let queue = DurableRepairQueue::open(&repair_path).unwrap();
        queue
            .enqueue(RepairRecord::hinted_handoff(
                PlacementKey::default_for_database("copper"),
                "missing-node".into(),
                Command::Put {
                    key: b"retry".to_vec(),
                    value: b"later".to_vec(),
                },
            ))
            .unwrap();

        let report = queue
            .replay_batch(Arc::new(InMemoryReplicaTransport::new()), 10)
            .await
            .unwrap();
        assert_eq!(
            report,
            RepairReplayReport {
                attempted: 1,
                completed: 0,
                retained: 1,
            }
        );
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].attempts, 1);
    }

    #[tokio::test]
    async fn scheduled_repair_worker_replays_pending_repairs_in_background() {
        let dir = tempfile::tempdir().unwrap();
        let repair_path = dir.path().join("repair");
        let queue = Arc::new(DurableRepairQueue::open(&repair_path).unwrap());
        queue
            .enqueue(RepairRecord::hinted_handoff(
                PlacementKey::default_for_database("copper"),
                "node-2".into(),
                Command::Put {
                    key: b"scheduled-repair".to_vec(),
                    value: b"done".to_vec(),
                },
            ))
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        let storage = Arc::new(MemoryStorage::new());
        transport.register("node-2", storage.clone());
        let worker = ScheduledRepairWorker::new(
            queue.clone(),
            transport,
            RepairWorkerConfig {
                interval: Duration::from_millis(10),
                max_records_per_tick: 10,
            },
        );
        let handle = worker.spawn();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if queue.pending().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let report = handle.shutdown().await.unwrap();

        assert!(report.attempted >= 1);
        assert!(report.completed >= 1);
        assert_eq!(storage.get(b"scheduled-repair"), Some(b"done".to_vec()));
    }

    #[tokio::test]
    async fn storage_engine_adapter_persists_replica_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().join("replica");
        {
            let storage = StorageEngine::open(&storage_path).unwrap();
            let adapter = StorageEngineAdapter::new(storage);
            adapter
                .apply_command(&Command::Put {
                    key: b"durable".to_vec(),
                    value: b"replica".to_vec(),
                })
                .unwrap();
            assert_eq!(
                adapter.read_key(b"durable").unwrap(),
                Some(b"replica".to_vec())
            );
        }

        let reopened = StorageEngine::open(&storage_path).unwrap();
        let adapter = StorageEngineAdapter::new(reopened);
        assert_eq!(
            adapter.read_key(b"durable").unwrap(),
            Some(b"replica".to_vec())
        );
    }

    #[tokio::test]
    async fn storage_engine_adapter_exposes_graph_access_metadata() {
        let storage = StorageEngine::open_temporary().unwrap();
        storage
            .put_knowledge_policy_access_metadata(
                "memory:replica-1",
                &KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(123),
                    access_count: 4,
                },
            )
            .unwrap();

        let adapter = StorageEngineAdapter::new(storage);
        let metadata = adapter.graph_access_metadata("memory:replica-1").unwrap();

        assert_eq!(
            metadata,
            Some(KnowledgePolicyAccessMetadata {
                last_accessed_at_unix_ms: Some(123),
                access_count: 4,
            })
        );
    }

    #[tokio::test]
    async fn storage_engine_adapter_applies_access_metadata_command() {
        let storage = StorageEngine::open_temporary().unwrap();
        let adapter = StorageEngineAdapter::new(storage);

        adapter
            .apply_command(&Command::PutKnowledgePolicyAccessMetadata {
                entity_id: "memory:replica-2".into(),
                metadata: KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(456),
                    access_count: 7,
                },
            })
            .unwrap();

        let metadata = adapter.graph_access_metadata("memory:replica-2").unwrap();
        assert_eq!(
            metadata,
            Some(KnowledgePolicyAccessMetadata {
                last_accessed_at_unix_ms: Some(456),
                access_count: 7,
            })
        );
    }

    #[tokio::test]
    async fn cassandra_coordinator_fails_when_quorum_is_unavailable() {
        use copperdb_topology::{
            ConsistencyLevel, MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["node-1", "node-2", "node-3"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(MemoryStorage::new()));
        let coordinator = CassandraCoordinator::new(topology, transport);
        let error = coordinator
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                None,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ReplicationError::NoQuorum { .. }));
    }
}
