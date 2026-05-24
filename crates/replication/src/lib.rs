//! Replication primitives for copperdb.
//!
//! This crate mirrors the Go package split used in NornicDB:
//! - a storage abstraction for replicated commands
//! - a `Replicator` interface with standalone and clustered implementations
//! - a `ReplicatedEngine` wrapper that routes writes through the replicator
//! - a transport abstraction with an in-memory implementation for tests

use async_trait::async_trait;
use copperdb_storage::{StorageEngine, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Notify;

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
    fn write_snapshot(&self) -> Result<Vec<u8>, ReplicationError>;
    fn restore_snapshot(&self, snapshot: &[u8]) -> Result<(), ReplicationError>;
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MemorySnapshot {
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
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
        }
        Ok(())
    }

    fn write_snapshot(&self) -> Result<Vec<u8>, ReplicationError> {
        let snapshot = self.state.read().unwrap().clone();
        serde_json::to_vec(&snapshot).map_err(|error| ReplicationError::Storage(error.to_string()))
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> Result<(), ReplicationError> {
        let restored: MemorySnapshot = serde_json::from_slice(snapshot)
            .map_err(|error| ReplicationError::Storage(error.to_string()))?;
        *self.state.write().unwrap() = restored;
        Ok(())
    }
}

pub struct StorageEngineAdapter {
    engine: Mutex<StorageEngine>,
}

impl StorageEngineAdapter {
    pub fn new(engine: StorageEngine) -> Self {
        Self {
            engine: Mutex::new(engine),
        }
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
}

#[async_trait]
impl ReplicationStorage for StorageEngineAdapter {
    fn apply_command(&self, command: &Command) -> Result<(), ReplicationError> {
        let engine = self.engine.lock().unwrap();
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
        }
    }

    fn write_snapshot(&self) -> Result<Vec<u8>, ReplicationError> {
        let engine = self.engine.lock().unwrap();
        let mut snapshot = BTreeMap::new();
        for entry in engine.scan_nodes_with_prefix("replication:") {
            let (key, value) = entry?;
            snapshot.insert(key.to_vec(), value.to_vec());
        }
        serde_json::to_vec(&snapshot).map_err(|error| ReplicationError::Storage(error.to_string()))
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> Result<(), ReplicationError> {
        let engine = self.engine.lock().unwrap();
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
            let mut next_slot = request.prev_log_index as usize;
            for entry in request.entries {
                if let Some(existing) = log.get(next_slot) {
                    if existing.term != entry.term || existing.payload != entry.payload {
                        log.truncate(next_slot);
                        log.push(entry);
                    }
                } else {
                    log.push(entry);
                }
                next_slot += 1;
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
}
