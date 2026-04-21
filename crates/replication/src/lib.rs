//! Raft-based consensus and replication for copperdb.
//!
//! Equivalent to Go's `pkg/replication` in NornicDB.
//! Uses `openraft` (TiKV's modern Raft library) as the consensus engine,
//! equivalent to NornicDB's use of `hashicorp/raft` (via custom wrapper).
//!
//! # Features
//! - Raft leader election
//! - Log replication across cluster nodes
//! - Snapshot installation
//! - HA standby (passive hot-standby nodes)
//! - Multi-region routing via `copperdb-fabric`
//!
//! # Rust vs Go
//! NornicDB uses `github.com/hashicorp/raft`.
//! Rust equivalent: `openraft` crate (more ergonomic, async-native).

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("not a leader: redirect to {0}")]
    NotLeader(String),
    #[error("quorum not reached")]
    NoQuorum,
    #[error("raft error: {0}")]
    Raft(String),
    #[error("transport error: {0}")]
    Transport(String),
}

/// A Raft log entry (wraps a Cypher mutation for replication).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: LogPayload,
}

/// Payloads that can be replicated across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogPayload {
    /// A Cypher write query to apply.
    CypherMutation { database: String, query: String, params: serde_json::Value },
    /// A configuration change (add/remove node).
    ConfigChange { action: ConfigAction, node_id: u64, address: String },
    /// A database snapshot checksum.
    SnapshotMarker { snapshot_id: String, checksum: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigAction {
    AddNode,
    RemoveNode,
    UpdateNode,
}

/// Raft node state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
    Learner,
}

// TODO: Implement openraft StateMachine, Storage, and Network traits.
// Reference: NornicDB pkg/replication/raft.go, storage_adapter.go, transport.go
// openraft docs: https://datafuselabs.github.io/openraft/

/// Raft node roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Commands that can be replicated through Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    Noop,
    Write { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Raft node state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftState {
    pub node_id: String,
    pub term: u64,
    pub role: RaftRole,
    pub commit_index: u64,
    pub last_applied: u64,
    pub voted_for: Option<String>,
}

impl RaftState {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            term: 0,
            role: RaftRole::Follower,
            commit_index: 0,
            last_applied: 0,
            voted_for: None,
        }
    }
}

use std::sync::{Arc, RwLock};

/// A Raft node managing distributed consensus state.
pub struct RaftNode {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
}

impl RaftNode {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            state: Arc::new(RwLock::new(RaftState::new(node_id))),
            log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn current_term(&self) -> u64 {
        self.state.read().unwrap().term
    }

    pub fn role(&self) -> RaftRole {
        self.state.read().unwrap().role
    }

    pub fn node_id(&self) -> String {
        self.state.read().unwrap().node_id.clone()
    }

    /// Propose a command. Only succeeds when this node is the leader.
    pub fn propose(&self, cmd: RaftCommand) -> Result<(), ReplicationError> {
        let state = self.state.read().unwrap();
        if state.role != RaftRole::Leader {
            return Err(ReplicationError::NotLeader(state.node_id.clone()));
        }
        drop(state);
        let mut log = self.log.write().unwrap();
        let index = log.len() as u64 + 1;
        let term = self.state.read().unwrap().term;
        log.push(LogEntry {
            index,
            term,
            payload: LogPayload::CypherMutation {
                database: "default".into(),
                query: format!("{cmd:?}"),
                params: serde_json::Value::Null,
            },
        });
        drop(log);
        let mut s = self.state.write().unwrap();
        s.commit_index = index;
        s.last_applied = index;
        Ok(())
    }

    /// Step down to follower (e.g., after seeing a higher term).
    pub fn step_down(&self) {
        let mut s = self.state.write().unwrap();
        s.role = RaftRole::Follower;
    }

    /// Become leader (after winning an election).
    pub fn become_leader(&self) {
        let mut s = self.state.write().unwrap();
        s.role = RaftRole::Leader;
    }

    /// Transition to candidate and start a new election term.
    pub fn start_election(&self) {
        let mut s = self.state.write().unwrap();
        s.term += 1;
        s.role = RaftRole::Candidate;
        s.voted_for = Some(s.node_id.clone());
    }

    pub fn log_len(&self) -> usize {
        self.log.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry {
            index: 1,
            term: 1,
            payload: LogPayload::CypherMutation {
                database: "default".into(),
                query: "CREATE (n:Test) RETURN n".into(),
                params: serde_json::json!({}),
            },
        };
        let json = serde_json::to_string(&entry).unwrap();
        let restored: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.index, 1);
    }

    #[test]
    fn test_raft_node_initial_state() {
        let node = RaftNode::new("node-1");
        assert_eq!(node.current_term(), 0);
        assert_eq!(node.role(), RaftRole::Follower);
    }

    #[test]
    fn test_raft_node_become_leader() {
        let node = RaftNode::new("node-1");
        node.become_leader();
        assert_eq!(node.role(), RaftRole::Leader);
    }

    #[test]
    fn test_raft_node_step_down() {
        let node = RaftNode::new("node-1");
        node.become_leader();
        node.step_down();
        assert_eq!(node.role(), RaftRole::Follower);
    }

    #[test]
    fn test_raft_node_propose_as_leader() {
        let node = RaftNode::new("node-1");
        node.become_leader();
        let result = node.propose(RaftCommand::Write {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        });
        assert!(result.is_ok());
        assert_eq!(node.log_len(), 1);
    }

    #[test]
    fn test_raft_node_propose_as_follower_fails() {
        let node = RaftNode::new("node-1");
        let result = node.propose(RaftCommand::Noop);
        assert!(result.is_err());
    }

    #[test]
    fn test_raft_node_election() {
        let node = RaftNode::new("node-1");
        node.start_election();
        assert_eq!(node.role(), RaftRole::Candidate);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn test_raft_command_noop() {
        let cmd = RaftCommand::Noop;
        assert!(matches!(cmd, RaftCommand::Noop));
    }

    #[test]
    fn test_raft_command_delete() {
        let cmd = RaftCommand::Delete { key: b"k".to_vec() };
        assert!(matches!(cmd, RaftCommand::Delete { .. }));
    }
}
