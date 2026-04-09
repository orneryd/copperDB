//! Raft-based consensus and replication for magnetDB.
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
//! - Multi-region routing via `magnetdb-fabric`
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
}
