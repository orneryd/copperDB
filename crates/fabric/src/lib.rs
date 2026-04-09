//! Distributed fabric for magnetDB.
//!
//! Equivalent to Go's `pkg/fabric` in NornicDB.
//! Provides multi-datacenter routing, workload distribution, and
//! network topology awareness for the distributed cluster.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FabricError {
    #[error("no available node for database {0}")]
    NoNodeAvailable(String),
    #[error("routing error: {0}")]
    RoutingError(String),
}

/// A cluster node descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: u64,
    pub address: String,
    pub region: String,
    pub role: NodeRole,
    pub databases: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Secondary,
    ReadReplica,
}

/// Route a query to the appropriate cluster node.
#[derive(Default)]
pub struct Router {
    nodes: Vec<ClusterNode>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: ClusterNode) {
        self.nodes.push(node);
    }

    /// Find the primary node for a given database.
    pub fn primary_for(&self, database: &str) -> Option<&ClusterNode> {
        self.nodes.iter().find(|n| {
            n.role == NodeRole::Primary && n.databases.contains(&database.to_string())
        })
    }

    /// Find any readable node for a given database (prefer local region).
    pub fn readable_for(&self, database: &str, preferred_region: &str) -> Option<&ClusterNode> {
        // Prefer local region
        self.nodes.iter()
            .filter(|n| n.databases.contains(&database.to_string()))
            .min_by_key(|n| if n.region == preferred_region { 0 } else { 1 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_find_primary() {
        let mut router = Router::new();
        router.add_node(ClusterNode {
            id: 1,
            address: "localhost:7687".into(),
            region: "us-east".into(),
            role: NodeRole::Primary,
            databases: vec!["default".into()],
        });
        let node = router.primary_for("default");
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, 1);
    }
}
