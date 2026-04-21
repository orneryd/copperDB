//! Temporal graph data for copperdb.
//!
//! Equivalent to Go's `pkg/temporal` in NornicDB.
//! Provides:
//! - Versioned relationships with timestamp ranges
//! - Time-travel queries ("what did the graph look like at time T?")
//! - Pattern detection over temporal sequences
//! - Integration with the decay system for memory-like forgetting

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TemporalError {
    #[error("invalid time range: start > end")]
    InvalidTimeRange,
    #[error("version not found for timestamp {0}")]
    VersionNotFound(u64),
}

/// A temporal version of a relationship.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalEdge {
    pub from_id: String,
    pub to_id: String,
    pub rel_type: String,
    pub properties: serde_json::Value,
    pub valid_from: u64,  // Unix seconds
    pub valid_until: Option<u64>,
    pub weight: f64,
}

impl TemporalEdge {
    /// Check if this edge is valid at the given Unix timestamp.
    pub fn is_valid_at(&self, ts: u64) -> bool {
        ts >= self.valid_from
            && self.valid_until.map_or(true, |end| ts < end)
    }

    /// Expire this edge at the current time.
    pub fn expire(&mut self) {
        self.valid_until = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs(),
        );
    }
}

/// A temporal session tracks a logical "snapshot" time for queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalSession {
    pub session_id: String,
    /// `None` = current time (live view); `Some(ts)` = point-in-time snapshot.
    pub as_of: Option<u64>,
}

impl TemporalSession {
    pub fn live(session_id: impl Into<String>) -> Self {
        Self { session_id: session_id.into(), as_of: None }
    }

    pub fn as_of(session_id: impl Into<String>, ts: u64) -> Self {
        Self { session_id: session_id.into(), as_of: Some(ts) }
    }

    pub fn effective_time(&self) -> u64 {
        self.as_of.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs()
        })
    }
}

/// Filter a list of temporal edges to those valid at a given time.
pub fn edges_at(edges: &[TemporalEdge], ts: u64) -> Vec<&TemporalEdge> {
    edges.iter().filter(|e| e.is_valid_at(ts)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_validity() {
        let edge = TemporalEdge {
            from_id: "a".into(),
            to_id: "b".into(),
            rel_type: "KNOWS".into(),
            properties: serde_json::json!({}),
            valid_from: 1000,
            valid_until: Some(2000),
            weight: 1.0,
        };
        assert!(edge.is_valid_at(1500));
        assert!(!edge.is_valid_at(500));
        assert!(!edge.is_valid_at(2001));
    }

    #[test]
    fn test_edges_at() {
        let edges = vec![
            TemporalEdge {
                from_id: "a".into(), to_id: "b".into(), rel_type: "KNOWS".into(),
                properties: serde_json::json!({}),
                valid_from: 0, valid_until: Some(100), weight: 1.0,
            },
            TemporalEdge {
                from_id: "b".into(), to_id: "c".into(), rel_type: "KNOWS".into(),
                properties: serde_json::json!({}),
                valid_from: 50, valid_until: None, weight: 1.0,
            },
        ];
        let at_75 = edges_at(&edges, 75);
        assert_eq!(at_75.len(), 2);
        let at_150 = edges_at(&edges, 150);
        assert_eq!(at_150.len(), 1);
    }
}
