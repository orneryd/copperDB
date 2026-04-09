//! Data retention policy enforcement for magnetDB.
//!
//! Equivalent to Go's `pkg/retention` in NornicDB.
//! Automatically expires nodes and relationships that exceed their TTL.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("storage error: {0}")]
    StorageError(String),
}

/// A retention policy for a node label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub label: String,
    /// Maximum age in seconds before a node is eligible for deletion.
    pub max_age_secs: u64,
    /// Whether to perform a DETACH DELETE (also remove relationships).
    pub cascade_delete: bool,
}

impl RetentionPolicy {
    /// Check if a node with the given creation timestamp is expired.
    pub fn is_expired(&self, created_at_secs: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(created_at_secs) > self.max_age_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expired() {
        let policy = RetentionPolicy {
            label: "Session".into(),
            max_age_secs: 3600,
            cascade_delete: true,
        };
        // Created 2 hours ago
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(policy.is_expired(now - 7201));
        assert!(!policy.is_expired(now - 1800));
    }
}
