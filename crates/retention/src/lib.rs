//! Data retention policy enforcement for magnetDB.
//!
//! Equivalent to Go's `pkg/retention` in NornicDB.
//! Automatically expires nodes and relationships that exceed their TTL.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("policy not found: {0}")]
    PolicyNotFound(String),
}

/// Units for expressing retention durations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetentionUnit {
    Days,
    Hours,
    Minutes,
}

impl RetentionUnit {
    /// Convert a count in this unit to seconds.
    pub fn to_seconds(self, count: u64) -> u64 {
        match self {
            RetentionUnit::Days => count * 86_400,
            RetentionUnit::Hours => count * 3_600,
            RetentionUnit::Minutes => count * 60,
        }
    }
}

/// A retention policy for a node label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub label: String,
    /// Maximum age in seconds before a node is eligible for deletion.
    pub max_age_seconds: u64,
    /// Whether this policy is currently active.
    pub enabled: bool,
    /// Whether to perform a DETACH DELETE (also remove relationships).
    pub cascade_delete: bool,
}

impl RetentionPolicy {
    pub fn new(label: impl Into<String>, max_age_seconds: u64) -> Self {
        Self {
            label: label.into(),
            max_age_seconds,
            enabled: true,
            cascade_delete: false,
        }
    }

    pub fn with_cascade(mut self) -> Self {
        self.cascade_delete = true;
        self
    }

    /// Check if a node with the given creation timestamp is expired.
    pub fn is_expired(&self, created_at_secs: u64) -> bool {
        if !self.enabled {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(created_at_secs) > self.max_age_seconds
    }
}

/// Manages a set of retention policies keyed by node label.
#[derive(Default)]
pub struct RetentionManager {
    policies: HashMap<String, RetentionPolicy>,
}

impl RetentionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_policy(&mut self, policy: RetentionPolicy) {
        self.policies.insert(policy.label.clone(), policy);
    }

    pub fn get_policy(&self, label: &str) -> Option<&RetentionPolicy> {
        self.policies.get(label)
    }

    pub fn remove_policy(&mut self, label: &str) {
        self.policies.remove(label);
    }

    /// Check whether a node of the given label created at `created_at` is expired.
    /// Returns `false` (not expired) if no policy exists for the label.
    pub fn is_expired(&self, label: &str, created_at: std::time::SystemTime) -> bool {
        let Some(policy) = self.policies.get(label) else {
            return false;
        };
        let created_secs = created_at
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        policy.is_expired(created_secs)
    }

    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }

    pub fn labels(&self) -> Vec<&str> {
        self.policies.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expired() {
        let policy = RetentionPolicy {
            label: "Session".into(),
            max_age_seconds: 3600,
            enabled: true,
            cascade_delete: true,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(policy.is_expired(now - 7201));
        assert!(!policy.is_expired(now - 1800));
    }

    #[test]
    fn test_disabled_policy_never_expires() {
        let mut policy = RetentionPolicy::new("Session", 3600);
        policy.enabled = false;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(!policy.is_expired(now - 7200));
    }

    #[test]
    fn test_retention_unit_conversions() {
        assert_eq!(RetentionUnit::Days.to_seconds(1), 86_400);
        assert_eq!(RetentionUnit::Hours.to_seconds(2), 7_200);
        assert_eq!(RetentionUnit::Minutes.to_seconds(5), 300);
    }

    #[test]
    fn test_retention_manager_add_get() {
        let mut mgr = RetentionManager::new();
        mgr.add_policy(RetentionPolicy::new("Event", 86400));
        assert!(mgr.get_policy("Event").is_some());
        assert_eq!(mgr.policy_count(), 1);
    }

    #[test]
    fn test_retention_manager_remove() {
        let mut mgr = RetentionManager::new();
        mgr.add_policy(RetentionPolicy::new("Event", 86400));
        mgr.remove_policy("Event");
        assert!(mgr.get_policy("Event").is_none());
        assert_eq!(mgr.policy_count(), 0);
    }

    #[test]
    fn test_retention_manager_is_expired() {
        let mut mgr = RetentionManager::new();
        mgr.add_policy(RetentionPolicy::new("Log", 60)); // 1-minute TTL
        // Created 2 minutes ago
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(120);
        assert!(mgr.is_expired("Log", old));
        // Created 10 seconds ago
        let fresh = std::time::SystemTime::now() - std::time::Duration::from_secs(10);
        assert!(!mgr.is_expired("Log", fresh));
    }

    #[test]
    fn test_retention_manager_unknown_label() {
        let mgr = RetentionManager::new();
        let old = std::time::SystemTime::UNIX_EPOCH;
        assert!(!mgr.is_expired("Unknown", old));
    }

    #[test]
    fn test_policy_with_cascade() {
        let p = RetentionPolicy::new("Node", 3600).with_cascade();
        assert!(p.cascade_delete);
    }
}
