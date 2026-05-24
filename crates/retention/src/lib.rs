//! Data retention policy enforcement for copperdb.
//!
//! Equivalent to Go's `pkg/retention` in NornicDB.
//! Automatically expires nodes and relationships that exceed their TTL.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("policy not found: {0}")]
    PolicyNotFound(String),
    #[error("policy already exists: {0}")]
    AlreadyExists(String),
    #[error("hold not found: {0}")]
    HoldNotFound(String),
    #[error("erasure request not found: {0}")]
    ErasureNotFound(String),
    #[error("active legal hold prevents erasure for subject: {0}")]
    ActiveLegalHold(String),
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

// ─── New full-featured types mirroring NornicDB retention package ─────────────

/// Opaque data-category string (e.g. "User", "AuditLog", "Financial", "PHI").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DataCategory(pub String);

impl DataCategory {
    pub const USER: &'static str = "User";
    pub const AUDIT: &'static str = "AuditLog";
    pub const FINANCIAL: &'static str = "Financial";
    pub const PHI: &'static str = "PHI";
    pub const ANALYTICS: &'static str = "Analytics";
}

/// A named retention policy with an ID, mirroring NornicDB's `retention.Policy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub id: String,
    pub label: String,
    pub max_age_seconds: u64,
    pub enabled: bool,
    pub cascade_delete: bool,
    pub description: Option<String>,
    pub data_category: Option<String>,
}

impl Policy {
    pub fn new(label: impl Into<String>, max_age_seconds: u64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            label: label.into(),
            max_age_seconds,
            enabled: true,
            cascade_delete: false,
            description: None,
            data_category: None,
        }
    }
}

/// A legal hold preventing data from being erased.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegalHold {
    pub id: String,
    pub subject_id: String,
    pub reason: String,
    pub placed_at: std::time::SystemTime,
    pub released_at: Option<std::time::SystemTime>,
    pub active: bool,
}

/// Status of a GDPR/CCPA erasure request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErasureStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

/// An erasure (right-to-be-forgotten) request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureRequest {
    pub id: String,
    pub subject_id: String,
    pub subject_email: Option<String>,
    pub status: ErasureStatus,
    pub created_at: std::time::SystemTime,
    pub processed_at: Option<std::time::SystemTime>,
}

/// Configuration for a retention sweep run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionSweepConfig {
    /// Maximum number of nodes to inspect in a single sweep.
    pub batch_size: usize,
    /// Whether to perform a dry-run (no actual deletes).
    pub dry_run: bool,
}

impl Default for RetentionSweepConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            dry_run: false,
        }
    }
}

/// Full-featured retention manager, mirroring NornicDB's `retention.Manager`.
#[derive(Default)]
pub struct Manager {
    policies: HashMap<String, Policy>,
    holds: HashMap<String, LegalHold>,
    erasures: HashMap<String, ErasureRequest>,
}

impl Manager {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Policy CRUD ──────────────────────────────────────────────────────────

    pub fn add_policy(&mut self, policy: Policy) -> Result<(), RetentionError> {
        if self.policies.contains_key(&policy.id) {
            return Err(RetentionError::AlreadyExists(policy.id.clone()));
        }
        self.policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    pub fn get_policy(&self, id: &str) -> Option<&Policy> {
        self.policies.get(id)
    }

    pub fn list_policies(&self) -> Vec<&Policy> {
        self.policies.values().collect()
    }

    pub fn update_policy(&mut self, policy: Policy) -> Result<(), RetentionError> {
        if !self.policies.contains_key(&policy.id) {
            return Err(RetentionError::PolicyNotFound(policy.id.clone()));
        }
        self.policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    pub fn delete_policy(&mut self, id: &str) -> Result<(), RetentionError> {
        self.policies
            .remove(id)
            .ok_or_else(|| RetentionError::PolicyNotFound(id.to_string()))?;
        Ok(())
    }

    // ── Legal holds ──────────────────────────────────────────────────────────

    pub fn place_legal_hold(
        &mut self,
        subject_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> LegalHold {
        let hold = LegalHold {
            id: Uuid::new_v4().to_string(),
            subject_id: subject_id.into(),
            reason: reason.into(),
            placed_at: std::time::SystemTime::now(),
            released_at: None,
            active: true,
        };
        self.holds.insert(hold.id.clone(), hold.clone());
        hold
    }

    pub fn release_legal_hold(&mut self, id: &str) -> Result<(), RetentionError> {
        let hold = self
            .holds
            .get_mut(id)
            .ok_or_else(|| RetentionError::HoldNotFound(id.to_string()))?;
        hold.active = false;
        hold.released_at = Some(std::time::SystemTime::now());
        Ok(())
    }

    pub fn list_legal_holds(&self) -> Vec<&LegalHold> {
        self.holds.values().collect()
    }

    pub fn has_active_hold(&self, subject_id: &str) -> bool {
        self.holds
            .values()
            .any(|h| h.active && h.subject_id == subject_id)
    }

    // ── Erasure requests ─────────────────────────────────────────────────────

    pub fn create_erasure_request(
        &mut self,
        subject_id: impl Into<String>,
        subject_email: Option<String>,
    ) -> Result<ErasureRequest, RetentionError> {
        let sid: String = subject_id.into();
        if self.has_active_hold(&sid) {
            return Err(RetentionError::ActiveLegalHold(sid));
        }
        let req = ErasureRequest {
            id: Uuid::new_v4().to_string(),
            subject_id: sid,
            subject_email,
            status: ErasureStatus::Pending,
            created_at: std::time::SystemTime::now(),
            processed_at: None,
        };
        self.erasures.insert(req.id.clone(), req.clone());
        Ok(req)
    }

    pub fn get_erasure_request(&self, id: &str) -> Option<&ErasureRequest> {
        self.erasures.get(id)
    }

    pub fn list_erasure_requests(&self) -> Vec<&ErasureRequest> {
        self.erasures.values().collect()
    }

    /// Mark an erasure request as processed.
    pub fn process_erasure(&mut self, id: &str) -> Result<(), RetentionError> {
        let req = self
            .erasures
            .get_mut(id)
            .ok_or_else(|| RetentionError::ErasureNotFound(id.to_string()))?;
        req.status = ErasureStatus::Completed;
        req.processed_at = Some(std::time::SystemTime::now());
        Ok(())
    }
}

/// Returns a vec of 5 sensible default policies (User 2yr, Audit 7yr,
/// Financial 7yr, PHI 6yr, Analytics 90d), mirroring NornicDB's defaults.
pub fn default_policies() -> Vec<Policy> {
    vec![
        Policy {
            id: Uuid::new_v4().to_string(),
            label: "User".into(),
            max_age_seconds: 2 * 365 * 86_400,
            enabled: true,
            cascade_delete: true,
            description: Some("User account data – 2-year retention".into()),
            data_category: Some(DataCategory::USER.into()),
        },
        Policy {
            id: Uuid::new_v4().to_string(),
            label: "AuditLog".into(),
            max_age_seconds: 7 * 365 * 86_400,
            enabled: true,
            cascade_delete: false,
            description: Some("Audit log entries – 7-year retention".into()),
            data_category: Some(DataCategory::AUDIT.into()),
        },
        Policy {
            id: Uuid::new_v4().to_string(),
            label: "Financial".into(),
            max_age_seconds: 7 * 365 * 86_400,
            enabled: true,
            cascade_delete: false,
            description: Some("Financial records – 7-year retention".into()),
            data_category: Some(DataCategory::FINANCIAL.into()),
        },
        Policy {
            id: Uuid::new_v4().to_string(),
            label: "PHI".into(),
            max_age_seconds: 6 * 365 * 86_400,
            enabled: true,
            cascade_delete: true,
            description: Some("Protected health information – 6-year retention".into()),
            data_category: Some(DataCategory::PHI.into()),
        },
        Policy {
            id: Uuid::new_v4().to_string(),
            label: "Analytics".into(),
            max_age_seconds: 90 * 86_400,
            enabled: true,
            cascade_delete: false,
            description: Some("Analytics events – 90-day retention".into()),
            data_category: Some(DataCategory::ANALYTICS.into()),
        },
    ]
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
