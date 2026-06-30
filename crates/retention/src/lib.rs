//! Data retention policy enforcement for copperdb.
//!
//! Equivalent to Go's `pkg/retention` in NornicDB.
//! Automatically expires nodes and relationships that exceed their TTL.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use copperdb_storage::{NodeRecord, StorageEngine};

const POLICY_LABEL: &str = "RetentionPolicy";
const HOLD_LABEL: &str = "RetentionLegalHold";
const ERASURE_LABEL: &str = "RetentionErasureRequest";
const PAYLOAD_PROPERTY: &str = "payload";

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
    #[error("serialization error: {0}")]
    SerializationError(String),
}

impl From<copperdb_storage::StorageError> for RetentionError {
    fn from(error: copperdb_storage::StorageError) -> Self {
        RetentionError::StorageError(error.to_string())
    }
}

impl From<serde_json::Error> for RetentionError {
    fn from(error: serde_json::Error) -> Self {
        RetentionError::SerializationError(error.to_string())
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RetentionSweepReport {
    pub inspected: usize,
    pub expired: usize,
    pub deleted: usize,
    pub held: usize,
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
    storage_path: Option<PathBuf>,
    /// Shared storage engine — when set, avoids opening a new storage instance.
    storage: Option<Arc<StorageEngine>>,
}

impl Manager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, RetentionError> {
        let storage_path = path.as_ref().to_path_buf();
        let storage = StorageEngine::open(&storage_path)?;
        let mut manager = Self {
            storage_path: Some(storage_path),
            ..Self::default()
        };
        manager.load_from_storage(&storage)?;
        // Don't store the storage Arc — open() is for standalone use
        // (e.g. tests). Use ensure_loaded() for shared-storage mode.
        Ok(manager)
    }

    /// Lazy-load retention data from a shared storage engine (avoids opening
    /// a new storage instance). Idempotent — subsequent calls are no-ops if
    /// data was already loaded.
    pub fn ensure_loaded(&mut self, storage: Arc<StorageEngine>) -> Result<(), RetentionError> {
        // Already loaded — skip.
        if !self.policies.is_empty() || !self.holds.is_empty() || !self.erasures.is_empty() {
            return Ok(());
        }
        self.load_from_storage(&storage)?;
        self.storage = Some(storage);
        Ok(())
    }

    fn load_from_storage(&mut self, storage: &StorageEngine) -> Result<(), RetentionError> {
        for node in storage.get_nodes_by_label(POLICY_LABEL)? {
            let policy: Policy = payload_from_node(&node)?;
            self.policies.insert(policy.id.clone(), policy);
        }
        for node in storage.get_nodes_by_label(HOLD_LABEL)? {
            let hold: LegalHold = payload_from_node(&node)?;
            self.holds.insert(hold.id.clone(), hold);
        }
        for node in storage.get_nodes_by_label(ERASURE_LABEL)? {
            let erasure: ErasureRequest = payload_from_node(&node)?;
            self.erasures.insert(erasure.id.clone(), erasure);
        }
        Ok(())
    }

    // ── Policy CRUD ──────────────────────────────────────────────────────────

    pub fn add_policy(&mut self, policy: Policy) -> Result<(), RetentionError> {
        if self.policies.contains_key(&policy.id) {
            return Err(RetentionError::AlreadyExists(policy.id.clone()));
        }
        self.persist_record(POLICY_LABEL, &policy.id, &policy)?;
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
        self.persist_record(POLICY_LABEL, &policy.id, &policy)?;
        self.policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    pub fn delete_policy(&mut self, id: &str) -> Result<(), RetentionError> {
        self.policies
            .remove(id)
            .ok_or_else(|| RetentionError::PolicyNotFound(id.to_string()))?;
        self.delete_record(POLICY_LABEL, id)?;
        Ok(())
    }

    // ── Legal holds ──────────────────────────────────────────────────────────

    pub fn place_legal_hold(
        &mut self,
        subject_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<LegalHold, RetentionError> {
        let hold = LegalHold {
            id: Uuid::new_v4().to_string(),
            subject_id: subject_id.into(),
            reason: reason.into(),
            placed_at: std::time::SystemTime::now(),
            released_at: None,
            active: true,
        };
        self.persist_record(HOLD_LABEL, &hold.id, &hold)?;
        self.holds.insert(hold.id.clone(), hold.clone());
        Ok(hold)
    }

    pub fn release_legal_hold(&mut self, id: &str) -> Result<(), RetentionError> {
        let hold = self
            .holds
            .get_mut(id)
            .ok_or_else(|| RetentionError::HoldNotFound(id.to_string()))?;
        hold.active = false;
        hold.released_at = Some(std::time::SystemTime::now());
        let hold = hold.clone();
        self.persist_record(HOLD_LABEL, id, &hold)?;
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
        self.persist_record(ERASURE_LABEL, &req.id, &req)?;
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
        let req = req.clone();
        self.persist_record(ERASURE_LABEL, id, &req)?;
        Ok(())
    }

    pub fn sweep(
        &self,
        config: RetentionSweepConfig,
    ) -> Result<RetentionSweepReport, RetentionError> {
        // Prefer shared storage, fall back to opening from path.
        let storage: Arc<StorageEngine>;
        let owned_storage;
        if let Some(shared) = &self.storage {
            storage = Arc::clone(shared);
        } else if let Some(path) = &self.storage_path {
            owned_storage = StorageEngine::open(path)?;
            storage = Arc::new(owned_storage);
        } else {
            return Ok(RetentionSweepReport {
                dry_run: config.dry_run,
                ..Default::default()
            });
        };
        let mut report = RetentionSweepReport {
            dry_run: config.dry_run,
            ..Default::default()
        };

        for policy in self.policies.values().filter(|policy| policy.enabled) {
            for node in storage.get_nodes_by_label(&policy.label)? {
                if report.inspected >= config.batch_size {
                    return Ok(report);
                }
                report.inspected += 1;
                let created_at_secs = (node.created_at_unix_ms.max(0) as u64) / 1000;
                if !policy_is_expired(policy, created_at_secs) {
                    continue;
                }
                report.expired += 1;
                let subject = node_subject_id(&node);
                if self.has_active_hold(&subject) {
                    report.held += 1;
                    continue;
                }
                if !config.dry_run {
                    storage.delete_node_record(&node.id)?;
                    report.deleted += 1;
                }
            }
        }
        Ok(report)
    }

    fn persist_record<T: Serialize>(
        &self,
        label: &str,
        id: &str,
        value: &T,
    ) -> Result<(), RetentionError> {
        if let Some(storage) = &self.storage {
            let mut properties = BTreeMap::new();
            properties.insert("id".into(), serde_json::Value::String(id.into()));
            properties.insert(PAYLOAD_PROPERTY.into(), serde_json::to_value(value)?);
            storage.put_node_record(&NodeRecord {
                id: retention_node_id(label, id),
                labels: vec![label.into()],
                properties,
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now_unix_ms(),
                updated_at_unix_ms: now_unix_ms(),
            })?;
            return Ok(());
        }
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let storage = StorageEngine::open(path)?;
        let mut properties = BTreeMap::new();
        properties.insert("id".into(), serde_json::Value::String(id.into()));
        properties.insert(PAYLOAD_PROPERTY.into(), serde_json::to_value(value)?);
        storage.put_node_record(&NodeRecord {
            id: retention_node_id(label, id),
            labels: vec![label.into()],
            properties,
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: now_unix_ms(),
            updated_at_unix_ms: now_unix_ms(),
        })?;
        Ok(())
    }

    fn delete_record(&self, label: &str, id: &str) -> Result<(), RetentionError> {
        if let Some(storage) = &self.storage {
            storage.delete_node_record(&retention_node_id(label, id))?;
            return Ok(());
        }
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let storage = StorageEngine::open(path)?;
        storage.delete_node_record(&retention_node_id(label, id))?;
        Ok(())
    }
}

fn retention_node_id(label: &str, id: &str) -> String {
    format!("retention:{label}:{id}")
}

fn payload_from_node<T: for<'de> Deserialize<'de>>(node: &NodeRecord) -> Result<T, RetentionError> {
    let payload = node
        .properties
        .get(PAYLOAD_PROPERTY)
        .cloned()
        .ok_or_else(|| RetentionError::SerializationError("missing payload".into()))?;
    Ok(serde_json::from_value(payload)?)
}

fn policy_is_expired(policy: &Policy, created_at_secs: u64) -> bool {
    if !policy.enabled {
        return false;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now.saturating_sub(created_at_secs) > policy.max_age_seconds
}

fn node_subject_id(node: &NodeRecord) -> String {
    node.properties
        .get("subject_id")
        .or_else(|| node.properties.get("user_id"))
        .or_else(|| node.properties.get("id"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| node.id.clone())
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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

    #[test]
    fn manager_persists_policies_holds_and_erasures() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_path_buf();

        let erasure_id;
        let hold_id;
        {
            let mut manager = Manager::open(&storage_path).unwrap();
            manager
                .add_policy(Policy {
                    id: "audit-policy".into(),
                    label: "AuditLog".into(),
                    max_age_seconds: 7 * 365 * 86_400,
                    enabled: true,
                    cascade_delete: false,
                    description: Some("audit retention".into()),
                    data_category: Some(DataCategory::AUDIT.into()),
                })
                .unwrap();
            let hold = manager
                .place_legal_hold("subject-1", "active investigation")
                .unwrap();
            hold_id = hold.id.clone();
            manager.release_legal_hold(&hold_id).unwrap();
            let erasure = manager
                .create_erasure_request("subject-1", Some("subject@example.com".into()))
                .unwrap();
            erasure_id = erasure.id.clone();
            manager.process_erasure(&erasure_id).unwrap();
        }

        let reloaded = Manager::open(&storage_path).unwrap();
        assert_eq!(reloaded.list_policies().len(), 1);
        assert_eq!(
            reloaded.get_policy("audit-policy").unwrap().label,
            "AuditLog"
        );
        assert!(
            !reloaded
                .list_legal_holds()
                .into_iter()
                .find(|hold| hold.id == hold_id)
                .unwrap()
                .active
        );
        assert_eq!(
            reloaded.get_erasure_request(&erasure_id).unwrap().status,
            ErasureStatus::Completed
        );
    }

    #[test]
    fn sweep_deletes_expired_nodes_and_respects_legal_holds() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_path_buf();
        let storage = StorageEngine::open(&storage_path).unwrap();
        let old_ms = now_unix_ms() - 10_000;
        storage
            .put_node_record(&NodeRecord {
                id: "user:expired".into(),
                labels: vec!["User".into()],
                properties: BTreeMap::from([(
                    "subject_id".into(),
                    serde_json::Value::String("expired".into()),
                )]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: old_ms,
                updated_at_unix_ms: old_ms,
            })
            .unwrap();
        storage
            .put_node_record(&NodeRecord {
                id: "user:held".into(),
                labels: vec!["User".into()],
                properties: BTreeMap::from([(
                    "subject_id".into(),
                    serde_json::Value::String("held".into()),
                )]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: old_ms,
                updated_at_unix_ms: old_ms,
            })
            .unwrap();
        drop(storage);

        let mut manager = Manager::open(&storage_path).unwrap();
        manager
            .add_policy(Policy {
                id: "short-user".into(),
                label: "User".into(),
                max_age_seconds: 1,
                enabled: true,
                cascade_delete: false,
                description: None,
                data_category: Some(DataCategory::USER.into()),
            })
            .unwrap();
        manager.place_legal_hold("held", "preserve").unwrap();

        let dry_run = manager
            .sweep(RetentionSweepConfig {
                batch_size: 100,
                dry_run: true,
            })
            .unwrap();
        assert_eq!(dry_run.expired, 2);
        assert_eq!(dry_run.deleted, 0);
        assert_eq!(dry_run.held, 1);

        let report = manager
            .sweep(RetentionSweepConfig {
                batch_size: 100,
                dry_run: false,
            })
            .unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(report.held, 1);

        let storage = StorageEngine::open(&storage_path).unwrap();
        assert!(storage.get_node_record("user:expired").unwrap().is_none());
        assert!(storage.get_node_record("user:held").unwrap().is_some());
    }
}
