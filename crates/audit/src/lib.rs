//! Durable audit logging for copperDB.
//!
//! Audit state is append-only and persisted as system `NodeRecord`s in
//! `copperdb-storage`. In-memory collections in this crate are test helpers or
//! transient hash-chain cursors only; the audit trail itself is durable.

use copperdb_storage::{NodeRecord, StorageEngine, StorageError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

const AUDIT_LABEL: &str = "_AuditEvent";
const SYSTEM_LABEL: &str = "_System";
const AUDIT_PREFIX: &str = "audit:event:";

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("audit write error: {0}")]
    WriteError(String),
    #[error("audit storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("audit serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("audit trail integrity error: {0}")]
    Integrity(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Login,
    Logout,
    LoginFailed,
    PasswordChange,
    AccessDenied,
    RoleChange,
    DataRead,
    DataCreate,
    DataUpdate,
    DataDelete,
    DataExport,
    ErasureRequest,
    ErasureComplete,
    ConsentGiven,
    ConsentRevoked,
    ConfigChange,
    Backup,
    Restore,
    SchemaChange,
    SecurityAlert,
    BreachDetected,
    SnapshotExpired,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::Login => "LOGIN",
            EventType::Logout => "LOGOUT",
            EventType::LoginFailed => "LOGIN_FAILED",
            EventType::PasswordChange => "PASSWORD_CHANGE",
            EventType::AccessDenied => "ACCESS_DENIED",
            EventType::RoleChange => "ROLE_CHANGE",
            EventType::DataRead => "DATA_READ",
            EventType::DataCreate => "DATA_CREATE",
            EventType::DataUpdate => "DATA_UPDATE",
            EventType::DataDelete => "DATA_DELETE",
            EventType::DataExport => "DATA_EXPORT",
            EventType::ErasureRequest => "ERASURE_REQUEST",
            EventType::ErasureComplete => "ERASURE_COMPLETE",
            EventType::ConsentGiven => "CONSENT_GIVEN",
            EventType::ConsentRevoked => "CONSENT_REVOKED",
            EventType::ConfigChange => "CONFIG_CHANGE",
            EventType::Backup => "BACKUP",
            EventType::Restore => "RESTORE",
            EventType::SchemaChange => "SCHEMA_CHANGE",
            EventType::SecurityAlert => "SECURITY_ALERT",
            EventType::BreachDetected => "BREACH_DETECTED",
            EventType::SnapshotExpired => "SNAPSHOT_EXPIRED",
        }
    }

    fn for_data_action(action: &str) -> Self {
        match action.trim().to_ascii_uppercase().as_str() {
            "CREATE" => Self::DataCreate,
            "UPDATE" | "SET" | "MERGE" => Self::DataUpdate,
            "DELETE" => Self::DataDelete,
            "EXPORT" => Self::DataExport,
            _ => Self::DataRead,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub id: String,
    pub timestamp_unix_ms: i64,
    pub event_type: EventType,
    pub user_id: Option<String>,
    pub username: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub resource: Option<String>,
    pub resource_id: Option<String>,
    pub action: Option<String>,
    pub success: bool,
    pub reason: Option<String>,
    pub data_classification: Option<String>,
    pub request_id: Option<String>,
    pub session_id: Option<String>,
    pub request_path: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub sequence: u64,
    pub previous_hash: Option<String>,
    pub hash: Option<String>,
}

impl Event {
    pub fn new(event_type: EventType) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp_unix_ms: now_unix_ms(),
            event_type,
            user_id: None,
            username: None,
            ip_address: None,
            user_agent: None,
            resource: None,
            resource_id: None,
            action: None,
            success: true,
            reason: None,
            data_classification: None,
            request_id: None,
            session_id: None,
            request_path: None,
            metadata: BTreeMap::new(),
            sequence: 0,
            previous_hash: None,
            hash: None,
        }
    }

    pub fn failed(mut self, reason: impl Into<String>) -> Self {
        self.success = false;
        self.reason = Some(reason.into());
        self
    }

    fn canonical_for_hash(&self) -> Result<Vec<u8>, AuditError> {
        let mut clone = self.clone();
        clone.hash = None;
        Ok(serde_json::to_vec(&clone)?)
    }
}

#[derive(Debug, Clone)]
pub struct AuditConfig {
    pub enabled: bool,
    pub alert_on_events: BTreeSet<EventType>,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            alert_on_events: BTreeSet::from([
                EventType::BreachDetected,
                EventType::SecurityAlert,
                EventType::AccessDenied,
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainVerification {
    pub valid: bool,
    pub checked: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
struct ChainState {
    next_sequence: u64,
    previous_hash: Option<String>,
}

pub struct AuditLog {
    storage: Arc<StorageEngine>,
    config: AuditConfig,
    chain: Mutex<ChainState>,
    alerts: Mutex<Vec<Event>>,
}

impl AuditLog {
    pub fn new(storage: Arc<StorageEngine>, config: AuditConfig) -> Result<Self, AuditError> {
        let events = load_events(&storage)?;
        let chain = ChainState {
            next_sequence: events.last().map(|event| event.sequence + 1).unwrap_or(1),
            previous_hash: events.last().and_then(|event| event.hash.clone()),
        };
        Ok(Self {
            storage,
            config,
            chain: Mutex::new(chain),
            alerts: Mutex::new(Vec::new()),
        })
    }

    pub fn record(&self, mut event: Event) -> Result<Event, AuditError> {
        if !self.config.enabled {
            return Ok(event);
        }
        if event.id.is_empty() {
            event.id = Uuid::new_v4().to_string();
        }
        if event.timestamp_unix_ms == 0 {
            event.timestamp_unix_ms = now_unix_ms();
        }

        let mut chain = self.chain.lock();
        event.sequence = chain.next_sequence;
        event.previous_hash = chain.previous_hash.clone();
        event.hash = Some(hash_event(&event)?);

        self.storage.put_node_record(&event_to_node(&event)?)?;
        chain.next_sequence += 1;
        chain.previous_hash = event.hash.clone();
        drop(chain);

        if self.config.alert_on_events.contains(&event.event_type) {
            self.alerts.lock().push(event.clone());
        }
        Ok(event)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn log_auth(
        &self,
        event_type: EventType,
        user_id: impl Into<String>,
        username: impl Into<String>,
        ip_address: impl Into<String>,
        user_agent: impl Into<String>,
        success: bool,
        reason: impl Into<String>,
    ) -> Result<Event, AuditError> {
        let reason = reason.into();
        let event = Event {
            user_id: Some(user_id.into()),
            username: Some(username.into()),
            ip_address: Some(ip_address.into()),
            user_agent: Some(user_agent.into()),
            success,
            reason: if reason.is_empty() {
                None
            } else {
                Some(reason)
            },
            ..Event::new(event_type)
        };
        self.record(event)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn log_data_access(
        &self,
        user_id: impl Into<String>,
        username: impl Into<String>,
        resource: impl Into<String>,
        resource_id: impl Into<String>,
        action: impl Into<String>,
        success: bool,
        data_classification: impl Into<String>,
    ) -> Result<Event, AuditError> {
        let action = action.into();
        let event = Event {
            event_type: EventType::for_data_action(&action),
            user_id: Some(user_id.into()),
            username: Some(username.into()),
            resource: Some(resource.into()),
            resource_id: Some(resource_id.into()),
            action: Some(action),
            success,
            data_classification: Some(data_classification.into()),
            ..Event::new(EventType::DataRead)
        };
        self.record(event)
    }

    pub fn log_erasure(
        &self,
        user_id: impl Into<String>,
        username: impl Into<String>,
        target_user_id: impl Into<String>,
        complete: bool,
        reason: impl Into<String>,
    ) -> Result<Event, AuditError> {
        let mut event = Event {
            event_type: if complete {
                EventType::ErasureComplete
            } else {
                EventType::ErasureRequest
            },
            user_id: Some(user_id.into()),
            username: Some(username.into()),
            reason: Some(reason.into()),
            ..Event::new(EventType::ErasureRequest)
        };
        event
            .metadata
            .insert("target_user_id".into(), target_user_id.into());
        self.record(event)
    }

    pub fn log_consent(
        &self,
        user_id: impl Into<String>,
        username: impl Into<String>,
        granted: bool,
        consent_type: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Event, AuditError> {
        let mut event = Event {
            event_type: if granted {
                EventType::ConsentGiven
            } else {
                EventType::ConsentRevoked
            },
            user_id: Some(user_id.into()),
            username: Some(username.into()),
            success: true,
            ..Event::new(EventType::ConsentGiven)
        };
        event
            .metadata
            .insert("consent_type".into(), consent_type.into());
        event.metadata.insert("version".into(), version.into());
        self.record(event)
    }

    pub fn events(&self) -> Result<Vec<Event>, AuditError> {
        load_events(&self.storage)
    }

    pub fn events_for_user(&self, user_id: &str) -> Result<Vec<Event>, AuditError> {
        Ok(self
            .events()?
            .into_iter()
            .filter(|event| event.user_id.as_deref() == Some(user_id))
            .collect())
    }

    pub fn alerts(&self) -> Vec<Event> {
        self.alerts.lock().clone()
    }

    pub fn verify_chain(&self) -> Result<ChainVerification, AuditError> {
        verify_events(&self.events()?)
    }
}

fn load_events(storage: &StorageEngine) -> Result<Vec<Event>, AuditError> {
    let mut events = Vec::new();
    for node in storage.get_nodes_by_label(AUDIT_LABEL)? {
        events.push(event_from_node(&node)?);
    }
    events.sort_by(|a, b| {
        a.sequence
            .cmp(&b.sequence)
            .then_with(|| a.timestamp_unix_ms.cmp(&b.timestamp_unix_ms))
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(events)
}

fn verify_events(events: &[Event]) -> Result<ChainVerification, AuditError> {
    let mut previous_hash: Option<String> = None;
    for (index, event) in events.iter().enumerate() {
        if event.sequence != (index as u64) + 1 {
            return Ok(ChainVerification {
                valid: false,
                checked: index,
                error: Some(format!("sequence gap at event {}", event.id)),
            });
        }
        if event.previous_hash != previous_hash {
            return Ok(ChainVerification {
                valid: false,
                checked: index,
                error: Some(format!("previous hash mismatch at event {}", event.id)),
            });
        }
        if event.hash.as_deref() != Some(hash_event(event)?.as_str()) {
            return Ok(ChainVerification {
                valid: false,
                checked: index,
                error: Some(format!("hash mismatch at event {}", event.id)),
            });
        }
        previous_hash = event.hash.clone();
    }
    Ok(ChainVerification {
        valid: true,
        checked: events.len(),
        error: None,
    })
}

fn hash_event(event: &Event) -> Result<String, AuditError> {
    Ok(hex::encode(Sha256::digest(event.canonical_for_hash()?)))
}

fn event_to_node(event: &Event) -> Result<NodeRecord, AuditError> {
    let value = serde_json::to_value(event)?;
    let properties = match value {
        Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>(),
        _ => BTreeMap::new(),
    };
    Ok(NodeRecord {
        id: format!(
            "{AUDIT_PREFIX}{:020}:{:020}:{}",
            event.timestamp_unix_ms, event.sequence, event.id
        ),
        labels: vec![AUDIT_LABEL.into(), SYSTEM_LABEL.into()],
        properties,
        created_at_unix_ms: event.timestamp_unix_ms,
        updated_at_unix_ms: event.timestamp_unix_ms,
    })
}

fn event_from_node(node: &NodeRecord) -> Result<Event, AuditError> {
    let value = Value::Object(node.properties.clone().into_iter().collect());
    Ok(serde_json::from_value(value)?)
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::arc_with_non_send_sync)]
    fn audit_log() -> (Arc<StorageEngine>, AuditLog) {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let log = AuditLog::new(Arc::clone(&storage), AuditConfig::default()).unwrap();
        (storage, log)
    }

    #[test]
    fn persistent_log_survives_reload_and_verifies_hash_chain() {
        let (storage, log) = audit_log();
        let login = log
            .log_auth(
                EventType::Login,
                "user-1",
                "alice",
                "127.0.0.1",
                "agent",
                true,
                "",
            )
            .unwrap();
        let read = log
            .log_data_access("user-1", "alice", "node", "node-1", "READ", true, "PII")
            .unwrap();
        assert_eq!(login.sequence, 1);
        assert_eq!(read.sequence, 2);
        assert_eq!(read.previous_hash, login.hash);
        assert!(log.verify_chain().unwrap().valid);

        let reloaded = AuditLog::new(storage, AuditConfig::default()).unwrap();
        let events = reloaded.events().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, EventType::Login);
        assert_eq!(events[1].data_classification.as_deref(), Some("PII"));
        assert!(reloaded.verify_chain().unwrap().valid);
    }

    #[test]
    fn gdpr_helpers_record_erasure_and_consent_metadata() {
        let (_storage, log) = audit_log();
        let erasure = log
            .log_erasure("admin-1", "admin", "user-2", false, "requested")
            .unwrap();
        assert_eq!(erasure.event_type, EventType::ErasureRequest);
        assert_eq!(
            erasure.metadata.get("target_user_id"),
            Some(&"user-2".into())
        );

        let consent = log
            .log_consent("user-2", "alice", true, "marketing", "v1")
            .unwrap();
        assert_eq!(consent.event_type, EventType::ConsentGiven);
        assert_eq!(
            consent.metadata.get("consent_type"),
            Some(&"marketing".into())
        );
    }

    #[test]
    fn security_events_are_captured_as_alerts() {
        let (_storage, log) = audit_log();
        log.record(Event::new(EventType::Login)).unwrap();
        log.record(Event::new(EventType::SecurityAlert).failed("suspicious"))
            .unwrap();
        log.record(Event::new(EventType::BreachDetected).failed("breach"))
            .unwrap();
        assert_eq!(log.alerts().len(), 2);
    }

    #[test]
    fn tampering_breaks_chain_verification() {
        let (storage, log) = audit_log();
        let event = log.record(Event::new(EventType::DataRead)).unwrap();
        let mut node = storage
            .get_nodes_by_label(AUDIT_LABEL)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        node.properties
            .insert("reason".into(), serde_json::json!("tampered"));
        storage.put_node_record(&node).unwrap();

        let reloaded = AuditLog::new(storage, AuditConfig::default()).unwrap();
        let result = reloaded.verify_chain().unwrap();
        assert!(!result.valid);
        assert!(result.error.unwrap().contains(&event.id));
    }

    #[test]
    fn events_for_user_filters_durable_events() {
        let (_storage, log) = audit_log();
        log.log_auth(EventType::Login, "user-1", "alice", "", "", true, "")
            .unwrap();
        log.log_auth(EventType::Login, "user-2", "bob", "", "", true, "")
            .unwrap();
        let events = log.events_for_user("user-1").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].username.as_deref(), Some("alice"));
    }
}
