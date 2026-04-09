//! Audit logging: structured event recording for compliance and debugging.
//!
//! Equivalent to Go's `pkg/audit` in NornicDB.
//! Records authenticated operations with user, database, query, and outcome.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("audit write error: {0}")]
    WriteError(String),
}

/// Severity level of an audit event.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: String,
    pub timestamp: u64,
    pub severity: Severity,
    pub username: String,
    pub database: String,
    pub action: String,
    pub resource: String,
    pub outcome: AuditOutcome,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuditOutcome {
    Success,
    Failure,
    Denied,
}

impl AuditEvent {
    pub fn new(
        severity: Severity,
        username: impl Into<String>,
        database: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
        outcome: AuditOutcome,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            severity,
            username: username.into(),
            database: database.into(),
            action: action.into(),
            resource: resource.into(),
            outcome,
            details: None,
        }
    }
}

/// Trait for audit log sinks (write to file, database, external SIEM, etc.).
pub trait AuditSink: Send + Sync {
    fn write(&self, event: &AuditEvent) -> Result<(), AuditError>;
}

/// In-memory audit sink for testing.
pub struct MemoryAuditSink {
    events: std::sync::Mutex<Vec<AuditEvent>>,
}

impl MemoryAuditSink {
    pub fn new() -> Self {
        Self { events: std::sync::Mutex::new(vec![]) }
    }

    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Default for MemoryAuditSink {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditSink for MemoryAuditSink {
    fn write(&self, event: &AuditEvent) -> Result<(), AuditError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_creation() {
        let event = AuditEvent::new(
            Severity::Info,
            "alice",
            "mydb",
            "READ",
            "Person",
            AuditOutcome::Success,
        );
        assert_eq!(event.username, "alice");
        assert_eq!(event.outcome, AuditOutcome::Success);
        assert!(!event.id.is_empty());
    }

    #[test]
    fn test_memory_sink() {
        let sink = MemoryAuditSink::new();
        let event = AuditEvent::new(
            Severity::Warning,
            "bob",
            "testdb",
            "DELETE",
            "Movie",
            AuditOutcome::Denied,
        );
        sink.write(&event).unwrap();
        assert_eq!(sink.events().len(), 1);
    }
}
