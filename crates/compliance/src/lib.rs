//! Durable compliance policy and evidence reporting for copperDB.
//!
//! Compliance owns persistent governance policy state and derives HIPAA/SOC2
//! evidence from the durable audit trail.

use copperdb_audit::{AuditError, AuditLog, Event};
use copperdb_storage::{NodeRecord, StorageEngine, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const POLICY_LABEL: &str = "_CompliancePolicy";
const SYSTEM_LABEL: &str = "_System";
const POLICY_PREFIX: &str = "compliance:policy:";

#[derive(Debug, Error)]
pub enum ComplianceError {
    #[error("policy already exists: {0}")]
    PolicyAlreadyExists(String),
    #[error("policy not found: {0}")]
    PolicyNotFound(String),
    #[error("policy violation: {policy} - {message}")]
    PolicyViolation { policy: String, message: String },
    #[error("compliance storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("compliance audit error: {0}")]
    Audit(#[from] AuditError),
    #[error("compliance serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompliancePolicy {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub control: ComplianceControl,
}

impl CompliancePolicy {
    pub fn new(id: impl Into<String>, name: impl Into<String>, control: ComplianceControl) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            enabled: true,
            control,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComplianceControl {
    MaskProperty {
        property: String,
        allowed_roles: Vec<String>,
    },
    RestrictLabel {
        label: String,
        allowed_roles: Vec<String>,
    },
    RequireAudit {
        label: String,
    },
    Retention {
        label: String,
        max_age_days: u64,
    },
}

pub struct ComplianceManager {
    storage: Arc<StorageEngine>,
}

impl ComplianceManager {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }

    pub fn add_policy(&self, policy: CompliancePolicy) -> Result<(), ComplianceError> {
        if self.get_policy(&policy.id)?.is_some() {
            return Err(ComplianceError::PolicyAlreadyExists(policy.id));
        }
        self.put_policy(policy)
    }

    pub fn put_policy(&self, policy: CompliancePolicy) -> Result<(), ComplianceError> {
        self.storage.put_node_record(&policy_to_node(&policy)?)?;
        Ok(())
    }

    pub fn get_policy(&self, id: &str) -> Result<Option<CompliancePolicy>, ComplianceError> {
        self.storage
            .get_node_record(&policy_node_id(id))?
            .map(|node| policy_from_node(&node))
            .transpose()
    }

    pub fn delete_policy(&self, id: &str) -> Result<(), ComplianceError> {
        if self.get_policy(id)?.is_none() {
            return Err(ComplianceError::PolicyNotFound(id.into()));
        }
        self.storage.delete_node_record(&policy_node_id(id))?;
        Ok(())
    }

    pub fn policies(&self) -> Result<Vec<CompliancePolicy>, ComplianceError> {
        let mut policies = Vec::new();
        for node in self.storage.get_nodes_by_label(POLICY_LABEL)? {
            policies.push(policy_from_node(&node)?);
        }
        policies.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(policies)
    }

    pub fn check_property_access(
        &self,
        property: &str,
        roles: &[String],
    ) -> Result<(), ComplianceError> {
        for policy in self.enabled_policies()? {
            if let ComplianceControl::MaskProperty {
                property: governed,
                allowed_roles,
            } = &policy.control
            {
                if governed == property && !role_allowed(roles, allowed_roles) {
                    return Err(ComplianceError::PolicyViolation {
                        policy: policy.id,
                        message: format!("access to property '{property}' is restricted"),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn check_label_access(&self, label: &str, roles: &[String]) -> Result<(), ComplianceError> {
        let policies = self.enabled_policies()?;
        self.check_label_access_with_policies(label, roles, &policies)
    }

    pub fn enabled_policies_snapshot(&self) -> Result<Vec<CompliancePolicy>, ComplianceError> {
        self.enabled_policies()
    }

    pub fn check_label_access_with_policies(
        &self,
        label: &str,
        roles: &[String],
        policies: &[CompliancePolicy],
    ) -> Result<(), ComplianceError> {
        for policy in policies {
            if let ComplianceControl::RestrictLabel {
                label: governed,
                allowed_roles,
            } = &policy.control
            {
                if governed == label && !role_allowed(roles, allowed_roles) {
                    return Err(ComplianceError::PolicyViolation {
                        policy: policy.id.clone(),
                        message: format!("access to label '{label}' is restricted"),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn requires_audit(&self, label: &str) -> Result<bool, ComplianceError> {
        Ok(self.enabled_policies()?.into_iter().any(|policy| {
            matches!(policy.control, ComplianceControl::RequireAudit { label: governed } if governed == label)
        }))
    }

    pub fn retention_days_for_label(&self, label: &str) -> Result<Option<u64>, ComplianceError> {
        Ok(self
            .enabled_policies()?
            .into_iter()
            .find_map(|policy| match policy.control {
                ComplianceControl::Retention {
                    label: governed,
                    max_age_days,
                } if governed == label => Some(max_age_days),
                _ => None,
            }))
    }

    fn enabled_policies(&self) -> Result<Vec<CompliancePolicy>, ComplianceError> {
        Ok(self
            .policies()?
            .into_iter()
            .filter(|policy| policy.enabled)
            .collect())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReportWindow {
    pub from_unix_ms: Option<i64>,
    pub to_unix_ms: Option<i64>,
}

impl ReportWindow {
    pub fn all_time() -> Self {
        Self {
            from_unix_ms: None,
            to_unix_ms: None,
        }
    }

    pub fn contains(&self, timestamp_unix_ms: i64) -> bool {
        self.from_unix_ms
            .map(|from| timestamp_unix_ms >= from)
            .unwrap_or(true)
            && self
                .to_unix_ms
                .map(|to| timestamp_unix_ms <= to)
                .unwrap_or(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventSummary {
    pub total: usize,
    pub success: usize,
    pub failure: usize,
    pub by_event_type: BTreeMap<String, usize>,
    pub by_status_code: BTreeMap<String, usize>,
    pub from_unix_ms: Option<i64>,
    pub to_unix_ms: Option<i64>,
}

impl EventSummary {
    fn new(window: ReportWindow) -> Self {
        Self {
            total: 0,
            success: 0,
            failure: 0,
            by_event_type: BTreeMap::new(),
            by_status_code: BTreeMap::new(),
            from_unix_ms: window.from_unix_ms,
            to_unix_ms: window.to_unix_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HipaaReport {
    pub generated_at_unix_ms: i64,
    pub summary: EventSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Soc2Report {
    pub generated_at_unix_ms: i64,
    pub summary: EventSummary,
}

pub struct ComplianceReporter {
    audit: Arc<AuditLog>,
}

impl ComplianceReporter {
    pub fn new(audit: Arc<AuditLog>) -> Self {
        Self { audit }
    }

    pub fn export_hipaa_evidence(
        &self,
        window: ReportWindow,
    ) -> Result<HipaaReport, ComplianceError> {
        Ok(HipaaReport {
            generated_at_unix_ms: now_unix_ms(),
            summary: self.read_summary(window)?,
        })
    }

    pub fn export_soc2_evidence(
        &self,
        window: ReportWindow,
    ) -> Result<Soc2Report, ComplianceError> {
        Ok(Soc2Report {
            generated_at_unix_ms: now_unix_ms(),
            summary: self.read_summary(window)?,
        })
    }

    pub fn read_summary(&self, window: ReportWindow) -> Result<EventSummary, ComplianceError> {
        let mut summary = EventSummary::new(window);
        for event in self.audit.events()? {
            if !window.contains(event.timestamp_unix_ms) {
                continue;
            }
            summarize_event(&mut summary, &event);
        }
        Ok(summary)
    }
}

fn summarize_event(summary: &mut EventSummary, event: &Event) {
    summary.total += 1;
    if event.success {
        summary.success += 1;
    } else {
        summary.failure += 1;
    }
    *summary
        .by_event_type
        .entry(event.event_type.as_str().into())
        .or_default() += 1;
    if let Some(status_code) = event
        .metadata
        .get("status_code")
        .or_else(|| event.metadata.get("error_code"))
    {
        *summary
            .by_status_code
            .entry(status_code.clone())
            .or_default() += 1;
    }
}

fn role_allowed(roles: &[String], allowed_roles: &[String]) -> bool {
    roles.iter().any(|role| allowed_roles.contains(role))
}

fn policy_node_id(id: &str) -> String {
    format!("{POLICY_PREFIX}{id}")
}

fn policy_to_node(policy: &CompliancePolicy) -> Result<NodeRecord, ComplianceError> {
    let value = serde_json::to_value(policy)?;
    let properties = match value {
        Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>(),
        _ => BTreeMap::new(),
    };
    let timestamp = now_unix_ms();
    Ok(NodeRecord {
        id: policy_node_id(&policy.id),
        labels: vec![POLICY_LABEL.into(), SYSTEM_LABEL.into()],
        properties,
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
    })
}

fn policy_from_node(node: &NodeRecord) -> Result<CompliancePolicy, ComplianceError> {
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
    use copperdb_audit::{AuditConfig, Event, EventType};

    #[allow(clippy::arc_with_non_send_sync)]
    fn manager() -> (Arc<StorageEngine>, ComplianceManager) {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let manager = ComplianceManager::new(Arc::clone(&storage));
        (storage, manager)
    }

    #[test]
    fn policies_persist_and_reload_from_storage() {
        let (storage, manager) = manager();
        manager
            .add_policy(CompliancePolicy::new(
                "mask-ssn",
                "Mask SSN",
                ComplianceControl::MaskProperty {
                    property: "ssn".into(),
                    allowed_roles: vec!["admin".into()],
                },
            ))
            .unwrap();

        let reloaded = ComplianceManager::new(storage);
        let policies = reloaded.policies().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, "mask-ssn");
        match reloaded.check_property_access("ssn", &["reader".into()]) {
            Err(ComplianceError::PolicyViolation { policy, message }) => {
                assert_eq!(policy, "mask-ssn");
                assert_eq!(message, "access to property 'ssn' is restricted");
            }
            result => panic!("expected policy violation, got {result:?}"),
        }
        reloaded
            .check_property_access("ssn", &["admin".into()])
            .unwrap();
    }

    #[test]
    fn label_audit_and_retention_controls_are_enforced() {
        let (_storage, manager) = manager();
        manager
            .add_policy(CompliancePolicy::new(
                "patient-label",
                "Patient Label",
                ComplianceControl::RestrictLabel {
                    label: "Patient".into(),
                    allowed_roles: vec!["doctor".into()],
                },
            ))
            .unwrap();
        manager
            .add_policy(CompliancePolicy::new(
                "audit-finance",
                "Audit Finance",
                ComplianceControl::RequireAudit {
                    label: "Finance".into(),
                },
            ))
            .unwrap();
        manager
            .add_policy(CompliancePolicy::new(
                "retain-log",
                "Retain Logs",
                ComplianceControl::Retention {
                    label: "Log".into(),
                    max_age_days: 30,
                },
            ))
            .unwrap();

        assert!(manager
            .check_label_access("Patient", &["reader".into()])
            .is_err());
        manager
            .check_label_access("Patient", &["doctor".into()])
            .unwrap();
        assert!(manager.requires_audit("Finance").unwrap());
        assert_eq!(manager.retention_days_for_label("Log").unwrap(), Some(30));
    }

    #[test]
    fn policies_can_be_updated_and_deleted() {
        let (_storage, manager) = manager();
        let mut policy = CompliancePolicy::new(
            "mask-email",
            "Mask Email",
            ComplianceControl::MaskProperty {
                property: "email".into(),
                allowed_roles: vec!["admin".into()],
            },
        );
        manager.add_policy(policy.clone()).unwrap();
        assert!(matches!(
            manager.add_policy(policy.clone()),
            Err(ComplianceError::PolicyAlreadyExists(id)) if id == "mask-email"
        ));

        policy.enabled = false;
        manager.put_policy(policy).unwrap();
        manager
            .check_property_access("email", &["reader".into()])
            .unwrap();
        manager.delete_policy("mask-email").unwrap();
        assert!(manager.get_policy("mask-email").unwrap().is_none());
    }

    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn reporter_exports_hipaa_and_soc2_evidence_from_audit_log() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let audit = Arc::new(AuditLog::new(Arc::clone(&storage), AuditConfig::default()).unwrap());
        let now = now_unix_ms();

        audit
            .record(Event {
                timestamp_unix_ms: now,
                event_type: EventType::DataRead,
                success: true,
                ..Event::new(EventType::DataRead)
            })
            .unwrap();
        let mut failed = Event {
            timestamp_unix_ms: now + 1,
            event_type: EventType::AccessDenied,
            success: false,
            ..Event::new(EventType::AccessDenied)
        };
        failed
            .metadata
            .insert("error_code".into(), "ACCESS_DENIED".into());
        audit.record(failed).unwrap();

        let reporter = ComplianceReporter::new(audit);
        let window = ReportWindow {
            from_unix_ms: Some(now - 1000),
            to_unix_ms: Some(now + 1000),
        };
        let hipaa = reporter.export_hipaa_evidence(window).unwrap();
        assert_eq!(hipaa.summary.total, 2);
        assert_eq!(hipaa.summary.success, 1);
        assert_eq!(hipaa.summary.failure, 1);
        assert_eq!(hipaa.summary.by_event_type.get("DATA_READ"), Some(&1));
        assert_eq!(hipaa.summary.by_status_code.get("ACCESS_DENIED"), Some(&1));

        let soc2 = reporter.export_soc2_evidence(window).unwrap();
        assert_eq!(soc2.summary.total, 2);
    }

    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn reporter_honors_report_window() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let audit = Arc::new(AuditLog::new(storage, AuditConfig::default()).unwrap());
        let now = now_unix_ms();
        audit
            .record(Event {
                timestamp_unix_ms: now - 10_000,
                event_type: EventType::Login,
                ..Event::new(EventType::Login)
            })
            .unwrap();
        audit
            .record(Event {
                timestamp_unix_ms: now,
                event_type: EventType::Logout,
                ..Event::new(EventType::Logout)
            })
            .unwrap();

        let reporter = ComplianceReporter::new(audit);
        let summary = reporter
            .read_summary(ReportWindow {
                from_unix_ms: Some(now - 1000),
                to_unix_ms: Some(now + 1000),
            })
            .unwrap();
        assert_eq!(summary.total, 1);
        assert_eq!(summary.by_event_type.get("LOGOUT"), Some(&1));
    }
}
