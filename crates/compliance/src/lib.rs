//! Compliance policy enforcement for magnetDB.
//!
//! Equivalent to Go's `pkg/compliance` in NornicDB.
//! Enforces GDPR data masking, HIPAA access controls, and configurable
//! data governance policies.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ComplianceError {
    #[error("policy violation: {policy} — {message}")]
    PolicyViolation { policy: String, message: String },
    #[error("data masking error: {0}")]
    MaskingError(String),
}

/// Compliance policy types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Policy {
    /// Mask a property value for users without the given role.
    MaskProperty { property: String, allowed_roles: Vec<String> },
    /// Restrict access to a node label for users without the given role.
    RestrictLabel { label: String, allowed_roles: Vec<String> },
    /// Require audit logging for all operations on a given label.
    RequireAudit { label: String },
    /// Data retention: auto-delete nodes older than the given duration.
    RetentionPolicy { label: String, max_age_days: u64 },
}

/// A compliance policy set.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicySet {
    pub policies: Vec<Policy>,
}

impl PolicySet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, policy: Policy) {
        self.policies.push(policy);
    }

    /// Check if a role is allowed to read a property.
    pub fn check_property_access(
        &self,
        property: &str,
        roles: &[String],
    ) -> Result<(), ComplianceError> {
        for policy in &self.policies {
            if let Policy::MaskProperty { property: p, allowed_roles } = policy {
                if p == property {
                    let allowed = roles.iter().any(|r| allowed_roles.contains(r));
                    if !allowed {
                        return Err(ComplianceError::PolicyViolation {
                            policy: "MaskProperty".into(),
                            message: format!("access to '{}' is restricted", property),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_property_denied() {
        let mut policies = PolicySet::new();
        policies.add(Policy::MaskProperty {
            property: "ssn".into(),
            allowed_roles: vec!["admin".into()],
        });
        let result = policies.check_property_access("ssn", &["reader".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_mask_property_allowed() {
        let mut policies = PolicySet::new();
        policies.add(Policy::MaskProperty {
            property: "ssn".into(),
            allowed_roles: vec!["admin".into()],
        });
        let result = policies.check_property_access("ssn", &["admin".to_string()]);
        assert!(result.is_ok());
    }
}
