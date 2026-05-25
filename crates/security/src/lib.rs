//! Security utilities for copperdb.
//!
//! Equivalent to Go's `pkg/security` in NornicDB.
//! Provides input sanitization, identifier validation, password hashing,
//! token generation, and TLS certificate management configuration.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("TLS configuration error: {0}")]
    TlsConfig(String),
    #[error("certificate error: {0}")]
    CertError(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error("invalid label: {0}")]
    InvalidLabel(String),
    #[error("invalid property key: {0}")]
    InvalidPropertyKey(String),
    #[error("hashing error: {0}")]
    HashingError(String),
}

/// TLS configuration options.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to PEM-encoded certificate chain.
    pub cert_path: String,
    /// Path to PEM-encoded private key.
    pub key_path: String,
    /// Path to CA certificate for client verification (mTLS).
    pub ca_cert_path: Option<String>,
    /// Require client certificates (mutual TLS).
    pub require_client_cert: bool,
    /// Minimum TLS version ("1.2" or "1.3").
    pub min_version: String,
}

impl TlsConfig {
    pub fn validate(&self) -> Result<(), SecurityError> {
        if !std::path::Path::new(&self.cert_path).exists() {
            return Err(SecurityError::CertError(format!(
                "certificate file not found: {}",
                self.cert_path
            )));
        }
        if !std::path::Path::new(&self.key_path).exists() {
            return Err(SecurityError::CertError(format!(
                "key file not found: {}",
                self.key_path
            )));
        }
        Ok(())
    }
}

/// Characters that are forbidden in Cypher identifiers to prevent injection.
const INJECTION_CHARS: &[char] = &['`', '"', '\'', ';', '\n', '\r', '\0', '\\'];

/// Sanitize a Cypher identifier (label, relationship type, property key).
/// Rejects any input containing injection characters or control characters.
pub fn sanitize_identifier(input: &str) -> Result<String, SecurityError> {
    if input.is_empty() {
        return Err(SecurityError::InvalidIdentifier(
            "identifier must not be empty".into(),
        ));
    }
    for ch in INJECTION_CHARS {
        if input.contains(*ch) {
            return Err(SecurityError::InvalidIdentifier(format!(
                "identifier contains forbidden character: {:?}",
                ch
            )));
        }
    }
    if input.chars().any(|c| c.is_control()) {
        return Err(SecurityError::InvalidIdentifier(
            "identifier contains control characters".into(),
        ));
    }
    Ok(input.to_string())
}

/// Escape single quotes in a string value for safe embedding in Cypher.
pub fn sanitize_string_value(input: &str) -> String {
    input.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Validate a node label: must be alphanumeric + underscore, non-empty.
pub fn validate_label(label: &str) -> Result<(), SecurityError> {
    if label.is_empty() {
        return Err(SecurityError::InvalidLabel(
            "label must not be empty".into(),
        ));
    }
    if !label.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(SecurityError::InvalidLabel(format!(
            "label '{}' contains invalid characters (only alphanumeric and _ allowed)",
            label
        )));
    }
    Ok(())
}

/// Validate a property key: must be alphanumeric + underscore, non-empty.
pub fn validate_property_key(key: &str) -> Result<(), SecurityError> {
    if key.is_empty() {
        return Err(SecurityError::InvalidPropertyKey(
            "property key must not be empty".into(),
        ));
    }
    if !key.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(SecurityError::InvalidPropertyKey(format!(
            "property key '{}' contains invalid characters",
            key
        )));
    }
    Ok(())
}

/// Hash a password using Argon2id with a random salt, returning a PHC string.
/// The returned string encodes the algorithm, parameters, salt, and hash and
/// can be stored directly. Use [`verify_password`] to check a candidate.
pub fn hash_password(password: &str) -> Result<String, SecurityError> {
    use argon2::{password_hash::PasswordHasher, Argon2};
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes())
        .map(|h| h.to_string())
        .map_err(|e| SecurityError::HashingError(e.to_string()))
}

/// Verify a password against an Argon2id PHC hash produced by [`hash_password`].
/// Uses constant-time comparison internally.
pub fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::{
        password_hash::{phc::PasswordHash, PasswordVerifier},
        Argon2,
    };
    let parsed = match PasswordHash::new(hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Generate a random 32-byte hex token.
pub fn generate_token() -> String {
    use getrandom::fill as fill_random;
    let mut bytes = [0u8; 32];
    fill_random(&mut bytes).expect("os rng should be available");
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_identifier_valid() {
        assert!(sanitize_identifier("Person").is_ok());
        assert!(sanitize_identifier("my_label_123").is_ok());
    }

    #[test]
    fn test_sanitize_identifier_injection() {
        assert!(sanitize_identifier("Person`").is_err());
        assert!(sanitize_identifier("label; DROP").is_err());
        assert!(sanitize_identifier("label'").is_err());
    }

    #[test]
    fn test_sanitize_identifier_empty() {
        assert!(sanitize_identifier("").is_err());
    }

    #[test]
    fn test_sanitize_string_value() {
        let safe = sanitize_string_value("it's a test");
        assert_eq!(safe, "it\\'s a test");
    }

    #[test]
    fn test_validate_label_valid() {
        assert!(validate_label("Person").is_ok());
        assert!(validate_label("Movie_Title").is_ok());
    }

    #[test]
    fn test_validate_label_invalid() {
        assert!(validate_label("Person-Node").is_err());
        assert!(validate_label("").is_err());
        assert!(validate_label("Label With Space").is_err());
    }

    #[test]
    fn test_validate_property_key_valid() {
        assert!(validate_property_key("name").is_ok());
        assert!(validate_property_key("created_at").is_ok());
    }

    #[test]
    fn test_validate_property_key_invalid() {
        assert!(validate_property_key("").is_err());
        assert!(validate_property_key("my-key").is_err());
    }

    #[test]
    fn test_hash_and_verify_password() {
        let hash = hash_password("s3cr3t!").unwrap();
        assert!(verify_password("s3cr3t!", &hash));
        assert!(!verify_password("wrong", &hash));
    }

    #[test]
    fn test_hash_password_is_argon2_phc() {
        let hash = hash_password("test").unwrap();
        // Argon2id PHC strings start with "$argon2id$"
        assert!(
            hash.starts_with("$argon2id$"),
            "expected Argon2id PHC string, got: {hash}"
        );
    }

    #[test]
    fn test_hash_password_different_salts() {
        // Each call produces a different hash (random salt)
        let h1 = hash_password("same").unwrap();
        let h2 = hash_password("same").unwrap();
        assert_ne!(
            h1, h2,
            "two hashes of the same password must differ (different salts)"
        );
    }

    #[test]
    fn test_generate_token_length() {
        let token = generate_token();
        assert_eq!(token.len(), 64); // 32 bytes = 64 hex chars
    }

    #[test]
    fn test_generate_token_unique() {
        let t1 = generate_token();
        let t2 = generate_token();
        assert_ne!(t1, t2);
    }
}
