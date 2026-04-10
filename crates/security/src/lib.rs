//! Security utilities for magnetDB.
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
            return Err(SecurityError::CertError(
                format!("certificate file not found: {}", self.cert_path)
            ));
        }
        if !std::path::Path::new(&self.key_path).exists() {
            return Err(SecurityError::CertError(
                format!("key file not found: {}", self.key_path)
            ));
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
        return Err(SecurityError::InvalidIdentifier("identifier must not be empty".into()));
    }
    for ch in INJECTION_CHARS {
        if input.contains(*ch) {
            return Err(SecurityError::InvalidIdentifier(
                format!("identifier contains forbidden character: {:?}", ch)
            ));
        }
    }
    if input.chars().any(|c| c.is_control()) {
        return Err(SecurityError::InvalidIdentifier(
            "identifier contains control characters".into()
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
        return Err(SecurityError::InvalidLabel("label must not be empty".into()));
    }
    if !label.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(SecurityError::InvalidLabel(
            format!("label '{}' contains invalid characters (only alphanumeric and _ allowed)", label)
        ));
    }
    Ok(())
}

/// Validate a property key: must be alphanumeric + underscore, non-empty.
pub fn validate_property_key(key: &str) -> Result<(), SecurityError> {
    if key.is_empty() {
        return Err(SecurityError::InvalidPropertyKey("property key must not be empty".into()));
    }
    if !key.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(SecurityError::InvalidPropertyKey(
            format!("property key '{}' contains invalid characters", key)
        ));
    }
    Ok(())
}

/// Hash a password using SHA-256, returning a hex-encoded digest.
/// In production, prefer Argon2 (available via the `argon2` crate in workspace).
pub fn hash_password(password: &str) -> Result<String, SecurityError> {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let result = hasher.finalize();
    Ok(hex::encode(result))
}

/// Verify a password against a SHA-256 hex hash.
pub fn verify_password(password: &str, hash: &str) -> bool {
    match hash_password(password) {
        Ok(computed) => computed == hash,
        Err(_) => false,
    }
}

/// Generate a random 32-byte hex token.
pub fn generate_token() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
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
    fn test_hash_password_is_hex() {
        let hash = hash_password("test").unwrap();
        assert_eq!(hash.len(), 64); // SHA-256 = 32 bytes = 64 hex chars
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
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
