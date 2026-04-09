//! Security utilities for magnetDB.
//!
//! Equivalent to Go's `pkg/security` in NornicDB.
//! Provides TLS certificate management, mutual TLS (mTLS) enforcement,
//! and secure channel configuration for Bolt/gRPC connections.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("TLS configuration error: {0}")]
    TlsConfig(String),
    #[error("certificate error: {0}")]
    CertError(String),
    #[error("unauthorized")]
    Unauthorized,
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

// TODO: Implement TLS setup using `rustls` crate.
// Go equivalent uses `crypto/tls` from the standard library.
// Rust equivalent: `tokio-rustls` + `rustls` crates.
