//! Bolt server TCP listener and connection handler.
//!
//! ⚠️ **Stub implementation.** Full Bolt handshake, authentication,
//! and message dispatch must be implemented here.

use crate::BoltError;

/// Bolt server configuration.
#[derive(Debug, Clone)]
pub struct BoltServer {
    pub listen_addr: String,
}

impl BoltServer {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self { listen_addr: listen_addr.into() }
    }

    /// Start accepting Bolt connections.
    ///
    /// ⚠️ Not yet implemented. See NornicDB's `pkg/bolt/server.go` for reference.
    pub async fn serve(&self) -> Result<(), BoltError> {
        // TODO: tokio::net::TcpListener::bind(self.listen_addr)
        //       Handle Bolt handshake, auth, and message loop
        Err(BoltError::ProtocolViolation("Bolt server not yet implemented".into()))
    }
}
