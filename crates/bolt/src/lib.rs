//! Neo4j Bolt protocol server for copperdb.
//!
//! Equivalent to Go's `pkg/bolt` in NornicDB.
//! Implements the Bolt wire protocol (v1–v5) allowing any Neo4j-compatible
//! client (Python neo4j driver, JavaScript driver, cypher-shell, etc.) to
//! connect to copperdb.
//!
//! # Protocol Overview
//! - TCP transport with optional TLS
//! - PackStream binary serialization (custom format, similar to MessagePack)
//! - Request/response messages: HELLO, RUN, PULL, BEGIN, COMMIT, ROLLBACK
//!
//! # Rust Implementation Notes
//! NornicDB implements the full Bolt server in Go.
//! In Rust, this crate uses:
//! - `tokio` for async TCP handling
//! - `bytes` for buffer management
//! - Custom PackStream encoder/decoder (see `packstream` module)
//!
//! There is no existing Rust Bolt *server* library. The `neo4rs` crate is
//! a Bolt *client*, not a server. Full server implementation is required.

pub mod messages;
pub mod packstream;
pub mod server;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BoltError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("PackStream error: {0}")]
    PackStream(String),
    #[error("unsupported Bolt version: {0}.{1}")]
    UnsupportedVersion(u8, u8),
    #[error("authentication failed")]
    AuthFailed,
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
}
