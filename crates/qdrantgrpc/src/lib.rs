//! Qdrant vector database gRPC client integration.
//!
//! Equivalent to Go's `pkg/qdrantgrpc` in NornicDB (uses `github.com/qdrant/go-client`).
//! Provides a gRPC client for offloading vector storage to a Qdrant instance.
//!
//! ## Rust equivalent
//! Use the official `qdrant-client` crate from crates.io, which wraps the
//! Qdrant gRPC API with a high-level Rust interface.
//!
//! ```toml
//! [dependencies]
//! qdrant-client = "1"
//! ```

use thiserror::Error;

#[derive(Debug, Error)]
pub enum QdrantError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("operation failed: {0}")]
    OperationFailed(String),
}

// TODO: Add `qdrant-client = "1"` to Cargo.toml once Qdrant integration is needed.
// The qdrant-client crate provides direct equivalents for all operations in
// NornicDB's pkg/qdrantgrpc package.
