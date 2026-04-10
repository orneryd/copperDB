//! gRPC server interface for magnetDB.
//!
//! Equivalent to Go's `pkg/nornicgrpc` in NornicDB.
//! Exposes a Protobuf/gRPC API as an alternative to the Bolt protocol.
//! Uses `tonic` (Rust gRPC) + `prost` (Protobuf codegen).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("gRPC transport error: {0}")]
    Transport(String),
    #[error("proto encoding error: {0}")]
    Encoding(String),
    #[error("server not started")]
    NotStarted,
}

/// gRPC server configuration.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub listen_addr: String,
    pub max_connections: usize,
    pub max_message_size_bytes: usize,
    pub tls_enabled: bool,
}

impl GrpcConfig {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            max_connections: 1000,
            max_message_size_bytes: 4 * 1024 * 1024, // 4 MiB
            tls_enabled: false,
        }
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self::new("[::1]:50051")
    }
}

/// gRPC server handle.
pub struct GrpcServer {
    config: GrpcConfig,
}

impl GrpcServer {
    pub fn new(config: GrpcConfig) -> Self {
        Self { config }
    }

    pub fn listen_addr(&self) -> &str {
        &self.config.listen_addr
    }

    pub fn max_connections(&self) -> usize {
        self.config.max_connections
    }

    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size_bytes
    }

    pub fn tls_enabled(&self) -> bool {
        self.config.tls_enabled
    }
}

/// A Cypher query request over gRPC.
#[derive(Debug, Clone)]
pub struct GrpcCypherRequest {
    pub query: String,
    pub database: String,
    pub parameters: std::collections::HashMap<String, serde_json::Value>,
}

/// A single row of results from a Cypher query.
#[derive(Debug, Clone)]
pub struct GrpcCypherRow {
    pub values: Vec<serde_json::Value>,
}

/// A Cypher query response over gRPC.
#[derive(Debug, Clone)]
pub struct GrpcCypherResponse {
    pub columns: Vec<String>,
    pub rows: Vec<GrpcCypherRow>,
    pub error: Option<String>,
}

impl GrpcCypherResponse {
    pub fn ok(columns: Vec<String>, rows: Vec<GrpcCypherRow>) -> Self {
        Self { columns, rows, error: None }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self { columns: vec![], rows: vec![], error: Some(msg.into()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_config_defaults() {
        let cfg = GrpcConfig::default();
        assert_eq!(cfg.listen_addr, "[::1]:50051");
        assert_eq!(cfg.max_connections, 1000);
        assert!(!cfg.tls_enabled);
    }

    #[test]
    fn test_grpc_server_accessors() {
        let cfg = GrpcConfig::new("0.0.0.0:50051");
        let server = GrpcServer::new(cfg);
        assert_eq!(server.listen_addr(), "0.0.0.0:50051");
        assert_eq!(server.max_connections(), 1000);
    }

    #[test]
    fn test_grpc_server_custom_config() {
        let mut cfg = GrpcConfig::new("127.0.0.1:9090");
        cfg.max_connections = 50;
        cfg.tls_enabled = true;
        let server = GrpcServer::new(cfg);
        assert_eq!(server.max_connections(), 50);
        assert!(server.tls_enabled());
    }

    #[test]
    fn test_grpc_cypher_response_ok() {
        let resp = GrpcCypherResponse::ok(
            vec!["n".into()],
            vec![GrpcCypherRow { values: vec![serde_json::json!(1)] }],
        );
        assert!(resp.error.is_none());
        assert_eq!(resp.columns.len(), 1);
    }

    #[test]
    fn test_grpc_cypher_response_error() {
        let resp = GrpcCypherResponse::error("syntax error");
        assert!(resp.error.is_some());
        assert!(resp.rows.is_empty());
    }
}
