//! Configuration loading and management.
//!
//! Equivalent to Go's `pkg/config` in NornicDB.
//! Supports YAML/TOML configuration files, environment variable overrides,
//! and a strongly-typed configuration struct for the database engine.

use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error reading config: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("missing required field: {0}")]
    MissingField(String),
    #[error("config parse error: {0}")]
    Parse(#[from] ::config::ConfigError),
}

/// Top-level copperdb configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub storage: StorageConfig,
    pub server: ServerConfig,
    pub bolt: BoltConfig,
    pub auth: AuthConfig,
    pub replication: ReplicationConfig,
    pub encryption: EncryptionConfig,
    pub vectorspace: VectorSpaceConfig,
    pub gpu: GpuConfig,
    pub log_level: String,
}

impl Config {
    /// Validate required fields that have no safe defaults.
    ///
    /// Returns `Err` if `auth.jwt_secret` is empty, which would allow any
    /// operator-issued token to be accepted by all deployments sharing the
    /// same (absent) key.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.auth.jwt_secret.is_empty() {
            return Err(ConfigError::MissingField(
                "auth.jwt_secret must be set (env: copperdb_AUTH__JWT_SECRET)".into(),
            ));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            server: ServerConfig::default(),
            bolt: BoltConfig::default(),
            auth: AuthConfig::default(),
            replication: ReplicationConfig::default(),
            encryption: EncryptionConfig::default(),
            vectorspace: VectorSpaceConfig::default(),
            gpu: GpuConfig::default(),
            log_level: "info".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Shared bind address for HTTP and Bolt unless overridden.
    pub address: String,
    /// Dedicated HTTP bind address override.
    pub http_address: Option<String>,
    /// Dedicated Bolt bind address override.
    pub bolt_address: Option<String>,
    /// HTTP/UI port.
    pub http_port: u16,
    /// Neo4j-compatible Bolt port.
    pub bolt_port: u16,
    /// Enable the HTTP server.
    pub http_enabled: bool,
    /// Enable the Bolt server.
    pub bolt_enabled: bool,
    /// Disable browser/UI routes when true.
    pub headless: bool,
    /// Base path for reverse proxy deployments.
    pub base_path: String,
    /// Optional static UI directory.
    pub static_dir: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".into(),
            http_address: None,
            bolt_address: None,
            http_port: 7474,
            bolt_port: 7687,
            http_enabled: true,
            bolt_enabled: true,
            headless: false,
            base_path: "/".into(),
            static_dir: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Path to the sled database directory.
    pub path: String,
    /// Maximum size of the in-memory cache (bytes).
    pub cache_size: usize,
    /// Fsync on every write.
    pub sync_writes: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: "./data".into(),
            cache_size: 256 * 1024 * 1024,
            sync_writes: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BoltConfig {
    /// TCP address to listen on for Bolt connections.
    pub listen_addr: String,
    /// TLS certificate path.
    pub tls_cert: Option<String>,
    /// TLS private key path.
    pub tls_key: Option<String>,
    /// Maximum concurrent connections.
    pub max_connections: usize,
}

impl Default for BoltConfig {
    fn default() -> Self {
        let server = ServerConfig::default();
        Self {
            listen_addr: format!("{}:{}", server.address, server.bolt_port),
            tls_cert: None,
            tls_key: None,
            max_connections: 256,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Secret used to sign JWT tokens.
    pub jwt_secret: String,
    /// Token expiry in seconds.
    pub token_expiry_secs: u64,
    /// Enable anonymous (unauthenticated) access.
    pub allow_anonymous: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            jwt_secret: String::new(),
            token_expiry_secs: 3600,
            allow_anonymous: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// Raft node ID.
    pub node_id: u64,
    /// Raft peer addresses.
    pub peers: Vec<String>,
    /// Raft heartbeat interval (ms).
    pub heartbeat_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: vec![],
            heartbeat_ms: 150,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionConfig {
    /// KMS provider: "local", "awskms", "azurekeyvault", "gcpkms"
    pub provider: String,
    /// KMS key identifier (ARN, key ID, resource name, etc.)
    pub key_id: Option<String>,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            provider: "local".into(),
            key_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VectorSpaceConfig {
    /// Default embedding dimensions.
    pub dimensions: usize,
    /// Number of nearest neighbors for HNSW index.
    pub hnsw_m: usize,
    /// HNSW ef_construction parameter.
    pub hnsw_ef_construction: usize,
}

impl Default for VectorSpaceConfig {
    fn default() -> Self {
        Self {
            dimensions: 1536,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GpuConfig {
    /// Enable GPU acceleration.
    pub enabled: bool,
    /// Preferred backend: "wgpu", "cuda", "metal", "vulkan", "opencl"
    pub backend: String,
    /// GPU device index to use.
    pub device_index: usize,
}

impl Default for GpuConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: "wgpu".into(),
            device_index: 0,
        }
    }
}

/// Load configuration from a YAML file.
pub fn load_yaml(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let contents = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&contents)?)
}

/// Load configuration from a TOML file.
pub fn load_toml(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let contents = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&contents)?)
}

/// Load configuration from environment variables using the `config` crate.
///
/// Environment variable prefix: `copperdb_`, separator `__`.
/// Example: `copperdb_STORAGE__PATH`, `copperdb_AUTH__JWT_SECRET`.
///
/// Returns `Err` if any variable fails to parse **or** if required fields
/// (e.g. `auth.jwt_secret`) are not set.
pub fn load_from_env() -> Result<Config, ConfigError> {
    let cfg: Config = ::config::Config::builder()
        .add_source(
            ::config::Environment::with_prefix("copperdb")
                .separator("__")
                .try_parsing(true),
        )
        .build()?
        .try_deserialize()?;
    cfg.validate()?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.server.address, "0.0.0.0");
        assert_eq!(cfg.server.http_port, 7474);
        assert_eq!(cfg.bolt.listen_addr, "0.0.0.0:7687");
        assert_eq!(cfg.storage.path, "./data");
    }

    #[test]
    fn test_default_jwt_secret_is_empty() {
        let cfg = Config::default();
        assert!(cfg.auth.jwt_secret.is_empty(), "default JWT secret must be empty");
    }

    #[test]
    fn test_validate_rejects_empty_jwt_secret() {
        let cfg = Config::default();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_accepts_non_empty_jwt_secret() {
        let mut cfg = Config::default();
        cfg.auth.jwt_secret = "super-secret-key-for-testing".into();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_load_from_env_requires_jwt_secret() {
        // Without copperdb_AUTH__JWT_SECRET set, load_from_env must error.
        // (Guard against the env variable already being set in CI.)
        if std::env::var("copperdb_AUTH__JWT_SECRET").is_err() {
            assert!(load_from_env().is_err());
        }
    }
}
