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
}

/// Top-level magnetDB configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub storage: StorageConfig,
    pub bolt: BoltConfig,
    pub auth: AuthConfig,
    pub replication: ReplicationConfig,
    pub encryption: EncryptionConfig,
    pub vectorspace: VectorSpaceConfig,
    pub gpu: GpuConfig,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
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
        Self {
            listen_addr: "0.0.0.0:7687".into(),
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
            jwt_secret: "change-me-in-production".into(),
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
pub fn load_from_env() -> Result<Config, ConfigError> {
    // Environment variable prefix: MAGNETDB_
    // e.g. MAGNETDB_STORAGE__PATH, MAGNETDB_BOLT__LISTEN_ADDR
    let builder = ::config::Config::builder()
        .add_source(
            ::config::Environment::with_prefix("MAGNETDB")
                .separator("__")
                .try_parsing(true),
        );
    let cfg: Config = builder
        .build()
        .and_then(|c| c.try_deserialize())
        .unwrap_or_default();
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.bolt.listen_addr, "0.0.0.0:7687");
        assert_eq!(cfg.storage.path, "./data");
    }

    #[test]
    fn test_load_from_env_returns_default_without_env_vars() {
        let cfg = load_from_env().unwrap();
        assert!(!cfg.log_level.is_empty());
    }
}
