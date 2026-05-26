//! Configuration loading and management.
//!
//! Equivalent to Go's `pkg/config` in NornicDB.
//! Supports YAML/TOML configuration files, environment variable overrides,
//! and a strongly-typed configuration struct for the database engine.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
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
    #[error("unsupported config format: {0}")]
    UnsupportedFormat(String),
    #[error("config parse error: {0}")]
    Parse(#[from] ::config::ConfigError),
}

pub const ENV_CONFIG_PATH: &str = "COPPERDB_CONFIG";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigOverrides {
    pub address: Option<String>,
    pub http_address: Option<String>,
    pub bolt_address: Option<String>,
    pub http_port: Option<u16>,
    pub bolt_port: Option<u16>,
    pub http_enabled: Option<bool>,
    pub bolt_enabled: Option<bool>,
    pub headless: Option<bool>,
    pub base_path: Option<String>,
    pub static_dir: Option<String>,
}

impl ConfigOverrides {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerConfig {
    pub http_address: String,
    pub bolt_address: String,
    pub http_enabled: bool,
    pub bolt_enabled: bool,
    pub headless: bool,
    pub base_path: String,
    pub static_dir: Option<String>,
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

    pub fn listener_config(&self) -> ListenerConfig {
        let base_address = self.server.address.clone();
        ListenerConfig {
            http_address: self
                .server
                .http_address
                .clone()
                .unwrap_or_else(|| format!("{}:{}", base_address, self.server.http_port)),
            bolt_address: self
                .server
                .bolt_address
                .clone()
                .unwrap_or_else(|| format!("{}:{}", self.server.address, self.server.bolt_port)),
            http_enabled: self.server.http_enabled,
            bolt_enabled: self.server.bolt_enabled,
            headless: self.server.headless,
            base_path: self.server.base_path.clone(),
            static_dir: self.server.static_dir.clone(),
        }
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

pub fn load_file(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("yaml") | Some("yml") => load_yaml(path),
        Some("toml") => load_toml(path),
        _ => Err(ConfigError::UnsupportedFormat(path.display().to_string())),
    }
}

pub fn default_config_candidates(base_dir: impl AsRef<Path>) -> [PathBuf; 3] {
    let base_dir = base_dir.as_ref();
    [
        base_dir.join("copperdb.yaml"),
        base_dir.join("copperdb.yml"),
        base_dir.join("copperdb.toml"),
    ]
}

pub fn find_default_config_path_in(base_dir: impl AsRef<Path>) -> Option<PathBuf> {
    default_config_candidates(base_dir)
        .into_iter()
        .find(|path| path.exists())
}

pub fn load_with_precedence(
    explicit_path: Option<&Path>,
    cli: &ConfigOverrides,
) -> Result<Config, ConfigError> {
    let env = std::env::vars().collect::<BTreeMap<_, _>>();
    load_with_precedence_from(explicit_path, &env, &std::env::current_dir()?, cli)
}

pub fn load_with_precedence_from(
    explicit_path: Option<&Path>,
    env: &BTreeMap<String, String>,
    base_dir: &Path,
    cli: &ConfigOverrides,
) -> Result<Config, ConfigError> {
    let config_path = explicit_path
        .map(PathBuf::from)
        .or_else(|| {
            env.get(ENV_CONFIG_PATH)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| find_default_config_path_in(base_dir));

    let mut cfg = if let Some(path) = config_path {
        load_file(path)?
    } else {
        Config::default()
    };

    apply_env_overrides_from(&mut cfg, env);
    apply_overrides(&mut cfg, cli);
    Ok(cfg)
}

pub fn apply_env_overrides(config: &mut Config) {
    let env = std::env::vars().collect::<BTreeMap<_, _>>();
    apply_env_overrides_from(config, &env);
}

pub fn apply_env_overrides_from(config: &mut Config, env: &BTreeMap<String, String>) {
    set_if_present(env_nonempty(env, "COPPERDB_ADDRESS"), |value| {
        config.server.address = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_HTTP_ADDRESS"), |value| {
        config.server.http_address = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_BOLT_ADDRESS"), |value| {
        config.server.bolt_address = Some(value)
    });
    set_if_present(
        parse_env_u16(env, "COPPERDB_HTTP_PORT")
            .or_else(|| parse_env_u16(env, "NEO4J_dbms_connector_http_listen__address_port")),
        |value| config.server.http_port = value,
    );
    set_if_present(
        parse_env_u16(env, "COPPERDB_BOLT_PORT")
            .or_else(|| parse_env_u16(env, "NEO4J_dbms_connector_bolt_listen__address_port")),
        |value| config.server.bolt_port = value,
    );
    set_if_present(parse_env_bool(env, "COPPERDB_HTTP_ENABLED"), |value| {
        config.server.http_enabled = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_BOLT_ENABLED"), |value| {
        config.server.bolt_enabled = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_HEADLESS"), |value| {
        config.server.headless = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_BASE_PATH"), |value| {
        config.server.base_path = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_STATIC_DIR"), |value| {
        config.server.static_dir = Some(value)
    });
}

pub fn apply_overrides(config: &mut Config, overrides: &ConfigOverrides) {
    if let Some(address) = &overrides.address {
        config.server.address = address.clone();
    }
    if let Some(address) = &overrides.http_address {
        config.server.http_address = Some(address.clone());
    }
    if let Some(address) = &overrides.bolt_address {
        config.server.bolt_address = Some(address.clone());
    }
    if let Some(port) = overrides.http_port {
        config.server.http_port = port;
    }
    if let Some(port) = overrides.bolt_port {
        config.server.bolt_port = port;
    }
    if let Some(enabled) = overrides.http_enabled {
        config.server.http_enabled = enabled;
    }
    if let Some(enabled) = overrides.bolt_enabled {
        config.server.bolt_enabled = enabled;
    }
    if let Some(headless) = overrides.headless {
        config.server.headless = headless;
    }
    if let Some(base_path) = &overrides.base_path {
        config.server.base_path = base_path.clone();
    }
    if let Some(static_dir) = &overrides.static_dir {
        config.server.static_dir = Some(static_dir.clone());
    }
}

fn env_nonempty(env: &BTreeMap<String, String>, key: &str) -> Option<String> {
    env.get(key).filter(|value| !value.is_empty()).cloned()
}

fn parse_env_u16(env: &BTreeMap<String, String>, key: &str) -> Option<u16> {
    env.get(key)?.parse().ok()
}

fn parse_env_bool(env: &BTreeMap<String, String>, key: &str) -> Option<bool> {
    copperdb_envutil::parse_loose_bool_value(env.get(key)?)
}

fn set_if_present<T>(value: Option<T>, apply: impl FnOnce(T)) {
    if let Some(value) = value {
        apply(value);
    }
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
        assert!(
            cfg.auth.jwt_secret.is_empty(),
            "default JWT secret must be empty"
        );
    }

    #[test]
    fn test_validate_rejects_empty_jwt_secret() {
        let cfg = Config::default();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn listener_config_derives_addresses_from_base_address_and_ports() {
        let cfg = Config::default();
        let listeners = cfg.listener_config();
        assert_eq!(listeners.http_address, "0.0.0.0:7474");
        assert_eq!(listeners.bolt_address, "0.0.0.0:7687");
        assert!(listeners.http_enabled);
        assert!(listeners.bolt_enabled);
    }

    #[test]
    fn precedence_is_defaults_file_env_then_cli() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("copperdb.yaml");
        std::fs::write(
            &path,
            r#"
server:
  address: "127.0.0.1"
  http_port: 8000
  bolt_port: 9000
  headless: false
"#,
        )
        .unwrap();

        let mut env = BTreeMap::new();
        env.insert("COPPERDB_HTTP_PORT".to_string(), "8100".to_string());
        env.insert("COPPERDB_HEADLESS".to_string(), "true".to_string());

        let cli = ConfigOverrides {
            bolt_port: Some(9100),
            headless: Some(false),
            ..Default::default()
        };
        let cfg = load_with_precedence_from(None, &env, temp.path(), &cli).unwrap();
        assert_eq!(cfg.server.address, "127.0.0.1");
        assert_eq!(cfg.server.http_port, 8100);
        assert_eq!(cfg.server.bolt_port, 9100);
        assert!(!cfg.server.headless);
    }

    #[test]
    fn explicit_config_path_wins_over_env_and_default_discovery() {
        let temp = tempfile::tempdir().unwrap();
        let explicit = temp.path().join("explicit.toml");
        let env_path = temp.path().join("from-env.toml");
        std::fs::write(&explicit, "[server]\nhttp_port = 7001\n").unwrap();
        std::fs::write(&env_path, "[server]\nhttp_port = 7002\n").unwrap();
        std::fs::write(
            temp.path().join("copperdb.toml"),
            "[server]\nhttp_port = 7003\n",
        )
        .unwrap();

        let mut env = BTreeMap::new();
        env.insert(ENV_CONFIG_PATH.to_string(), env_path.display().to_string());

        let cfg = load_with_precedence_from(
            Some(explicit.as_path()),
            &env,
            temp.path(),
            &ConfigOverrides::default(),
        )
        .unwrap();
        assert_eq!(cfg.server.http_port, 7001);
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
