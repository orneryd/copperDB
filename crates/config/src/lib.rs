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
    #[error("invalid per-database override key: {0}")]
    InvalidPerDatabaseOverrideKey(String),
    #[error("invalid value '{value}' for per-database override {key}")]
    InvalidPerDatabaseOverrideValue { key: String, value: String },
}

pub const ENV_CONFIG_PATH: &str = "COPPERDB_CONFIG";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigOverrides {
    pub address: Option<String>,
    pub http_address: Option<String>,
    pub bolt_address: Option<String>,
    pub grpc_address: Option<String>,
    pub grpc_auth_token: Option<String>,
    pub grpc_auth_token_kms_ciphertext: Option<String>,
    pub grpc_auth_token_kms_key_uri: Option<String>,
    pub grpc_tls_enabled: Option<bool>,
    pub grpc_tls_cert: Option<String>,
    pub grpc_tls_key: Option<String>,
    pub grpc_tls_ca_cert: Option<String>,
    pub grpc_tls_domain_name: Option<String>,
    pub grpc_tls_client_cert: Option<String>,
    pub grpc_tls_client_key: Option<String>,
    pub grpc_tls_client_auth_ca_cert: Option<String>,
    pub grpc_tls_client_auth_optional: Option<bool>,
    pub http_port: Option<u16>,
    pub bolt_port: Option<u16>,
    pub grpc_port: Option<u16>,
    pub http_enabled: Option<bool>,
    pub bolt_enabled: Option<bool>,
    pub grpc_enabled: Option<bool>,
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
    pub grpc_address: String,
    pub http_enabled: bool,
    pub bolt_enabled: bool,
    pub grpc_enabled: bool,
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
    pub embedding: EmbeddingConfig,
    pub search: SearchConfig,
    pub features: FeatureConfig,
    pub vectorspace: VectorSpaceConfig,
    pub gpu: GpuConfig,
    #[serde(skip)]
    pub cli_overrides: BTreeMap<String, String>,
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
        if self.server.grpc_tls_enabled {
            if self.server.grpc_tls_cert.as_deref().unwrap_or_default().is_empty() {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_cert must be set when gRPC TLS is enabled".into(),
                ));
            }
            if self.server.grpc_tls_key.as_deref().unwrap_or_default().is_empty() {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_key must be set when gRPC TLS is enabled".into(),
                ));
            }
            if self.server.grpc_tls_client_cert.is_some() ^ self.server.grpc_tls_client_key.is_some()
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_client_cert and server.grpc_tls_client_key must be set together"
                        .into(),
                ));
            }
            if self.server.grpc_tls_client_auth_ca_cert.is_some()
                && self.server.grpc_tls_client_auth_optional
                && self.server.grpc_tls_client_auth_ca_cert.as_deref().unwrap_or_default().is_empty()
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_client_auth_ca_cert must be set when optional client auth is enabled"
                        .into(),
                ));
            }
            if self.server.grpc_tls_client_auth_ca_cert.is_none()
                && self.server.grpc_tls_client_auth_optional
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_client_auth_ca_cert must be set when optional client auth is enabled"
                        .into(),
                ));
            }
            if self.server.grpc_tls_client_auth_ca_cert.is_some()
                && (self.server.grpc_tls_client_cert.is_none()
                    || self.server.grpc_tls_client_key.is_none())
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_client_cert and server.grpc_tls_client_key must be set when mTLS client auth is configured"
                        .into(),
                ));
            }
        }
        if self.server.grpc_auth_token.is_some()
            && self.server.grpc_auth_token_kms_ciphertext.is_some()
        {
            return Err(ConfigError::InvalidPerDatabaseOverrideValue {
                key: "server.grpc_auth_token".into(),
                value: "set either plain gRPC auth token or KMS-encrypted gRPC auth token, not both"
                    .into(),
            });
        }
        if self.server.grpc_auth_token_kms_ciphertext.is_some()
            && self.server.grpc_auth_token_kms_key_uri.is_none()
        {
            return Err(ConfigError::MissingField(
                "server.grpc_auth_token_kms_key_uri must be set when using a KMS-encrypted gRPC auth token"
                    .into(),
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
            grpc_address: self
                .server
                .grpc_address
                .clone()
                .unwrap_or_else(|| format!("{}:{}", self.server.address, self.server.grpc_port)),
            http_enabled: self.server.http_enabled,
            bolt_enabled: self.server.bolt_enabled,
            grpc_enabled: self.server.grpc_enabled,
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
            embedding: EmbeddingConfig::default(),
            search: SearchConfig::default(),
            features: FeatureConfig::default(),
            vectorspace: VectorSpaceConfig::default(),
            gpu: GpuConfig::default(),
            cli_overrides: BTreeMap::new(),
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
    /// Dedicated gRPC bind address override.
    pub grpc_address: Option<String>,
    /// Shared bearer token required for internal gRPC RPCs when set.
    pub grpc_auth_token: Option<String>,
    /// Base64 ciphertext for a KMS-encrypted shared gRPC bearer token.
    pub grpc_auth_token_kms_ciphertext: Option<String>,
    /// KMS key URI used to decrypt the shared gRPC bearer token.
    pub grpc_auth_token_kms_key_uri: Option<String>,
    /// Enable TLS for the internal gRPC listener and clients.
    pub grpc_tls_enabled: bool,
    /// PEM certificate path for the internal gRPC listener.
    pub grpc_tls_cert: Option<String>,
    /// PEM private key path for the internal gRPC listener.
    pub grpc_tls_key: Option<String>,
    /// Optional PEM CA certificate path for internal gRPC clients.
    pub grpc_tls_ca_cert: Option<String>,
    /// Optional TLS domain name override for internal gRPC clients.
    pub grpc_tls_domain_name: Option<String>,
    /// Optional client certificate path for mTLS-authenticated internal gRPC clients.
    pub grpc_tls_client_cert: Option<String>,
    /// Optional client private key path for mTLS-authenticated internal gRPC clients.
    pub grpc_tls_client_key: Option<String>,
    /// Optional CA certificate path used by the server to validate client certificates.
    pub grpc_tls_client_auth_ca_cert: Option<String>,
    /// Allow clients without a certificate when client-auth CA validation is configured.
    pub grpc_tls_client_auth_optional: bool,
    /// HTTP/UI port.
    pub http_port: u16,
    /// Neo4j-compatible Bolt port.
    pub bolt_port: u16,
    /// Internal gRPC port.
    pub grpc_port: u16,
    /// Enable the HTTP server.
    pub http_enabled: bool,
    /// Enable the Bolt server.
    pub bolt_enabled: bool,
    /// Enable the internal gRPC server.
    pub grpc_enabled: bool,
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
            grpc_address: None,
            grpc_auth_token: None,
            grpc_auth_token_kms_ciphertext: None,
            grpc_auth_token_kms_key_uri: None,
            grpc_tls_enabled: false,
            grpc_tls_cert: None,
            grpc_tls_key: None,
            grpc_tls_ca_cert: None,
            grpc_tls_domain_name: None,
            grpc_tls_client_cert: None,
            grpc_tls_client_key: None,
            grpc_tls_client_auth_ca_cert: None,
            grpc_tls_client_auth_optional: false,
            http_port: 7474,
            bolt_port: 7687,
            grpc_port: 50051,
            http_enabled: true,
            bolt_enabled: true,
            grpc_enabled: false,
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
pub struct EmbeddingConfig {
    /// Master switch for embedding work.
    pub enabled: bool,
    /// Embedding backend identifier.
    pub provider: String,
    /// Embedding model name or path.
    pub model: String,
    /// Optional embedding API URL.
    pub api_url: Option<String>,
    /// Embedding vector dimensions.
    pub dimensions: usize,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: String::new(),
            model: String::new(),
            api_url: None,
            dimensions: VectorSpaceConfig::default().dimensions,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    /// Minimum similarity threshold for vector search.
    pub min_similarity: f64,
    /// Master switch for BM25 fulltext.
    pub bm25_enabled: bool,
    /// Warming mode for BM25 indexes.
    pub bm25_warming: String,
    /// Master switch for vector search.
    pub vector_enabled: bool,
    /// Warming mode for vector indexes.
    pub vector_warming: String,
    /// Reranking master switch. Represented for parity but default-off.
    pub rerank_enabled: bool,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            min_similarity: 0.0,
            bm25_enabled: false,
            bm25_warming: "lazy".into(),
            vector_enabled: false,
            vector_warming: "lazy".into(),
            rerank_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeatureConfig {
    /// Automatic link creation.
    pub auto_links_enabled: bool,
    /// Automatic topology/TLP workflows.
    pub auto_tlp_enabled: bool,
}

impl Default for FeatureConfig {
    fn default() -> Self {
        Self {
            auto_links_enabled: false,
            auto_tlp_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PerDatabaseConfigKey {
    pub key: &'static str,
    pub value_type: &'static str,
    pub category: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EffectiveDatabaseConfig {
    pub embedding_enabled: bool,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding_api_url: Option<String>,
    pub embedding_dimensions: usize,
    pub search_min_similarity: f64,
    pub bm25_enabled: bool,
    pub bm25_warming: String,
    pub vector_enabled: bool,
    pub vector_warming: String,
    pub rerank_enabled: bool,
    pub auto_links_enabled: bool,
    pub auto_tlp_enabled: bool,
    pub effective: BTreeMap<String, String>,
}

pub const PER_DATABASE_CONFIG_KEYS: [PerDatabaseConfigKey; 13] = [
    PerDatabaseConfigKey {
        key: "COPPERDB_EMBEDDING_ENABLED",
        value_type: "boolean",
        category: "Embeddings",
        description: "Master switch for embedding work on this database. Default: false.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_EMBEDDING_PROVIDER",
        value_type: "string",
        category: "Embeddings",
        description: "Embedding backend identifier for this database.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_EMBEDDING_MODEL",
        value_type: "string",
        category: "Embeddings",
        description: "Embedding model name or path for this database.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_EMBEDDING_API_URL",
        value_type: "string",
        category: "Embeddings",
        description: "Optional embedding API URL for this database.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_EMBEDDING_DIMENSIONS",
        value_type: "number",
        category: "Embeddings",
        description: "Embedding dimensions for this database.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_SEARCH_MIN_SIMILARITY",
        value_type: "number",
        category: "Search",
        description: "Minimum similarity threshold for vector search.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_SEARCH_BM25_ENABLED",
        value_type: "boolean",
        category: "Search",
        description: "Master switch for BM25 fulltext on this database. Default: false.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_SEARCH_BM25_WARMING",
        value_type: "enum:startup,lazy",
        category: "Search",
        description: "BM25 warming mode for this database.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_SEARCH_VECTOR_ENABLED",
        value_type: "boolean",
        category: "Search",
        description: "Master switch for vector search on this database. Default: false.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_SEARCH_VECTOR_WARMING",
        value_type: "enum:startup,lazy",
        category: "Search",
        description: "Vector warming mode for this database.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_SEARCH_RERANK_ENABLED",
        value_type: "boolean",
        category: "Search",
        description: "Reranking switch. Deferred from MVP and default false.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_AUTO_LINKS_ENABLED",
        value_type: "boolean",
        category: "Features",
        description: "Automatic link creation on this database. Default: false.",
    },
    PerDatabaseConfigKey {
        key: "COPPERDB_AUTO_TLP_ENABLED",
        value_type: "boolean",
        category: "Features",
        description: "Automatic topology/TLP workflows on this database. Default: false.",
    },
];

pub fn allowed_per_database_config_keys() -> &'static [PerDatabaseConfigKey] {
    &PER_DATABASE_CONFIG_KEYS
}

pub fn is_allowed_per_database_config_key(key: &str) -> bool {
    allowed_per_database_config_keys()
        .iter()
        .any(|meta| meta.key.eq_ignore_ascii_case(key))
}

pub fn validate_per_database_overrides(
    overrides: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    for (key, value) in overrides {
        let Some(meta) = allowed_per_database_config_keys()
            .iter()
            .find(|meta| meta.key.eq_ignore_ascii_case(key))
        else {
            return Err(ConfigError::InvalidPerDatabaseOverrideKey(key.clone()));
        };
        if !value_matches_type(meta.value_type, value) {
            return Err(ConfigError::InvalidPerDatabaseOverrideValue {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

pub fn resolve_per_database_config(
    global: &Config,
    overrides: &BTreeMap<String, String>,
) -> Result<EffectiveDatabaseConfig, ConfigError> {
    validate_per_database_overrides(overrides)?;

    let mut resolved = EffectiveDatabaseConfig {
        embedding_enabled: global.embedding.enabled,
        embedding_provider: global.embedding.provider.clone(),
        embedding_model: global.embedding.model.clone(),
        embedding_api_url: global.embedding.api_url.clone(),
        embedding_dimensions: normalized_embedding_dimensions(global.embedding.dimensions),
        search_min_similarity: global.search.min_similarity,
        bm25_enabled: global.search.bm25_enabled,
        bm25_warming: normalize_warming(&global.search.bm25_warming),
        vector_enabled: global.search.vector_enabled,
        vector_warming: normalize_warming(&global.search.vector_warming),
        rerank_enabled: global.search.rerank_enabled,
        auto_links_enabled: global.features.auto_links_enabled,
        auto_tlp_enabled: global.features.auto_tlp_enabled,
        effective: effective_values_from_global(global),
    };

    for (key, value) in overrides {
        apply_per_database_override(&mut resolved, key, value);
    }
    for (key, value) in &global.cli_overrides {
        if is_allowed_per_database_config_key(key) && value_matches_known_type(key, value) {
            apply_per_database_override(&mut resolved, key, value);
        }
    }
    Ok(resolved)
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
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_ADDRESS"), |value| {
        config.server.grpc_address = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_AUTH_TOKEN"), |value| {
        config.server.grpc_auth_token = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_AUTH_TOKEN_KMS_CIPHERTEXT"), |value| {
        config.server.grpc_auth_token_kms_ciphertext = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_AUTH_TOKEN_KMS_KEY_URI"), |value| {
        config.server.grpc_auth_token_kms_key_uri = Some(value)
    });
    set_if_present(parse_env_bool(env, "COPPERDB_GRPC_TLS_ENABLED"), |value| {
        config.server.grpc_tls_enabled = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_CERT"), |value| {
        config.server.grpc_tls_cert = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_KEY"), |value| {
        config.server.grpc_tls_key = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_CA_CERT"), |value| {
        config.server.grpc_tls_ca_cert = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_DOMAIN_NAME"), |value| {
        config.server.grpc_tls_domain_name = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_CLIENT_CERT"), |value| {
        config.server.grpc_tls_client_cert = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_CLIENT_KEY"), |value| {
        config.server.grpc_tls_client_key = Some(value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_CLIENT_AUTH_CA_CERT"), |value| {
        config.server.grpc_tls_client_auth_ca_cert = Some(value)
    });
    set_if_present(parse_env_bool(env, "COPPERDB_GRPC_TLS_CLIENT_AUTH_OPTIONAL"), |value| {
        config.server.grpc_tls_client_auth_optional = value
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
    set_if_present(parse_env_u16(env, "COPPERDB_GRPC_PORT"), |value| {
        config.server.grpc_port = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_HTTP_ENABLED"), |value| {
        config.server.http_enabled = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_BOLT_ENABLED"), |value| {
        config.server.bolt_enabled = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_GRPC_ENABLED"), |value| {
        config.server.grpc_enabled = value
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
    set_if_present(parse_env_bool(env, "COPPERDB_EMBEDDING_ENABLED"), |value| {
        config.embedding.enabled = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_EMBEDDING_PROVIDER"), |value| {
        config.embedding.provider = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_EMBEDDING_MODEL"), |value| {
        config.embedding.model = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_EMBEDDING_API_URL"), |value| {
        config.embedding.api_url = Some(value)
    });
    set_if_present(parse_env_usize(env, "COPPERDB_EMBEDDING_DIMENSIONS"), |value| {
        config.embedding.dimensions = value;
        config.vectorspace.dimensions = value;
    });
    set_if_present(parse_env_f64(env, "COPPERDB_SEARCH_MIN_SIMILARITY"), |value| {
        config.search.min_similarity = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_SEARCH_BM25_ENABLED"), |value| {
        config.search.bm25_enabled = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_SEARCH_BM25_WARMING"), |value| {
        config.search.bm25_warming = normalize_warming(&value)
    });
    set_if_present(parse_env_bool(env, "COPPERDB_SEARCH_VECTOR_ENABLED"), |value| {
        config.search.vector_enabled = value
    });
    set_if_present(env_nonempty(env, "COPPERDB_SEARCH_VECTOR_WARMING"), |value| {
        config.search.vector_warming = normalize_warming(&value)
    });
    set_if_present(parse_env_bool(env, "COPPERDB_SEARCH_RERANK_ENABLED"), |value| {
        config.search.rerank_enabled = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_AUTO_LINKS_ENABLED"), |value| {
        config.features.auto_links_enabled = value
    });
    set_if_present(parse_env_bool(env, "COPPERDB_AUTO_TLP_ENABLED"), |value| {
        config.features.auto_tlp_enabled = value
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
    if let Some(address) = &overrides.grpc_address {
        config.server.grpc_address = Some(address.clone());
    }
    if let Some(token) = &overrides.grpc_auth_token {
        config.server.grpc_auth_token = Some(token.clone());
    }
    if let Some(ciphertext) = &overrides.grpc_auth_token_kms_ciphertext {
        config.server.grpc_auth_token_kms_ciphertext = Some(ciphertext.clone());
    }
    if let Some(key_uri) = &overrides.grpc_auth_token_kms_key_uri {
        config.server.grpc_auth_token_kms_key_uri = Some(key_uri.clone());
    }
    if let Some(enabled) = overrides.grpc_tls_enabled {
        config.server.grpc_tls_enabled = enabled;
    }
    if let Some(cert) = &overrides.grpc_tls_cert {
        config.server.grpc_tls_cert = Some(cert.clone());
    }
    if let Some(key) = &overrides.grpc_tls_key {
        config.server.grpc_tls_key = Some(key.clone());
    }
    if let Some(ca_cert) = &overrides.grpc_tls_ca_cert {
        config.server.grpc_tls_ca_cert = Some(ca_cert.clone());
    }
    if let Some(domain_name) = &overrides.grpc_tls_domain_name {
        config.server.grpc_tls_domain_name = Some(domain_name.clone());
    }
    if let Some(cert) = &overrides.grpc_tls_client_cert {
        config.server.grpc_tls_client_cert = Some(cert.clone());
    }
    if let Some(key) = &overrides.grpc_tls_client_key {
        config.server.grpc_tls_client_key = Some(key.clone());
    }
    if let Some(ca_cert) = &overrides.grpc_tls_client_auth_ca_cert {
        config.server.grpc_tls_client_auth_ca_cert = Some(ca_cert.clone());
    }
    if let Some(optional) = overrides.grpc_tls_client_auth_optional {
        config.server.grpc_tls_client_auth_optional = optional;
    }
    if let Some(port) = overrides.http_port {
        config.server.http_port = port;
    }
    if let Some(port) = overrides.bolt_port {
        config.server.bolt_port = port;
    }
    if let Some(port) = overrides.grpc_port {
        config.server.grpc_port = port;
    }
    if let Some(enabled) = overrides.http_enabled {
        config.server.http_enabled = enabled;
    }
    if let Some(enabled) = overrides.bolt_enabled {
        config.server.bolt_enabled = enabled;
    }
    if let Some(enabled) = overrides.grpc_enabled {
        config.server.grpc_enabled = enabled;
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

fn parse_env_usize(env: &BTreeMap<String, String>, key: &str) -> Option<usize> {
    env.get(key)?.parse().ok()
}

fn parse_env_f64(env: &BTreeMap<String, String>, key: &str) -> Option<f64> {
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

fn normalize_warming(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "startup" => "startup".into(),
        _ => "lazy".into(),
    }
}

fn normalized_embedding_dimensions(value: usize) -> usize {
    if value == 0 {
        VectorSpaceConfig::default().dimensions
    } else {
        value
    }
}

fn parse_bool_override(value: &str, fallback: bool) -> bool {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => true,
        "0" | "false" | "no" => false,
        _ => fallback,
    }
}

fn value_matches_type(expected: &str, value: &str) -> bool {
    if let Some(values) = expected.strip_prefix("enum:") {
        return values
            .split(',')
            .any(|candidate| candidate.eq_ignore_ascii_case(value.trim()));
    }
    match expected {
        "boolean" => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "0" | "true" | "false" | "yes" | "no"
        ),
        "number" => value.parse::<f64>().is_ok(),
        "string" => true,
        _ => false,
    }
}

fn value_matches_known_type(key: &str, value: &str) -> bool {
    allowed_per_database_config_keys()
        .iter()
        .find(|meta| meta.key.eq_ignore_ascii_case(key))
        .map(|meta| value_matches_type(meta.value_type, value))
        .unwrap_or(false)
}

fn effective_values_from_global(global: &Config) -> BTreeMap<String, String> {
    let mut effective = BTreeMap::new();
    effective.insert(
        "COPPERDB_EMBEDDING_ENABLED".into(),
        global.embedding.enabled.to_string(),
    );
    effective.insert(
        "COPPERDB_EMBEDDING_PROVIDER".into(),
        global.embedding.provider.clone(),
    );
    effective.insert(
        "COPPERDB_EMBEDDING_MODEL".into(),
        global.embedding.model.clone(),
    );
    effective.insert(
        "COPPERDB_EMBEDDING_API_URL".into(),
        global.embedding.api_url.clone().unwrap_or_default(),
    );
    effective.insert(
        "COPPERDB_EMBEDDING_DIMENSIONS".into(),
        normalized_embedding_dimensions(global.embedding.dimensions).to_string(),
    );
    effective.insert(
        "COPPERDB_SEARCH_MIN_SIMILARITY".into(),
        global.search.min_similarity.to_string(),
    );
    effective.insert(
        "COPPERDB_SEARCH_BM25_ENABLED".into(),
        global.search.bm25_enabled.to_string(),
    );
    effective.insert(
        "COPPERDB_SEARCH_BM25_WARMING".into(),
        normalize_warming(&global.search.bm25_warming),
    );
    effective.insert(
        "COPPERDB_SEARCH_VECTOR_ENABLED".into(),
        global.search.vector_enabled.to_string(),
    );
    effective.insert(
        "COPPERDB_SEARCH_VECTOR_WARMING".into(),
        normalize_warming(&global.search.vector_warming),
    );
    effective.insert(
        "COPPERDB_SEARCH_RERANK_ENABLED".into(),
        global.search.rerank_enabled.to_string(),
    );
    effective.insert(
        "COPPERDB_AUTO_LINKS_ENABLED".into(),
        global.features.auto_links_enabled.to_string(),
    );
    effective.insert(
        "COPPERDB_AUTO_TLP_ENABLED".into(),
        global.features.auto_tlp_enabled.to_string(),
    );
    effective
}

fn apply_per_database_override(
    resolved: &mut EffectiveDatabaseConfig,
    key: &str,
    value: &str,
) {
    match key.to_ascii_uppercase().as_str() {
        "COPPERDB_EMBEDDING_ENABLED" => {
            resolved.embedding_enabled = parse_bool_override(value, resolved.embedding_enabled)
        }
        "COPPERDB_EMBEDDING_PROVIDER" => resolved.embedding_provider = value.to_owned(),
        "COPPERDB_EMBEDDING_MODEL" => resolved.embedding_model = value.to_owned(),
        "COPPERDB_EMBEDDING_API_URL" => {
            resolved.embedding_api_url = if value.trim().is_empty() {
                None
            } else {
                Some(value.to_owned())
            }
        }
        "COPPERDB_EMBEDDING_DIMENSIONS" => {
            if let Ok(parsed) = value.parse::<usize>() {
                resolved.embedding_dimensions = normalized_embedding_dimensions(parsed);
            }
        }
        "COPPERDB_SEARCH_MIN_SIMILARITY" => {
            if let Ok(parsed) = value.parse::<f64>() {
                resolved.search_min_similarity = parsed;
            }
        }
        "COPPERDB_SEARCH_BM25_ENABLED" => {
            resolved.bm25_enabled = parse_bool_override(value, resolved.bm25_enabled)
        }
        "COPPERDB_SEARCH_BM25_WARMING" => resolved.bm25_warming = normalize_warming(value),
        "COPPERDB_SEARCH_VECTOR_ENABLED" => {
            resolved.vector_enabled = parse_bool_override(value, resolved.vector_enabled)
        }
        "COPPERDB_SEARCH_VECTOR_WARMING" => {
            resolved.vector_warming = normalize_warming(value)
        }
        "COPPERDB_SEARCH_RERANK_ENABLED" => {
            resolved.rerank_enabled = parse_bool_override(value, resolved.rerank_enabled)
        }
        "COPPERDB_AUTO_LINKS_ENABLED" => {
            resolved.auto_links_enabled = parse_bool_override(value, resolved.auto_links_enabled)
        }
        "COPPERDB_AUTO_TLP_ENABLED" => {
            resolved.auto_tlp_enabled = parse_bool_override(value, resolved.auto_tlp_enabled)
        }
        _ => {}
    }
    resolved.effective.insert(key.to_ascii_uppercase(), value.to_owned());
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
        assert!(!cfg.embedding.enabled);
        assert!(!cfg.search.bm25_enabled);
        assert!(!cfg.search.vector_enabled);
        assert!(!cfg.features.auto_links_enabled);
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
        assert_eq!(listeners.grpc_address, "0.0.0.0:50051");
        assert!(listeners.http_enabled);
        assert!(listeners.bolt_enabled);
        assert!(!listeners.grpc_enabled);
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
        env.insert("COPPERDB_GRPC_PORT".to_string(), "9101".to_string());
        env.insert(
            "COPPERDB_GRPC_AUTH_TOKEN".to_string(),
            "env-shared-secret".to_string(),
        );
        env.insert("COPPERDB_GRPC_TLS_ENABLED".to_string(), "true".to_string());
        env.insert(
            "COPPERDB_GRPC_TLS_DOMAIN_NAME".to_string(),
            "env.mesh.local".to_string(),
        );
        env.insert("COPPERDB_HEADLESS".to_string(), "true".to_string());

        let cli = ConfigOverrides {
            bolt_port: Some(9100),
            grpc_auth_token: Some("cli-shared-secret".into()),
            grpc_tls_domain_name: Some("cli.mesh.local".into()),
            grpc_tls_client_auth_optional: Some(true),
            grpc_enabled: Some(true),
            headless: Some(false),
            ..Default::default()
        };
        let cfg = load_with_precedence_from(None, &env, temp.path(), &cli).unwrap();
        assert_eq!(cfg.server.address, "127.0.0.1");
        assert_eq!(cfg.server.http_port, 8100);
        assert_eq!(cfg.server.bolt_port, 9100);
        assert_eq!(cfg.server.grpc_port, 9101);
        assert_eq!(cfg.server.grpc_auth_token.as_deref(), Some("cli-shared-secret"));
        assert!(cfg.server.grpc_tls_enabled);
        assert_eq!(
            cfg.server.grpc_tls_domain_name.as_deref(),
            Some("cli.mesh.local")
        );
        assert!(cfg.server.grpc_tls_client_auth_optional);
        assert!(cfg.server.grpc_enabled);
        assert!(!cfg.server.headless);
    }

    #[test]
    fn env_overrides_apply_default_off_search_and_embedding_settings() {
        let mut cfg = Config::default();
        let mut env = BTreeMap::new();
        env.insert("COPPERDB_EMBEDDING_ENABLED".into(), "true".into());
        env.insert("COPPERDB_EMBEDDING_DIMENSIONS".into(), "1024".into());
        env.insert("COPPERDB_SEARCH_BM25_ENABLED".into(), "true".into());
        env.insert("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into());
        env.insert("COPPERDB_AUTO_LINKS_ENABLED".into(), "true".into());

        apply_env_overrides_from(&mut cfg, &env);

        assert!(cfg.embedding.enabled);
        assert_eq!(cfg.embedding.dimensions, 1024);
        assert_eq!(cfg.vectorspace.dimensions, 1024);
        assert!(cfg.search.bm25_enabled);
        assert!(cfg.search.vector_enabled);
        assert!(cfg.features.auto_links_enabled);
    }

    #[test]
    fn resolve_per_database_config_applies_overrides_then_cli() {
        let mut cfg = Config::default();
        cfg.cli_overrides.insert(
            "COPPERDB_SEARCH_VECTOR_ENABLED".into(),
            "false".into(),
        );

        let overrides = BTreeMap::from([
            ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
            ("COPPERDB_SEARCH_VECTOR_WARMING".into(), "startup".into()),
            ("COPPERDB_EMBEDDING_DIMENSIONS".into(), "2048".into()),
        ]);

        let resolved = resolve_per_database_config(&cfg, &overrides).unwrap();

        assert!(!resolved.vector_enabled);
        assert_eq!(resolved.vector_warming, "startup");
        assert_eq!(resolved.embedding_dimensions, 2048);
        assert_eq!(
            resolved
                .effective
                .get("COPPERDB_SEARCH_VECTOR_ENABLED")
                .unwrap(),
            "false"
        );
    }

    #[test]
    fn validate_per_database_overrides_rejects_unknown_keys() {
        let overrides = BTreeMap::from([("COPPERDB_UNKNOWN".into(), "true".into())]);
        assert!(matches!(
            validate_per_database_overrides(&overrides),
            Err(ConfigError::InvalidPerDatabaseOverrideKey(_))
        ));
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
