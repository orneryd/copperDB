//! Configuration loading and management.
//!
//! Equivalent to Go's `pkg/config` in NornicDB.
//! Supports YAML/TOML configuration files, environment variable overrides,
//! and a strongly-typed configuration struct for the database engine.

use copperdb_plugin::PackageCapability;
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use rustls::sign::CertifiedKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;
use x509_parser::pem::Pem;

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
    #[error("invalid certificate at {path}: {message}")]
    InvalidCertificate { path: String, message: String },
    #[error("invalid private key at {path}: {message}")]
    InvalidPrivateKey { path: String, message: String },
    #[error("invalid TLS identity for {cert_path} and {key_path}: {message}")]
    InvalidTlsIdentity {
        cert_path: String,
        key_path: String,
        message: String,
    },
}

pub const ENV_CONFIG_PATH: &str = "COPPERDB_CONFIG";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigOverrides {
    pub address: Option<String>,
    pub http_address: Option<String>,
    pub bolt_address: Option<String>,
    pub grpc_address: Option<String>,
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
    pub localization: LocalizationConfig,
    pub bolt: BoltConfig,
    pub auth: AuthConfig,
    pub replication: ReplicationConfig,
    pub encryption: EncryptionConfig,
    pub embedding: EmbeddingConfig,
    pub search: SearchConfig,
    pub databases: BTreeMap<String, BTreeMap<String, String>>,
    pub packages: PackageConfig,
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
            if self
                .server
                .grpc_tls_cert
                .as_deref()
                .unwrap_or_default()
                .is_empty()
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_cert must be set when gRPC TLS is enabled".into(),
                ));
            }
            if self
                .server
                .grpc_tls_key
                .as_deref()
                .unwrap_or_default()
                .is_empty()
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_key must be set when gRPC TLS is enabled".into(),
                ));
            }
            if self.server.grpc_tls_client_cert.is_some()
                ^ self.server.grpc_tls_client_key.is_some()
            {
                return Err(ConfigError::MissingField(
                    "server.grpc_tls_client_cert and server.grpc_tls_client_key must be set together"
                        .into(),
                ));
            }
            if self.server.grpc_tls_client_auth_ca_cert.is_some()
                && self.server.grpc_tls_client_auth_optional
                && self
                    .server
                    .grpc_tls_client_auth_ca_cert
                    .as_deref()
                    .unwrap_or_default()
                    .is_empty()
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
            validate_certificate_bundle(
                self.server.grpc_tls_cert.as_deref().unwrap_or_default(),
                "server.grpc_tls_cert",
            )?;
            validate_certificate_key_pair(
                self.server.grpc_tls_cert.as_deref().unwrap_or_default(),
                self.server.grpc_tls_key.as_deref().unwrap_or_default(),
                "server.grpc_tls_cert",
                "server.grpc_tls_key",
            )?;
            if let Some(path) = self.server.grpc_tls_ca_cert.as_deref() {
                validate_certificate_bundle(path, "server.grpc_tls_ca_cert")?;
            }
            if let Some(path) = self.server.grpc_tls_client_cert.as_deref() {
                validate_certificate_bundle(path, "server.grpc_tls_client_cert")?;
            }
            if let (Some(cert_path), Some(key_path)) = (
                self.server.grpc_tls_client_cert.as_deref(),
                self.server.grpc_tls_client_key.as_deref(),
            ) {
                validate_certificate_key_pair(
                    cert_path,
                    key_path,
                    "server.grpc_tls_client_cert",
                    "server.grpc_tls_client_key",
                )?;
            }
            if let Some(path) = self.server.grpc_tls_client_auth_ca_cert.as_deref() {
                validate_certificate_bundle(path, "server.grpc_tls_client_auth_ca_cert")?;
            }
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

fn validate_certificate_bundle(path: &str, label: &str) -> Result<(), ConfigError> {
    for cert in load_certificate_chain(path, label)? {
        let cert = x509_parser::parse_x509_certificate(cert.as_ref())
            .map_err(|error| ConfigError::InvalidCertificate {
                path: label.into(),
                message: error.to_string(),
            })?
            .1;
        if !cert.validity().is_valid() {
            return Err(ConfigError::InvalidCertificate {
                path: label.into(),
                message: format!(
                    "certificate validity window is not active (not_before={}, not_after={})",
                    cert.validity().not_before,
                    cert.validity().not_after
                ),
            });
        }
    }
    Ok(())
}

fn validate_certificate_key_pair(
    cert_path: &str,
    key_path: &str,
    cert_label: &str,
    key_label: &str,
) -> Result<(), ConfigError> {
    let cert_chain = load_certificate_chain(cert_path, cert_label)?;
    let private_key = load_private_key(key_path, key_label)?;
    let provider = rustls::crypto::ring::default_provider();
    CertifiedKey::from_der(cert_chain, private_key, &provider).map_err(|error| {
        ConfigError::InvalidTlsIdentity {
            cert_path: cert_label.into(),
            key_path: key_label.into(),
            message: error.to_string(),
        }
    })?;
    Ok(())
}

fn load_certificate_chain(
    path: &str,
    label: &str,
) -> Result<Vec<CertificateDer<'static>>, ConfigError> {
    let pem = std::fs::read(path).map_err(ConfigError::Io)?;
    let mut certs = Vec::new();
    for block in Pem::iter_from_buffer(&pem) {
        let block = block.map_err(|error| ConfigError::InvalidCertificate {
            path: label.into(),
            message: error.to_string(),
        })?;
        if block.label == "CERTIFICATE" {
            certs.push(CertificateDer::from(block.contents));
        }
    }
    if certs.is_empty() {
        return Err(ConfigError::InvalidCertificate {
            path: label.into(),
            message: "no CERTIFICATE PEM blocks found".into(),
        });
    }
    Ok(certs)
}

fn load_private_key(path: &str, label: &str) -> Result<PrivateKeyDer<'static>, ConfigError> {
    let pem = std::fs::read(path).map_err(ConfigError::Io)?;
    for block in Pem::iter_from_buffer(&pem) {
        let block = block.map_err(|error| ConfigError::InvalidPrivateKey {
            path: label.into(),
            message: error.to_string(),
        })?;
        let key = match block.label.as_str() {
            "PRIVATE KEY" => Some(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                block.contents,
            ))),
            "RSA PRIVATE KEY" => Some(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(
                block.contents,
            ))),
            "EC PRIVATE KEY" => Some(PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(block.contents))),
            _ => None,
        };
        if let Some(key) = key {
            return Ok(key);
        }
    }
    Err(ConfigError::InvalidPrivateKey {
        path: label.into(),
        message: "no supported PRIVATE KEY PEM blocks found".into(),
    })
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            server: ServerConfig::default(),
            localization: LocalizationConfig::default(),
            bolt: BoltConfig::default(),
            auth: AuthConfig::default(),
            replication: ReplicationConfig::default(),
            encryption: EncryptionConfig::default(),
            embedding: EmbeddingConfig::default(),
            search: SearchConfig::default(),
            databases: BTreeMap::new(),
            packages: PackageConfig::default(),
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
pub struct LocalizationConfig {
    /// Process language or `auto` to use the operating-system locale.
    pub language: String,
}

impl Default for LocalizationConfig {
    fn default() -> Self {
        Self {
            language: "auto".into(),
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
            address: "127.0.0.1".into(),
            http_address: None,
            bolt_address: None,
            grpc_address: None,
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
    /// Path to the fjall database directory.
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
    /// Require authentication for protected ingress routes.
    pub enabled: bool,
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
            enabled: true,
            jwt_secret: default_jwt_secret(),
            token_expiry_secs: 3600,
            allow_anonymous: false,
        }
    }
}

/// Generate a default JWT secret. Mirrors NornicDB's `generateDefaultSecret()`:
/// a clearly-unsafe placeholder so operators know to set a real secret.
fn default_jwt_secret() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("CHANGE_ME_IN_PRODUCTION_{nanos:x}")
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
    /// Provider loading policy: `startup` or `lazy`.
    pub warming: String,
    /// Interval between local provider warmup embeddings; zero disables periodic warmup.
    pub warmup_interval_ms: u64,
    /// Maximum cached embeddings per database; zero disables the runtime cache.
    pub cache_capacity: usize,
    /// Maximum concurrent embedding workers per enabled database.
    pub workers: usize,
    /// Maximum failed attempts before moving work to the embedding dead letter queue.
    pub max_attempts: u32,
    /// Delay before retrying a failed embedding operation.
    pub retry_backoff_ms: u64,
    /// Maximum time to wait for embedding workers during shutdown.
    pub shutdown_timeout_ms: u64,
    /// Property allowlist used to build canonical embedding text; empty includes all.
    pub properties_include: Vec<String>,
    /// Property denylist used to build canonical embedding text.
    pub properties_exclude: Vec<String>,
    /// Whether canonical embedding text includes node labels.
    pub include_labels: bool,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: String::new(),
            model: String::new(),
            api_url: None,
            dimensions: VectorSpaceConfig::default().dimensions,
            warming: "startup".into(),
            warmup_interval_ms: 0,
            cache_capacity: 0,
            workers: 1,
            max_attempts: 3,
            retry_backoff_ms: 250,
            shutdown_timeout_ms: 1_000,
            properties_include: Vec::new(),
            properties_exclude: Vec::new(),
            include_labels: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    /// Minimum similarity threshold for vector search.
    pub min_similarity: f64,
    /// Full-text search engine implementation (`v1` or `v2`).
    pub bm25_engine: String,
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
    pub rerank_provider: String,
    pub rerank_model: String,
    pub rerank_api_url: Option<String>,
    pub rerank_api_key: String,
    /// Default query cache capacity inherited by each database.
    pub query_cache_max_entries: usize,
    /// Default query cache lifetime inherited by each database, in milliseconds.
    pub query_cache_ttl_ms: u64,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            min_similarity: 0.0,
            bm25_engine: "v2".into(),
            bm25_enabled: false,
            bm25_warming: "lazy".into(),
            vector_enabled: false,
            vector_warming: "lazy".into(),
            rerank_enabled: false,
            rerank_provider: String::new(),
            rerank_model: String::new(),
            rerank_api_url: None,
            rerank_api_key: String::new(),
            query_cache_max_entries: 1_000,
            query_cache_ttl_ms: 300_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PackageConfig {
    /// Statically linked package IDs to load.
    pub enabled: Vec<String>,
    /// Enabled package IDs whose startup failure must fail the process.
    pub required: Vec<String>,
    /// Secret-free package configuration keyed by package ID.
    pub configuration: BTreeMap<String, serde_json::Value>,
    /// Explicit typed capability grants keyed by package ID.
    pub grants: BTreeMap<String, Vec<PackageCapability>>,
    /// Bound applied independently to every package lifecycle hook.
    pub lifecycle_timeout_ms: u64,
}

impl Default for PackageConfig {
    fn default() -> Self {
        Self {
            enabled: Vec::new(),
            required: Vec::new(),
            configuration: BTreeMap::new(),
            grants: BTreeMap::new(),
            lifecycle_timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FeatureConfig {
    /// Automatic link creation.
    pub auto_links_enabled: bool,
    /// Automatic topology/TLP workflows.
    pub auto_tlp_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PerDatabaseConfigKey {
    pub key: &'static str,
    pub environment_variable: Option<&'static str>,
    pub value_type: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub default_value: &'static str,
    pub scope: &'static str,
    pub dynamic: bool,
    pub restart_level: &'static str,
    pub hot_reload: Option<&'static str>,
    pub zero_semantics: Option<&'static str>,
    pub deprecated: bool,
    pub redacted: bool,
    pub valid_values: &'static [&'static str],
}

const fn setting(
    key: &'static str,
    environment_variable: Option<&'static str>,
    value_type: &'static str,
    category: &'static str,
) -> PerDatabaseConfigKey {
    PerDatabaseConfigKey {
        key,
        environment_variable,
        value_type,
        category,
        description: "",
        default_value: "",
        scope: "database",
        dynamic: false,
        restart_level: "process",
        hot_reload: None,
        zero_semantics: None,
        deprecated: false,
        redacted: false,
        valid_values: &[],
    }
}

macro_rules! setting {
    ($key:literal, $environment_variable:expr, $value_type:literal, $category:literal) => {
        setting($key, $environment_variable, $value_type, $category)
    };
    ($key:literal, $environment_variable:expr, $value_type:literal, $category:literal, $($field:ident = $value:expr),+ $(,)?) => {{
        let mut definition = setting($key, $environment_variable, $value_type, $category);
        $(definition.$field = $value;)+
        definition
    }};
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EffectiveDatabaseConfig {
    pub embedding_enabled: bool,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding_api_url: Option<String>,
    pub embedding_dimensions: usize,
    pub embedding_warming: String,
    pub embedding_warmup_interval_ms: u64,
    pub embedding_cache_capacity: usize,
    pub embedding_workers: usize,
    pub embedding_max_attempts: u32,
    pub embedding_retry_backoff_ms: u64,
    pub embedding_shutdown_timeout_ms: u64,
    pub embedding_properties_include: Vec<String>,
    pub embedding_properties_exclude: Vec<String>,
    pub embedding_include_labels: bool,
    pub search_min_similarity: f64,
    pub bm25_engine: String,
    pub bm25_enabled: bool,
    pub bm25_warming: String,
    pub vector_enabled: bool,
    pub vector_warming: String,
    pub rerank_enabled: bool,
    pub rerank_provider: String,
    pub rerank_model: String,
    pub rerank_api_url: Option<String>,
    pub rerank_api_key: String,
    pub auto_links_enabled: bool,
    pub auto_tlp_enabled: bool,
    pub query_cache_max_entries: usize,
    pub query_cache_ttl_ms: u64,
    pub search_result_cache_max_entries: usize,
    pub search_result_cache_ttl_ms: u64,
    pub bm25_memory_max_bytes: i64,
    pub vector_memory_max_bytes: i64,
    pub metadata_memory_max_bytes: i64,
    pub bm25_storage_mode: String,
    pub vector_storage_mode: String,
    pub effective: BTreeMap<String, String>,
}

pub const SETTINGS_REGISTRY: [PerDatabaseConfigKey; 72] = [
    setting!(
        "db.copper.embedding.enabled",
        Some("COPPERDB_EMBEDDING_ENABLED"),
        "boolean",
        "Embeddings"
    ),
    setting!(
        "db.copper.embedding.provider",
        Some("COPPERDB_EMBEDDING_PROVIDER"),
        "string",
        "Embeddings",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.embedding.model",
        Some("COPPERDB_EMBEDDING_MODEL"),
        "string",
        "Embeddings",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.embedding.api.url",
        Some("COPPERDB_EMBEDDING_API_URL"),
        "string",
        "Embeddings",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.embedding.api.key",
        Some("COPPERDB_EMBEDDING_API_KEY"),
        "string",
        "Embeddings",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild"),
        redacted = true
    ),
    setting!(
        "db.copper.embedding.dimensions",
        Some("COPPERDB_EMBEDDING_DIMENSIONS"),
        "number",
        "Embeddings",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.embedding.cache.size",
        Some("COPPERDB_EMBEDDING_CACHE_SIZE"),
        "number",
        "Embeddings"
    ),
    setting!(
        "db.copper.embedding.properties.include",
        Some("COPPERDB_EMBEDDING_PROPERTIES_INCLUDE"),
        "string",
        "Embeddings"
    ),
    setting!(
        "db.copper.embedding.properties.exclude",
        Some("COPPERDB_EMBEDDING_PROPERTIES_EXCLUDE"),
        "string",
        "Embeddings"
    ),
    setting!(
        "db.copper.embedding.include.labels",
        Some("COPPERDB_EMBEDDING_INCLUDE_LABELS"),
        "boolean",
        "Embeddings"
    ),
    setting!(
        "db.copper.embedding.gpu.layers",
        Some("COPPERDB_EMBEDDING_GPU_LAYERS"),
        "number",
        "Embeddings",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.embedding.warmup.interval",
        Some("COPPERDB_EMBEDDING_WARMUP_INTERVAL"),
        "duration",
        "Embeddings"
    ),
    setting!(
        "db.copper.search.min.similarity",
        Some("COPPERDB_SEARCH_MIN_SIMILARITY"),
        "number",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.bm25.engine",
        Some("COPPERDB_SEARCH_BM25_ENGINE"),
        "string",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.bm25.enabled",
        Some("COPPERDB_SEARCH_BM25_ENABLED"),
        "boolean",
        "Search",
        description = "Master switch for BM25 fulltext search on this database. When false, no BM25 build runs and search returns no fulltext results. Default: true.",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.bm25.warming",
        Some("COPPERDB_SEARCH_BM25_WARMING"),
        "enum",
        "Search",
        description = "When BM25 is enabled, choose startup or lazy warming.",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild"),
        valid_values = &["startup", "lazy"]
    ),
    setting!(
        "db.copper.search.vector.enabled",
        Some("COPPERDB_SEARCH_VECTOR_ENABLED"),
        "boolean",
        "Search",
        description = "Master switch for vector search on this database. Default: true.",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.vector.warming",
        Some("COPPERDB_SEARCH_VECTOR_WARMING"),
        "enum",
        "Search",
        description = "When vector search is enabled, choose startup or lazy warming.",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild"),
        valid_values = &["startup", "lazy"]
    ),
    setting!(
        "db.copper.search.rerank.enabled",
        Some("COPPERDB_SEARCH_RERANK_ENABLED"),
        "boolean",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.rerank.provider",
        Some("COPPERDB_SEARCH_RERANK_PROVIDER"),
        "string",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.rerank.model",
        Some("COPPERDB_SEARCH_RERANK_MODEL"),
        "string",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.rerank.api.url",
        Some("COPPERDB_SEARCH_RERANK_API_URL"),
        "string",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild")
    ),
    setting!(
        "db.copper.search.rerank.api.key",
        Some("COPPERDB_SEARCH_RERANK_API_KEY"),
        "string",
        "Search",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-rebuild"),
        redacted = true
    ),
    setting!(
        "db.copper.search.index.persist.delay.sec",
        Some("COPPERDB_SEARCH_INDEX_PERSIST_DELAY_SEC"),
        "number",
        "Search"
    ),
    setting!(
        "db.copper.vector.ann.quality",
        Some("COPPERDB_VECTOR_ANN_QUALITY"),
        "string",
        "HNSW"
    ),
    setting!(
        "db.copper.vector.hnsw.m",
        Some("COPPERDB_VECTOR_HNSW_M"),
        "number",
        "HNSW"
    ),
    setting!(
        "db.copper.vector.hnsw.ef.construction",
        Some("COPPERDB_VECTOR_HNSW_EF_CONSTRUCTION"),
        "number",
        "HNSW"
    ),
    setting!(
        "db.copper.vector.hnsw.ef.search",
        Some("COPPERDB_VECTOR_HNSW_EF_SEARCH"),
        "number",
        "HNSW"
    ),
    setting!(
        "db.copper.vector.hnsw.metal.min.candidates",
        Some("COPPERDB_VECTOR_HNSW_METAL_MIN_CANDIDATES"),
        "number",
        "HNSW"
    ),
    setting!(
        "db.copper.vector.ivf.hnsw.enabled",
        Some("COPPERDB_VECTOR_IVF_HNSW_ENABLED"),
        "boolean",
        "IVF-HNSW"
    ),
    setting!(
        "db.copper.vector.ivf.hnsw.min.cluster.size",
        Some("COPPERDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE"),
        "number",
        "IVF-HNSW"
    ),
    setting!(
        "db.copper.vector.ivf.hnsw.max.clusters",
        Some("COPPERDB_VECTOR_IVF_HNSW_MAX_CLUSTERS"),
        "number",
        "IVF-HNSW"
    ),
    setting!(
        "db.copper.vector.cpu.brute.max.n",
        Some("COPPERDB_VECTOR_CPU_BRUTE_MAX_N"),
        "number",
        "Vector"
    ),
    setting!(
        "db.copper.vector.gpu.brute.min.n",
        Some("COPPERDB_VECTOR_GPU_BRUTE_MIN_N"),
        "number",
        "Vector"
    ),
    setting!(
        "db.copper.vector.gpu.brute.max.n",
        Some("COPPERDB_VECTOR_GPU_BRUTE_MAX_N"),
        "number",
        "Vector"
    ),
    setting!(
        "db.copper.kmeans.clustering.enabled",
        Some("COPPERDB_KMEANS_CLUSTERING_ENABLED"),
        "boolean",
        "K-means"
    ),
    setting!(
        "db.copper.kmeans.min.embeddings",
        Some("COPPERDB_KMEANS_MIN_EMBEDDINGS"),
        "number",
        "K-means"
    ),
    setting!(
        "db.copper.kmeans.cluster.interval",
        Some("COPPERDB_KMEANS_CLUSTER_INTERVAL"),
        "duration",
        "K-means"
    ),
    setting!(
        "db.copper.kmeans.num.clusters",
        Some("COPPERDB_KMEANS_NUM_CLUSTERS"),
        "number",
        "K-means"
    ),
    setting!(
        "db.copper.kmeans.max.iterations",
        Some("COPPERDB_KMEANS_MAX_ITERATIONS"),
        "number",
        "K-means"
    ),
    setting!(
        "db.copper.auto.links.enabled",
        Some("COPPERDB_AUTO_LINKS_ENABLED"),
        "boolean",
        "Auto-links"
    ),
    setting!(
        "db.copper.auto.links.threshold",
        Some("COPPERDB_AUTO_LINKS_THRESHOLD"),
        "number",
        "Auto-links"
    ),
    setting!(
        "db.copper.auto.tlp.enabled",
        Some("COPPERDB_AUTO_TLP_ENABLED"),
        "boolean",
        "Auto-TLP"
    ),
    setting!(
        "db.copper.auto.tlp.llm.qc.enabled",
        Some("COPPERDB_AUTO_TLP_LLM_QC_ENABLED"),
        "boolean",
        "Auto-TLP"
    ),
    setting!(
        "db.copper.auto.tlp.llm.augment.enabled",
        Some("COPPERDB_AUTO_TLP_LLM_AUGMENT_ENABLED"),
        "boolean",
        "Auto-TLP"
    ),
    setting!(
        "db.copper.embed.worker.num.workers",
        Some("COPPERDB_EMBED_WORKER_NUM_WORKERS"),
        "number",
        "Embed worker"
    ),
    setting!(
        "db.copper.embed.scan.interval",
        Some("COPPERDB_EMBED_SCAN_INTERVAL"),
        "duration",
        "Embed worker"
    ),
    setting!(
        "db.copper.embed.batch.delay",
        Some("COPPERDB_EMBED_BATCH_DELAY"),
        "duration",
        "Embed worker"
    ),
    setting!(
        "db.copper.embed.trigger.debounce",
        Some("COPPERDB_EMBED_TRIGGER_DEBOUNCE"),
        "duration",
        "Embed worker"
    ),
    setting!(
        "db.copper.embed.max.retries",
        Some("COPPERDB_EMBED_MAX_RETRIES"),
        "number",
        "Embed worker"
    ),
    setting!(
        "db.copper.embed.chunk.size",
        Some("COPPERDB_EMBED_CHUNK_SIZE"),
        "number",
        "Embed worker"
    ),
    setting!(
        "db.copper.embed.chunk.overlap",
        Some("COPPERDB_EMBED_CHUNK_OVERLAP"),
        "number",
        "Embed worker"
    ),
    setting!(
        "db.copper.mvcc.lifecycle.interval",
        Some("COPPERDB_MVCC_LIFECYCLE_INTERVAL"),
        "duration",
        "MVCC lifecycle"
    ),
    setting!(
        "db.copper.query_cache.max_entries",
        Some("COPPERDB_QUERY_CACHE_SIZE"),
        "number",
        "Query cache",
        description = "Maximum entries in this database's query cache. Correlates with Neo4j's server.memory.query_cache.per_db_cache_num_entries, but is independently configurable per database.",
        default_value = "1000",
        zero_semantics = Some("disabled")
    ),
    setting!(
        "db.copper.query_cache.ttl",
        Some("COPPERDB_QUERY_CACHE_TTL"),
        "number",
        "Query cache",
        description = "Query result cache TTL in milliseconds for this database.",
        default_value = "300000"
    ),
    setting!(
        "db.copper.query_plan_cache.max_entries",
        None,
        "number",
        "Query cache",
        default_value = "500",
        zero_semantics = Some("disabled")
    ),
    setting!(
        "db.copper.fabric_plan_cache.max_entries",
        None,
        "number",
        "Query cache",
        default_value = "500",
        zero_semantics = Some("disabled")
    ),
    setting!(
        "db.copper.query_analysis_cache.max_entries",
        None,
        "number",
        "Query cache",
        default_value = "1000",
        zero_semantics = Some("disabled")
    ),
    setting!(
        "db.copper.search_result_cache.max_entries",
        None,
        "number",
        "Search",
        default_value = "1000",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-cache"),
        zero_semantics = Some("disabled")
    ),
    setting!(
        "db.copper.search_result_cache.ttl",
        None,
        "duration",
        "Search",
        default_value = "5m",
        dynamic = true,
        restart_level = "none",
        hot_reload = Some("search-cache")
    ),
    setting!(
        "db.copper.query_lookup_metadata.max_entries",
        None,
        "number",
        "Query cache",
        default_value = "0",
        zero_semantics = Some("existing per-cache bounds")
    ),
    setting!(
        "db.memory.transaction.total.max",
        None,
        "bytes",
        "Transactions",
        default_value = "0",
        zero_semantics = Some("unlimited")
    ),
    setting!(
        "db.copper.memory.index.bm25.max",
        None,
        "bytes",
        "Index capacity",
        default_value = "0",
        zero_semantics = Some("unlimited")
    ),
    setting!(
        "db.copper.memory.index.vector.max",
        None,
        "bytes",
        "Index capacity",
        default_value = "0",
        zero_semantics = Some("unlimited")
    ),
    setting!(
        "db.copper.memory.index.metadata.max",
        None,
        "bytes",
        "Index capacity",
        default_value = "0",
        zero_semantics = Some("unlimited per index")
    ),
    setting!(
        "db.copper.index.bm25.storage",
        None,
        "enum",
        "Index capacity",
        default_value = "memory",
        valid_values = &["memory"]
    ),
    setting!(
        "db.copper.index.vector.storage",
        None,
        "enum",
        "Index capacity",
        default_value = "auto",
        valid_values = &["auto", "memory", "disk"]
    ),
    setting!(
        "db.copper.memory.storage.mode",
        None,
        "enum",
        "Storage",
        default_value = "default",
        scope = "physical-engine",
        valid_values = &["default", "low"]
    ),
    setting!(
        "db.copper.memory.storage.node_cache.max_entries",
        Some("COPPERDB_BADGER_NODE_CACHE_MAX_ENTRIES"),
        "number",
        "Storage",
        default_value = "10000",
        scope = "physical-engine"
    ),
    setting!(
        "db.copper.memory.storage.edge_type_cache.max_entries",
        Some("COPPERDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES"),
        "number",
        "Storage",
        default_value = "50",
        scope = "physical-engine"
    ),
    setting!(
        "db.copper.recovery.batch.max_bytes",
        None,
        "bytes",
        "Recovery",
        default_value = "0",
        scope = "physical-engine",
        zero_semantics = Some("unlimited")
    ),
    setting!(
        "db.copper.recovery.memory.max",
        None,
        "bytes",
        "Recovery",
        default_value = "0",
        scope = "physical-engine",
        zero_semantics = Some("unlimited")
    ),
];

pub fn settings() -> &'static [PerDatabaseConfigKey] {
    &SETTINGS_REGISTRY
}

pub fn allowed_per_database_config_keys() -> &'static [PerDatabaseConfigKey] {
    &SETTINGS_REGISTRY[..67]
}

pub fn canonical_setting_name(name: &str) -> String {
    let trimmed = name.trim();
    lookup_setting(trimmed)
        .map(|setting| setting.key)
        .unwrap_or(trimmed)
        .to_owned()
}

pub fn lookup_setting(name: &str) -> Option<&'static PerDatabaseConfigKey> {
    let trimmed = name.trim();
    settings()
        .iter()
        .find(|setting| setting.key == trimmed || setting.environment_variable == Some(trimmed))
}

pub fn is_allowed_per_database_config_key(key: &str) -> bool {
    lookup_setting(key).is_some_and(|setting| setting.scope != "physical-engine")
}

pub fn canonicalize_per_database_overrides(
    overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut canonical = BTreeMap::new();
    for setting in settings() {
        if let Some(environment_variable) = setting.environment_variable
            && let Some(value) = overrides.get(environment_variable)
        {
            canonical.insert(setting.key.to_owned(), value.clone());
        }
    }
    for (key, value) in overrides {
        let name = canonical_setting_name(key);
        if name == key.trim() {
            canonical.insert(name, value.clone());
        }
    }
    canonical
}

pub fn normalize_setting_value(key: &str, value: &str) -> Result<String, ConfigError> {
    let setting = lookup_setting(key)
        .ok_or_else(|| ConfigError::InvalidPerDatabaseOverrideKey(key.to_owned()))?;
    let value = value.trim();
    let normalized = if setting.value_type == "enum" {
        setting
            .valid_values
            .iter()
            .copied()
            .find(|candidate| candidate.eq_ignore_ascii_case(value))
            .map(str::to_owned)
    } else {
        match setting.value_type {
            "boolean" => match value.to_ascii_lowercase().as_str() {
                "1" | "t" | "true" => Some("true".into()),
                "0" | "f" | "false" => Some("false".into()),
                _ => None,
            },
            "number" if is_floating_point_setting(setting.key) => {
                value.parse::<f64>().ok().and_then(|parsed| {
                    (parsed.is_finite() && parsed >= 0.0).then(|| parsed.to_string())
                })
            }
            "number" => value
                .parse::<i64>()
                .ok()
                .and_then(|parsed| (parsed >= 0).then(|| parsed.to_string())),
            "duration" => normalize_duration(value),
            "bytes" => parse_byte_size(value).map(|parsed| parsed.to_string()),
            "string" => Some(value.to_owned()),
            _ => None,
        }
    };
    normalized.ok_or_else(|| ConfigError::InvalidPerDatabaseOverrideValue {
        key: setting.key.to_owned(),
        value: value.to_owned(),
    })
}

fn is_floating_point_setting(key: &str) -> bool {
    matches!(
        key,
        "db.copper.search.min.similarity" | "db.copper.auto.links.threshold"
    )
}

pub fn parse_byte_size(raw: &str) -> Option<i64> {
    let value = raw.trim().to_ascii_lowercase();
    let (digits, multiplier) = match value.as_bytes().last().copied() {
        Some(b'k') => (&value[..value.len() - 1], 1_i64 << 10),
        Some(b'm') => (&value[..value.len() - 1], 1_i64 << 20),
        Some(b'g') => (&value[..value.len() - 1], 1_i64 << 30),
        Some(b't') => (&value[..value.len() - 1], 1_i64 << 40),
        Some(b'0'..=b'9') => (value.as_str(), 1),
        _ => return None,
    };
    let parsed = digits.trim().parse::<i64>().ok()?;
    (parsed >= 0).then_some(parsed)?.checked_mul(multiplier)
}

fn normalize_duration(raw: &str) -> Option<String> {
    let nanoseconds = go_parse_duration::parse_duration(raw).ok()?;
    (nanoseconds >= 0).then(|| format_go_duration(nanoseconds))
}

fn format_go_duration(nanoseconds: i64) -> String {
    if nanoseconds == 0 {
        return "0s".to_owned();
    }
    if nanoseconds < 1_000_000_000 {
        let (unit, suffix) = if nanoseconds < 1_000 {
            (1, "ns")
        } else if nanoseconds < 1_000_000 {
            (1_000, "us")
        } else {
            (1_000_000, "ms")
        };
        return format_duration_component(nanoseconds, unit, suffix);
    }

    let hours = nanoseconds / 3_600_000_000_000;
    let after_hours = nanoseconds % 3_600_000_000_000;
    let minutes = after_hours / 60_000_000_000;
    let after_minutes = after_hours % 60_000_000_000;
    let mut formatted = String::new();
    if hours > 0 {
        formatted.push_str(&format!("{hours}h"));
    }
    if hours > 0 || minutes > 0 {
        formatted.push_str(&format!("{minutes}m"));
    }
    formatted.push_str(&format_duration_component(
        after_minutes,
        1_000_000_000,
        "s",
    ));
    formatted
}

fn format_duration_component(value: i64, unit: i64, suffix: &str) -> String {
    let whole = value / unit;
    let remainder = value % unit;
    if remainder == 0 {
        return format!("{whole}{suffix}");
    }
    let width = unit.ilog10() as usize;
    let fraction = format!("{remainder:0width$}")
        .trim_end_matches('0')
        .to_owned();
    format!("{whole}.{fraction}{suffix}")
}

pub fn validate_per_database_overrides(
    overrides: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    normalize_per_database_overrides(overrides).map(|_| ())
}

pub fn normalize_per_database_overrides(
    overrides: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    canonicalize_per_database_overrides(overrides)
        .into_iter()
        .map(|(key, value)| {
            if !is_allowed_per_database_config_key(&key) {
                return Err(ConfigError::InvalidPerDatabaseOverrideKey(key));
            }
            let normalized = normalize_setting_value(&key, &value)?;
            Ok((key, normalized))
        })
        .collect()
}

pub fn resolve_per_database_config(
    global: &Config,
    overrides: &BTreeMap<String, String>,
) -> Result<EffectiveDatabaseConfig, ConfigError> {
    let mut resolved = EffectiveDatabaseConfig {
        embedding_enabled: global.embedding.enabled,
        embedding_provider: global.embedding.provider.clone(),
        embedding_model: global.embedding.model.clone(),
        embedding_api_url: global.embedding.api_url.clone(),
        embedding_dimensions: normalized_embedding_dimensions(global.embedding.dimensions),
        embedding_warming: normalize_warming(&global.embedding.warming),
        embedding_warmup_interval_ms: global.embedding.warmup_interval_ms,
        embedding_cache_capacity: global.embedding.cache_capacity,
        embedding_workers: global.embedding.workers.max(1),
        embedding_max_attempts: global.embedding.max_attempts.max(1),
        embedding_retry_backoff_ms: global.embedding.retry_backoff_ms,
        embedding_shutdown_timeout_ms: global.embedding.shutdown_timeout_ms,
        embedding_properties_include: global.embedding.properties_include.clone(),
        embedding_properties_exclude: global.embedding.properties_exclude.clone(),
        embedding_include_labels: global.embedding.include_labels,
        search_min_similarity: global.search.min_similarity,
        bm25_engine: normalize_bm25_engine(&global.search.bm25_engine),
        bm25_enabled: global.search.bm25_enabled,
        bm25_warming: normalize_warming(&global.search.bm25_warming),
        vector_enabled: global.search.vector_enabled,
        vector_warming: normalize_warming(&global.search.vector_warming),
        rerank_enabled: global.search.rerank_enabled,
        rerank_provider: global.search.rerank_provider.clone(),
        rerank_model: global.search.rerank_model.clone(),
        rerank_api_url: global.search.rerank_api_url.clone(),
        rerank_api_key: global.search.rerank_api_key.clone(),
        auto_links_enabled: global.features.auto_links_enabled,
        auto_tlp_enabled: global.features.auto_tlp_enabled,
        query_cache_max_entries: global.search.query_cache_max_entries,
        query_cache_ttl_ms: global.search.query_cache_ttl_ms,
        search_result_cache_max_entries: 1_000,
        search_result_cache_ttl_ms: 300_000,
        bm25_memory_max_bytes: 0,
        vector_memory_max_bytes: 0,
        metadata_memory_max_bytes: 0,
        bm25_storage_mode: "memory".into(),
        vector_storage_mode: "auto".into(),
        effective: effective_values_from_global(global),
    };

    for (key, value) in canonicalize_per_database_overrides(&global.cli_overrides) {
        if !is_allowed_per_database_config_key(&key) {
            continue;
        }
        if let Ok(value) = normalize_setting_value(&key, &value) {
            apply_per_database_override(&mut resolved, &key, &value);
        }
    }
    for (key, value) in canonicalize_per_database_overrides(overrides) {
        if !is_allowed_per_database_config_key(&key) {
            continue;
        }
        if let Ok(value) = normalize_setting_value(&key, &value) {
            apply_per_database_override(&mut resolved, &key, &value);
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
            dimensions: 1024,
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
    set_if_present(
        env_nonempty(env, "COPPERDB_GRPC_TLS_DOMAIN_NAME"),
        |value| config.server.grpc_tls_domain_name = Some(value),
    );
    set_if_present(
        env_nonempty(env, "COPPERDB_GRPC_TLS_CLIENT_CERT"),
        |value| config.server.grpc_tls_client_cert = Some(value),
    );
    set_if_present(env_nonempty(env, "COPPERDB_GRPC_TLS_CLIENT_KEY"), |value| {
        config.server.grpc_tls_client_key = Some(value)
    });
    set_if_present(
        env_nonempty(env, "COPPERDB_GRPC_TLS_CLIENT_AUTH_CA_CERT"),
        |value| config.server.grpc_tls_client_auth_ca_cert = Some(value),
    );
    set_if_present(
        parse_env_bool(env, "COPPERDB_GRPC_TLS_CLIENT_AUTH_OPTIONAL"),
        |value| config.server.grpc_tls_client_auth_optional = value,
    );
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
    set_if_present(parse_env_bool(env, "COPPERDB_AUTH_ENABLED"), |value| {
        config.auth.enabled = value
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
    set_if_present(
        parse_env_usize(env, "COPPERDB_EMBEDDING_DIMENSIONS"),
        |value| {
            config.embedding.dimensions = value;
            config.vectorspace.dimensions = value;
        },
    );
    set_if_present(env_nonempty(env, "COPPERDB_EMBEDDING_WARMING"), |value| {
        config.embedding.warming = normalize_warming(&value)
    });
    set_if_present(
        parse_env_u64(env, "COPPERDB_EMBEDDING_WARMUP_INTERVAL_MS"),
        |value| config.embedding.warmup_interval_ms = value,
    );
    set_if_present(
        parse_env_usize(env, "COPPERDB_EMBEDDING_CACHE_CAPACITY"),
        |value| config.embedding.cache_capacity = value,
    );
    set_if_present(
        parse_env_usize(env, "COPPERDB_EMBEDDING_WORKERS"),
        |value| config.embedding.workers = value.max(1),
    );
    set_if_present(
        parse_env_u32(env, "COPPERDB_EMBEDDING_MAX_ATTEMPTS"),
        |value| config.embedding.max_attempts = value.max(1),
    );
    set_if_present(
        parse_env_u64(env, "COPPERDB_EMBEDDING_RETRY_BACKOFF_MS"),
        |value| config.embedding.retry_backoff_ms = value,
    );
    set_if_present(
        parse_env_u64(env, "COPPERDB_EMBEDDING_SHUTDOWN_TIMEOUT_MS"),
        |value| config.embedding.shutdown_timeout_ms = value,
    );
    set_if_present(
        env.get("COPPERDB_EMBEDDING_PROPERTIES_INCLUDE")
            .map(|value| parse_csv(value)),
        |value| config.embedding.properties_include = value,
    );
    set_if_present(
        env.get("COPPERDB_EMBEDDING_PROPERTIES_EXCLUDE")
            .map(|value| parse_csv(value)),
        |value| config.embedding.properties_exclude = value,
    );
    set_if_present(
        parse_env_bool(env, "COPPERDB_EMBEDDING_INCLUDE_LABELS"),
        |value| config.embedding.include_labels = value,
    );
    set_if_present(
        parse_env_f64(env, "COPPERDB_SEARCH_MIN_SIMILARITY"),
        |value| config.search.min_similarity = value,
    );
    set_if_present(env_nonempty(env, "COPPERDB_SEARCH_BM25_ENGINE"), |value| {
        config.search.bm25_engine = normalize_bm25_engine(&value)
    });
    set_if_present(
        parse_env_bool(env, "COPPERDB_SEARCH_BM25_ENABLED"),
        |value| config.search.bm25_enabled = value,
    );
    set_if_present(env_nonempty(env, "COPPERDB_SEARCH_BM25_WARMING"), |value| {
        config.search.bm25_warming = normalize_warming(&value)
    });
    set_if_present(
        parse_env_bool(env, "COPPERDB_SEARCH_VECTOR_ENABLED"),
        |value| config.search.vector_enabled = value,
    );
    set_if_present(
        env_nonempty(env, "COPPERDB_SEARCH_VECTOR_WARMING"),
        |value| config.search.vector_warming = normalize_warming(&value),
    );
    set_if_present(
        parse_env_bool(env, "COPPERDB_SEARCH_RERANK_ENABLED"),
        |value| config.search.rerank_enabled = value,
    );
    set_if_present(
        env_nonempty(env, "COPPERDB_SEARCH_RERANK_PROVIDER"),
        |value| config.search.rerank_provider = value,
    );
    set_if_present(env_nonempty(env, "COPPERDB_SEARCH_RERANK_MODEL"), |value| {
        config.search.rerank_model = value
    });
    set_if_present(
        env_nonempty(env, "COPPERDB_SEARCH_RERANK_API_URL"),
        |value| config.search.rerank_api_url = Some(value),
    );
    set_if_present(
        env.get("COPPERDB_SEARCH_RERANK_API_KEY").cloned(),
        |value| config.search.rerank_api_key = value,
    );
    set_if_present(parse_env_usize(env, "COPPERDB_QUERY_CACHE_SIZE"), |value| {
        config.search.query_cache_max_entries = value
    });
    set_if_present(parse_env_u64(env, "COPPERDB_QUERY_CACHE_TTL"), |value| {
        if value > 0 {
            config.search.query_cache_ttl_ms = value;
        }
    });
    set_if_present(env_nonempty(env, "COPPERDB_PACKAGES_ENABLED"), |value| {
        config.packages.enabled = split_package_ids(&value)
    });
    set_if_present(env_nonempty(env, "COPPERDB_PACKAGES_REQUIRED"), |value| {
        config.packages.required = split_package_ids(&value)
    });
    set_if_present(
        parse_env_bool(env, "COPPERDB_AUTO_LINKS_ENABLED"),
        |value| config.features.auto_links_enabled = value,
    );
    set_if_present(parse_env_bool(env, "COPPERDB_AUTO_TLP_ENABLED"), |value| {
        config.features.auto_tlp_enabled = value
    });
}

fn split_package_ids(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
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

fn parse_env_u32(env: &BTreeMap<String, String>, key: &str) -> Option<u32> {
    env.get(key)?.parse().ok()
}

fn parse_env_u64(env: &BTreeMap<String, String>, key: &str) -> Option<u64> {
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

fn normalize_bm25_engine(value: &str) -> String {
    if value.trim().eq_ignore_ascii_case("v1") {
        "v1".into()
    } else {
        "v2".into()
    }
}

fn effective_values_from_global(global: &Config) -> BTreeMap<String, String> {
    let mut effective: BTreeMap<String, String> = allowed_per_database_config_keys()
        .iter()
        .filter(|setting| !setting.default_value.is_empty())
        .map(|setting| (setting.key.to_owned(), setting.default_value.to_owned()))
        .collect();
    effective.insert(
        "db.copper.embedding.enabled".into(),
        global.embedding.enabled.to_string(),
    );
    effective.insert(
        "db.copper.embedding.provider".into(),
        global.embedding.provider.clone(),
    );
    effective.insert(
        "db.copper.embedding.model".into(),
        global.embedding.model.clone(),
    );
    effective.insert(
        "db.copper.embedding.api.url".into(),
        global.embedding.api_url.clone().unwrap_or_default(),
    );
    effective.insert(
        "db.copper.embedding.dimensions".into(),
        normalized_embedding_dimensions(global.embedding.dimensions).to_string(),
    );
    effective.insert(
        "db.copper.embedding.warmup.interval".into(),
        format_go_duration(
            i64::try_from(global.embedding.warmup_interval_ms)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000),
        ),
    );
    effective.insert(
        "db.copper.embedding.cache.size".into(),
        global.embedding.cache_capacity.to_string(),
    );
    effective.insert(
        "db.copper.embed.worker.num.workers".into(),
        global.embedding.workers.max(1).to_string(),
    );
    effective.insert(
        "db.copper.embedding.properties.include".into(),
        global.embedding.properties_include.join(","),
    );
    effective.insert(
        "db.copper.embedding.properties.exclude".into(),
        global.embedding.properties_exclude.join(","),
    );
    effective.insert(
        "db.copper.embedding.include.labels".into(),
        global.embedding.include_labels.to_string(),
    );
    effective.insert(
        "db.copper.search.min.similarity".into(),
        global.search.min_similarity.to_string(),
    );
    effective.insert(
        "db.copper.search.bm25.engine".into(),
        normalize_bm25_engine(&global.search.bm25_engine),
    );
    effective.insert(
        "db.copper.search.bm25.enabled".into(),
        global.search.bm25_enabled.to_string(),
    );
    effective.insert(
        "db.copper.search.bm25.warming".into(),
        normalize_warming(&global.search.bm25_warming),
    );
    effective.insert(
        "db.copper.search.vector.enabled".into(),
        global.search.vector_enabled.to_string(),
    );
    effective.insert(
        "db.copper.search.vector.warming".into(),
        normalize_warming(&global.search.vector_warming),
    );
    effective.insert(
        "db.copper.search.rerank.enabled".into(),
        global.search.rerank_enabled.to_string(),
    );
    effective.insert(
        "db.copper.search.rerank.provider".into(),
        global.search.rerank_provider.clone(),
    );
    effective.insert(
        "db.copper.search.rerank.model".into(),
        global.search.rerank_model.clone(),
    );
    effective.insert(
        "db.copper.search.rerank.api.url".into(),
        global.search.rerank_api_url.clone().unwrap_or_default(),
    );
    effective.insert(
        "db.copper.search.rerank.api.key".into(),
        global.search.rerank_api_key.clone(),
    );
    effective.insert(
        "db.copper.query_cache.max_entries".into(),
        global.search.query_cache_max_entries.to_string(),
    );
    effective.insert(
        "db.copper.query_cache.ttl".into(),
        global.search.query_cache_ttl_ms.to_string(),
    );
    effective.insert(
        "db.copper.auto.links.enabled".into(),
        global.features.auto_links_enabled.to_string(),
    );
    effective.insert(
        "db.copper.auto.tlp.enabled".into(),
        global.features.auto_tlp_enabled.to_string(),
    );
    effective
}

fn apply_per_database_override(resolved: &mut EffectiveDatabaseConfig, key: &str, value: &str) {
    let Some(setting) = lookup_setting(key) else {
        return;
    };
    let behavior_key = setting.environment_variable.unwrap_or(setting.key);
    match behavior_key {
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
        "COPPERDB_EMBEDDING_WARMUP_INTERVAL" => {
            if let Ok(parsed) = go_parse_duration::parse_duration(value) {
                resolved.embedding_warmup_interval_ms =
                    u64::try_from(parsed / 1_000_000).unwrap_or(0);
            }
        }
        "COPPERDB_EMBEDDING_CACHE_SIZE" => {
            if let Ok(parsed) = value.parse::<usize>() {
                resolved.embedding_cache_capacity = parsed;
            }
        }
        "COPPERDB_EMBED_WORKER_NUM_WORKERS" => {
            if let Ok(parsed) = value.parse::<usize>() {
                resolved.embedding_workers = parsed.max(1);
            }
        }
        "COPPERDB_EMBEDDING_PROPERTIES_INCLUDE" => {
            resolved.embedding_properties_include = parse_csv(value)
        }
        "COPPERDB_EMBEDDING_PROPERTIES_EXCLUDE" => {
            resolved.embedding_properties_exclude = parse_csv(value)
        }
        "COPPERDB_EMBEDDING_INCLUDE_LABELS" => {
            resolved.embedding_include_labels =
                parse_bool_override(value, resolved.embedding_include_labels)
        }
        "COPPERDB_SEARCH_MIN_SIMILARITY" => {
            if let Ok(parsed) = value.parse::<f64>() {
                resolved.search_min_similarity = parsed;
            }
        }
        "COPPERDB_SEARCH_BM25_ENGINE" => resolved.bm25_engine = normalize_bm25_engine(value),
        "COPPERDB_SEARCH_BM25_ENABLED" => {
            resolved.bm25_enabled = parse_bool_override(value, resolved.bm25_enabled)
        }
        "COPPERDB_SEARCH_BM25_WARMING" => resolved.bm25_warming = normalize_warming(value),
        "COPPERDB_SEARCH_VECTOR_ENABLED" => {
            resolved.vector_enabled = parse_bool_override(value, resolved.vector_enabled)
        }
        "COPPERDB_SEARCH_VECTOR_WARMING" => resolved.vector_warming = normalize_warming(value),
        "COPPERDB_SEARCH_RERANK_ENABLED" => {
            resolved.rerank_enabled = parse_bool_override(value, resolved.rerank_enabled)
        }
        "COPPERDB_SEARCH_RERANK_PROVIDER" => resolved.rerank_provider = value.to_owned(),
        "COPPERDB_SEARCH_RERANK_MODEL" => resolved.rerank_model = value.to_owned(),
        "COPPERDB_SEARCH_RERANK_API_URL" => {
            resolved.rerank_api_url = (!value.is_empty()).then(|| value.to_owned())
        }
        "COPPERDB_SEARCH_RERANK_API_KEY" => resolved.rerank_api_key = value.to_owned(),
        "COPPERDB_AUTO_LINKS_ENABLED" => {
            resolved.auto_links_enabled = parse_bool_override(value, resolved.auto_links_enabled)
        }
        "COPPERDB_AUTO_TLP_ENABLED" => {
            resolved.auto_tlp_enabled = parse_bool_override(value, resolved.auto_tlp_enabled)
        }
        "COPPERDB_QUERY_CACHE_SIZE" => {
            if let Ok(parsed) = value.parse::<usize>() {
                resolved.query_cache_max_entries = parsed;
            }
        }
        "COPPERDB_QUERY_CACHE_TTL" => {
            if let Ok(parsed) = value.parse::<u64>()
                && parsed > 0
            {
                resolved.query_cache_ttl_ms = parsed;
            }
        }
        "db.copper.search_result_cache.max_entries" => {
            if let Ok(parsed) = value.parse::<usize>() {
                resolved.search_result_cache_max_entries = parsed;
            }
        }
        "db.copper.search_result_cache.ttl" => {
            if let Ok(nanoseconds) = go_parse_duration::parse_duration(value) {
                resolved.search_result_cache_ttl_ms =
                    u64::try_from(nanoseconds / 1_000_000).unwrap_or(0);
            }
        }
        "db.copper.memory.index.bm25.max" => {
            if let Ok(parsed) = value.parse::<i64>() {
                resolved.bm25_memory_max_bytes = parsed;
            }
        }
        "db.copper.memory.index.vector.max" => {
            if let Ok(parsed) = value.parse::<i64>() {
                resolved.vector_memory_max_bytes = parsed;
            }
        }
        "db.copper.memory.index.metadata.max" => {
            if let Ok(parsed) = value.parse::<i64>() {
                resolved.metadata_memory_max_bytes = parsed;
            }
        }
        "db.copper.index.bm25.storage" => resolved.bm25_storage_mode = value.to_ascii_lowercase(),
        "db.copper.index.vector.storage" => {
            resolved.vector_storage_mode = value.to_ascii_lowercase()
        }
        _ => {}
    }
    resolved
        .effective
        .insert(setting.key.to_owned(), value.to_owned());
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
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
    use rcgen::{CertificateParams, KeyPair};
    use std::path::PathBuf;
    use time::{Duration, OffsetDateTime};

    fn valid_test_config() -> Config {
        let mut cfg = Config::default();
        cfg.auth.jwt_secret = "test-secret".into();
        cfg
    }

    fn write_cert_and_key(
        dir: &tempfile::TempDir,
        cert_name: &str,
        key_name: &str,
        not_before: OffsetDateTime,
        not_after: OffsetDateTime,
    ) -> (PathBuf, PathBuf) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.not_before = not_before;
        params.not_after = not_after;
        let cert = params.self_signed(&key).unwrap();
        let cert_path = dir.path().join(cert_name);
        let key_path = dir.path().join(key_name);
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.server.address, "127.0.0.1");
        assert_eq!(cfg.server.http_port, 7474);
        assert_eq!(cfg.bolt.listen_addr, "127.0.0.1:7687");
        assert_eq!(cfg.storage.path, "./data");
        assert!(!cfg.embedding.enabled);
        assert!(cfg.auth.enabled);
        assert!(!cfg.search.bm25_enabled);
        assert!(!cfg.search.vector_enabled);
        assert!(cfg.packages.enabled.is_empty());
        assert!(cfg.packages.required.is_empty());
        assert!(!cfg.features.auto_links_enabled);
    }

    #[test]
    fn test_default_jwt_secret_is_not_empty() {
        let cfg = Config::default();
        assert!(
            !cfg.auth.jwt_secret.is_empty(),
            "default JWT secret must be non-empty (mirrors NornicDB generateDefaultSecret)"
        );
        assert!(
            cfg.auth.jwt_secret.starts_with("CHANGE_ME_IN_PRODUCTION_"),
            "default secret must use the clearly-insecure prefix"
        );
    }

    #[test]
    fn test_validate_passes_with_default_jwt_secret() {
        let cfg = Config::default();
        // Now that JWT has a generated default, validation should succeed
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_expired_grpc_tls_certificate() {
        let temp = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_cert_and_key(
            &temp,
            "expired-cert.pem",
            "server.key",
            OffsetDateTime::from_unix_timestamp(1_000).unwrap(),
            OffsetDateTime::from_unix_timestamp(2_000).unwrap(),
        );

        let mut cfg = valid_test_config();
        cfg.server.grpc_tls_enabled = true;
        cfg.server.grpc_tls_cert = Some(cert_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_key = Some(key_path.to_string_lossy().into_owned());

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidCertificate { .. })
        ));
    }

    #[test]
    fn validate_rejects_future_grpc_client_certificate() {
        let temp = tempfile::tempdir().unwrap();
        let now = OffsetDateTime::now_utc();
        let (server_cert_path, key_path) = write_cert_and_key(
            &temp,
            "server-cert.pem",
            "server.key",
            now - Duration::days(1),
            now + Duration::days(30),
        );
        let (client_cert_path, client_key_path) = write_cert_and_key(
            &temp,
            "future-client-cert.pem",
            "client.key",
            now + Duration::days(2),
            now + Duration::days(30),
        );

        let mut cfg = valid_test_config();
        cfg.server.grpc_tls_enabled = true;
        cfg.server.grpc_tls_cert = Some(server_cert_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_key = Some(key_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_client_cert = Some(client_cert_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_client_key = Some(client_key_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_client_auth_ca_cert =
            Some(server_cert_path.to_string_lossy().into_owned());

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidCertificate { .. })
        ));
    }

    #[test]
    fn validate_rejects_mismatched_grpc_tls_server_key_pair() {
        let temp = tempfile::tempdir().unwrap();
        let now = OffsetDateTime::now_utc();
        let (server_cert_path, _) = write_cert_and_key(
            &temp,
            "server-cert.pem",
            "server.key",
            now - Duration::days(1),
            now + Duration::days(30),
        );
        let (_, other_key_path) = write_cert_and_key(
            &temp,
            "other-cert.pem",
            "other.key",
            now - Duration::days(1),
            now + Duration::days(30),
        );

        let mut cfg = valid_test_config();
        cfg.server.grpc_tls_enabled = true;
        cfg.server.grpc_tls_cert = Some(server_cert_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_key = Some(other_key_path.to_string_lossy().into_owned());

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidTlsIdentity { .. })
        ));
    }

    #[test]
    fn validate_rejects_mismatched_grpc_tls_client_key_pair() {
        let temp = tempfile::tempdir().unwrap();
        let now = OffsetDateTime::now_utc();
        let (server_cert_path, server_key_path) = write_cert_and_key(
            &temp,
            "server-cert.pem",
            "server.key",
            now - Duration::days(1),
            now + Duration::days(30),
        );
        let (client_cert_path, _) = write_cert_and_key(
            &temp,
            "client-cert.pem",
            "client.key",
            now - Duration::days(1),
            now + Duration::days(30),
        );
        let (_, other_client_key_path) = write_cert_and_key(
            &temp,
            "other-client-cert.pem",
            "other-client.key",
            now - Duration::days(1),
            now + Duration::days(30),
        );

        let mut cfg = valid_test_config();
        cfg.server.grpc_tls_enabled = true;
        cfg.server.grpc_tls_cert = Some(server_cert_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_key = Some(server_key_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_client_cert = Some(client_cert_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_client_key = Some(other_client_key_path.to_string_lossy().into_owned());
        cfg.server.grpc_tls_client_auth_ca_cert =
            Some(server_cert_path.to_string_lossy().into_owned());

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidTlsIdentity { .. })
        ));
    }

    #[test]
    fn listener_config_derives_addresses_from_base_address_and_ports() {
        let cfg = Config::default();
        let listeners = cfg.listener_config();
        assert_eq!(listeners.http_address, "127.0.0.1:7474");
        assert_eq!(listeners.bolt_address, "127.0.0.1:7687");
        assert_eq!(listeners.grpc_address, "127.0.0.1:50051");
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
auth:
    enabled: false
"#,
        )
        .unwrap();

        let mut env = BTreeMap::new();
        env.insert("COPPERDB_HTTP_PORT".to_string(), "8100".to_string());
        env.insert("COPPERDB_GRPC_PORT".to_string(), "9101".to_string());
        env.insert("COPPERDB_GRPC_TLS_ENABLED".to_string(), "true".to_string());
        env.insert(
            "COPPERDB_GRPC_TLS_DOMAIN_NAME".to_string(),
            "env.mesh.local".to_string(),
        );
        env.insert("COPPERDB_HEADLESS".to_string(), "true".to_string());
        env.insert("COPPERDB_AUTH_ENABLED".to_string(), "true".to_string());

        let cli = ConfigOverrides {
            bolt_port: Some(9100),
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
        assert!(cfg.server.grpc_tls_enabled);
        assert_eq!(
            cfg.server.grpc_tls_domain_name.as_deref(),
            Some("cli.mesh.local")
        );
        assert!(cfg.server.grpc_tls_client_auth_optional);
        assert!(cfg.server.grpc_enabled);
        assert!(cfg.auth.enabled);
        assert!(!cfg.server.headless);
    }

    #[test]
    fn auth_enabled_preserves_file_false_without_higher_overrides() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("copperdb.toml");
        std::fs::write(&path, "[auth]\nenabled = false\n").unwrap();

        let cfg = load_with_precedence_from(
            Some(path.as_path()),
            &BTreeMap::new(),
            temp.path(),
            &ConfigOverrides::default(),
        )
        .unwrap();

        assert!(!cfg.auth.enabled);
    }

    #[test]
    fn env_overrides_apply_default_off_search_and_embedding_settings() {
        let mut cfg = Config::default();
        let mut env = BTreeMap::new();
        env.insert("COPPERDB_EMBEDDING_ENABLED".into(), "true".into());
        env.insert("COPPERDB_EMBEDDING_DIMENSIONS".into(), "1024".into());
        env.insert("COPPERDB_EMBEDDING_WARMING".into(), "lazy".into());
        env.insert(
            "COPPERDB_EMBEDDING_WARMUP_INTERVAL_MS".into(),
            "5000".into(),
        );
        env.insert("COPPERDB_EMBEDDING_CACHE_CAPACITY".into(), "256".into());
        env.insert("COPPERDB_EMBEDDING_WORKERS".into(), "3".into());
        env.insert("COPPERDB_EMBEDDING_MAX_ATTEMPTS".into(), "5".into());
        env.insert("COPPERDB_EMBEDDING_RETRY_BACKOFF_MS".into(), "750".into());
        env.insert(
            "COPPERDB_EMBEDDING_SHUTDOWN_TIMEOUT_MS".into(),
            "1500".into(),
        );
        env.insert(
            "COPPERDB_EMBEDDING_PROPERTIES_INCLUDE".into(),
            "title, content".into(),
        );
        env.insert(
            "COPPERDB_EMBEDDING_PROPERTIES_EXCLUDE".into(),
            "secret".into(),
        );
        env.insert("COPPERDB_EMBEDDING_INCLUDE_LABELS".into(), "false".into());
        env.insert("COPPERDB_SEARCH_BM25_ENABLED".into(), "true".into());
        env.insert("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into());
        env.insert(
            "COPPERDB_PACKAGES_ENABLED".into(),
            "copperdb.apoc, copperdb.heimdall".into(),
        );
        env.insert("COPPERDB_PACKAGES_REQUIRED".into(), "copperdb.apoc".into());
        env.insert("COPPERDB_AUTO_LINKS_ENABLED".into(), "true".into());

        apply_env_overrides_from(&mut cfg, &env);

        assert!(cfg.embedding.enabled);
        assert_eq!(cfg.embedding.dimensions, 1024);
        assert_eq!(cfg.embedding.warming, "lazy");
        assert_eq!(cfg.embedding.warmup_interval_ms, 5000);
        assert_eq!(cfg.embedding.cache_capacity, 256);
        assert_eq!(cfg.embedding.workers, 3);
        assert_eq!(cfg.embedding.max_attempts, 5);
        assert_eq!(cfg.embedding.retry_backoff_ms, 750);
        assert_eq!(cfg.embedding.shutdown_timeout_ms, 1500);
        assert_eq!(cfg.embedding.properties_include, ["title", "content"]);
        assert_eq!(cfg.embedding.properties_exclude, ["secret"]);
        assert!(!cfg.embedding.include_labels);
        assert_eq!(cfg.vectorspace.dimensions, 1024);
        assert!(cfg.search.bm25_enabled);
        assert!(cfg.search.vector_enabled);
        assert_eq!(cfg.packages.enabled, ["copperdb.apoc", "copperdb.heimdall"]);
        assert_eq!(cfg.packages.required, ["copperdb.apoc"]);
        assert!(cfg.features.auto_links_enabled);
    }

    #[test]
    fn resolve_per_database_config_applies_cli_then_database_overrides() {
        let mut cfg = Config::default();
        cfg.cli_overrides
            .insert("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "false".into());

        let overrides = BTreeMap::from([
            ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
            ("COPPERDB_SEARCH_VECTOR_WARMING".into(), "startup".into()),
            ("COPPERDB_EMBEDDING_DIMENSIONS".into(), "2048".into()),
            ("COPPERDB_EMBEDDING_WARMUP_INTERVAL".into(), "10s".into()),
            ("COPPERDB_EMBEDDING_CACHE_SIZE".into(), "128".into()),
            ("COPPERDB_EMBED_WORKER_NUM_WORKERS".into(), "0".into()),
        ]);

        let resolved = resolve_per_database_config(&cfg, &overrides).unwrap();

        assert!(resolved.vector_enabled);
        assert_eq!(resolved.vector_warming, "startup");
        assert_eq!(resolved.embedding_dimensions, 2048);
        assert_eq!(resolved.embedding_warmup_interval_ms, 10000);
        assert_eq!(resolved.embedding_cache_capacity, 128);
        assert_eq!(resolved.embedding_workers, 1);
        assert_eq!(
            resolved
                .effective
                .get("db.copper.search.vector.enabled")
                .unwrap(),
            "true"
        );
    }

    #[test]
    fn resolve_per_database_index_capacity_policy() {
        let overrides = BTreeMap::from([
            ("COPPERDB_SEARCH_BM25_ENGINE".into(), "V1".into()),
            ("db.copper.memory.index.bm25.max".into(), "1m".into()),
            ("db.copper.memory.index.vector.max".into(), "2m".into()),
            ("db.copper.memory.index.metadata.max".into(), "3m".into()),
            ("db.copper.index.bm25.storage".into(), "memory".into()),
            ("db.copper.index.vector.storage".into(), "disk".into()),
        ]);

        let resolved = resolve_per_database_config(&Config::default(), &overrides).unwrap();

        assert_eq!(resolved.bm25_engine, "v1");
        assert_eq!(resolved.bm25_memory_max_bytes, 1 << 20);
        assert_eq!(resolved.vector_memory_max_bytes, 2 << 20);
        assert_eq!(resolved.metadata_memory_max_bytes, 3 << 20);
        assert_eq!(resolved.bm25_storage_mode, "memory");
        assert_eq!(resolved.vector_storage_mode, "disk");
    }

    #[test]
    fn database_query_cache_overrides_process_policy() {
        let mut config = Config::default();
        config.search.query_cache_max_entries = 25;
        config.search.query_cache_ttl_ms = 2_000;

        let inherited = resolve_per_database_config(&config, &BTreeMap::new()).unwrap();
        assert_eq!(inherited.query_cache_max_entries, 25);
        assert_eq!(inherited.query_cache_ttl_ms, 2_000);
        assert_eq!(inherited.effective["db.copper.query_cache.ttl"], "2000");

        let overridden = resolve_per_database_config(
            &config,
            &BTreeMap::from([
                ("db.copper.query_cache.max_entries".into(), "2500".into()),
                ("db.copper.query_cache.ttl".into(), "1800000".into()),
            ]),
        )
        .unwrap();
        assert_eq!(overridden.query_cache_max_entries, 2_500);
        assert_eq!(overridden.query_cache_ttl_ms, 1_800_000);
    }

    #[test]
    fn resolver_ignores_invalid_direct_overrides() {
        let mut config = Config::default();
        config.embedding.dimensions = 0;
        config.search.min_similarity = 0.55;
        let resolved = resolve_per_database_config(
            &config,
            &BTreeMap::from([
                ("NOT_ALLOWED".into(), "ignored".into()),
                ("COPPERDB_EMBEDDING_DIMENSIONS".into(), "-5".into()),
                ("COPPERDB_SEARCH_BM25_ENGINE".into(), "V1".into()),
                ("COPPERDB_SEARCH_MIN_SIMILARITY".into(), "bad".into()),
            ]),
        )
        .unwrap();

        assert_eq!(resolved.embedding_dimensions, 1_024);
        assert_eq!(resolved.search_min_similarity, 0.55);
        assert_eq!(resolved.bm25_engine, "v1");
        assert!(!resolved.effective.contains_key("NOT_ALLOWED"));
    }

    #[test]
    fn database_inference_switches_are_explicit_opt_in() {
        let mut config = Config::default();
        config
            .cli_overrides
            .insert("COPPERDB_AUTO_LINKS_ENABLED".into(), "true".into());
        config
            .cli_overrides
            .insert("COPPERDB_AUTO_TLP_ENABLED".into(), "true".into());
        let overrides = BTreeMap::from([
            ("COPPERDB_AUTO_LINKS_ENABLED".into(), "true".into()),
            ("COPPERDB_AUTO_TLP_ENABLED".into(), "true".into()),
        ]);

        let resolved = resolve_per_database_config(&config, &overrides).unwrap();

        assert!(resolved.auto_links_enabled);
        assert!(resolved.auto_tlp_enabled);
        assert_eq!(resolved.effective["db.copper.auto.links.enabled"], "true");
        assert_eq!(resolved.effective["db.copper.auto.tlp.enabled"], "true");
    }

    #[test]
    fn resolver_projects_external_reranker_settings() {
        let resolved = resolve_per_database_config(
            &Config::default(),
            &BTreeMap::from([
                ("COPPERDB_SEARCH_RERANK_ENABLED".into(), "true".into()),
                ("COPPERDB_SEARCH_RERANK_PROVIDER".into(), "ollama".into()),
                ("COPPERDB_SEARCH_RERANK_MODEL".into(), "reranker".into()),
                (
                    "COPPERDB_SEARCH_RERANK_API_URL".into(),
                    "http://localhost:11434/rerank".into(),
                ),
                ("COPPERDB_SEARCH_RERANK_API_KEY".into(), "secret".into()),
            ]),
        )
        .unwrap();

        assert!(resolved.rerank_enabled);
        assert_eq!(resolved.rerank_provider, "ollama");
        assert_eq!(resolved.rerank_model, "reranker");
        assert_eq!(
            resolved.rerank_api_url.as_deref(),
            Some("http://localhost:11434/rerank")
        );
        assert_eq!(resolved.rerank_api_key, "secret");
    }

    #[test]
    fn canonical_database_setting_wins_alias_and_cli() {
        let mut config = Config::default();
        config
            .cli_overrides
            .insert("COPPERDB_SEARCH_BM25_ENABLED".into(), "false".into());
        let overrides = BTreeMap::from([
            ("COPPERDB_SEARCH_BM25_ENABLED".into(), "false".into()),
            ("db.copper.search.bm25.enabled".into(), "true".into()),
        ]);

        let resolved = resolve_per_database_config(&config, &overrides).unwrap();

        assert!(resolved.bm25_enabled);
        assert_eq!(resolved.effective["db.copper.search.bm25.enabled"], "true");
        assert!(
            !resolved
                .effective
                .contains_key("COPPERDB_SEARCH_BM25_ENABLED")
        );
    }

    #[test]
    fn aliases_are_exact_and_unknown_names_are_not_inferred() {
        assert_eq!(
            canonical_setting_name(" COPPERDB_SEARCH_VECTOR_ENABLED "),
            "db.copper.search.vector.enabled"
        );
        assert_eq!(
            canonical_setting_name("copperdb_search_vector_enabled"),
            "copperdb_search_vector_enabled"
        );
        assert_eq!(
            canonical_setting_name("COPPERDB_NOT_A_REGISTERED_SETTING"),
            "COPPERDB_NOT_A_REGISTERED_SETTING"
        );
        assert!(lookup_setting("server.db.query_cache_size").is_none());
    }

    #[test]
    fn typed_values_normalize_strictly() {
        assert_eq!(
            normalize_setting_value("COPPERDB_SEARCH_VECTOR_ENABLED", " T ").unwrap(),
            "true"
        );
        assert_eq!(
            normalize_setting_value("db.copper.search.vector.warming", " STARTUP ").unwrap(),
            "startup"
        );
        assert_eq!(
            normalize_setting_value("db.copper.embedding.dimensions", "001024").unwrap(),
            "1024"
        );
        for value in ["yes", "no", "1.5", "NaN", "+Inf", "-1"] {
            assert!(normalize_setting_value("db.copper.embedding.dimensions", value).is_err());
        }
        for value in ["NaN", "+Inf", "-1"] {
            assert!(normalize_setting_value("db.copper.search.min.similarity", value).is_err());
        }
        assert_eq!(
            normalize_setting_value("db.copper.search.min.similarity", "0.7").unwrap(),
            "0.7"
        );
    }

    #[test]
    fn byte_sizes_match_upstream_contract() {
        for (input, expected) in [
            ("0", 0),
            ("512", 512),
            ("1k", 1 << 10),
            ("2M", 2 << 20),
            ("3 g ", 3 << 30),
            ("1t", 1 << 40),
        ] {
            assert_eq!(parse_byte_size(input), Some(expected), "{input}");
        }
        for input in ["", "-1", "1kb", "1.5m", "999999999999999999999t"] {
            assert_eq!(parse_byte_size(input), None, "{input}");
        }
    }

    #[test]
    fn go_durations_normalize_and_reject_negative_values() {
        for (input, expected) in [
            ("0", "0s"),
            ("250ms", "250ms"),
            ("1.5s", "1.5s"),
            ("1h45m", "1h45m0s"),
        ] {
            assert_eq!(
                normalize_duration(input).as_deref(),
                Some(expected),
                "{input}"
            );
        }
        for input in ["", "-1s", "1d", "1"] {
            assert_eq!(normalize_duration(input), None, "{input}");
        }
    }

    #[test]
    fn setting_activation_metadata_is_consistent() {
        let mut aliases = std::collections::HashSet::new();
        let mut canonical_keys = std::collections::HashSet::new();
        for setting in settings() {
            assert_eq!(
                setting.dynamic,
                setting.restart_level == "none",
                "{}",
                setting.key
            );
            assert_eq!(
                setting.dynamic,
                setting.hot_reload.is_some(),
                "{}",
                setting.key
            );
            assert!(
                canonical_keys.insert(setting.key),
                "duplicate key {}",
                setting.key
            );
            if let Some(alias) = setting.environment_variable {
                assert!(aliases.insert(alias), "duplicate alias {alias}");
                assert_eq!(canonical_setting_name(alias), setting.key);
                assert!(setting.key.contains('.'), "{}", setting.key);
                assert!(!setting.key.contains("COPPERDB_"), "{}", setting.key);
            }
        }
    }

    #[test]
    fn settings_registry_matches_upstream_scope_contract() {
        assert_eq!(settings().len(), 72);
        assert_eq!(allowed_per_database_config_keys().len(), 67);

        let physical_keys: Vec<_> = settings()
            .iter()
            .filter(|setting| setting.scope == "physical-engine")
            .map(|setting| setting.key)
            .collect();
        assert_eq!(
            physical_keys,
            [
                "db.copper.memory.storage.mode",
                "db.copper.memory.storage.node_cache.max_entries",
                "db.copper.memory.storage.edge_type_cache.max_entries",
                "db.copper.recovery.batch.max_bytes",
                "db.copper.recovery.memory.max",
            ]
        );
        for key in physical_keys {
            assert!(lookup_setting(key).is_some(), "{key}");
            assert!(!is_allowed_per_database_config_key(key), "{key}");
        }
        assert!(
            allowed_per_database_config_keys()
                .iter()
                .all(|setting| setting.scope == "database")
        );
    }

    #[test]
    fn sensitive_setting_metadata_is_redacted() {
        let redacted: Vec<_> = settings()
            .iter()
            .filter(|setting| setting.redacted)
            .map(|setting| (setting.key, setting.environment_variable))
            .collect();
        assert_eq!(
            redacted,
            [
                (
                    "db.copper.embedding.api.key",
                    Some("COPPERDB_EMBEDDING_API_KEY")
                ),
                (
                    "db.copper.search.rerank.api.key",
                    Some("COPPERDB_SEARCH_RERANK_API_KEY")
                ),
            ]
        );
        assert!(settings().iter().all(|setting| !setting.deprecated));
    }

    #[test]
    fn query_cache_ttl_is_integer_milliseconds() {
        let setting = lookup_setting("db.copper.query_cache.ttl").unwrap();
        assert_eq!(setting.value_type, "number");
        assert_eq!(setting.default_value, "300000");
        assert!(setting.description.contains("milliseconds"));
        assert!(!setting.dynamic);
        assert_eq!(setting.restart_level, "process");
        assert_eq!(setting.hot_reload, None);
        assert_eq!(
            normalize_setting_value("db.copper.query_cache.ttl", "1800000").unwrap(),
            "1800000"
        );
        for invalid in ["30m", "2s", "1.5", "-1"] {
            assert!(
                normalize_setting_value("db.copper.query_cache.ttl", invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn effective_map_uses_only_canonical_keys_and_registry_defaults() {
        let mut config = Config::default();
        config
            .cli_overrides
            .insert("db.copper.memory.storage.mode".into(), "low".into());
        let resolved = resolve_per_database_config(&config, &BTreeMap::new()).unwrap();

        assert!(resolved.effective.keys().all(|key| key.contains('.')));
        assert!(
            resolved
                .effective
                .keys()
                .all(|key| !key.starts_with("COPPERDB_"))
        );
        assert_eq!(
            resolved.effective["db.copper.query_cache.max_entries"],
            "1000"
        );
        assert_eq!(resolved.effective["db.copper.query_cache.ttl"], "300000");
        assert_eq!(
            resolved.effective["db.copper.query_plan_cache.max_entries"],
            "500"
        );
        assert_eq!(
            resolved.effective["db.copper.search_result_cache.ttl"],
            "5m"
        );
        assert_eq!(resolved.effective["db.memory.transaction.total.max"], "0");
        assert!(
            !resolved
                .effective
                .contains_key("db.copper.memory.storage.mode")
        );
        for removed in [
            "db.copper.embedding.warming",
            "db.copper.embedding.warmup.interval_ms",
            "db.copper.embedding.cache.max_entries",
            "db.copper.embedding.workers",
            "db.copper.embedding.max_attempts",
            "db.copper.embedding.retry_backoff_ms",
            "db.copper.embedding.shutdown_timeout_ms",
        ] {
            assert!(!resolved.effective.contains_key(removed), "{removed}");
            assert!(lookup_setting(removed).is_none(), "{removed}");
        }
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
    fn test_load_from_env_succeeds_with_default_secret() {
        // With the generated default JWT secret, load_from_env should succeed
        // even without copperdb_AUTH__JWT_SECRET in the environment.
        if std::env::var("copperdb_AUTH__JWT_SECRET").is_err() {
            let cfg = load_from_env();
            assert!(
                cfg.is_ok(),
                "load_from_env should succeed with default JWT: {:?}",
                cfg.err()
            );
        }
    }
}
