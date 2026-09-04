//! Key Management Service contracts and local provider for copperDB.
//!
//! KMS owns data-key lifecycle: generate, decrypt, rotate, metadata, audit, and
//! provider construction. Envelope encryption consumes the resulting DEKs.

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use getrandom::fill as fill_random;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use zeroize::Zeroize;

const WRAPPED_KEY_HEADER_VERSION: u8 = 1;
pub const ALGORITHM_AES_256_GCM: &str = "AES-256-GCM";
pub const DATA_KEY_LEN: usize = 32;
pub const NONCE_LEN: usize = 12;
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum KmsError {
    #[error("invalid KMS config: {0}")]
    InvalidConfig(String),
    #[error("KMS provider error: {0}")]
    ProviderError(String),
    #[error("key not found: {0}")]
    KeyNotFound(String),
    #[error("decrypt failed")]
    DecryptFailed,
    #[error("provider is closed")]
    Closed,
    #[error("unsupported provider: {0}")]
    UnsupportedProvider(String),
    #[error("audit signing failed: {0}")]
    AuditSigningFailed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyGenOptions {
    pub algorithm: String,
    pub ttl: Option<Duration>,
    pub label: Option<String>,
}

impl Default for KeyGenOptions {
    fn default() -> Self {
        Self {
            algorithm: ALGORITHM_AES_256_GCM.into(),
            ttl: None,
            label: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecryptOptions {
    pub key_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RotateOptions {
    pub key_uri: Option<String>,
    pub ttl: Option<Duration>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataKey {
    pub key_uri: String,
    pub ciphertext: Vec<u8>,
    pub plaintext: Vec<u8>,
    pub version: u32,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: Option<i64>,
    pub algorithm: String,
}

impl DataKey {
    pub fn is_expired_at(&self, now_unix_ms: i64) -> bool {
        self.expires_at_unix_ms
            .map(|expires| now_unix_ms >= expires)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyMetadata {
    pub key_uri: String,
    pub version: u32,
    pub algorithm: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: Option<i64>,
    pub provider: String,
    pub fips_level: String,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub event_type: String,
    pub key_uri: Option<String>,
    pub principal: Option<String>,
    pub timestamp_unix_ms: i64,
    pub status: String,
    pub error_code: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub signature: Option<String>,
    pub provider_id: Option<String>,
}

impl AuditEvent {
    pub fn success(event_type: impl Into<String>, key_uri: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            key_uri: Some(key_uri.into()),
            principal: None,
            timestamp_unix_ms: now_unix_ms(),
            status: "SUCCESS".into(),
            error_code: None,
            metadata: BTreeMap::new(),
            signature: None,
            provider_id: None,
        }
    }
}

#[async_trait::async_trait]
pub trait KeyProvider: Send + Sync {
    async fn generate_data_key(&self, opts: KeyGenOptions) -> Result<DataKey, KmsError>;
    async fn decrypt_data_key(
        &self,
        encrypted_key: &[u8],
        opts: DecryptOptions,
    ) -> Result<Vec<u8>, KmsError>;
    async fn rotate_data_key(
        &self,
        encrypted_key: &[u8],
        opts: RotateOptions,
    ) -> Result<DataKey, KmsError>;
    async fn key_metadata(&self, key_uri: &str) -> Result<KeyMetadata, KmsError>;
    async fn sign_audit_event(&self, event: AuditEvent) -> Result<AuditEvent, KmsError>;
    async fn close(&self) -> Result<(), KmsError>;
}

#[derive(Debug, Clone)]
pub struct LocalKmsConfig {
    pub master_key: Vec<u8>,
    pub key_uri: String,
    pub fips_level: String,
    pub audit_signing_key: Option<Vec<u8>>,
}

impl LocalKmsConfig {
    pub fn new(master_key: Vec<u8>) -> Self {
        Self {
            master_key,
            key_uri: "kms://local/default".into(),
            fips_level: "software-module".into(),
            audit_signing_key: None,
        }
    }
}

#[derive(Debug)]
pub struct LocalKms {
    master_key: RwLock<Vec<u8>>,
    key_uri: String,
    fips_level: String,
    version: RwLock<u32>,
    created_at_unix_ms: i64,
    closed: RwLock<bool>,
    audit_signer: Option<AuditSigner>,
    audit_events: Mutex<Vec<AuditEvent>>,
}

impl LocalKms {
    pub fn new(config: LocalKmsConfig) -> Result<Self, KmsError> {
        if config.master_key.len() != DATA_KEY_LEN {
            return Err(KmsError::InvalidConfig(format!(
                "local provider requires {DATA_KEY_LEN}-byte master key, got {}",
                config.master_key.len()
            )));
        }
        Ok(Self {
            master_key: RwLock::new(config.master_key),
            key_uri: if config.key_uri.is_empty() {
                "kms://local/default".into()
            } else {
                config.key_uri
            },
            fips_level: if config.fips_level.is_empty() {
                "software-module".into()
            } else {
                config.fips_level
            },
            version: RwLock::new(1),
            created_at_unix_ms: now_unix_ms(),
            closed: RwLock::new(false),
            audit_signer: config.audit_signing_key.map(AuditSigner::new),
            audit_events: Mutex::new(Vec::new()),
        })
    }

    pub fn audit_events(&self) -> Vec<AuditEvent> {
        self.audit_events
            .lock()
            .expect("audit lock poisoned")
            .clone()
    }

    fn ensure_open(&self) -> Result<(), KmsError> {
        if *self.closed.read().expect("kms lock poisoned") {
            Err(KmsError::Closed)
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl KeyProvider for LocalKms {
    async fn generate_data_key(&self, opts: KeyGenOptions) -> Result<DataKey, KmsError> {
        self.ensure_open()?;
        let algorithm = normalize_algorithm(&opts.algorithm)?;
        let mut plaintext = vec![0u8; DATA_KEY_LEN];
        fill_random(plaintext.as_mut_slice())
            .map_err(|_| KmsError::ProviderError("data key generation failed".into()))?;
        let version = *self.version.read().expect("kms lock poisoned");
        let master_key = self.master_key.read().expect("kms lock poisoned").clone();
        let ciphertext = wrap_with_master(&master_key, version, &plaintext)?;
        let created_at = now_unix_ms();
        let expires_at = opts.ttl.map(|ttl| created_at + ttl.as_millis() as i64);
        let data_key = DataKey {
            key_uri: self.key_uri.clone(),
            ciphertext,
            plaintext,
            version,
            created_at_unix_ms: created_at,
            expires_at_unix_ms: expires_at,
            algorithm,
        };
        let _ = self
            .sign_audit_event(AuditEvent::success("KEY_GENERATE", self.key_uri.clone()))
            .await;
        Ok(data_key)
    }

    async fn decrypt_data_key(
        &self,
        encrypted_key: &[u8],
        opts: DecryptOptions,
    ) -> Result<Vec<u8>, KmsError> {
        self.ensure_open()?;
        if let Some(key_uri) = opts.key_uri.as_deref()
            && !key_uri.is_empty()
            && key_uri != self.key_uri
        {
            return Err(KmsError::KeyNotFound(key_uri.into()));
        }
        let master_key = self.master_key.read().expect("kms lock poisoned").clone();
        let plaintext = unwrap_with_master(&master_key, encrypted_key)?;
        let _ = self
            .sign_audit_event(AuditEvent::success("KEY_DECRYPT", self.key_uri.clone()))
            .await;
        Ok(plaintext)
    }

    async fn rotate_data_key(
        &self,
        encrypted_key: &[u8],
        opts: RotateOptions,
    ) -> Result<DataKey, KmsError> {
        let mut plaintext = self
            .decrypt_data_key(
                encrypted_key,
                DecryptOptions {
                    key_uri: opts.key_uri.clone(),
                },
            )
            .await?;
        self.ensure_open()?;
        let version = {
            let mut guard = self.version.write().expect("kms lock poisoned");
            *guard += 1;
            *guard
        };
        let master_key = self.master_key.read().expect("kms lock poisoned").clone();
        let ciphertext = wrap_with_master(&master_key, version, &plaintext)?;
        let created_at = now_unix_ms();
        let expires_at = opts.ttl.map(|ttl| created_at + ttl.as_millis() as i64);
        let data_key = DataKey {
            key_uri: self.key_uri.clone(),
            ciphertext,
            plaintext: plaintext.clone(),
            version,
            created_at_unix_ms: created_at,
            expires_at_unix_ms: expires_at,
            algorithm: ALGORITHM_AES_256_GCM.into(),
        };
        plaintext.zeroize();
        let _ = self
            .sign_audit_event(AuditEvent::success("KEY_ROTATE", self.key_uri.clone()))
            .await;
        Ok(data_key)
    }

    async fn key_metadata(&self, key_uri: &str) -> Result<KeyMetadata, KmsError> {
        self.ensure_open()?;
        if !key_uri.is_empty() && key_uri != self.key_uri {
            return Err(KmsError::KeyNotFound(key_uri.into()));
        }
        Ok(KeyMetadata {
            key_uri: self.key_uri.clone(),
            version: *self.version.read().expect("kms lock poisoned"),
            algorithm: ALGORITHM_AES_256_GCM.into(),
            created_at_unix_ms: self.created_at_unix_ms,
            expires_at_unix_ms: None,
            provider: "local".into(),
            fips_level: self.fips_level.clone(),
            properties: BTreeMap::new(),
        })
    }

    async fn sign_audit_event(&self, mut event: AuditEvent) -> Result<AuditEvent, KmsError> {
        event.provider_id.get_or_insert_with(|| "local".into());
        let signed = if let Some(signer) = &self.audit_signer {
            signer.sign(event)?
        } else {
            event
        };
        self.audit_events
            .lock()
            .expect("audit lock poisoned")
            .push(signed.clone());
        Ok(signed)
    }

    async fn close(&self) -> Result<(), KmsError> {
        let mut closed = self.closed.write().expect("kms lock poisoned");
        if *closed {
            return Ok(());
        }
        self.master_key
            .write()
            .expect("kms lock poisoned")
            .zeroize();
        *closed = true;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AuditSigner {
    key: Vec<u8>,
}

impl AuditSigner {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    pub fn sign(&self, mut event: AuditEvent) -> Result<AuditEvent, KmsError> {
        event.signature = None;
        let payload = serde_json::to_vec(&event)
            .map_err(|err| KmsError::AuditSigningFailed(err.to_string()))?;
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .map_err(|err| KmsError::AuditSigningFailed(err.to_string()))?;
        mac.update(&payload);
        event.signature = Some(hex::encode(mac.finalize().into_bytes()));
        Ok(event)
    }

    pub fn verify(&self, mut event: AuditEvent) -> bool {
        let Some(signature) = event.signature.take() else {
            return false;
        };
        let Ok(expected) = hex::decode(signature) else {
            return false;
        };
        let Ok(payload) = serde_json::to_vec(&event) else {
            return false;
        };
        let Ok(mut mac) = HmacSha256::new_from_slice(&self.key) else {
            return false;
        };
        mac.update(&payload);
        mac.verify_slice(&expected).is_ok()
    }
}

#[derive(Debug, Clone)]
pub struct ProviderFactoryConfig {
    pub provider: String,
    pub key_uri: String,
    pub master_key: Vec<u8>,
    pub audit_signing_key: Option<Vec<u8>>,
}

impl ProviderFactoryConfig {
    pub fn local(master_key: Vec<u8>) -> Self {
        Self {
            provider: "local".into(),
            key_uri: "kms://local/default".into(),
            master_key,
            audit_signing_key: None,
        }
    }
}

pub fn new_provider(config: ProviderFactoryConfig) -> Result<Arc<dyn KeyProvider>, KmsError> {
    match config.provider.as_str() {
        "" | "local" => Ok(Arc::new(LocalKms::new(LocalKmsConfig {
            master_key: config.master_key,
            key_uri: config.key_uri,
            fips_level: "software-module".into(),
            audit_signing_key: config.audit_signing_key,
        })?)),
        "aws-kms" | "azure-keyvault" | "gcp-cloudkms" => {
            Err(KmsError::UnsupportedProvider(config.provider))
        }
        other => Err(KmsError::UnsupportedProvider(other.into())),
    }
}

fn normalize_algorithm(algorithm: &str) -> Result<String, KmsError> {
    let algorithm = if algorithm.trim().is_empty() {
        ALGORITHM_AES_256_GCM
    } else {
        algorithm.trim()
    };
    if algorithm != ALGORITHM_AES_256_GCM {
        return Err(KmsError::InvalidConfig(format!(
            "unsupported data key algorithm {algorithm}"
        )));
    }
    Ok(algorithm.into())
}

fn wrap_with_master(
    master_key: &[u8],
    version: u32,
    plaintext: &[u8],
) -> Result<Vec<u8>, KmsError> {
    if master_key.len() != DATA_KEY_LEN {
        return Err(KmsError::InvalidConfig("invalid master key length".into()));
    }
    let key = Key::<Aes256Gcm>::try_from(master_key)
        .map_err(|_| KmsError::InvalidConfig("invalid master key length".into()))?;
    let cipher = Aes256Gcm::new(&key);
    let mut nonce_bytes = [0u8; NONCE_LEN];
    fill_random(&mut nonce_bytes)
        .map_err(|_| KmsError::ProviderError("nonce generation failed".into()))?;
    let nonce = Nonce::from(nonce_bytes);
    let mut out = Vec::with_capacity(1 + 4 + NONCE_LEN + plaintext.len() + 16);
    out.push(WRAPPED_KEY_HEADER_VERSION);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&nonce_bytes);
    out.extend(
        cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| KmsError::ProviderError("AES-GCM wrap failed".into()))?,
    );
    Ok(out)
}

fn unwrap_with_master(master_key: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, KmsError> {
    if ciphertext.len() < 1 + 4 + NONCE_LEN {
        return Err(KmsError::DecryptFailed);
    }
    if ciphertext[0] != WRAPPED_KEY_HEADER_VERSION {
        return Err(KmsError::DecryptFailed);
    }
    let nonce_offset = 1 + 4;
    let nonce = &ciphertext[nonce_offset..nonce_offset + NONCE_LEN];
    let body = &ciphertext[nonce_offset + NONCE_LEN..];
    let key = Key::<Aes256Gcm>::try_from(master_key)
        .map_err(|_| KmsError::InvalidConfig("invalid master key length".into()))?;
    let cipher = Aes256Gcm::new(&key);
    let nonce = Nonce::try_from(nonce).map_err(|_| KmsError::DecryptFailed)?;
    cipher
        .decrypt(&nonce, body)
        .map_err(|_| KmsError::DecryptFailed)
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn master_key(byte: u8) -> Vec<u8> {
        vec![byte; DATA_KEY_LEN]
    }

    #[tokio::test]
    async fn local_provider_generates_and_decrypts_data_keys() {
        let kms = LocalKms::new(LocalKmsConfig::new(master_key(0x42))).unwrap();
        let data_key = kms
            .generate_data_key(KeyGenOptions {
                ttl: Some(Duration::from_secs(60)),
                label: Some("tenant-a".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(data_key.plaintext.len(), DATA_KEY_LEN);
        assert_eq!(data_key.version, 1);
        assert!(!data_key.is_expired_at(data_key.created_at_unix_ms));
        let decrypted = kms
            .decrypt_data_key(
                &data_key.ciphertext,
                DecryptOptions {
                    key_uri: Some(data_key.key_uri.clone()),
                },
            )
            .await
            .unwrap();
        assert_eq!(decrypted, data_key.plaintext);
    }

    #[tokio::test]
    async fn local_provider_rotates_data_key_version() {
        let kms = LocalKms::new(LocalKmsConfig::new(master_key(0x42))).unwrap();
        let first = kms
            .generate_data_key(KeyGenOptions::default())
            .await
            .unwrap();
        let rotated = kms
            .rotate_data_key(
                &first.ciphertext,
                RotateOptions {
                    key_uri: Some(first.key_uri.clone()),
                    ttl: Some(Duration::from_secs(60)),
                    label: Some("rotation".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(rotated.version, 2);
        assert_eq!(rotated.plaintext, first.plaintext);
        assert_ne!(rotated.ciphertext, first.ciphertext);
    }

    #[tokio::test]
    async fn local_provider_rejects_wrong_key_uri_and_closed_provider() {
        let kms = LocalKms::new(LocalKmsConfig::new(master_key(0x42))).unwrap();
        let data_key = kms
            .generate_data_key(KeyGenOptions::default())
            .await
            .unwrap();
        assert!(matches!(
            kms.decrypt_data_key(
                &data_key.ciphertext,
                DecryptOptions {
                    key_uri: Some("kms://local/other".into()),
                },
            )
            .await,
            Err(KmsError::KeyNotFound(_))
        ));
        kms.close().await.unwrap();
        assert!(matches!(
            kms.generate_data_key(KeyGenOptions::default()).await,
            Err(KmsError::Closed)
        ));
    }

    #[test]
    fn audit_signer_signs_and_verifies_events() {
        let signer = AuditSigner::new(b"audit-key".to_vec());
        let event = AuditEvent::success("KEY_GENERATE", "kms://local/default");
        let signed = signer.sign(event).unwrap();
        assert!(signed.signature.is_some());
        assert!(signer.verify(signed.clone()));
        let mut tampered = signed;
        tampered.status = "FAILURE".into();
        assert!(!signer.verify(tampered));
    }

    #[tokio::test]
    async fn local_provider_records_signed_audit_events() {
        let kms = LocalKms::new(LocalKmsConfig {
            audit_signing_key: Some(b"audit-key".to_vec()),
            ..LocalKmsConfig::new(master_key(0x99))
        })
        .unwrap();
        let data_key = kms
            .generate_data_key(KeyGenOptions::default())
            .await
            .unwrap();
        let _ = kms
            .decrypt_data_key(&data_key.ciphertext, DecryptOptions { key_uri: None })
            .await
            .unwrap();
        let events = kms.audit_events();
        assert!(events.len() >= 2);
        assert!(events.iter().all(|event| event.signature.is_some()));
    }

    #[test]
    fn provider_factory_builds_local_and_rejects_unsupported_providers() {
        let provider = new_provider(ProviderFactoryConfig::local(master_key(0x42))).unwrap();
        drop(provider);
        assert!(matches!(
            new_provider(ProviderFactoryConfig {
                provider: "azure-keyvault".into(),
                key_uri: String::new(),
                master_key: master_key(0x42),
                audit_signing_key: None,
            }),
            Err(KmsError::UnsupportedProvider(_))
        ));
    }
}
