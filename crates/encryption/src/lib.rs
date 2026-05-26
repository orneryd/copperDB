//! Versioned envelope encryption for copperDB.
//!
//! The crate owns only cryptographic envelope mechanics. KMS providers generate,
//! wrap, unwrap, and rotate DEKs; this crate binds a plaintext payload to a DEK
//! and stores the KMS metadata required to decrypt it later.

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use copperdb_kms::{AuditEvent, DataKey, DecryptOptions, KeyGenOptions, KeyProvider, KmsError};
use getrandom::fill as fill_random;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use zeroize::Zeroizing;

pub const ENVELOPE_VERSION: u8 = 1;
pub const ALGORITHM_AES_256_GCM: &str = "AES-256-GCM";
pub const DATA_KEY_LEN: usize = 32;
pub const NONCE_LEN: usize = 12;

#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("AES-GCM encryption failed")]
    EncryptFailed,
    #[error("AES-GCM decryption failed")]
    DecryptFailed,
    #[error("invalid data key length: expected 32 bytes, got {0}")]
    InvalidDataKeyLength(usize),
    #[error("invalid nonce length: expected 12 bytes, got {0}")]
    InvalidNonceLength(usize),
    #[error("unsupported envelope version: {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported encryption algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("KMS error: {0}")]
    Kms(#[from] KmsError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Envelope {
    pub version: u8,
    pub key_uri: String,
    pub key_version: u32,
    pub algorithm: String,
    pub encrypted_dek: Vec<u8>,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct DataKeyRef<'a> {
    pub key_uri: &'a str,
    pub key_version: u32,
    pub algorithm: &'a str,
    pub plaintext_dek: &'a [u8],
    pub encrypted_dek: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct EnvelopeConfig {
    pub dek_cache_ttl: Duration,
    pub label: Option<String>,
    pub associated_data: Vec<u8>,
}

impl Default for EnvelopeConfig {
    fn default() -> Self {
        Self {
            dek_cache_ttl: Duration::from_secs(24 * 60 * 60),
            label: None,
            associated_data: Vec::new(),
        }
    }
}

pub struct EnvelopeEncryptor {
    provider: Arc<dyn KeyProvider>,
    cache: DekCache,
    associated_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RotationConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub retention_count: usize,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(90 * 24 * 60 * 60),
            retention_count: 5,
        }
    }
}

#[async_trait::async_trait]
pub trait Reencryptor: Send + Sync {
    async fn reencrypt_with_data_key(&self, key: &DataKey) -> Result<usize, EncryptionError>;
}

pub struct RotationManager {
    provider: Arc<dyn KeyProvider>,
    config: RotationConfig,
}

impl RotationManager {
    pub fn new(provider: Arc<dyn KeyProvider>, config: RotationConfig) -> Self {
        let default = RotationConfig::default();
        Self {
            provider,
            config: RotationConfig {
                enabled: config.enabled,
                interval: if config.interval.is_zero() {
                    default.interval
                } else {
                    config.interval
                },
                retention_count: if config.retention_count == 0 {
                    default.retention_count
                } else {
                    config.retention_count
                },
            },
        }
    }

    pub fn config(&self) -> &RotationConfig {
        &self.config
    }

    pub async fn perform_rotation<R: Reencryptor>(
        &self,
        reencryptor: &R,
    ) -> Result<usize, EncryptionError> {
        let key = self
            .provider
            .generate_data_key(KeyGenOptions {
                algorithm: ALGORITHM_AES_256_GCM.into(),
                ttl: Some(self.config.interval),
                label: Some("rotation".into()),
            })
            .await?;
        reencryptor.reencrypt_with_data_key(&key).await
    }
}

impl EnvelopeEncryptor {
    pub fn new(provider: Arc<dyn KeyProvider>, config: EnvelopeConfig) -> Self {
        Self {
            provider: Arc::clone(&provider),
            cache: DekCache::new(provider, config.dek_cache_ttl, config.label),
            associated_data: config.associated_data,
        }
    }

    pub async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let data_key = self.cache.get_or_generate().await?;
        let envelope = encrypt_with_data_key(
            plaintext,
            DataKeyRef {
                key_uri: data_key.key_uri.as_str(),
                key_version: data_key.version,
                algorithm: data_key.algorithm.as_str(),
                plaintext_dek: data_key.plaintext.as_slice(),
                encrypted_dek: data_key.ciphertext.as_slice(),
            },
            self.associated_data.as_slice(),
        )?;
        envelope.to_bytes()
    }

    pub async fn decrypt(&self, envelope_bytes: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let envelope = Envelope::from_bytes(envelope_bytes)?;
        let dek = match self
            .provider
            .decrypt_data_key(
                envelope.encrypted_dek.as_slice(),
                DecryptOptions {
                    key_uri: Some(envelope.key_uri.clone()),
                },
            )
            .await
        {
            Ok(dek) => dek,
            Err(err) => {
                let _ = self
                    .provider
                    .sign_audit_event(AuditEvent {
                        event_type: "KEY_DECRYPT_FAILED".into(),
                        key_uri: Some(envelope.key_uri),
                        principal: None,
                        timestamp_unix_ms: now_unix_ms(),
                        status: "FAILURE".into(),
                        error_code: Some("DEK_DECRYPT_FAILED".into()),
                        metadata: Default::default(),
                        signature: None,
                        provider_id: None,
                    })
                    .await;
                return Err(EncryptionError::Kms(err));
            }
        };
        decrypt_with_data_key(&envelope, dek.as_slice(), self.associated_data.as_slice())
    }

    pub fn cache_snapshot(&self) -> Option<DataKey> {
        self.cache.snapshot()
    }
}

struct DekCache {
    provider: Arc<dyn KeyProvider>,
    current: Mutex<Option<DataKey>>,
    max_age: Duration,
    label: Option<String>,
}

impl DekCache {
    fn new(provider: Arc<dyn KeyProvider>, max_age: Duration, label: Option<String>) -> Self {
        let max_age = if max_age.is_zero() {
            Duration::from_secs(24 * 60 * 60)
        } else {
            max_age
        };
        Self {
            provider,
            current: Mutex::new(None),
            max_age,
            label,
        }
    }

    async fn get_or_generate(&self) -> Result<DataKey, EncryptionError> {
        let now = now_unix_ms();
        if let Some(current) = self.snapshot() {
            let max_age_expired =
                now >= current.created_at_unix_ms + self.max_age.as_millis() as i64;
            if !current.is_expired_at(now) && !max_age_expired {
                return Ok(current);
            }
        }

        let generated = self
            .provider
            .generate_data_key(KeyGenOptions {
                algorithm: ALGORITHM_AES_256_GCM.into(),
                ttl: Some(self.max_age),
                label: self.label.clone(),
            })
            .await?;
        *self.current.lock().expect("dek cache lock poisoned") = Some(generated.clone());
        Ok(generated)
    }

    fn snapshot(&self) -> Option<DataKey> {
        self.current
            .lock()
            .expect("dek cache lock poisoned")
            .clone()
    }
}

impl<'a> DataKeyRef<'a> {
    pub fn local(plaintext_dek: &'a [u8], encrypted_dek: &'a [u8]) -> Self {
        Self {
            key_uri: "kms://local/default",
            key_version: 1,
            algorithm: ALGORITHM_AES_256_GCM,
            plaintext_dek,
            encrypted_dek,
        }
    }
}

impl Envelope {
    pub fn to_bytes(&self) -> Result<Vec<u8>, EncryptionError> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EncryptionError> {
        let envelope: Self = serde_json::from_slice(bytes)?;
        envelope.validate_metadata()?;
        Ok(envelope)
    }

    pub fn rewrapped(
        &self,
        key_uri: impl Into<String>,
        key_version: u32,
        encrypted_dek: Vec<u8>,
    ) -> Self {
        Self {
            version: self.version,
            key_uri: key_uri.into(),
            key_version,
            algorithm: self.algorithm.clone(),
            encrypted_dek,
            nonce: self.nonce.clone(),
            ciphertext: self.ciphertext.clone(),
        }
    }

    fn validate_metadata(&self) -> Result<(), EncryptionError> {
        if self.version != ENVELOPE_VERSION {
            return Err(EncryptionError::UnsupportedVersion(self.version));
        }
        if self.algorithm != ALGORITHM_AES_256_GCM {
            return Err(EncryptionError::UnsupportedAlgorithm(
                self.algorithm.clone(),
            ));
        }
        if self.nonce.len() != NONCE_LEN {
            return Err(EncryptionError::InvalidNonceLength(self.nonce.len()));
        }
        Ok(())
    }
}

pub fn encrypt_with_data_key(
    plaintext: &[u8],
    data_key: DataKeyRef<'_>,
    associated_data: &[u8],
) -> Result<Envelope, EncryptionError> {
    validate_data_key(data_key.plaintext_dek)?;
    if data_key.algorithm != ALGORITHM_AES_256_GCM {
        return Err(EncryptionError::UnsupportedAlgorithm(
            data_key.algorithm.into(),
        ));
    }

    let cipher = cipher_for_dek(data_key.plaintext_dek)?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    fill_random(&mut nonce_bytes).map_err(|_| EncryptionError::EncryptFailed)?;
    let nonce = Nonce::from(nonce_bytes);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: associated_data,
            },
        )
        .map_err(|_| EncryptionError::EncryptFailed)?;

    Ok(Envelope {
        version: ENVELOPE_VERSION,
        key_uri: data_key.key_uri.to_string(),
        key_version: data_key.key_version,
        algorithm: data_key.algorithm.to_string(),
        encrypted_dek: data_key.encrypted_dek.to_vec(),
        nonce: nonce_bytes.to_vec(),
        ciphertext,
    })
}

pub fn decrypt_with_data_key(
    envelope: &Envelope,
    plaintext_dek: &[u8],
    associated_data: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    envelope.validate_metadata()?;
    validate_data_key(plaintext_dek)?;
    let cipher = cipher_for_dek(plaintext_dek)?;
    let nonce = Nonce::try_from(envelope.nonce.as_slice())
        .map_err(|_| EncryptionError::InvalidNonceLength(envelope.nonce.len()))?;
    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: envelope.ciphertext.as_ref(),
                aad: associated_data,
            },
        )
        .map_err(|_| EncryptionError::DecryptFailed)
}

pub fn encrypt(plaintext: &[u8], kek: &[u8]) -> Result<Envelope, EncryptionError> {
    validate_data_key(kek)?;
    let mut dek = Zeroizing::new(vec![0u8; DATA_KEY_LEN]);
    fill_random(dek.as_mut_slice()).map_err(|_| EncryptionError::EncryptFailed)?;
    let encrypted_dek = wrap_dek_with_kek(dek.as_slice(), kek)?;
    encrypt_with_data_key(
        plaintext,
        DataKeyRef::local(dek.as_slice(), &encrypted_dek),
        &[],
    )
}

pub fn decrypt(envelope: &Envelope, kek: &[u8]) -> Result<Vec<u8>, EncryptionError> {
    validate_data_key(kek)?;
    let dek = Zeroizing::new(unwrap_dek_with_kek(&envelope.encrypted_dek, kek)?);
    decrypt_with_data_key(envelope, dek.as_slice(), &[])
}

pub fn rotate_kek(
    envelope: &Envelope,
    old_kek: &[u8],
    new_kek: &[u8],
) -> Result<Envelope, EncryptionError> {
    validate_data_key(old_kek)?;
    validate_data_key(new_kek)?;
    let dek = Zeroizing::new(unwrap_dek_with_kek(&envelope.encrypted_dek, old_kek)?);
    let encrypted_dek = wrap_dek_with_kek(dek.as_slice(), new_kek)?;
    Ok(envelope.rewrapped(&envelope.key_uri, envelope.key_version + 1, encrypted_dek))
}

fn wrap_dek_with_kek(dek: &[u8], kek: &[u8]) -> Result<Vec<u8>, EncryptionError> {
    validate_data_key(dek)?;
    validate_data_key(kek)?;
    let cipher = cipher_for_dek(kek)?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    fill_random(&mut nonce_bytes).map_err(|_| EncryptionError::EncryptFailed)?;
    let nonce = Nonce::from(nonce_bytes);
    let mut wrapped = nonce_bytes.to_vec();
    wrapped.extend(
        cipher
            .encrypt(&nonce, dek)
            .map_err(|_| EncryptionError::EncryptFailed)?,
    );
    Ok(wrapped)
}

fn unwrap_dek_with_kek(encrypted_dek: &[u8], kek: &[u8]) -> Result<Vec<u8>, EncryptionError> {
    validate_data_key(kek)?;
    if encrypted_dek.len() < NONCE_LEN {
        return Err(EncryptionError::InvalidNonceLength(encrypted_dek.len()));
    }
    let (nonce_bytes, ciphertext) = encrypted_dek.split_at(NONCE_LEN);
    let cipher = cipher_for_dek(kek)?;
    let nonce = Nonce::try_from(nonce_bytes)
        .map_err(|_| EncryptionError::InvalidNonceLength(nonce_bytes.len()))?;
    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| EncryptionError::DecryptFailed)
}

fn cipher_for_dek(key: &[u8]) -> Result<Aes256Gcm, EncryptionError> {
    validate_data_key(key)?;
    let key = Key::<Aes256Gcm>::try_from(key)
        .map_err(|_| EncryptionError::InvalidDataKeyLength(key.len()))?;
    Ok(Aes256Gcm::new(&key))
}

fn validate_data_key(key: &[u8]) -> Result<(), EncryptionError> {
    if key.len() != DATA_KEY_LEN {
        return Err(EncryptionError::InvalidDataKeyLength(key.len()));
    }
    Ok(())
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
    use copperdb_kms::{LocalKms, LocalKmsConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_kek(byte: u8) -> Vec<u8> {
        vec![byte; DATA_KEY_LEN]
    }

    fn local_provider() -> Arc<dyn KeyProvider> {
        Arc::new(LocalKms::new(LocalKmsConfig::new(make_kek(0x66))).unwrap())
    }

    #[test]
    fn envelope_round_trips_with_local_kek() {
        let kek = make_kek(0x42);
        let plaintext = b"secret graph data";
        let envelope = encrypt(plaintext, &kek).unwrap();
        assert_eq!(envelope.version, ENVELOPE_VERSION);
        assert_eq!(envelope.algorithm, ALGORITHM_AES_256_GCM);
        assert_eq!(decrypt(&envelope, &kek).unwrap(), plaintext);
    }

    #[test]
    fn envelope_serializes_and_preserves_metadata() {
        let kek = make_kek(0x42);
        let envelope = encrypt(b"secret", &kek).unwrap();
        let bytes = envelope.to_bytes().unwrap();
        let decoded = Envelope::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.key_uri, envelope.key_uri);
        assert_eq!(decoded.key_version, envelope.key_version);
        assert_eq!(decrypt(&decoded, &kek).unwrap(), b"secret");
    }

    #[test]
    fn associated_data_is_authenticated() {
        let dek = vec![0xAB; DATA_KEY_LEN];
        let wrapped = vec![0xCD; DATA_KEY_LEN];
        let data_key = DataKeyRef {
            key_uri: "kms://local/aad",
            key_version: 7,
            algorithm: ALGORITHM_AES_256_GCM,
            plaintext_dek: &dek,
            encrypted_dek: &wrapped,
        };
        let envelope = encrypt_with_data_key(b"payload", data_key, b"tenant-a").unwrap();
        assert_eq!(
            decrypt_with_data_key(&envelope, &dek, b"tenant-a").unwrap(),
            b"payload"
        );
        assert!(matches!(
            decrypt_with_data_key(&envelope, &dek, b"tenant-b"),
            Err(EncryptionError::DecryptFailed)
        ));
    }

    #[test]
    fn wrong_kek_or_tamper_fails_decryption() {
        let kek = make_kek(0x42);
        let wrong_kek = make_kek(0x00);
        let mut envelope = encrypt(b"secret", &kek).unwrap();
        assert!(decrypt(&envelope, &wrong_kek).is_err());
        let last = envelope.ciphertext.len() - 1;
        envelope.ciphertext[last] ^= 0xFF;
        assert!(decrypt(&envelope, &kek).is_err());
    }

    #[test]
    fn kek_rotation_rewraps_dek_without_changing_ciphertext() {
        let old_kek = make_kek(0x42);
        let new_kek = make_kek(0xAB);
        let env = encrypt(b"rotate me", &old_kek).unwrap();
        let rotated = rotate_kek(&env, &old_kek, &new_kek).unwrap();
        assert_eq!(rotated.ciphertext, env.ciphertext);
        assert_ne!(rotated.encrypted_dek, env.encrypted_dek);
        assert_eq!(decrypt(&rotated, &new_kek).unwrap(), b"rotate me");
    }

    #[test]
    fn invalid_metadata_is_rejected() {
        assert!(matches!(
            encrypt(b"data", &[0u8; 16]),
            Err(EncryptionError::InvalidDataKeyLength(16))
        ));
        let mut env = encrypt(b"data", &make_kek(0x42)).unwrap();
        env.version = 99;
        assert!(matches!(
            decrypt(&env, &make_kek(0x42)),
            Err(EncryptionError::UnsupportedVersion(99))
        ));
    }

    #[tokio::test]
    async fn envelope_encryptor_round_trips_with_provider_backed_data_key() {
        let encryptor = EnvelopeEncryptor::new(
            local_provider(),
            EnvelopeConfig {
                dek_cache_ttl: Duration::from_secs(60),
                label: Some("test".into()),
                associated_data: b"tenant-a".to_vec(),
            },
        );

        let payload = encryptor.encrypt(b"hello-cmek-envelope").await.unwrap();
        let out = encryptor.decrypt(&payload).await.unwrap();
        assert_eq!(out, b"hello-cmek-envelope");
    }

    #[tokio::test]
    async fn envelope_encryptor_reuses_cached_data_key_until_expired() {
        let encryptor = EnvelopeEncryptor::new(
            local_provider(),
            EnvelopeConfig {
                dek_cache_ttl: Duration::from_secs(60),
                label: Some("cache".into()),
                associated_data: Vec::new(),
            },
        );

        let first = encryptor.encrypt(b"one").await.unwrap();
        let first_key = encryptor.cache_snapshot().unwrap();
        let second = encryptor.encrypt(b"two").await.unwrap();
        let second_key = encryptor.cache_snapshot().unwrap();
        assert_ne!(first, second);
        assert_eq!(first_key.ciphertext, second_key.ciphertext);
    }

    #[tokio::test]
    async fn envelope_encryptor_tamper_failure_is_reported() {
        let encryptor = EnvelopeEncryptor::new(local_provider(), EnvelopeConfig::default());
        let mut payload = encryptor.encrypt(b"tamper-me").await.unwrap();
        let last = payload.len() - 1;
        payload[last] ^= 0xFF;
        assert!(encryptor.decrypt(&payload).await.is_err());
    }

    struct CountingReencryptor {
        count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Reencryptor for CountingReencryptor {
        async fn reencrypt_with_data_key(&self, key: &DataKey) -> Result<usize, EncryptionError> {
            assert_eq!(key.algorithm, ALGORITHM_AES_256_GCM);
            Ok(self.count.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    #[tokio::test]
    async fn rotation_manager_defaults_and_performs_rotation() {
        let manager = RotationManager::new(
            local_provider(),
            RotationConfig {
                enabled: true,
                interval: Duration::ZERO,
                retention_count: 0,
            },
        );
        assert_eq!(
            manager.config().interval,
            Duration::from_secs(90 * 24 * 60 * 60)
        );
        assert_eq!(manager.config().retention_count, 5);
        let reencryptor = CountingReencryptor {
            count: AtomicUsize::new(0),
        };
        assert_eq!(manager.perform_rotation(&reencryptor).await.unwrap(), 1);
    }
}
