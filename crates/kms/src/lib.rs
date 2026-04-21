//! Key Management Service (KMS) integration for copperdb.
//!
//! Equivalent to Go's `pkg/kms` in NornicDB.
//! Wraps and unwraps Data Encryption Keys (DEKs) using:
//! - **Local**: in-process key (development only)
//! - **AWS KMS**: `GenerateDataKey` / `Decrypt` API
//! - **Azure Key Vault**: `WrapKey` / `UnwrapKey` API
//! - **GCP Cloud KMS**: `CryptoKeyVersions.AsymmetricDecrypt` API
//!
//! ## Rust equivalents
//! - AWS KMS: `aws-sdk-kms` (official AWS SDK)
//! - Azure Key Vault: `azure_security_keyvault_keys` crate
//! - GCP KMS: no official Rust SDK — use `google-cloud-kms` crate or
//!   call the REST API via `reqwest` + `google-auth` credentials.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum KmsError {
    #[error("KMS provider error: {0}")]
    ProviderError(String),
    #[error("key not found: {0}")]
    KeyNotFound(String),
    #[error("decrypt failed")]
    DecryptFailed,
    #[error("unsupported provider: {0}")]
    UnsupportedProvider(String),
}

/// KMS provider trait for wrapping/unwrapping DEKs.
#[async_trait::async_trait]
pub trait KmsProvider: Send + Sync {
    /// Encrypt (wrap) a plaintext DEK using the master key.
    async fn wrap_key(&self, key_id: &str, plaintext_dek: &[u8]) -> Result<Vec<u8>, KmsError>;
    /// Decrypt (unwrap) an encrypted DEK.
    async fn unwrap_key(&self, key_id: &str, encrypted_dek: &[u8]) -> Result<Vec<u8>, KmsError>;
}

/// Local (in-memory) KMS provider for development and testing.
///
/// Uses AES-256-GCM for key wrapping (same as `copperdb-encryption`).
#[derive(Debug)]
pub struct LocalKms {
    master_key: Vec<u8>,
}

impl LocalKms {
    /// Create a local KMS with a 32-byte master key.
    pub fn new(master_key: Vec<u8>) -> Result<Self, KmsError> {
        if master_key.len() != 32 {
            return Err(KmsError::ProviderError(
                format!("master key must be 32 bytes, got {}", master_key.len()),
            ));
        }
        Ok(Self { master_key })
    }
}

#[async_trait::async_trait]
impl KmsProvider for LocalKms {
    async fn wrap_key(&self, _key_id: &str, plaintext_dek: &[u8]) -> Result<Vec<u8>, KmsError> {
        use aes_gcm::{aead::{Aead, KeyInit}, Aes256Gcm, Key, Nonce};
        use getrandom::fill as fill_random;

        let key = Key::<Aes256Gcm>::try_from(self.master_key.as_slice())
            .map_err(|_| KmsError::ProviderError("invalid master key length".into()))?;
        let cipher = Aes256Gcm::new(&key);
        let mut nonce_bytes = [0u8; 12];
        fill_random(&mut nonce_bytes)
            .map_err(|_| KmsError::ProviderError("nonce generation failed".into()))?;
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = cipher.encrypt(&nonce, plaintext_dek)
            .map_err(|_| KmsError::ProviderError("AES-GCM wrap failed".into()))?;
        let mut result = nonce.to_vec();
        result.extend(ciphertext);
        Ok(result)
    }

    async fn unwrap_key(&self, _key_id: &str, encrypted_dek: &[u8]) -> Result<Vec<u8>, KmsError> {
        use aes_gcm::{aead::{Aead, KeyInit}, Aes256Gcm, Key, Nonce};
        if encrypted_dek.len() < 12 {
            return Err(KmsError::DecryptFailed);
        }
        let (nonce_bytes, ciphertext) = encrypted_dek.split_at(12);
        let key = Key::<Aes256Gcm>::try_from(self.master_key.as_slice())
            .map_err(|_| KmsError::ProviderError("invalid master key length".into()))?;
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::try_from(nonce_bytes).map_err(|_| KmsError::DecryptFailed)?;
        cipher.decrypt(&nonce, ciphertext).map_err(|_| KmsError::DecryptFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_kms_wrap_unwrap() {
        let master_key = vec![0x42u8; 32];
        let kms = LocalKms::new(master_key).unwrap();
        let dek = b"super-secret-dek-key-material";
        let wrapped = kms.wrap_key("key-1", dek).await.unwrap();
        let unwrapped = kms.unwrap_key("key-1", &wrapped).await.unwrap();
        assert_eq!(unwrapped, dek);
    }

    #[test]
    fn test_local_kms_invalid_key_length() {
        let result = LocalKms::new(vec![0u8; 16]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), KmsError::ProviderError(_)));
    }

    #[tokio::test]
    async fn test_local_kms_different_keys_each_wrap() {
        let master_key = vec![0x99u8; 32];
        let kms = LocalKms::new(master_key).unwrap();
        let dek = b"my-encryption-key";
        let w1 = kms.wrap_key("k1", dek).await.unwrap();
        let w2 = kms.wrap_key("k1", dek).await.unwrap();
        // Nonces should be random, so ciphertexts differ
        assert_ne!(w1, w2);
    }

    #[tokio::test]
    async fn test_local_kms_tampered_ciphertext_fails() {
        let master_key = vec![0x11u8; 32];
        let kms = LocalKms::new(master_key).unwrap();
        let dek = b"plaintext-key";
        let mut wrapped = kms.wrap_key("k", dek).await.unwrap();
        // Flip a byte in the ciphertext
        let last = wrapped.len() - 1;
        wrapped[last] ^= 0xFF;
        let result = kms.unwrap_key("k", &wrapped).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_local_kms_too_short_encrypted_fails() {
        let master_key = vec![0xABu8; 32];
        let kms = LocalKms::new(master_key).unwrap();
        let result = kms.unwrap_key("k", &[0u8; 5]).await;
        assert!(matches!(result, Err(KmsError::DecryptFailed)));
    }
}
