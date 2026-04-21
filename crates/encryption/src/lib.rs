//! Envelope encryption for copperdb.
//!
//! Equivalent to Go's `pkg/encryption` in NornicDB.
//! Implements AES-256-GCM envelope encryption with a DEK/KEK key hierarchy.
//!
//! - **DEK** (Data Encryption Key): unique per record, encrypts the data
//! - **KEK** (Key Encryption Key): held in KMS, encrypts the DEK
//!
//! The encrypted envelope contains: `[ encrypted_dek | nonce | ciphertext ]`

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use getrandom::fill as fill_random;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("AES-GCM encryption failed")]
    EncryptFailed,
    #[error("AES-GCM decryption failed (authentication tag mismatch)")]
    DecryptFailed,
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLength(usize),
    #[error("invalid nonce length")]
    InvalidNonce,
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// An encrypted data envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Encrypted DEK (wrapped by the KEK via KMS or local key).
    pub encrypted_dek: Vec<u8>,
    /// AES-GCM nonce (12 bytes).
    pub nonce: Vec<u8>,
    /// AES-GCM ciphertext.
    pub ciphertext: Vec<u8>,
}

/// Encrypt plaintext under a freshly-generated DEK, then wrap the DEK with the KEK.
///
/// Returns an `Envelope` ready for storage.
pub fn encrypt(plaintext: &[u8], kek: &[u8]) -> Result<Envelope, EncryptionError> {
    if kek.len() != 32 {
        return Err(EncryptionError::InvalidKeyLength(kek.len()));
    }

    // Generate a fresh 256-bit DEK.
    let mut dek_bytes = [0u8; 32];
    fill_random(&mut dek_bytes).map_err(|_| EncryptionError::EncryptFailed)?;
    let dek = Zeroizing::new(dek_bytes.to_vec());
    let dek_key = Key::<Aes256Gcm>::try_from(dek.as_slice())
        .map_err(|_| EncryptionError::InvalidKeyLength(dek.len()))?;
    let cipher = Aes256Gcm::new(&dek_key);

    // Encrypt plaintext with DEK.
    let mut nonce_bytes = [0u8; 12];
    fill_random(&mut nonce_bytes).map_err(|_| EncryptionError::EncryptFailed)?;
    let nonce = Nonce::from(nonce_bytes);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| EncryptionError::EncryptFailed)?;

    // Wrap DEK with KEK (also using AES-GCM key-wrapping).
    let kek_key = Key::<Aes256Gcm>::try_from(kek)
        .map_err(|_| EncryptionError::InvalidKeyLength(kek.len()))?;
    let kek_cipher = Aes256Gcm::new(&kek_key);
    let mut kek_nonce_bytes = [0u8; 12];
    fill_random(&mut kek_nonce_bytes).map_err(|_| EncryptionError::EncryptFailed)?;
    let kek_nonce = Nonce::from(kek_nonce_bytes);
    let mut encrypted_dek = kek_nonce.to_vec();
    encrypted_dek.extend(
        kek_cipher
            .encrypt(&kek_nonce, dek.as_ref())
            .map_err(|_| EncryptionError::EncryptFailed)?,
    );

    Ok(Envelope {
        encrypted_dek,
        nonce: nonce.to_vec(),
        ciphertext,
    })
}

/// Decrypt an `Envelope` using the KEK.
pub fn decrypt(envelope: &Envelope, kek: &[u8]) -> Result<Vec<u8>, EncryptionError> {
    if kek.len() != 32 {
        return Err(EncryptionError::InvalidKeyLength(kek.len()));
    }

    // Unwrap the DEK with the KEK.
    if envelope.encrypted_dek.len() < 12 {
        return Err(EncryptionError::InvalidNonce);
    }
    let (kek_nonce_bytes, dek_ciphertext) = envelope.encrypted_dek.split_at(12);
    let kek_key = Key::<Aes256Gcm>::try_from(kek)
        .map_err(|_| EncryptionError::InvalidKeyLength(kek.len()))?;
    let kek_cipher = Aes256Gcm::new(&kek_key);
    let kek_nonce = Nonce::try_from(kek_nonce_bytes)
        .map_err(|_| EncryptionError::InvalidNonce)?;
    let dek = Zeroizing::new(
        kek_cipher
            .decrypt(&kek_nonce, dek_ciphertext)
            .map_err(|_| EncryptionError::DecryptFailed)?,
    );

    // Decrypt the data with the DEK.
    let dek_key = Key::<Aes256Gcm>::try_from(dek.as_slice())
        .map_err(|_| EncryptionError::InvalidKeyLength(dek.len()))?;
    let cipher = Aes256Gcm::new(&dek_key);
    if envelope.nonce.len() != 12 {
        return Err(EncryptionError::InvalidNonce);
    }
    let nonce = Nonce::try_from(envelope.nonce.as_slice())
        .map_err(|_| EncryptionError::InvalidNonce)?;
    cipher
        .decrypt(&nonce, envelope.ciphertext.as_ref())
        .map_err(|_| EncryptionError::DecryptFailed)
}

/// Rotate the KEK by re-wrapping all DEKs with a new KEK.
pub fn rotate_kek(envelope: &Envelope, old_kek: &[u8], new_kek: &[u8]) -> Result<Envelope, EncryptionError> {
    // Decrypt the envelope to get the plaintext, then re-encrypt with new KEK.
    let plaintext = decrypt(envelope, old_kek)?;
    encrypt(&plaintext, new_kek)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_kek() -> Vec<u8> {
        vec![0x42u8; 32]
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let kek = make_kek();
        let plaintext = b"secret graph data";
        let envelope = encrypt(plaintext, &kek).unwrap();
        let decrypted = decrypt(&envelope, &kek).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_wrong_kek_fails_decryption() {
        let kek = make_kek();
        let wrong_kek = vec![0x00u8; 32];
        let envelope = encrypt(b"secret", &kek).unwrap();
        assert!(decrypt(&envelope, &wrong_kek).is_err());
    }

    #[test]
    fn test_kek_rotation() {
        let old_kek = make_kek();
        let new_kek = vec![0xABu8; 32];
        let plaintext = b"rotate me";
        let env = encrypt(plaintext, &old_kek).unwrap();
        let rotated = rotate_kek(&env, &old_kek, &new_kek).unwrap();
        let decrypted = decrypt(&rotated, &new_kek).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_invalid_kek_length() {
        assert!(matches!(
            encrypt(b"data", &[0u8; 16]),
            Err(EncryptionError::InvalidKeyLength(16))
        ));
    }
}
