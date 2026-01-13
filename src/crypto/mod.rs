//! Cryptographic utilities for secure credential storage.
//!
//! This module provides AES-256-GCM encryption for storing connection credentials
//! securely in the SQLite cache. It uses the `ring` library for cryptographic operations.
//!
//! # Example
//!
//! ```rust
//! use mantis::crypto::{generate_master_key, encrypt, decrypt};
//!
//! let key = generate_master_key();
//! let plaintext = b"my secret password";
//!
//! let ciphertext = encrypt(&key, plaintext).expect("encryption failed");
//! let decrypted = decrypt(&key, &ciphertext).expect("decryption failed");
//!
//! assert_eq!(plaintext.to_vec(), decrypted);
//! ```

pub mod master_key;

// Re-export for convenience
pub use master_key::{init as init_master_key, get as get_master_key};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use thiserror::Error;

/// The length of an AES-256 key in bytes.
pub const KEY_LENGTH: usize = 32;

/// Result type for cryptographic operations.
pub type CryptoResult<T> = Result<T, CryptoError>;

/// Errors that can occur during cryptographic operations.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// Encryption operation failed.
    #[error("encryption failed")]
    EncryptionFailed,

    /// Decryption operation failed.
    #[error("decryption failed")]
    DecryptionFailed,

    /// The provided key has an invalid length (must be 32 bytes).
    #[error("invalid key length: expected {KEY_LENGTH} bytes")]
    InvalidKeyLength,

    /// The ciphertext is malformed or too short.
    #[error("invalid ciphertext")]
    InvalidCiphertext,
}

/// Generates a cryptographically secure 32-byte master key.
///
/// Uses `ring::rand::SystemRandom` for secure random generation.
///
/// # Panics
///
/// Panics if the system's random number generator fails.
pub fn generate_master_key() -> [u8; KEY_LENGTH] {
    let rng = SystemRandom::new();
    let mut key = [0u8; KEY_LENGTH];
    rng.fill(&mut key).expect("failed to generate random key");
    key
}

/// Encodes a master key as a base64 string.
///
/// This is useful for storing the key in environment variables or configuration files.
pub fn encode_key(key: &[u8; KEY_LENGTH]) -> String {
    BASE64.encode(key)
}

/// Decodes a base64-encoded master key.
///
/// # Errors
///
/// Returns `CryptoError::InvalidKeyLength` if the decoded key is not exactly 32 bytes.
pub fn decode_key(encoded: &str) -> CryptoResult<[u8; KEY_LENGTH]> {
    let bytes = BASE64
        .decode(encoded)
        .map_err(|_| CryptoError::InvalidKeyLength)?;

    bytes
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)
}

/// Encrypts plaintext using AES-256-GCM.
///
/// The function:
/// 1. Generates a random 12-byte nonce
/// 2. Encrypts the plaintext with AES-256-GCM
/// 3. Prepends the nonce to the ciphertext
/// 4. Returns the result as a base64-encoded string
///
/// # Arguments
///
/// * `key` - A 32-byte AES-256 key
/// * `plaintext` - The data to encrypt
///
/// # Errors
///
/// Returns `CryptoError::EncryptionFailed` if encryption fails.
pub fn encrypt(key: &[u8; KEY_LENGTH], plaintext: &[u8]) -> CryptoResult<String> {
    let rng = SystemRandom::new();

    // Generate random nonce
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rng.fill(&mut nonce_bytes)
        .map_err(|_| CryptoError::EncryptionFailed)?;

    // Create the encryption key
    let unbound_key =
        UnboundKey::new(&AES_256_GCM, key).map_err(|_| CryptoError::EncryptionFailed)?;
    let sealing_key = LessSafeKey::new(unbound_key);

    // Create the nonce
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    // Encrypt in place - we need to append the auth tag
    let mut in_out = plaintext.to_vec();
    sealing_key
        .seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| CryptoError::EncryptionFailed)?;

    // Prepend nonce to ciphertext
    let mut result = Vec::with_capacity(NONCE_LEN + in_out.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&in_out);

    Ok(BASE64.encode(&result))
}

/// Decrypts a base64-encoded ciphertext using AES-256-GCM.
///
/// The function:
/// 1. Decodes the base64 input
/// 2. Extracts the 12-byte nonce from the beginning
/// 3. Decrypts the remaining ciphertext
/// 4. Returns the plaintext
///
/// # Arguments
///
/// * `key` - A 32-byte AES-256 key
/// * `ciphertext` - The base64-encoded ciphertext (nonce + encrypted data)
///
/// # Errors
///
/// Returns `CryptoError::InvalidCiphertext` if the input is malformed.
/// Returns `CryptoError::DecryptionFailed` if decryption fails (wrong key or tampered data).
pub fn decrypt(key: &[u8; KEY_LENGTH], ciphertext: &str) -> CryptoResult<Vec<u8>> {
    // Decode base64
    let data = BASE64
        .decode(ciphertext)
        .map_err(|_| CryptoError::InvalidCiphertext)?;

    // Check minimum length (nonce + at least 1 byte + tag)
    if data.len() < NONCE_LEN + 1 {
        return Err(CryptoError::InvalidCiphertext);
    }

    // Extract nonce and ciphertext
    let (nonce_bytes, encrypted) = data.split_at(NONCE_LEN);
    let nonce_array: [u8; NONCE_LEN] = nonce_bytes
        .try_into()
        .map_err(|_| CryptoError::InvalidCiphertext)?;

    // Create the decryption key
    let unbound_key =
        UnboundKey::new(&AES_256_GCM, key).map_err(|_| CryptoError::DecryptionFailed)?;
    let opening_key = LessSafeKey::new(unbound_key);

    // Create the nonce
    let nonce = Nonce::assume_unique_for_key(nonce_array);

    // Decrypt in place
    let mut in_out = encrypted.to_vec();
    let plaintext = opening_key
        .open_in_place(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| CryptoError::DecryptionFailed)?;

    Ok(plaintext.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_key() {
        let key = generate_master_key();
        assert_eq!(key.len(), KEY_LENGTH);

        // Verify keys are random (not all zeros)
        assert!(key.iter().any(|&b| b != 0));

        // Verify two keys are different
        let key2 = generate_master_key();
        assert_ne!(key, key2);
    }

    #[test]
    fn test_encode_decode_key() {
        let key = generate_master_key();
        let encoded = encode_key(&key);
        let decoded = decode_key(&encoded).expect("decode should succeed");
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_decode_key_invalid_base64() {
        let result = decode_key("not valid base64!!!");
        assert!(matches!(result, Err(CryptoError::InvalidKeyLength)));
    }

    #[test]
    fn test_decode_key_wrong_length() {
        // Valid base64 but wrong length
        let result = decode_key(&BASE64.encode(b"too short"));
        assert!(matches!(result, Err(CryptoError::InvalidKeyLength)));
    }

    #[test]
    fn test_encrypt_decrypt() {
        let key = generate_master_key();
        let plaintext = b"Hello, World! This is a secret message.";

        let ciphertext = encrypt(&key, plaintext).expect("encryption should succeed");
        let decrypted = decrypt(&key, &ciphertext).expect("decryption should succeed");

        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_encrypt_decrypt_empty() {
        let key = generate_master_key();
        let plaintext = b"";

        let ciphertext = encrypt(&key, plaintext).expect("encryption should succeed");
        let decrypted = decrypt(&key, &ciphertext).expect("decryption should succeed");

        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_encrypt_produces_different_ciphertext() {
        // Due to random nonce, same plaintext should produce different ciphertext
        let key = generate_master_key();
        let plaintext = b"same message";

        let ct1 = encrypt(&key, plaintext).expect("encryption should succeed");
        let ct2 = encrypt(&key, plaintext).expect("encryption should succeed");

        assert_ne!(ct1, ct2);

        // But both should decrypt to the same plaintext
        let pt1 = decrypt(&key, &ct1).expect("decryption should succeed");
        let pt2 = decrypt(&key, &ct2).expect("decryption should succeed");
        assert_eq!(pt1, pt2);
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = generate_master_key();
        let key2 = generate_master_key();
        let plaintext = b"secret data";

        let ciphertext = encrypt(&key1, plaintext).expect("encryption should succeed");
        let result = decrypt(&key2, &ciphertext);

        assert!(matches!(result, Err(CryptoError::DecryptionFailed)));
    }

    #[test]
    fn test_invalid_ciphertext() {
        let key = generate_master_key();

        // Not valid base64
        let result = decrypt(&key, "not valid base64!!!");
        assert!(matches!(result, Err(CryptoError::InvalidCiphertext)));

        // Valid base64 but too short (less than nonce + tag)
        let result = decrypt(&key, &BASE64.encode(b"short"));
        assert!(matches!(result, Err(CryptoError::InvalidCiphertext)));
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let key = generate_master_key();
        let plaintext = b"sensitive data";

        let ciphertext = encrypt(&key, plaintext).expect("encryption should succeed");

        // Decode, tamper, re-encode
        let mut data = BASE64.decode(&ciphertext).unwrap();
        if let Some(byte) = data.last_mut() {
            *byte ^= 0xFF; // Flip all bits in last byte
        }
        let tampered = BASE64.encode(&data);

        let result = decrypt(&key, &tampered);
        assert!(matches!(result, Err(CryptoError::DecryptionFailed)));
    }
}
