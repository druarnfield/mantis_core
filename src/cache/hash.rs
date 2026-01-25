//! Content hashing utilities for cache keys.

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Compute SHA256 hash of a serializable value.
///
/// The value is serialized to JSON before hashing, ensuring deterministic output.
/// Returns a 64-character lowercase hexadecimal string.
///
/// # Errors
/// Returns an error if the value cannot be serialized to JSON.
pub fn compute_hash<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let json = serde_json::to_string(value)?;
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compute_hash_deterministic() {
        let value = json!({"name": "test", "value": 42});
        let hash1 = compute_hash(&value).unwrap();
        let hash2 = compute_hash(&value).unwrap();
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // SHA256 hex = 64 chars
    }

    #[test]
    fn test_compute_hash_different_values() {
        let v1 = json!({"a": 1});
        let v2 = json!({"a": 2});
        assert_ne!(compute_hash(&v1).unwrap(), compute_hash(&v2).unwrap());
    }
}
