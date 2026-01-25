//! Content hashing utilities for cache keys.

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Compute SHA256 hash of a serializable value.
pub fn compute_hash<T: Serialize>(value: &T) -> String {
    let mut hasher = Sha256::new();
    let json = serde_json::to_string(value).expect("serialization failed");
    hasher.update(json.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compute_hash_deterministic() {
        let value = json!({"name": "test", "value": 42});
        let hash1 = compute_hash(&value);
        let hash2 = compute_hash(&value);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // SHA256 hex = 64 chars
    }

    #[test]
    fn test_compute_hash_different_values() {
        let v1 = json!({"a": 1});
        let v2 = json!({"a": 2});
        assert_ne!(compute_hash(&v1), compute_hash(&v2));
    }
}
