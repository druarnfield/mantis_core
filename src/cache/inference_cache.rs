//! Inference result caching with TTL and failure-based invalidation.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique version identifier for inference results.
///
/// Format: `v1_{unix_timestamp}`
/// When inference re-runs, version changes, invalidating dependent graph cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceVersion(String);

impl InferenceVersion {
    /// Generate a new version with current timestamp.
    pub fn new() -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs();
        Self(format!("v1_{}", ts))
    }

    /// Get the version string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for InferenceVersion {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_inference_version_format() {
        let v = InferenceVersion::new();
        assert!(v.as_str().starts_with("v1_"));
        assert!(v.as_str().len() > 3);
    }

    #[test]
    fn test_inference_version_changes() {
        let v1 = InferenceVersion::new();
        sleep(Duration::from_millis(10));
        let v2 = InferenceVersion::new();
        assert_ne!(
            v1, v2,
            "versions should differ when generated at different times"
        );
    }

    #[test]
    fn test_inference_version_serialization() {
        let v = InferenceVersion::new();
        let json = serde_json::to_string(&v).unwrap();
        let deserialized: InferenceVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(v, deserialized);
    }
}
