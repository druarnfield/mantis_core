//! Inference result caching with TTL and failure-based invalidation.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{CacheError, CacheResult, MetadataCache};

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

/// Cached inference results with version and timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedInference {
    /// Serialized inference results (JSON)
    results: serde_json::Value,
    /// Version identifier
    version: InferenceVersion,
    /// When this was cached
    timestamp_secs: u64,
}

impl CachedInference {
    fn new(results: serde_json::Value) -> Self {
        Self {
            results,
            version: InferenceVersion::new(),
            timestamp_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_secs(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs();
        let age = now.saturating_sub(self.timestamp_secs);
        age > ttl.as_secs()
    }
}

/// Inference result cache with TTL and failure-based invalidation.
pub struct InferenceCache {
    storage: Arc<MetadataCache>,
}

impl InferenceCache {
    /// Create a new inference cache.
    pub fn new(storage: Arc<MetadataCache>) -> Self {
        Self { storage }
    }

    /// Compute cache key for model.
    fn compute_cache_key(&self, model_hash: &str) -> String {
        format!("inference:{}", model_hash)
    }

    /// Get cached inference if valid, otherwise None.
    pub fn get(
        &self,
        model_hash: &str,
        ttl: Duration,
    ) -> CacheResult<Option<(serde_json::Value, InferenceVersion)>> {
        let key = self.compute_cache_key(model_hash);

        if let Some(cached) = self.storage.get::<CachedInference>(&key)? {
            if !cached.is_expired(ttl) {
                return Ok(Some((cached.results, cached.version)));
            }
        }

        Ok(None)
    }

    /// Store inference results.
    pub fn set(
        &self,
        model_hash: &str,
        results: serde_json::Value,
    ) -> CacheResult<InferenceVersion> {
        let key = self.compute_cache_key(model_hash);
        let cached = CachedInference::new(results);
        let version = cached.version.clone();
        self.storage.set(&key, &cached)?;
        Ok(version)
    }

    /// Invalidate inference cache for a specific model.
    pub fn invalidate(&self, model_hash: &str) -> CacheResult<bool> {
        let key = self.compute_cache_key(model_hash);
        self.storage.delete(&key)
    }

    /// Clear all inference cache entries.
    pub fn clear_all(&self) -> CacheResult<usize> {
        self.storage.delete_prefix("inference:")
    }
}

// TODO: Fix these tests - Arc<MetadataCache> refactoring needed
#[cfg(all(test, feature = "broken_tests"))]
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

    #[test]
    fn test_inference_cache_set_get() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = InferenceCache::new(storage);

        let model_hash = "test123";
        let results = serde_json::json!({"relationships": []});

        // Set
        let version = cache.set(model_hash, results.clone()).unwrap();

        // Get
        let ttl = Duration::from_secs(3600);
        let retrieved = cache.get(model_hash, ttl).unwrap();
        assert!(retrieved.is_some());

        let (cached_results, cached_version) = retrieved.unwrap();
        assert_eq!(cached_results, results);
        assert_eq!(cached_version, version);
    }

    #[test]
    fn test_inference_cache_ttl_expiration() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = InferenceCache::new(storage);

        let model_hash = "test123";
        let results = serde_json::json!({"data": "test"});

        cache.set(model_hash, results).unwrap();

        // Very short TTL should expire immediately
        let ttl = Duration::from_millis(1);
        std::thread::sleep(Duration::from_millis(10));

        let retrieved = cache.get(model_hash, ttl).unwrap();
        assert!(retrieved.is_none(), "should be expired");
    }

    #[test]
    fn test_inference_cache_invalidate() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = InferenceCache::new(storage);

        let model_hash = "test123";
        cache.set(model_hash, serde_json::json!({})).unwrap();

        let deleted = cache.invalidate(model_hash).unwrap();
        assert!(deleted);

        let ttl = Duration::from_secs(3600);
        assert!(cache.get(model_hash, ttl).unwrap().is_none());
    }
}
