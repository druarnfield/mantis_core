//! Unified graph caching with per-entity granularity.

use super::{compute_hash, CacheError, CacheResult, InferenceVersion, MetadataCache};
use serde::{Deserialize, Serialize};

/// Helper for generating graph cache keys.
pub struct GraphCacheKey;

impl GraphCacheKey {
    /// Key for complete cached graph.
    pub fn complete(model_hash: &str, inference_version: &InferenceVersion) -> String {
        format!(
            "graph:{}:{}:complete",
            model_hash,
            inference_version.as_str()
        )
    }

    /// Key for table nodes.
    pub fn table_nodes(
        model_hash: &str,
        inference_version: &InferenceVersion,
        table_hash: &str,
    ) -> String {
        format!(
            "graph:{}:{}:table:{}:nodes",
            model_hash,
            inference_version.as_str(),
            table_hash
        )
    }

    /// Key for dimension nodes.
    pub fn dimension_nodes(
        model_hash: &str,
        inference_version: &InferenceVersion,
        dimension_hash: &str,
    ) -> String {
        format!(
            "graph:{}:{}:dimension:{}:nodes",
            model_hash,
            inference_version.as_str(),
            dimension_hash
        )
    }

    /// Key for all edges.
    pub fn edges(model_hash: &str, inference_version: &InferenceVersion) -> String {
        format!("graph:{}:{}:edges", model_hash, inference_version.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_complete_key_format() {
        let version = InferenceVersion::new();
        let key = GraphCacheKey::complete("abc123", &version);
        assert!(key.starts_with("graph:abc123:"));
        assert!(key.ends_with(":complete"));
        assert!(key.contains(version.as_str()));
    }

    #[test]
    fn test_table_nodes_key_format() {
        let version = InferenceVersion::new();
        let key = GraphCacheKey::table_nodes("model123", &version, "table456");
        assert!(key.starts_with("graph:model123:"));
        assert!(key.contains(":table:table456:nodes"));
    }

    #[test]
    fn test_dimension_nodes_key_format() {
        let version = InferenceVersion::new();
        let key = GraphCacheKey::dimension_nodes("model123", &version, "dim789");
        assert!(key.starts_with("graph:model123:"));
        assert!(key.contains(":dimension:dim789:nodes"));
    }

    #[test]
    fn test_edges_key_format() {
        let version = InferenceVersion::new();
        let key = GraphCacheKey::edges("model123", &version);
        assert!(key.starts_with("graph:model123:"));
        assert!(key.ends_with(":edges"));
    }

    #[test]
    fn test_keys_include_inference_version() {
        let v1 = InferenceVersion::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let v2 = InferenceVersion::new();

        let key1 = GraphCacheKey::complete("same_model", &v1);
        let key2 = GraphCacheKey::complete("same_model", &v2);

        assert_ne!(
            key1, key2,
            "different inference versions should produce different keys"
        );
    }
}
