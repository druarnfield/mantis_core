//! Unified graph caching with per-entity granularity.

use super::{
    compute_hash, CacheError, CacheResult, InferenceCache, InferenceVersion, MetadataCache,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

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

/// Cached graph nodes with versioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedNodes {
    /// Schema version for forward compatibility
    pub version: u32,
    /// Serialized nodes (JSON array of GraphNode)
    pub nodes: serde_json::Value,
}

impl CachedNodes {
    const CURRENT_VERSION: u32 = 1;

    pub fn new(nodes: serde_json::Value) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            nodes,
        }
    }
}

/// Cached graph edges with versioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedEdges {
    /// Schema version for forward compatibility
    pub version: u32,
    /// Edges as (from_id, to_id, edge_type) tuples
    pub edges: Vec<(String, String, String)>,
}

impl CachedEdges {
    const CURRENT_VERSION: u32 = 1;

    pub fn new(edges: Vec<(String, String, String)>) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            edges,
        }
    }
}

/// Complete cached graph with versioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedGraph {
    /// Schema version
    pub version: u32,
    /// Serialized UnifiedGraph
    pub graph: serde_json::Value,
}

impl CachedGraph {
    const CURRENT_VERSION: u32 = 1;

    pub fn new(graph: serde_json::Value) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            graph,
        }
    }
}

/// Configuration for graph cache behavior.
#[derive(Debug, Clone)]
pub struct GraphCacheConfig {
    /// TTL for inference cache
    pub inference_ttl: Duration,
    /// Maximum cache size in bytes (None = unlimited)
    pub max_cache_size: Option<usize>,
    /// Enable zstd compression for cached graphs
    pub enable_compression: bool,
}

impl Default for GraphCacheConfig {
    fn default() -> Self {
        Self {
            inference_ttl: Duration::from_secs(3600), // 1 hour
            max_cache_size: Some(100 * 1024 * 1024),  // 100MB
            enable_compression: false,                // Start simple
        }
    }
}

/// Graph cache service with two-tier caching (inference + graph).
pub struct GraphCache {
    storage: MetadataCache,
    inference_cache: Arc<InferenceCache>,
    config: GraphCacheConfig,
}

impl GraphCache {
    /// Create a new graph cache.
    pub fn new(storage: MetadataCache, config: GraphCacheConfig) -> Self {
        let inference_cache = Arc::new(InferenceCache::new(storage.clone()));
        Self {
            storage,
            inference_cache,
            config,
        }
    }

    /// Get reference to inference cache.
    pub fn inference(&self) -> &InferenceCache {
        &self.inference_cache
    }

    /// Get configuration.
    pub fn config(&self) -> &GraphCacheConfig {
        &self.config
    }

    /// Clear all graph cache entries (keeps inference cache).
    pub fn clear_graph_cache(&self) -> CacheResult<usize> {
        self.storage.delete_prefix("graph:")
    }

    /// Clear all caches (graph + inference).
    pub fn clear_all(&self) -> CacheResult<()> {
        self.storage.delete_prefix("graph:")?;
        self.inference_cache.clear_all()?;
        Ok(())
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheResult<GraphCacheStats> {
        let graph_entries = self.storage.keys_with_prefix("graph:")?.len();
        let inference_entries = self.storage.keys_with_prefix("inference:")?.len();
        let overall = self.storage.stats()?;

        Ok(GraphCacheStats {
            graph_entries,
            inference_entries,
            total_size_bytes: overall.total_size_bytes,
        })
    }

    /// Compute content hash for model globals (defaults + calendars).
    pub fn compute_model_hash(
        defaults: &serde_json::Value,
        calendars: &serde_json::Value,
    ) -> String {
        use serde_json::json;
        compute_hash(&json!({
            "defaults": defaults,
            "calendars": calendars,
        }))
        .expect("Failed to compute model hash")
    }

    /// Compute content hash for a table (including measures).
    pub fn compute_table_hash(table: &serde_json::Value, measures: &serde_json::Value) -> String {
        use serde_json::json;
        compute_hash(&json!({
            "table": table,
            "measures": measures,
        }))
        .expect("Failed to compute table hash")
    }

    /// Compute content hash for a dimension.
    pub fn compute_dimension_hash(dimension: &serde_json::Value) -> String {
        compute_hash(dimension).expect("Failed to compute dimension hash")
    }
}

/// Statistics about graph cache usage.
#[derive(Debug, Clone)]
pub struct GraphCacheStats {
    /// Number of graph cache entries
    pub graph_entries: usize,
    /// Number of inference cache entries
    pub inference_entries: usize,
    /// Total size in bytes
    pub total_size_bytes: usize,
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

    #[test]
    fn test_cached_nodes_serialization() {
        let nodes = serde_json::json!([{"id": "t1", "type": "table"}]);
        let cached = CachedNodes::new(nodes.clone());

        assert_eq!(cached.version, 1);
        assert_eq!(cached.nodes, nodes);

        // Roundtrip
        let json = serde_json::to_string(&cached).unwrap();
        let deserialized: CachedNodes = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.version, cached.version);
        assert_eq!(deserialized.nodes, cached.nodes);
    }

    #[test]
    fn test_cached_edges_serialization() {
        let edges = vec![
            ("n1".to_string(), "n2".to_string(), "BELONGS_TO".to_string()),
            ("n2".to_string(), "n3".to_string(), "REFERENCES".to_string()),
        ];
        let cached = CachedEdges::new(edges.clone());

        assert_eq!(cached.version, 1);
        assert_eq!(cached.edges, edges);

        // Roundtrip
        let json = serde_json::to_string(&cached).unwrap();
        let deserialized: CachedEdges = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.edges, cached.edges);
    }

    #[test]
    fn test_cached_graph_serialization() {
        let graph = serde_json::json!({"nodes": [], "edges": []});
        let cached = CachedGraph::new(graph.clone());

        assert_eq!(cached.version, 1);
        assert_eq!(cached.graph, graph);

        // Roundtrip
        let json = serde_json::to_string(&cached).unwrap();
        let deserialized: CachedGraph = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.graph, cached.graph);
    }

    #[test]
    fn test_default_config() {
        let config = GraphCacheConfig::default();
        assert_eq!(config.inference_ttl, Duration::from_secs(3600));
        assert_eq!(config.max_cache_size, Some(100 * 1024 * 1024));
        assert!(!config.enable_compression);
    }

    #[test]
    fn test_custom_config() {
        let config = GraphCacheConfig {
            inference_ttl: Duration::from_secs(7200),
            max_cache_size: None,
            enable_compression: true,
        };
        assert_eq!(config.inference_ttl, Duration::from_secs(7200));
        assert_eq!(config.max_cache_size, None);
        assert!(config.enable_compression);
    }

    #[test]
    fn test_graph_cache_creation() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let config = GraphCacheConfig::default();
        let cache = GraphCache::new(storage, config);

        assert_eq!(cache.config().inference_ttl, Duration::from_secs(3600));
    }

    #[test]
    fn test_graph_cache_inference_access() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = GraphCache::new(storage, GraphCacheConfig::default());

        // Should be able to access inference cache
        let _inference = cache.inference();
    }

    #[test]
    fn test_clear_graph_cache() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = GraphCache::new(storage.clone(), GraphCacheConfig::default());

        // Add some graph entries
        storage.set("graph:test:v1:complete", &"data").unwrap();
        storage.set("graph:test:v1:edges", &"edges").unwrap();

        // Add inference entry
        storage.set("inference:test", &"inference").unwrap();

        // Clear graph cache only
        let deleted = cache.clear_graph_cache().unwrap();
        assert_eq!(deleted, 2);

        // Inference should still exist
        let inf: Option<String> = storage.get("inference:test").unwrap();
        assert!(inf.is_some());
    }

    #[test]
    fn test_clear_all() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = GraphCache::new(storage.clone(), GraphCacheConfig::default());

        storage.set("graph:test:v1:complete", &"data").unwrap();
        storage.set("inference:test", &"inference").unwrap();

        cache.clear_all().unwrap();

        let graph: Option<String> = storage.get("graph:test:v1:complete").unwrap();
        let inf: Option<String> = storage.get("inference:test").unwrap();
        assert!(graph.is_none());
        assert!(inf.is_none());
    }

    #[test]
    fn test_cache_stats() {
        let storage = MetadataCache::open_in_memory().unwrap();
        let cache = GraphCache::new(storage.clone(), GraphCacheConfig::default());

        storage.set("graph:m1:v1:complete", &"data1").unwrap();
        storage.set("graph:m1:v1:edges", &"data2").unwrap();
        storage.set("inference:m1", &"inf").unwrap();

        let stats = cache.stats().unwrap();
        assert_eq!(stats.graph_entries, 2);
        assert_eq!(stats.inference_entries, 1);
        assert!(stats.total_size_bytes > 0);
    }

    #[test]
    fn test_compute_model_hash() {
        let defaults = serde_json::json!({"timezone": "UTC"});
        let calendars = serde_json::json!({"fiscal": {}});

        let hash1 = GraphCache::compute_model_hash(&defaults, &calendars);
        let hash2 = GraphCache::compute_model_hash(&defaults, &calendars);
        assert_eq!(hash1, hash2, "same inputs should produce same hash");
        assert_eq!(hash1.len(), 64, "SHA256 hex = 64 chars");
    }

    #[test]
    fn test_compute_table_hash() {
        let table = serde_json::json!({"name": "orders", "source": "db.orders"});
        let measures = serde_json::json!({"total": {"expr": "SUM(amount)"}});

        let hash1 = GraphCache::compute_table_hash(&table, &measures);
        let hash2 = GraphCache::compute_table_hash(&table, &measures);
        assert_eq!(hash1, hash2);

        // Different measures should produce different hash
        let different_measures = serde_json::json!({"count": {"expr": "COUNT(*)"}});
        let hash3 = GraphCache::compute_table_hash(&table, &different_measures);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_compute_dimension_hash() {
        let dim = serde_json::json!({"name": "customer", "key": "customer_id"});

        let hash1 = GraphCache::compute_dimension_hash(&dim);
        let hash2 = GraphCache::compute_dimension_hash(&dim);
        assert_eq!(hash1, hash2);
    }
}
