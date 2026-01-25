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
}
