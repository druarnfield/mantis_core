# Graph Cache Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement two-tier caching (inference + graph) with per-entity granularity for UnifiedGraph.

**Architecture:** Build InferenceCache with TTL/failure invalidation, then GraphCache with content-hash keys, integrate with existing MetadataCache, update consumers.

**Tech Stack:** Rust, rusqlite, serde_json, sha2, petgraph, existing MetadataCache infrastructure

---

## Task 1: Add Dependencies and Hash Utilities

**Files:**
- Modify: `Cargo.toml`
- Create: `src/cache/hash.rs`
- Modify: `src/cache/mod.rs`

**Step 1: Add sha2 dependency**

Add to `Cargo.toml`:
```toml
sha2 = "0.10"
```

**Step 2: Run cargo check to verify dependency**

Run: `cargo check`
Expected: SUCCESS (dependency resolved)

**Step 3: Create hash utilities module**

Create `src/cache/hash.rs`:
```rust
//! Content hashing utilities for cache keys.

use sha2::{Digest, Sha256};
use serde::Serialize;

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
```

**Step 4: Run tests**

Run: `cargo test cache::hash`
Expected: 2 tests PASS

**Step 5: Export hash module**

In `src/cache/mod.rs`, add after the module doc comment:
```rust
mod hash;
pub use hash::compute_hash;
```

**Step 6: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 7: Commit**

```bash
git add Cargo.toml src/cache/hash.rs src/cache/mod.rs
git commit -m "feat(cache): add SHA256 hash utilities for cache keys"
```

---

## Task 2: Implement InferenceVersion Type

**Files:**
- Create: `src/cache/inference_cache.rs`
- Modify: `src/cache/mod.rs`

**Step 1: Write test for InferenceVersion generation**

Create `src/cache/inference_cache.rs`:
```rust
//! Inference result caching with TTL and failure-based invalidation.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

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
        assert_ne!(v1, v2, "versions should differ when generated at different times");
    }

    #[test]
    fn test_inference_version_serialization() {
        let v = InferenceVersion::new();
        let json = serde_json::to_string(&v).unwrap();
        let deserialized: InferenceVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(v, deserialized);
    }
}
```

**Step 2: Run tests**

Run: `cargo test cache::inference_cache::tests`
Expected: 3 tests PASS

**Step 3: Export inference_cache module**

In `src/cache/mod.rs`, add after `mod hash;`:
```rust
mod inference_cache;
pub use inference_cache::InferenceVersion;
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/inference_cache.rs src/cache/mod.rs
git commit -m "feat(cache): add InferenceVersion for cache invalidation"
```

---

## Task 3: Implement InferenceCache Storage

**Files:**
- Modify: `src/cache/inference_cache.rs`
- Modify: `src/cache/mod.rs`

**Step 1: Write test for storing and retrieving inference cache**

Add to `src/cache/inference_cache.rs` after `InferenceVersion` impl:
```rust
use super::{CacheError, CacheResult, MetadataCache};

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
    storage: MetadataCache,
}

impl InferenceCache {
    /// Create a new inference cache.
    pub fn new(storage: MetadataCache) -> Self {
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

// Add to tests module:
#[cfg(test)]
mod inference_cache_tests {
    use super::*;

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
```

**Step 2: Run tests**

Run: `cargo test cache::inference_cache`
Expected: 6 tests PASS (3 from previous + 3 new)

**Step 3: Export InferenceCache**

In `src/cache/mod.rs`, update the export line:
```rust
pub use inference_cache::{InferenceCache, InferenceVersion};
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/inference_cache.rs src/cache/mod.rs
git commit -m "feat(cache): implement InferenceCache with TTL support"
```

---

## Task 4: Implement Graph Cache Key Generation

**Files:**
- Create: `src/cache/graph_cache.rs`
- Modify: `src/cache/mod.rs`

**Step 1: Write tests for cache key generation**

Create `src/cache/graph_cache.rs`:
```rust
//! Unified graph caching with per-entity granularity.

use super::{compute_hash, CacheError, CacheResult, InferenceVersion, MetadataCache};
use serde::{Deserialize, Serialize};

/// Helper for generating graph cache keys.
pub struct GraphCacheKey;

impl GraphCacheKey {
    /// Key for complete cached graph.
    pub fn complete(model_hash: &str, inference_version: &InferenceVersion) -> String {
        format!("graph:{}:{}:complete", model_hash, inference_version.as_str())
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
        
        assert_ne!(key1, key2, "different inference versions should produce different keys");
    }
}
```

**Step 2: Run tests**

Run: `cargo test cache::graph_cache::tests`
Expected: 5 tests PASS

**Step 3: Export graph_cache module**

In `src/cache/mod.rs`, add after inference_cache module:
```rust
mod graph_cache;
pub use graph_cache::GraphCacheKey;
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/graph_cache.rs src/cache/mod.rs
git commit -m "feat(cache): add graph cache key generation"
```

---

## Task 5: Implement Graph Cache Storage Types

**Files:**
- Modify: `src/cache/graph_cache.rs`

**Step 1: Write test for cached graph serialization**

Add to `src/cache/graph_cache.rs` after `GraphCacheKey` impl:
```rust
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

// Add to tests module:
#[cfg(test)]
mod storage_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cached_nodes_serialization() {
        let nodes = json!([{"id": "t1", "type": "table"}]);
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
        let graph = json!({"nodes": [], "edges": []});
        let cached = CachedGraph::new(graph.clone());
        
        assert_eq!(cached.version, 1);
        assert_eq!(cached.graph, graph);
        
        // Roundtrip
        let json = serde_json::to_string(&cached).unwrap();
        let deserialized: CachedGraph = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.graph, cached.graph);
    }
}
```

**Step 2: Run tests**

Run: `cargo test cache::graph_cache`
Expected: 8 tests PASS (5 previous + 3 new)

**Step 3: Export storage types**

In `src/cache/mod.rs`, update the export:
```rust
pub use graph_cache::{CachedEdges, CachedGraph, CachedNodes, GraphCacheKey};
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/graph_cache.rs src/cache/mod.rs
git commit -m "feat(cache): add graph storage types with versioning"
```

---

## Task 6: Implement GraphCacheConfig

**Files:**
- Modify: `src/cache/graph_cache.rs`
- Modify: `src/cache/mod.rs`

**Step 1: Write test for config creation**

Add to `src/cache/graph_cache.rs` after storage types:
```rust
use std::time::Duration;

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
            enable_compression: false,                 // Start simple
        }
    }
}

// Add to tests module:
#[cfg(test)]
mod config_tests {
    use super::*;

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
}
```

**Step 2: Run tests**

Run: `cargo test cache::graph_cache`
Expected: 10 tests PASS

**Step 3: Export GraphCacheConfig**

In `src/cache/mod.rs`, update export:
```rust
pub use graph_cache::{CachedEdges, CachedGraph, CachedNodes, GraphCacheConfig, GraphCacheKey};
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/graph_cache.rs src/cache/mod.rs
git commit -m "feat(cache): add GraphCacheConfig with defaults"
```

---

## Task 7: Implement GraphCache Struct and Constructor

**Files:**
- Modify: `src/cache/graph_cache.rs`
- Modify: `src/cache/mod.rs`

**Step 1: Write test for GraphCache creation**

Add to `src/cache/graph_cache.rs` after `GraphCacheConfig`:
```rust
use super::InferenceCache;
use std::sync::Arc;

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
}

// Add to tests module:
#[cfg(test)]
mod graph_cache_tests {
    use super::*;

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
}
```

**Step 2: Run tests**

Run: `cargo test cache::graph_cache`
Expected: 12 tests PASS

**Step 3: Export GraphCache**

In `src/cache/mod.rs`, update export:
```rust
pub use graph_cache::{CachedEdges, CachedGraph, CachedNodes, GraphCache, GraphCacheConfig, GraphCacheKey};
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/graph_cache.rs src/cache/mod.rs
git commit -m "feat(cache): implement GraphCache struct and constructor"
```

---

## Task 8: Implement Cache Clear Methods

**Files:**
- Modify: `src/cache/graph_cache.rs`

**Step 1: Write tests for cache clearing**

Add to `GraphCache` impl:
```rust
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
    pub fn stats(&self) -> CacheResult<CacheStats> {
        let graph_entries = self.storage.keys_with_prefix("graph:")?.len();
        let inference_entries = self.storage.keys_with_prefix("inference:")?.len();
        let overall = self.storage.stats()?;
        
        Ok(CacheStats {
            graph_entries,
            inference_entries,
            total_size_bytes: overall.total_size_bytes,
        })
    }
```

Add before tests module:
```rust
/// Statistics about cache usage.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of graph cache entries
    pub graph_entries: usize,
    /// Number of inference cache entries
    pub inference_entries: usize,
    /// Total size in bytes
    pub total_size_bytes: usize,
}
```

Add to tests module:
```rust
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
```

**Step 2: Run tests**

Run: `cargo test cache::graph_cache`
Expected: 15 tests PASS

**Step 3: Export CacheStats**

In `src/cache/mod.rs`, update export:
```rust
pub use graph_cache::{CachedEdges, CachedGraph, CachedNodes, CacheStats, GraphCache, GraphCacheConfig, GraphCacheKey};
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/cache/graph_cache.rs src/cache/mod.rs
git commit -m "feat(cache): add cache clearing and statistics methods"
```

---

## Task 9: Add Model Hash Computation

**Files:**
- Modify: `src/cache/graph_cache.rs`

**Step 1: Write test for model hash computation**

Add to `GraphCache` impl:
```rust
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
    }

    /// Compute content hash for a table (including measures).
    pub fn compute_table_hash(
        table: &serde_json::Value,
        measures: &serde_json::Value,
    ) -> String {
        use serde_json::json;
        compute_hash(&json!({
            "table": table,
            "measures": measures,
        }))
    }

    /// Compute content hash for a dimension.
    pub fn compute_dimension_hash(dimension: &serde_json::Value) -> String {
        compute_hash(dimension)
    }
```

Add to tests module:
```rust
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
```

**Step 2: Run tests**

Run: `cargo test cache::graph_cache`
Expected: 18 tests PASS

**Step 3: Run cargo check**

Run: `cargo check`
Expected: SUCCESS

**Step 4: Commit**

```bash
git add src/cache/graph_cache.rs
git commit -m "feat(cache): add model and entity hash computation"
```

---

## Task 10: Update Documentation

**Files:**
- Modify: `src/cache/mod.rs`

**Step 1: Update module documentation**

Replace the module doc comment in `src/cache/mod.rs`:
```rust
//! SQLite-based metadata and graph cache.
//!
//! Provides persistent caching of database metadata and semantic graphs
//! to avoid repeated introspection and graph construction.
//!
//! # Components
//!
//! - **MetadataCache**: Low-level SQLite key-value store
//! - **InferenceCache**: TTL-based cache for inference results with version tracking
//! - **GraphCache**: Two-tier cache for UnifiedGraph with per-entity granularity
//!
//! # Cache Architecture
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │     Inference Cache (Layer 1)       │
//! │  - TTL-based (configurable)         │
//! │  - Query failure triggers invalidation│
//! │  - Produces inference_version       │
//! └─────────────────────────────────────┘
//!                  │
//!                  ▼ (inference_version)
//! ┌─────────────────────────────────────┐
//! │      Graph Cache (Layer 2)          │
//! │  - Content-hash based               │
//! │  - Per-entity granularity           │
//! │  - Depends on inference_version     │
//! └─────────────────────────────────────┘
//! ```
//!
//! # Key Format
//!
//! **Metadata keys:**
//! ```text
//! {conn_hash}:schemas                     -> ["main", "analytics", ...]
//! {conn_hash}:tables:{schema}             -> [TableInfo, ...]
//! {conn_hash}:metadata:{schema}.{table}   -> TableMetadata
//! ```
//!
//! **Inference keys:**
//! ```text
//! inference:{model_hash}                  -> CachedInference
//! ```
//!
//! **Graph keys:**
//! ```text
//! graph:{model_hash}:{inference_version}:complete               -> CachedGraph
//! graph:{model_hash}:{inference_version}:table:{table_hash}:nodes    -> CachedNodes
//! graph:{model_hash}:{inference_version}:dimension:{dim_hash}:nodes  -> CachedNodes
//! graph:{model_hash}:{inference_version}:edges                       -> CachedEdges
//! ```
//!
//! # Example
//!
//! ```no_run
//! use mantis_core::cache::{MetadataCache, GraphCache, GraphCacheConfig};
//! use std::time::Duration;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = MetadataCache::open()?;
//! let config = GraphCacheConfig {
//!     inference_ttl: Duration::from_secs(3600),
//!     max_cache_size: Some(100 * 1024 * 1024),
//!     enable_compression: false,
//! };
//! let cache = GraphCache::new(storage, config);
//!
//! // Cache will be used by QueryPlanner and translation layer
//! # Ok(())
//! # }
//! ```
```

**Step 2: Run cargo doc to verify**

Run: `cargo doc --no-deps --package mantis_core`
Expected: SUCCESS (no warnings)

**Step 3: Commit**

```bash
git add src/cache/mod.rs
git commit -m "docs(cache): add comprehensive module documentation"
```

---

## Summary

This implementation plan delivers the foundation for graph caching:

**Completed:**
1. ✅ Hash utilities for content-based cache keys
2. ✅ InferenceVersion type for cache invalidation
3. ✅ InferenceCache with TTL and storage
4. ✅ Graph cache key generation
5. ✅ Graph storage types (CachedNodes, CachedEdges, CachedGraph)
6. ✅ GraphCacheConfig with sensible defaults
7. ✅ GraphCache struct and constructor
8. ✅ Cache clearing and statistics
9. ✅ Model and entity hash computation
10. ✅ Comprehensive documentation

**Next Steps (Separate Implementation Plan):**
- Integrate with UnifiedGraph builder
- Add graph serialization/deserialization
- Implement get_or_build with incremental caching
- Update QueryPlanner to use GraphCache
- Update translation layer integration
- Remove old ModelGraph and ColumnLineageGraph
- End-to-end integration testing

**Testing:** Each task includes tests that verify behavior in isolation. Integration with UnifiedGraph will be in a follow-up plan.
