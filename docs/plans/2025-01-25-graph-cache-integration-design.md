# Graph Cache Integration Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a two-tier cache system for UnifiedGraph with per-entity granularity and inference-aware invalidation.

**Architecture:** SQLite-based cache with content-hash keys for graph components and TTL-based inference cache. Graph cache depends on inference version for automatic invalidation.

**Tech Stack:** SQLite (existing MetadataCache), SHA256 hashing, JSON serialization, optional zstd compression

---

## Background

The UnifiedGraph implementation (completed in Tasks 1-5) provides a column-level semantic graph that unifies entity-level and column-level queries. However, building the graph is expensive and currently happens on every query. This design adds a proper caching layer to:

1. Build UnifiedGraph once per model version
2. Invalidate cache granularly when only specific entities change
3. Coordinate with inference cache to auto-invalidate when schema changes
4. Persist cache between sessions for faster startup

## Section 1: Cache Architecture & Key Structure

### Cache Key Hierarchy

Keys follow a structured format that encodes dependencies:

```
graph:{model_hash}:{inference_version}:table:{table_hash}:node
graph:{model_hash}:{inference_version}:dimension:{dimension_hash}:node
graph:{model_hash}:{inference_version}:edges
graph:{model_hash}:{inference_version}:complete
```

**Components:**
- `model_hash`: SHA256 of model defaults + calendars (global config)
- `inference_version`: Version string from inference cache (e.g., `v1_2024-01-25T10:30:00Z`)
- `table_hash`: SHA256 of specific Table definition (including its MeasureBlock)
- `dimension_hash`: SHA256 of specific Dimension definition

### Hash Strategy

**Per-Entity Hashing (Chosen Approach):**

```rust
fn compute_table_hash(table: &Table, measures: &MeasureBlock) -> String {
    let mut hasher = Sha256::new();
    
    // Hash table definition
    hasher.update(serde_json::to_string(table).unwrap());
    
    // Hash associated measures
    hasher.update(serde_json::to_string(measures).unwrap());
    
    format!("{:x}", hasher.finalize())
}

fn compute_dimension_hash(dimension: &Dimension) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_string(dimension).unwrap());
    format!("{:x}", hasher.finalize())
}

fn compute_model_hash(model: &Model) -> String {
    let mut hasher = Sha256::new();
    
    // Only hash global config that affects all entities
    hasher.update(serde_json::to_string(&model.defaults).unwrap());
    hasher.update(serde_json::to_string(&model.calendars).unwrap());
    
    format!("{:x}", hasher.finalize())
}
```

**Why Per-Entity?**
- Changing one table definition only invalidates that table's cache
- Other tables, dimensions remain cached
- Minimizes cache churn during development
- Model structure (Tables, Dimensions, Measures self-contained) supports this

**Trade-offs:**
- ✅ Minimal invalidation on partial changes
- ✅ Incremental graph building
- ⚠️ More complex cache key management
- ⚠️ Need to track entity dependencies for edge caching

### Storage Format

**JSON with Optional Compression:**

```rust
pub struct CachedGraphNode {
    pub version: u32,  // Schema version for forward compatibility
    pub node_type: String,  // "table", "dimension", "column", "measure", "calendar"
    pub node_data: serde_json::Value,  // NodeData serialized
}

pub struct CachedGraphEdges {
    pub version: u32,
    pub edges: Vec<(String, String, String)>,  // (from_id, to_id, edge_type)
}
```

**Compression Strategy:**
- Store uncompressed by default for simplicity
- Add zstd compression if cache size becomes issue
- Version field allows migration to compressed format later

## Section 2: Cache Population & Invalidation Logic

### Two-Tier Cache System

```
┌─────────────────────────────────────┐
│     Inference Cache (Layer 1)       │
│  - TTL-based (configurable)         │
│  - Query failure triggers invalidation│
│  - Produces inference_version       │
└─────────────────────────────────────┘
                 │
                 ▼ (inference_version)
┌─────────────────────────────────────┐
│      Graph Cache (Layer 2)          │
│  - Content-hash based               │
│  - Per-entity granularity           │
│  - Depends on inference_version     │
└─────────────────────────────────────┘
```

### Inference Cache Invalidation

```rust
impl InferenceCache {
    /// Get cached inference or run fresh inference
    pub fn get_or_infer(
        &self,
        model: &Model,
        db_connection: &DatabaseConnection,
        config: &InferenceConfig,
    ) -> Result<(InferenceResults, InferenceVersion)> {
        let cache_key = self.compute_cache_key(model);
        
        // Check if cached inference exists and is valid
        if let Some(cached) = self.storage.get(&cache_key)? {
            if !self.is_expired(&cached, config.ttl) {
                return Ok((cached.results, cached.version));
            }
        }
        
        // Run fresh inference
        let results = run_inference(model, db_connection)?;
        let version = InferenceVersion::new();
        
        self.storage.set(&cache_key, &CachedInference {
            results: results.clone(),
            version: version.clone(),
            timestamp: SystemTime::now(),
        })?;
        
        Ok((results, version))
    }
    
    /// Invalidate inference cache for specific table (on query failure)
    pub fn invalidate_on_error(&self, model_hash: &str, table: &str) {
        let cache_key = format!("inference:{}:table:{}", model_hash, table);
        self.storage.delete(&cache_key).ok();
        
        // This will bump inference_version on next get_or_infer,
        // which automatically invalidates all graph cache entries
    }
    
    fn is_expired(&self, cached: &CachedInference, ttl: Duration) -> bool {
        cached.timestamp.elapsed().unwrap_or(Duration::MAX) > ttl
    }
}

pub struct InferenceVersion(String);

impl InferenceVersion {
    pub fn new() -> Self {
        // Format: v{counter}_{timestamp}
        InferenceVersion(format!("v1_{}", 
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
    }
}
```

**Invalidation Triggers:**
1. **Time-based (TTL)**: Configurable, default 1 hour
2. **Query failure**: When query fails, invalidate inference for affected tables
3. **Manual**: Clear all inference cache (forces full rebuild)

**Why Inference Version in Graph Keys?**
- When inference refreshes (TTL or failure), version changes
- All graph cache entries with old inference_version become stale automatically
- No need to manually invalidate graph cache when schema changes
- Graph cache naturally stays in sync with database schema

### Graph Cache Population Flow

```rust
impl GraphCache {
    pub fn get_or_build(
        &self,
        model: &Model,
        inference: &InferenceEngine,
        db_connection: &DatabaseConnection,
    ) -> Result<UnifiedGraph, CacheError> {
        // Step 1: Get inference with version
        let (inference_results, inference_version) = 
            self.inference_cache.get_or_infer(model, db_connection, &self.config)?;
        
        // Step 2: Compute hashes
        let model_hash = compute_model_hash(model);
        let complete_key = format!("graph:{}:{}:complete", model_hash, inference_version);
        
        // Step 3: Check for complete cached graph
        if let Some(cached_graph) = self.storage.get(&complete_key)? {
            return Ok(deserialize_graph(&cached_graph)?);
        }
        
        // Step 4: Build incrementally with per-entity caching
        let mut graph = UnifiedGraph::new();
        
        // Build nodes (with per-entity cache check)
        for (table_name, table) in &model.tables {
            let table_hash = compute_table_hash(table, &model.measures[table_name]);
            let node_key = format!("graph:{}:{}:table:{}:node", 
                model_hash, inference_version, table_hash);
            
            if let Some(cached_nodes) = self.storage.get(&node_key)? {
                // Reuse cached nodes for this table
                graph.add_cached_nodes(deserialize_nodes(&cached_nodes)?);
            } else {
                // Build fresh nodes for this table
                let nodes = build_table_nodes(table, &model.measures[table_name], &inference_results);
                graph.add_nodes(nodes.clone());
                
                // Cache these nodes
                self.storage.set(&node_key, &serialize_nodes(&nodes))?;
            }
        }
        
        for (dim_name, dimension) in &model.dimensions {
            let dimension_hash = compute_dimension_hash(dimension);
            let node_key = format!("graph:{}:{}:dimension:{}:node",
                model_hash, inference_version, dimension_hash);
            
            if let Some(cached_nodes) = self.storage.get(&node_key)? {
                graph.add_cached_nodes(deserialize_nodes(&cached_nodes)?);
            } else {
                let nodes = build_dimension_nodes(dimension);
                graph.add_nodes(nodes.clone());
                self.storage.set(&node_key, &serialize_nodes(&nodes))?;
            }
        }
        
        // Build edges (cached as single unit for now)
        let edges_key = format!("graph:{}:{}:edges", model_hash, inference_version);
        if let Some(cached_edges) = self.storage.get(&edges_key)? {
            graph.add_edges(deserialize_edges(&cached_edges)?);
        } else {
            let edges = build_all_edges(&graph, model);
            graph.add_edges(edges.clone());
            self.storage.set(&edges_key, &serialize_edges(&edges))?;
        }
        
        // Step 5: Cache complete graph
        self.storage.set(&complete_key, &serialize_graph(&graph))?;
        
        Ok(graph)
    }
}
```

**Cache Hit Scenarios:**
1. **Full cache hit**: Complete graph key exists → instant load
2. **Partial cache hit**: Some entities unchanged → build only changed entities
3. **Cache miss**: Build entire graph, cache all components

**Invalidation on Model Change:**
- Change table definition → only that table_hash changes → rebuild that table's nodes
- Add new table → new table_hash → build new table, reuse existing
- Change dimension → only that dimension_hash changes
- Change defaults/calendars → model_hash changes → full rebuild (affects all entities)

## Section 3: Cache Service & Graph Loading

### GraphCache Service

```rust
pub struct GraphCache {
    storage: MetadataCache,
    inference_cache: Arc<InferenceCache>,
    config: GraphCacheConfig,
}

#[derive(Debug, Clone)]
pub struct GraphCacheConfig {
    pub inference_ttl: Duration,
    pub max_cache_size: Option<usize>,
    pub enable_compression: bool,
}

impl GraphCache {
    pub fn new(storage: MetadataCache, config: GraphCacheConfig) -> Self {
        Self {
            storage,
            inference_cache: Arc::new(InferenceCache::new(storage.clone())),
            config,
        }
    }
    
    /// Main entry point: get cached graph or build fresh
    pub fn get_or_build(
        &self,
        model: &Model,
        inference: &InferenceEngine,
        db_connection: &DatabaseConnection,
    ) -> Result<UnifiedGraph, CacheError> {
        // Implementation shown in Section 2
    }
    
    /// Clear all graph cache (keeps inference cache)
    pub fn clear_graph_cache(&self) -> Result<(), CacheError> {
        self.storage.delete_prefix("graph:")?;
        Ok(())
    }
    
    /// Clear all caches (graph + inference)
    pub fn clear_all(&self) -> Result<(), CacheError> {
        self.storage.delete_prefix("graph:")?;
        self.storage.delete_prefix("inference:")?;
        Ok(())
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            graph_entries: self.storage.count_prefix("graph:"),
            inference_entries: self.storage.count_prefix("inference:"),
            total_size_bytes: self.storage.total_size(),
        }
    }
}
```

### InferenceCache Service

```rust
pub struct InferenceCache {
    storage: MetadataCache,
}

struct CachedInference {
    results: InferenceResults,
    version: InferenceVersion,
    timestamp: SystemTime,
}

impl InferenceCache {
    pub fn new(storage: MetadataCache) -> Self {
        Self { storage }
    }
    
    /// Get current inference version (without running inference)
    pub fn get_version(&self, model: &Model) -> Option<InferenceVersion> {
        let cache_key = self.compute_cache_key(model);
        self.storage.get(&cache_key).ok().flatten()
            .map(|cached: CachedInference| cached.version)
    }
    
    fn compute_cache_key(&self, model: &Model) -> String {
        let model_hash = compute_model_hash(model);
        format!("inference:{}", model_hash)
    }
}
```

## Section 4: Implementation Components & Integration Points

### New Components

**`src/cache/graph_cache.rs`:**
- `GraphCache` struct with get_or_build, clear, stats methods
- Per-entity cache key generation
- Incremental graph building logic
- Serialization/deserialization helpers

**`src/cache/inference_cache.rs`:**
- `InferenceCache` wrapper around MetadataCache
- TTL-based invalidation
- Query failure handling
- Version generation and tracking

**`src/cache/mod.rs` (updates):**
- Export GraphCache and InferenceCache
- Add cache key helpers for graph entries
- Extend CacheKey with graph-specific methods

### Integration Points

**QueryPlanner (Task 6):**

```rust
// OLD (builds graph every time)
impl QueryPlanner {
    pub fn plan(&self, query: &Query) -> Result<ExecutionPlan> {
        let graph = build_model_graph(&self.model)?;  // ❌ Expensive
        self.plan_with_graph(query, &graph)
    }
}

// NEW (uses cache)
impl QueryPlanner {
    pub fn plan(&self, query: &Query) -> Result<ExecutionPlan> {
        let graph = self.graph_cache.get_or_build(
            &self.model,
            &self.inference_engine,
            &self.db_connection,
        )?;  // ✅ Cached
        
        self.plan_with_graph(query, &graph)
    }
}
```

**Translation Layer (Task 7):**

```rust
// OLD (builds graph every time)
pub fn translate_query(model: &Model, query: &Query) -> Result<TranslatedQuery> {
    let graph = build_column_lineage_graph(&model)?;  // ❌ Expensive
    translate_with_graph(query, &graph)
}

// NEW (receives cached graph)
pub fn translate_query(
    graph: &UnifiedGraph,  // ✅ Passed from caller who got it from cache
    query: &Query,
) -> Result<TranslatedQuery> {
    translate_with_graph(query, graph)
}
```

**Error Handling (Query Failure Invalidation):**

```rust
impl QueryExecutor {
    pub fn execute(&self, query: &Query) -> Result<QueryResult> {
        match self.execute_internal(query) {
            Ok(result) => Ok(result),
            Err(e) if e.is_schema_error() => {
                // Schema mismatch detected, invalidate inference cache
                if let Some(table) = e.affected_table() {
                    self.graph_cache.inference_cache
                        .invalidate_on_error(&compute_model_hash(&self.model), table);
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }
}
```

### Configuration

```rust
// In main application setup
let cache_config = GraphCacheConfig {
    inference_ttl: Duration::from_secs(3600),  // 1 hour default
    max_cache_size: Some(100 * 1024 * 1024),   // 100MB limit
    enable_compression: false,                  // Start simple
};

let metadata_cache = MetadataCache::new("cache.db")?;
let graph_cache = GraphCache::new(metadata_cache, cache_config);

let query_planner = QueryPlanner::new(model, graph_cache, inference_engine, db_connection);
```

### Old Code Removal (Task 8)

After integration complete:
- Remove `src/model_graph.rs` (old entity-level graph)
- Remove `src/column_lineage.rs` (old column-level graph)
- Remove any temporary compatibility shims
- Update all imports to use UnifiedGraph

## Summary

This design provides:

1. **Two-tier caching**: Inference cache (TTL + failure) + Graph cache (content-hash)
2. **Per-entity granularity**: Only rebuild changed tables/dimensions
3. **Automatic invalidation**: Graph cache depends on inference version
4. **SQLite persistence**: Cache survives between sessions
5. **Simple serialization**: JSON with optional compression
6. **Clean integration**: QueryPlanner and translation layer use cached graph
7. **Error handling**: Query failures trigger inference invalidation

**Next Steps:**
1. Create implementation plan with bite-sized tasks
2. Implement in order: InferenceCache → GraphCache → Integration → Cleanup
3. Test incremental cache invalidation scenarios
4. Measure cache hit rates and performance improvements
