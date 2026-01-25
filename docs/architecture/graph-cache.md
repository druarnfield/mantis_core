# Graph Cache Architecture

## Overview

The graph cache system implements a two-tier caching strategy for the UnifiedGraph, providing persistent, incremental caching with automatic invalidation when the underlying data changes.

## Design Goals

1. **Minimize Graph Rebuilding**: Build the UnifiedGraph once per model version, cache it persistently
2. **Granular Invalidation**: Only rebuild changed entities (tables, dimensions) when model changes
3. **Schema Synchronization**: Automatically invalidate when database schema changes via inference
4. **Session Persistence**: Maintain cache across application restarts using SQLite

## Architecture Layers

### Layer 1: Inference Cache (Schema Tracking)

The inference cache stores database schema inference results with TTL-based and failure-triggered invalidation.

**Key Responsibilities:**
- Cache database schema inference results (foreign keys, relationships, data types)
- Track inference version (timestamp-based) for cache dependency
- Invalidate on TTL expiration (default: 1 hour, configurable)
- Invalidate on query failure (schema mismatch detection)

**Cache Keys:**
```
inference:{model_hash} → CachedInference {
    results: InferenceResults,
    version: InferenceVersion (v1_{unix_timestamp}),
    timestamp_secs: u64
}
```

**Invalidation Strategy:**
- **Time-based**: Configurable TTL (default 1 hour)
- **Failure-based**: Query failures trigger invalidation for affected tables
- **Manual**: `clear_all()` for forced refresh

### Layer 2: Graph Cache (Content-Hash Based)

The graph cache stores UnifiedGraph components with per-entity granularity, depending on the inference version for automatic invalidation.

**Key Responsibilities:**
- Cache complete graphs and individual entity components
- Use content-hash based keys for each table and dimension
- Include inference version in all keys for automatic invalidation
- Support incremental builds (reuse unchanged entities)

**Cache Keys:**
```
graph:{model_hash}:{inference_version}:complete → CachedGraph
graph:{model_hash}:{inference_version}:table:{table_hash}:nodes → CachedNodes
graph:{model_hash}:{inference_version}:dimension:{dim_hash}:nodes → CachedNodes
graph:{model_hash}:{inference_version}:edges → CachedEdges
```

**Hash Strategy:**
- **model_hash**: SHA256 of model defaults + calendars (global config)
- **table_hash**: SHA256 of table definition + associated measures
- **dimension_hash**: SHA256 of dimension definition
- **inference_version**: Embedded in all graph cache keys

## Cache Invalidation Flow

### Scenario 1: Model Change (e.g., Add/Modify Table)

```
1. User modifies table definition in model
   ↓
2. model_hash remains same (only tables changed, not defaults/calendars)
   ↓
3. Changed table's table_hash changes
   ↓
4. Cache lookup for that table misses (new hash)
   ↓
5. Other tables hit cache (unchanged hashes)
   ↓
6. Build only the changed table nodes
   ↓
7. Rebuild edges (depend on all nodes)
   ↓
8. Cache new table nodes + complete graph
```

**Result**: Only changed table rebuilt, others reused from cache.

### Scenario 2: Database Schema Change (Detected on Query Failure)

```
1. Query fails with schema error (column not found, etc.)
   ↓
2. Error handler calls inference_cache.invalidate(model_hash)
   ↓
3. Inference cache entry deleted
   ↓
4. Next cache access:
   - inference_cache.get() returns None
   - New inference runs → new InferenceVersion created
   ↓
5. All graph cache keys now have old inference_version
   ↓
6. Graph cache misses (version mismatch)
   ↓
7. Full graph rebuild with fresh inference
   ↓
8. Cache with new inference_version
```

**Result**: All graph cache automatically invalidated when inference refreshes.

### Scenario 3: TTL Expiration

```
1. Time passes beyond inference_ttl (default 1 hour)
   ↓
2. inference_cache.get() checks timestamp
   ↓
3. Cached entry is_expired() returns true
   ↓
4. Returns None (same as cache miss)
   ↓
5. New inference runs → new InferenceVersion
   ↓
6. Graph cache invalidated via version change (same as Scenario 2)
```

**Result**: Periodic refresh ensures schema changes are detected even without failures.

## Component Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                      Application Layer                       │
│  (QueryPlanner, Translation Layer)                          │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼ get_or_build(model, inference, db)
┌─────────────────────────────────────────────────────────────┐
│                       GraphCache                             │
│  - Coordinates two-tier caching                             │
│  - Manages incremental builds                               │
│  - Returns UnifiedGraph                                     │
└─────────────────────────────────────────────────────────────┘
           │                              │
           │                              │
           ▼                              ▼
┌──────────────────────┐      ┌──────────────────────┐
│  InferenceCache      │      │  Graph Storage       │
│  - TTL checking      │      │  - Complete graphs   │
│  - Version tracking  │      │  - Per-entity nodes  │
│  - Invalidation      │      │  - Edges             │
└──────────────────────┘      └──────────────────────┘
           │                              │
           └──────────────┬───────────────┘
                          ▼
           ┌──────────────────────────────┐
           │      MetadataCache           │
           │  (SQLite Key-Value Store)    │
           │  ~/.mantis/cache.db          │
           └──────────────────────────────┘
```

## Storage Format

### Versioned Storage Types

All cached data includes schema version for forward compatibility:

**CachedNodes** (for tables and dimensions):
```json
{
  "version": 1,
  "nodes": [
    {
      "id": "orders",
      "node_type": "Entity",
      "data": { ... }
    },
    {
      "id": "orders.customer_id",
      "node_type": "Column",
      "data": { ... }
    }
  ]
}
```

**CachedEdges**:
```json
{
  "version": 1,
  "edges": [
    ["orders.customer_id", "customers", "REFERENCES"],
    ["orders.amount", "orders", "BELONGS_TO"]
  ]
}
```

**CachedGraph** (complete serialized UnifiedGraph):
```json
{
  "version": 1,
  "graph": {
    "nodes": [...],
    "edges": [...],
    "indices": { ... }
  }
}
```

**CachedInference**:
```json
{
  "results": {
    "relationships": [...],
    "data_types": {...}
  },
  "version": "v1_1738016571",
  "timestamp_secs": 1738016571
}
```

### Compression Strategy

**Current**: JSON without compression (simple, debuggable)

**Future**: Optional zstd compression when `enable_compression = true`
- Tradeoff: CPU time vs storage space
- Beneficial for large models (100+ tables)
- Config flag allows per-deployment tuning

## Configuration

```rust
GraphCacheConfig {
    inference_ttl: Duration::from_secs(3600),    // 1 hour
    max_cache_size: Some(100 * 1024 * 1024),     // 100MB
    enable_compression: false,                    // Future feature
}
```

**Tuning Guidelines:**
- **inference_ttl**: 
  - Development: 300s (5 min) for quick schema change detection
  - Production: 3600s (1 hour) for stability
  - Critical systems: 7200s (2 hours) + manual invalidation
- **max_cache_size**:
  - Small models (<50 tables): 50MB
  - Medium models (50-200 tables): 100MB
  - Large models (200+ tables): 250MB+
- **enable_compression**:
  - Enable if cache size > 75% of max_cache_size
  - Adds ~5-10ms per cache operation

## Performance Characteristics

### Cache Hit (Complete Graph)

```
Time Complexity: O(1) - single SQLite lookup
Latency: ~1-5ms
- Key generation: <0.1ms
- SQLite query: 0.5-2ms
- Deserialization: 0.5-3ms (depends on graph size)
```

### Cache Miss (Full Rebuild)

```
Time Complexity: O(T + D + E) where:
  T = number of tables
  D = number of dimensions
  E = number of edges

Latency: 50-500ms+ (depends on model complexity)
- Inference: 20-200ms (database queries)
- Graph construction: 30-300ms (node/edge creation)
- Serialization: 5-50ms
- Cache write: 5-50ms
```

### Partial Cache Hit (Entity Change)

```
Time Complexity: O(T_changed + E)
- Unchanged entities: cache hit (fast)
- Changed entities: rebuild (slow)
- Edges: always rebuilt (depend on all nodes)

Latency: 10-100ms (much faster than full rebuild)
Example: Changing 1 table in a 100-table model
  - 99 tables: cached (~5ms total)
  - 1 table: rebuilt (~15ms)
  - Edges: rebuilt (~20ms)
  - Total: ~40ms vs ~200ms for full rebuild
```

## Memory Footprint

### In-Memory Structures

```
GraphCache: ~200 bytes
  storage: Arc<MetadataCache> (shared)
  inference_cache: Arc<InferenceCache> (shared)
  config: GraphCacheConfig (~48 bytes)

Per-Model Cached Data (estimated):
  Small model (10 tables): ~50KB per graph
  Medium model (50 tables): ~500KB per graph
  Large model (200 tables): ~2-5MB per graph
```

### SQLite Database Size

```
~/.mantis/cache.db typical sizes:
  Empty: 8KB (SQLite overhead)
  After 1 model: 50KB-5MB (depends on model size)
  After 10 models: 500KB-50MB
  With history: Grows indefinitely without cleanup

Maintenance:
  - Auto-cleanup on version mismatch (CACHE_VERSION bump)
  - Manual: cache.clear_all()
  - Periodic: vacuum during idle periods
```

## Error Handling

### Cache Errors

```rust
pub enum CacheError {
    Sqlite(rusqlite::Error),      // Database errors
    Json(serde_json::Error),      // Serialization errors
    NoCacheDir,                   // Cannot find cache directory
    Io(std::io::Error),          // File system errors
    Crypto(String),               // Encryption errors (credentials)
}
```

### Error Recovery Strategy

1. **Transient Errors** (SQLite busy, disk full):
   - Log warning
   - Return cache miss (rebuild graph)
   - Continue operation (degraded performance)

2. **Serialization Errors**:
   - Log error with context
   - Invalidate corrupted entry
   - Rebuild from source

3. **Version Mismatch**:
   - Automatic: clear entire cache
   - Rebuild with current version
   - Prevents stale data issues

4. **Query Failure Detection**:
   ```rust
   match executor.execute(query) {
       Err(e) if e.is_schema_error() => {
           // Invalidate inference for affected table
           cache.inference().invalidate(model_hash)?;
           Err(e)
       }
       result => result
   }
   ```

## Cache Coherence

### Single-Process Guarantee

Within a single process, cache is coherent:
- All components share same `MetadataCache` instance via `Arc`
- SQLite connection is serialized (thread-safe)
- Writes are immediately visible to subsequent reads

### Multi-Process Scenario

**Warning**: Multiple processes sharing `~/.mantis/cache.db` can see stale data.

**Mitigation**:
- SQLite locking prevents corruption
- Each process may have different in-memory state
- Solution: Use process-local TTL to force periodic refresh

**Future Enhancement**: Add cache invalidation signals (e.g., file watcher, IPC)

## Security Considerations

### Credential Storage

Credentials in cache are encrypted:
```rust
connection_string_encrypted: String  // AES-256-GCM encrypted
```

Master key derived from user password/keychain (handled by `crypto` module).

### Cache Poisoning

Risk: Malicious modification of `~/.mantis/cache.db`

Mitigations:
1. File permissions: User-only read/write
2. Version checking: Rejects unknown schema versions
3. No code execution: JSON data only
4. Validation: Deserialized data validated before use

**Not Protected Against**: Local attacker with user privileges (by design - same threat model as application data)

## Observability

### Statistics API

```rust
pub struct GraphCacheStats {
    graph_entries: usize,        // Number of cached graphs/components
    inference_entries: usize,    // Number of cached inferences
    total_size_bytes: usize,    // Total cache size
}

cache.stats()?  // Get current statistics
```

### Recommended Metrics

For production deployments:
```rust
// Hit rate tracking
metrics.increment("cache.graph.hit");
metrics.increment("cache.graph.miss");

// Latency tracking
let start = Instant::now();
let graph = cache.get_or_build(...)?;
metrics.histogram("cache.get_or_build.duration_ms", start.elapsed().as_millis());

// Size tracking
let stats = cache.stats()?;
metrics.gauge("cache.size_bytes", stats.total_size_bytes);
metrics.gauge("cache.entry_count", stats.graph_entries + stats.inference_entries);

// Invalidation tracking
metrics.increment("cache.inference.invalidated");
metrics.increment("cache.graph.cleared");
```

## Future Enhancements

### Short-Term (Next Implementation Phase)

1. **`get_or_build()` Implementation**
   - Core cache retrieval with incremental building
   - Proper handling of partial cache hits

2. **UnifiedGraph Serialization**
   - Add serde derives to all graph types
   - Handle petgraph serialization (serde-1 feature)

3. **Integration Points**
   - QueryPlanner integration
   - Translation layer updates
   - Error handling hooks

### Medium-Term

1. **Compression Support**
   - Implement zstd compression for large graphs
   - Benchmark tradeoffs (CPU vs storage)

2. **Cache Warming**
   - Pre-build graphs on startup
   - Background refresh before TTL expiration

3. **Advanced Invalidation**
   - Dependency tracking for measures/reports
   - Selective edge rebuilding

### Long-Term

1. **Distributed Caching**
   - Redis/Memcached backend option
   - Multi-process coherence
   - Horizontal scaling support

2. **Query-Aware Caching**
   - Cache frequently accessed subgraphs
   - Query pattern analysis
   - Adaptive TTL based on query patterns

3. **Incremental Inference**
   - Cache individual table inferences
   - Parallel inference execution
   - Diff-based inference updates

## References

- Implementation Plan: `/docs/plans/2025-01-25-graph-cache-implementation.md`
- Design Document: `/docs/plans/2025-01-25-graph-cache-integration-design.md`
- UnifiedGraph Architecture: `/docs/architecture/unified-graph.md`
- Code Location: `/src/cache/`
