# Graph Cache Quick Reference

## API Cheat Sheet

### Initialization

```rust
use mantis_core::cache::{MetadataCache, GraphCache, GraphCacheConfig};
use std::time::Duration;

// Create cache with default config
let storage = MetadataCache::open()?;
let cache = GraphCache::new(storage, GraphCacheConfig::default());

// Create cache with custom config
let config = GraphCacheConfig {
    inference_ttl: Duration::from_secs(1800),  // 30 minutes
    max_cache_size: Some(200 * 1024 * 1024),   // 200MB
    enable_compression: false,
};
let cache = GraphCache::new(storage, config);
```

### Core Operations

```rust
// Get or build graph (main entry point)
let graph = cache.get_or_build(model, inference_engine, db_connection)?;

// Access inference cache
let inference = cache.inference();
let version = inference.set(model_hash, results)?;
let cached = inference.get(model_hash, Duration::from_secs(3600))?;
inference.invalidate(model_hash)?;

// Cache management
cache.clear_graph_cache()?;  // Clear only graph cache
cache.clear_all()?;           // Clear everything

// Statistics
let stats = cache.stats()?;
println!("Graph entries: {}", stats.graph_entries);
println!("Inference entries: {}", stats.inference_entries);
println!("Total size: {} bytes", stats.total_size_bytes);
```

### Hash Computation

```rust
use mantis_core::cache::GraphCache;
use serde_json::json;

// Model-level hash (defaults + calendars)
let model_hash = GraphCache::compute_model_hash(
    &json!({"timezone": "UTC"}),
    &json!({"fiscal_year_start": "07-01"})
);

// Table hash (table + measures)
let table_hash = GraphCache::compute_table_hash(
    &json!({"name": "orders", "source": "db.orders"}),
    &json!({"total_amount": {"expr": "SUM(amount)"}})
);

// Dimension hash
let dim_hash = GraphCache::compute_dimension_hash(
    &json!({"name": "customer", "key": "customer_id"})
);
```

### Cache Key Generation

```rust
use mantis_core::cache::{GraphCacheKey, InferenceVersion};

let model_hash = "abc123...";
let inference_version = InferenceVersion::new();
let table_hash = "def456...";
let dim_hash = "ghi789...";

// Complete graph key
let key = GraphCacheKey::complete(model_hash, &inference_version);
// => "graph:abc123...:v1_1738016571:complete"

// Table nodes key
let key = GraphCacheKey::table_nodes(model_hash, &inference_version, table_hash);
// => "graph:abc123...:v1_1738016571:table:def456...:nodes"

// Dimension nodes key
let key = GraphCacheKey::dimension_nodes(model_hash, &inference_version, dim_hash);
// => "graph:abc123...:v1_1738016571:dimension:ghi789...:nodes"

// Edges key
let key = GraphCacheKey::edges(model_hash, &inference_version);
// => "graph:abc123...:v1_1738016571:edges"
```

## Decision Trees

### When Does Cache Invalidate?

```
┌─────────────────────────────────────────────────┐
│ Will the cache invalidate?                      │
├─────────────────────────────────────────────────┤
│                                                 │
│ ✓ Inference TTL expires (default: 1 hour)      │
│ ✓ Query fails with schema error                │
│ ✓ Manual invalidation: cache.clear_all()       │
│ ✓ CACHE_VERSION bumped (code upgrade)          │
│ ✓ Inference version changes (after any above)  │
│                                                 │
│ ✗ Model defaults change (only model_hash)      │
│ ✗ Calendar configuration changes               │
│ ✗ Table definition changes (only table_hash)   │
│ ✗ Dimension definition changes (only dim_hash) │
│ ✗ Application restart (cache persists)         │
└─────────────────────────────────────────────────┘
```

### When Do I Get a Cache Hit?

```
Complete Graph Hit (fastest: ~5ms)
├─ Model hash matches
├─ Inference version matches
└─ Complete graph key exists
   → graph:{model}:{inf_ver}:complete

Partial Graph Hit (medium: ~40ms)
├─ Model hash matches
├─ Inference version matches
├─ Some entity hashes match (unchanged entities)
└─ Some entity hashes differ (changed entities)
   → Rebuild only changed entities + edges

Complete Miss (slowest: ~400ms)
├─ Inference version changed (TTL/failure)
└─ OR all entity hashes changed
   → Full rebuild required
```

### Should I Adjust TTL?

```
Environment         | Recommended TTL | Reasoning
--------------------|-----------------|---------------------------
Development         | 5-10 minutes    | Frequent schema changes
Staging             | 30 minutes      | Moderate changes, testing
Production (stable) | 1-2 hours       | Rare schema changes
Production (active) | 30-60 minutes   | Ongoing migrations
CI/CD               | 0 (disabled)    | Always fresh builds
```

## Common Patterns

### Pattern 1: Error-Triggered Invalidation

```rust
impl QueryExecutor {
    pub fn execute(&self, query: &Query) -> Result<QueryResult> {
        match self.execute_internal(query) {
            Ok(result) => Ok(result),
            Err(e) if e.is_schema_error() => {
                // Schema mismatch detected - invalidate inference
                if let Some(table) = e.affected_table() {
                    log::warn!(
                        "Schema error on table '{}', invalidating inference cache",
                        table
                    );
                    
                    let model_hash = compute_model_hash(...);
                    self.graph_cache
                        .inference()
                        .invalidate(&model_hash)?;
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }
}
```

### Pattern 2: Conditional TTL Based on Environment

```rust
use std::env;

fn cache_config() -> GraphCacheConfig {
    let ttl = match env::var("ENVIRONMENT").as_deref() {
        Ok("development") => Duration::from_secs(300),   // 5 min
        Ok("staging") => Duration::from_secs(1800),      // 30 min
        Ok("production") => Duration::from_secs(7200),   // 2 hours
        _ => Duration::from_secs(3600),                  // 1 hour default
    };
    
    GraphCacheConfig {
        inference_ttl: ttl,
        max_cache_size: Some(100 * 1024 * 1024),
        enable_compression: false,
    }
}
```

### Pattern 3: Cache Warming on Startup

```rust
async fn warmup_cache(
    cache: &GraphCache,
    models: Vec<Model>,
    inference: &InferenceEngine,
    db: &DatabaseConnection,
) -> Result<()> {
    log::info!("Warming cache for {} models", models.len());
    
    for model in models {
        log::debug!("Building graph for model: {}", model.name);
        
        let start = Instant::now();
        cache.get_or_build(&model, inference, db)?;
        
        log::debug!(
            "Model '{}' cached in {:?}",
            model.name,
            start.elapsed()
        );
    }
    
    let stats = cache.stats()?;
    log::info!(
        "Cache warm-up complete: {} entries, {} MB",
        stats.graph_entries + stats.inference_entries,
        stats.total_size_bytes / (1024 * 1024)
    );
    
    Ok(())
}
```

### Pattern 4: Metrics Collection

```rust
struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl CacheMetrics {
    fn record_access(&self, was_hit: bool, latency: Duration) {
        if was_hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        
        self.total_latency_ms.fetch_add(
            latency.as_millis() as u64,
            Ordering::Relaxed
        );
    }
    
    fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.misses.load(Ordering::Relaxed) as f64;
        if total == 0.0 { 0.0 } else { hits / total }
    }
    
    fn avg_latency_ms(&self) -> f64 {
        let total_ms = self.total_latency_ms.load(Ordering::Relaxed) as f64;
        let count = (self.hits.load(Ordering::Relaxed) + 
                     self.misses.load(Ordering::Relaxed)) as f64;
        if count == 0.0 { 0.0 } else { total_ms / count }
    }
}

// Usage
let start = Instant::now();
let graph = cache.get_or_build(model, inference, db)?;
let was_hit = start.elapsed().as_millis() < 10; // Heuristic
metrics.record_access(was_hit, start.elapsed());
```

## Troubleshooting Guide

### Problem: Cache Not Hitting

```
Symptoms:
  - Every request takes ~400ms
  - Cache stats show 0 entries
  - Logs show repeated graph builds

Diagnosis:
  1. Check cache location:
     ls -lh ~/.mantis/cache.db
  
  2. Check cache permissions:
     ls -l ~/.mantis/
     (should be user-writable)
  
  3. Enable debug logging:
     RUST_LOG=mantis_core::cache=debug
  
  4. Check for version mismatches:
     sqlite3 ~/.mantis/cache.db "SELECT * FROM meta"

Solutions:
  - Ensure cache directory exists and is writable
  - Check that inference_ttl is not 0
  - Verify CACHE_VERSION hasn't changed mid-session
  - Check SQLite database isn't corrupted:
    sqlite3 ~/.mantis/cache.db "PRAGMA integrity_check"
```

### Problem: High Memory Usage

```
Symptoms:
  - Application memory grows over time
  - OOM errors during graph building
  - System becomes unresponsive

Diagnosis:
  1. Check cache size:
     let stats = cache.stats()?;
     println!("Cache size: {} MB", stats.total_size_bytes / 1024 / 1024);
  
  2. Check model complexity:
     println!("Tables: {}", model.tables.len());
     println!("Dimensions: {}", model.dimensions.len());
  
  3. Profile memory with:
     heaptrack ./target/release/mantis

Solutions:
  - Reduce max_cache_size in config
  - Enable compression (future feature)
  - Clear cache periodically: cache.clear_all()?
  - Implement LRU eviction (future enhancement)
  - Reduce inference_ttl to avoid accumulation
```

### Problem: Stale Data After Schema Change

```
Symptoms:
  - Queries return wrong results
  - Missing columns not detected
  - Old relationships still present

Diagnosis:
  1. Check inference cache age:
     Get last inference timestamp from cache
  
  2. Verify error handling:
     Errors should trigger invalidation
  
  3. Check TTL configuration:
     May be too long for rapid changes

Solutions:
  - Manually invalidate: cache.inference().invalidate(model_hash)?
  - Reduce inference_ttl for development
  - Ensure error handlers call invalidate()
  - Use cache.clear_all()? after schema migrations
```

### Problem: Cache Corruption

```
Symptoms:
  - Deserialization errors
  - SQLite errors
  - Unexpected cache misses

Diagnosis:
  1. Check SQLite integrity:
     sqlite3 ~/.mantis/cache.db "PRAGMA integrity_check"
  
  2. Check for version mismatches:
     grep "version" ~/.mantis/cache.db

Solutions:
  - Clear cache: cache.clear_all()?
  - Delete cache file: rm ~/.mantis/cache.db
  - Bump CACHE_VERSION to force clear
  - Check disk space: df -h ~/.mantis
```

## Performance Benchmarks

### Typical Latencies (M1 MacBook Pro, SQLite on SSD)

```
Operation                           | Latency (ms) | Notes
------------------------------------|--------------|------------------
Hash computation (small table)      | 0.05 - 0.1   | Pure CPU
Hash computation (large table)      | 0.5 - 2      | JSON serialization
SQLite lookup (cache hit)           | 0.5 - 2      | Disk I/O
JSON deserialization (small graph)  | 1 - 3        | ~50 nodes
JSON deserialization (large graph)  | 10 - 30      | ~500 nodes
Complete graph cache hit            | 2 - 10       | End-to-end
Partial cache hit (1/100 rebuild)   | 30 - 60      | Incremental build
Complete cache miss (inference)     | 100 - 500    | Database queries
Complete cache miss (build)         | 200 - 800    | Full graph build
```

### Scaling Characteristics

```
Model Size          | Cache Hit | Partial Hit | Full Miss
--------------------|-----------|-------------|----------
Small (10 tables)   | 2ms       | 15ms        | 100ms
Medium (50 tables)  | 5ms       | 40ms        | 300ms
Large (200 tables)  | 15ms      | 100ms       | 1000ms
XL (500 tables)     | 30ms      | 200ms       | 2500ms
```

## Cache Size Estimation

```
Entity Type          | Size per Entity | Example
---------------------|-----------------|------------------------
Table (simple)       | ~1 KB           | id, name, created_at
Table (complex)      | ~5 KB           | 20 columns, 10 measures
Dimension (simple)   | ~500 bytes      | id, name, 2 attributes
Dimension (complex)  | ~3 KB           | 10 attributes, drill paths
Edge                 | ~100 bytes      | from, to, relationship type
Complete graph       | ~Sum of all + 20% overhead

Example Model (e-commerce):
  - 50 tables × 3 KB =         150 KB
  - 20 dimensions × 2 KB =      40 KB
  - 200 edges × 100 bytes =     20 KB
  - Overhead (20%) =            42 KB
  ----------------------------------------
  Total per version:           ~252 KB

With 3 cached versions:        ~750 KB
(old inference versions not cleaned up yet)
```

## File Locations

```
Cache Database:
  ~/.mantis/cache.db

Log Output (if RUST_LOG enabled):
  stderr or configured log destination

Configuration:
  Programmatic (no config file currently)

Cleanup:
  # Clear all cache
  rm ~/.mantis/cache.db
  
  # Or programmatically
  cache.clear_all()?
  
  # Check cache size
  du -h ~/.mantis/cache.db
```
