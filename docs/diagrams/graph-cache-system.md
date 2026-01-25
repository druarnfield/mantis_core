# Graph Cache System Diagrams

## System Overview

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      Application Layer                           │
│                                                                  │
│  ┌────────────────┐              ┌──────────────────┐          │
│  │ QueryPlanner   │              │ Translation Layer│          │
│  └────────┬───────┘              └────────┬─────────┘          │
│           │                               │                     │
│           └───────────┬───────────────────┘                     │
└───────────────────────┼─────────────────────────────────────────┘
                        │
                        │ get_or_build(model, inference, db)
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                        GraphCache                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Core Logic:                                              │   │
│  │  1. Check inference cache for version                    │   │
│  │  2. Check complete graph cache                           │   │
│  │  3. If miss, check per-entity caches                     │   │
│  │  4. Build missing pieces incrementally                   │   │
│  │  5. Cache results with current inference version         │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────┐       ┌──────────────────────────┐    │
│  │  InferenceCache     │       │  Graph Storage           │    │
│  │  (Layer 1)          │       │  (Layer 2)               │    │
│  │                     │       │                          │    │
│  │  • TTL checking     │       │  • Complete graphs       │    │
│  │  • Version tracking │       │  • Per-entity nodes      │    │
│  │  • Invalidation     │       │  • Edges                 │    │
│  └──────────┬──────────┘       └───────────┬──────────────┘    │
└─────────────┼──────────────────────────────┼───────────────────┘
              │                              │
              └──────────┬───────────────────┘
                         ▼
              ┌─────────────────────────┐
              │   MetadataCache         │
              │   (SQLite Storage)      │
              │                         │
              │   ~/.mantis/cache.db    │
              └─────────────────────────┘
```

## Cache Key Hierarchy

```
SQLite Database: ~/.mantis/cache.db
│
├─ inference:{model_hash}
│  └─ CachedInference {
│       results: JSON,
│       version: "v1_1738016571",
│       timestamp_secs: 1738016571
│     }
│
└─ graph:{model_hash}:{inference_version}:*
   │
   ├─ :complete
   │  └─ CachedGraph (entire UnifiedGraph serialized)
   │
   ├─ :table:{table_hash}:nodes
   │  └─ CachedNodes (table + columns + measures)
   │
   ├─ :dimension:{dimension_hash}:nodes
   │  └─ CachedNodes (dimension + attributes)
   │
   └─ :edges
      └─ CachedEdges (all relationships)
```

### Hash Computation Tree

```
Model
│
├─ model_hash = SHA256(defaults + calendars)
│  │
│  └─ Used in: inference:{model_hash}
│             graph:{model_hash}:*
│
├─ For each Table:
│  └─ table_hash = SHA256(table_def + measures)
│     │
│     └─ Used in: graph:{model_hash}:{inf_ver}:table:{table_hash}:nodes
│
└─ For each Dimension:
   └─ dimension_hash = SHA256(dimension_def)
      │
      └─ Used in: graph:{model_hash}:{inf_ver}:dimension:{dim_hash}:nodes
```

## Cache Invalidation Flow

### Scenario 1: Table Modification

```
┌─────────────────┐
│ User modifies   │
│ table "orders"  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Recompute hashes                    │
│  • model_hash: UNCHANGED            │
│  • orders_table_hash: CHANGED       │
│  • other_table_hash: UNCHANGED      │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ get_or_build() executes:            │
│                                     │
│ 1. Get inference (cached, valid)    │
│    inference_version: v1_1738016571 │
│                                     │
│ 2. Check complete graph             │
│    Key: graph:{model}:{v1_...}:complete │
│    Result: MISS (new orders hash)   │
│                                     │
│ 3. Check per-entity caches:         │
│    ┌─────────────────────────────┐ │
│    │ orders: MISS (hash changed) │ │
│    │ customers: HIT ✓            │ │
│    │ products: HIT ✓             │ │
│    │ line_items: HIT ✓           │ │
│    └─────────────────────────────┘ │
│                                     │
│ 4. Build orders nodes (rebuild)     │
│    Load others from cache (fast)    │
│                                     │
│ 5. Rebuild edges (depend on all)    │
│                                     │
│ 6. Cache new state:                 │
│    • orders nodes (new hash)        │
│    • complete graph                 │
│    • edges                          │
└─────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│ Return graph    │
│ (partial cache  │
│  hit ~40ms vs   │
│  full rebuild   │
│  ~200ms)        │
└─────────────────┘
```

### Scenario 2: Schema Change Detection

```
┌──────────────────┐
│ Query execution  │
│ fails with:      │
│ "Column 'email'  │
│  not found"      │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Error Handler                       │
│                                     │
│ if error.is_schema_error() {        │
│   let table = error.affected_table();│
│   cache.inference()                 │
│     .invalidate(model_hash)?;       │
│ }                                   │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Inference Cache Cleared             │
│                                     │
│ DELETE FROM cache                   │
│ WHERE key = 'inference:{model_hash}'│
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Next get_or_build() call:           │
│                                     │
│ 1. Get inference                    │
│    Result: MISS (deleted)           │
│                                     │
│ 2. Run fresh inference              │
│    • Query database metadata        │
│    • Discover new 'email' column    │
│    • Generate new relationships     │
│    version: v1_1738020171 (NEW!)    │
│                                     │
│ 3. Check complete graph             │
│    Key: graph:{model}:{v1_1738020171}:complete │
│    Result: MISS (new version!)      │
│                                     │
│ 4. Check per-entity caches:         │
│    ┌─────────────────────────────┐ │
│    │ All have old version        │ │
│    │ graph:{model}:{v1_1738016571}:...│
│    │                             │ │
│    │ ALL MISS ❌                 │ │
│    └─────────────────────────────┘ │
│                                     │
│ 5. Full graph rebuild               │
│    • All tables                     │
│    • All dimensions                 │
│    • All edges                      │
│                                     │
│ 6. Cache with NEW version:          │
│    graph:{model}:{v1_1738020171}:*  │
└─────────────────────────────────────┘
         │
         ▼
┌──────────────────┐
│ Old cache entries│
│ remain but unused│
│ (different inf_  │
│  version in key) │
│                  │
│ Cleaned up on:   │
│ • CACHE_VERSION  │
│   bump           │
│ • Manual clear   │
│ • Size limit     │
└──────────────────┘
```

### Scenario 3: TTL Expiration

```
Time: T0
┌─────────────────────────────────────┐
│ Inference cached                    │
│                                     │
│ inference:{model_hash} = {          │
│   results: {...},                   │
│   version: "v1_1738016571",         │
│   timestamp_secs: 1738016571        │
│ }                                   │
└─────────────────────────────────────┘

Time: T0 + 3600s (1 hour later)
┌─────────────────────────────────────┐
│ inference_cache.get(model_hash,     │
│                    ttl=3600s)       │
│                                     │
│ 1. Retrieve from storage            │
│    cached.timestamp_secs = 1738016571│
│                                     │
│ 2. Check expiration                 │
│    now = 1738020171                 │
│    age = 1738020171 - 1738016571    │
│        = 3600s                      │
│                                     │
│ 3. is_expired()?                    │
│    age (3600s) > ttl (3600s)?       │
│    = false (equal, NOT expired)     │
│                                     │
│ Result: CACHE HIT (still valid)     │
└─────────────────────────────────────┘

Time: T0 + 3601s (1 second past TTL)
┌─────────────────────────────────────┐
│ inference_cache.get(model_hash,     │
│                    ttl=3600s)       │
│                                     │
│ 1. Retrieve from storage            │
│    cached.timestamp_secs = 1738016571│
│                                     │
│ 2. Check expiration                 │
│    now = 1738020172                 │
│    age = 1738020172 - 1738016571    │
│        = 3601s                      │
│                                     │
│ 3. is_expired()?                    │
│    age (3601s) > ttl (3600s)?       │
│    = true (EXPIRED)                 │
│                                     │
│ 4. Return None (cache miss)         │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Trigger fresh inference             │
│ (Same flow as Schema Change)        │
│                                     │
│ New version: v1_1738020172          │
│ → All graph cache invalidated       │
└─────────────────────────────────────┘
```

## Cache Hit/Miss Decision Tree

```
get_or_build(model, inference_engine, db)
│
├─ Step 1: Get Inference
│  │
│  ├─ inference_cache.get(model_hash, ttl)
│  │  │
│  │  ├─ Cache Entry Exists?
│  │  │  ├─ YES → Check TTL
│  │  │  │  ├─ Expired? → [MISS] Run Inference
│  │  │  │  └─ Valid → [HIT] Use cached version
│  │  │  │
│  │  │  └─ NO → [MISS] Run Inference
│  │
│  └─ Output: (InferenceResults, InferenceVersion)
│
├─ Step 2: Check Complete Graph
│  │
│  ├─ Key: graph:{model_hash}:{inf_version}:complete
│  │
│  ├─ Cache Entry Exists?
│  │  ├─ YES → [FULL HIT] Deserialize and return (1-5ms) ✓
│  │  └─ NO → Continue to Step 3
│
├─ Step 3: Check Per-Entity Caches
│  │
│  ├─ For each table:
│  │  ├─ table_hash = compute_table_hash(table, measures)
│  │  ├─ Key: graph:{model_hash}:{inf_ver}:table:{table_hash}:nodes
│  │  ├─ Exists? → [HIT] Load from cache
│  │  └─ Missing? → [MISS] Build nodes
│  │
│  └─ For each dimension:
│     ├─ dim_hash = compute_dimension_hash(dimension)
│     ├─ Key: graph:{model_hash}:{inf_ver}:dim:{dim_hash}:nodes
│     ├─ Exists? → [HIT] Load from cache
│     └─ Missing? → [MISS] Build nodes
│
├─ Step 4: Check Edges Cache
│  │
│  ├─ Key: graph:{model_hash}:{inf_version}:edges
│  │
│  ├─ All nodes cached?
│  │  ├─ YES → Check edge cache
│  │  │  ├─ Exists? → [HIT] Load edges
│  │  │  └─ Missing? → [MISS] Build edges
│  │  │
│  │  └─ NO → [MISS] Rebuild edges (depend on all nodes)
│
└─ Step 5: Cache Results
   │
   ├─ Cache any rebuilt components:
   │  ├─ New table nodes → cache with table_hash
   │  ├─ New dimension nodes → cache with dim_hash
   │  └─ New edges → cache
   │
   └─ Cache complete graph:
      └─ graph:{model_hash}:{inf_version}:complete
```

## Performance Comparison

### Latency by Cache Hit Type

```
Complete Cache Hit (Step 2)
┌──────────────────────────────────┐
│ Key lookup                0.5ms  │
│ SQLite query              1.5ms  │
│ JSON deserialization      2.0ms  │
│ Graph reconstruction      1.0ms  │
├──────────────────────────────────┤
│ TOTAL                    ~5ms    │
└──────────────────────────────────┘

Partial Cache Hit (Step 3, 1 table changed out of 100)
┌──────────────────────────────────┐
│ Inference (cached)        2ms    │
│ 99 tables (cached)        5ms    │
│ 1 table (rebuilt)        15ms    │
│ Edges (rebuilt)          20ms    │
│ Cache writes              5ms    │
├──────────────────────────────────┤
│ TOTAL                   ~47ms    │
└──────────────────────────────────┘

Complete Cache Miss (Full rebuild)
┌──────────────────────────────────┐
│ Inference (DB queries)  120ms    │
│ 100 tables (built)      150ms    │
│ Edges (built)            80ms    │
│ Serialization            30ms    │
│ Cache writes             20ms    │
├──────────────────────────────────┤
│ TOTAL                  ~400ms    │
└──────────────────────────────────┘
```

## Data Flow: First Access vs Subsequent Access

### First Access (Cold Cache)

```
Request: get_or_build(model, inference, db)
│
├─ 1. Inference Cache Lookup
│  │
│  └─ SQLite: SELECT value FROM cache 
│     WHERE key = 'inference:abc123'
│     └─ Result: NULL (empty cache)
│
├─ 2. Run Inference
│  │
│  ├─ Query database metadata
│  │  ├─ SELECT table_name, column_name, data_type
│  │  │  FROM information_schema.columns
│  │  │
│  │  ├─ SELECT constraint_name, table_name, column_name
│  │  │  FROM information_schema.key_column_usage
│  │  │
│  │  └─ Run statistical queries (DISTINCT counts, etc.)
│  │
│  ├─ Analyze relationships
│  │  └─ Score potential foreign keys
│  │
│  └─ Generate InferenceResults + InferenceVersion
│     └─ version = "v1_1738016571"
│
├─ 3. Cache Inference
│  │
│  └─ SQLite: INSERT INTO cache
│     VALUES ('inference:abc123', '{results:..., version:v1_...}')
│
├─ 4. Build UnifiedGraph
│  │
│  ├─ Create nodes for each table
│  ├─ Create nodes for each dimension
│  ├─ Create edges based on relationships
│  └─ Build graph indices
│
└─ 5. Cache Graph Components
   │
   ├─ SQLite: INSERT INTO cache
   │  VALUES ('graph:abc123:v1_...:table:def456:nodes', '{...}')
   │  ... (for each table/dimension)
   │
   └─ SQLite: INSERT INTO cache
      VALUES ('graph:abc123:v1_...:complete', '{...}')

TOTAL TIME: ~400ms
```

### Subsequent Access (Warm Cache)

```
Request: get_or_build(model, inference, db)
│
├─ 1. Inference Cache Lookup
│  │
│  └─ SQLite: SELECT value FROM cache 
│     WHERE key = 'inference:abc123'
│     │
│     └─ Result: {
│           results: {...},
│           version: "v1_1738016571",
│           timestamp: 1738016571
│         }
│
├─ 2. Check TTL
│  │
│  ├─ now - timestamp = 600 seconds
│  └─ 600 < 3600 (ttl) → VALID ✓
│
├─ 3. Complete Graph Cache Lookup
│  │
│  └─ SQLite: SELECT value FROM cache
│     WHERE key = 'graph:abc123:v1_1738016571:complete'
│     │
│     └─ Result: {
│           version: 1,
│           graph: {nodes: [...], edges: [...]}
│         }
│
├─ 4. Deserialize Graph
│  │
│  └─ serde_json::from_value::<UnifiedGraph>(cached.graph)
│     └─ Reconstruct petgraph, indices, etc.
│
└─ 5. Return Graph

TOTAL TIME: ~5ms (80x faster!)
```

## Storage Layout Visualization

```
~/.mantis/cache.db (SQLite)

┌─────────────────────────────────────────────────────────────────┐
│ Table: cache                                                    │
├──────────────────────────────┬──────────────────────────────────┤
│ key (TEXT PRIMARY KEY)       │ value (TEXT/JSON)                │
├──────────────────────────────┼──────────────────────────────────┤
│ inference:model_abc123       │ {"results":{...},"version":...}  │
│                              │                                  │
│ graph:abc:v1_123:complete    │ {"version":1,"graph":{...}}      │
│                              │                                  │
│ graph:abc:v1_123:table:      │ {"version":1,"nodes":[...]}      │
│   def456:nodes               │                                  │
│                              │                                  │
│ graph:abc:v1_123:table:      │ {"version":1,"nodes":[...]}      │
│   ghi789:nodes               │                                  │
│                              │                                  │
│ graph:abc:v1_123:dimension:  │ {"version":1,"nodes":[...]}      │
│   jkl012:nodes               │                                  │
│                              │                                  │
│ graph:abc:v1_123:edges       │ {"version":1,"edges":[[...]]}    │
│                              │                                  │
│ [old entries with v1_100]    │ [stale but harmless]             │
└──────────────────────────────┴──────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Table: meta                                                     │
├──────────────────────────────┬──────────────────────────────────┤
│ key (TEXT PRIMARY KEY)       │ value (TEXT)                     │
├──────────────────────────────┼──────────────────────────────────┤
│ version                      │ "2"                              │
└──────────────────────────────┴──────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Table: credentials                                              │
├────────┬─────────┬─────────┬──────────────┬──────────┬──────────┤
│ id     │ driver  │ display │ encrypted_   │ created  │ last_used│
│        │         │ _name   │ conn_string  │ _at      │ _at      │
├────────┼─────────┼─────────┼──────────────┼──────────┼──────────┤
│ uuid   │ mssql   │ Prod DB │ <encrypted>  │ 17380... │ 17380... │
└────────┴─────────┴─────────┴──────────────┴──────────┴──────────┘
```

## Concurrency & Thread Safety

```
Application (Multi-threaded)
│
├─ Thread 1: QueryPlanner
│  └─ cache.get_or_build(...) ──┐
│                                │
├─ Thread 2: Translation        │
│  └─ cache.get_or_build(...) ──┤
│                                │
└─ Thread 3: API Handler        │
   └─ cache.get_or_build(...) ──┤
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │   GraphCache           │
                    │   (Thread-safe)        │
                    │                        │
                    │  Arc<InferenceCache>   │
                    │  Arc<MetadataCache>    │
                    └────────┬───────────────┘
                             │
                             ▼
                    ┌────────────────────────┐
                    │  MetadataCache         │
                    │  (Serialized access)   │
                    │                        │
                    │  Mutex<SQLite>         │
                    └────────┬───────────────┘
                             │
                             ▼
                    ┌────────────────────────┐
                    │  SQLite Database       │
                    │  (Single-threaded)     │
                    │                        │
                    │  Operations serialized │
                    │  via mutex             │
                    └────────────────────────┘
```

**Key Points:**
- `GraphCache` is cheaply cloneable (uses `Arc`)
- SQLite connection protected by Rust's type system
- Concurrent reads are safe
- Concurrent writes are serialized (one at a time)
- No deadlocks (single mutex, no nested locks)

## Version Evolution

### Cache Schema Versioning

```
Version 1 (Current)
┌─────────────────────────────────────┐
│ CachedNodes {                       │
│   version: 1,                       │
│   nodes: serde_json::Value          │
│ }                                   │
└─────────────────────────────────────┘

Version 2 (Future - with compression)
┌─────────────────────────────────────┐
│ CachedNodes {                       │
│   version: 2,                       │
│   compression: "zstd",              │
│   compressed_nodes: Vec<u8>         │
│ }                                   │
└─────────────────────────────────────┘

Backward Compatibility Strategy
┌─────────────────────────────────────┐
│ On deserialize:                     │
│   match cached.version {            │
│     1 => parse as JSON              │
│     2 => decompress then parse      │
│     _ => return error (unknown)     │
│   }                                 │
└─────────────────────────────────────┘
```

### CACHE_VERSION Bump Strategy

```
Event: Code updated with schema change
│
├─ Old Code: CACHE_VERSION = 2
└─ New Code: CACHE_VERSION = 3

Application Startup
│
├─ 1. Open SQLite database
│
├─ 2. Read meta table
│  │
│  └─ SELECT value FROM meta WHERE key = 'version'
│     └─ Returns: "2"
│
├─ 3. Compare versions
│  │
│  ├─ Stored: 2
│  └─ Expected: 3
│     └─ MISMATCH!
│
├─ 4. Clear cache
│  │
│  └─ DELETE FROM cache
│     (Keep meta and credentials tables)
│
├─ 5. Update version
│  │
│  └─ UPDATE meta SET value = '3' WHERE key = 'version'
│
└─ 6. Continue with empty cache
   └─ First access will populate
```
