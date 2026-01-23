# Unified Graph System - Complete Overview

This document provides a comprehensive system-level overview of the unified semantic graph architecture, showing data flow, capabilities, and optimization metadata.

## 1. Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         INPUT SOURCES                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │   DSL Model  │  │  Database    │  │  Column      │  │  Inferred   │ │
│  │   (AST)      │  │  Metadata    │  │  Statistics  │  │  Relations  │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬──────┘ │
│         │                 │                 │                 │         │
└─────────┼─────────────────┼─────────────────┼─────────────────┼─────────┘
          │                 │                 │                 │
          ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      GRAPH BUILDER                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  UnifiedGraph::from_model_with_inference()                              │
│                                                                           │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐ │
│  │ Create      │   │ Create      │   │ Create      │   │ Create      │ │
│  │ Entities    │──▶│ Columns     │──▶│ Measures    │──▶│ Calendars   │ │
│  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘ │
│         │                 │                 │                 │         │
│         └─────────────────┴─────────────────┴─────────────────┘         │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐ │
│  │ BELONGS_TO  │   │ REFERENCES  │   │ DERIVED_FROM│   │ DEPENDS_ON  │ │
│  │ edges       │   │ edges (FK)  │   │ edges       │   │ edges       │ │
│  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘ │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │ JOINS_TO edges (entity-level, with cardinality)         │            │
│  └─────────────────────────────────────────────────────────┘            │
│                                                                           │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       UNIFIED GRAPH                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  DiGraph<GraphNode, GraphEdge>                                          │
│                                                                           │
│  Nodes:                              Edges:                             │
│  ┌─────────────┐                     ┌─────────────┐                    │
│  │ Entity      │◀────────────────────│ BELONGS_TO  │                    │
│  │ - name      │                     │ (col → ent) │                    │
│  │ - type      │                     └─────────────┘                    │
│  │ - row_count │                     ┌─────────────┐                    │
│  │ - size_cat  │                     │ REFERENCES  │                    │
│  └─────────────┘                     │ (col → col) │                    │
│                                      └─────────────┘                    │
│  ┌─────────────┐                     ┌─────────────┐                    │
│  │ Column      │                     │ DERIVED_FROM│                    │
│  │ - entity    │                     │ (col → col) │                    │
│  │ - name      │                     └─────────────┘                    │
│  │ - data_type │                     ┌─────────────┐                    │
│  │ - unique    │                     │ DEPENDS_ON  │                    │
│  │ - nullable  │                     │ (msr → col) │                    │
│  └─────────────┘                     └─────────────┘                    │
│                                      ┌─────────────┐                    │
│  ┌─────────────┐                     │ JOINS_TO    │                    │
│  │ Measure     │                     │ (ent → ent) │                    │
│  │ - name      │                     │ + cardinality│                   │
│  │ - entity    │                     └─────────────┘                    │
│  │ - aggregation│                                                       │
│  │ - expression│                                                        │
│  └─────────────┘                                                        │
│                                                                           │
│  ┌─────────────┐                                                        │
│  │ Calendar    │                                                        │
│  │ - name      │                                                        │
│  │ - date_col  │                                                        │
│  │ - grains    │                                                        │
│  └─────────────┘                                                        │
│                                                                           │
│  Indices: entity_index, column_index, measure_index, calendar_index    │
│                                                                           │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      QUERY INTERFACE                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  Entity-Level:          Column-Level:          Hybrid:                  │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────────┐    │
│  │ find_path()    │    │ required_      │    │ find_path_with_    │    │
│  │                │    │   columns()    │    │   required_columns│    │
│  │ validate_safe_ │    │                │    │                    │    │
│  │   path()       │    │ column_        │    │ find_best_join_   │    │
│  │                │    │   lineage()    │    │   strategy()       │    │
│  │ infer_grain()  │    │                │    │                    │    │
│  │                │    │ is_column_     │    │ should_aggregate_ │    │
│  │                │    │   unique()     │    │   before_join()    │    │
│  │                │    │                │    │                    │    │
│  │                │    │ is_high_       │    │                    │    │
│  │                │    │   cardinality()│    │                    │    │
│  └────────────────┘    └────────────────┘    └────────────────────┘    │
│                                                                           │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         OUTPUT / CONSUMERS                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │  SQL Query   │  │  Query       │  │  Semantic    │  │  LSP        │ │
│  │  Planner     │  │  Optimizer   │  │  Validation  │  │  Completion │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └─────────────┘ │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

## 2. Capabilities Mind Map

```
                           ┌─────────────────────────┐
                           │   UNIFIED GRAPH         │
                           │   (Single Source of     │
                           │    Semantic Truth)      │
                           └────────────┬────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            │                           │                           │
            ▼                           ▼                           ▼
    ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
    │  ENTITY-LEVEL │          │ COLUMN-LEVEL  │          │    HYBRID     │
    │   QUERIES     │          │   QUERIES     │          │   QUERIES     │
    └───────┬───────┘          └───────┬───────┘          └───────┬───────┘
            │                          │                          │
            │                          │                          │
    ┌───────┴───────┐          ┌───────┴───────┐          ┌───────┴───────┐
    │               │          │               │          │               │
    ▼               ▼          ▼               ▼          ▼               ▼
┌────────┐    ┌────────┐  ┌────────┐    ┌────────┐  ┌────────┐    ┌────────┐
│ Path   │    │Validate│  │Required│    │Column  │  │Path +  │    │Join    │
│Finding │    │Safety  │  │Columns │    │Lineage │  │Columns │    │Strategy│
└───┬────┘    └───┬────┘  └───┬────┘    └───┬────┘  └───┬────┘    └───┬────┘
    │             │           │             │           │             │
    │             │           │             │           │             │
    │    ┌────────┴────┐      │    ┌────────┴────┐     │    ┌────────┴────┐
    ▼    ▼             ▼      ▼    ▼             ▼     ▼    ▼             ▼
┌────────────┐   ┌────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ BFS on     │   │ Check for  │   │ Traverse        │   │ Size-based      │
│ JOINS_TO   │   │ OneToMany  │   │ DEPENDS_ON      │   │ build/probe     │
│ edges      │   │ fan-out    │   │ edges from      │   │ hints           │
│            │   │            │   │ measures        │   │                 │
└────────────┘   └────────────┘   └─────────────────┘   └─────────────────┘
                                  
┌────────────┐   ┌────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Reconstruct│   │ Prevent    │   │ Traverse        │   │ Pre-aggregate   │
│ path from  │   │ row        │   │ DERIVED_FROM    │   │ decision based  │
│ parent map │   │ duplication│   │ edges for       │   │ on entity sizes │
│            │   │            │   │ lineage         │   │                 │
└────────────┘   └────────────┘   └─────────────────┘   └─────────────────┘

┌────────────┐   ┌────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Return     │   │ Error on   │   │ Check unique/   │   │ Hash join with  │
│ shortest   │   │ unsafe     │   │ primary key     │   │ small as build  │
│ join path  │   │ paths      │   │ properties      │   │ side            │
└────────────┘   └────────────┘   └─────────────────┘   └─────────────────┘

┌────────────┐                    ┌─────────────────┐
│ Infer grain│                    │ Check cardinality│
│ from row   │                    │ metadata for    │
│ counts     │                    │ high/low        │
└────────────┘                    └─────────────────┘
```

## 3. Optimization Metadata Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    METADATA ENRICHMENT                                   │
└─────────────────────────────────────────────────────────────────────────┘

INPUT STATISTICS                    GRAPH NODES                   OPTIMIZATION
                                                                  CAPABILITIES
┌──────────────┐                    ┌──────────────┐             ┌──────────────┐
│ Column Stats │                    │ Entity Node  │             │ Join Strategy│
│ - total_count│───────────────────▶│ - row_count  │────────────▶│ Recommendation│
│ - distinct   │                    │              │             │              │
│ - null_count │                    │              │             │ • Hash Join  │
└──────────────┘                    └──────────────┘             │   Build/Probe│
       │                                   │                     │ • Nested Loop│
       │                                   │                     │ • Merge Join │
       │         ┌─────────────────────────┘                     └──────────────┘
       │         │
       │         ▼
       │    ┌──────────────┐             ┌──────────────┐
       │    │ Size Category│             │ Pre-aggregate│
       │    │ Inference    │────────────▶│ Decision     │
       │    │              │             │              │
       │    │ Small  <100K │             │ Aggregate    │
       │    │ Medium 100K- │             │ large fact   │
       │    │        10M   │             │ before join  │
       │    │ Large  >10M  │             │ to dimension │
       │    └──────────────┘             └──────────────┘
       │
       ▼
┌──────────────┐                    ┌──────────────┐             ┌──────────────┐
│ Uniqueness   │                    │ Column Node  │             │ Cardinality  │
│ Detection    │───────────────────▶│ - unique     │────────────▶│ Inference    │
│              │                    │ - primary_key│             │              │
│ • is_unique  │                    │ - nullable   │             │ OneToOne     │
│ • distinct   │                    │              │             │ ManyToOne    │
│   = total    │                    │              │             │ OneToMany    │
└──────────────┘                    └──────────────┘             └──────────────┘
                                           │
                                           │
                                           ▼
                                    ┌──────────────┐             ┌──────────────┐
                                    │ Cardinality  │             │ Safe Path    │
                                    │ Metadata     │────────────▶│ Validation   │
                                    │              │             │              │
                                    │ high/low     │             │ Warn on      │
                                    │              │             │ high-card    │
                                    │              │             │ joins        │
                                    └──────────────┘             └──────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    FLOW SUMMARY                                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  1. Column statistics (from DB profiling) → Entity row_count            │
│  2. Row count → Size category (Small/Medium/Large)                      │
│  3. Size category → Join strategy hints (build vs probe)                │
│  4. Size comparison → Pre-aggregation recommendations                   │
│  5. Distinct/total counts → Column uniqueness flags                     │
│  6. Uniqueness flags → Cardinality inference (1:1, 1:N, N:1, N:N)      │
│  7. Cardinality → Safe path validation (prevent fan-out)                │
│  8. Distinct count → High/low cardinality metadata                      │
│  9. Cardinality metadata → Index recommendations                        │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

## 4. Query Method Decision Tree

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   WHICH QUERY METHOD TO USE?                             │
└─────────────────────────────────────────────────────────────────────────┘

                        START: What do you need?
                                    │
                ┌───────────────────┼───────────────────┐
                │                   │                   │
                ▼                   ▼                   ▼
        ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
        │  Table-level  │   │  Column-level │   │  Combined/    │
        │  information  │   │  information  │   │  Optimization │
        └───────┬───────┘   └───────┬───────┘   └───────┬───────┘
                │                   │                   │
    ┌───────────┴───────────┐       │       ┌───────────┴───────────┐
    ▼                       ▼       │       ▼                       ▼
┌────────┐            ┌────────┐   │   ┌────────┐            ┌────────┐
│ Need   │            │ Need   │   │   │ Need   │            │ Need   │
│ join   │            │ safety │   │   │ path + │            │ join   │
│ path?  │            │ check? │   │   │ columns│            │ hints? │
└───┬────┘            └───┬────┘   │   └───┬────┘            └───┬────┘
    │                     │        │       │                     │
    ▼                     ▼        │       ▼                     ▼
┌────────────┐        ┌─────────────┐  ┌─────────────────────┐  ┌──────────────┐
│find_path() │        │validate_    │  │find_path_with_      │  │find_best_    │
│            │        │safe_path()  │  │required_columns()   │  │join_strategy│
│Returns:    │        │             │  │                     │  │              │
│JoinPath    │        │Returns:     │  │Returns:             │  │Returns:      │
│            │        │Result<()>   │  │(JoinPath, Vec<Col>) │  │JoinStrategy  │
└────────────┘        └─────────────┘  └─────────────────────┘  └──────────────┘

                ┌───────────────────┼───────────────────┐
                ▼                   ▼                   ▼
        ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
        │ Which columns │   │ Where did     │   │ Should I pre- │
        │ does measure  │   │ column come   │   │ aggregate?    │
        │ need?         │   │ from?         │   │               │
        └───────┬───────┘   └───────┬───────┘   └───────┬───────┘
                │                   │                   │
                ▼                   ▼                   ▼
        ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
        │required_      │   │column_        │   │should_        │
        │columns()      │   │lineage()      │   │aggregate_     │
        │               │   │               │   │before_join()  │
        │Returns:       │   │Returns:       │   │               │
        │Vec<ColumnRef> │   │Vec<ColumnRef> │   │Returns: bool  │
        └───────────────┘   └───────────────┘   └───────────────┘
                                │
                        ┌───────┴───────┐
                        ▼               ▼
                ┌───────────────┐   ┌───────────────┐
                │ Is column     │   │ Is column     │
                │ unique/PK?    │   │ high-card?    │
                └───────┬───────┘   └───────┬───────┘
                        │                   │
                        ▼                   ▼
                ┌───────────────┐   ┌───────────────┐
                │is_column_     │   │is_high_       │
                │unique()       │   │cardinality()  │
                │               │   │               │
                │Returns: bool  │   │Returns: bool  │
                └───────────────┘   └───────────────┘

                        ┌───────┐
                        │ Need  │
                        │ grain?│
                        └───┬───┘
                            │
                            ▼
                    ┌───────────────┐
                    │infer_grain()  │
                    │               │
                    │Returns: String│
                    └───────────────┘
```

## 5. Architecture Principles

### Single Graph Philosophy
- **One graph to rule them all**: Entity, column, measure, and calendar nodes coexist
- **Unified traversal**: All relationships (FK, lineage, dependencies) are edges
- **No synchronization**: No dual-graph sync issues
- **Natural composition**: Entity queries + column queries = hybrid queries

### Edge Type Semantics
```
BELONGS_TO:    column → entity          (ownership)
REFERENCES:    column → column          (foreign key)
DERIVED_FROM:  column → column(s)       (lineage/transformation)
DEPENDS_ON:    measure → column(s)      (measure dependencies)
JOINS_TO:      entity → entity          (table relationships + cardinality)
```

### Metadata-Driven Optimization
- Size categories guide join strategy (small = build, large = probe)
- Cardinality prevents unsafe fan-out (OneToMany warnings)
- Row counts enable pre-aggregation decisions
- Uniqueness flags support cardinality inference

### Query Interface Layers
1. **Entity-level**: Table relationships, join paths, grain inference
2. **Column-level**: Dependencies, lineage, property checks
3. **Hybrid**: Combined queries for real-world optimization scenarios

## 6. Real-World Usage Examples

### Example 1: Query Planning
```rust
// User asks: "Total sales amount by customer region"
// Planner needs to:

// 1. Find join path from sales to regions
let path = graph.find_path("sales", "regions")?;
// Result: sales → customers → regions

// 2. Get columns needed for the measure
let columns = graph.required_columns("sales.total_amount")?;
// Result: [sales.amount]

// 3. Get join strategy hints
let strategy = graph.find_best_join_strategy(&path)?;
// Result: 
//   - sales (Large) probe → customers (Small) build
//   - customers (Small) probe → regions (Small) build

// 4. Should we pre-aggregate?
let should_pre_agg = graph.should_aggregate_before_join(
    "sales.total_amount",
    "regions"
)?;
// Result: true (sales is Large, regions is Small)
```

### Example 2: Semantic Validation
```rust
// User tries: "Customer name by order details"
// Validator needs to check if path is safe

// 1. Find path (if it exists)
let path = graph.find_path("customers", "orders")?;
// Result: customers → orders

// 2. Validate it won't cause row duplication
graph.validate_safe_path("customers", "orders")?;
// Result: Error! OneToMany join from customers → orders
//         could duplicate customer names
```

### Example 3: Lineage Tracking
```rust
// User asks: "Where does sales.revenue come from?"

// 1. Trace column lineage
let lineage = graph.column_lineage("sales.revenue")?;
// Result: [sales.unit_price, sales.quantity]
//         (revenue = unit_price * quantity)

// 2. Continue tracing each source
let price_lineage = graph.column_lineage("sales.unit_price")?;
// Result: [products.list_price, sales.discount]
//         (unit_price derived from product price + discount)
```

## 7. Performance Characteristics

### Graph Construction
- **Time**: O(N + E) where N = nodes, E = edges
- **Space**: O(N + E) for graph + O(N) for each index
- **Typical**: < 100ms for models with 50 tables, 500 columns

### Query Performance
| Operation | Time Complexity | Typical |
|-----------|----------------|---------|
| `find_path()` | O(N + E) BFS | < 1ms for 3-hop path |
| `required_columns()` | O(E) edge traversal | < 1ms for typical measure |
| `column_lineage()` | O(E) edge traversal | < 1ms for 3-level lineage |
| `is_column_unique()` | O(1) property lookup | < 1μs |
| `find_best_join_strategy()` | O(path length) | < 1ms for 5-step path |

### Memory Usage
- **Entity node**: ~200 bytes
- **Column node**: ~150 bytes
- **Measure node**: ~250 bytes
- **Edge**: ~100 bytes
- **Typical model** (50 tables, 500 columns, 100 measures): ~200KB

## 8. Extension Points

### Custom Metadata
All nodes have a `metadata: HashMap<String, String>` field for custom properties:
- Cardinality hints (`"cardinality": "high"`)
- Performance hints (`"indexed": "true"`)
- Business context (`"pii": "true"`, `"sensitive": "true"`)
- Governance tags (`"owner": "finance"`, `"domain": "sales"`)

### Custom Edge Types
The `GraphEdge` enum can be extended with new edge types:
- `AGGREGATES_TO`: fact → dimension (rollup relationships)
- `PARTITIONED_BY`: entity → column (partitioning metadata)
- `INDEXED_ON`: entity → column (physical optimization)

### Custom Query Methods
The query interface can be extended with domain-specific methods:
- `find_canonical_path()`: Find "blessed" join path from model
- `check_access_policy()`: Verify user can access entities
- `estimate_cost()`: Predict query execution cost
- `suggest_materialization()`: Recommend derived tables

## Summary

The unified graph provides:
1. **Single source of truth** for all semantic metadata
2. **Flexible query interface** spanning entity, column, and hybrid levels
3. **Metadata-driven optimization** using size, cardinality, and statistics
4. **Natural composition** of simple queries into complex analyses
5. **Extension points** for custom metadata, edges, and queries

This architecture replaces dual-graph complexity with a single, coherent model that's easier to understand, extend, and optimize.
