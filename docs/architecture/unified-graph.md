# Unified Semantic Graph Architecture

## Table of Contents

1. [Overview](#overview)
2. [Design Principles](#design-principles)
3. [Node and Edge Types](#node-and-edge-types)
4. [Construction Process](#construction-process)
5. [Query Interface](#query-interface)
6. [Integration Points](#integration-points)
7. [Migration Guide](#migration-guide)
8. [Troubleshooting](#troubleshooting)

## Overview

The Unified Semantic Graph is a comprehensive architecture that replaces the dual-graph system (ModelGraph + ColumnLineageGraph) with a single unified graph containing all semantic elements as first-class nodes.

### Purpose

The unified graph serves as the single source of truth for:
- **Entity relationships**: Tables, dimensions, and their join patterns
- **Column metadata**: Data types, uniqueness, nullability
- **Measure dependencies**: Which columns each measure requires
- **Calendar definitions**: Time dimensions and grain levels
- **Data lineage**: How columns are derived and transformed
- **Query optimization**: Size categories, cardinality, join strategies

### Key Benefits

1. **Simplicity**: One graph instead of two, reducing mental overhead
2. **Completeness**: Full lineage from measure → column → entity in one structure
3. **Consistency**: No cross-graph synchronization issues
4. **Extensibility**: Easy to add new node or edge types
5. **Performance**: O(1) lookups via multiple specialized indices

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                       INPUT SOURCES                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │   DSL    │  │ Metadata │  │  Stats   │  │ Inferred │       │
│  │  Model   │  │          │  │          │  │ Relations│       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
└───────┼─────────────┼─────────────┼─────────────┼──────────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    UNIFIED GRAPH                                 │
│                                                                  │
│  Nodes:                    Edges:                               │
│  • Entity (tables)         • BELONGS_TO (col → entity)         │
│  • Column (fields)         • REFERENCES (col → col, FK)        │
│  • Measure (aggs)          • DERIVED_FROM (col → col, lineage) │
│  • Calendar (time)         • DEPENDS_ON (measure → col)        │
│                            • JOINS_TO (entity → entity)        │
│                                                                  │
│  Indices:                                                       │
│  • entity_index, column_index, measure_index, calendar_index   │
└─────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    QUERY INTERFACE                               │
│                                                                  │
│  Entity-level:             Column-level:         Hybrid:        │
│  • find_path()             • required_columns()  • find_path_   │
│  • validate_safe_path()    • column_lineage()      with_cols() │
│  • infer_grain()           • is_column_unique()  • join_       │
│                            • is_high_cardinality() strategy()  │
│                                                   • should_     │
│                                                     aggregate() │
└─────────────────────────────────────────────────────────────────┘
```

## Design Principles

### 1. Single Source of Truth

All semantic elements exist as nodes in one graph. There are no separate graphs for different concerns:

- **Old architecture**: ModelGraph (entities) + ColumnLineageGraph (columns)
- **New architecture**: UnifiedGraph (entities + columns + measures + calendars)

This eliminates:
- Cross-graph synchronization issues
- Duplicate metadata storage
- Complex update propagation logic
- Mental model complexity

### 2. Typed Edges for Relationships

Each edge type has a specific semantic meaning:

| Edge Type | Direction | Purpose | Example |
|-----------|-----------|---------|---------|
| `BELONGS_TO` | Column → Entity | Column ownership | `sales.amount → sales` |
| `REFERENCES` | Column → Column | Foreign key | `sales.customer_id → customers.id` |
| `DERIVED_FROM` | Column → Column(s) | Lineage | `revenue → [price, quantity]` |
| `DEPENDS_ON` | Measure → Column(s) | Measure dependencies | `total_revenue → amount` |
| `JOINS_TO` | Entity → Entity | Table joins | `sales → customers (N:1)` |

### 3. Rich Metadata for Optimization

Every node carries optimization metadata:

**Entity nodes**:
- `row_count`: Estimated table size
- `size_category`: Small (<100K), Medium (100K-10M), Large (>10M)
- `entity_type`: Fact, Dimension, Source, Calendar

**Column nodes**:
- `unique`: Uniqueness constraint flag
- `primary_key`: Primary key flag
- `nullable`: NULL constraint
- `data_type`: Type information

**Edge metadata**:
- `cardinality`: OneToOne, OneToMany, ManyToOne, ManyToMany
- `source`: Explicit, ForeignKey, Convention, Statistical

This metadata drives:
- Join strategy selection (hash join build/probe sides)
- Pre-aggregation decisions
- Cardinality estimation
- Safe path validation

### 4. Multi-Index Lookups

The graph maintains specialized indices for O(1) lookups:

```rust
pub struct UnifiedGraph {
    graph: DiGraph<GraphNode, GraphEdge>,
    
    // Indices for fast lookups
    node_index: HashMap<String, NodeIndex>,           // Any node by name
    entity_index: HashMap<String, NodeIndex>,         // Entity by name
    column_index: HashMap<String, NodeIndex>,         // "entity.column"
    measure_index: HashMap<String, NodeIndex>,        // "entity.measure"
    calendar_index: HashMap<String, NodeIndex>,       // Calendar by name
}
```

This enables:
- Fast entity lookups for path finding
- Quick column resolution for measure dependencies
- Efficient measure validation
- Rapid calendar queries

### 5. Composable Query Interface

The query interface is layered:

1. **Entity-level**: Work with tables and join relationships
2. **Column-level**: Work with fields and dependencies
3. **Hybrid**: Combine both for real-world optimization

This composability allows building complex queries from simple primitives.

## Node and Edge Types

### Node Types

See the complete node type hierarchy in [unified-graph-types.md](../diagrams/unified-graph-types.md).

#### EntityNode

Represents a table or dimension in the model.

```rust
pub struct EntityNode {
    pub name: String,                      // Logical name
    pub entity_type: EntityType,           // Fact, Dimension, Source, Calendar
    pub physical_name: Option<String>,     // Physical table name
    pub schema: Option<String>,            // Database schema
    pub row_count: Option<usize>,          // Estimated rows
    pub size_category: SizeCategory,       // Small, Medium, Large
    pub metadata: HashMap<String, String>, // Custom metadata
}
```

**Created from**: Tables and dimensions in DSL model

**Size categories**:
- `Small`: < 100K rows (typical dimensions)
- `Medium`: 100K-10M rows (medium facts)
- `Large`: > 10M rows (large facts)

#### ColumnNode

Represents a column in an entity.

```rust
pub struct ColumnNode {
    pub entity: String,                    // Owning entity
    pub name: String,                      // Column name
    pub data_type: DataType,               // String, Integer, Float, etc.
    pub nullable: bool,                    // NULL constraint
    pub unique: bool,                      // Uniqueness flag
    pub primary_key: bool,                 // Primary key flag
    pub metadata: HashMap<String, String>, // Custom metadata
}
```

**Created from**: Atoms (table) and attributes (dimension)

**Qualified name**: `entity.column` (e.g., `sales.amount`)

#### MeasureNode

Represents a pre-defined aggregation.

```rust
pub struct MeasureNode {
    pub name: String,                      // Measure name
    pub entity: String,                    // Source entity
    pub aggregation: String,               // Aggregation type
    pub source_column: Option<String>,     // Single source column
    pub expression: Option<String>,        // SQL expression
    pub metadata: HashMap<String, String>, // Custom metadata
}
```

**Created from**: Measure blocks in DSL model

**Qualified name**: `entity.measure` (e.g., `sales.total_revenue`)

#### CalendarNode

Represents a time dimension.

```rust
pub struct CalendarNode {
    pub name: String,                      // Calendar name
    pub physical_name: String,             // Physical table/source
    pub schema: Option<String>,            // Database schema
    pub date_column: String,               // Date column name
    pub grain_levels: Vec<String>,         // Available grains
    pub metadata: HashMap<String, String>, // Custom metadata
}
```

**Created from**: Calendar definitions (physical or generated)

**Grain levels**: day, week, month, quarter, year, etc.

### Edge Types

See the complete edge type hierarchy in [unified-graph-types.md](../diagrams/unified-graph-types.md).

#### BELONGS_TO Edge

Links a column to its owning entity.

```rust
pub struct BelongsToEdge {
    pub column: String,   // Qualified column name
    pub entity: String,   // Entity name
}
```

**Direction**: Column → Entity

**Created**: During column node creation (Phase 1)

**Example**: `sales.amount → sales`

#### REFERENCES Edge

Links a foreign key column to a primary key column.

```rust
pub struct ReferencesEdge {
    pub from_column: String,         // FK column
    pub to_column: String,           // PK column
    pub source: RelationshipSource,  // Provenance
}
```

**Direction**: FK Column → PK Column

**Created**: From inferred relationships (Phase 2)

**Example**: `sales.customer_id → customers.customer_id`

#### DERIVED_FROM Edge

Links a derived column to its source columns.

```rust
pub struct DerivedFromEdge {
    pub target: String,              // Derived column
    pub sources: Vec<String>,        // Source columns
    pub expression: Option<String>,  // Derivation logic
}
```

**Direction**: Derived Column → Source Column(s)

**Created**: From column transformation metadata

**Example**: `revenue → [unit_price, quantity]`

#### DEPENDS_ON Edge

Links a measure to the columns it depends on.

```rust
pub struct DependsOnEdge {
    pub measure: String,       // Qualified measure name
    pub columns: Vec<String>,  // Required columns
}
```

**Direction**: Measure → Column(s)

**Created**: By parsing measure SQL expressions (Phase 2)

**Example**: `sales.total_revenue → [sales.amount]`

#### JOINS_TO Edge

Links two entities via a join relationship.

```rust
pub struct JoinsToEdge {
    pub from_entity: String,              // Source entity
    pub to_entity: String,                // Target entity
    pub join_columns: Vec<(String, String)>, // Join column pairs
    pub cardinality: Cardinality,         // Relationship cardinality
    pub source: RelationshipSource,       // Provenance
}
```

**Direction**: Entity → Entity

**Created**: Aggregated from REFERENCES edges (Phase 2)

**Example**: `sales → customers` (join on customer_id, cardinality N:1)

## Construction Process

The graph is built in two phases:

### Phase 1: Node Creation

See detailed diagrams in [unified-graph-construction-phase1.md](../diagrams/unified-graph-construction-phase1.md).

#### Step 1: Create Entity Nodes

```rust
fn create_entity_nodes(&mut self, model: &Model) -> GraphBuildResult<()>
```

**Input**: DSL Model (tables and dimensions)

**Process**:
1. Iterate through all tables and dimensions
2. Create `EntityNode` for each
3. Determine entity type (Fact for tables, Dimension for dimensions)
4. Store physical name from `source` field
5. Add to graph and update `entity_index`

**Output**: Entity nodes with no edges

**Example**:
```rust
table sales {
    source "dbo.fact_sales";
    // ...
}
```
→ `EntityNode { name: "sales", entity_type: Fact, physical_name: "dbo.fact_sales", ... }`

#### Step 2: Create Column Nodes

```rust
fn create_column_nodes(
    &mut self,
    model: &Model,
    stats: &HashMap<(String, String), ColumnStats>
) -> GraphBuildResult<()>
```

**Input**: DSL Model + Column Statistics

**Process**:
1. For each table, create `ColumnNode` for each atom
2. For each dimension, create `ColumnNode` for key + attributes
3. Map DSL data types to graph data types
4. Enrich columns with statistics (uniqueness, cardinality)
5. Enrich entities with row counts and size categories
6. Create `BELONGS_TO` edges from columns to entities
7. Add to graph and update `column_index`

**Output**: Column nodes with BELONGS_TO edges to entities

**Example**:
```rust
table sales {
    atoms {
        amount decimal;
        quantity int;
    }
}
```
→ `ColumnNode { entity: "sales", name: "amount", data_type: Float, ... }`
→ `ColumnNode { entity: "sales", name: "quantity", data_type: Integer, ... }`

**Size category assignment**:
- Row count from first column's statistics
- `< 100K` → `Small`
- `100K - 10M` → `Medium`
- `> 10M` → `Large`

#### Step 3: Create Measure Nodes

```rust
fn create_measure_nodes(&mut self, model: &Model) -> GraphBuildResult<()>
```

**Input**: DSL Model (measure blocks)

**Process**:
1. Iterate through all measure blocks
2. For each measure, create `MeasureNode`
3. Store SQL expression
4. Set aggregation type to "CUSTOM" (measures use SQL)
5. Add to graph and update `measure_index`

**Output**: Measure nodes with no edges (dependencies added in Phase 2)

**Example**:
```rust
measures sales {
    total_revenue = { SUM(@amount) };
    avg_price = { SUM(@amount) / SUM(@quantity) };
}
```
→ `MeasureNode { name: "total_revenue", entity: "sales", expression: "SUM(@amount)", ... }`

#### Step 4: Create Calendar Nodes

```rust
fn create_calendar_nodes(&mut self, model: &Model) -> GraphBuildResult<()>
```

**Input**: DSL Model (calendar definitions)

**Process**:
1. Iterate through all calendars (physical and generated)
2. For each calendar, create `CalendarNode`
3. Extract grain levels from grain mappings
4. Store physical source or generated identifier
5. Add to graph and update `calendar_index`

**Output**: Calendar nodes with no edges

**Example**:
```rust
calendar fiscal {
    generated day+;
    fiscal_year_start january;
}
```
→ `CalendarNode { name: "fiscal", grain_levels: ["day", "week", "month", ...], ... }`

### Phase 2: Edge Creation

See detailed diagrams in [unified-graph-construction-phase2.md](../diagrams/unified-graph-construction-phase2.md).

#### Step 1: Create REFERENCES Edges

```rust
fn create_references_edges(
    &mut self,
    relationships: &[InferredRelationship]
) -> GraphBuildResult<()>
```

**Input**: Inferred relationships from inference engine

**Process**:
1. For each inferred relationship:
   - Build qualified column names (`entity.column`)
   - Look up column nodes in `column_index`
   - Convert inference source to graph source
   - Create `REFERENCES` edge from FK to PK
   - Add edge to graph

**Source conversion**:
- `DatabaseConstraint` → `ForeignKey`
- `Inferred` → `Statistical`
- `UserDefined` → `Explicit`

**Output**: REFERENCES edges between foreign key and primary key columns

**Example**:
```rust
InferredRelationship {
    from_table: "sales",
    from_column: "customer_id",
    to_table: "customers",
    to_column: "customer_id",
    source: DatabaseConstraint,
}
```
→ `REFERENCES { from: "sales.customer_id", to: "customers.customer_id", source: ForeignKey }`

#### Step 2: Create JOINS_TO Edges

```rust
fn create_joins_to_edges(
    &mut self,
    relationships: &[InferredRelationship]
) -> GraphBuildResult<()>
```

**Input**: Inferred relationships (same as Step 1)

**Process**:
1. Group relationships by entity pair `(from_table, to_table)`
2. For each entity pair:
   - Collect all join column pairs
   - Find the highest confidence relationship
   - Look up entity nodes
   - Create `JOINS_TO` edge with cardinality
   - Add edge to graph

**Cardinality preservation**:
- Uses cardinality from best (highest confidence) relationship
- Aggregates multiple column relationships into single entity join

**Output**: JOINS_TO edges between entities

**Example**:
```rust
InferredRelationship {
    from_table: "sales",
    to_table: "customers",
    cardinality: ManyToOne,
    confidence: 0.95,
}
```
→ `JOINS_TO { from: "sales", to: "customers", join_columns: [("customer_id", "customer_id")], cardinality: N:1 }`

#### Step 3: Create DEPENDS_ON Edges

```rust
fn create_depends_on_edges(&mut self, model: &Model) -> GraphBuildResult<()>
```

**Input**: DSL Model (measure definitions)

**Process**:
1. For each measure block and measure:
   - Parse SQL expression for `@atom_name` references
   - Use regex pattern `@(\w+)` to extract atom names
   - Build qualified column names
   - Look up column nodes
   - Create `DEPENDS_ON` edge from measure to column
   - Add edge to graph

**Atom reference pattern**: `@atom_name`

**Output**: DEPENDS_ON edges from measures to columns

**Example**:
```rust
Measure {
    name: "avg_price",
    expr: "SUM(@amount) / SUM(@quantity)",
}
```
→ `DEPENDS_ON { measure: "sales.avg_price", columns: ["sales.amount"] }`
→ `DEPENDS_ON { measure: "sales.avg_price", columns: ["sales.quantity"] }`

### Construction Entry Point

```rust
pub fn from_model_with_inference(
    model: &Model,
    relationships: &[InferredRelationship],
    stats: &HashMap<(String, String), ColumnStats>,
) -> GraphBuildResult<Self>
```

**Full construction flow**:
1. Create empty `UnifiedGraph`
2. **Phase 1**: Create all nodes
   - `create_entity_nodes(model)`
   - `create_column_nodes(model, stats)`
   - `create_measure_nodes(model)`
   - `create_calendar_nodes(model)`
3. **Phase 2**: Create all edges
   - `create_references_edges(relationships)`
   - `create_joins_to_edges(relationships)`
   - `create_depends_on_edges(model)`
4. Return fully constructed graph

**Error handling**: Returns `GraphBuildError` for:
- Duplicate entity/column/measure/calendar names
- Missing entity when creating columns
- Missing column when creating references
- Invalid measure references

## Query Interface

The query interface provides methods to traverse the graph and answer questions. See detailed diagrams in [unified-graph-query-interface.md](../diagrams/unified-graph-query-interface.md).

### Entity-Level Queries

#### find_path()

Find the shortest join path between two entities using BFS.

```rust
pub fn find_path(&self, from: &str, to: &str) -> QueryResult<JoinPath>
```

**Algorithm**: Breadth-First Search on `JOINS_TO` edges

**Returns**: `JoinPath` with a sequence of `JoinStep` containing:
- `from`: Source entity
- `to`: Target entity
- `cardinality`: Relationship cardinality ("N:1", "1:N", etc.)

**Example**:
```rust
let path = graph.find_path("sales", "regions")?;
// Returns: JoinPath {
//   steps: [
//     JoinStep { from: "sales", to: "customers", cardinality: "N:1" },
//     JoinStep { from: "customers", to: "regions", cardinality: "N:1" },
//   ]
// }
```

**Use cases**:
- SQL query planning (determine join order)
- Semantic validation (verify entities can be joined)
- UI navigation (show relationships between tables)

#### validate_safe_path()

Check if a join path contains dangerous fan-out (OneToMany joins).

```rust
pub fn validate_safe_path(&self, from: &str, to: &str) -> QueryResult<()>
```

**Algorithm**:
1. Call `find_path()` to get join path
2. Check each step's cardinality
3. Return error if any step is `OneToMany`

**Returns**: `Ok(())` if safe, `Err(UnsafeJoinPath)` if dangerous

**Example**:
```rust
// Safe: Many-to-one joins don't duplicate rows
graph.validate_safe_path("sales", "customers")?; // OK

// Unsafe: One-to-many join causes row duplication
let result = graph.validate_safe_path("customers", "sales");
// Returns: Err(UnsafeJoinPath {
//   reason: "Join from customers to sales is OneToMany and may cause row duplication"
// })
```

**Use cases**:
- Prevent accidental row duplication in queries
- Warn users about fan-out before execution
- Validate semantic model definitions

#### infer_grain()

Determine the most granular entity in a set (highest row count).

```rust
pub fn infer_grain(&self, entities: &[&str]) -> QueryResult<String>
```

**Algorithm**:
1. Iterate through all entities
2. Find entity with highest `row_count`
3. Return that entity's name

**Returns**: Name of the grain entity

**Example**:
```rust
let grain = graph.infer_grain(&["sales", "customers", "products"])?;
// Returns: "sales" (has highest row count = most detailed)
```

**Use cases**:
- Automatic grain detection for queries
- Determine aggregation level
- Validate query grain consistency

### Column-Level Queries

#### required_columns()

Find all columns required by a measure.

```rust
pub fn required_columns(&self, measure_id: &str) -> QueryResult<Vec<ColumnRef>>
```

**Algorithm**:
1. Look up measure node by qualified name
2. Traverse `DEPENDS_ON` edges using BFS
3. Collect all column nodes reached
4. Return as `ColumnRef` (entity + column name)

**Returns**: Vector of `ColumnRef` containing entity and column name

**Example**:
```rust
let columns = graph.required_columns("sales.total_revenue")?;
// Returns: [ColumnRef { entity: "sales", column: "amount" }]

let columns = graph.required_columns("sales.avg_price")?;
// Returns: [
//   ColumnRef { entity: "sales", column: "amount" },
//   ColumnRef { entity: "sales", column: "quantity" },
// ]
```

**Use cases**:
- Determine which columns to SELECT
- Validate measure definitions
- Impact analysis (which measures use this column?)

#### column_lineage()

Trace column lineage (upstream dependencies via DERIVED_FROM edges).

```rust
pub fn column_lineage(&self, column_id: &str) -> QueryResult<Vec<ColumnRef>>
```

**Algorithm**:
1. Look up column node by qualified name
2. Traverse `DERIVED_FROM` edges backward using BFS
3. Collect all source column nodes
4. Return as `ColumnRef`

**Returns**: Vector of source columns

**Example**:
```rust
let lineage = graph.column_lineage("sales.revenue")?;
// Returns: [
//   ColumnRef { entity: "sales", column: "unit_price" },
//   ColumnRef { entity: "sales", column: "quantity" },
// ]
```

**Use cases**:
- Impact analysis (what happens if I change unit_price?)
- Data lineage visualization
- Debugging derived columns

#### is_column_unique()

Check if a column has a uniqueness constraint or is a primary key.

```rust
pub fn is_column_unique(&self, column_id: &str) -> QueryResult<bool>
```

**Algorithm**:
1. Look up column node
2. Return `column.unique || column.primary_key`

**Returns**: `true` if unique, `false` otherwise

**Example**:
```rust
let is_unique = graph.is_column_unique("customers.customer_id")?;
// Returns: true (primary key)

let is_unique = graph.is_column_unique("customers.name")?;
// Returns: false (not unique)
```

**Use cases**:
- Cardinality estimation
- Uniqueness validation
- Index recommendations

#### is_high_cardinality()

Check if a column has high cardinality (many distinct values).

```rust
pub fn is_high_cardinality(&self, column_id: &str) -> QueryResult<bool>
```

**Algorithm**:
1. Look up column node
2. Check `metadata.get("cardinality")` for "high" value

**Returns**: `true` if high cardinality, `false` otherwise

**Example**:
```rust
let is_high_card = graph.is_high_cardinality("sales.transaction_id")?;
// Returns: true (transaction IDs have high cardinality)
```

**Use cases**:
- Index recommendations
- Join strategy selection
- Aggregation warnings

### Hybrid Queries

Hybrid queries combine entity-level and column-level operations for real-world optimization scenarios.

#### find_path_with_required_columns()

Find join path AND required columns in one call.

```rust
pub fn find_path_with_required_columns(
    &self,
    from: &str,
    to: &str,
    measure_id: &str,
) -> QueryResult<(JoinPath, Vec<ColumnRef>)>
```

**Returns**: Tuple of (join path, required columns)

**Example**:
```rust
let (path, columns) = graph.find_path_with_required_columns(
    "sales",
    "customers",
    "sales.total_revenue"
)?;
// Returns: (
//   JoinPath { steps: [sales → customers] },
//   [ColumnRef { entity: "sales", column: "amount" }]
// )
```

**Use cases**:
- Query planning (get path and columns in one call)
- Optimization (avoid redundant lookups)

#### find_best_join_strategy()

Recommend join strategy based on entity size categories.

```rust
pub fn find_best_join_strategy(&self, path: &JoinPath) -> QueryResult<JoinStrategy>
```

**Algorithm**:
1. For each step in the path:
   - Get size categories of both entities
   - Recommend hash join build/probe sides based on sizes
2. Return `JoinStrategy` with recommendations

**Strategy rules**:
- Small entity → Hash join build side (fits in memory)
- Large entity → Hash join probe side (stream through)
- Both small → Nested loop is acceptable
- Both large → Hash join with left as build

**Returns**: `JoinStrategy` with hints for each step

**Example**:
```rust
let path = graph.find_path("sales", "customers")?;
let strategy = graph.find_best_join_strategy(&path)?;
// Returns: JoinStrategy {
//   steps: [
//     JoinStrategyStep {
//       step: JoinStep { from: "sales", to: "customers", cardinality: "N:1" },
//       left_hint: HashJoinProbe,    // sales is Large
//       right_hint: HashJoinBuild,   // customers is Small
//       reason: "sales is large (probe side), customers is small (build side)"
//     }
//   ]
// }
```

**Use cases**:
- Query optimization (choose join algorithm)
- Physical plan generation
- Performance tuning

#### should_aggregate_before_join()

Determine if pre-aggregation would reduce data volume before joining.

```rust
pub fn should_aggregate_before_join(
    &self,
    measure_id: &str,
    target_entity: &str,
) -> QueryResult<bool>
```

**Algorithm**:
1. Get measure's entity
2. Compare sizes of measure entity and target entity
3. Return `true` if measure entity is much larger

**Decision rules**:
- Large → Small: Pre-aggregate (reduces rows before join)
- Large → Medium: Pre-aggregate (reduces rows before join)
- Medium → Small: Pre-aggregate (reduces rows before join)
- Otherwise: Don't pre-aggregate

**Returns**: `true` if should pre-aggregate, `false` otherwise

**Example**:
```rust
let should_pre_agg = graph.should_aggregate_before_join(
    "sales.total_revenue",
    "customers"
)?;
// Returns: true (sales is Large, customers is Small)
```

**Use cases**:
- Query optimization (reduce data before join)
- Physical plan generation
- Performance tuning

## Integration Points

### QueryPlanner Integration

The QueryPlanner uses the unified graph to plan SQL queries from semantic queries.

**Integration points**:
1. **Join path finding**: `find_path()` to determine join order
2. **Column resolution**: `required_columns()` to build SELECT clause
3. **Grain inference**: `infer_grain()` to determine aggregation level
4. **Safety validation**: `validate_safe_path()` to prevent fan-out
5. **Join optimization**: `find_best_join_strategy()` for physical plan

**Example flow**:
```rust
// User asks: "Total revenue by customer region"
// 1. Resolve entities: sales, customers, regions
// 2. Find join path
let path = graph.find_path("sales", "regions")?;

// 3. Validate safety
graph.validate_safe_path("sales", "regions")?;

// 4. Get required columns for measure
let columns = graph.required_columns("sales.total_revenue")?;

// 5. Infer grain
let grain = graph.infer_grain(&["sales", "customers", "regions"])?;

// 6. Get join strategy
let strategy = graph.find_best_join_strategy(&path)?;

// 7. Generate SQL with optimizations
```

### Translation Layer Integration

The translation layer converts DSL queries to SQL using graph metadata.

**Integration points**:
1. **Entity lookup**: Use `entity_index` to resolve table names
2. **Column lookup**: Use `column_index` to resolve field references
3. **Measure lookup**: Use `measure_index` to expand measure definitions
4. **Join inference**: Use `JOINS_TO` edges for implicit joins

**Example**:
```rust
// DSL: select sales.total_revenue by customers.region
// 1. Resolve "sales.total_revenue" measure
let measure = graph.measure_index.get("sales.total_revenue")?;

// 2. Get measure dependencies
let columns = graph.required_columns("sales.total_revenue")?;

// 3. Find join path to customers
let path = graph.find_path("sales", "customers")?;

// 4. Generate SQL:
// SELECT customers.region, SUM(sales.amount) as total_revenue
// FROM sales
// JOIN customers ON sales.customer_id = customers.customer_id
// GROUP BY customers.region
```

### Semantic Validation Integration

The semantic validator uses the graph to validate model definitions and queries.

**Integration points**:
1. **Circular dependency detection**: Traverse `DERIVED_FROM` edges
2. **Orphan detection**: Find columns with no `BELONGS_TO` edge
3. **Missing FK detection**: Find slicers without `REFERENCES` edges
4. **Measure validation**: Check `DEPENDS_ON` edges point to valid columns
5. **Cardinality validation**: Check `JOINS_TO` edges have valid cardinality

**Example checks**:
```rust
// Check for circular dependencies in column derivations
fn check_circular_derivation(graph: &UnifiedGraph, column: &str) -> bool {
    let mut visited = HashSet::new();
    let mut stack = vec![column];
    
    while let Some(current) = stack.pop() {
        if visited.contains(current) {
            return true; // Circular dependency detected
        }
        visited.insert(current);
        
        // Traverse DERIVED_FROM edges
        for source in graph.column_lineage(current)? {
            stack.push(&source.qualified_name());
        }
    }
    
    false
}
```

## Migration Guide

### From ModelGraph + ColumnLineageGraph

The unified graph replaces the dual-graph architecture. Here's how to migrate:

#### Before (Old Architecture)

```rust
// Old: Two separate graphs
let model_graph = ModelGraph::from_model(&model)?;
let lineage_graph = ColumnLineageGraph::new();

// Old: Cross-graph operations
let entity = model_graph.get_entity("sales")?;
let columns = lineage_graph.get_columns_for_entity("sales")?;

// Old: Separate indices
let entity_idx = model_graph.entity_by_name("sales")?;
let column_idx = lineage_graph.column_by_name("sales.amount")?;

// Old: Manual synchronization
model_graph.update_entity("sales", entity)?;
lineage_graph.update_columns_for_entity("sales")?; // Must remember to sync!
```

#### After (New Architecture)

```rust
// New: Single unified graph
let graph = UnifiedGraph::from_model_with_inference(
    &model,
    &relationships,
    &stats
)?;

// New: Unified operations
let entity_idx = graph.entity_index.get("sales")?;
let column_idx = graph.column_index.get("sales.amount")?;

// New: No synchronization needed (single source of truth)
// Just rebuild the graph when model changes
```

#### Migration Checklist

1. **Replace graph construction**:
   - Old: `ModelGraph::from_model()` + `ColumnLineageGraph::new()`
   - New: `UnifiedGraph::from_model_with_inference()`

2. **Update entity lookups**:
   - Old: `model_graph.get_entity(name)`
   - New: `graph.entity_index.get(name)` + `graph.graph.node_weight(idx)`

3. **Update column lookups**:
   - Old: `lineage_graph.get_column(qualified_name)`
   - New: `graph.column_index.get(qualified_name)` + `graph.graph.node_weight(idx)`

4. **Update join path finding**:
   - Old: `model_graph.find_path(from, to)`
   - New: `graph.find_path(from, to)` (same API, unified implementation)

5. **Update lineage tracing**:
   - Old: `lineage_graph.trace_lineage(column)`
   - New: `graph.column_lineage(column)`

6. **Update measure resolution**:
   - Old: Custom logic in query planner
   - New: `graph.required_columns(measure)`

7. **Remove synchronization logic**:
   - Delete all cross-graph update code
   - Delete dual-graph consistency checks
   - Simplify model update logic

#### Breaking Changes

1. **Node access**: Must use indices + `graph.node_weight()` instead of direct access
2. **Edge iteration**: Must use `graph.edges()` instead of separate edge lists
3. **Type checking**: Use pattern matching on `GraphNode` and `GraphEdge` enums
4. **Error handling**: New error types (`GraphBuildError`, `QueryError`)

#### Compatibility Layer (Optional)

For gradual migration, create compatibility wrappers:

```rust
// Wrapper for old ModelGraph API
impl UnifiedGraph {
    pub fn get_entity(&self, name: &str) -> Option<&EntityNode> {
        self.entity_index
            .get(name)
            .and_then(|idx| self.graph.node_weight(*idx))
            .and_then(|node| match node {
                GraphNode::Entity(e) => Some(e),
                _ => None,
            })
    }
    
    pub fn get_column(&self, qualified_name: &str) -> Option<&ColumnNode> {
        self.column_index
            .get(qualified_name)
            .and_then(|idx| self.graph.node_weight(*idx))
            .and_then(|node| match node {
                GraphNode::Column(c) => Some(c),
                _ => None,
            })
    }
}
```

## Troubleshooting

### Common Issues

#### Issue 1: EntityNotFound Error

**Symptom**:
```rust
Error: EntityNotFound("customers")
```

**Causes**:
1. Entity name typo
2. Entity not defined in DSL model
3. Graph built before model updated

**Solutions**:
1. Check entity name spelling
2. Verify entity exists in `model.items`
3. Rebuild graph after model changes

**Debug**:
```rust
// List all entities
for (name, _idx) in &graph.entity_index {
    println!("Entity: {}", name);
}
```

#### Issue 2: ColumnNotFound Error

**Symptom**:
```rust
Error: ColumnNotFound("sales.amount")
```

**Causes**:
1. Column name typo
2. Column not defined as atom or attribute
3. Wrong entity name in qualified name

**Solutions**:
1. Check qualified name format (entity.column)
2. Verify column exists in atoms or attributes
3. Ensure entity exists before adding columns

**Debug**:
```rust
// List all columns for an entity
for (qualified_name, _idx) in &graph.column_index {
    if qualified_name.starts_with("sales.") {
        println!("Column: {}", qualified_name);
    }
}
```

#### Issue 3: NoPathFound Error

**Symptom**:
```rust
Error: NoPathFound { from: "sales", to: "regions" }
```

**Causes**:
1. No join relationship between entities
2. Missing inferred relationships
3. Disconnected entity graph

**Solutions**:
1. Check if relationship exists in database schema
2. Run inference engine to detect relationships
3. Add explicit relationship in DSL

**Debug**:
```rust
// List all JOINS_TO edges
for edge_ref in graph.graph.edge_references() {
    if let GraphEdge::JoinsTo(edge) = edge_ref.weight() {
        println!("Join: {} → {}", edge.from_entity, edge.to_entity);
    }
}
```

#### Issue 4: UnsafeJoinPath Error

**Symptom**:
```rust
Error: UnsafeJoinPath {
    from: "customers",
    to: "orders",
    reason: "Join from customers to orders is OneToMany..."
}
```

**Causes**:
1. Joining in wrong direction (should be orders → customers, not customers → orders)
2. Query requires fan-out (intentional duplication)

**Solutions**:
1. Reverse join direction (use many-to-one instead of one-to-many)
2. If fan-out is intentional, use `find_path()` instead of `validate_safe_path()`
3. Add explicit aggregation to handle duplicates

**Debug**:
```rust
// Check cardinality of join
let path = graph.find_path("customers", "orders")?;
for step in &path.steps {
    println!("Step: {} → {} ({})", step.from, step.to, step.cardinality);
}
```

#### Issue 5: Duplicate Entity/Column Error

**Symptom**:
```rust
Error: DuplicateEntity("sales")
```

**Causes**:
1. Entity defined multiple times in DSL
2. Table and dimension with same name
3. Model merge conflict

**Solutions**:
1. Search DSL for duplicate entity definitions
2. Rename one of the conflicting entities
3. Remove duplicate from model

**Debug**:
```rust
// Find duplicates before building
let mut entity_names = HashSet::new();
for item in &model.items {
    match &item.value {
        Item::Table(table) => {
            if !entity_names.insert(&table.name.value) {
                eprintln!("Duplicate table: {}", table.name.value);
            }
        }
        Item::Dimension(dim) => {
            if !entity_names.insert(&dim.name.value) {
                eprintln!("Duplicate dimension: {}", dim.name.value);
            }
        }
        _ => {}
    }
}
```

### Debugging Techniques

#### Visualize Graph Structure

```rust
// Print graph summary
fn print_graph_summary(graph: &UnifiedGraph) {
    println!("Entities: {}", graph.entity_index.len());
    println!("Columns: {}", graph.column_index.len());
    println!("Measures: {}", graph.measure_index.len());
    println!("Calendars: {}", graph.calendar_index.len());
    println!("Edges: {}", graph.graph.edge_count());
}
```

#### Export to DOT Format

```rust
// Export graph to Graphviz DOT format for visualization
fn export_to_dot(graph: &UnifiedGraph, output_path: &str) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Write;
    
    let mut file = File::create(output_path)?;
    writeln!(file, "digraph UnifiedGraph {{")?;
    
    // Write nodes
    for (name, idx) in &graph.entity_index {
        writeln!(file, "  \"{}\" [shape=box,color=blue];", name)?;
    }
    for (name, idx) in &graph.column_index {
        writeln!(file, "  \"{}\" [shape=ellipse,color=green];", name)?;
    }
    
    // Write edges
    for edge_ref in graph.graph.edge_references() {
        let source = graph.graph.node_weight(edge_ref.source()).unwrap();
        let target = graph.graph.node_weight(edge_ref.target()).unwrap();
        let edge_type = edge_ref.weight().edge_type();
        
        writeln!(
            file,
            "  \"{}\" -> \"{}\" [label=\"{}\"];",
            source.qualified_name(),
            target.qualified_name(),
            edge_type
        )?;
    }
    
    writeln!(file, "}}")?;
    Ok(())
}

// Usage:
// export_to_dot(&graph, "graph.dot")?;
// dot -Tpng graph.dot -o graph.png
```

#### Trace Query Execution

```rust
// Add logging to query methods
fn find_path_debug(graph: &UnifiedGraph, from: &str, to: &str) {
    println!("Finding path from {} to {}", from, to);
    
    let path = graph.find_path(from, to);
    match path {
        Ok(path) => {
            println!("Found path with {} steps:", path.steps.len());
            for (i, step) in path.steps.iter().enumerate() {
                println!("  Step {}: {} → {} ({})", 
                    i + 1, step.from, step.to, step.cardinality);
            }
        }
        Err(e) => {
            println!("Path finding failed: {:?}", e);
        }
    }
}
```

### Performance Troubleshooting

#### Slow Graph Construction

**Symptoms**: Graph building takes > 1 second for medium models

**Solutions**:
1. Profile with `cargo flamegraph`
2. Check for O(n²) operations in custom code
3. Verify statistics aren't being recomputed repeatedly
4. Use batch operations for edge creation

#### Slow Query Performance

**Symptoms**: `find_path()` takes > 10ms for simple queries

**Solutions**:
1. Check graph size (nodes + edges)
2. Verify indices are being used (should be O(1) lookups)
3. Profile BFS traversal for bottlenecks
4. Consider caching frequent path queries

#### High Memory Usage

**Symptoms**: Graph uses > 10MB for small models

**Solutions**:
1. Check for metadata bloat (large HashMap values)
2. Verify no duplicate nodes or edges
3. Consider using `Arc<str>` for shared strings
4. Profile with `valgrind --tool=massif`

---

## Summary

The Unified Semantic Graph provides:

1. **Single Source of Truth**: One graph for all semantic metadata
2. **Rich Metadata**: Optimization hints embedded in nodes and edges
3. **Flexible Queries**: Entity, column, and hybrid query methods
4. **Easy Integration**: Clean APIs for QueryPlanner, translation, validation
5. **Extensibility**: Custom metadata, edge types, and query methods

**Key files**:
- Implementation: `/src/semantic/graph/`
- Types: `/src/semantic/graph/types.rs`
- Builder: `/src/semantic/graph/builder.rs`
- Queries: `/src/semantic/graph/query.rs`
- Tests: `/src/semantic/graph/integration_tests.rs`

**Related documentation**:
- [Type System Diagrams](../diagrams/unified-graph-types.md)
- [Construction Phase 1](../diagrams/unified-graph-construction-phase1.md)
- [Construction Phase 2](../diagrams/unified-graph-construction-phase2.md)
- [Query Interface](../diagrams/unified-graph-query-interface.md)
- [System Overview](../diagrams/unified-graph-system-overview.md)
