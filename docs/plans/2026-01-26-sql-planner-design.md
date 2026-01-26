# SQL Planner Design - Cost-Based Query Optimization

**Date:** 2026-01-26  
**Status:** Design  
**Context:** Complete the DSL flow by rebuilding the SQL planner to work with the new unified graph architecture

## Background

The SQL planner is the missing piece in the new DSL flow:

```
DSL → AST → Lowering → Model → Unified Graph → [SQL Planner] → Multi-dialect SQL
```

The old planner (archived) worked with the previous model structure and used a 4-phase pipeline (Resolve → Validate → Plan → Emit). With the new unified graph providing rich metadata (entity sizes, cardinality, column stats, relationship sources), we can build a sophisticated cost-based optimizer.

## Goals

1. **Complete the flow** - Enable report compilation from DSL to optimized SQL
2. **Performance-first** - Generate the most efficient SQL possible using cost-based optimization
3. **Multi-dialect support** - Leverage the existing Query builder for dialect-specific emission
4. **Maintainability** - Clear separation of concerns across planning phases
5. **Testability** - Comprehensive integration tests to validate the entire pipeline

## Architecture

### Three-Phase Pipeline

The planner uses a three-phase architecture inspired by modern query optimizers (Calcite, DataFusion, Spark SQL):

```
Input: model::Report + UnifiedGraph
           ↓
┌──────────────────────────────────────┐
│ Phase 1: Logical Planning            │
│                                       │
│ Input:  model::Report                │
│ Output: LogicalPlan                  │
│                                       │
│ Responsibilities:                    │
│ - Parse report structure             │
│ - Build abstract operation tree      │
│ - No physical decisions              │
└──────────────────────────────────────┘
           ↓
┌──────────────────────────────────────┐
│ Phase 2: Physical Candidates         │
│                                       │
│ Input:  LogicalPlan + UnifiedGraph   │
│ Output: Vec<PhysicalPlan>            │
│                                       │
│ Responsibilities:                    │
│ - Generate alternative strategies    │
│ - Join order variants                │
│ - Aggregation placement options      │
│ - Time calculation strategies        │
│ - Staged generation with pruning     │
└──────────────────────────────────────┘
           ↓
┌──────────────────────────────────────┐
│ Phase 3: Cost Estimation & Selection │
│                                       │
│ Input:  Vec<PhysicalPlan>            │
│ Output: Query (SQL builder)          │
│                                       │
│ Responsibilities:                    │
│ - Estimate cost for each candidate   │
│ - Multi-objective scoring            │
│ - Select best plan                   │
│ - Convert to Query builder           │
└──────────────────────────────────────┘
           ↓
      Query.to_sql(dialect) → SQL
```

### Why Three Phases?

**Separation of concerns:**
- Logical plan: "What needs to happen" (semantics)
- Physical candidates: "Different ways to do it" (alternatives)
- Cost selection: "Pick the best one" (optimization)

**Benefits:**
- Each phase is independently testable
- Easy to add new physical strategies without changing logical planning
- Cost model can evolve without touching plan generation
- Clear debugging path when SQL looks wrong

## Phase 1: Logical Planning

### Input

```rust
fn plan_logical(report: &model::Report, graph: &UnifiedGraph) -> Result<LogicalPlan>
```

Takes:
- `model::Report` - The lowered report definition from the DSL
- `UnifiedGraph` - For basic metadata lookups during planning

### LogicalPlan Structure

Hybrid approach: Core relational operations + Report-specific nodes

```rust
enum LogicalPlan {
    // Core relational operations
    Scan(ScanNode),           // Table scan
    Join(JoinNode),           // Entity join
    Filter(FilterNode),       // WHERE conditions
    Aggregate(AggregateNode), // GROUP BY + aggregations
    
    // Report-specific operations (new)
    TimeMeasure(TimeMeasureNode),     // YTD, prior period, rolling avg
    DrillPath(DrillPathNode),         // Navigate calendar hierarchies
    InlineMeasure(InlineMeasureNode), // User-defined calculations
    
    // Output formatting
    Project(ProjectNode),     // SELECT list
    Sort(SortNode),          // ORDER BY
    Limit(LimitNode),        // LIMIT/OFFSET
}
```

**Why hybrid?**
- Core operations are universal (joins, filters, aggregations)
- Report-specific nodes make time calculations and drill paths first-class
- Cost model can see these operations explicitly and generate appropriate candidates

### Key Structures

```rust
struct ScanNode {
    entity: String,           // Table/dimension name
    entity_type: EntityType,  // Fact/Dimension/Calendar
}

struct JoinNode {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
    on: JoinCondition,        // Columns to join on
    cardinality: Cardinality, // From graph metadata
}

struct TimeMeasureNode {
    base_measure: String,
    time_suffix: TimeSuffix,  // YTD, PriorYear, etc.
    calendar: String,
    input: Box<LogicalPlan>,
}

struct AggregateNode {
    input: Box<LogicalPlan>,
    group_by: Vec<ColumnRef>,
    measures: Vec<MeasureRef>,
}
```

### Responsibilities

1. Parse `model::Report` structure into logical operations
2. Query `UnifiedGraph` for basic metadata (entity types, relationships)
3. Build operation tree representing semantic intent
4. **No physical decisions** - no CTEs, no join order, no pre-aggregation

## Phase 2: Physical Candidates

### Input

```rust
fn generate_candidates(
    logical: &LogicalPlan,
    graph: &UnifiedGraph
) -> Result<Vec<PhysicalPlan>>
```

### Candidate Types

Generate alternatives across multiple dimensions:

1. **Join Order Variants**
   - Different sequences for multi-table joins
   - Use graph metadata (size categories) for heuristic pruning
   - Example: `[ABC, ACB, BAC]` where A is fact, B/C are dimensions

2. **Aggregation Placement**
   - Pre-aggregate: Push GROUP BY before joins (reduces join cardinality)
   - Post-aggregate: Join first, aggregate after (avoids multiple aggregations)
   - Graph method: `should_aggregate_before_join(measure, target_entity)`

3. **Time Calculation Strategies**
   - Self-join: JOIN table to itself with date offset
   - Window functions: Use OVER() clauses
   - Filter-based: WHERE date range for YTD/QTD
   - Choose based on complexity and database capabilities

### Staged Generation (Pruning)

To avoid exponential explosion, use staged generation:

```
Stage 1: Generate per-dimension alternatives
├─ Join ordering: Use graph.find_best_join_strategy() for top-k candidates
├─ Aggregation: Binary choice per measure (pre/post)
└─ Time calc: 2-3 strategies per time measure

Stage 2: Compose compatible combinations
├─ Apply compatibility rules
│  Example: "Can't use window functions with pre-aggregation in same CTE"
├─ Use heuristics to limit search space
│  Example: "Only try top 3 join orders for queries with 5+ tables"

Stage 3: Yield final candidates (target: 10-20 candidates)
```

### PhysicalPlan Structure

```rust
struct PhysicalPlan {
    // Strategy choices
    join_order: Vec<String>,
    aggregation_strategy: HashMap<String, AggStrategy>,
    time_calc_strategy: HashMap<String, TimeStrategy>,
    
    // Lazy SQL generation (built on demand)
    query: OnceCell<Query>,
    
    // Cost estimation cache
    estimated_cost: OnceCell<f64>,
}

enum AggStrategy {
    PreAggregate,  // CTE with GROUP BY before join
    PostAggregate, // Join then GROUP BY
}

enum TimeStrategy {
    SelfJoin,      // JOIN with date offset
    WindowFunction, // OVER() clause
    FilterBased,   // WHERE date IN (...)
}
```

### Integration with Query Builder

Physical plans build `Query` objects incrementally:

```rust
impl PhysicalPlan {
    fn to_query(&self) -> &Query {
        self.query.get_or_init(|| self.build_query())
    }
    
    fn build_query(&self) -> Query {
        let mut query = Query::new();
        
        // Add CTEs for pre-aggregations
        if let Some(pre_agg) = self.pre_aggregation_cte() {
            query = query.with_cte(pre_agg);
        }
        
        // Build FROM + JOINs in specified order
        query = query.from(self.base_table());
        for join in self.build_joins() {
            query = query.join(join.join_type, join.table, join.on);
        }
        
        // Add WHERE, GROUP BY, SELECT, ORDER BY, LIMIT
        // ...
        
        query
    }
}
```

## Phase 3: Cost Estimation & Selection

### Cost Model

Multi-objective scoring with weighted factors:

```
cost = w1 * max_intermediate_size +  // Memory pressure
       w2 * total_rows_processed +   // CPU/IO work
       w3 * join_complexity +        // Number of joins
       w4 * aggregation_cost +       // GROUP BY cardinality
       w5 * subquery_depth           // Readability/optimizer hints
```

**Default weights:**
```rust
const DEFAULT_WEIGHTS: CostWeights = CostWeights {
    max_intermediate_size: 0.4,
    total_rows_processed: 0.3,
    join_complexity: 0.15,
    aggregation_cost: 0.1,
    subquery_depth: 0.05,
};
```

### Cost Estimation Details

1. **Intermediate Result Sizes**
   - Use row counts from `UnifiedGraph` entity metadata
   - Apply selectivity estimates for filters
   - Account for join cardinality (1:1, 1:N, N:1, N:N)

2. **Total Rows Processed**
   - Sum of: rows scanned + rows joined + rows aggregated
   - Pre-aggregation reduces join rows but adds aggregation work

3. **Join Complexity**
   - Penalize high join counts (nested loops expensive)
   - Favor broadcast joins for small dimensions (from size categories)

4. **Aggregation Cost**
   - Estimate GROUP BY cardinality from column metadata
   - High cardinality GROUP BY is expensive (many groups)

5. **Subquery Depth**
   - Prefer flatter queries (easier for DB optimizers)
   - CTEs at top level better than nested subqueries

### Selection

```rust
fn select_best_plan(candidates: Vec<PhysicalPlan>) -> PhysicalPlan {
    candidates
        .into_iter()
        .min_by(|a, b| {
            a.estimated_cost()
                .partial_cmp(&b.estimated_cost())
                .unwrap()
        })
        .unwrap()
}
```

Returns the candidate with lowest estimated cost.

## Metadata Usage from UnifiedGraph

The planner leverages rich metadata from the unified graph:

### Entity-Level
- **Row counts** → Estimate result sizes
- **Size categories** (Small/Medium/Large) → Join strategy hints
- **Entity types** (Fact/Dimension/Calendar) → Structural decisions

### Column-Level
- **Uniqueness** → Cardinality estimates
- **Data types** → Type compatibility checks
- **Cardinality metadata** → GROUP BY cost estimation

### Relationship-Level
- **Cardinality** (1:1, 1:N, N:1, N:N) → Fan-out detection
- **Relationship source** (Explicit/FK/Convention/Statistical) → Trust levels

### Graph Query Methods
- `find_path(from, to)` → Join path discovery
- `validate_safe_path(from, to)` → Fan-out detection
- `find_best_join_strategy(path)` → Hash/nested loop recommendations
- `should_aggregate_before_join(measure, entity)` → Pre-agg decisions
- `required_columns(measure)` → Dependency resolution

## Integration Tests

Comprehensive test coverage to validate the entire pipeline and catch regressions.

### Test Structure

Both end-to-end and isolated tests:

1. **End-to-end (DSL → SQL)**
   - Parse DSL string
   - Lower to model
   - Build unified graph
   - Plan report
   - Emit SQL
   - Validate correctness

2. **Planner-focused (Report → SQL)**
   - Hand-craft `model::Report` objects
   - Test planner in isolation
   - Faster iteration

### Test Scenarios

#### A. Core Relational Patterns

**Single table with measures + filters:**
```rust
#[test]
fn test_single_table_aggregation() {
    // Report: SELECT SUM(revenue) FROM sales WHERE region = 'APAC'
    // Validates: Basic scan → filter → aggregate
}
```

**Two-table join (fact → dimension):**
```rust
#[test]
fn test_fact_dimension_join() {
    // Report: GROUP BY customers.region, SELECT SUM(sales.revenue)
    // Validates: Join path finding, ManyToOne safety
}
```

**Multi-table join with safe paths:**
```rust
#[test]
fn test_multi_table_safe_join() {
    // Report: sales → customers → regions (all ManyToOne)
    // Validates: Multi-hop join planning, fan-out prevention
}
```

#### B. Report-Specific Features

**Time calculations (YTD, prior period):**
```rust
#[test]
fn test_ytd_calculation() {
    // Report: revenue.ytd
    // Validates: TimeMeasure node → self-join or filter strategy
}

#[test]
fn test_prior_period() {
    // Report: revenue.prior_year
    // Validates: Self-join with date offset
}
```

**Drill paths:**
```rust
#[test]
fn test_drill_path_navigation() {
    // Report: GROUP BY dates.standard.month
    // Validates: Calendar hierarchy traversal
}
```

**Inline measures:**
```rust
#[test]
fn test_inline_measure() {
    // Report: net_profit = { revenue - costs }
    // Validates: Expression compilation, column dependencies
}
```

#### C. Optimization Validation

**Pre-aggregation vs post-aggregation:**
```rust
#[test]
fn test_pre_aggregation_selection() {
    // Large fact → small dimension
    // Validates: Cost model chooses pre-aggregation (reduces join size)
}

#[test]
fn test_post_aggregation_selection() {
    // Small fact → large dimension
    // Validates: Cost model chooses post-aggregation (avoids multiple aggs)
}
```

**Join order optimization:**
```rust
#[test]
fn test_join_order_small_build_side() {
    // Fact (large) → Dimension (small)
    // Validates: Small dimension used as hash join build side
}
```

**Filter pushdown:**
```rust
#[test]
fn test_filter_pushdown_to_cte() {
    // Filter on dimension should push into CTE
    // Validates: Filters applied before joins when safe
}
```

### Test Data

Use in-memory mock graphs with controlled metadata:

```rust
fn mock_sales_graph() -> UnifiedGraph {
    // Sales fact: 10M rows (Large)
    // Customers dimension: 50K rows (Small)
    // Products dimension: 1K rows (Small)
    // Relationships: sales → customers (N:1), sales → products (N:1)
}
```

### Assertion Strategy

For each test:
1. **Correctness** - Generated SQL is semantically correct
2. **Optimization** - Cost model picked the expected strategy
3. **Multi-dialect** - SQL valid for DuckDB, Postgres, T-SQL

Example:
```rust
let sql = planner.plan(&report, &graph)?.to_sql(Dialect::DuckDb);

// Correctness: Has expected structure
assert!(sql.contains("GROUP BY"));
assert!(sql.contains("SUM(revenue)"));

// Optimization: Used pre-aggregation (has CTE)
assert!(sql.contains("WITH"));

// Multi-dialect: Also works in T-SQL
let tsql = planner.plan(&report, &graph)?.to_sql(Dialect::TSql);
assert!(tsql.contains("FETCH")); // T-SQL pagination
```

## Module Structure

```
src/planner/
├── mod.rs              # Main entry point, orchestrates 3 phases
├── logical/
│   ├── mod.rs          # LogicalPlanner
│   ├── plan.rs         # LogicalPlan enum + nodes
│   └── builder.rs      # Report → LogicalPlan conversion
├── physical/
│   ├── mod.rs          # PhysicalPlanner
│   ├── plan.rs         # PhysicalPlan struct
│   ├── candidates.rs   # Candidate generation strategies
│   └── strategies/
│       ├── join.rs     # Join order variants
│       ├── aggregate.rs # Pre/post aggregation
│       └── time.rs     # Time calculation strategies
├── cost/
│   ├── mod.rs          # Cost estimator
│   ├── model.rs        # Cost model + weights
│   └── estimators/
│       ├── size.rs     # Result size estimation
│       ├── work.rs     # Total work estimation
│       └── cardinality.rs # GROUP BY cardinality
└── tests/
    ├── integration/
    │   ├── single_table.rs
    │   ├── joins.rs
    │   ├── time_calcs.rs
    │   └── optimization.rs
    └── unit/
        ├── logical_tests.rs
        ├── physical_tests.rs
        └── cost_tests.rs
```

## Implementation Phases

### Phase 1: Foundation
- Logical planner skeleton
- Basic LogicalPlan nodes (Scan, Join, Filter, Aggregate, Project)
- Simple Report → LogicalPlan conversion
- Unit tests for logical planning

### Phase 2: Physical Planning
- PhysicalPlan structure
- Basic candidate generation (single strategy per dimension)
- Query builder integration
- Unit tests for physical planning

### Phase 3: Cost Model
- Cost estimation framework
- Size/work estimators using graph metadata
- Multi-objective scoring
- Unit tests for cost estimation

### Phase 4: Advanced Strategies
- Join order optimization
- Pre/post aggregation strategies
- Time calculation strategies (self-join, window, filter)
- Candidate pruning heuristics

### Phase 5: Integration Tests
- Core relational pattern tests
- Report-specific feature tests
- Optimization validation tests
- Multi-dialect validation

### Phase 6: Optimization & Tuning
- Benchmark cost model on real queries
- Tune weights based on results
- Add missing strategies
- Performance profiling

## Open Questions

1. **Weight tuning** - How do we learn optimal weights? Manual tuning vs ML-based?
2. **Statistics freshness** - How often should we update row counts and cardinality estimates?
3. **Plan caching** - Should we cache physical plans for identical reports?
4. **Explain plan** - Should we expose cost estimates and reasoning to users?

## Success Criteria

1. **Completeness** - All report features compile to SQL (measures, time calcs, drill paths)
2. **Correctness** - Generated SQL produces accurate results for all test scenarios
3. **Performance** - Cost model selects efficient plans (validated by integration tests)
4. **Multi-dialect** - SQL valid for DuckDB, Postgres, T-SQL
5. **Maintainability** - Clear phase separation, comprehensive tests

## References

- Old planner: `archive/semantic/planner/`
- Query builder: `src/sql/query.rs`
- Unified graph: `src/semantic/graph/`
- Model types: `src/model/`
