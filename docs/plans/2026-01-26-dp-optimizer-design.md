# Dynamic Programming Join Optimizer - Design Document

**Date:** 2026-01-26  
**Status:** Design Complete, Ready for Implementation  
**Goal:** Fix Wave 2 optimizer to achieve meaningful performance improvements using Dynamic Programming

---

## Executive Summary

The current Wave 2 join optimizer is functionally complete but **achieves minimal performance improvement** (1.08x-1.17x vs expected 1.5x-5x). This design implements a **Dynamic Programming (DP) join optimizer** based on the classic Selinger algorithm to fix four critical issues:

1. **Join order selection** - Not minimizing intermediate result sizes
2. **Bushy join trees** - Only exploring left-deep trees
3. **Filter integration** - Not considering filter selectivity
4. **Small table priority** - Not reliably joining dimensions before fact tables

**Expected Outcome:** All 4 performance tests passing with required improvements.

---

## Problem Statement

### Current Performance Test Results

| Test | Expected | Actual | Gap |
|------|----------|--------|-----|
| Three-table optimization | ≥1.5x | 1.08x | ❌ Suboptimal join order |
| Star schema optimization | ≥5x | 1.00x | ❌ Not joining dims first |
| Bushy join benefit | ≥3x | 1.17x | ❌ Only left-deep trees |
| Filter optimization | ≥2x | 1.00x | ❌ Filter not integrated |

### Root Causes

1. **Limited tree exploration:** Only left-deep trees like `((A ⋈ B) ⋈ C) ⋈ D`
   - Missing optimal bushy plans like `(A ⋈ B) ⋈ (C ⋈ D)`

2. **Inaccurate cost estimation:** Current heuristics don't track intermediate cardinalities properly
   - Example: `ManyToMany` uses `left * sqrt(right)` instead of actual join selectivity

3. **Filter blindness:** Optimizer doesn't see filters, can't push them down
   - Example: Highly selective filter (0.1%) applied after expensive join instead of before

4. **No dimension-first heuristics:** Star schema doesn't prioritize small dimension joins
   - Example: Joins 10M fact table early instead of joining 100-row dimensions first

---

## Solution: Dynamic Programming Optimizer

### Core Algorithm - Selinger's Algorithm

The DP optimizer builds optimal plans **bottom-up** using optimal substructure:

```
Algorithm: DP Join Optimizer
Input: Tables T = {t1, t2, ..., tn}, Filters F
Output: Optimal join plan for all tables

1. Build JoinGraph from UnifiedGraph relationships
2. Classify filters by table dependencies
3. Base case: DP[{ti}] = Scan(ti) with applicable filters
4. For subset_size = 2 to n:
     For each subset S of size subset_size:
       For each partition (S1, S2) of S where S1 ∪ S2 = S:
         If S1 and S2 are joinable:
           plan = Join(DP[S1], DP[S2])
           cost = estimate_cost(plan)
           If cost < DP[S].cost:
             DP[S] = plan
5. Return DP[all tables]
```

**Key Insight:** If `(A ⋈ B) ⋈ C` is optimal, then `(A ⋈ B)` must be the optimal 2-table join. We reuse that subproblem!

**Complexity:**
- **Time:** O(2^N × N²) for N tables
- **Space:** O(2^N) to store one plan per subset
- **Practical limit:** ~10-12 tables (1024-4096 subsets)

### Architecture

```
src/planner/join_optimizer/
├── mod.rs              // Public API, strategy selection
├── dp_optimizer.rs     // DP algorithm (NEW)
├── join_graph.rs       // Join graph builder (NEW)
├── cardinality.rs      // Cardinality estimator (NEW)
└── legacy.rs           // Old enumeration/greedy (keep for comparison)
```

---

## Component 1: Join Graph

### Purpose
Extract join relationships from UnifiedGraph to know which tables CAN be joined.

### Data Structure

```rust
pub struct JoinGraph {
    tables: HashSet<String>,
    edges: HashMap<TablePair, JoinEdge>,
}

#[derive(Hash, Eq, PartialEq)]
struct TablePair(String, String);

struct JoinEdge {
    condition: JoinCondition,
    cardinality: Cardinality,
    join_columns: Vec<(String, String)>,
}

impl JoinGraph {
    /// Build join graph from UnifiedGraph.
    pub fn build(graph: &UnifiedGraph, tables: &[String]) -> Self;
    
    /// Check if two table sets can be joined.
    pub fn are_joinable(&self, s1: &TableSet, s2: &TableSet) -> bool;
    
    /// Get join edge information between tables.
    pub fn get_join_edge(&self, t1: &str, t2: &str) -> Option<&JoinEdge>;
}
```

### Algorithm

```rust
fn build(graph: &UnifiedGraph, tables: &[String]) -> JoinGraph {
    let mut edges = HashMap::new();
    
    for t1 in tables {
        for t2 in tables {
            if t1 >= t2 { continue; } // Avoid duplicates
            
            // Use UnifiedGraph.find_path() to get join info
            if let Some(path) = graph.find_path(t1, t2) {
                let edge = JoinEdge {
                    condition: extract_join_condition(&path),
                    cardinality: path.cardinality,
                    join_columns: path.join_columns,
                };
                edges.insert(TablePair(t1.clone(), t2.clone()), edge);
            }
        }
    }
    
    JoinGraph { tables: tables.iter().cloned().collect(), edges }
}
```

### Testing

- `test_build_join_graph_from_unified_graph` - Correctly extracts edges
- `test_are_tables_joinable` - Detects valid join paths
- `test_get_join_edge_info` - Retrieves cardinality and columns
- `test_disconnected_tables` - Handles no join path gracefully

---

## Component 2: Cardinality Estimator

### Purpose
Accurately estimate intermediate join result sizes using relationship metadata.

### Data Structure

```rust
pub struct CardinalityEstimator<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> CardinalityEstimator<'a> {
    /// Estimate output rows for a join.
    pub fn estimate_join_output(
        &self,
        left_rows: usize,
        right_rows: usize,
        join_info: &JoinEdge,
    ) -> usize;
    
    /// Estimate selectivity of a filter predicate.
    pub fn estimate_filter_selectivity(&self, filter: &Expr) -> f64;
    
    /// Get join column cardinality from graph.
    fn get_join_column_cardinality(
        &self, 
        table: &str, 
        column: &str
    ) -> usize;
}
```

### Join Cardinality Formulas

```rust
match join_info.cardinality {
    // 1:1 - Each left row matches at most one right row
    Cardinality::OneToOne => left_rows.min(right_rows),
    
    // 1:N - Each left row matches multiple right rows
    // Output is the "many" side (right)
    Cardinality::OneToMany => right_rows,
    
    // N:1 - Many left rows match one right row
    // Output is the "many" side (left)
    Cardinality::ManyToOne => left_rows,
    
    // N:N - Cross product reduced by join selectivity
    // Use foreign key column cardinality to estimate overlap
    Cardinality::ManyToMany => {
        let left_card = self.get_join_column_cardinality(left_table, join_col);
        let right_card = self.get_join_column_cardinality(right_table, join_col);
        
        // Selectivity based on distinct values in join columns
        let selectivity = 1.0 / left_card.max(right_card) as f64;
        ((left_rows * right_rows) as f64 * selectivity) as usize
    }
}
```

### Filter Selectivity

```rust
fn estimate_filter_selectivity(&self, filter: &Expr) -> f64 {
    match filter {
        Expr::BinaryOp { op: BinaryOp::Eq, left, right } => {
            // Equality on high-cardinality column: 0.001 (1 in 1000)
            // Equality on low-cardinality column: 0.1 (1 in 10)
            if let Expr::Column { entity, column } = left {
                if self.graph.is_high_cardinality(entity, column).unwrap_or(false) {
                    0.001
                } else {
                    0.1
                }
            } else {
                0.1
            }
        }
        Expr::BinaryOp { op: BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte, .. } => {
            0.33 // Range predicates: 1/3 selectivity
        }
        Expr::BinaryOp { op: BinaryOp::And, left, right } => {
            // AND combines multiplicatively
            self.estimate_filter_selectivity(left) * self.estimate_filter_selectivity(right)
        }
        Expr::BinaryOp { op: BinaryOp::Or, left, right } => {
            // OR combines with probability union
            let s1 = self.estimate_filter_selectivity(left);
            let s2 = self.estimate_filter_selectivity(right);
            s1 + s2 - (s1 * s2)
        }
        _ => 0.5 // Default: 50% selectivity
    }
}
```

### Example: Three-Table Join

```
Tables:
  sales: 10M rows
  products: 100K rows (N:1 with sales)
  categories: 1K rows (N:1 with products)

Naive order: sales ⋈ products ⋈ categories
  sales ⋈ products:
    - N:1 relationship
    - Output: 10M rows (all sales matched)
  
  10M ⋈ categories:
    - Via products, indirect N:1
    - Output: 10M rows
  
  Total intermediate rows: 10M + 10M = 20M

Optimal order: products ⋈ categories ⋈ sales
  products ⋈ categories:
    - N:1 relationship
    - Output: 100K rows (all products matched)
  
  100K ⋈ sales:
    - 1:N relationship (reverse of N:1)
    - Output: 10M rows (all sales)
  
  Total intermediate rows: 100K + 10M = 10.1M

Improvement: 20M / 10.1M ≈ 2x faster! ✅
```

### Testing

- `test_estimate_one_to_one_join` - 1:1 returns min(left, right)
- `test_estimate_one_to_many_join` - 1:N returns many side
- `test_estimate_many_to_many_join` - N:N uses column cardinality
- `test_filter_selectivity_reduces_cardinality` - Filters compound correctly
- `test_high_cardinality_columns` - Uses UnifiedGraph metadata

---

## Component 3: Filter Classification & Pushdown

### Purpose
Integrate filters into join optimization so they can be pushed down early.

### Data Structure

```rust
struct ClassifiedFilter {
    expr: Expr,
    referenced_tables: HashSet<String>,
    selectivity: f64,
}

impl DPOptimizer {
    fn classify_filters(&self, filters: Vec<Expr>) -> Vec<ClassifiedFilter> {
        filters.into_iter().map(|expr| {
            let tables = self.extract_referenced_tables(&expr);
            let selectivity = self.cardinality_estimator.estimate_filter_selectivity(&expr);
            
            ClassifiedFilter {
                expr,
                referenced_tables: tables,
                selectivity,
            }
        }).collect()
    }
    
    fn extract_referenced_tables(&self, expr: &Expr) -> HashSet<String> {
        // Recursively walk expression tree to find all Column references
        // and collect their entity names
        let mut tables = HashSet::new();
        self.collect_tables_from_expr(expr, &mut tables);
        tables
    }
}
```

### Filter Pushdown Strategy

```rust
impl DPOptimizer {
    fn build_base_plan(&self, table: &str) -> SubsetPlan {
        let mut plan = LogicalPlan::Scan(ScanNode { entity: table.to_string() });
        let mut estimated_rows = self.get_table_row_count(table);
        
        // Find filters that ONLY reference this table
        let applicable_filters: Vec<_> = self.filters.iter()
            .filter(|f| f.referenced_tables.len() == 1 
                     && f.referenced_tables.contains(table))
            .collect();
        
        // Apply filters and reduce cardinality
        if !applicable_filters.is_empty() {
            let predicates: Vec<_> = applicable_filters.iter()
                .map(|f| f.expr.clone())
                .collect();
            
            plan = LogicalPlan::Filter(FilterNode {
                input: Box::new(plan),
                predicates,
            });
            
            // Reduce estimated rows by filter selectivity
            for filter in &applicable_filters {
                estimated_rows = (estimated_rows as f64 * filter.selectivity) as usize;
            }
        }
        
        SubsetPlan {
            plan,
            estimated_rows,
            cost: self.estimate_cost(&plan),
            applicable_filters: vec![],
        }
    }
    
    fn apply_filters_to_join(&self, subset: &TableSet, plan: LogicalPlan) -> LogicalPlan {
        // Find filters that reference ALL tables in subset
        let applicable_filters: Vec<_> = self.filters.iter()
            .filter(|f| f.referenced_tables.is_subset(subset))
            .collect();
        
        if applicable_filters.is_empty() {
            return plan;
        }
        
        let predicates: Vec<_> = applicable_filters.iter()
            .map(|f| f.expr.clone())
            .collect();
        
        LogicalPlan::Filter(FilterNode {
            input: Box::new(plan),
            predicates,
        })
    }
}
```

### Example: Filter Pushdown

```
Query: SELECT * FROM sales JOIN products 
       WHERE sales.amount > 1000 
       AND products.category = 'Electronics'

Without pushdown:
  sales(10M) ⋈ products(100K) = 10M rows
  Then filter: 10M × 0.001 × 0.1 = 1K rows
  Work done: 10M rows processed

With pushdown:
  sales(10M) + filter(amount > 1000) = 10K rows
  products(100K) + filter(category = 'Electronics') = 10K rows
  10K ⋈ 10K = 10K rows
  Work done: 30K rows processed
  
Improvement: 10M / 30K ≈ 333x faster! ✅
```

---

## Component 4: DP Core Algorithm

### Data Structures

```rust
pub struct DPOptimizer<'a> {
    graph: &'a UnifiedGraph,
    join_graph: JoinGraph,
    filters: Vec<ClassifiedFilter>,
    cardinality_estimator: CardinalityEstimator<'a>,
    memo: HashMap<TableSet, SubsetPlan>,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct TableSet {
    tables: BTreeSet<String>, // BTreeSet for deterministic ordering
}

struct SubsetPlan {
    plan: LogicalPlan,
    estimated_rows: usize,
    cost: CostEstimate,
}
```

### Main Algorithm

```rust
impl<'a> DPOptimizer<'a> {
    pub fn optimize(
        &mut self,
        tables: Vec<String>,
        filters: Vec<Expr>,
    ) -> Option<LogicalPlan> {
        // 1. Build join graph from UnifiedGraph
        self.join_graph = JoinGraph::build(self.graph, &tables);
        
        // 2. Classify filters by table dependencies
        self.filters = self.classify_filters(filters);
        
        // 3. Base case: single-table plans with applicable filters
        for table in &tables {
            let plan = self.build_base_plan(table);
            self.memo.insert(TableSet::single(table), plan);
        }
        
        // 4. DP: build optimal plans for increasing subset sizes
        for size in 2..=tables.len() {
            for subset in self.generate_subsets(&tables, size) {
                self.find_best_plan_for_subset(&subset);
            }
        }
        
        // 5. Return optimal plan for all tables
        let all_tables = TableSet::from_vec(tables);
        self.memo.get(&all_tables).map(|p| p.plan.clone())
    }
    
    fn find_best_plan_for_subset(&mut self, subset: &TableSet) {
        let mut best_plan: Option<SubsetPlan> = None;
        let mut best_cost = f64::MAX;
        
        // Try all ways to partition subset into two joinable parts
        for (s1, s2) in self.enumerate_splits(subset) {
            // Skip if tables can't be joined
            if !self.join_graph.are_joinable(&s1, &s2) {
                continue;
            }
            
            let left = self.memo.get(&s1).unwrap();
            let right = self.memo.get(&s2).unwrap();
            
            // Try both join orders: left ⋈ right and right ⋈ left
            for (l, r) in [(left, right), (right, left)] {
                let candidate = self.build_join_plan(l, r, subset);
                let cost = candidate.cost.total();
                
                if cost < best_cost {
                    best_cost = cost;
                    best_plan = Some(candidate);
                }
            }
        }
        
        if let Some(plan) = best_plan {
            self.memo.insert(subset.clone(), plan);
        }
    }
    
    fn build_join_plan(
        &self,
        left: &SubsetPlan,
        right: &SubsetPlan,
        subset: &TableSet,
    ) -> SubsetPlan {
        // Get join edge between left and right table sets
        let join_edge = self.join_graph.get_join_edge_between_sets(
            &left.plan, 
            &right.plan
        ).unwrap();
        
        // Estimate output cardinality
        let estimated_rows = self.cardinality_estimator.estimate_join_output(
            left.estimated_rows,
            right.estimated_rows,
            join_edge,
        );
        
        // Build join plan
        let mut plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left.plan.clone()),
            right: Box::new(right.plan.clone()),
            on: join_edge.condition.clone(),
            join_type: JoinType::Inner,
            cardinality: Some(join_edge.cardinality),
        });
        
        // Apply filters that span this subset
        plan = self.apply_filters_to_join(subset, plan);
        
        // Estimate cost
        let cost = self.estimate_plan_cost(&plan, estimated_rows);
        
        SubsetPlan {
            plan,
            estimated_rows,
            cost,
        }
    }
    
    fn enumerate_splits(&self, subset: &TableSet) -> Vec<(TableSet, TableSet)> {
        let mut splits = Vec::new();
        let tables: Vec<_> = subset.tables.iter().collect();
        
        // Try all non-empty, non-full subsets as S1
        for size in 1..tables.len() {
            for s1_tables in combinations(&tables, size) {
                let s1 = TableSet::from_vec(s1_tables.clone());
                let s2_tables: Vec<_> = tables.iter()
                    .filter(|t| !s1_tables.contains(t))
                    .cloned()
                    .collect();
                let s2 = TableSet::from_vec(s2_tables);
                
                splits.push((s1, s2));
            }
        }
        
        splits
    }
}
```

### Complexity Analysis

**Time Complexity:**
- Subset generation: O(2^N) subsets
- For each subset: O(2^|S|) splits
- Join edge lookup: O(1) with HashMap
- Total: O(3^N) worst case, O(2^N × N²) practical

**Space Complexity:**
- Memo table: O(2^N) plans
- Each plan: O(N) for tree structure
- Total: O(2^N × N)

**Practical Limits:**
- N=10: 1,024 subsets ≈ 100KB memory, < 10ms
- N=12: 4,096 subsets ≈ 400KB memory, < 50ms
- N=15: 32,768 subsets ≈ 3MB memory, < 500ms

### Testing

- `test_generate_subsets` - All combinations for size N
- `test_enumerate_splits` - Valid S1/S2 partitions
- `test_classify_filters` - Filters tagged with table dependencies
- `test_base_plan_with_filters` - Single table + applicable filters
- `test_memo_reuse` - Subproblems solved once, reused

---

## Integration Strategy

### Optimizer Strategy Selection

```rust
// src/planner/physical/join_optimizer/mod.rs
pub enum OptimizerStrategy {
    Legacy,      // Current enumeration + greedy
    DP,          // New dynamic programming
    Adaptive,    // Choose based on table count
}

pub struct JoinOrderOptimizer<'a> {
    graph: &'a UnifiedGraph,
    strategy: OptimizerStrategy,
}

impl<'a> JoinOrderOptimizer<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self {
            graph,
            strategy: OptimizerStrategy::Adaptive, // Default
        }
    }
    
    pub fn with_strategy(graph: &'a UnifiedGraph, strategy: OptimizerStrategy) -> Self {
        Self { graph, strategy }
    }
    
    pub fn optimize(
        &self,
        tables: Vec<String>,
        filters: Vec<Expr>,
    ) -> Option<LogicalPlan> {
        match self.strategy {
            OptimizerStrategy::Legacy => {
                // Use old enumeration/greedy algorithm
                self.legacy_optimize(tables)
            }
            OptimizerStrategy::DP => {
                // Use new DP algorithm
                let mut dp_optimizer = DPOptimizer::new(self.graph);
                dp_optimizer.optimize(tables, filters)
            }
            OptimizerStrategy::Adaptive => {
                if tables.len() <= 10 {
                    // Use DP for small/medium queries
                    let mut dp_optimizer = DPOptimizer::new(self.graph);
                    dp_optimizer.optimize(tables, filters)
                } else {
                    // Fall back to greedy for large queries
                    self.legacy_optimize(tables)
                }
            }
        }
    }
}
```

### PhysicalConverter Integration

```rust
// src/planner/physical/converter.rs
impl<'a> PhysicalConverter<'a> {
    fn convert_multi_table_join(&self, join: &JoinNode) -> PlanResult<Vec<PhysicalPlan>> {
        let join_plan = LogicalPlan::Join(join.clone());
        let optimizer = JoinOrderOptimizer::new(self.graph);
        let tables = optimizer.extract_tables(&join_plan);
        
        // Extract filters from the logical plan (if any)
        let filters = self.extract_filters(&join_plan);
        
        // Use adaptive strategy (DP for ≤10 tables, greedy for >10)
        let optimized_candidates = if tables.len() <= 3 {
            // Small queries: enumerate all permutations
            optimizer.enumerate_join_orders(&join_plan)
        } else {
            // Medium/large queries: use DP or greedy
            vec![optimizer.optimize(tables.into_iter().collect(), filters)]
        };
        
        // Convert each logical candidate to multiple physical strategies
        let mut physical_plans = Vec::new();
        for logical_plan in optimized_candidates.into_iter().flatten() {
            physical_plans.extend(self.convert_logical_to_physical(&logical_plan)?);
        }
        
        Ok(physical_plans)
    }
}
```

---

## Testing Strategy

### Test Pyramid

```
Level 3: Performance Tests (4 existing, strengthen assertions)
  ↑ Verify ≥1.5x, ≥3x, ≥5x, ≥2x improvements
  
Level 2: Integration Tests (~8 new tests)
  ↑ Verify DP finds optimal plans end-to-end
  
Level 1: Unit Tests (~15 new tests)
  ↑ Verify JoinGraph, Cardinality, DP components
```

### Unit Tests (15 new)

**JoinGraph** (`join_graph_test.rs`)
1. `test_build_join_graph_from_unified_graph`
2. `test_are_tables_joinable`
3. `test_get_join_edge_info`
4. `test_disconnected_tables`

**CardinalityEstimator** (`cardinality_test.rs`)
1. `test_estimate_one_to_one_join`
2. `test_estimate_one_to_many_join`
3. `test_estimate_many_to_many_join`
4. `test_filter_selectivity_reduces_cardinality`
5. `test_high_cardinality_columns`

**DPOptimizer** (`dp_optimizer_test.rs`)
1. `test_generate_subsets`
2. `test_enumerate_splits`
3. `test_classify_filters`
4. `test_base_plan_with_filters`
5. `test_memo_reuse`
6. `test_subset_table_set_operations`

### Integration Tests (8 new)

**DP Algorithm Correctness** (`dp_integration_test.rs`)
1. `test_two_table_dp_optimal` - DP picks cheaper of 2 join orders
2. `test_three_table_dp_bushy_vs_left_deep` - DP explores bushy trees
3. `test_star_schema_dimensions_first` - DP joins small dims before fact
4. `test_chain_schema_optimal_order` - Linear chain A-B-C-D picks best
5. `test_filter_pushdown_reduces_cost` - Selective filter applied early
6. `test_dp_vs_greedy_comparison` - DP finds better plan than greedy
7. `test_disconnected_components` - Handles cartesian products
8. `test_memo_size_reasonable` - Memory usage within bounds

### Performance Tests (4 existing, strengthen)

**Must Pass with DP Optimizer:**
1. `test_three_table_optimization_improves_cost`
   - **Current:** 1.08x
   - **Required:** ≥1.5x
   - **Expected with DP:** ~2x (joins products⋈categories first)

2. `test_star_schema_optimization`
   - **Current:** 1.00x (broken)
   - **Required:** ≥5x
   - **Expected with DP:** ~10x (joins all dims first, then fact)

3. `test_bushy_join_benefit`
   - **Current:** 1.17x
   - **Required:** ≥3x
   - **Expected with DP:** ~5x (explores bushy trees)

4. `test_high_selectivity_filter_optimization`
   - **Current:** 1.00x (broken)
   - **Required:** ≥2x
   - **Expected with DP:** ~100x (0.1% filter pushed down early)

---

## Implementation Plan

### Phase 1: Foundation (Week 1)
**Goal:** Build JoinGraph and CardalityEstimator components

**Tasks:**
1. Create module structure (`join_graph.rs`, `cardinality.rs`)
2. Implement `JoinGraph::build()` from UnifiedGraph
3. Implement `JoinGraph::are_joinable()` and `get_join_edge()`
4. Write 4 JoinGraph unit tests
5. Implement `CardinalityEstimator::estimate_join_output()` with all formulas
6. Implement `CardinalityEstimator::estimate_filter_selectivity()`
7. Write 5 CardinalityEstimator unit tests

**Deliverable:** 9 unit tests passing, no integration yet

**Validation:**
- JoinGraph correctly extracts relationships
- Cardinality formulas match design
- All unit tests green

---

### Phase 2: DP Core Algorithm (Week 2)
**Goal:** Implement DP optimizer algorithm

**Tasks:**
1. Create `dp_optimizer.rs` with core types (TableSet, SubsetPlan)
2. Implement `generate_subsets()` and `enumerate_splits()`
3. Implement `classify_filters()` for filter pushdown
4. Implement `build_base_plan()` with filter application
5. Implement `find_best_plan_for_subset()` DP core logic
6. Implement `build_join_plan()` with cardinality estimation
7. Write 6 DP unit tests

**Deliverable:** 15 unit tests passing total, DP algorithm working in isolation

**Validation:**
- Subset enumeration correct
- Memo table populated correctly
- Filter classification working

---

### Phase 3: Integration (Week 2)
**Goal:** Wire DP into PhysicalConverter and add strategy selection

**Tasks:**
1. Refactor `join_optimizer/mod.rs` to support multiple strategies
2. Implement `OptimizerStrategy` enum and selection logic
3. Update `PhysicalConverter::convert_multi_table_join()` to use optimizer
4. Add filter extraction from logical plans
5. Write 8 integration tests

**Deliverable:** 23 tests passing (15 unit + 8 integration), end-to-end working

**Validation:**
- Integration tests verify DP finds optimal plans
- Star schema test shows dimension-first joins
- Filter pushdown test shows early filtering

---

### Phase 4: Performance Validation (Week 3)
**Goal:** Make all 4 performance tests pass

**Tasks:**
1. Run `test_three_table_optimization_improves_cost` - debug if needed
2. Run `test_star_schema_optimization` - debug if needed
3. Run `test_bushy_join_benefit` - debug if needed
4. Run `test_high_selectivity_filter_optimization` - debug if needed
5. Profile optimizer for 10-table queries (must be < 100ms)
6. Tune cardinality formulas if tests fail
7. Add debug logging to understand cost decisions

**Deliverable:** All 4 performance tests passing, < 100ms for 10 tables

**Validation:**
- ✅ Three-table: ≥1.5x improvement
- ✅ Star schema: ≥5x improvement
- ✅ Bushy join: ≥3x improvement
- ✅ Filter optimization: ≥2x improvement

---

### Phase 5: Documentation & Cleanup (Week 3)
**Goal:** Prepare for production use

**Tasks:**
1. Add rustdoc comments to all public APIs
2. Create usage examples in module docs
3. Update WAVE2_STATUS.md with results
4. Mark legacy optimizer as deprecated
5. Add performance benchmarks
6. Write migration guide

**Deliverable:** Production-ready DP optimizer with full documentation

**Validation:**
- All tests passing (27 total)
- Documentation complete
- Ready for deprecation of legacy optimizer

---

## Success Criteria

### Minimum Viable Success
- ✅ 3-table test passes (≥1.5x)
- ✅ Star schema test passes (≥5x)
- ✅ DP completes in < 100ms for 10 tables
- ✅ No regressions in existing queries

### Full Success
- ✅ All 4 performance tests pass
- ✅ DP outperforms legacy in all scenarios
- ✅ Integration tests validate correctness
- ✅ Ready for production deployment

### Stretch Goals
- 📊 Bushy join test passes (≥3x)
- 📊 Filter optimization test passes (≥2x)
- 📊 Performance profiling shows < 50ms for 10 tables
- 📊 Benchmark suite comparing DP vs legacy

---

## Risks & Mitigations

### Risk 1: DP doesn't achieve performance targets
**Probability:** Medium  
**Impact:** High - invalidates approach

**Mitigation:**
- Build integration tests early to validate DP finds optimal plans
- Compare DP vs naive manually before performance tests
- Keep legacy optimizer as fallback
- Start with correctness, tune cost formulas iteratively

### Risk 2: Memory usage too high
**Probability:** Low (only if > 12 tables)  
**Impact:** Medium

**Mitigation:**
- Hard limit: DP for ≤10 tables, greedy for 11+
- Add memory profiling in tests
- Document memory requirements clearly

### Risk 3: Performance tuning takes longer than expected
**Probability:** High  
**Impact:** Medium - delays but doesn't block

**Mitigation:**
- Timebox performance tuning to 1 week
- Accept "good enough" if 3/4 tests pass
- Can improve formulas iteratively after launch

### Risk 4: Filter integration complexity
**Probability:** Medium  
**Impact:** Medium - one test stays broken

**Mitigation:**
- Implement filter pushdown incrementally
- Can ship without it (filter test lowest priority)
- Focus on cardinality estimation first

---

## Post-Implementation: Deprecation Plan

**After DP proves superior (4 weeks after launch):**

1. **Mark legacy optimizer as deprecated** (Version 2.1)
   ```rust
   #[deprecated(
       since = "2.1.0",
       note = "Use OptimizerStrategy::DP instead. Legacy will be removed in 3.0"
   )]
   pub fn legacy_optimize(...) { ... }
   ```

2. **Remove from default builds** (8 weeks, behind feature flag)
   ```toml
   [features]
   legacy-optimizer = []  # Opt-in only
   ```

3. **Delete entirely** (Version 3.0)
   - Remove all legacy code
   - DP is the only optimizer

---

## Estimated Effort

### Development Time: 2-3 weeks

| Phase | Effort | Lines of Code |
|-------|--------|---------------|
| JoinGraph | 2-3 days | ~200 |
| CardinalityEstimator | 2-3 days | ~250 |
| DP core algorithm | 3-4 days | ~400 |
| Integration | 2-3 days | ~150 |
| Testing | 3-5 days | ~800 |
| Documentation | 1-2 days | ~200 (docs) |

**Total:** ~1,650 lines of production code + 800 lines of tests = **2,450 lines**

### Resource Requirements
- 1 engineer full-time for 3 weeks
- Code reviews: ~2 hours per phase (5 phases = 10 hours)
- Testing assistance: ~4 hours for performance validation

---

## Expected Outcomes

### Performance Improvements

| Test Scenario | Current | Expected | Improvement |
|---------------|---------|----------|-------------|
| 3-table join | 1.08x | ≥1.5x | ✅ 40%+ better |
| Star schema (10M + 3×100) | 1.00x | ≥5.0x | ✅ 5x+ better |
| Bushy join (4 tables) | 1.17x | ≥3.0x | ✅ 2.5x+ better |
| Filter optimization | 1.00x | ≥2.0x | ✅ 2x+ better |

### Quality Metrics
- ✅ 27 total tests (15 unit + 8 integration + 4 performance)
- ✅ 100% test coverage on DP algorithm
- ✅ < 100ms optimization time for 10 tables
- ✅ O(2^N) space complexity with practical limits documented

### Production Readiness
- ✅ Fully documented with rustdoc
- ✅ Comprehensive error handling
- ✅ Performance benchmarks included
- ✅ Migration path from legacy optimizer
- ✅ Feature flag support for gradual rollout

---

## Conclusion

This design implements a production-grade Dynamic Programming join optimizer based on proven database research (Selinger algorithm). It addresses all four root causes of the current optimizer's poor performance:

1. ✅ **Join order selection** - DP finds optimal order by trying all valid combinations
2. ✅ **Bushy join trees** - DP explores all tree shapes, not just left-deep
3. ✅ **Filter integration** - Filters classified and pushed down early
4. ✅ **Cardinality estimation** - Accurate formulas using graph metadata

**Expected Result:** All 4 performance tests passing with meaningful improvements (1.5x-5x faster plans).

**Timeline:** 3 weeks development + testing  
**Risk:** Medium (standard algorithm, may need tuning)  
**Reward:** High (foundation for future optimizations, production-ready optimizer)

---

**Ready for Implementation:** ✅  
**Next Step:** Create implementation plan with superpowers:writing-plans skill
