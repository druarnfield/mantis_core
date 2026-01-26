# SQL Planner Wave 2: Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Optimize query performance through advanced cost estimation and join order optimization using UnifiedGraph metadata.

**Architecture:** Enhance cost estimator to use actual cardinality, selectivity, and relationship metadata from UnifiedGraph. Implement multi-objective cost model (CPU, IO, memory) with weighted scoring. Add join order optimizer with enumeration for small joins and greedy heuristic for large joins.

**Tech Stack:** Rust, UnifiedGraph metadata, existing `src/planner/cost/` module, petgraph for graph queries

---

## WAVE 2: OPTIMIZATION

### Task 1: CostEstimate Struct with Multi-Objective Scoring

**Purpose:** Replace simple numeric cost with detailed breakdown (rows, CPU, IO, memory)

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/cost_estimate_test.rs`

**Test:** Create CostEstimate instances, verify total() calculates weighted sum correctly (IO weighted higher than CPU/memory)

**Implement:** Replace `f64` cost with CostEstimate struct containing rows_out, cpu_cost, io_cost, memory_cost. Add total() method with configurable weights. Reference design doc "Component 1: Advanced Cost Estimator".

**Verify:** total() produces correct weighted values, individual components accessible

**Commit:** `feat(planner): add multi-objective CostEstimate struct`

---

### Task 2: Enhanced TableScan Cost Estimation

**Purpose:** Use actual row counts from UnifiedGraph instead of hardcoded estimates

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Modify: `src/semantic/graph/mod.rs` (if get_entity_row_count doesn't exist)
- Test: `tests/planner/table_scan_cost_test.rs`

**Test:** Mock UnifiedGraph with entity row counts, verify TableScan cost uses actual values

**Implement:** In estimate_cost() for TableScan, call `graph.get_entity_row_count(table)`. Calculate IO cost based on strategy (FullScan = full rows, IndexScan = 10% of rows). Set rows_out to row count. See design doc "estimate_cost() - TableScan".

**Verify:** Cost varies correctly with different row counts and scan strategies

**Commit:** `feat(planner): use actual row counts for table scan costs`

---

### Task 3: Filter Selectivity Estimation - Equality Predicates

**Purpose:** Estimate how selective equality filters are using column cardinality

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/filter_selectivity_test.rs`

**Test:** Filter with col = value on high-cardinality column returns 0.001 selectivity, low-cardinality returns 0.1

**Implement:** Add `estimate_filter_selectivity()` method. For BinaryOp::Eq, extract column reference and check `graph.is_high_cardinality()`. High-card = 0.001, low-card = 0.1. Reference design doc "estimate_filter_selectivity()".

**Verify:** Selectivity varies correctly based on column cardinality metadata

**Commit:** `feat(planner): estimate equality filter selectivity from cardinality`

---

### Task 4: Filter Selectivity Estimation - Range and Logical Predicates

**Purpose:** Handle >, <, AND, OR predicates with reasonable estimates

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/filter_selectivity_test.rs`

**Test:** Range predicates return 0.33, AND combines multiplicatively, OR combines additively

**Implement:** Extend `estimate_filter_selectivity()` to handle: Gt/Lt = 0.33, And = left * right, Or = left + right - (left * right). Reference design doc filter selectivity section.

**Verify:** Complex predicates combine correctly (e.g., A AND B more selective than A OR B)

**Commit:** `feat(planner): add range and logical predicate selectivity`

---

### Task 5: Filter Cost Estimation with Selectivity

**Purpose:** Update Filter node cost to use selectivity estimates

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/filter_cost_test.rs`

**Test:** Filter reduces rows_out proportional to selectivity, adds CPU cost for evaluation

**Implement:** In estimate_cost() for Filter: calculate selectivity, set rows_out = input.rows_out * selectivity, add CPU cost = input.rows_out (evaluate each row). See design doc "estimate_cost() - Filter".

**Verify:** Highly selective filters reduce rows_out significantly, CPU cost reflects input size

**Commit:** `feat(planner): calculate filter costs using selectivity estimates`

---

### Task 6: Join Cardinality Estimation from Graph Metadata

**Purpose:** Estimate join output size using relationship cardinality (1:1, 1:N, N:M)

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/join_cardinality_test.rs`

**Test:** 1:1 join returns max(left, right) rows, 1:N returns right rows, N:M returns left * right / 100

**Implement:** Add `estimate_join_cardinality()` method. Extract table names from plans, call `graph.find_path()`, read cardinality from path steps. Apply formula based on type. Reference design doc "estimate_join_cardinality()".

**Verify:** Different cardinality types produce correct row estimates

**Commit:** `feat(planner): estimate join cardinality from graph relationships`

---

### Task 7: Join Cost Estimation with Hash and Nested Loop

**Purpose:** Calculate realistic join costs for different strategies

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/join_cost_test.rs`

**Test:** HashJoin has memory cost for build side, NestedLoopJoin has CPU cost = left * right

**Implement:** In estimate_cost() for HashJoin: CPU = left + right + probe, IO = left + right, memory = smaller side, rows_out = estimated cardinality. For NestedLoopJoin: CPU = left * right, no memory. See design doc "estimate_cost() - HashJoin".

**Verify:** HashJoin and NestedLoopJoin have different cost profiles

**Commit:** `feat(planner): add realistic join strategy costs`

---

### Task 8: Group Cardinality Estimation

**Purpose:** Estimate output rows from GROUP BY operations

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/group_cost_test.rs`

**Test:** GROUP BY on high-cardinality column returns high rows_out, low-cardinality returns low rows_out

**Implement:** In estimate_cost() for Aggregate: check cardinality of group_by columns using graph metadata. High-card = 50% of input rows, low-card = 10% of input rows, multiple columns = product of individual selectivities. Reference design doc cost estimator section.

**Verify:** GROUP BY row estimates vary correctly with column cardinality

**Commit:** `feat(planner): estimate GROUP BY cardinality from column metadata`

---

### Task 9: Join Order Optimizer - Foundation

**Purpose:** Create optimizer that reorders joins for better performance

**Files:**
- Create: `src/planner/physical/join_optimizer.rs`
- Modify: `src/planner/physical/mod.rs`
- Test: `tests/planner/join_optimizer_test.rs`

**Test:** JoinOptimizer extracts tables from logical plan correctly

**Implement:** Create JoinOptimizer struct with graph and cost_estimator references. Add `extract_tables()` method that walks LogicalPlan tree and collects all table names. Reference design doc "Component 2: Join Order Optimizer".

**Verify:** extract_tables() returns all tables from complex join trees

**Commit:** `feat(planner): add join order optimizer foundation`

---

### Task 10: Join Order Enumeration for Small Joins

**Purpose:** Try all join orders for 2-3 table queries to find optimal

**Files:**
- Modify: `src/planner/physical/join_optimizer.rs`
- Test: `tests/planner/join_optimizer_test.rs`

**Test:** 2-table query enumerates both orders (A⋈B and B⋈A), picks lower cost. 3-table enumerates 12 options.

**Implement:** Add `enumerate_all_join_orders()` that generates all permutations of tables, builds physical plan for each, estimates cost, returns sorted by cost. For n tables, generates n! plans. Reference design doc "optimize_join_order() - Small".

**Verify:** All permutations generated, lowest cost returned first

**Commit:** `feat(planner): enumerate all join orders for small queries`

---

### Task 11: Greedy Join Order for Large Joins

**Purpose:** Use heuristic for 4+ tables (factorial explosion avoidance)

**Files:**
- Modify: `src/planner/physical/join_optimizer.rs`
- Test: `tests/planner/join_optimizer_test.rs`

**Test:** 5-table query uses greedy algorithm, returns single plan in reasonable time

**Implement:** Add `greedy_join_order()` that: 1) finds smallest two-table join, 2) iteratively adds next-best table to current plan, 3) returns final plan. O(n²) complexity. Reference design doc "greedy_join_order()".

**Verify:** Greedy produces reasonable plan quickly for large joins (< 100ms for 10 tables)

**Commit:** `feat(planner): add greedy join order optimization for large queries`

---

### Task 12: Find Smallest Join Pair Helper

**Purpose:** Support greedy optimizer by finding best starting pair

**Files:**
- Modify: `src/planner/physical/join_optimizer.rs`
- Test: `tests/planner/join_optimizer_test.rs`

**Test:** Given set of tables, returns pair with lowest join cost

**Implement:** Add `find_smallest_join_pair()` that iterates all table pairs, checks if valid join path exists in graph, builds join plan, estimates cost, returns lowest. Reference design doc greedy algorithm helpers.

**Verify:** Returns valid pair with lowest estimated cost

**Commit:** `feat(planner): find smallest join pair for greedy optimization`

---

### Task 13: Find Best Next Join Helper

**Purpose:** Support greedy optimizer by finding best table to add next

**Files:**
- Modify: `src/planner/physical/join_optimizer.rs`
- Test: `tests/planner/join_optimizer_test.rs`

**Test:** Given current plan and remaining tables, returns table with lowest incremental join cost

**Implement:** Add `find_best_next_join()` that tries joining each remaining table to current plan, estimates cost of each, returns table with lowest cost. Reference design doc greedy algorithm.

**Verify:** Returns table that minimizes total cost when added

**Commit:** `feat(planner): find best next join for greedy optimization`

---

### Task 14: Integrate Join Optimizer into Physical Converter

**Purpose:** Use optimizer when converting multi-table logical plans

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/physical_converter_test.rs`

**Test:** LogicalPlan with multiple joins generates multiple physical plan candidates with different join orders

**Implement:** In convert() for Join nodes, detect multi-way join, call join_optimizer.optimize_join_order() to get candidates, return all. For 2-3 tables return all permutations, for 4+ return greedy result. Reference design doc integration.

**Verify:** Multi-table reports generate optimized join orders

**Commit:** `feat(planner): integrate join optimizer into physical conversion`

---

### Task 15: Cost Comparison and Best Plan Selection

**Purpose:** Select best physical plan based on total cost

**Files:**
- Modify: `src/planner/mod.rs`
- Test: `tests/planner/plan_selection_test.rs`

**Test:** Given multiple physical plan candidates, select_best_plan() returns lowest cost

**Implement:** Update main planner to collect all physical plan candidates, estimate cost for each using enhanced cost estimator, sort by total cost, return first (lowest). Reference design doc three-phase architecture.

**Verify:** Correct plan selected when multiple candidates exist

**Commit:** `feat(planner): select best plan using enhanced cost estimates`

---

### Task 16: Cost Estimation Logging and Debugging

**Purpose:** Log cost estimates for debugging and validation

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Modify: `src/planner/mod.rs`

**Test:** Manual verification of log output during tests

**Implement:** Add debug logging in estimate_cost() showing rows_out, cpu_cost, io_cost, memory_cost for each node. Log final plan selection with costs of all candidates. Use tracing or log crate.

**Verify:** cargo test shows detailed cost breakdown in logs

**Commit:** `feat(planner): add cost estimation debug logging`

---

### Task 17: Performance Test - Small Query Optimization

**Purpose:** Verify optimization improves query performance

**Files:**
- Create: `benches/planner_optimization_bench.rs`
- Modify: `Cargo.toml` (add bench section)

**Test:** Benchmark 3-table join with optimal vs naive order, verify optimal is faster

**Implement:** Create criterion benchmark that: 1) builds UnifiedGraph with realistic sizes, 2) creates report with 3 tables, 3) times naive order vs optimized, 4) asserts optimized cost is lower. Reference design doc performance tests.

**Verify:** Optimized plan has 2-10x lower cost than naive

**Commit:** `test(planner): add small query optimization benchmark`

---

### Task 18: Performance Test - Large Query Optimization

**Purpose:** Verify greedy optimizer works for complex queries

**Files:**
- Modify: `benches/planner_optimization_bench.rs`

**Test:** Benchmark 7-table join, verify planning completes in < 200ms

**Implement:** Extend benchmark with 7-table query, measure planning time, assert < 200ms. Verify greedy algorithm produces reasonable join order (smallest tables first). Reference design doc performance considerations.

**Verify:** Planning time scales reasonably, doesn't explode exponentially

**Commit:** `test(planner): add large query planning performance test`

---

### Task 19: Cost Estimate Accuracy Validation

**Purpose:** Verify cost estimates correlate with actual execution (if possible)

**Files:**
- Create: `tests/planner/cost_accuracy_test.rs`

**Test:** For simple queries, verify estimated rows_out is within 2x of actual

**Implement:** If execution layer exists, run queries and compare actual row counts to estimates. Otherwise, use graph metadata as ground truth and verify estimates are reasonable. Reference design doc success metrics.

**Verify:** Estimates within 2x of actuals for test cases

**Commit:** `test(planner): validate cost estimate accuracy`

---

### Task 20: Wave 2 Integration Test - Optimized Complex Report

**Purpose:** End-to-end test of all optimization features

**Files:**
- Test: `tests/planner/wave2_integration_test.rs`

**Test:** Create 5-table report with filters and joins. Verify: 1) multiple plan candidates generated, 2) costs estimated for each, 3) best plan selected, 4) optimized plan better than naive order.

**Implement:** Comprehensive test with realistic UnifiedGraph (varying table sizes, cardinalities). Compare optimized vs naive join order costs. Verify filter selectivity affects row estimates. Reference design doc appendix example.

**Verify:** All optimization components work together, best plan selected correctly

**Commit:** `test(planner): add Wave 2 comprehensive optimization test`

---

## Wave 2 Completion Checklist

- ✅ Multi-objective cost model (CPU, IO, memory)
- ✅ Actual row counts from UnifiedGraph
- ✅ Filter selectivity estimation (equality, range, logical)
- ✅ Join cardinality estimation (1:1, 1:N, N:M)
- ✅ GROUP BY cardinality estimation
- ✅ Join order enumeration (optimal for small)
- ✅ Greedy join order (fast for large)
- ✅ Cost comparison and best plan selection
- ✅ Performance benchmarks pass
- ✅ Cost estimates within 2x of actuals

**Next:** Proceed to Wave 3 (Time Intelligence) after Wave 2 verification.
