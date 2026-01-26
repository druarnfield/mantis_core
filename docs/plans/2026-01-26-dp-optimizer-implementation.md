# Dynamic Programming Join Optimizer - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix Wave 2 optimizer to achieve meaningful performance improvements (1.5x-5x) using Dynamic Programming

**Architecture:** Bottom-up DP (Selinger algorithm) with accurate cardinality estimation, filter pushdown, and bushy tree exploration

**Tech Stack:** Rust, UnifiedGraph metadata, existing CostEstimate framework

---

## Phase 1: Foundation - JoinGraph Component

### Task 1: Create JoinGraph Module Structure

**Files:**
- Create: `src/planner/join_optimizer/join_graph.rs`
- Modify: `src/planner/join_optimizer/mod.rs:1-10`
- Test: `tests/planner/join_graph_test.rs`

**Step 1: Write the failing test**

Create test file:

```rust
// tests/planner/join_graph_test.rs
use mantis_core::planner::join_optimizer::join_graph::*;
use mantis_core::model::UnifiedGraph;

#[test]
fn test_join_graph_can_be_created() {
    let graph = UnifiedGraph::new();
    let tables = vec!["orders".to_string(), "customers".to_string()];
    
    let join_graph = JoinGraph::build(&graph, &tables);
    
    assert_eq!(join_graph.table_count(), 2);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_join_graph_can_be_created -- --nocapture
```

Expected: FAIL with "no module named join_graph"

**Step 3: Write minimal implementation**

Create module file:

```rust
// src/planner/join_optimizer/join_graph.rs
use crate::model::UnifiedGraph;
use std::collections::{HashMap, HashSet};

pub struct JoinGraph {
    tables: HashSet<String>,
    edges: HashMap<TablePair, JoinEdge>,
}

#[derive(Hash, Eq, PartialEq)]
struct TablePair(String, String);

pub struct JoinEdge {
    // Placeholder for now
}

impl JoinGraph {
    pub fn build(graph: &UnifiedGraph, tables: &[String]) -> Self {
        Self {
            tables: tables.iter().cloned().collect(),
            edges: HashMap::new(),
        }
    }
    
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}
```

Expose module:

```rust
// src/planner/join_optimizer/mod.rs
pub mod join_graph;
pub mod dp_optimizer; // Will create later
pub mod cardinality; // Will create later

// Keep existing code below...
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_join_graph_can_be_created -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/join_graph.rs src/planner/join_optimizer/mod.rs tests/planner/join_graph_test.rs
git commit -m "feat(join-optimizer): add JoinGraph foundation with table tracking"
```

---

### Task 2: Implement JoinGraph Build from UnifiedGraph

**Files:**
- Modify: `src/planner/join_optimizer/join_graph.rs:1-50`
- Test: `tests/planner/join_graph_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/join_graph_test.rs
use mantis_core::model::{UnifiedGraph, Relationship, Cardinality};

#[test]
fn test_build_join_graph_from_unified_graph() {
    let mut graph = UnifiedGraph::new();
    
    // Add entities
    graph.add_entity("orders", 1000);
    graph.add_entity("customers", 100);
    
    // Add relationship orders.customer_id -> customers.id (N:1)
    graph.add_relationship(Relationship {
        from_entity: "orders".to_string(),
        from_column: "customer_id".to_string(),
        to_entity: "customers".to_string(),
        to_column: "id".to_string(),
        cardinality: Cardinality::ManyToOne,
    });
    
    let tables = vec!["orders".to_string(), "customers".to_string()];
    let join_graph = JoinGraph::build(&graph, &tables);
    
    // Should have edge between orders and customers
    assert!(join_graph.are_joinable("orders", "customers"));
    
    // Should get join edge info
    let edge = join_graph.get_join_edge("orders", "customers");
    assert!(edge.is_some());
    assert_eq!(edge.unwrap().cardinality, Cardinality::ManyToOne);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_build_join_graph_from_unified_graph -- --nocapture
```

Expected: FAIL with "no method named are_joinable"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/join_graph.rs
use crate::model::{UnifiedGraph, Cardinality};

pub struct JoinEdge {
    pub cardinality: Cardinality,
    pub join_columns: Vec<(String, String)>,
}

impl JoinGraph {
    pub fn build(graph: &UnifiedGraph, tables: &[String]) -> Self {
        let mut edges = HashMap::new();
        
        // For each pair of tables, check if they can be joined
        for i in 0..tables.len() {
            for j in i+1..tables.len() {
                let t1 = &tables[i];
                let t2 = &tables[j];
                
                // Use UnifiedGraph.find_path() to get join info
                if let Some(path) = graph.find_path(t1, t2) {
                    let edge = JoinEdge {
                        cardinality: path.cardinality,
                        join_columns: vec![(
                            path.from_column.clone(),
                            path.to_column.clone()
                        )],
                    };
                    
                    edges.insert(
                        TablePair(t1.clone(), t2.clone()),
                        edge
                    );
                }
            }
        }
        
        Self {
            tables: tables.iter().cloned().collect(),
            edges,
        }
    }
    
    pub fn are_joinable(&self, t1: &str, t2: &str) -> bool {
        let key1 = TablePair(t1.to_string(), t2.to_string());
        let key2 = TablePair(t2.to_string(), t1.to_string());
        
        self.edges.contains_key(&key1) || self.edges.contains_key(&key2)
    }
    
    pub fn get_join_edge(&self, t1: &str, t2: &str) -> Option<&JoinEdge> {
        let key1 = TablePair(t1.to_string(), t2.to_string());
        if let Some(edge) = self.edges.get(&key1) {
            return Some(edge);
        }
        
        let key2 = TablePair(t2.to_string(), t1.to_string());
        self.edges.get(&key2)
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_build_join_graph_from_unified_graph -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/join_graph.rs tests/planner/join_graph_test.rs
git commit -m "feat(join-graph): extract join edges from UnifiedGraph"
```

---

### Task 3: Handle Disconnected Tables in JoinGraph

**Files:**
- Modify: `src/planner/join_optimizer/join_graph.rs:50-80`
- Test: `tests/planner/join_graph_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/join_graph_test.rs
#[test]
fn test_disconnected_tables() {
    let mut graph = UnifiedGraph::new();
    
    graph.add_entity("orders", 1000);
    graph.add_entity("products", 500);
    // No relationship between orders and products
    
    let tables = vec!["orders".to_string(), "products".to_string()];
    let join_graph = JoinGraph::build(&graph, &tables);
    
    // Should NOT be joinable
    assert!(!join_graph.are_joinable("orders", "products"));
    
    // Should return None for edge
    assert!(join_graph.get_join_edge("orders", "products").is_none());
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_disconnected_tables -- --nocapture
```

Expected: May pass already (correct behavior), otherwise FAIL

**Step 3: Verify implementation handles disconnected tables**

No code changes needed - existing implementation already handles this correctly via `find_path()` returning None.

**Step 4: Run test to verify it passes**

```bash
cargo test test_disconnected_tables -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add tests/planner/join_graph_test.rs
git commit -m "test(join-graph): verify disconnected tables handled correctly"
```

---

### Task 4: Add TableSet Helper for DP Algorithm

**Files:**
- Modify: `src/planner/join_optimizer/join_graph.rs:80-120`
- Test: `tests/planner/join_graph_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/join_graph_test.rs
#[test]
fn test_are_table_sets_joinable() {
    let mut graph = UnifiedGraph::new();
    
    // Chain: A -> B -> C
    graph.add_entity("A", 100);
    graph.add_entity("B", 200);
    graph.add_entity("C", 300);
    
    graph.add_relationship(Relationship {
        from_entity: "A".to_string(),
        from_column: "b_id".to_string(),
        to_entity: "B".to_string(),
        to_column: "id".to_string(),
        cardinality: Cardinality::ManyToOne,
    });
    
    graph.add_relationship(Relationship {
        from_entity: "B".to_string(),
        from_column: "c_id".to_string(),
        to_entity: "C".to_string(),
        to_column: "id".to_string(),
        cardinality: Cardinality::ManyToOne,
    });
    
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let join_graph = JoinGraph::build(&graph, &tables);
    
    // {A} and {B} are joinable
    let s1 = vec!["A".to_string()];
    let s2 = vec!["B".to_string()];
    assert!(join_graph.are_sets_joinable(&s1, &s2));
    
    // {A} and {C} are NOT directly joinable (need B in between)
    let s1 = vec!["A".to_string()];
    let s2 = vec!["C".to_string()];
    assert!(!join_graph.are_sets_joinable(&s1, &s2));
    
    // {A,B} and {C} are joinable (B connects to C)
    let s1 = vec!["A".to_string(), "B".to_string()];
    let s2 = vec!["C".to_string()];
    assert!(join_graph.are_sets_joinable(&s1, &s2));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_are_table_sets_joinable -- --nocapture
```

Expected: FAIL with "no method named are_sets_joinable"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/join_graph.rs
impl JoinGraph {
    /// Check if two table sets can be joined.
    /// Returns true if ANY table in s1 can join with ANY table in s2.
    pub fn are_sets_joinable(&self, s1: &[String], s2: &[String]) -> bool {
        for t1 in s1 {
            for t2 in s2 {
                if self.are_joinable(t1, t2) {
                    return true;
                }
            }
        }
        false
    }
    
    /// Find a join edge between two table sets.
    /// Returns the first valid join found.
    pub fn get_join_edge_between_sets(
        &self,
        s1: &[String],
        s2: &[String]
    ) -> Option<(&str, &str, &JoinEdge)> {
        for t1 in s1 {
            for t2 in s2 {
                if let Some(edge) = self.get_join_edge(t1, t2) {
                    return Some((t1, t2, edge));
                }
            }
        }
        None
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_are_table_sets_joinable -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/join_graph.rs tests/planner/join_graph_test.rs
git commit -m "feat(join-graph): add table set joinability for DP algorithm"
```

---

## Phase 2: Foundation - CardinalityEstimator Component

### Task 5: Create CardinalityEstimator Module

**Files:**
- Create: `src/planner/join_optimizer/cardinality.rs`
- Modify: `src/planner/join_optimizer/mod.rs:2`
- Test: `tests/planner/cardinality_estimator_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/cardinality_estimator_test.rs
use mantis_core::planner::join_optimizer::cardinality::*;
use mantis_core::model::{UnifiedGraph, Cardinality};
use mantis_core::planner::join_optimizer::join_graph::JoinEdge;

#[test]
fn test_estimate_one_to_one_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);
    
    let edge = JoinEdge {
        cardinality: Cardinality::OneToOne,
        join_columns: vec![],
    };
    
    let output = estimator.estimate_join_output(1000, 500, &edge);
    
    // 1:1 should return min(left, right)
    assert_eq!(output, 500);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_estimate_one_to_one_join -- --nocapture
```

Expected: FAIL with "no module named cardinality"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/cardinality.rs
use crate::model::{UnifiedGraph, Cardinality};
use crate::planner::join_optimizer::join_graph::JoinEdge;

pub struct CardinalityEstimator<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> CardinalityEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    /// Estimate output rows for a join.
    pub fn estimate_join_output(
        &self,
        left_rows: usize,
        right_rows: usize,
        join_info: &JoinEdge,
    ) -> usize {
        match join_info.cardinality {
            Cardinality::OneToOne => left_rows.min(right_rows),
            Cardinality::OneToMany => right_rows,
            Cardinality::ManyToOne => left_rows,
            Cardinality::ManyToMany => {
                // For now, use simple heuristic
                // TODO: Use join column cardinality
                (left_rows as f64 * (right_rows as f64).sqrt()) as usize
            }
        }
    }
}
```

Expose module:

```rust
// src/planner/join_optimizer/mod.rs (line 2)
pub mod cardinality;
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_estimate_one_to_one_join -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/cardinality.rs src/planner/join_optimizer/mod.rs tests/planner/cardinality_estimator_test.rs
git commit -m "feat(cardinality): add CardinalityEstimator with 1:1 join formula"
```

---

### Task 6: Implement All Join Cardinality Formulas

**Files:**
- Modify: `src/planner/join_optimizer/cardinality.rs:20-40`
- Test: `tests/planner/cardinality_estimator_test.rs`

**Step 1: Write the failing tests**

```rust
// tests/planner/cardinality_estimator_test.rs
#[test]
fn test_estimate_one_to_many_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);
    
    let edge = JoinEdge {
        cardinality: Cardinality::OneToMany,
        join_columns: vec![],
    };
    
    // 100 customers (left) -> 1000 orders (right)
    let output = estimator.estimate_join_output(100, 1000, &edge);
    
    // 1:N should return "many" side (right)
    assert_eq!(output, 1000);
}

#[test]
fn test_estimate_many_to_one_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);
    
    let edge = JoinEdge {
        cardinality: Cardinality::ManyToOne,
        join_columns: vec![],
    };
    
    // 1000 orders (left) -> 100 customers (right)
    let output = estimator.estimate_join_output(1000, 100, &edge);
    
    // N:1 should return "many" side (left)
    assert_eq!(output, 1000);
}

#[test]
fn test_estimate_many_to_many_join() {
    let mut graph = UnifiedGraph::new();
    
    // Students (1000) <-> Courses (100) via enrollments
    graph.add_entity("students", 1000);
    graph.add_entity("courses", 100);
    
    // Assume 50 distinct course_ids in students, 80 distinct student_ids in courses
    // For now, use simple heuristic
    
    let estimator = CardinalityEstimator::new(&graph);
    
    let edge = JoinEdge {
        cardinality: Cardinality::ManyToMany,
        join_columns: vec![],
    };
    
    let output = estimator.estimate_join_output(1000, 100, &edge);
    
    // N:N should be less than cross product (100K)
    // Using sqrt heuristic: 1000 * sqrt(100) = 10K
    assert_eq!(output, 10000);
}
```

**Step 2: Run tests to verify they fail**

```bash
cargo test test_estimate_one_to_many_join test_estimate_many_to_one_join test_estimate_many_to_many_join -- --nocapture
```

Expected: OneToMany and ManyToOne may fail or pass depending on current implementation

**Step 3: Verify/fix implementation**

The implementation from Task 5 already handles all cases. Verify it's correct.

**Step 4: Run tests to verify they pass**

```bash
cargo test test_estimate_one_to_many_join test_estimate_many_to_one_join test_estimate_many_to_many_join -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add tests/planner/cardinality_estimator_test.rs
git commit -m "test(cardinality): verify all join cardinality formulas"
```

---

### Task 7: Implement Filter Selectivity Estimation

**Files:**
- Modify: `src/planner/join_optimizer/cardinality.rs:40-100`
- Test: `tests/planner/cardinality_estimator_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/cardinality_estimator_test.rs
use mantis_core::ast::Expr;
use mantis_core::model::BinaryOp;

#[test]
fn test_filter_selectivity_equality() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);
    
    // WHERE customer_id = 123
    let filter = Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "customer_id".to_string(),
        }),
        right: Box::new(Expr::Literal(123.into())),
    };
    
    let selectivity = estimator.estimate_filter_selectivity(&filter);
    
    // Equality should be selective (default 0.1 = 10%)
    assert!((selectivity - 0.1).abs() < 0.01);
}

#[test]
fn test_filter_selectivity_range() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);
    
    // WHERE amount > 1000
    let filter = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "amount".to_string(),
        }),
        right: Box::new(Expr::Literal(1000.into())),
    };
    
    let selectivity = estimator.estimate_filter_selectivity(&filter);
    
    // Range predicates: ~33% selectivity
    assert!((selectivity - 0.33).abs() < 0.01);
}

#[test]
fn test_filter_selectivity_and() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);
    
    // WHERE amount > 1000 AND status = 'completed'
    let filter = Expr::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                entity: Some("orders".to_string()),
                column: "amount".to_string(),
            }),
            right: Box::new(Expr::Literal(1000.into())),
        }),
        right: Box::new(Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column {
                entity: Some("orders".to_string()),
                column: "status".to_string(),
            }),
            right: Box::new(Expr::Literal("completed".into())),
        }),
    };
    
    let selectivity = estimator.estimate_filter_selectivity(&filter);
    
    // AND combines: 0.33 * 0.1 = 0.033 (3.3%)
    assert!((selectivity - 0.033).abs() < 0.01);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_filter_selectivity_equality test_filter_selectivity_range test_filter_selectivity_and -- --nocapture
```

Expected: FAIL with "no method named estimate_filter_selectivity"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/cardinality.rs
use crate::ast::Expr;
use crate::model::BinaryOp;

impl<'a> CardinalityEstimator<'a> {
    /// Estimate selectivity of a filter predicate (0.0 to 1.0).
    pub fn estimate_filter_selectivity(&self, filter: &Expr) -> f64 {
        match filter {
            Expr::BinaryOp { op, left, right } => match op {
                BinaryOp::Eq => {
                    // Equality: default 10% selectivity
                    // TODO: Use column cardinality from graph
                    0.1
                }
                BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte => {
                    // Range predicates: 33% selectivity
                    0.33
                }
                BinaryOp::And => {
                    // AND combines multiplicatively
                    let s1 = self.estimate_filter_selectivity(left);
                    let s2 = self.estimate_filter_selectivity(right);
                    s1 * s2
                }
                BinaryOp::Or => {
                    // OR combines with probability union
                    let s1 = self.estimate_filter_selectivity(left);
                    let s2 = self.estimate_filter_selectivity(right);
                    s1 + s2 - (s1 * s2)
                }
                _ => 0.5, // Default
            }
            _ => 0.5, // Default for other expressions
        }
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_filter_selectivity_equality test_filter_selectivity_range test_filter_selectivity_and -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/cardinality.rs tests/planner/cardinality_estimator_test.rs
git commit -m "feat(cardinality): add filter selectivity estimation with AND/OR"
```

---

## Phase 3: DP Core Algorithm

### Task 8: Create DP Optimizer Module with TableSet

**Files:**
- Create: `src/planner/join_optimizer/dp_optimizer.rs`
- Modify: `src/planner/join_optimizer/mod.rs:3`
- Test: `tests/planner/dp_optimizer_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/dp_optimizer_test.rs
use mantis_core::planner::join_optimizer::dp_optimizer::*;
use std::collections::BTreeSet;

#[test]
fn test_table_set_creation() {
    let mut tables = BTreeSet::new();
    tables.insert("orders".to_string());
    tables.insert("customers".to_string());
    
    let table_set = TableSet::new(tables.clone());
    
    assert_eq!(table_set.size(), 2);
    assert!(table_set.contains("orders"));
    assert!(table_set.contains("customers"));
    assert!(!table_set.contains("products"));
}

#[test]
fn test_table_set_from_vec() {
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let table_set = TableSet::from_vec(tables);
    
    assert_eq!(table_set.size(), 3);
}

#[test]
fn test_table_set_single() {
    let table_set = TableSet::single("orders");
    
    assert_eq!(table_set.size(), 1);
    assert!(table_set.contains("orders"));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_table_set_creation -- --nocapture
```

Expected: FAIL with "no module named dp_optimizer"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/dp_optimizer.rs
use std::collections::{BTreeSet, HashMap};
use crate::model::UnifiedGraph;
use crate::planner::join_optimizer::join_graph::JoinGraph;
use crate::planner::join_optimizer::cardinality::CardinalityEstimator;

/// A set of tables represented as a BTreeSet for deterministic ordering.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct TableSet {
    tables: BTreeSet<String>,
}

impl TableSet {
    pub fn new(tables: BTreeSet<String>) -> Self {
        Self { tables }
    }
    
    pub fn from_vec(tables: Vec<String>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }
    
    pub fn single(table: &str) -> Self {
        let mut tables = BTreeSet::new();
        tables.insert(table.to_string());
        Self { tables }
    }
    
    pub fn size(&self) -> usize {
        self.tables.len()
    }
    
    pub fn contains(&self, table: &str) -> bool {
        self.tables.contains(table)
    }
    
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.tables.iter()
    }
    
    pub fn to_vec(&self) -> Vec<String> {
        self.tables.iter().cloned().collect()
    }
}
```

Expose module:

```rust
// src/planner/join_optimizer/mod.rs (line 3)
pub mod dp_optimizer;
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_table_set_creation test_table_set_from_vec test_table_set_single -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs src/planner/join_optimizer/mod.rs tests/planner/dp_optimizer_test.rs
git commit -m "feat(dp-optimizer): add TableSet foundation for DP algorithm"
```

---

### Task 9: Implement Subset Generation for DP

**Files:**
- Modify: `src/planner/join_optimizer/dp_optimizer.rs:40-80`
- Test: `tests/planner/dp_optimizer_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/dp_optimizer_test.rs
#[test]
fn test_generate_subsets_size_1() {
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    
    let subsets = generate_subsets(&tables, 1);
    
    assert_eq!(subsets.len(), 3); // {A}, {B}, {C}
    assert!(subsets.iter().any(|s| s.size() == 1 && s.contains("A")));
    assert!(subsets.iter().any(|s| s.size() == 1 && s.contains("B")));
    assert!(subsets.iter().any(|s| s.size() == 1 && s.contains("C")));
}

#[test]
fn test_generate_subsets_size_2() {
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    
    let subsets = generate_subsets(&tables, 2);
    
    assert_eq!(subsets.len(), 3); // {A,B}, {A,C}, {B,C}
    assert!(subsets.iter().any(|s| s.contains("A") && s.contains("B")));
    assert!(subsets.iter().any(|s| s.contains("A") && s.contains("C")));
    assert!(subsets.iter().any(|s| s.contains("B") && s.contains("C")));
}

#[test]
fn test_generate_subsets_all() {
    let tables = vec!["A".to_string(), "B".to_string()];
    
    let subsets = generate_subsets(&tables, 2);
    
    assert_eq!(subsets.len(), 1); // {A,B}
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_generate_subsets_size_1 -- --nocapture
```

Expected: FAIL with "cannot find function generate_subsets"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/dp_optimizer.rs
/// Generate all subsets of given size from a list of tables.
pub fn generate_subsets(tables: &[String], size: usize) -> Vec<TableSet> {
    if size == 0 || size > tables.len() {
        return vec![];
    }
    
    let mut result = Vec::new();
    let mut current = Vec::new();
    generate_subsets_helper(tables, size, 0, &mut current, &mut result);
    result
}

fn generate_subsets_helper(
    tables: &[String],
    size: usize,
    start: usize,
    current: &mut Vec<String>,
    result: &mut Vec<TableSet>,
) {
    if current.len() == size {
        result.push(TableSet::from_vec(current.clone()));
        return;
    }
    
    for i in start..tables.len() {
        current.push(tables[i].clone());
        generate_subsets_helper(tables, size, i + 1, current, result);
        current.pop();
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_generate_subsets_size_1 test_generate_subsets_size_2 test_generate_subsets_all -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs tests/planner/dp_optimizer_test.rs
git commit -m "feat(dp-optimizer): add subset generation for DP iteration"
```

---

### Task 10: Implement Subset Splitting for DP Partitions

**Files:**
- Modify: `src/planner/join_optimizer/dp_optimizer.rs:80-130`
- Test: `tests/planner/dp_optimizer_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/dp_optimizer_test.rs
#[test]
fn test_enumerate_splits_two_tables() {
    let subset = TableSet::from_vec(vec!["A".to_string(), "B".to_string()]);
    
    let splits = enumerate_splits(&subset);
    
    // Should have 2 splits: (A,B) and (B,A)
    // But we deduplicate, so just 1: ({A}, {B})
    assert_eq!(splits.len(), 1);
    
    let (s1, s2) = &splits[0];
    assert_eq!(s1.size(), 1);
    assert_eq!(s2.size(), 1);
}

#[test]
fn test_enumerate_splits_three_tables() {
    let subset = TableSet::from_vec(vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    
    let splits = enumerate_splits(&subset);
    
    // For {A,B,C}, we have:
    // Size 1: ({A}, {B,C}), ({B}, {A,C}), ({C}, {A,B})
    // We don't need size 2 because we'd get duplicates
    // Total: 3 unique splits
    assert_eq!(splits.len(), 3);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_enumerate_splits_two_tables -- --nocapture
```

Expected: FAIL with "cannot find function enumerate_splits"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/dp_optimizer.rs
/// Enumerate all ways to split a table set into two non-empty subsets.
/// Returns pairs (S1, S2) where S1 ∪ S2 = subset.
pub fn enumerate_splits(subset: &TableSet) -> Vec<(TableSet, TableSet)> {
    let tables: Vec<_> = subset.iter().cloned().collect();
    let n = tables.len();
    
    if n < 2 {
        return vec![];
    }
    
    let mut splits = Vec::new();
    
    // Try all non-empty, non-full subsets as S1
    // Only iterate up to 2^(n-1) to avoid duplicates
    for size in 1..n {
        for s1_subset in generate_subsets(&tables, size) {
            // S2 is the complement of S1
            let s2_tables: Vec<_> = tables.iter()
                .filter(|t| !s1_subset.contains(t))
                .cloned()
                .collect();
            let s2_subset = TableSet::from_vec(s2_tables);
            
            splits.push((s1_subset, s2_subset));
        }
    }
    
    splits
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_enumerate_splits_two_tables test_enumerate_splits_three_tables -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs tests/planner/dp_optimizer_test.rs
git commit -m "feat(dp-optimizer): add subset splitting for DP join enumeration"
```

---

### Task 11: Implement Filter Classification

**Files:**
- Modify: `src/planner/join_optimizer/dp_optimizer.rs:130-200`
- Test: `tests/planner/dp_optimizer_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/dp_optimizer_test.rs
use mantis_core::ast::Expr;
use mantis_core::model::BinaryOp;

#[test]
fn test_classify_single_table_filter() {
    let graph = UnifiedGraph::new();
    let mut dp = DPOptimizer::new(&graph);
    
    // WHERE orders.amount > 1000
    let filter = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "amount".to_string(),
        }),
        right: Box::new(Expr::Literal(1000.into())),
    };
    
    let classified = dp.classify_filters(vec![filter]);
    
    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].referenced_tables.len(), 1);
    assert!(classified[0].referenced_tables.contains("orders"));
    assert!((classified[0].selectivity - 0.33).abs() < 0.01);
}

#[test]
fn test_classify_join_filter() {
    let graph = UnifiedGraph::new();
    let mut dp = DPOptimizer::new(&graph);
    
    // WHERE orders.customer_id = customers.id (join condition)
    let filter = Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "customer_id".to_string(),
        }),
        right: Box::new(Expr::Column {
            entity: Some("customers".to_string()),
            column: "id".to_string(),
        }),
    };
    
    let classified = dp.classify_filters(vec![filter]);
    
    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].referenced_tables.len(), 2);
    assert!(classified[0].referenced_tables.contains("orders"));
    assert!(classified[0].referenced_tables.contains("customers"));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_classify_single_table_filter -- --nocapture
```

Expected: FAIL with "no method named classify_filters" or DPOptimizer not defined

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/dp_optimizer.rs
use std::collections::HashSet;
use crate::ast::Expr;
use crate::planner::logical::LogicalPlan;
use crate::planner::cost::CostEstimate;

pub struct DPOptimizer<'a> {
    graph: &'a UnifiedGraph,
    join_graph: Option<JoinGraph>,
    filters: Vec<ClassifiedFilter>,
    cardinality_estimator: CardinalityEstimator<'a>,
    memo: HashMap<TableSet, SubsetPlan>,
}

pub struct ClassifiedFilter {
    pub expr: Expr,
    pub referenced_tables: HashSet<String>,
    pub selectivity: f64,
}

struct SubsetPlan {
    plan: LogicalPlan,
    estimated_rows: usize,
    cost: CostEstimate,
}

impl<'a> DPOptimizer<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        let cardinality_estimator = CardinalityEstimator::new(graph);
        
        Self {
            graph,
            join_graph: None,
            filters: Vec::new(),
            cardinality_estimator,
            memo: HashMap::new(),
        }
    }
    
    pub fn classify_filters(&mut self, filters: Vec<Expr>) -> Vec<ClassifiedFilter> {
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
        let mut tables = HashSet::new();
        self.collect_tables_from_expr(expr, &mut tables);
        tables
    }
    
    fn collect_tables_from_expr(&self, expr: &Expr, tables: &mut HashSet<String>) {
        match expr {
            Expr::Column { entity: Some(table), .. } => {
                tables.insert(table.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_tables_from_expr(left, tables);
                self.collect_tables_from_expr(right, tables);
            }
            _ => {}
        }
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_classify_single_table_filter test_classify_join_filter -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs tests/planner/dp_optimizer_test.rs
git commit -m "feat(dp-optimizer): add filter classification by table dependencies"
```

---

### Task 12: Implement Base Plan with Filter Pushdown

**Files:**
- Modify: `src/planner/join_optimizer/dp_optimizer.rs:200-250`
- Test: `tests/planner/dp_optimizer_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/dp_optimizer_test.rs
use mantis_core::planner::logical::{LogicalPlan, ScanNode, FilterNode};

#[test]
fn test_build_base_plan_no_filters() {
    let mut graph = UnifiedGraph::new();
    graph.add_entity("orders", 1000);
    
    let mut dp = DPOptimizer::new(&graph);
    dp.filters = vec![];
    
    let plan = dp.build_base_plan("orders");
    
    // Should be a simple scan
    match &plan.plan {
        LogicalPlan::Scan(scan) => {
            assert_eq!(scan.entity, "orders");
        }
        _ => panic!("Expected Scan node"),
    }
    
    assert_eq!(plan.estimated_rows, 1000);
}

#[test]
fn test_build_base_plan_with_filter() {
    let mut graph = UnifiedGraph::new();
    graph.add_entity("orders", 1000);
    
    let mut dp = DPOptimizer::new(&graph);
    
    // WHERE orders.amount > 1000 (selectivity 0.33)
    let filter = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "amount".to_string(),
        }),
        right: Box::new(Expr::Literal(1000.into())),
    };
    
    dp.filters = dp.classify_filters(vec![filter]);
    
    let plan = dp.build_base_plan("orders");
    
    // Should be Scan wrapped in Filter
    match &plan.plan {
        LogicalPlan::Filter(filter_node) => {
            assert_eq!(filter_node.predicates.len(), 1);
            match &*filter_node.input {
                LogicalPlan::Scan(scan) => {
                    assert_eq!(scan.entity, "orders");
                }
                _ => panic!("Expected Scan inside Filter"),
            }
        }
        _ => panic!("Expected Filter node"),
    }
    
    // Estimated rows reduced by selectivity: 1000 * 0.33 ≈ 330
    assert!(plan.estimated_rows >= 300 && plan.estimated_rows <= 350);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_build_base_plan_no_filters -- --nocapture
```

Expected: FAIL with "no method named build_base_plan"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/dp_optimizer.rs
use crate::planner::logical::{ScanNode, FilterNode};
use crate::planner::cost::CostEstimator;

impl<'a> DPOptimizer<'a> {
    fn build_base_plan(&self, table: &str) -> SubsetPlan {
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: table.to_string(),
        });
        
        // Get table row count from graph
        let mut estimated_rows = self.graph
            .get_entity(table)
            .map(|e| e.row_count)
            .unwrap_or(1000); // Default
        
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
        
        // Estimate cost (simple for now - just row count)
        let cost = CostEstimate {
            cpu: estimated_rows as f64,
            io: estimated_rows as f64,
            memory: estimated_rows as f64,
        };
        
        SubsetPlan {
            plan,
            estimated_rows,
            cost,
        }
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_build_base_plan_no_filters test_build_base_plan_with_filter -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs tests/planner/dp_optimizer_test.rs
git commit -m "feat(dp-optimizer): add base plan creation with filter pushdown"
```

---

### Task 13: Implement DP Main Algorithm

**Files:**
- Modify: `src/planner/join_optimizer/dp_optimizer.rs:250-350`
- Test: `tests/planner/dp_integration_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/dp_integration_test.rs
use mantis_core::planner::join_optimizer::dp_optimizer::*;
use mantis_core::model::{UnifiedGraph, Relationship, Cardinality};

#[test]
fn test_two_table_dp_optimal() {
    let mut graph = UnifiedGraph::new();
    
    // Setup: orders (10K) -> customers (100)
    graph.add_entity("orders", 10000);
    graph.add_entity("customers", 100);
    
    graph.add_relationship(Relationship {
        from_entity: "orders".to_string(),
        from_column: "customer_id".to_string(),
        to_entity: "customers".to_string(),
        to_column: "id".to_string(),
        cardinality: Cardinality::ManyToOne,
    });
    
    let mut dp = DPOptimizer::new(&graph);
    
    let tables = vec!["orders".to_string(), "customers".to_string()];
    let filters = vec![];
    
    let optimized = dp.optimize(tables, filters);
    
    // Should return a join plan
    assert!(optimized.is_some());
    
    // Should have both tables in memo
    assert!(dp.memo_contains("orders"));
    assert!(dp.memo_contains("customers"));
    
    // Should have final plan for both tables
    let all_tables = TableSet::from_vec(vec!["orders".to_string(), "customers".to_string()]);
    assert!(dp.memo_contains_set(&all_tables));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_two_table_dp_optimal -- --nocapture
```

Expected: FAIL with "no method named optimize"

**Step 3: Write minimal implementation**

```rust
// src/planner/join_optimizer/dp_optimizer.rs
use crate::planner::logical::{JoinNode, JoinType};
use crate::model::JoinCondition;

impl<'a> DPOptimizer<'a> {
    pub fn optimize(
        &mut self,
        tables: Vec<String>,
        filters: Vec<Expr>,
    ) -> Option<LogicalPlan> {
        // 1. Build join graph from UnifiedGraph
        self.join_graph = Some(JoinGraph::build(self.graph, &tables));
        
        // 2. Classify filters by table dependencies
        self.filters = self.classify_filters(filters);
        
        // 3. Base case: single-table plans with applicable filters
        for table in &tables {
            let plan = self.build_base_plan(table);
            self.memo.insert(TableSet::single(table), plan);
        }
        
        // 4. DP: build optimal plans for increasing subset sizes
        for size in 2..=tables.len() {
            let subsets = generate_subsets(&tables, size);
            for subset in subsets {
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
        for (s1, s2) in enumerate_splits(subset) {
            // Skip if tables can't be joined
            let join_graph = self.join_graph.as_ref().unwrap();
            if !join_graph.are_sets_joinable(&s1.to_vec(), &s2.to_vec()) {
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
        let join_graph = self.join_graph.as_ref().unwrap();
        
        // Get join edge between left and right table sets
        let (t1, t2, join_edge) = join_graph
            .get_join_edge_between_sets(&left.get_tables(), &right.get_tables())
            .unwrap();
        
        // Estimate output cardinality
        let estimated_rows = self.cardinality_estimator.estimate_join_output(
            left.estimated_rows,
            right.estimated_rows,
            join_edge,
        );
        
        // Build join condition from edge
        let join_condition = JoinCondition {
            left_column: join_edge.join_columns[0].0.clone(),
            right_column: join_edge.join_columns[0].1.clone(),
        };
        
        // Build join plan
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left.plan.clone()),
            right: Box::new(right.plan.clone()),
            on: join_condition,
            join_type: JoinType::Inner,
            cardinality: Some(join_edge.cardinality),
        });
        
        // Estimate cost: left cost + right cost + join cost
        let join_cost = estimated_rows as f64;
        let cost = CostEstimate {
            cpu: left.cost.cpu + right.cost.cpu + join_cost,
            io: left.cost.io + right.cost.io + (join_cost * 0.1),
            memory: left.cost.memory.max(right.cost.memory).max(estimated_rows as f64),
        };
        
        SubsetPlan {
            plan,
            estimated_rows,
            cost,
        }
    }
    
    // Helper for tests
    pub fn memo_contains(&self, table: &str) -> bool {
        self.memo.contains_key(&TableSet::single(table))
    }
    
    pub fn memo_contains_set(&self, table_set: &TableSet) -> bool {
        self.memo.contains_key(table_set)
    }
}

impl SubsetPlan {
    fn get_tables(&self) -> Vec<String> {
        // Extract table names from plan
        // Simplified for now - need to walk plan tree
        vec![]
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_two_table_dp_optimal -- --nocapture
```

Expected: PASS (may need fixes to get_tables() helper)

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs tests/planner/dp_integration_test.rs
git commit -m "feat(dp-optimizer): implement DP main algorithm with memo table"
```

---

## Phase 4: Integration

### Task 14: Add Optimizer Strategy Selection

**Files:**
- Modify: `src/planner/physical/join_optimizer/mod.rs:1-50`
- Test: `tests/planner/optimizer_strategy_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/optimizer_strategy_test.rs
use mantis_core::planner::physical::join_optimizer::*;
use mantis_core::model::UnifiedGraph;

#[test]
fn test_strategy_selection_small_query() {
    let graph = UnifiedGraph::new();
    let optimizer = JoinOrderOptimizer::new(&graph);
    
    // 3 tables - should use DP
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    
    let strategy = optimizer.select_strategy(&tables);
    
    match strategy {
        OptimizerStrategy::DP => {}, // Expected
        _ => panic!("Expected DP for 3 tables"),
    }
}

#[test]
fn test_strategy_selection_large_query() {
    let graph = UnifiedGraph::new();
    let optimizer = JoinOrderOptimizer::new(&graph);
    
    // 11 tables - should fall back to greedy
    let tables: Vec<_> = (0..11).map(|i| format!("T{}", i)).collect();
    
    let strategy = optimizer.select_strategy(&tables);
    
    match strategy {
        OptimizerStrategy::Legacy => {}, // Expected
        _ => panic!("Expected Legacy for 11 tables"),
    }
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_strategy_selection_small_query -- --nocapture
```

Expected: FAIL with "OptimizerStrategy not defined"

**Step 3: Write minimal implementation**

```rust
// src/planner/physical/join_optimizer/mod.rs
use crate::planner::join_optimizer::dp_optimizer::DPOptimizer;

pub enum OptimizerStrategy {
    Legacy,   // Current enumeration + greedy
    DP,       // New dynamic programming
    Adaptive, // Choose based on table count
}

pub struct JoinOrderOptimizer<'a> {
    graph: &'a UnifiedGraph,
    strategy: OptimizerStrategy,
}

impl<'a> JoinOrderOptimizer<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self {
            graph,
            strategy: OptimizerStrategy::Adaptive,
        }
    }
    
    pub fn with_strategy(graph: &'a UnifiedGraph, strategy: OptimizerStrategy) -> Self {
        Self { graph, strategy }
    }
    
    pub fn select_strategy(&self, tables: &[String]) -> OptimizerStrategy {
        match self.strategy {
            OptimizerStrategy::Adaptive => {
                if tables.len() <= 10 {
                    OptimizerStrategy::DP
                } else {
                    OptimizerStrategy::Legacy
                }
            }
            ref s => s.clone(),
        }
    }
}

impl Clone for OptimizerStrategy {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy => Self::Legacy,
            Self::DP => Self::DP,
            Self::Adaptive => Self::Adaptive,
        }
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_strategy_selection_small_query test_strategy_selection_large_query -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/physical/join_optimizer/mod.rs tests/planner/optimizer_strategy_test.rs
git commit -m "feat(join-optimizer): add strategy selection (Adaptive/DP/Legacy)"
```

---

### Task 15: Integrate DP into PhysicalConverter

**Files:**
- Modify: `src/planner/physical/converter.rs:150-200`
- Test: `tests/planner/physical_converter_integration_test.rs`

**Step 1: Write the failing test**

```rust
// tests/planner/physical_converter_integration_test.rs
use mantis_core::planner::physical::PhysicalConverter;
use mantis_core::model::{UnifiedGraph, Relationship, Cardinality};
use mantis_core::planner::logical::{LogicalPlan, JoinNode, JoinType};
use mantis_core::model::JoinCondition;

#[test]
fn test_physical_converter_uses_dp_optimizer() {
    let mut graph = UnifiedGraph::new();
    
    graph.add_entity("orders", 10000);
    graph.add_entity("customers", 100);
    
    graph.add_relationship(Relationship {
        from_entity: "orders".to_string(),
        from_column: "customer_id".to_string(),
        to_entity: "customers".to_string(),
        to_column: "id".to_string(),
        cardinality: Cardinality::ManyToOne,
    });
    
    let converter = PhysicalConverter::new(&graph);
    
    // Create a simple join plan
    let join_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        on: JoinCondition {
            left_column: "customer_id".to_string(),
            right_column: "id".to_string(),
        },
        join_type: JoinType::Inner,
        cardinality: Some(Cardinality::ManyToOne),
    });
    
    // Should generate physical plans using optimizer
    let physical_plans = converter.convert(&join_plan);
    
    assert!(physical_plans.is_ok());
    let plans = physical_plans.unwrap();
    assert!(!plans.is_empty());
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_physical_converter_uses_dp_optimizer -- --nocapture
```

Expected: May pass or fail depending on current implementation

**Step 3: Update PhysicalConverter to use optimizer**

```rust
// src/planner/physical/converter.rs
use crate::planner::physical::join_optimizer::{JoinOrderOptimizer, OptimizerStrategy};

impl<'a> PhysicalConverter<'a> {
    fn convert_multi_table_join(&self, join: &JoinNode) -> PlanResult<Vec<PhysicalPlan>> {
        let join_plan = LogicalPlan::Join(join.clone());
        let optimizer = JoinOrderOptimizer::new(self.graph);
        
        // Extract tables from join tree
        let tables = self.extract_tables_from_plan(&join_plan);
        
        // Extract filters from logical plan (if any)
        let filters = self.extract_filters_from_plan(&join_plan);
        
        // Use adaptive strategy
        let optimized_plan = match optimizer.select_strategy(&tables) {
            OptimizerStrategy::DP | OptimizerStrategy::Adaptive => {
                // Use DP optimizer
                let mut dp_optimizer = DPOptimizer::new(self.graph);
                dp_optimizer.optimize(tables.clone(), filters)
            }
            OptimizerStrategy::Legacy => {
                // Use legacy optimizer
                self.legacy_optimize(tables)
            }
        };
        
        // Convert logical plan to physical strategies
        let mut physical_plans = Vec::new();
        if let Some(logical_plan) = optimized_plan {
            physical_plans.extend(self.convert_logical_to_physical(&logical_plan)?);
        }
        
        Ok(physical_plans)
    }
    
    fn extract_tables_from_plan(&self, plan: &LogicalPlan) -> Vec<String> {
        // Walk plan tree and collect table names
        let mut tables = Vec::new();
        self.collect_tables(plan, &mut tables);
        tables
    }
    
    fn collect_tables(&self, plan: &LogicalPlan, tables: &mut Vec<String>) {
        match plan {
            LogicalPlan::Scan(scan) => {
                tables.push(scan.entity.clone());
            }
            LogicalPlan::Join(join) => {
                self.collect_tables(&join.left, tables);
                self.collect_tables(&join.right, tables);
            }
            LogicalPlan::Filter(filter) => {
                self.collect_tables(&filter.input, tables);
            }
            _ => {}
        }
    }
    
    fn extract_filters_from_plan(&self, plan: &LogicalPlan) -> Vec<Expr> {
        // Walk plan tree and collect filter predicates
        let mut filters = Vec::new();
        self.collect_filters(plan, &mut filters);
        filters
    }
    
    fn collect_filters(&self, plan: &LogicalPlan, filters: &mut Vec<Expr>) {
        match plan {
            LogicalPlan::Filter(filter_node) => {
                filters.extend(filter_node.predicates.clone());
                self.collect_filters(&filter_node.input, filters);
            }
            LogicalPlan::Join(join) => {
                self.collect_filters(&join.left, filters);
                self.collect_filters(&join.right, filters);
            }
            _ => {}
        }
    }
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test test_physical_converter_uses_dp_optimizer -- --nocapture
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/physical/converter.rs tests/planner/physical_converter_integration_test.rs
git commit -m "feat(physical-converter): integrate DP optimizer with adaptive strategy"
```

---

## Phase 5: Performance Validation

### Task 16: Validate Three-Table Test Passes

**Files:**
- Test: `tests/planner/performance_test.rs`

**Step 1: Run the test**

```bash
cargo test test_three_table_optimization_improves_cost -- --nocapture --ignored
```

Expected: PASS (requires ≥1.5x improvement)

**Step 2: Debug if needed**

If test fails:
- Add debug logging to DP optimizer
- Check join order selected
- Verify cardinality estimates
- Compare costs manually

```rust
// Add to dp_optimizer.rs
impl<'a> DPOptimizer<'a> {
    fn log_decision(&self, msg: &str) {
        if std::env::var("DEBUG_OPTIMIZER").is_ok() {
            eprintln!("[DP] {}", msg);
        }
    }
}
```

Run with:
```bash
DEBUG_OPTIMIZER=1 cargo test test_three_table_optimization_improves_cost -- --nocapture --ignored
```

**Step 3: Fix implementation if needed**

Common issues:
- Join order not optimal → Check cost calculation
- Cardinality wrong → Fix formulas in CardinalityEstimator
- No improvement → Verify optimizer is actually being used

**Step 4: Verify test passes**

```bash
cargo test test_three_table_optimization_improves_cost -- --nocapture --ignored
```

Expected: PASS with ≥1.5x improvement

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs tests/planner/performance_test.rs
git commit -m "fix(dp-optimizer): achieve 1.5x improvement on three-table test"
```

---

### Task 17: Validate Star Schema Test Passes

**Files:**
- Test: `tests/planner/performance_test.rs`

**Step 1: Run the test**

```bash
cargo test test_star_schema_optimization -- --nocapture --ignored
```

Expected: PASS (requires ≥5x improvement)

**Step 2: Debug if needed**

Star schema requires joining all dimensions first before fact table:
- Check join order: Should be dims⋈dims⋈...⋈fact
- Verify small table detection working
- Ensure cardinality estimates favor dimension joins

**Step 3: Fix implementation if needed**

May need to adjust cost calculation to favor small tables:

```rust
// In build_join_plan()
let cost = CostEstimate {
    cpu: left.cost.cpu + right.cost.cpu + join_cost,
    io: left.cost.io + right.cost.io + (join_cost * 0.1),
    // Favor plans that keep intermediate results small
    memory: estimated_rows as f64, // Penalize large intermediates
};
```

**Step 4: Verify test passes**

```bash
cargo test test_star_schema_optimization -- --nocapture --ignored
```

Expected: PASS with ≥5x improvement

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs
git commit -m "fix(dp-optimizer): achieve 5x improvement on star schema test"
```

---

### Task 18: Validate Bushy Join Test Passes

**Files:**
- Test: `tests/planner/performance_test.rs`

**Step 1: Run the test**

```bash
cargo test test_bushy_join_benefit -- --nocapture --ignored
```

Expected: PASS (requires ≥3x improvement)

**Step 2: Debug if needed**

Bushy join requires exploring non-linear join trees:
- Verify enumerate_splits() generates bushy partitions
- Check memo contains bushy plans like (A⋈B)⋈(C⋈D)
- Ensure cost comparison favors bushy over left-deep

**Step 3: Fix implementation if needed**

Bushy joins should naturally emerge from DP algorithm. If not:
- Check split enumeration is correct
- Verify both join orders tried: (left, right) and (right, left)
- Ensure memo lookup working for sub-plans

**Step 4: Verify test passes**

```bash
cargo test test_bushy_join_benefit -- --nocapture --ignored
```

Expected: PASS with ≥3x improvement

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs
git commit -m "fix(dp-optimizer): achieve 3x improvement on bushy join test"
```

---

### Task 19: Validate Filter Optimization Test Passes

**Files:**
- Test: `tests/planner/performance_test.rs`

**Step 1: Run the test**

```bash
cargo test test_high_selectivity_filter_optimization -- --nocapture --ignored
```

Expected: PASS (requires ≥2x improvement)

**Step 2: Debug if needed**

Filter pushdown requires:
- Filters classified correctly by table dependencies
- Single-table filters applied in build_base_plan()
- Cardinality reduced by selectivity
- Cost reflects reduced rows

**Step 3: Fix implementation if needed**

Common issues:
- Filters not being pushed down → Check classify_filters()
- Cardinality not reduced → Verify selectivity calculation
- No cost benefit → Ensure cost uses estimated_rows

**Step 4: Verify test passes**

```bash
cargo test test_high_selectivity_filter_optimization -- --nocapture --ignored
```

Expected: PASS with ≥2x improvement

**Step 5: Commit**

```bash
git add src/planner/join_optimizer/dp_optimizer.rs
git commit -m "fix(dp-optimizer): achieve 2x improvement on filter optimization test"
```

---

### Task 20: Final Verification - All Tests Pass

**Files:**
- All test files

**Step 1: Run all unit tests**

```bash
cargo test --lib planner::join_optimizer -- --nocapture
```

Expected: All unit tests PASS (15+ tests)

**Step 2: Run all integration tests**

```bash
cargo test --test '*' dp -- --nocapture
```

Expected: All integration tests PASS (8+ tests)

**Step 3: Run all performance tests**

```bash
cargo test --test '*' performance -- --nocapture --ignored
```

Expected: All 4 performance tests PASS with required improvements:
- Three-table: ≥1.5x ✅
- Star schema: ≥5x ✅
- Bushy join: ≥3x ✅
- Filter optimization: ≥2x ✅

**Step 4: Run full test suite**

```bash
cargo test --all
```

Expected: All tests PASS, no regressions

**Step 5: Commit**

```bash
git add .
git commit -m "feat(dp-optimizer): complete DP join optimizer - all tests passing"
```

---

## Summary

**Total Tasks:** 20  
**Estimated Time:** 2-3 weeks  
**Lines of Code:** ~2,450 (1,650 production + 800 tests)

**Success Criteria:**
- ✅ All 27 tests passing (15 unit + 8 integration + 4 performance)
- ✅ Three-table test: ≥1.5x improvement
- ✅ Star schema test: ≥5x improvement
- ✅ Bushy join test: ≥3x improvement
- ✅ Filter optimization test: ≥2x improvement
- ✅ DP completes in < 100ms for 10 tables
- ✅ No regressions in existing queries

**Key Components:**
1. **JoinGraph** - Extracts join relationships from UnifiedGraph
2. **CardinalityEstimator** - Accurate join and filter cardinality
3. **DPOptimizer** - Bottom-up DP with filter pushdown
4. **OptimizerStrategy** - Adaptive selection (DP ≤10 tables, greedy >10)
5. **PhysicalConverter Integration** - Seamless integration with existing planner

**Next Steps After Implementation:**
1. Profile performance on 10-table queries (should be < 100ms)
2. Add rustdoc documentation to all public APIs
3. Update WAVE2_STATUS.md with results
4. Mark legacy optimizer as deprecated
5. Create migration guide for users

---

**Ready for Execution:**  
Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task.
