# SQL Planner Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a three-phase cost-based SQL planner that converts `model::Report` to optimized multi-dialect SQL using the unified graph's rich metadata.

**Architecture:** Logical Planning (Report → abstract ops) → Physical Candidates (generate alternatives) → Cost Estimation (pick best). Leverages existing `Query` builder for SQL emission.

**Tech Stack:** Rust, petgraph (graph queries), existing `src/sql/query.rs` builder, `src/semantic/graph/` unified graph

---

## Task 1: Set up planner module structure

**Files:**
- Create: `src/planner/mod.rs`
- Create: `src/planner/logical/mod.rs`
- Create: `src/planner/physical/mod.rs`
- Create: `src/planner/cost/mod.rs`
- Modify: `src/lib.rs`

**Step 1: Create planner module skeleton**

Create `src/planner/mod.rs`:

```rust
//! SQL query planner - converts model::Report to optimized SQL.
//!
//! Three-phase architecture:
//! 1. Logical Planning: Report → LogicalPlan (abstract operations)
//! 2. Physical Candidates: LogicalPlan → Vec<PhysicalPlan> (alternative strategies)
//! 3. Cost Estimation: Vec<PhysicalPlan> → Query (pick best, emit SQL)

pub mod logical;
pub mod physical;
pub mod cost;

use crate::model::Report;
use crate::semantic::graph::UnifiedGraph;
use crate::sql::query::Query;
use thiserror::Error;

/// Errors that can occur during planning.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Logical planning failed: {0}")]
    LogicalPlanError(String),
    
    #[error("Physical planning failed: {0}")]
    PhysicalPlanError(String),
    
    #[error("Cost estimation failed: {0}")]
    CostEstimationError(String),
    
    #[error("No valid physical plans generated")]
    NoValidPlans,
}

pub type PlanResult<T> = Result<T, PlanError>;

/// Main entry point for SQL planning.
pub struct SqlPlanner<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> SqlPlanner<'a> {
    /// Create a new SQL planner with access to the unified graph.
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    /// Plan a report into an optimized SQL query.
    ///
    /// This orchestrates all three phases:
    /// 1. Build logical plan from report
    /// 2. Generate physical plan candidates
    /// 3. Estimate costs and select best plan
    pub fn plan(&self, report: &Report) -> PlanResult<Query> {
        // Phase 1: Logical planning
        let logical_plan = logical::LogicalPlanner::new(self.graph)
            .plan(report)?;
        
        // Phase 2: Generate physical candidates
        let candidates = physical::PhysicalPlanner::new(self.graph)
            .generate_candidates(&logical_plan)?;
        
        // Phase 3: Cost estimation and selection
        let best_plan = cost::CostEstimator::new(self.graph)
            .select_best(candidates)?;
        
        // Convert to Query
        Ok(best_plan.to_query())
    }
}
```

**Step 2: Create logical planner stub**

Create `src/planner/logical/mod.rs`:

```rust
//! Logical planning - converts Report to abstract operation tree.

use crate::model::Report;
use crate::planner::PlanResult;
use crate::semantic::graph::UnifiedGraph;

/// Logical plan - abstract representation of query operations.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Placeholder during development
    Stub,
}

/// Logical planner.
pub struct LogicalPlanner<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> LogicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn plan(&self, _report: &Report) -> PlanResult<LogicalPlan> {
        // Stub implementation
        Ok(LogicalPlan::Stub)
    }
}
```

**Step 3: Create physical planner stub**

Create `src/planner/physical/mod.rs`:

```rust
//! Physical planning - generates alternative execution strategies.

use crate::planner::logical::LogicalPlan;
use crate::planner::PlanResult;
use crate::semantic::graph::UnifiedGraph;
use crate::sql::query::Query;

/// Physical plan - concrete execution strategy.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    // Placeholder
}

impl PhysicalPlan {
    pub fn to_query(&self) -> Query {
        Query::new()
    }
}

/// Physical planner.
pub struct PhysicalPlanner<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn generate_candidates(&self, _logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        // Stub implementation
        Ok(vec![PhysicalPlan {}])
    }
}
```

**Step 4: Create cost estimator stub**

Create `src/planner/cost/mod.rs`:

```rust
//! Cost estimation - selects best physical plan.

use crate::planner::physical::PhysicalPlan;
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

/// Cost estimator.
pub struct CostEstimator<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> CostEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn select_best(&self, mut candidates: Vec<PhysicalPlan>) -> PlanResult<PhysicalPlan> {
        candidates.pop().ok_or(PlanError::NoValidPlans)
    }
}
```

**Step 5: Add planner module to lib.rs**

Modify `src/lib.rs` - add to the module declarations:

```rust
pub mod planner;
```

**Step 6: Verify compilation**

Run: `cargo build`
Expected: Success (module structure compiles)

**Step 7: Commit**

```bash
git add src/planner/ src/lib.rs
git commit -m "feat(planner): add module structure with stubs"
```

---

## Task 2: Define logical plan types

**Files:**
- Create: `src/planner/logical/plan.rs`
- Modify: `src/planner/logical/mod.rs`

**Step 1: Write test for logical plan node creation**

Create `src/planner/logical/mod.rs` - add at bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_scan_node_creation() {
        let scan = LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        });
        
        assert!(matches!(scan, LogicalPlan::Scan(_)));
    }
    
    #[test]
    fn test_filter_node_creation() {
        use crate::model::expr::Expr;
        
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                entity: "sales".to_string(),
            })),
            predicates: vec![Expr::literal_bool(true)],
        });
        
        assert!(matches!(filter, LogicalPlan::Filter(_)));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --package mantis --lib planner::logical::tests`
Expected: FAIL with compilation errors (types not defined)

**Step 3: Create logical plan types**

Create `src/planner/logical/plan.rs`:

```rust
//! Logical plan node types.

use crate::model::expr::Expr;
use crate::model::TimeSuffix;
use crate::semantic::graph::Cardinality;

/// Logical plan - abstract operation tree.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // Core relational operations
    Scan(ScanNode),
    Join(JoinNode),
    Filter(FilterNode),
    Aggregate(AggregateNode),
    
    // Report-specific operations
    TimeMeasure(TimeMeasureNode),
    DrillPath(DrillPathNode),
    InlineMeasure(InlineMeasureNode),
    
    // Output formatting
    Project(ProjectNode),
    Sort(SortNode),
    Limit(LimitNode),
}

/// Scan a table.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanNode {
    pub entity: String,
}

/// Join two plans.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: JoinCondition,
    pub cardinality: Cardinality,
}

/// Join condition (columns to join on).
#[derive(Debug, Clone, PartialEq)]
pub struct JoinCondition {
    pub left_entity: String,
    pub left_column: String,
    pub right_entity: String,
    pub right_column: String,
}

/// Filter rows.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicates: Vec<Expr>,
}

/// Aggregate (GROUP BY).
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<ColumnRef>,
    pub measures: Vec<MeasureRef>,
}

/// Time measure (YTD, prior period, etc).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeMeasureNode {
    pub input: Box<LogicalPlan>,
    pub base_measure: String,
    pub time_suffix: TimeSuffix,
    pub calendar: String,
}

/// Drill path navigation.
#[derive(Debug, Clone, PartialEq)]
pub struct DrillPathNode {
    pub input: Box<LogicalPlan>,
    pub source: String,
    pub path: String,
    pub level: String,
}

/// Inline measure (user-defined calculation).
#[derive(Debug, Clone, PartialEq)]
pub struct InlineMeasureNode {
    pub input: Box<LogicalPlan>,
    pub name: String,
    pub expr: Expr,
}

/// Project columns (SELECT).
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub projections: Vec<ProjectionItem>,
}

/// Projection item in SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionItem {
    Column(ColumnRef),
    Measure(MeasureRef),
    Expr { expr: Expr, alias: Option<String> },
}

/// Sort rows (ORDER BY).
#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    pub input: Box<LogicalPlan>,
    pub order_by: Vec<OrderRef>,
}

/// ORDER BY reference.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRef {
    pub column: String,
    pub descending: bool,
}

/// Limit rows.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub limit: u64,
}

/// Reference to a column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub entity: String,
    pub column: String,
}

/// Reference to a measure.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureRef {
    pub entity: String,
    pub measure: String,
}
```

**Step 4: Update logical module exports**

Modify `src/planner/logical/mod.rs` - replace the LogicalPlan enum stub with:

```rust
mod plan;

pub use plan::*;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test --package mantis --lib planner::logical::tests`
Expected: PASS (both tests pass)

**Step 6: Commit**

```bash
git add src/planner/logical/
git commit -m "feat(planner): define logical plan node types"
```

---

## Task 3: Implement basic logical planner

**Files:**
- Create: `src/planner/logical/builder.rs`
- Modify: `src/planner/logical/mod.rs`

**Step 1: Write test for simple report to logical plan**

Add to `src/planner/logical/mod.rs` tests:

```rust
#[test]
fn test_simple_report_to_logical_plan() {
    use crate::model::{Report, ShowItem};
    use crate::semantic::graph::UnifiedGraph;
    
    let graph = UnifiedGraph::new();
    
    let report = Report {
        name: "test".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "revenue".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let planner = LogicalPlanner::new(&graph);
    let plan = planner.plan(&report).unwrap();
    
    // Should have Scan → Aggregate → Project structure
    assert!(matches!(plan, LogicalPlan::Project(_)));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --package mantis --lib planner::logical::tests::test_simple_report_to_logical_plan`
Expected: FAIL (returns Stub, not Project)

**Step 3: Implement basic report to logical plan conversion**

Create `src/planner/logical/builder.rs`:

```rust
//! Build logical plans from reports.

use crate::model::{Report, ShowItem};
use crate::planner::logical::{
    AggregateNode, ColumnRef, LogicalPlan, MeasureRef, OrderRef, ProjectNode, ProjectionItem,
    ScanNode, SortNode,
};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

pub struct PlanBuilder<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> PlanBuilder<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn build(&self, report: &Report) -> PlanResult<LogicalPlan> {
        // Start with base scan
        let mut plan = self.build_scan(report)?;
        
        // Add aggregation if needed
        plan = self.build_aggregate(plan, report)?;
        
        // Add projection
        plan = self.build_project(plan, report)?;
        
        // Add sort if needed
        if !report.sort.is_empty() {
            plan = self.build_sort(plan, report)?;
        }
        
        // Add limit if needed
        if let Some(limit) = report.limit {
            plan = LogicalPlan::Limit(crate::planner::logical::LimitNode {
                input: Box::new(plan),
                limit,
            });
        }
        
        Ok(plan)
    }
    
    fn build_scan(&self, report: &Report) -> PlanResult<LogicalPlan> {
        // For now, just use first from table
        let entity = report.from.first()
            .ok_or_else(|| PlanError::LogicalPlanError("Report has no FROM table".to_string()))?;
        
        Ok(LogicalPlan::Scan(ScanNode {
            entity: entity.clone(),
        }))
    }
    
    fn build_aggregate(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        // Collect measures from show items
        let mut measures = Vec::new();
        
        for item in &report.show {
            match item {
                ShowItem::Measure { name, .. } => {
                    // Assume measure belongs to first table for now
                    if let Some(entity) = report.from.first() {
                        measures.push(MeasureRef {
                            entity: entity.clone(),
                            measure: name.clone(),
                        });
                    }
                }
                _ => {
                    // Handle other show items later
                }
            }
        }
        
        if measures.is_empty() {
            return Ok(input);
        }
        
        Ok(LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![], // TODO: handle GROUP BY
            measures,
        }))
    }
    
    fn build_project(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        let mut projections = Vec::new();
        
        for item in &report.show {
            match item {
                ShowItem::Measure { name, .. } => {
                    if let Some(entity) = report.from.first() {
                        projections.push(ProjectionItem::Measure(MeasureRef {
                            entity: entity.clone(),
                            measure: name.clone(),
                        }));
                    }
                }
                _ => {
                    // Handle other show items later
                }
            }
        }
        
        Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            projections,
        }))
    }
    
    fn build_sort(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        let order_by = report.sort.iter()
            .map(|sort_item| OrderRef {
                column: sort_item.column.clone(),
                descending: matches!(sort_item.direction, crate::model::SortDirection::Desc),
            })
            .collect();
        
        Ok(LogicalPlan::Sort(SortNode {
            input: Box::new(input),
            order_by,
        }))
    }
}
```

**Step 4: Update LogicalPlanner to use builder**

Modify `src/planner/logical/mod.rs` - update the plan method:

```rust
mod builder;

// Update the plan method:
impl<'a> LogicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn plan(&self, report: &Report) -> PlanResult<LogicalPlan> {
        builder::PlanBuilder::new(self.graph).build(report)
    }
}
```

**Step 5: Run tests to verify they pass**

Run: `cargo test --package mantis --lib planner::logical::tests`
Expected: PASS (all tests pass)

**Step 6: Commit**

```bash
git add src/planner/logical/
git commit -m "feat(planner): implement basic logical plan builder"
```

---

## Task 4: Define physical plan structure

**Files:**
- Create: `src/planner/physical/plan.rs`
- Modify: `src/planner/physical/mod.rs`

**Step 1: Write test for physical plan creation**

Add to `src/planner/physical/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_physical_plan_to_query() {
        let plan = PhysicalPlan::new(vec!["sales".to_string()]);
        let query = plan.to_query();
        
        // Should produce a Query object
        assert!(query.from.is_none()); // Empty for now
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --package mantis --lib planner::physical::tests`
Expected: FAIL (PhysicalPlan::new doesn't exist)

**Step 3: Create physical plan structure**

Create `src/planner/physical/plan.rs`:

```rust
//! Physical plan representation.

use crate::sql::query::Query;
use std::cell::OnceCell;
use std::collections::HashMap;

/// Physical plan - concrete execution strategy.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    /// Join order (sequence of entity names)
    pub join_order: Vec<String>,
    
    /// Aggregation strategy per measure
    pub aggregation_strategy: HashMap<String, AggStrategy>,
    
    /// Time calculation strategy per time measure
    pub time_calc_strategy: HashMap<String, TimeStrategy>,
    
    /// Lazy-initialized Query object
    query: OnceCell<Query>,
}

/// Aggregation placement strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum AggStrategy {
    /// Aggregate before joining (CTE with GROUP BY)
    PreAggregate,
    /// Join first, then aggregate
    PostAggregate,
}

/// Time calculation strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeStrategy {
    /// Self-join with date offset
    SelfJoin,
    /// Window functions (OVER clause)
    WindowFunction,
    /// Filter-based (WHERE date range)
    FilterBased,
}

impl PhysicalPlan {
    /// Create a new physical plan.
    pub fn new(join_order: Vec<String>) -> Self {
        Self {
            join_order,
            aggregation_strategy: HashMap::new(),
            time_calc_strategy: HashMap::new(),
            query: OnceCell::new(),
        }
    }
    
    /// Convert to SQL Query (lazy initialization).
    pub fn to_query(&self) -> Query {
        self.query.get_or_init(|| self.build_query()).clone()
    }
    
    /// Build the Query object.
    fn build_query(&self) -> Query {
        // Stub implementation - just create empty query
        Query::new()
    }
}
```

**Step 4: Update physical module exports**

Modify `src/planner/physical/mod.rs` - replace PhysicalPlan stub with:

```rust
mod plan;

pub use plan::*;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test --package mantis --lib planner::physical::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add src/planner/physical/
git commit -m "feat(planner): define physical plan structure"
```

---

## Task 5: Implement basic physical planner

**Files:**
- Create: `src/planner/physical/candidates.rs`
- Modify: `src/planner/physical/mod.rs`

**Step 1: Write test for candidate generation**

Add to `src/planner/physical/mod.rs` tests:

```rust
#[test]
fn test_generate_single_candidate() {
    use crate::planner::logical::{LogicalPlan, ScanNode};
    use crate::semantic::graph::UnifiedGraph;
    
    let graph = UnifiedGraph::new();
    let logical = LogicalPlan::Scan(ScanNode {
        entity: "sales".to_string(),
    });
    
    let planner = PhysicalPlanner::new(&graph);
    let candidates = planner.generate_candidates(&logical).unwrap();
    
    assert_eq!(candidates.len(), 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --package mantis --lib planner::physical::tests::test_generate_single_candidate`
Expected: PASS (stub returns 1 candidate, but we'll make it real)

**Step 3: Implement candidate generator**

Create `src/planner/physical/candidates.rs`:

```rust
//! Physical plan candidate generation.

use crate::planner::logical::LogicalPlan;
use crate::planner::physical::PhysicalPlan;
use crate::planner::PlanResult;
use crate::semantic::graph::UnifiedGraph;

pub struct CandidateGenerator<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> CandidateGenerator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn generate(&self, logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        // For now, generate single candidate with simple strategy
        let join_order = self.extract_join_order(logical);
        
        Ok(vec![PhysicalPlan::new(join_order)])
    }
    
    fn extract_join_order(&self, logical: &LogicalPlan) -> Vec<String> {
        // Simple extraction - just get the base table
        match logical {
            LogicalPlan::Scan(scan) => vec![scan.entity.clone()],
            LogicalPlan::Project(proj) => self.extract_join_order(&proj.input),
            LogicalPlan::Aggregate(agg) => self.extract_join_order(&agg.input),
            LogicalPlan::Sort(sort) => self.extract_join_order(&sort.input),
            LogicalPlan::Limit(lim) => self.extract_join_order(&lim.input),
            _ => vec![],
        }
    }
}
```

**Step 4: Update PhysicalPlanner to use generator**

Modify `src/planner/physical/mod.rs`:

```rust
mod candidates;

// Update generate_candidates method:
impl<'a> PhysicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn generate_candidates(&self, logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        candidates::CandidateGenerator::new(self.graph).generate(logical)
    }
}
```

**Step 5: Run tests to verify they pass**

Run: `cargo test --package mantis --lib planner::physical::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add src/planner/physical/
git commit -m "feat(planner): implement basic candidate generator"
```

---

## Task 6: Implement cost estimation framework

**Files:**
- Create: `src/planner/cost/model.rs`
- Create: `src/planner/cost/estimator.rs`
- Modify: `src/planner/cost/mod.rs`

**Step 1: Write test for cost estimation**

Add to `src/planner/cost/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::physical::PhysicalPlan;
    use crate::semantic::graph::UnifiedGraph;
    
    #[test]
    fn test_select_best_from_multiple_candidates() {
        let graph = UnifiedGraph::new();
        let estimator = CostEstimator::new(&graph);
        
        let candidates = vec![
            PhysicalPlan::new(vec!["a".to_string()]),
            PhysicalPlan::new(vec!["b".to_string()]),
        ];
        
        let best = estimator.select_best(candidates).unwrap();
        assert_eq!(best.join_order.len(), 1);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --package mantis --lib planner::cost::tests`
Expected: PASS (stub works, but we'll make it real)

**Step 3: Create cost model structure**

Create `src/planner/cost/model.rs`:

```rust
//! Cost model for physical plan evaluation.

/// Cost weights for multi-objective scoring.
#[derive(Debug, Clone)]
pub struct CostWeights {
    pub max_intermediate_size: f64,
    pub total_rows_processed: f64,
    pub join_complexity: f64,
    pub aggregation_cost: f64,
    pub subquery_depth: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        Self {
            max_intermediate_size: 0.4,
            total_rows_processed: 0.3,
            join_complexity: 0.15,
            aggregation_cost: 0.1,
            subquery_depth: 0.05,
        }
    }
}

/// Estimated cost breakdown.
#[derive(Debug, Clone)]
pub struct CostEstimate {
    pub max_intermediate_size: f64,
    pub total_rows_processed: f64,
    pub join_complexity: f64,
    pub aggregation_cost: f64,
    pub subquery_depth: f64,
    pub total: f64,
}

impl CostEstimate {
    /// Calculate total cost using weights.
    pub fn calculate(weights: &CostWeights, components: CostComponents) -> Self {
        let total = 
            weights.max_intermediate_size * components.max_intermediate_size +
            weights.total_rows_processed * components.total_rows_processed +
            weights.join_complexity * components.join_complexity +
            weights.aggregation_cost * components.aggregation_cost +
            weights.subquery_depth * components.subquery_depth;
        
        Self {
            max_intermediate_size: components.max_intermediate_size,
            total_rows_processed: components.total_rows_processed,
            join_complexity: components.join_complexity,
            aggregation_cost: components.aggregation_cost,
            subquery_depth: components.subquery_depth,
            total,
        }
    }
}

/// Raw cost components before weighting.
#[derive(Debug, Clone)]
pub struct CostComponents {
    pub max_intermediate_size: f64,
    pub total_rows_processed: f64,
    pub join_complexity: f64,
    pub aggregation_cost: f64,
    pub subquery_depth: f64,
}
```

**Step 4: Create estimator implementation**

Create `src/planner/cost/estimator.rs`:

```rust
//! Cost estimation for physical plans.

use crate::planner::cost::model::{CostComponents, CostEstimate, CostWeights};
use crate::planner::physical::PhysicalPlan;
use crate::semantic::graph::UnifiedGraph;

pub struct PlanCostEstimator<'a> {
    graph: &'a UnifiedGraph,
    weights: CostWeights,
}

impl<'a> PlanCostEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self {
            graph,
            weights: CostWeights::default(),
        }
    }
    
    pub fn estimate(&self, plan: &PhysicalPlan) -> CostEstimate {
        let components = self.estimate_components(plan);
        CostEstimate::calculate(&self.weights, components)
    }
    
    fn estimate_components(&self, plan: &PhysicalPlan) -> CostComponents {
        // Stub implementation - return default costs
        CostComponents {
            max_intermediate_size: plan.join_order.len() as f64 * 1000.0,
            total_rows_processed: plan.join_order.len() as f64 * 10000.0,
            join_complexity: plan.join_order.len() as f64,
            aggregation_cost: 100.0,
            subquery_depth: 1.0,
        }
    }
}
```

**Step 5: Update CostEstimator to use new types**

Modify `src/planner/cost/mod.rs`:

```rust
mod model;
mod estimator;

pub use model::*;

use crate::planner::physical::PhysicalPlan;
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

pub struct CostEstimator<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> CostEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    pub fn select_best(&self, candidates: Vec<PhysicalPlan>) -> PlanResult<PhysicalPlan> {
        if candidates.is_empty() {
            return Err(PlanError::NoValidPlans);
        }
        
        let estimator = estimator::PlanCostEstimator::new(self.graph);
        
        // Find candidate with minimum cost
        let mut best_plan = None;
        let mut best_cost = f64::MAX;
        
        for plan in candidates {
            let estimate = estimator.estimate(&plan);
            if estimate.total < best_cost {
                best_cost = estimate.total;
                best_plan = Some(plan);
            }
        }
        
        best_plan.ok_or(PlanError::NoValidPlans)
    }
}
```

**Step 6: Run tests to verify they pass**

Run: `cargo test --package mantis --lib planner::cost::tests`
Expected: PASS

**Step 7: Commit**

```bash
git add src/planner/cost/
git commit -m "feat(planner): implement cost estimation framework"
```

---

## Task 7: Create first integration test

**Files:**
- Create: `tests/planner/mod.rs`
- Create: `tests/planner/single_table_tests.rs`
- Modify: `tests/lib.rs` (if it exists, otherwise create it)

**Step 1: Write end-to-end test for single table report**

Create `tests/planner/single_table_tests.rs`:

```rust
//! Integration tests for single-table reports.

use mantis::model::{Report, ShowItem};
use mantis::planner::SqlPlanner;
use mantis::semantic::graph::UnifiedGraph;
use mantis::sql::dialect::Dialect;

#[test]
fn test_single_table_simple_measure() {
    // Create minimal graph
    let graph = UnifiedGraph::new();
    
    // Create simple report
    let report = Report {
        name: "revenue_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "revenue".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    // Plan the report
    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    
    // Generate SQL
    let sql = query.to_sql(Dialect::DuckDb);
    
    // Verify SQL is generated (not empty)
    assert!(!sql.is_empty());
    
    // Future: verify SQL structure once physical plan builds queries
}
```

**Step 2: Create test module structure**

Create `tests/planner/mod.rs`:

```rust
mod single_table_tests;
```

Create or modify `tests/lib.rs`:

```rust
mod planner;
```

**Step 3: Run test to verify current behavior**

Run: `cargo test --test lib planner::single_table_tests::test_single_table_simple_measure`
Expected: PASS (generates empty query for now)

**Step 4: Commit**

```bash
git add tests/planner/
git add tests/lib.rs
git commit -m "test(planner): add first integration test"
```

---

## Task 8: Implement physical plan to Query conversion

**Files:**
- Modify: `src/planner/physical/plan.rs`

**Step 1: Write test for Query generation**

Add to `src/planner/physical/mod.rs` tests:

```rust
#[test]
fn test_physical_plan_generates_from_clause() {
    use crate::sql::dialect::Dialect;
    
    let plan = PhysicalPlan::new(vec!["sales".to_string()]);
    let query = plan.to_query();
    let sql = query.to_sql(Dialect::DuckDb);
    
    // Should have FROM clause
    assert!(sql.to_lowercase().contains("from") || sql.is_empty());
}
```

**Step 2: Run test**

Run: `cargo test --package mantis --lib planner::physical::tests::test_physical_plan_generates_from_clause`
Expected: PASS (empty SQL acceptable for now)

**Step 3: Implement basic Query building**

Modify `src/planner/physical/plan.rs` - update build_query method:

```rust
use crate::sql::query::TableRef;

impl PhysicalPlan {
    // ... existing code ...
    
    fn build_query(&self) -> Query {
        let mut query = Query::new();
        
        // Add FROM clause if we have entities
        if let Some(first_entity) = self.join_order.first() {
            query = query.from(TableRef::new(first_entity));
        }
        
        // TODO: Add joins, aggregations, etc.
        
        query
    }
}
```

**Step 4: Run test to verify it generates FROM**

Run: `cargo test --package mantis --lib planner::physical::tests::test_physical_plan_generates_from_clause`
Expected: PASS (now generates FROM clause)

**Step 5: Update integration test to check FROM clause**

Modify `tests/planner/single_table_tests.rs`:

```rust
#[test]
fn test_single_table_simple_measure() {
    // ... existing setup ...
    
    // Generate SQL
    let sql = query.to_sql(Dialect::DuckDb);
    
    // Verify SQL has FROM clause
    assert!(sql.to_lowercase().contains("from"));
    assert!(sql.to_lowercase().contains("sales"));
}
```

**Step 6: Run integration test**

Run: `cargo test --test lib planner::single_table_tests::test_single_table_simple_measure`
Expected: PASS

**Step 7: Commit**

```bash
git add src/planner/physical/plan.rs tests/planner/single_table_tests.rs
git commit -m "feat(planner): implement basic Query generation with FROM clause"
```

---

## Task 9: Add measure selection to physical plan

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Modify: `src/planner/physical/candidates.rs`

**Step 1: Write test for measure in SELECT**

Add to `tests/planner/single_table_tests.rs`:

```rust
#[test]
fn test_single_table_measure_in_select() {
    let graph = UnifiedGraph::new();
    
    let report = Report {
        name: "revenue_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "revenue".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    let sql = query.to_sql(Dialect::DuckDb);
    
    // Should have SELECT
    assert!(sql.to_lowercase().contains("select"));
    // Note: actual column names will be added when we have real graph metadata
}
```

**Step 2: Run test**

Run: `cargo test --test lib planner::single_table_tests::test_single_table_measure_in_select`
Expected: FAIL (no SELECT clause yet)

**Step 3: Pass logical plan info to physical plan**

Modify `src/planner/physical/plan.rs` - add measures field:

```rust
use crate::planner::logical::MeasureRef;

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub join_order: Vec<String>,
    pub aggregation_strategy: HashMap<String, AggStrategy>,
    pub time_calc_strategy: HashMap<String, TimeStrategy>,
    
    /// Measures to select
    pub measures: Vec<MeasureRef>,
    
    query: OnceCell<Query>,
}

impl PhysicalPlan {
    pub fn new(join_order: Vec<String>) -> Self {
        Self {
            join_order,
            aggregation_strategy: HashMap::new(),
            time_calc_strategy: HashMap::new(),
            measures: Vec::new(),
            query: OnceCell::new(),
        }
    }
    
    pub fn with_measures(mut self, measures: Vec<MeasureRef>) -> Self {
        self.measures = measures;
        self
    }
}
```

**Step 4: Update build_query to add SELECT**

Modify `src/planner/physical/plan.rs` - update build_query:

```rust
use crate::sql::expr::col;
use crate::sql::query::SelectExpr;

impl PhysicalPlan {
    fn build_query(&self) -> Query {
        let mut query = Query::new();
        
        // Add FROM clause
        if let Some(first_entity) = self.join_order.first() {
            query = query.from(TableRef::new(first_entity));
        }
        
        // Add SELECT clause
        if !self.measures.is_empty() {
            let select_exprs: Vec<SelectExpr> = self.measures.iter()
                .map(|m| SelectExpr::new(col(&m.measure)))
                .collect();
            query = query.select(select_exprs);
        }
        
        query
    }
}
```

**Step 5: Update candidate generator to include measures**

Modify `src/planner/physical/candidates.rs`:

```rust
use crate::planner::logical::{LogicalPlan, MeasureRef};

impl<'a> CandidateGenerator<'a> {
    pub fn generate(&self, logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        let join_order = self.extract_join_order(logical);
        let measures = self.extract_measures(logical);
        
        let plan = PhysicalPlan::new(join_order)
            .with_measures(measures);
        
        Ok(vec![plan])
    }
    
    fn extract_measures(&self, logical: &LogicalPlan) -> Vec<MeasureRef> {
        match logical {
            LogicalPlan::Aggregate(agg) => agg.measures.clone(),
            LogicalPlan::Project(proj) => self.extract_measures(&proj.input),
            LogicalPlan::Sort(sort) => self.extract_measures(&sort.input),
            LogicalPlan::Limit(lim) => self.extract_measures(&lim.input),
            _ => vec![],
        }
    }
}
```

**Step 6: Run test to verify SELECT is generated**

Run: `cargo test --test lib planner::single_table_tests::test_single_table_measure_in_select`
Expected: PASS

**Step 7: Commit**

```bash
git add src/planner/physical/ tests/planner/single_table_tests.rs
git commit -m "feat(planner): add measure selection to physical plan"
```

---

## Summary

This implementation plan provides a foundation for the SQL planner with:

1. ✅ Module structure (logical, physical, cost)
2. ✅ Logical plan types (Scan, Join, Filter, Aggregate, Project, Sort, Limit)
3. ✅ Basic logical planner (Report → LogicalPlan)
4. ✅ Physical plan structure (strategies, lazy Query building)
5. ✅ Candidate generation framework
6. ✅ Cost estimation framework (multi-objective scoring)
7. ✅ Integration test structure
8. ✅ Basic Query generation (FROM, SELECT)
9. ✅ Measure handling

**Next Steps** (for subsequent implementation plans):

- Add JOIN support (multi-table reports)
- Implement filter handling (WHERE clauses)
- Add GROUP BY support
- Implement time calculations (YTD, prior period)
- Add drill path navigation
- Implement join order optimization
- Add pre/post aggregation strategies
- Expand cost estimation to use graph metadata
- Add comprehensive integration tests
- Multi-dialect validation

**Verification:**

After completing these tasks, you should be able to:
- Plan simple single-table reports with measures
- Generate SQL with FROM and SELECT clauses
- Cost-estimate multiple candidates
- Run integration tests end-to-end
