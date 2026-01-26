# SQL Planner Enhancements Design

**Date:** 2026-01-26  
**Status:** Design  
**Context:** Complete the SQL planner by implementing deferred features: filters, joins, GROUP BY, advanced cost models, join optimization, and time intelligence.

## Background

The initial SQL planner implementation (completed 2026-01-26) established the three-phase architecture with basic functionality:

- ✅ Phase 1: Logical Planning (Report → LogicalPlan)
- ✅ Phase 2: Physical Planning (LogicalPlan → PhysicalPlan candidates)  
- ✅ Phase 3: Cost Estimation (select best plan)
- ✅ Query Generation (PhysicalPlan → SQL)

However, several critical features were intentionally deferred with TODOs:

- ❌ Filter predicates (WHERE clause generation)
- ❌ Join operations (multi-table queries with ON conditions)
- ❌ GROUP BY extraction from reports
- ❌ Advanced cost models using graph metadata
- ❌ Join order optimization
- ❌ TimeMeasure support (YTD, prior period, rolling averages)
- ❌ InlineMeasure support (user-defined calculations)

This design completes the planner by implementing all deferred features, leveraging the rich metadata in UnifiedGraph.

## Goals

1. **Enable real-world reports** - Support filters, joins, and grouping for production queries
2. **Performance optimization** - Use graph metadata for accurate cost estimation and join ordering
3. **Semantic layer capabilities** - Full time intelligence and calculated measures
4. **Production quality** - No more TODOs, fully functional planner

## Architecture Overview

### Three Implementation Waves

**Wave 1: Core Query Features** (Highest ROI)
- Expression converter (ModelExpr → SqlExpr)
- Join path resolution using UnifiedGraph
- WHERE clause generation
- GROUP BY extraction
- JOIN ON clause generation

**Wave 2: Optimization** (Performance)
- Advanced cost models with graph metadata
- Filter selectivity estimation
- Join cardinality estimation  
- Join order optimization
- Multi-objective cost scoring

**Wave 3: Time Intelligence** (Semantic Layer Power)
- TimeMeasure nodes with window functions
- InlineMeasure computed columns
- Complete ShowItem variant handling
- Calendar dimension integration

### Leveraging UnifiedGraph

The UnifiedGraph already provides sophisticated metadata that the planner should use:

```rust
// What UnifiedGraph offers:
graph.find_path(from, to) -> JoinPath         // BFS shortest path with join steps
graph.find_best_join_strategy(path)            // Hash/NL hints based on table sizes
graph.get_entity_size(entity) -> SizeCategory // Small/Medium/Large
graph.is_high_cardinality(column) -> bool     // Column selectivity info
JoinsToEdge.join_columns: Vec<(String, String)> // Actual FK/PK pairs
JoinsToEdge.cardinality: Cardinality          // 1:1, 1:N, N:M
```

**Key Design Principle:** Don't reimplement what UnifiedGraph already does well. Use its metadata for accurate cost estimation and intelligent optimization.

---

## Wave 1: Core Query Features

### Component 1: Expression Converter

**Problem:** Reports contain filters as `model::Expr`, but Query builder needs `sql::expr::Expr`.

**Design: Stateless converter with explicit context**

```rust
// src/planner/expr_converter.rs
pub struct ExprConverter;

impl ExprConverter {
    /// Convert model expression to SQL expression.
    /// 
    /// Context provides table aliases for the current query scope.
    pub fn convert(
        expr: &model::Expr,
        context: &QueryContext,
    ) -> Result<sql::expr::Expr, PlanError> {
        match expr {
            model::Expr::Column(col_ref) => {
                let table_alias = context.get_table_alias(&col_ref.entity)?;
                Ok(sql::expr::col(&format!("{}.{}", table_alias, col_ref.column)))
            }
            model::Expr::Literal(lit) => Self::convert_literal(lit),
            model::Expr::BinaryOp { op, left, right } => {
                let left_sql = Self::convert(left, context)?;
                let right_sql = Self::convert(right, context)?;
                Self::convert_binary_op(op, left_sql, right_sql)
            }
            model::Expr::UnaryOp { op, operand } => {
                let operand_sql = Self::convert(operand, context)?;
                Self::convert_unary_op(op, operand_sql)
            }
            model::Expr::Function { name, args } => {
                let args_sql: Result<Vec<_>, _> = args
                    .iter()
                    .map(|arg| Self::convert(arg, context))
                    .collect();
                Self::convert_function(name, args_sql?)
            }
            model::Expr::Case { conditions, else_result } => {
                Self::convert_case(conditions, else_result.as_deref(), context)
            }
        }
    }
}

/// Context for expression conversion
pub struct QueryContext {
    table_aliases: HashMap<String, String>,
}
```

**Rationale:**
- Stateless converter (pure function, easier to test)
- Context passed explicitly (no hidden state)
- Handles all expression types comprehensively
- Clear error handling for missing entities

### Component 2: Join Path Resolution

**Problem:** Reports specify tables to join, but we need actual FK/PK columns.

**Design: Leverage UnifiedGraph's existing capabilities**

```rust
// src/planner/logical/join_builder.rs
pub struct JoinBuilder<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> JoinBuilder<'a> {
    /// Build join tree for multiple tables.
    /// 
    /// Uses graph.find_path() to get shortest join path,
    /// then extracts actual join columns from JoinsToEdge.
    pub fn build_join_tree(
        &self,
        tables: &[String],
    ) -> Result<LogicalPlan, PlanError> {
        // Start with first table
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: tables[0].clone(),
        });
        
        // Join remaining tables in order
        // (Wave 2 will optimize this order)
        for right_table in &tables[1..] {
            let left_table = self.get_rightmost_table(&plan);
            let join_info = self.resolve_join(&left_table, right_table)?;
            
            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan(ScanNode {
                    entity: right_table.clone(),
                })),
                on: join_info.condition,
                join_type: JoinType::Inner,
                cardinality: join_info.cardinality,
            });
        }
        
        Ok(plan)
    }
    
    fn resolve_join(
        &self,
        from: &str,
        to: &str,
    ) -> Result<JoinInfo, PlanError> {
        // Use graph's find_path
        let path = self.graph.find_path(from, to)?;
        
        // Get actual join columns from graph edge
        let join_columns = self.get_join_columns(from, to)?;
        
        Ok(JoinInfo {
            condition: JoinCondition::Equi(join_columns),
            cardinality: path.steps[0].cardinality.clone(),
        })
    }
    
    fn get_join_columns(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<(ColumnRef, ColumnRef)>, PlanError> {
        // Look up JoinsTo edge in graph
        let edge = self.graph.find_edge(from, to)?;
        
        if let GraphEdge::JoinsTo(edge_data) = edge {
            // Convert (String, String) to (ColumnRef, ColumnRef)
            Ok(edge_data.join_columns
                .iter()
                .map(|(from_col, to_col)| {
                    (
                        ColumnRef::new(from.to_string(), from_col.clone()),
                        ColumnRef::new(to.to_string(), to_col.clone()),
                    )
                })
                .collect())
        } else {
            Err(PlanError::NoJoinPath { from, to })
        }
    }
}
```

**Enhanced Logical Plan Types:**

```rust
// src/planner/logical/plan.rs

pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: JoinCondition,
    pub join_type: JoinType,
    pub cardinality: String,  // NEW: from graph metadata
}

pub enum JoinCondition {
    Equi(Vec<(ColumnRef, ColumnRef)>),  // Most common
    Expr(model::Expr),                   // Complex conditions
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
```

**Rationale:**
- Uses UnifiedGraph's sophisticated join path finding (BFS)
- Gets actual FK/PK column names from metadata
- Stores cardinality for cost estimation
- Separates concerns: graph knows relationships, planner builds trees

### Component 3: Enhanced Logical Plan Builder

**Updated build() to extract all report information:**

```rust
// src/planner/logical/builder.rs

impl<'a> PlanBuilder<'a> {
    pub fn build(&self, report: &Report) -> PlanResult<LogicalPlan> {
        // 1. Base: Scan or Joins
        let mut plan = if report.from.len() == 1 {
            LogicalPlan::Scan(ScanNode {
                entity: report.from[0].clone(),
            })
        } else {
            JoinBuilder::new(self.graph).build_join_tree(&report.from)?
        };
        
        // 2. Filters (WHERE clause)
        if !report.filters.is_empty() {
            plan = LogicalPlan::Filter(FilterNode {
                input: Box::new(plan),
                predicates: report.filters.clone(),
            });
        }
        
        // 3. Aggregation (GROUP BY + measures)
        let group_by = self.extract_group_by(report)?;
        let measures = self.extract_measures(report)?;
        
        if !group_by.is_empty() || !measures.is_empty() {
            plan = LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(plan),
                group_by,
                measures,
            });
        }
        
        // 4. Projection, Sort, Limit (existing)
        // ...
        
        Ok(plan)
    }
    
    /// Extract GROUP BY columns.
    /// 
    /// Combines:
    /// - Explicit report.group items
    /// - Dimensions in report.show that aren't measures
    fn extract_group_by(&self, report: &Report) -> PlanResult<Vec<ColumnRef>> {
        let mut group_by = Vec::new();
        
        // Explicit GROUP BY
        for group_item in &report.group {
            if let Some(entity) = report.from.first() {
                group_by.push(ColumnRef::new(entity.clone(), group_item.clone()));
            }
        }
        
        // Implicit GROUP BY from dimensions in SHOW
        for show_item in &report.show {
            if let ShowItem::Column { entity, column } = show_item {
                group_by.push(ColumnRef::new(entity.clone(), column.clone()));
            }
        }
        
        Ok(group_by)
    }
}
```

### Component 4: Physical Plan Query Generation

**Enhanced to_query() with WHERE and JOIN support:**

```rust
// src/planner/physical/plan.rs

impl PhysicalPlan {
    pub fn to_query(&self) -> Query {
        match self {
            PhysicalPlan::Filter { input, predicates } => {
                let query = input.to_query();
                let context = self.build_query_context(&query);
                
                // Convert all predicates to SQL
                let where_exprs: Vec<_> = predicates
                    .iter()
                    .filter_map(|pred| ExprConverter::convert(pred, &context).ok())
                    .collect();
                
                if !where_exprs.is_empty() {
                    let combined = Self::combine_with_and(where_exprs);
                    query.where_(combined)
                } else {
                    query
                }
            }
            
            PhysicalPlan::HashJoin { left, right, on, .. } => {
                let left_query = left.to_query();
                let right_table = Self::extract_table(right);
                let context = self.build_query_context_for_join(&left_query, right);
                
                let on_expr = Self::build_join_condition(on, &context);
                
                left_query.join(right_table, JoinType::Inner, on_expr)
            }
            
            // ... other variants
        }
    }
    
    fn build_join_condition(
        on: &JoinCondition,
        context: &QueryContext,
    ) -> sql::expr::Expr {
        match on {
            JoinCondition::Equi(pairs) => {
                // Build: left.col1 = right.col1 AND left.col2 = right.col2
                let conditions: Vec<_> = pairs
                    .iter()
                    .map(|(left_col, right_col)| {
                        sql::expr::Expr::BinaryOp {
                            op: sql::expr::BinaryOp::Eq,
                            left: Box::new(sql::expr::col(&left_col.qualified_name())),
                            right: Box::new(sql::expr::col(&right_col.qualified_name())),
                        }
                    })
                    .collect();
                
                Self::combine_with_and(conditions)
            }
            JoinCondition::Expr(expr) => {
                ExprConverter::convert(expr, context).unwrap()
            }
        }
    }
}
```

**Wave 1 Summary:**
- ✅ ModelExpr → SqlExpr conversion with context
- ✅ Join path resolution using UnifiedGraph
- ✅ WHERE clause generation
- ✅ GROUP BY extraction (explicit + implicit)
- ✅ JOIN ON clause generation

---

## Wave 2: Optimization

### Component 1: Advanced Cost Estimator

**Problem:** Current cost model uses simple heuristics. Need accurate estimates using graph metadata.

**Design: Multi-objective cost model with cardinality-based estimates**

```rust
// src/planner/cost/estimator.rs

#[derive(Debug, Clone)]
pub struct CostEstimate {
    pub rows_out: usize,
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub memory_cost: f64,
}

impl CostEstimate {
    pub fn total(&self) -> f64 {
        self.cpu_cost * 1.0 +
        self.io_cost * 10.0 +   // IO is expensive
        self.memory_cost * 0.1   // Memory is cheap
    }
}

impl<'a> CostEstimator<'a> {
    fn estimate_cost(&self, plan: &PhysicalPlan) -> CostEstimate {
        match plan {
            PhysicalPlan::TableScan { table, strategy, .. } => {
                // Get actual row count from graph
                let row_count = self.graph
                    .get_entity_row_count(table)
                    .unwrap_or(1_000_000);
                
                let io_cost = match strategy {
                    TableScanStrategy::FullScan => row_count as f64,
                    TableScanStrategy::IndexScan { .. } => (row_count as f64) * 0.1,
                };
                
                CostEstimate {
                    rows_out: row_count,
                    cpu_cost: row_count as f64,
                    io_cost,
                    memory_cost: 0.0,
                }
            }
            
            PhysicalPlan::Filter { input, predicates } => {
                let input_cost = self.estimate_cost(input);
                let selectivity = self.estimate_filter_selectivity(predicates);
                
                CostEstimate {
                    rows_out: (input_cost.rows_out as f64 * selectivity) as usize,
                    cpu_cost: input_cost.cpu_cost + (input_cost.rows_out as f64),
                    io_cost: input_cost.io_cost,
                    memory_cost: input_cost.memory_cost,
                }
            }
            
            PhysicalPlan::HashJoin { left, right, on, .. } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let join_cardinality = self.estimate_join_cardinality(left, right, on);
                
                CostEstimate {
                    rows_out: join_cardinality,
                    cpu_cost: left_cost.cpu_cost + right_cost.cpu_cost 
                            + (left_cost.rows_out + right_cost.rows_out) as f64,
                    io_cost: left_cost.io_cost + right_cost.io_cost,
                    memory_cost: right_cost.rows_out as f64, // Hash table
                }
            }
            
            // ... other variants
        }
    }
    
    /// Estimate filter selectivity using graph metadata.
    fn estimate_filter_selectivity(&self, predicates: &[model::Expr]) -> f64 {
        let mut selectivity = 1.0;
        
        for pred in predicates {
            let pred_selectivity = match pred {
                model::Expr::BinaryOp { op: BinaryOp::Eq, left, .. } => {
                    // col = value: use cardinality
                    if let Some(col_ref) = Self::extract_column(left) {
                        if self.graph.is_high_cardinality(&col_ref.qualified_name()).unwrap_or(false) {
                            0.001 // High cardinality: very selective
                        } else {
                            0.1   // Low cardinality: less selective
                        }
                    } else {
                        0.1
                    }
                }
                model::Expr::BinaryOp { op: BinaryOp::Gt | BinaryOp::Lt, .. } => {
                    0.33 // Range predicates
                }
                model::Expr::BinaryOp { op: BinaryOp::And, left, right } => {
                    self.estimate_expr_selectivity(left) * 
                    self.estimate_expr_selectivity(right)
                }
                _ => 0.5,
            };
            
            selectivity *= pred_selectivity;
        }
        
        selectivity.max(0.01).min(1.0)
    }
    
    /// Estimate join cardinality using graph metadata.
    fn estimate_join_cardinality(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        on: &JoinCondition,
    ) -> usize {
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);
        
        // Get join cardinality from graph
        if let Some((left_table, right_table)) = Self::extract_join_tables(left, right) {
            if let Ok(path) = self.graph.find_path(&left_table, &right_table) {
                if let Some(step) = path.steps.first() {
                    return match step.cardinality.as_str() {
                        "1:1" => left_cost.rows_out.max(right_cost.rows_out),
                        "1:N" => right_cost.rows_out, // FK side
                        "N:1" => left_cost.rows_out,  // FK side
                        "N:M" => left_cost.rows_out * right_cost.rows_out / 100,
                        _ => left_cost.rows_out.max(right_cost.rows_out),
                    };
                }
            }
        }
        
        // Fallback: assume FK relationship
        left_cost.rows_out.max(right_cost.rows_out)
    }
}
```

**Rationale:**
- Uses actual row counts from graph instead of guessing
- Analyzes predicates for selectivity (high-card = more selective)
- Uses join cardinality metadata (1:1, 1:N, N:M)
- Multi-objective cost (CPU, IO, memory with weights)

### Component 2: Join Order Optimizer

**Problem:** Current planner joins tables in report order. Need to optimize join order for performance.

**Design: Enumerate small joins, greedy for large joins**

```rust
// src/planner/physical/join_optimizer.rs

pub struct JoinOptimizer<'a> {
    graph: &'a UnifiedGraph,
    cost_estimator: CostEstimator<'a>,
}

impl<'a> JoinOptimizer<'a> {
    /// Optimize join order for multi-way join.
    /// 
    /// Strategy:
    /// - 2-3 tables: Enumerate all orders (factorial)
    /// - 4+ tables: Greedy heuristic (smallest results first)
    pub fn optimize_join_order(
        &self,
        logical_join: &LogicalPlan,
    ) -> Vec<PhysicalPlan> {
        let tables = self.extract_tables(logical_join);
        
        if tables.len() <= 3 {
            // Small: try all permutations
            self.enumerate_all_join_orders(&tables)
        } else {
            // Large: greedy algorithm
            vec![self.greedy_join_order(&tables)]
        }
    }
    
    /// Greedy: always join smallest result next.
    fn greedy_join_order(&self, tables: &[String]) -> PhysicalPlan {
        let mut remaining: HashSet<_> = tables.iter().cloned().collect();
        let mut current_plan: Option<PhysicalPlan> = None;
        
        while !remaining.is_empty() {
            if current_plan.is_none() {
                // Start with smallest two tables
                let (t1, t2) = self.find_smallest_join_pair(&remaining);
                remaining.remove(&t1);
                remaining.remove(&t2);
                current_plan = Some(self.build_join(t1, t2));
            } else {
                // Find best table to join next
                let next = self.find_best_next_join(&current_plan.unwrap(), &remaining);
                remaining.remove(&next);
                current_plan = Some(self.build_join(current_plan.unwrap(), next));
            }
        }
        
        current_plan.unwrap()
    }
    
    fn find_smallest_join_pair(&self, tables: &HashSet<String>) -> (String, String) {
        let mut best_pair = None;
        let mut best_cost = f64::MAX;
        
        for t1 in tables {
            for t2 in tables {
                if t1 != t2 && self.graph.find_path(t1, t2).is_ok() {
                    let join_plan = self.build_join(t1.clone(), t2.clone());
                    let cost = self.cost_estimator.estimate_cost(&join_plan).total();
                    
                    if cost < best_cost {
                        best_cost = cost;
                        best_pair = Some((t1.clone(), t2.clone()));
                    }
                }
            }
        }
        
        best_pair.expect("No valid join pairs")
    }
}
```

**Rationale:**
- Small joins (≤3 tables): Optimal solution via enumeration
- Large joins (4+ tables): Fast greedy heuristic
- Uses cost estimator to pick best order
- Leverages graph's join path finding for validity

**Wave 2 Summary:**
- ✅ Accurate cost estimates using graph metadata
- ✅ Filter selectivity based on cardinality
- ✅ Join cardinality from relationship metadata
- ✅ Join order optimization (optimal for small, greedy for large)
- ✅ Multi-objective cost model

---

## Wave 3: Time Intelligence

### Component 1: TimeMeasure Support

**Problem:** Reports need time calculations like YTD, prior period, rolling averages.

**Design: Window functions for modern SQL, self-join fallback**

**Logical Plan Extension:**

```rust
// src/planner/logical/plan.rs

pub struct TimeMeasureNode {
    pub base_measure: MeasureRef,
    pub time_modifier: TimeModifier,
    pub calendar: String,
    pub input: Box<LogicalPlan>,
}

pub enum TimeModifier {
    YearToDate,
    QuarterToDate,
    MonthToDate,
    PriorPeriod { periods_back: usize },
    Rolling {
        window_size: usize,
        window_unit: TimeUnit,
        aggregation: RollingAgg,
    },
    PeriodOverPeriod { periods_back: usize },
}

pub enum TimeUnit {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

pub enum RollingAgg {
    Sum,
    Avg,
    Min,
    Max,
}
```

**Physical Plan Extension:**

```rust
// src/planner/physical/plan.rs

pub enum PhysicalPlan {
    // ... existing variants
    
    WindowFunction {
        input: Box<PhysicalPlan>,
        function: WindowFunc,
        partition_by: Vec<ColumnRef>,
        order_by: ColumnRef,
        frame: WindowFrame,
    },
}

pub enum WindowFunc {
    Sum,
    Avg,
    Min,
    Max,
    RowNumber,
    Rank,
    Lag { offset: usize },
    Lead { offset: usize },
}

pub struct WindowFrame {
    pub start: FrameBound,
    pub end: FrameBound,
}

pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
}
```

**Physical Strategy Selection:**

```rust
// src/planner/physical/converter.rs

impl<'a> PhysicalConverter<'a> {
    fn convert_time_measure(&self, node: &TimeMeasureNode) -> PlanResult<Vec<PhysicalPlan>> {
        let input_candidates = self.convert(&node.input)?;
        
        // Prefer window functions (modern, efficient)
        let supports_windows = true; // TODO: from dialect config
        
        if supports_windows {
            Ok(input_candidates
                .into_iter()
                .map(|input| {
                    let frame = self.build_window_frame(&node.time_modifier);
                    
                    PhysicalPlan::WindowFunction {
                        input: Box::new(input),
                        function: WindowFunc::Sum,
                        partition_by: self.get_partition_columns(&node.calendar),
                        order_by: self.get_date_column(&node.calendar),
                        frame,
                    }
                })
                .collect())
        } else {
            // Fallback: self-join strategy
            self.convert_time_measure_self_join(node, input_candidates)
        }
    }
    
    fn build_window_frame(&self, modifier: &TimeModifier) -> WindowFrame {
        match modifier {
            TimeModifier::YearToDate |
            TimeModifier::QuarterToDate |
            TimeModifier::MonthToDate => {
                WindowFrame {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::CurrentRow,
                }
            }
            
            TimeModifier::Rolling { window_size, .. } => {
                WindowFrame {
                    start: FrameBound::Preceding(*window_size - 1),
                    end: FrameBound::CurrentRow,
                }
            }
            
            TimeModifier::PriorPeriod { periods_back } => {
                WindowFrame {
                    start: FrameBound::Preceding(*periods_back),
                    end: FrameBound::Preceding(*periods_back),
                }
            }
            
            // ... other variants
        }
    }
}
```

**Query Generation:**

```rust
impl PhysicalPlan {
    pub fn to_query(&self) -> Query {
        match self {
            PhysicalPlan::WindowFunction {
                input,
                function,
                partition_by,
                order_by,
                frame,
            } => {
                let query = input.to_query();
                
                let window_expr = sql::expr::Expr::Window {
                    function: self.convert_window_func(function),
                    partition_by: partition_by
                        .iter()
                        .map(|col| sql::expr::col(&col.qualified_name()))
                        .collect(),
                    order_by: vec![sql::expr::col(&order_by.qualified_name())],
                    frame: Some(self.convert_window_frame(frame)),
                };
                
                query.select(vec![SelectExpr::new(window_expr)])
            }
            
            // ... existing variants
        }
    }
}
```

**Example SQL Generated:**

For `sales.total_amount_ytd`:

```sql
SELECT 
    date,
    SUM(total_amount) OVER (
        PARTITION BY YEAR(date)
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_amount_ytd
FROM sales
```

### Component 2: InlineMeasure Support

**Problem:** Users need custom calculated measures.

**Design: Computed columns with expression conversion**

**Logical Plan Extension:**

```rust
// src/planner/logical/plan.rs

pub struct InlineMeasureNode {
    pub name: String,
    pub expression: model::Expr,
    pub input: Box<LogicalPlan>,
}
```

**Physical Plan Extension:**

```rust
// src/planner/physical/plan.rs

pub enum PhysicalPlan {
    // ... existing variants
    
    ComputedColumn {
        input: Box<PhysicalPlan>,
        name: String,
        expression: model::Expr,
    },
}
```

**Query Generation:**

```rust
impl PhysicalPlan {
    pub fn to_query(&self) -> Query {
        match self {
            PhysicalPlan::ComputedColumn { input, name, expression } => {
                let query = input.to_query();
                let context = self.build_query_context(&query);
                
                let sql_expr = ExprConverter::convert(expression, &context)
                    .unwrap_or_else(|_| sql::expr::col("NULL"));
                
                query.select(vec![SelectExpr::new(sql_expr).alias(name)])
            }
            
            // ... existing variants
        }
    }
}
```

**Example SQL Generated:**

For inline measure `profit = revenue - cost`:

```sql
SELECT 
    *,
    (revenue - cost) as profit
FROM sales
```

### Component 3: Enhanced ShowItem Handling

**Update PlanBuilder to handle all ShowItem variants:**

```rust
// src/planner/logical/builder.rs

impl<'a> PlanBuilder<'a> {
    fn build_project(&self, plan: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        let mut enhanced_plan = plan;
        
        for show_item in &report.show {
            match show_item {
                ShowItem::Measure { .. } => {
                    // Regular measure - handled in aggregate
                }
                
                ShowItem::MeasureWithSuffix { name, suffix, .. } => {
                    let time_modifier = self.parse_time_suffix(suffix)?;
                    
                    enhanced_plan = LogicalPlan::TimeMeasure(TimeMeasureNode {
                        base_measure: MeasureRef {
                            entity: report.from[0].clone(),
                            measure: name.clone(),
                        },
                        time_modifier,
                        calendar: self.find_calendar_dimension(report)?,
                        input: Box::new(enhanced_plan),
                    });
                }
                
                ShowItem::InlineMeasure { name, expression, .. } => {
                    enhanced_plan = LogicalPlan::InlineMeasure(InlineMeasureNode {
                        name: name.clone(),
                        expression: expression.clone(),
                        input: Box::new(enhanced_plan),
                    });
                }
                
                ShowItem::Column { .. } => {
                    // Regular column - handled in projection
                }
            }
        }
        
        Ok(enhanced_plan)
    }
    
    fn parse_time_suffix(&self, suffix: &str) -> PlanResult<TimeModifier> {
        Ok(match suffix {
            "ytd" => TimeModifier::YearToDate,
            "qtd" => TimeModifier::QuarterToDate,
            "mtd" => TimeModifier::MonthToDate,
            "prior_year" => TimeModifier::PriorPeriod { periods_back: 1 },
            "rolling_3m" => TimeModifier::Rolling {
                window_size: 3,
                window_unit: TimeUnit::Month,
                aggregation: RollingAgg::Avg,
            },
            _ => return Err(PlanError::UnknownTimeSuffix(suffix.to_string())),
        })
    }
}
```

**Wave 3 Summary:**
- ✅ TimeMeasure nodes with window functions
- ✅ Self-join fallback for older databases
- ✅ InlineMeasure computed columns
- ✅ Complete ShowItem variant handling
- ✅ Calendar dimension integration

---

## Implementation Order

### Phase 1: Wave 1 (Core Query Features)
**Goal:** Enable real-world multi-table reports with filters and grouping

**Tasks:**
1. Expression converter (ModelExpr → SqlExpr)
2. Join path resolution using UnifiedGraph
3. Enhanced logical plan builder (filters, joins, GROUP BY)
4. Physical plan query generation (WHERE, JOIN ON)
5. Integration tests (multi-table reports with filters)

**Success Criteria:**
- Reports with multiple tables generate correct JOINs
- WHERE clauses work for all expression types
- GROUP BY correctly extracts dimensions and measures

### Phase 2: Wave 2 (Optimization)
**Goal:** Make queries fast through intelligent optimization

**Tasks:**
1. Enhanced cost estimator with graph metadata
2. Filter selectivity estimation
3. Join cardinality estimation
4. Join order optimizer (enumerate + greedy)
5. Performance tests (verify optimization works)

**Success Criteria:**
- Cost estimates within 2x of actual row counts
- Join order optimizer picks better plans than naive order
- Complex reports (5+ tables) execute efficiently

### Phase 3: Wave 3 (Time Intelligence)
**Goal:** Full semantic layer capabilities

**Tasks:**
1. TimeMeasure logical/physical plan nodes
2. Window function physical strategy
3. InlineMeasure computed columns
4. Enhanced ShowItem handling
5. Time intelligence integration tests

**Success Criteria:**
- YTD, prior period, rolling averages work correctly
- Window functions generate valid SQL
- Inline measures support complex expressions

---

## Testing Strategy

### Unit Tests

**Expression Converter:**
- All binary operators (=, >, <, AND, OR, etc.)
- All literal types (int, float, string, date, boolean)
- Nested expressions with correct precedence
- Function calls with multiple arguments
- CASE expressions

**Join Path Resolution:**
- Single-hop joins (direct FK)
- Multi-hop joins (through intermediate tables)
- Missing relationships (error handling)
- Circular references (prevent infinite loops)

**Cost Estimator:**
- Selectivity estimates for different predicate types
- Join cardinality for different relationship types (1:1, 1:N, N:M)
- Multi-objective cost calculation
- Edge cases (empty tables, very large tables)

**Join Optimizer:**
- Small joins (enumerate all orders correctly)
- Large joins (greedy produces reasonable results)
- Cost comparison (optimized < naive)

### Integration Tests

**Wave 1:**
- Single-table report with filters
- Two-table join with WHERE
- Three-table join with GROUP BY
- Complex predicates (AND/OR combinations)
- All ShowItem types (columns + measures)

**Wave 2:**
- Join order optimization (verify best plan selected)
- Cost estimation accuracy (compare estimates to actuals)
- Large joins (5+ tables execute without timeout)

**Wave 3:**
- YTD measures generate correct window functions
- Prior period comparisons work
- Rolling averages calculate correctly
- Inline measures with complex expressions
- Combined time + inline measures

### Performance Tests

**Benchmarks:**
- Small report (1 table, simple filter): < 10ms planning time
- Medium report (3 tables, multiple filters): < 50ms planning time
- Large report (7 tables, complex joins): < 200ms planning time

**Optimization Validation:**
- Optimized join order 2-10x faster than naive order
- Filter pushdown reduces intermediate results by 50-90%
- Cost estimates within 2x of actual query execution

---

## Error Handling

### Logical Planning Errors

```rust
pub enum PlanError {
    // Wave 1
    UnknownEntity(String),
    NoJoinPath { from: String, to: String },
    InvalidExpression(String),
    AmbiguousColumn(String),
    
    // Wave 2
    CostEstimationFailed(String),
    NoValidJoinOrder(String),
    
    // Wave 3
    UnknownTimeSuffix(String),
    InvalidWindowFrame(String),
    MissingCalendarDimension(String),
}
```

### Graceful Degradation

**When optimization fails:**
- Fall back to naive join order (report order)
- Use default selectivity estimates (0.5)
- Log warning but continue planning

**When time intelligence unavailable:**
- Detect database dialect capabilities
- Fall back to self-join strategy if no window functions
- Error clearly if feature truly unsupported

**When expression conversion fails:**
- Log specific expression that failed
- Return NULL literal as placeholder
- Include error in query metadata for debugging

---

## Performance Considerations

### Planning Time Budget

**Target:** < 100ms planning time for typical reports (3-5 tables)

**Optimization strategies:**
- Cache graph queries (join paths, cardinality)
- Limit join enumeration to ≤3 tables (factorial explosion)
- Use greedy heuristic for large joins (O(n²) vs O(n!))
- Prune obviously bad plans early

### Query Execution Time

**Optimization goals:**
- Filter pushdown reduces intermediate results by 50-90%
- Join order optimization improves 2-10x for complex reports
- Index scan selection (when available) improves 10-100x

**Monitoring:**
- Log cost estimates vs actual execution time
- Track optimization decisions (why this join order?)
- Measure planning time overhead

---

## Success Metrics

### Functional Completeness
- ✅ All TODO comments removed
- ✅ All ShowItem variants handled
- ✅ All Report fields used in planning
- ✅ All test cases passing

### Performance
- ✅ Planning time < 100ms for typical reports
- ✅ Optimized queries 2-10x faster than naive
- ✅ Cost estimates within 2x of actuals

### Code Quality
- ✅ Comprehensive unit test coverage (>80%)
- ✅ Integration tests for all features
- ✅ Clear error messages for failures
- ✅ Production-ready error handling

---

## Future Enhancements (Post-Implementation)

### Advanced Optimizations
- **Predicate pushdown through joins** - Apply filters early
- **Column pruning** - Only select needed columns
- **Materialized view matching** - Use pre-computed aggregates
- **Query result caching** - Cache common patterns

### Advanced Time Intelligence
- **Custom calendars** - Fiscal years, 4-4-5 weeks
- **Multi-calendar reports** - Compare fiscal vs calendar
- **Time dimension hierarchies** - Drill year→quarter→month→day

### Advanced Join Strategies
- **Bushy join trees** - Not just left-deep
- **Dynamic programming join enumeration** - Optimal for medium joins (4-6 tables)
- **Multi-way join optimization** - Star schema optimizations

### SQL Dialect Specialization
- **Database-specific hints** - Use optimizer hints where available
- **Dialect-specific features** - Leverage unique capabilities
- **Cost model tuning** - Per-database calibration

---

## Appendix: Example Query Progression

### Input Report

```yaml
from: [sales, products, customers]
show:
  - sales.total_amount
  - sales.total_amount_ytd
  - products.category
filters:
  - sales.date >= '2024-01-01'
  - customers.region = 'West'
group:
  - products.category
sort:
  - sales.total_amount DESC
limit: 100
```

### Logical Plan

```
Limit(100)
  └─ Sort(sales.total_amount DESC)
      └─ Project(category, total_amount, total_amount_ytd)
          └─ TimeMeasure(YTD, total_amount)
              └─ Aggregate(GROUP BY category, SUM(total_amount))
                  └─ Filter(sales.date >= '2024-01-01' AND customers.region = 'West')
                      └─ Join(customers, ON customers.id = sales.customer_id)
                          └─ Join(products, ON products.id = sales.product_id)
                              └─ Scan(sales)
```

### Physical Plan (Optimized)

```
Limit(100)
  └─ Sort(total_amount DESC)
      └─ WindowFunction(SUM OVER PARTITION BY YEAR(date))
          └─ HashAggregate(GROUP BY category)
              └─ HashJoin(sales ⋈ customers, build=customers[Small])
                  └─ HashJoin(sales ⋈ products, build=products[Small])
                      └─ Filter(sales.date >= '2024-01-01')
                          └─ TableScan(sales, rows=1M)
                      └─ TableScan(products, rows=10K)
                  └─ Filter(customers.region = 'West', selectivity=0.2)
                      └─ TableScan(customers, rows=100K)
```

**Optimizations applied:**
1. Join order: sales → products → customers (smallest builds first)
2. Filter pushdown: date filter on sales scan
3. Customer region filter before join (reduces join size)
4. Window function for YTD (efficient)
5. Hash joins with correct build/probe sides

### Generated SQL

```sql
SELECT 
    p.category,
    SUM(s.total_amount) as total_amount,
    SUM(SUM(s.total_amount)) OVER (
        PARTITION BY YEAR(s.date)
        ORDER BY s.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_amount_ytd
FROM sales s
JOIN products p ON p.id = s.product_id
JOIN customers c ON c.id = s.customer_id
WHERE s.date >= '2024-01-01'
  AND c.region = 'West'
GROUP BY p.category
ORDER BY total_amount DESC
LIMIT 100
```

---

## Conclusion

This enhancement design completes the SQL planner by implementing all deferred features across three waves:

1. **Wave 1 (Core):** Filters, joins, GROUP BY - enables real-world reports
2. **Wave 2 (Optimization):** Advanced cost models, join order - makes queries fast
3. **Wave 3 (Intelligence):** Time calculations, inline measures - semantic layer superpowers

The design leverages UnifiedGraph's rich metadata for accurate cost estimation and intelligent optimization, ensuring production-quality performance. With comprehensive error handling, testing, and monitoring, the planner will be ready for production deployment.
