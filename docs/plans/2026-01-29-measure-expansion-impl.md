# Measure Expansion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix SQL generation to expand measure expressions (e.g., `SUM(@amount)`) instead of outputting literal column names.

**Architecture:** Add `expand_measure()` to UnifiedGraph that parses measure expressions and resolves `@atom` refs. Create `ExpandedMeasure` type to carry resolved expressions through logical → physical → SQL pipeline.

**Tech Stack:** 
- Existing `parse_sql_expr()` in `src/model/expr_parser.rs`
- Existing `ExprConverter` in `src/planner/expr_converter.rs`
- `model::expr::Expr` AST

---

## Task 1: Add ExpandedMeasure Type

**Files:**
- Modify: `src/planner/logical/plan.rs`

**Step 1: Add the ExpandedMeasure struct**

Add after the `MeasureRef` struct (around line 95):

```rust
/// A measure with its expression fully resolved and ready for SQL generation.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpandedMeasure {
    /// Measure name (used for AS alias in SELECT)
    pub name: String,
    /// Source entity (for context/debugging)
    pub entity: String,
    /// Fully resolved expression (all @atoms converted to qualified columns)
    pub expr: Expr,
}
```

**Step 2: Verify it compiles**

Run: `cargo check --lib`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add src/planner/logical/plan.rs
git commit -m "feat(planner): add ExpandedMeasure type"
```

---

## Task 2: Add InvalidExpression Error Variant

**Files:**
- Modify: `src/semantic/graph/query.rs`

**Step 1: Add the error variant**

Add to the `QueryError` enum (around line 15):

```rust
    #[error("Invalid expression for {measure}: {reason}")]
    InvalidExpression { measure: String, reason: String },
```

**Step 2: Verify it compiles**

Run: `cargo check --lib`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add src/semantic/graph/query.rs
git commit -m "feat(graph): add InvalidExpression error variant"
```

---

## Task 3: Implement resolve_atom_refs Helper

**Files:**
- Modify: `src/semantic/graph/query.rs`

**Step 1: Write the test**

Add at the bottom of the file (or in a tests module):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::expr::{Expr, Func, AggregateFunc};

    #[test]
    fn test_resolve_atom_refs_simple() {
        // AtomRef("amount") should become Column { entity: "sales", column: "amount" }
        let expr = Expr::AtomRef("amount".to_string());
        let resolved = resolve_atom_refs(expr, "sales").unwrap();
        
        assert_eq!(resolved, Expr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        });
    }

    #[test]
    fn test_resolve_atom_refs_in_function() {
        // SUM(@amount) should become SUM(sales.amount)
        let expr = Expr::Function {
            func: Func::Aggregate(AggregateFunc::Sum),
            args: vec![Expr::AtomRef("amount".to_string())],
        };
        let resolved = resolve_atom_refs(expr, "sales").unwrap();
        
        match resolved {
            Expr::Function { func: Func::Aggregate(AggregateFunc::Sum), args } => {
                assert_eq!(args.len(), 1);
                assert_eq!(args[0], Expr::Column {
                    entity: Some("sales".to_string()),
                    column: "amount".to_string(),
                });
            }
            _ => panic!("Expected Function"),
        }
    }

    #[test]
    fn test_resolve_atom_refs_nested() {
        // SUM(@amount * @quantity) should resolve both atoms
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::AtomRef("amount".to_string())),
            op: crate::model::expr::BinaryOp::Mul,
            right: Box::new(Expr::AtomRef("quantity".to_string())),
        };
        let resolved = resolve_atom_refs(expr, "sales").unwrap();
        
        match resolved {
            Expr::BinaryOp { left, right, .. } => {
                assert_eq!(*left, Expr::Column {
                    entity: Some("sales".to_string()),
                    column: "amount".to_string(),
                });
                assert_eq!(*right, Expr::Column {
                    entity: Some("sales".to_string()),
                    column: "quantity".to_string(),
                });
            }
            _ => panic!("Expected BinaryOp"),
        }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib semantic::graph::query::tests::test_resolve_atom_refs_simple`
Expected: FAIL with "cannot find function `resolve_atom_refs`"

**Step 3: Implement resolve_atom_refs**

Add the function (can be a free function or impl method):

```rust
use crate::model::expr::{Expr, BinaryOp as ExprBinaryOp, UnaryOp as ExprUnaryOp, Func, Literal};

/// Resolve all AtomRef in an expression to qualified Column references.
pub fn resolve_atom_refs(expr: Expr, entity: &str) -> QueryResult<Expr> {
    match expr {
        Expr::AtomRef(atom_name) => {
            Ok(Expr::Column {
                entity: Some(entity.to_string()),
                column: atom_name,
            })
        }
        
        Expr::Column { .. } => Ok(expr),
        Expr::Literal(_) => Ok(expr),
        
        Expr::Function { func, args } => {
            let resolved_args: Result<Vec<_>, _> = args
                .into_iter()
                .map(|arg| resolve_atom_refs(arg, entity))
                .collect();
            Ok(Expr::Function {
                func,
                args: resolved_args?,
            })
        }
        
        Expr::BinaryOp { left, op, right } => {
            Ok(Expr::BinaryOp {
                left: Box::new(resolve_atom_refs(*left, entity)?),
                op,
                right: Box::new(resolve_atom_refs(*right, entity)?),
            })
        }
        
        Expr::UnaryOp { op, expr: inner } => {
            Ok(Expr::UnaryOp {
                op,
                expr: Box::new(resolve_atom_refs(*inner, entity)?),
            })
        }
        
        Expr::Case { conditions, else_expr } => {
            let resolved_conditions: Result<Vec<_>, _> = conditions
                .into_iter()
                .map(|(cond, result)| {
                    Ok((
                        resolve_atom_refs(cond, entity)?,
                        resolve_atom_refs(result, entity)?,
                    ))
                })
                .collect();
            
            let resolved_else = match else_expr {
                Some(e) => Some(Box::new(resolve_atom_refs(*e, entity)?)),
                None => None,
            };
            
            Ok(Expr::Case {
                conditions: resolved_conditions?,
                else_expr: resolved_else,
            })
        }
        
        Expr::Cast { expr: inner, data_type } => {
            Ok(Expr::Cast {
                expr: Box::new(resolve_atom_refs(*inner, entity)?),
                data_type,
            })
        }
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --lib semantic::graph::query::tests`
Expected: PASS (all 3 tests)

**Step 5: Commit**

```bash
git add src/semantic/graph/query.rs
git commit -m "feat(graph): implement resolve_atom_refs helper"
```

---

## Task 4: Implement expand_measure on UnifiedGraph

**Files:**
- Modify: `src/semantic/graph/query.rs`

**Step 1: Write the test**

Add to the tests module:

```rust
use crate::semantic::graph::{UnifiedGraph, EntityNode, EntityType, SizeCategory, MeasureNode, GraphNode};
use std::collections::HashMap;

fn create_test_graph_with_measure() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();
    
    // Add entity
    let entity = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: Some("dbo.fact_sales".to_string()),
        schema: None,
        row_count: None,
        size_category: SizeCategory::Unknown,
        metadata: HashMap::new(),
    };
    graph.add_test_entity(entity);
    
    // Add measure with expression
    let measure = MeasureNode {
        name: "revenue".to_string(),
        entity: "sales".to_string(),
        aggregation: "CUSTOM".to_string(),
        source_column: None,
        expression: Some("SUM(@amount)".to_string()),
        metadata: HashMap::new(),
    };
    let measure_idx = graph.graph.add_node(GraphNode::Measure(measure));
    graph.measure_index.insert("sales.revenue".to_string(), measure_idx);
    
    graph
}

#[test]
fn test_expand_measure_simple() {
    let graph = create_test_graph_with_measure();
    
    let expr = graph.expand_measure("sales", "revenue").unwrap();
    
    // Should be SUM(sales.amount)
    match expr {
        Expr::Function { func: Func::Aggregate(AggregateFunc::Sum), args } => {
            assert_eq!(args.len(), 1);
            assert_eq!(args[0], Expr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            });
        }
        _ => panic!("Expected SUM function, got {:?}", expr),
    }
}

#[test]
fn test_expand_measure_not_found() {
    let graph = UnifiedGraph::new();
    
    let result = graph.expand_measure("sales", "nonexistent");
    assert!(result.is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib semantic::graph::query::tests::test_expand_measure_simple`
Expected: FAIL with "no method named `expand_measure`"

**Step 3: Implement expand_measure**

Add to `impl UnifiedGraph` in `query.rs`:

```rust
use crate::model::expr_parser::{parse_sql_expr, ExprContext};
use crate::dsl::Span;

impl UnifiedGraph {
    /// Expand a measure to its fully resolved expression.
    ///
    /// 1. Looks up MeasureNode by "entity.measure"
    /// 2. Parses the expression string
    /// 3. Resolves @atom references to qualified columns
    pub fn expand_measure(&self, entity: &str, measure: &str) -> QueryResult<crate::model::expr::Expr> {
        let qualified = format!("{}.{}", entity, measure);
        
        // 1. Look up measure
        let measure_idx = self.measure_index.get(&qualified)
            .ok_or_else(|| QueryError::MeasureNotFound(qualified.clone()))?;
        
        let measure_node = match self.graph.node_weight(*measure_idx) {
            Some(GraphNode::Measure(m)) => m,
            _ => return Err(QueryError::MeasureNotFound(qualified)),
        };
        
        // 2. Parse expression
        let expr_str = measure_node.expression.as_ref()
            .ok_or_else(|| QueryError::InvalidExpression {
                measure: qualified.clone(),
                reason: "measure has no expression".to_string(),
            })?;
        
        let expr = parse_sql_expr(expr_str, Span::new(0, 0), ExprContext::Measure)
            .map_err(|e| QueryError::InvalidExpression {
                measure: qualified.clone(),
                reason: e.to_string(),
            })?;
        
        // 3. Resolve atom refs
        resolve_atom_refs(expr, entity)
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --lib semantic::graph::query::tests::test_expand_measure`
Expected: PASS

**Step 5: Commit**

```bash
git add src/semantic/graph/query.rs
git commit -m "feat(graph): implement expand_measure method"
```

---

## Task 5: Update AggregateNode to Use ExpandedMeasure

**Files:**
- Modify: `src/planner/logical/plan.rs`

**Step 1: Update AggregateNode**

Change the `measures` field type (around line 50):

```rust
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<ColumnRef>,
    pub measures: Vec<ExpandedMeasure>,  // was Vec<MeasureRef>
}
```

**Step 2: Verify it compiles (expect errors)**

Run: `cargo check --lib 2>&1 | head -50`
Expected: FAIL with type mismatch errors in builder.rs and converter.rs

**Step 3: Commit partial change**

```bash
git add src/planner/logical/plan.rs
git commit -m "refactor(planner): change AggregateNode.measures to Vec<ExpandedMeasure>"
```

---

## Task 6: Update Logical Planner build_aggregate

**Files:**
- Modify: `src/planner/logical/builder.rs`

**Step 1: Update imports**

Add at the top:

```rust
use crate::planner::logical::ExpandedMeasure;
```

**Step 2: Update build_aggregate method**

Replace the measures collection logic (around line 75-90):

```rust
fn build_aggregate(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
    let mut measures = Vec::new();

    for item in &report.show {
        match item {
            ShowItem::Measure { name, .. } => {
                if let Some(entity) = report.from.first() {
                    // Expand measure using graph helper
                    let expr = self.graph.expand_measure(entity, name)
                        .map_err(|e| PlanError::LogicalPlanError(e.to_string()))?;
                    
                    measures.push(ExpandedMeasure {
                        name: name.clone(),
                        entity: entity.clone(),
                        expr,
                    });
                }
            }
            _ => {}
        }
    }

    if measures.is_empty() {
        return Ok(input);
    }

    let group_by = self.extract_group_by(report)?;

    Ok(LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(input),
        group_by,
        measures,
    }))
}
```

**Step 3: Verify it compiles**

Run: `cargo check --lib 2>&1 | head -50`
Expected: May still have errors in converter.rs (next task)

**Step 4: Commit**

```bash
git add src/planner/logical/builder.rs
git commit -m "feat(planner): expand measures in build_aggregate"
```

---

## Task 7: Update ProjectionItem to Use ExpandedMeasure

**Files:**
- Modify: `src/planner/logical/plan.rs`

**Step 1: Update ProjectionItem enum**

Change the Measure variant (around line 70):

```rust
pub enum ProjectionItem {
    Column(ColumnRef),
    Measure(ExpandedMeasure),  // was MeasureRef
    Expr { expr: Expr, alias: Option<String> },
}
```

**Step 2: Verify it compiles (expect errors)**

Run: `cargo check --lib 2>&1 | head -50`
Expected: FAIL with type mismatch in builder.rs build_project

**Step 3: Commit partial change**

```bash
git add src/planner/logical/plan.rs
git commit -m "refactor(planner): change ProjectionItem::Measure to ExpandedMeasure"
```

---

## Task 8: Update Logical Planner build_project

**Files:**
- Modify: `src/planner/logical/builder.rs`

**Step 1: Update build_project method**

Replace the projections collection logic (around line 100-120):

```rust
fn build_project(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
    let mut projections = Vec::new();

    for item in &report.show {
        match item {
            ShowItem::Measure { name, .. } => {
                if let Some(entity) = report.from.first() {
                    // Expand measure using graph helper
                    let expr = self.graph.expand_measure(entity, name)
                        .map_err(|e| PlanError::LogicalPlanError(e.to_string()))?;
                    
                    projections.push(ProjectionItem::Measure(ExpandedMeasure {
                        name: name.clone(),
                        entity: entity.clone(),
                        expr,
                    }));
                }
            }
            _ => {}
        }
    }

    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        projections,
    }))
}
```

**Step 2: Verify it compiles**

Run: `cargo check --lib 2>&1 | head -50`
Expected: May still have errors in physical converter (next task)

**Step 3: Commit**

```bash
git add src/planner/logical/builder.rs
git commit -m "feat(planner): expand measures in build_project"
```

---

## Task 9: Update Physical Plan HashAggregate Type

**Files:**
- Modify: `src/planner/physical/plan.rs`

**Step 1: Update imports**

Add at the top:

```rust
use crate::planner::logical::ExpandedMeasure;
```

**Step 2: Update HashAggregate variant**

Change the aggregates field (around line 55):

```rust
/// Hash aggregate
HashAggregate {
    input: Box<PhysicalPlan>,
    group_by: Vec<String>,
    aggregates: Vec<ExpandedMeasure>,  // was Vec<String>
},
```

**Step 3: Verify it compiles (expect errors)**

Run: `cargo check --lib 2>&1 | head -50`
Expected: FAIL with errors in converter.rs and to_query

**Step 4: Commit partial change**

```bash
git add src/planner/physical/plan.rs
git commit -m "refactor(physical): change HashAggregate.aggregates to Vec<ExpandedMeasure>"
```

---

## Task 10: Update Physical Converter convert_aggregate

**Files:**
- Modify: `src/planner/physical/converter.rs`

**Step 1: Update convert_aggregate method**

Replace the method (around line 250):

```rust
fn convert_aggregate(
    &self,
    agg: &crate::planner::logical::AggregateNode,
) -> PlanResult<Vec<PhysicalPlan>> {
    let input_candidates = self.convert(&agg.input)?;
    Ok(input_candidates
        .into_iter()
        .map(|input| PhysicalPlan::HashAggregate {
            input: Box::new(input),
            group_by: agg
                .group_by
                .iter()
                .map(|col| format!("{}.{}", col.entity, col.column))
                .collect(),
            aggregates: agg.measures.clone(),  // pass ExpandedMeasure through
        })
        .collect())
}
```

**Step 2: Update format_projection_item function**

Update the function at the top of the file (around line 10):

```rust
fn format_projection_item(item: &crate::planner::logical::ProjectionItem) -> crate::planner::logical::ProjectionItem {
    item.clone()  // Pass through as-is now
}
```

Actually, we need to change convert_project too. Let's update both:

**Step 3: Update convert_project method**

```rust
fn convert_project(
    &self,
    proj: &crate::planner::logical::ProjectNode,
) -> PlanResult<Vec<PhysicalPlan>> {
    let input_candidates = self.convert(&proj.input)?;
    Ok(input_candidates
        .into_iter()
        .map(|input| PhysicalPlan::Project {
            input: Box::new(input),
            projections: proj.projections.clone(),  // pass ProjectionItem through
        })
        .collect())
}
```

**Step 4: Update PhysicalPlan::Project type**

In `src/planner/physical/plan.rs`, update Project variant:

```rust
/// Projection
Project {
    input: Box<PhysicalPlan>,
    projections: Vec<crate::planner::logical::ProjectionItem>,  // was Vec<String>
},
```

**Step 5: Verify it compiles**

Run: `cargo check --lib 2>&1 | head -50`
Expected: May have errors in to_query (next task)

**Step 6: Commit**

```bash
git add src/planner/physical/converter.rs src/planner/physical/plan.rs
git commit -m "feat(physical): pass ExpandedMeasure through converter"
```

---

## Task 11: Update to_query for HashAggregate

**Files:**
- Modify: `src/planner/physical/plan.rs`

**Step 1: Update to_query HashAggregate arm**

Replace the HashAggregate match arm in `to_query()` (around line 200):

```rust
PhysicalPlan::HashAggregate {
    input,
    group_by,
    aggregates,
} => {
    let mut query = input.to_query();
    
    // GROUP BY
    let group_exprs: Vec<_> = group_by
        .iter()
        .map(|column| parse_column_ref(column))
        .collect();
    if !group_exprs.is_empty() {
        query = query.group_by(group_exprs);
    }
    
    // SELECT: convert ExpandedMeasure.expr to SQL with alias
    let context = self.extract_query_context();
    let agg_exprs: Vec<SelectExpr> = aggregates
        .iter()
        .map(|m| {
            let sql_expr = ExprConverter::convert(&m.expr, &context)
                .expect("Failed to convert measure expression");
            SelectExpr::new(sql_expr).alias(&m.name)
        })
        .collect();
    
    if !agg_exprs.is_empty() {
        query = query.select(agg_exprs);
    }
    query
}
```

**Step 2: Verify it compiles**

Run: `cargo check --lib`
Expected: May have errors in Project arm (next step)

**Step 3: Update to_query Project arm**

Replace the Project match arm:

```rust
PhysicalPlan::Project { input, projections } => {
    let query = input.to_query();
    let context = self.extract_query_context();
    
    let select_exprs: Vec<SelectExpr> = projections
        .iter()
        .map(|item| {
            use crate::planner::logical::ProjectionItem;
            match item {
                ProjectionItem::Column(col) => {
                    let expr = crate::sql::expr::table_col(&col.entity, &col.column);
                    SelectExpr::new(expr)
                }
                ProjectionItem::Measure(m) => {
                    let sql_expr = ExprConverter::convert(&m.expr, &context)
                        .expect("Failed to convert measure expression");
                    SelectExpr::new(sql_expr).alias(&m.name)
                }
                ProjectionItem::Expr { expr, alias } => {
                    let sql_expr = ExprConverter::convert(expr, &context)
                        .expect("Failed to convert expression");
                    let mut se = SelectExpr::new(sql_expr);
                    if let Some(a) = alias {
                        se = se.alias(a);
                    }
                    se
                }
            }
        })
        .collect();
    
    query.select(select_exprs)
}
```

**Step 4: Verify it compiles**

Run: `cargo check --lib`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add src/planner/physical/plan.rs
git commit -m "feat(physical): update to_query to emit measure expressions"
```

---

## Task 12: End-to-End Test

**Files:**
- Modify: `tests/compile_integration_test.rs` (or create new test)

**Step 1: Write integration test**

Add a test that verifies the full pipeline:

```rust
#[test]
fn test_measure_expansion_in_sql() {
    let dsl = r#"
table sales {
    source "dbo.fact_sales";
    
    atoms {
        amount decimal;
        quantity int;
    }
}

measures sales {
    revenue = { sum(@amount) };
    total_qty = { sum(@quantity) };
}

report test_report {
    from sales;
    show revenue;
}
"#;

    let sql = compile_to_sql(dsl, crate::sql::Dialect::Postgres).unwrap();
    
    // Should contain SUM with the actual column, not just "revenue"
    assert!(sql.contains("SUM"), "SQL should contain SUM aggregate: {}", sql);
    assert!(sql.contains("amount"), "SQL should reference amount column: {}", sql);
    assert!(sql.contains("AS"), "SQL should have alias: {}", sql);
    
    // Should NOT contain revenue as a column reference
    assert!(!sql.contains("\"sales\".\"revenue\""), 
        "SQL should not treat revenue as a column: {}", sql);
}
```

**Step 2: Run the test**

Run: `cargo test --test compile_integration_test test_measure_expansion_in_sql`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/compile_integration_test.rs
git commit -m "test: add measure expansion integration test"
```

---

## Task 13: Test with CLI

**Step 1: Build and run CLI**

```bash
cargo build --release
./target/release/mantis compile examples/sales.mantis --dialect postgres
```

**Step 2: Verify output**

Expected output should look like:
```sql
SELECT SUM("sales"."amount") AS "revenue"
FROM "dbo"."fact_sales" AS "sales"
```

NOT:
```sql
SELECT "sales"."revenue"
FROM "sales"
```

**Step 3: Final commit if needed**

```bash
git add -A
git commit -m "feat: complete measure expansion implementation"
```

---

## Summary

Tasks in order:
1. Add ExpandedMeasure type
2. Add InvalidExpression error variant
3. Implement resolve_atom_refs helper
4. Implement expand_measure on UnifiedGraph
5. Update AggregateNode to use ExpandedMeasure
6. Update logical planner build_aggregate
7. Update ProjectionItem to use ExpandedMeasure
8. Update logical planner build_project
9. Update physical plan HashAggregate type
10. Update physical converter
11. Update to_query for SQL generation
12. End-to-end integration test
13. Manual CLI verification
