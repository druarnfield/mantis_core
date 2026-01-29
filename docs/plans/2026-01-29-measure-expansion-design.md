# Measure Expansion Design

## Problem

The SQL planner outputs measure names as literal column references instead of expanding them to actual SQL expressions.

**Current output:**
```sql
SELECT "sales"."revenue" FROM "sales"
```

**Expected output:**
```sql
SELECT SUM("sales"."amount") AS "revenue" FROM "sales"
```

The `UnifiedGraph` stores measure expressions in `MeasureNode.expression` (e.g., `"SUM(@amount)"`), but `PhysicalPlan::to_query()` never retrieves or uses this data.

## Solution

Expand measures early in logical planning:

1. Look up the measure expression from the graph
2. Parse it into an `Expr` AST
3. Resolve `@atom` references to qualified column names
4. Store the resolved `Expr` in an `ExpandedMeasure` struct

## Data Flow

```
MeasureNode.expression: "SUM(@amount)"
         │
         ▼ parse_sql_expr()
Expr::Function { func: Sum, args: [Expr::AtomRef("amount")] }
         │
         ▼ resolve_atom_refs()
Expr::Function { func: Sum, args: [Expr::Column { entity: "sales", column: "amount" }] }
         │
         ▼ wrap in ExpandedMeasure
ExpandedMeasure { name: "revenue", entity: "sales", expr: ... }
```

The `ExpandedMeasure` flows through:
- `AggregateNode.measures: Vec<ExpandedMeasure>` (logical plan)
- `PhysicalPlan::HashAggregate.aggregates` (physical plan)
- `to_query()` (SQL generation)

## New Types

### ExpandedMeasure

New struct in `logical/plan.rs`:

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

### Type Changes

```rust
// logical/plan.rs - AggregateNode
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<ColumnRef>,
    pub measures: Vec<ExpandedMeasure>,  // was Vec<MeasureRef>
}

// logical/plan.rs - ProjectionItem
pub enum ProjectionItem {
    Column(ColumnRef),
    Measure(ExpandedMeasure),  // was MeasureRef
    Expr { expr: Expr, alias: Option<String> },
}
```

`MeasureRef` remains as the input type (what the report requests). Expansion happens when building `AggregateNode`.

## Graph Helper Method

New method in `semantic/graph/query.rs`:

```rust
impl UnifiedGraph {
    /// Expand a measure to its fully resolved expression.
    ///
    /// 1. Looks up MeasureNode by "entity.measure"
    /// 2. Parses the expression string
    /// 3. Resolves @atom references to qualified columns
    pub fn expand_measure(&self, entity: &str, measure: &str) -> QueryResult<Expr> {
        // 1. Look up measure
        let qualified = format!("{}.{}", entity, measure);
        let measure_idx = self.measure_index.get(&qualified)
            .ok_or_else(|| QueryError::MeasureNotFound(qualified.clone()))?;
        
        let measure_node = match self.graph.node_weight(*measure_idx) {
            Some(GraphNode::Measure(m)) => m,
            _ => return Err(QueryError::MeasureNotFound(qualified)),
        };
        
        // 2. Parse expression
        let expr_str = measure_node.expression.as_ref()
            .ok_or_else(|| QueryError::MeasureNotFound(
                format!("{} has no expression", qualified)
            ))?;
        
        let expr = parse_sql_expr(expr_str, Span::new(0, 0), ExprContext::Measure)
            .map_err(|e| QueryError::InvalidExpression(e.to_string()))?;
        
        // 3. Resolve atom refs
        self.resolve_atom_refs(expr, entity)
    }
    
    /// Resolve all AtomRef in an expression to qualified Column references.
    fn resolve_atom_refs(&self, expr: Expr, entity: &str) -> QueryResult<Expr> {
        // Walk the Expr tree, replace AtomRef("x") with Column { entity, column: "x" }
    }
}
```

New error variant:

```rust
pub enum QueryError {
    // ... existing variants ...
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),
}
```

## Logical Planner Changes

Update `build_aggregate()` in `logical/builder.rs`:

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

    // ... rest unchanged ...
    
    Ok(LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(input),
        group_by,
        measures,  // now Vec<ExpandedMeasure>
    }))
}
```

Similarly update `build_project()` to use `ExpandedMeasure` in `ProjectionItem::Measure`.

## Physical Plan Changes

Update `physical/plan.rs`:

```rust
pub enum PhysicalPlan {
    // ...
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<ExpandedMeasure>,  // was Vec<String>
    },
}
```

Update `to_query()`:

```rust
PhysicalPlan::HashAggregate { input, group_by, aggregates } => {
    let mut query = input.to_query();
    
    // GROUP BY unchanged
    let group_exprs: Vec<_> = group_by.iter()
        .map(|col| parse_column_ref(col))
        .collect();
    if !group_exprs.is_empty() {
        query = query.group_by(group_exprs);
    }
    
    // SELECT: convert ExpandedMeasure.expr to SQL, with alias
    let agg_exprs: Vec<SelectExpr> = aggregates.iter()
        .map(|m| {
            let sql_expr = convert_model_expr_to_sql(&m.expr);
            SelectExpr::new(sql_expr).alias(&m.name)
        })
        .collect();
    
    if !agg_exprs.is_empty() {
        query = query.select(agg_exprs);
    }
    query
}
```

Update `physical/converter.rs` to pass `ExpandedMeasure` through `convert_aggregate()` and `convert_project()`.

## Files to Modify

| File | Changes |
|------|---------|
| `src/planner/logical/plan.rs` | Add `ExpandedMeasure` struct; change `AggregateNode.measures` and `ProjectionItem::Measure` types |
| `src/semantic/graph/query.rs` | Add `expand_measure()` and `resolve_atom_refs()` methods; add `InvalidExpression` error variant |
| `src/planner/logical/builder.rs` | Update `build_aggregate()` and `build_project()` to expand measures |
| `src/planner/physical/plan.rs` | Change `HashAggregate.aggregates` type; update `to_query()` to emit actual expressions |
| `src/planner/physical/converter.rs` | Update `convert_aggregate()` and `convert_project()` to pass `ExpandedMeasure` |

## New Functionality Required

1. `UnifiedGraph::expand_measure()` - lookup + parse + resolve
2. `UnifiedGraph::resolve_atom_refs()` - tree walk to replace `AtomRef` with `Column`
3. `convert_model_expr_to_sql()` - convert `model::expr::Expr` to `sql::expr::Expr` (may already exist in `expr_converter.rs`)

## Design Decisions

1. **Early expansion** - Expand measures in logical planning, not SQL generation. Keeps `to_query()` simple.
2. **Parsed Expr AST** - Store resolved expressions as typed `Expr`, not raw SQL strings. Enables validation and future optimizations.
3. **Separate resolution pass** - Parse first, then resolve `@atom` refs. Keeps parsing pure.
4. **Qualified columns** - Resolve `@amount` to `Column { entity: "sales", column: "amount" }` for multi-table query support.
5. **Graph helper method** - `expand_measure()` encapsulates lookup/parse/resolve complexity, consistent with existing query methods.
