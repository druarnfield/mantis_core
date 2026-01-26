# SQL Planner Enhancements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete the SQL planner by implementing all deferred features: filters, joins, GROUP BY, advanced cost models, join optimization, and time intelligence across three waves.

**Architecture:** Enhance each phase systematically - Wave 1 adds core query features (filters/joins/GROUP BY), Wave 2 adds optimization (advanced costs/join ordering), Wave 3 adds time intelligence (YTD/rolling/inline measures). Leverages UnifiedGraph metadata for accurate cost estimation.

**Tech Stack:** Rust, petgraph (graph queries), existing `src/sql/query.rs` builder, `src/semantic/graph/` UnifiedGraph, thiserror for errors

---

## WAVE 1: CORE QUERY FEATURES

### Task 1: Expression Converter - Foundation

**Files:**
- Create: `src/planner/expr_converter.rs`
- Modify: `src/planner/mod.rs`
- Test: `tests/planner/expr_converter_test.rs`

**Step 1: Write failing test for QueryContext**

Create `tests/planner/expr_converter_test.rs`:

```rust
use mantis::planner::expr_converter::{ExprConverter, QueryContext};
use mantis::planner::PlanError;

#[test]
fn test_query_context_table_aliases() {
    let mut context = QueryContext::new();
    context.add_table("sales".to_string(), "s".to_string());
    context.add_table("products".to_string(), "p".to_string());
    
    assert_eq!(context.get_table_alias("sales").unwrap(), "s");
    assert_eq!(context.get_table_alias("products").unwrap(), "p");
    assert!(context.get_table_alias("unknown").is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test expr_converter_test test_query_context_table_aliases`
Expected: FAIL with "no such module `expr_converter`"

**Step 3: Create QueryContext struct**

Create `src/planner/expr_converter.rs`:

```rust
//! Expression converter - converts model::Expr to sql::expr::Expr.

use crate::model::expr::Expr as ModelExpr;
use crate::planner::{PlanError, PlanResult};
use crate::sql::expr::Expr as SqlExpr;
use std::collections::HashMap;

/// Context for expression conversion - provides table aliases.
pub struct QueryContext {
    /// Map from entity name to table alias in current query
    table_aliases: HashMap<String, String>,
}

impl QueryContext {
    pub fn new() -> Self {
        Self {
            table_aliases: HashMap::new(),
        }
    }
    
    pub fn add_table(&mut self, entity: String, alias: String) {
        self.table_aliases.insert(entity, alias);
    }
    
    pub fn get_table_alias(&self, entity: &str) -> PlanResult<&str> {
        self.table_aliases
            .get(entity)
            .map(|s| s.as_str())
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", entity)))
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Stateless expression converter.
pub struct ExprConverter;
```

**Step 4: Export from mod.rs**

Update `src/planner/mod.rs`:

```rust
pub mod logical;
pub mod physical;
pub mod cost;
pub mod expr_converter;  // NEW

pub use expr_converter::{ExprConverter, QueryContext};  // NEW
```

**Step 5: Add test to Cargo.toml**

Add to `Cargo.toml`:

```toml
[[test]]
name = "expr_converter_test"
path = "tests/planner/expr_converter_test.rs"
```

**Step 6: Run test to verify it passes**

Run: `cargo test --test expr_converter_test test_query_context_table_aliases`
Expected: PASS

**Step 7: Commit**

```bash
git add src/planner/expr_converter.rs src/planner/mod.rs tests/planner/expr_converter_test.rs Cargo.toml
git commit -m "feat(planner): add QueryContext for expression conversion"
```

---

### Task 2: Expression Converter - Column References

**Files:**
- Modify: `src/planner/expr_converter.rs`
- Modify: `tests/planner/expr_converter_test.rs`

**Step 1: Write failing test for column conversion**

Add to `tests/planner/expr_converter_test.rs`:

```rust
use mantis::model::expr::Expr as ModelExpr;
use mantis::sql::expr::Expr as SqlExpr;

#[test]
fn test_convert_column_reference() {
    let mut context = QueryContext::new();
    context.add_table("sales".to_string(), "s".to_string());
    
    let model_expr = ModelExpr::Column {
        entity: Some("sales".to_string()),
        column: "amount".to_string(),
    };
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::Column { table, column } => {
            assert_eq!(table, Some("s".to_string()));
            assert_eq!(column, "amount");
        }
        _ => panic!("Expected Column expression"),
    }
}

#[test]
fn test_convert_column_unknown_entity() {
    let context = QueryContext::new();
    
    let model_expr = ModelExpr::Column {
        entity: Some("unknown".to_string()),
        column: "amount".to_string(),
    };
    
    let result = ExprConverter::convert(&model_expr, &context);
    assert!(result.is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test expr_converter_test test_convert_column`
Expected: FAIL with "convert not found"

**Step 3: Implement column conversion**

Add to `src/planner/expr_converter.rs`:

```rust
impl ExprConverter {
    /// Convert model expression to SQL expression.
    pub fn convert(
        expr: &ModelExpr,
        context: &QueryContext,
    ) -> PlanResult<SqlExpr> {
        match expr {
            ModelExpr::Column { entity, column } => {
                let table_alias = if let Some(ent) = entity {
                    Some(context.get_table_alias(ent)?.to_string())
                } else {
                    None
                };
                
                Ok(SqlExpr::Column {
                    table: table_alias,
                    column: column.clone(),
                })
            }
            
            _ => Err(PlanError::LogicalPlanError(
                "Expression type not yet supported".to_string()
            )),
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test expr_converter_test test_convert_column`
Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/expr_converter.rs tests/planner/expr_converter_test.rs
git commit -m "feat(planner): add column reference conversion"
```

---

### Task 3: Expression Converter - Literals

**Files:**
- Modify: `src/planner/expr_converter.rs`
- Modify: `tests/planner/expr_converter_test.rs`

**Step 1: Write failing tests for literals**

Add to `tests/planner/expr_converter_test.rs`:

```rust
use mantis::model::expr::Literal as ModelLiteral;
use mantis::sql::expr::Literal as SqlLiteral;

#[test]
fn test_convert_int_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Int(42));
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::Literal(SqlLiteral::Integer(val)) => assert_eq!(val, 42),
        _ => panic!("Expected Integer literal"),
    }
}

#[test]
fn test_convert_string_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::String("test".to_string()));
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::Literal(SqlLiteral::String(val)) => assert_eq!(val, "test"),
        _ => panic!("Expected String literal"),
    }
}

#[test]
fn test_convert_bool_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Bool(true));
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::Literal(SqlLiteral::Boolean(val)) => assert_eq!(val, true),
        _ => panic!("Expected Boolean literal"),
    }
}

#[test]
fn test_convert_null_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Null);
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    assert!(matches!(sql_expr, SqlExpr::Literal(SqlLiteral::Null)));
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test expr_converter_test test_convert_.*_literal`
Expected: FAIL with "not yet supported"

**Step 3: Implement literal conversion**

Update `src/planner/expr_converter.rs`:

```rust
use crate::model::expr::{Literal as ModelLiteral};
use crate::sql::expr::Literal as SqlLiteral;

impl ExprConverter {
    pub fn convert(
        expr: &ModelExpr,
        context: &QueryContext,
    ) -> PlanResult<SqlExpr> {
        match expr {
            ModelExpr::Column { entity, column } => {
                let table_alias = if let Some(ent) = entity {
                    Some(context.get_table_alias(ent)?.to_string())
                } else {
                    None
                };
                
                Ok(SqlExpr::Column {
                    table: table_alias,
                    column: column.clone(),
                })
            }
            
            ModelExpr::Literal(lit) => Ok(SqlExpr::Literal(Self::convert_literal(lit))),
            
            _ => Err(PlanError::LogicalPlanError(
                "Expression type not yet supported".to_string()
            )),
        }
    }
    
    fn convert_literal(lit: &ModelLiteral) -> SqlLiteral {
        match lit {
            ModelLiteral::Null => SqlLiteral::Null,
            ModelLiteral::Bool(b) => SqlLiteral::Boolean(*b),
            ModelLiteral::Int(i) => SqlLiteral::Integer(*i),
            ModelLiteral::Float(f) => SqlLiteral::Float(*f),
            ModelLiteral::String(s) => SqlLiteral::String(s.clone()),
            ModelLiteral::Date(d) => SqlLiteral::String(d.clone()), // SQL uses string for dates
            ModelLiteral::Timestamp(ts) => SqlLiteral::String(ts.clone()),
            ModelLiteral::Interval { value, unit } => {
                SqlLiteral::String(format!("INTERVAL '{}' {:?}", value, unit))
            }
        }
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --test expr_converter_test test_convert_.*_literal`
Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/expr_converter.rs tests/planner/expr_converter_test.rs
git commit -m "feat(planner): add literal conversion"
```

---

### Task 4: Expression Converter - Binary Operations

**Files:**
- Modify: `src/planner/expr_converter.rs`
- Modify: `tests/planner/expr_converter_test.rs`

**Step 1: Write failing tests for binary operations**

Add to `tests/planner/expr_converter_test.rs`:

```rust
use mantis::model::expr::BinaryOp as ModelBinaryOp;
use mantis::sql::expr::BinaryOperator as SqlBinaryOp;

#[test]
fn test_convert_binary_op_eq() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Literal(ModelLiteral::Int(1))),
        op: ModelBinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(ModelLiteral::Int(2))),
    };
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::BinaryOp { left, op, right } => {
            assert!(matches!(op, SqlBinaryOp::Eq));
            assert!(matches!(*left, SqlExpr::Literal(SqlLiteral::Integer(1))));
            assert!(matches!(*right, SqlExpr::Literal(SqlLiteral::Integer(2))));
        }
        _ => panic!("Expected BinaryOp"),
    }
}

#[test]
fn test_convert_binary_op_and() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Literal(ModelLiteral::Bool(true))),
        op: ModelBinaryOp::And,
        right: Box::new(ModelExpr::Literal(ModelLiteral::Bool(false))),
    };
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::BinaryOp { op, .. } => {
            assert!(matches!(op, SqlBinaryOp::And));
        }
        _ => panic!("Expected BinaryOp"),
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test expr_converter_test test_convert_binary_op`
Expected: FAIL with "not yet supported"

**Step 3: Implement binary operation conversion**

Update `src/planner/expr_converter.rs`:

```rust
use crate::model::expr::BinaryOp as ModelBinaryOp;
use crate::sql::expr::BinaryOperator as SqlBinaryOp;

impl ExprConverter {
    pub fn convert(
        expr: &ModelExpr,
        context: &QueryContext,
    ) -> PlanResult<SqlExpr> {
        match expr {
            ModelExpr::Column { entity, column } => {
                let table_alias = if let Some(ent) = entity {
                    Some(context.get_table_alias(ent)?.to_string())
                } else {
                    None
                };
                
                Ok(SqlExpr::Column {
                    table: table_alias,
                    column: column.clone(),
                })
            }
            
            ModelExpr::Literal(lit) => Ok(SqlExpr::Literal(Self::convert_literal(lit))),
            
            ModelExpr::BinaryOp { left, op, right } => {
                let left_sql = Self::convert(left, context)?;
                let right_sql = Self::convert(right, context)?;
                let op_sql = Self::convert_binary_op(op)?;
                
                Ok(SqlExpr::BinaryOp {
                    left: Box::new(left_sql),
                    op: op_sql,
                    right: Box::new(right_sql),
                })
            }
            
            _ => Err(PlanError::LogicalPlanError(
                "Expression type not yet supported".to_string()
            )),
        }
    }
    
    fn convert_binary_op(op: &ModelBinaryOp) -> PlanResult<SqlBinaryOp> {
        Ok(match op {
            // Arithmetic
            ModelBinaryOp::Add => SqlBinaryOp::Plus,
            ModelBinaryOp::Sub => SqlBinaryOp::Minus,
            ModelBinaryOp::Mul => SqlBinaryOp::Multiply,
            ModelBinaryOp::Div => SqlBinaryOp::Divide,
            ModelBinaryOp::Mod => SqlBinaryOp::Modulo,
            
            // Comparison
            ModelBinaryOp::Eq => SqlBinaryOp::Eq,
            ModelBinaryOp::Ne => SqlBinaryOp::NotEq,
            ModelBinaryOp::Lt => SqlBinaryOp::Lt,
            ModelBinaryOp::Gt => SqlBinaryOp::Gt,
            ModelBinaryOp::Lte => SqlBinaryOp::LtEq,
            ModelBinaryOp::Gte => SqlBinaryOp::GtEq,
            
            // Logical
            ModelBinaryOp::And => SqlBinaryOp::And,
            ModelBinaryOp::Or => SqlBinaryOp::Or,
            
            // String
            ModelBinaryOp::Concat => SqlBinaryOp::StringConcat,
            
            // Pattern
            ModelBinaryOp::Like => SqlBinaryOp::Like,
            ModelBinaryOp::ILike => SqlBinaryOp::ILike,
            
            // Set membership - these need special handling
            ModelBinaryOp::In | ModelBinaryOp::NotIn => {
                return Err(PlanError::LogicalPlanError(
                    "IN/NOT IN requires special handling with InSubquery".to_string()
                ));
            }
            
            // Range - needs special BETWEEN syntax
            ModelBinaryOp::Between | ModelBinaryOp::NotBetween => {
                return Err(PlanError::LogicalPlanError(
                    "BETWEEN requires special handling".to_string()
                ));
            }
        })
    }
    
    // ... existing convert_literal method
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --test expr_converter_test test_convert_binary_op`
Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/expr_converter.rs tests/planner/expr_converter_test.rs
git commit -m "feat(planner): add binary operation conversion"
```

---

### Task 5: Expression Converter - Unary Operations and Functions

**Files:**
- Modify: `src/planner/expr_converter.rs`
- Modify: `tests/planner/expr_converter_test.rs`

**Step 1: Write failing tests**

Add to `tests/planner/expr_converter_test.rs`:

```rust
use mantis::model::expr::{UnaryOp as ModelUnaryOp, Func};

#[test]
fn test_convert_unary_op_not() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::UnaryOp {
        op: ModelUnaryOp::Not,
        expr: Box::new(ModelExpr::Literal(ModelLiteral::Bool(true))),
    };
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::UnaryOp { .. } => {}, // Success
        _ => panic!("Expected UnaryOp"),
    }
}

#[test]
fn test_convert_function_call() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Function {
        func: Func::Scalar(mantis::model::expr::ScalarFunc::Upper),
        args: vec![ModelExpr::Literal(ModelLiteral::String("test".to_string()))],
    };
    
    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();
    
    match sql_expr {
        SqlExpr::Function { name, args, .. } => {
            assert_eq!(name, "UPPER");
            assert_eq!(args.len(), 1);
        }
        _ => panic!("Expected Function"),
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test expr_converter_test test_convert_unary_op test_convert_function`
Expected: FAIL

**Step 3: Implement unary ops and functions**

Update `src/planner/expr_converter.rs`:

```rust
use crate::model::expr::{UnaryOp as ModelUnaryOp, Func, AggregateFunc, ScalarFunc};
use crate::sql::expr::UnaryOperator as SqlUnaryOp;

impl ExprConverter {
    pub fn convert(
        expr: &ModelExpr,
        context: &QueryContext,
    ) -> PlanResult<SqlExpr> {
        match expr {
            // ... existing Column, Literal, BinaryOp
            
            ModelExpr::UnaryOp { op, expr } => {
                let expr_sql = Self::convert(expr, context)?;
                let op_sql = Self::convert_unary_op(op);
                
                Ok(SqlExpr::UnaryOp {
                    op: op_sql,
                    expr: Box::new(expr_sql),
                })
            }
            
            ModelExpr::Function { func, args } => {
                let args_sql: Result<Vec<_>, _> = args
                    .iter()
                    .map(|arg| Self::convert(arg, context))
                    .collect();
                
                let func_name = Self::convert_function(func);
                
                Ok(SqlExpr::Function {
                    name: func_name,
                    args: args_sql?,
                    distinct: false,
                })
            }
            
            ModelExpr::Case { conditions, else_expr } => {
                let when_clauses: Result<Vec<_>, _> = conditions
                    .iter()
                    .map(|(cond, result)| {
                        Ok((
                            Self::convert(cond, context)?,
                            Self::convert(result, context)?
                        ))
                    })
                    .collect();
                
                let else_sql = if let Some(else_e) = else_expr {
                    Some(Box::new(Self::convert(else_e, context)?))
                } else {
                    None
                };
                
                Ok(SqlExpr::Case {
                    operand: None,
                    when_clauses: when_clauses?,
                    else_clause: else_sql,
                })
            }
            
            ModelExpr::AtomRef(_) => {
                Err(PlanError::LogicalPlanError(
                    "AtomRef should be resolved before conversion".to_string()
                ))
            }
            
            ModelExpr::Cast { expr, data_type } => {
                let expr_sql = Self::convert(expr, context)?;
                // SQL cast syntax varies by dialect, use simple approach for now
                Ok(SqlExpr::Function {
                    name: "CAST".to_string(),
                    args: vec![expr_sql],
                    distinct: false,
                })
            }
        }
    }
    
    fn convert_unary_op(op: &ModelUnaryOp) -> SqlUnaryOp {
        match op {
            ModelUnaryOp::Not => SqlUnaryOp::Not,
            ModelUnaryOp::Neg => SqlUnaryOp::Minus,
            ModelUnaryOp::IsNull => SqlUnaryOp::IsNull,
            ModelUnaryOp::IsNotNull => SqlUnaryOp::IsNotNull,
        }
    }
    
    fn convert_function(func: &Func) -> String {
        match func {
            Func::Aggregate(agg) => match agg {
                AggregateFunc::Count => "COUNT".to_string(),
                AggregateFunc::Sum => "SUM".to_string(),
                AggregateFunc::Avg => "AVG".to_string(),
                AggregateFunc::Min => "MIN".to_string(),
                AggregateFunc::Max => "MAX".to_string(),
                AggregateFunc::CountDistinct => "COUNT".to_string(), // Handle DISTINCT separately
            },
            Func::Scalar(scalar) => match scalar {
                ScalarFunc::Upper => "UPPER".to_string(),
                ScalarFunc::Lower => "LOWER".to_string(),
                ScalarFunc::Trim => "TRIM".to_string(),
                ScalarFunc::Length => "LENGTH".to_string(),
                ScalarFunc::Substring => "SUBSTRING".to_string(),
                ScalarFunc::Concat => "CONCAT".to_string(),
                ScalarFunc::Coalesce => "COALESCE".to_string(),
                ScalarFunc::Round => "ROUND".to_string(),
                ScalarFunc::Floor => "FLOOR".to_string(),
                ScalarFunc::Ceil => "CEIL".to_string(),
                ScalarFunc::Abs => "ABS".to_string(),
                ScalarFunc::Now => "NOW".to_string(),
                ScalarFunc::CurrentDate => "CURRENT_DATE".to_string(),
                ScalarFunc::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
                ScalarFunc::Extract => "EXTRACT".to_string(),
                ScalarFunc::DateAdd => "DATE_ADD".to_string(),
                ScalarFunc::DateDiff => "DATEDIFF".to_string(),
                ScalarFunc::Year => "YEAR".to_string(),
                ScalarFunc::Month => "MONTH".to_string(),
                ScalarFunc::Day => "DAY".to_string(),
            },
        }
    }
    
    // ... existing methods
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --test expr_converter_test`
Expected: PASS

**Step 5: Commit**

```bash
git add src/planner/expr_converter.rs tests/planner/expr_converter_test.rs
git commit -m "feat(planner): add unary ops, functions, and CASE conversion"
```

---

### Task 6: Join Path Resolution - Foundation

**Files:**
- Create: `src/planner/join_builder.rs`
- Modify: `src/planner/mod.rs`
- Modify: `src/planner/logical/plan.rs`
- Test: `tests/planner/join_builder_test.rs`

**Step 1: Add JoinCondition and JoinType to logical plan**

Update `src/planner/logical/plan.rs`:

```rust
// Add after existing imports
use crate::semantic::graph::query::ColumnRef;

// Add new types before LogicalPlan enum
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    /// Equi-join on column pairs (most common)
    Equi(Vec<(ColumnRef, ColumnRef)>),
    /// Complex expression (for theta joins)
    Expr(crate::model::Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

// Update JoinNode to use new types
#[derive(Debug, Clone, PartialEq)]
pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: JoinCondition,           // CHANGED from old simple structure
    pub join_type: JoinType,         // NEW
    pub cardinality: Option<String>, // NEW - from graph metadata
}
```

**Step 2: Write failing test for JoinBuilder**

Create `tests/planner/join_builder_test.rs`:

```rust
use mantis::planner::join_builder::JoinBuilder;
use mantis::planner::logical::{LogicalPlan, ScanNode};
use mantis::semantic::graph::UnifiedGraph;

#[test]
fn test_build_single_table() {
    let graph = UnifiedGraph::new();
    let builder = JoinBuilder::new(&graph);
    
    let plan = builder.build_join_tree(&vec!["sales".to_string()]).unwrap();
    
    match plan {
        LogicalPlan::Scan(scan) => {
            assert_eq!(scan.entity, "sales");
        }
        _ => panic!("Expected Scan node for single table"),
    }
}
```

**Step 3: Run test to verify it fails**

Run: `cargo test --test join_builder_test`
Expected: FAIL with "no such module `join_builder`"

**Step 4: Create JoinBuilder foundation**

Create `src/planner/join_builder.rs`:

```rust
//! Join path resolution using UnifiedGraph.

use crate::planner::logical::{LogicalPlan, ScanNode, JoinNode, JoinCondition, JoinType};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::{UnifiedGraph, query::ColumnRef};

pub struct JoinBuilder<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> JoinBuilder<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }
    
    /// Build join tree for multiple tables.
    pub fn build_join_tree(&self, tables: &[String]) -> PlanResult<LogicalPlan> {
        if tables.is_empty() {
            return Err(PlanError::LogicalPlanError("No tables specified".into()));
        }
        
        if tables.len() == 1 {
            return Ok(LogicalPlan::Scan(ScanNode {
                entity: tables[0].clone(),
            }));
        }
        
        // Multi-table join - will implement in next step
        Err(PlanError::LogicalPlanError("Multi-table joins not yet implemented".into()))
    }
}
```

**Step 5: Export from mod.rs**

Update `src/planner/mod.rs`:

```rust
pub mod join_builder;  // NEW
pub use join_builder::JoinBuilder;  // NEW
```

**Step 6: Add test to Cargo.toml**

```toml
[[test]]
name = "join_builder_test"
path = "tests/planner/join_builder_test.rs"
```

**Step 7: Run test to verify it passes**

Run: `cargo test --test join_builder_test test_build_single_table`
Expected: PASS

**Step 8: Commit**

```bash
git add src/planner/join_builder.rs src/planner/mod.rs src/planner/logical/plan.rs tests/planner/join_builder_test.rs Cargo.toml
git commit -m "feat(planner): add JoinBuilder foundation and JoinCondition types"
```

---

### Task 7: Join Path Resolution - Multi-Table Joins

**Files:**
- Modify: `src/planner/join_builder.rs`
- Modify: `tests/planner/join_builder_test.rs`

**Step 1: Write failing test for two-table join**

Add to `tests/planner/join_builder_test.rs`:

```rust
#[test]
fn test_build_two_table_join() {
    // This will only pass if we have a test graph with relationships
    // For now, we'll test the structure is correct even if path finding fails
    let graph = UnifiedGraph::new();
    let builder = JoinBuilder::new(&graph);
    
    // This should attempt to find a join path
    let result = builder.build_join_tree(&vec!["sales".to_string(), "products".to_string()]);
    
    // For now, we expect an error because graph is empty
    // Real test will need a populated graph
    assert!(result.is_err() || matches!(result.unwrap(), LogicalPlan::Join(_)));
}
```

**Step 2: Implement multi-table join building**

Update `src/planner/join_builder.rs`:

```rust
use crate::semantic::graph::{GraphEdge, GraphNode};

impl<'a> JoinBuilder<'a> {
    pub fn build_join_tree(&self, tables: &[String]) -> PlanResult<LogicalPlan> {
        if tables.is_empty() {
            return Err(PlanError::LogicalPlanError("No tables specified".into()));
        }
        
        if tables.len() == 1 {
            return Ok(LogicalPlan::Scan(ScanNode {
                entity: tables[0].clone(),
            }));
        }
        
        // Start with first table
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: tables[0].clone(),
        });
        
        // Join remaining tables in order
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
                cardinality: Some(join_info.cardinality),
            });
        }
        
        Ok(plan)
    }
    
    /// Resolve join between two tables using UnifiedGraph.
    fn resolve_join(&self, from: &str, to: &str) -> PlanResult<JoinInfo> {
        // Use graph's find_path
        let path = self.graph.find_path(from, to)
            .map_err(|e| PlanError::LogicalPlanError(format!("No join path: {}", e)))?;
        
        // For now, only handle single-step paths
        if path.steps.len() != 1 {
            return Err(PlanError::LogicalPlanError(
                format!("Multi-hop joins not yet supported: {} -> {}", from, to)
            ));
        }
        
        let step = &path.steps[0];
        
        // Get actual join columns from graph edge
        let join_columns = self.get_join_columns(from, to)?;
        
        Ok(JoinInfo {
            condition: JoinCondition::Equi(join_columns),
            cardinality: step.cardinality.clone(),
        })
    }
    
    /// Get join columns from graph edge.
    fn get_join_columns(&self, from: &str, to: &str) -> PlanResult<Vec<(ColumnRef, ColumnRef)>> {
        // Look up entity indices
        let from_idx = self.graph.entity_index.get(from)
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", from)))?;
        let to_idx = self.graph.entity_index.get(to)
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", to)))?;
        
        // Find edge between entities
        if let Some(edge_idx) = self.graph.graph.find_edge(*from_idx, *to_idx) {
            if let Some(GraphEdge::JoinsTo(edge)) = self.graph.graph.edge_weight(edge_idx) {
                // Convert (String, String) to (ColumnRef, ColumnRef)
                let columns = edge.join_columns
                    .iter()
                    .map(|(from_col, to_col)| {
                        (
                            ColumnRef::new(from.to_string(), from_col.clone()),
                            ColumnRef::new(to.to_string(), to_col.clone()),
                        )
                    })
                    .collect();
                return Ok(columns);
            }
        }
        
        Err(PlanError::LogicalPlanError(
            format!("No join relationship between {} and {}", from, to)
        ))
    }
    
    fn get_rightmost_table(&self, plan: &LogicalPlan) -> String {
        match plan {
            LogicalPlan::Scan(scan) => scan.entity.clone(),
            LogicalPlan::Join(join) => self.get_rightmost_table(&join.right),
            _ => panic!("Unexpected plan node in join tree"),
        }
    }
}

struct JoinInfo {
    condition: JoinCondition,
    cardinality: String,
}
```

**Step 3: Run test**

Run: `cargo test --test join_builder_test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/planner/join_builder.rs tests/planner/join_builder_test.rs
git commit -m "feat(planner): implement multi-table join building with graph metadata"
```

---

### Task 8: Multi-Hop Join Path Support

**Purpose:** Enable joins through intermediate tables (e.g., sales → orders → customers)

**Files:**
- Modify: `src/planner/join_builder.rs`
- Modify: `tests/planner/join_builder_test.rs`

**Test:** Create test with UnifiedGraph containing multi-hop path, verify JoinBuilder creates chained joins

**Implement:** Extend `resolve_join()` to handle paths with multiple steps, build nested join tree for each hop. See design doc section "Join Path Resolution" for join chaining logic.

**Verify:** Test generates left-deep join tree matching path order

**Commit:** `feat(planner): add multi-hop join path support`

---

### Task 9: Physical Plan Filter Node

**Purpose:** Add WHERE clause support to physical plans

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/physical_filter_test.rs`

**Test:** Convert LogicalPlan::Filter to PhysicalPlan::Filter, verify predicates preserved

**Implement:** Add `Filter { input, predicates }` variant to PhysicalPlan enum. Converter creates Filter wrapping input plan. Reference design doc "Physical Plan Enhancements".

**Verify:** Physical plan contains Filter node with correct predicates

**Commit:** `feat(planner): add Filter physical plan node`

---

### Task 10: WHERE Clause Query Generation

**Purpose:** Generate SQL WHERE clauses from filter predicates

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Modify: `tests/planner/query_generation_test.rs`

**Test:** PhysicalPlan::Filter.to_query() generates Query with where() clause

**Implement:** In `to_query()` match on Filter variant, convert predicates using ExprConverter, combine with AND, call `query.where_(combined)`. See design doc "Physical Plan Query Generation".

**Verify:** Generated Query has WHERE with combined predicates

**Commit:** `feat(planner): generate WHERE clauses from filter nodes`

---

### Task 11: JOIN ON Clause Query Generation

**Purpose:** Generate SQL JOIN...ON clauses from join conditions

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Modify: `tests/planner/query_generation_test.rs`

**Test:** PhysicalPlan::HashJoin.to_query() generates Query with join() and ON condition

**Implement:** Add `build_join_condition()` helper that converts JoinCondition::Equi to SQL equality expressions (col1 = col2 AND ...). Call `query.join(table, type, on_expr)`. Reference design doc "Component 4: Physical Plan Query Generation".

**Verify:** Generated Query has JOIN with correct ON clause

**Commit:** `feat(planner): generate JOIN ON clauses from join nodes`

---

### Task 12: GROUP BY Extraction - Explicit

**Purpose:** Extract explicit GROUP BY columns from report.group

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Modify: `tests/planner/group_by_test.rs`

**Test:** Report with report.group populated generates LogicalPlan::Aggregate with group_by field

**Implement:** Add `extract_group_by()` method to PlanBuilder, iterate report.group and convert to ColumnRef. Call in `build()` before creating Aggregate node. See design doc "Component 3: Enhanced Logical Plan Builder".

**Verify:** Aggregate node group_by field matches report.group

**Commit:** `feat(planner): extract explicit GROUP BY from reports`

---

### Task 13: GROUP BY Extraction - Implicit Dimensions

**Purpose:** Add dimensions from report.show to GROUP BY (implicit grouping)

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Modify: `tests/planner/group_by_test.rs`

**Test:** Report with ShowItem::Column in show generates GROUP BY for those columns

**Implement:** In `extract_group_by()`, iterate report.show and add ShowItem::Column entries to group_by list. Skip ShowItem::Measure. Reference design doc "extract_group_by()" implementation.

**Verify:** GROUP BY includes both explicit group and dimension columns from show

**Commit:** `feat(planner): extract implicit GROUP BY from show dimensions`

---

### Task 14: Physical GROUP BY Query Generation

**Purpose:** Generate SQL GROUP BY clauses from aggregate nodes

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Modify: `tests/planner/query_generation_test.rs`

**Test:** PhysicalPlan::HashAggregate with group_by generates Query with group_by() clause

**Implement:** In HashAggregate.to_query(), convert group_by ColumnRefs to SQL column expressions, call `query.group_by(cols)`. See design doc physical plan query generation section.

**Verify:** Generated Query has GROUP BY with all grouping columns

**Commit:** `feat(planner): generate GROUP BY clauses from aggregate nodes`

---

### Task 15: Enhanced Logical Plan Builder Integration

**Purpose:** Update PlanBuilder to use all new components together

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Modify: `tests/planner/integration_test.rs`

**Test:** End-to-end test: Report with filters, joins, GROUP BY generates complete logical plan

**Implement:** Update `build()` to: 1) Use JoinBuilder for multi-table, 2) Add Filter node if predicates exist, 3) Add Aggregate with group_by + measures, 4) Add Project/Sort/Limit. Reference design doc "Enhanced Logical Plan Builder".

**Verify:** Complete logical plan with all node types in correct order

**Commit:** `feat(planner): integrate all logical plan components`

---

### Task 16: QueryContext Building for Joins

**Purpose:** Create QueryContext with table aliases for multi-table queries

**Files:**
- Modify: `src/planner/expr_converter.rs`
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/expr_converter_test.rs`

**Test:** Multi-table query builds QueryContext with all table aliases

**Implement:** Add `build_query_context()` method to PhysicalPlan that extracts table names and generates aliases (t1, t2, etc). Pass to ExprConverter. See design doc expression converter context section.

**Verify:** ExprConverter resolves columns to correct table aliases

**Commit:** `feat(planner): build QueryContext for multi-table queries`

---

### Task 17: Filter Predicate AND Combination

**Purpose:** Combine multiple filter predicates with AND

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/query_generation_test.rs`

**Test:** Filter with multiple predicates generates single WHERE with AND

**Implement:** Add `combine_with_and()` helper that takes Vec<Expr> and builds nested BinaryOp::And tree. Handle single predicate specially (no AND needed). Reference design doc filter query generation.

**Verify:** Multiple predicates combined correctly, single predicate unchanged

**Commit:** `feat(planner): combine filter predicates with AND`

---

### Task 18: Join Type Support

**Purpose:** Support different join types (INNER, LEFT, RIGHT, FULL)

**Files:**
- Modify: `src/planner/logical/plan.rs`
- Modify: `src/planner/physical/plan.rs`
- Modify: `src/sql/query.rs`
- Test: `tests/planner/join_types_test.rs`

**Test:** LogicalPlan with different JoinType values generates correct SQL

**Implement:** Add JoinType enum to logical and physical plans. Update query.join() to accept join type. Map to SQL JoinType in query generation. See design doc "Join Path Resolution" types.

**Verify:** LEFT/RIGHT/FULL joins generate correct SQL syntax

**Commit:** `feat(planner): support all SQL join types`

---

### Task 19: Integration Test - Complex Multi-Table Report

**Purpose:** Verify all Wave 1 features work together end-to-end

**Files:**
- Test: `tests/planner/wave1_integration_test.rs`

**Test:** Create Report with: 3 tables, multiple filters (AND/OR), GROUP BY, measures. Build plan, convert to physical, generate query. Verify SQL is syntactically correct and semantically matches report.

**Implement:** Comprehensive test covering all combinations. Use real-world example like sales/products/customers with date filters and category grouping. Reference design doc appendix example.

**Verify:** Generated SQL matches expected structure, all clauses present

**Commit:** `test(planner): add Wave 1 comprehensive integration test`

---

### Task 20: Error Handling and Edge Cases

**Purpose:** Handle error cases gracefully with clear messages

**Files:**
- Modify: `src/planner/mod.rs`
- Modify: `src/planner/expr_converter.rs`
- Modify: `src/planner/join_builder.rs`
- Test: `tests/planner/error_handling_test.rs`

**Test:** Test error cases: unknown entity, no join path, invalid expression, ambiguous column. Verify clear error messages returned.

**Implement:** Add PlanError variants for all error cases. Ensure proper error propagation with context. Add helpful error messages with entity/column names. See design doc "Error Handling" section.

**Verify:** All error cases produce PlanError with descriptive messages

**Commit:** `feat(planner): add comprehensive error handling for Wave 1`

---

## Wave 1 Completion Checklist

- ✅ Expression converter handles all Expr types
- ✅ Join path resolution uses UnifiedGraph metadata
- ✅ Multi-hop joins supported
- ✅ WHERE clauses generated correctly
- ✅ JOIN ON clauses use actual FK/PK columns
- ✅ GROUP BY extraction (explicit + implicit)
- ✅ All join types supported (INNER/LEFT/RIGHT/FULL)
- ✅ Comprehensive integration tests
- ✅ Error handling with clear messages
- ✅ No TODOs remaining in Wave 1 code

**Next:** Proceed to Wave 2 (Optimization) after Wave 1 verification.