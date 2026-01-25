# SQL Expression Parsing Design

**Date:** 2026-01-26  
**Status:** Design Complete - Ready for Implementation  
**Goal:** Replace regex-based SQL parsing with proper AST-based parsing for @atom references

---

## Executive Summary

The current system uses regex pattern matching to extract `@atom` references from SQL expressions. This is fragile, limited, and prevents proper SQL validation. We need to restore the AST-based SQL parsing from the archived model, enhanced with explicit `@atom` support for the new DSL system.

**Key Benefits:**
- **Immediate SQL syntax validation** (critical for LSP feedback)
- **Explicit @atom modeling** in AST (type-safe, compiler-verified)
- **Reliable dependency extraction** (no regex false positives/negatives)
- **Comprehensive testing** (snapshot + corpus tests for entire DSL parser)

---

## Architecture Overview

### Two-Phase Strategy

**Phase 1 - Parse Time (DSL Parser)**
- Parse SQL expressions into `Expr` AST using sqlparser-rs
- Preprocess: Transform `@identifier` → `__ATOM__identifier` (sqlparser marker)
- Postprocess: Transform markers back to `Expr::AtomRef(name)` in our AST
- Result: Validated SQL syntax + explicit atom references
- Errors: SQL syntax errors reported immediately (LSP-friendly)

**Phase 2 - Lowering Time (AST → Model)**
- Walk `Expr` AST to find all `AtomRef` nodes
- Validate each referenced atom exists in the table's atom list
- Track dependencies for graph building
- Result: Semantically validated expressions
- Errors: "Undefined atom '@revenue'" with span information

**Key Insight:** We restore the old `Expr` AST from the archived model, add `AtomRef` variant, and use it everywhere `SqlExpr` is currently used (measures, filters, calculated slicers).

---

## Type Definitions

### Core Expression AST

```rust
// src/model/expr.rs (restored from archive/model/expr.rs with additions)

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // NEW: Atom reference (@atom_name)
    AtomRef(String),
    
    // Existing from old model:
    Column { 
        entity: Option<String>, 
        column: String 
    },
    Literal(Literal),
    Function { 
        func: Func, 
        args: Vec<Expr> 
    },
    BinaryOp { 
        left: Box<Expr>, 
        op: BinaryOp, 
        right: Box<Expr> 
    },
    UnaryOp { 
        op: UnaryOp, 
        expr: Box<Expr> 
    },
    Case { 
        operand: Option<Box<Expr>>, 
        when_clauses: Vec<WhenClause>, 
        else_clause: Option<Box<Expr>> 
    },
    Cast { 
        expr: Box<Expr>, 
        target_type: DataType 
    },
    Window { 
        func: WindowFunc, 
        args: Vec<Expr>, 
        partition_by: Vec<Expr>, 
        order_by: Vec<OrderByExpr>, 
        frame: Option<WindowFrame> 
    },
}
```

### Supporting Types

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
    Date(String),      // ISO 8601 format
    Timestamp(String), // ISO 8601 format
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    // Arithmetic
    Add, Sub, Mul, Div, Mod,
    
    // Comparison
    Eq, Ne, Lt, Gt, Le, Ge,
    
    // Logical
    And, Or,
    
    // String
    Like, NotLike,
    
    // Other
    In, NotIn,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
    Negate,
}

// IMPROVED: Separate aggregate vs scalar functions
#[derive(Debug, Clone, PartialEq)]
pub enum Func {
    Aggregate(AggregateFunc),
    Scalar(ScalarFunc),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunc {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    CountDistinct,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarFunc {
    Coalesce,
    NullIf,
    Upper,
    Lower,
    Substring,
    Abs,
    Round,
    Floor,
    Ceil,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub dir: SortDir,
    pub nulls: Option<NullsOrder>,
}

// ... WindowFunc, WindowFrame, etc. (restored from archive)
```

### Updated Model Types

```rust
// src/model/measure.rs
pub struct Measure {
    pub name: String,
    pub expr: Expr,  // Changed from SqlExpr
    pub filter: Option<Expr>,  // Changed from SqlExpr
    pub null_handling: Option<NullHandling>,
}

// src/model/table.rs - Calculated slicer
pub enum Slicer {
    Inline { name: String, dimension: String },
    ForeignKey { name: String, dimension: String, key_column: String },
    Via { name: String, dimension: String, through_table: String, foreign_key: String },
    Calculated {
        name: String,
        data_type: DataType,
        expr: Expr,  // Changed from SqlExpr
    },
}

// src/model/report.rs
pub struct Report {
    pub name: String,
    pub from_tables: Vec<String>,
    pub period: Option<PeriodExpr>,
    pub group: Vec<GroupItem>,
    pub show: Vec<ShowItem>,
    pub filters: Vec<Expr>,  // Changed from SqlExpr
    pub sort: Vec<SortItem>,
    pub limit: Option<u64>,
}

pub enum ShowItem {
    Measure { name: String, suffix: Option<TimeSuffix>, label: Option<String> },
    Dimension { name: String, label: Option<String> },
    InlineMeasure {
        name: String,
        expr: Expr,  // Changed from SqlExpr
        label: Option<String>,
    },
}
```

**Remove old SqlExpr:**
- Delete `src/model/table.rs::SqlExpr` (raw string wrapper)
- Delete `src/dsl/sql_expr.rs` (optional validation module - no longer needed)

---

## Parsing Strategy

### Token Substitution Approach

```rust
// src/model/expr_parser.rs

use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast as sql;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::expr::*;
use super::expr_validation::ExprContext;
use crate::dsl::Span;

static ATOM_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"@(\w+)").unwrap());

/// Parse a SQL expression string into our Expr AST.
///
/// This function:
/// 1. Preprocesses @atom references → __ATOM__atom
/// 2. Parses SQL using sqlparser-rs
/// 3. Converts sqlparser AST → our Expr AST
/// 4. Validates expression is appropriate for context
///
/// # Arguments
/// * `sql` - The SQL expression string (may contain @atom references)
/// * `span` - Source location for error reporting
/// * `context` - Where this expression is used (Measure/Filter/CalculatedSlicer)
pub fn parse_sql_expr(
    sql: &str, 
    span: Span, 
    context: ExprContext
) -> Result<Expr, ParseError> {
    // Step 1: Preprocess @atoms
    let preprocessed = preprocess_sql_for_parsing(sql);
    
    // Step 2: Parse with sqlparser
    let dialect = GenericDialect {};
    let wrapped = format!("SELECT {}", preprocessed);
    
    let statements = Parser::parse_sql(&dialect, &wrapped)
        .map_err(|e| ParseError::SqlParseError {
            message: e.to_string(),
            span: span.clone(),
        })?;
    
    // Step 3: Extract expression from SELECT
    let sql_expr = extract_select_expr(&statements, span.clone())?;
    
    // Step 4: Convert to our AST
    let expr = convert_expr(sql_expr, span.clone())?;
    
    // Step 5: Validate context
    expr.validate_context(context)
        .map_err(|e| ParseError::SqlParseError {
            message: e.to_string(),
            span,
        })?;
    
    Ok(expr)
}

fn preprocess_sql_for_parsing(sql: &str) -> String {
    ATOM_PATTERN.replace_all(sql, "__ATOM__$1").to_string()
}
```

### Expression Conversion

```rust
fn convert_expr(sql_expr: &sql::Expr, span: Span) -> Result<Expr, ParseError> {
    match sql_expr {
        // Identifiers → AtomRef or Column
        sql::Expr::Identifier(ident) => {
            // Check if this is a marker identifier
            if let Some(atom_name) = ident.value.strip_prefix("__ATOM__") {
                Ok(Expr::AtomRef(atom_name.to_string()))
            } else {
                Ok(Expr::Column {
                    entity: None,
                    column: ident.value.clone(),
                })
            }
        }
        
        sql::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 1 {
                if let Some(atom_name) = parts[0].value.strip_prefix("__ATOM__") {
                    Ok(Expr::AtomRef(atom_name.to_string()))
                } else {
                    Ok(Expr::Column {
                        entity: None,
                        column: parts[0].value.clone(),
                    })
                }
            } else if parts.len() == 2 {
                Ok(Expr::Column {
                    entity: Some(parts[0].value.clone()),
                    column: parts[1].value.clone(),
                })
            } else {
                // schema.table.column → use last two
                let len = parts.len();
                Ok(Expr::Column {
                    entity: Some(parts[len - 2].value.clone()),
                    column: parts[len - 1].value.clone(),
                })
            }
        }
        
        // Literals
        sql::Expr::Value(val) => convert_literal(val, span),
        
        // Binary operations
        sql::Expr::BinaryOp { left, op, right } => {
            Ok(Expr::BinaryOp {
                left: Box::new(convert_expr(left, span.clone())?),
                op: convert_binary_op(op, span.clone())?,
                right: Box::new(convert_expr(right, span)?),
            })
        }
        
        // Unary operations
        sql::Expr::UnaryOp { op, expr } => {
            Ok(Expr::UnaryOp {
                op: convert_unary_op(op, span.clone())?,
                expr: Box::new(convert_expr(expr, span)?),
            })
        }
        
        // Function calls
        sql::Expr::Function(func) => convert_function(func, span),
        
        // CASE expressions
        sql::Expr::Case { operand, conditions, results, else_result } => {
            let operand_expr = operand
                .as_ref()
                .map(|e| convert_expr(e, span.clone()))
                .transpose()?
                .map(Box::new);
            
            let mut when_clauses = Vec::new();
            for (condition, result) in conditions.iter().zip(results.iter()) {
                when_clauses.push(WhenClause {
                    condition: convert_expr(condition, span.clone())?,
                    result: convert_expr(result, span.clone())?,
                });
            }
            
            let else_clause = else_result
                .as_ref()
                .map(|e| convert_expr(e, span.clone()))
                .transpose()?
                .map(Box::new);
            
            Ok(Expr::Case {
                operand: operand_expr,
                when_clauses,
                else_clause,
            })
        }
        
        // CAST expressions
        sql::Expr::Cast { expr, data_type, .. } => {
            Ok(Expr::Cast {
                expr: Box::new(convert_expr(expr, span.clone())?),
                target_type: convert_data_type(data_type, span)?,
            })
        }
        
        // Nested expressions
        sql::Expr::Nested(inner) => convert_expr(inner, span),
        
        // IS NULL / IS NOT NULL
        sql::Expr::IsNull(expr) => {
            Ok(Expr::UnaryOp {
                op: UnaryOp::IsNull,
                expr: Box::new(convert_expr(expr, span)?),
            })
        }
        
        sql::Expr::IsNotNull(expr) => {
            Ok(Expr::UnaryOp {
                op: UnaryOp::IsNotNull,
                expr: Box::new(convert_expr(expr, span)?),
            })
        }
        
        // Unsupported features
        unsupported => Err(ParseError::UnsupportedFeature {
            feature: format!("{:?}", unsupported),
            span,
        }),
    }
}
```

### Whitelisted Function Conversion

```rust
fn convert_function(func: &sql::Function, span: Span) -> Result<Expr, ParseError> {
    let func_name = func.name.to_string().to_uppercase();
    
    // Check for window function syntax
    if func.over.is_some() {
        return convert_window_function(func, span);
    }
    
    let our_func = match func_name.as_str() {
        // Aggregate functions
        "SUM" => Func::Aggregate(AggregateFunc::Sum),
        "COUNT" => {
            // Handle COUNT(*) vs COUNT(expr)
            if func.args.len() == 1 {
                match &func.args[0] {
                    sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Wildcard) => {
                        return Ok(Expr::Function {
                            func: Func::Aggregate(AggregateFunc::Count),
                            args: vec![],
                        });
                    }
                    _ => Func::Aggregate(AggregateFunc::Count),
                }
            } else {
                Func::Aggregate(AggregateFunc::Count)
            }
        }
        "AVG" => Func::Aggregate(AggregateFunc::Avg),
        "MIN" => Func::Aggregate(AggregateFunc::Min),
        "MAX" => Func::Aggregate(AggregateFunc::Max),
        
        // Scalar functions
        "COALESCE" => Func::Scalar(ScalarFunc::Coalesce),
        "NULLIF" => Func::Scalar(ScalarFunc::NullIf),
        "UPPER" => Func::Scalar(ScalarFunc::Upper),
        "LOWER" => Func::Scalar(ScalarFunc::Lower),
        "SUBSTRING" | "SUBSTR" => Func::Scalar(ScalarFunc::Substring),
        "ABS" => Func::Scalar(ScalarFunc::Abs),
        "ROUND" => Func::Scalar(ScalarFunc::Round),
        "FLOOR" => Func::Scalar(ScalarFunc::Floor),
        "CEIL" | "CEILING" => Func::Scalar(ScalarFunc::Ceil),
        
        // Unknown function
        unsupported => return Err(ParseError::UnsupportedFeature {
            feature: format!("Function '{}'", unsupported),
            span,
        }),
    };
    
    // Convert arguments
    let args = func.args.iter()
        .filter_map(|arg| match arg {
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Some(e),
            sql::FunctionArg::Named { arg: sql::FunctionArgExpr::Expr(e), .. } => Some(e),
            _ => None,
        })
        .map(|e| convert_expr(e, span.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    
    Ok(Expr::Function {
        func: our_func,
        args,
    })
}
```

### Error Types

```rust
// src/model/expr_parser.rs

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL syntax error at {span:?}: {message}")]
    SqlParseError { 
        message: String, 
        span: Span 
    },
    
    #[error("Unsupported SQL feature '{feature}' at {span:?}")]
    UnsupportedFeature { 
        feature: String, 
        span: Span 
    },
    
    #[error("Invalid number format '{value}' at {span:?}: {error}")]
    InvalidNumber {
        value: String,
        error: String,
        span: Span,
    },
    
    #[error("Invalid data type at {span:?}: {message}")]
    InvalidDataType {
        message: String,
        span: Span,
    },
}
```

---

## Expression Validation & Utilities

### Expression Context

```rust
// src/model/expr_validation.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprContext {
    Measure,           // Aggregates allowed
    Filter,            // No aggregates
    CalculatedSlicer,  // No aggregates
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Aggregate functions not allowed in {context:?} expressions")]
    AggregateNotAllowed { context: ExprContext },
    
    #[error("Undefined atom reference: @{atom}")]
    UndefinedAtom { atom: String },
    
    #[error("Undefined column reference: {column}")]
    UndefinedColumn { column: String },
}
```

### Expression Walker & Utilities

```rust
impl Expr {
    /// Extract all atom references from this expression.
    ///
    /// Returns a vector of atom names (without @ prefix).
    /// Useful for dependency analysis and validation.
    pub fn atom_refs(&self) -> Vec<String> {
        let mut refs = Vec::new();
        self.walk(&mut |expr| {
            if let Expr::AtomRef(name) = expr {
                refs.push(name.clone());
            }
        });
        refs
    }
    
    /// Extract all column references (entity.column) from this expression.
    ///
    /// Returns a vector of (entity, column) tuples where entity is None
    /// for unqualified column references.
    pub fn column_refs(&self) -> Vec<(Option<String>, String)> {
        let mut refs = Vec::new();
        self.walk(&mut |expr| {
            if let Expr::Column { entity, column } = expr {
                refs.push((entity.clone(), column.clone()));
            }
        });
        refs
    }
    
    /// Walk the expression tree, calling the visitor function on each node.
    ///
    /// Performs a depth-first traversal, visiting parent before children.
    pub fn walk<F>(&self, f: &mut F) 
    where 
        F: FnMut(&Expr) 
    {
        f(self);
        
        match self {
            Expr::BinaryOp { left, right, .. } => {
                left.walk(f);
                right.walk(f);
            }
            Expr::UnaryOp { expr, .. } => {
                expr.walk(f);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.walk(f);
                }
            }
            Expr::Case { operand, when_clauses, else_clause } => {
                if let Some(op) = operand {
                    op.walk(f);
                }
                for clause in when_clauses {
                    clause.condition.walk(f);
                    clause.result.walk(f);
                }
                if let Some(else_expr) = else_clause {
                    else_expr.walk(f);
                }
            }
            Expr::Cast { expr, .. } => {
                expr.walk(f);
            }
            Expr::Window { args, partition_by, order_by, .. } => {
                for arg in args {
                    arg.walk(f);
                }
                for part in partition_by {
                    part.walk(f);
                }
                for order in order_by {
                    order.expr.walk(f);
                }
            }
            // Terminal nodes
            Expr::AtomRef(_) | Expr::Column { .. } | Expr::Literal(_) => {}
        }
    }
    
    /// Check if this expression contains any aggregate functions.
    pub fn contains_aggregate(&self) -> bool {
        let mut has_agg = false;
        self.walk(&mut |expr| {
            if let Expr::Function { func: Func::Aggregate(_), .. } = expr {
                has_agg = true;
            }
        });
        has_agg
    }
    
    /// Validate this expression is appropriate for the given context.
    pub fn validate_context(&self, context: ExprContext) -> Result<(), ValidationError> {
        match context {
            ExprContext::Filter | ExprContext::CalculatedSlicer => {
                if self.contains_aggregate() {
                    return Err(ValidationError::AggregateNotAllowed { context });
                }
            }
            ExprContext::Measure => {
                // Measures typically should have aggregates, but not required
            }
        }
        Ok(())
    }
}
```

---

## DSL Parser Integration

### Update sql_expr Parser

**Current State:**
```rust
// Currently extracts raw SQL string
let sql_expr = just(Token::LBrace)
    .then(sql_token.repeated().collect())
    .then(just(Token::RBrace))
    .map(|((lbrace_span, tokens), rbrace_span)| {
        let sql = tokens.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>().join(" ");
        SqlExpr::new(sql, lbrace_span.start..rbrace_span.end)
    });
```

**New Approach:**
```rust
// Extract SQL string AND parse it into Expr
let sql_expr = |context: ExprContext| {
    just(Token::LBrace)
        .map_with(|_, e| to_span(e.span()))
        .then(sql_token.clone().repeated().collect::<Vec<_>>())
        .then(just(Token::RBrace).map_with(|_, e| to_span(e.span())))
        .try_map(move |((lbrace_span, tokens), rbrace_span), span| {
            // Reconstruct SQL string
            let sql = tokens
                .iter()
                .map(|(s, _)| s.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            
            // Full span from LBrace to RBrace
            let full_span = lbrace_span.start..rbrace_span.end;
            
            // Parse SQL into Expr AST
            crate::model::expr_parser::parse_sql_expr(&sql, full_span.clone(), context)
                .map_err(|e| Rich::custom(span, e.to_string()))
        })
};
```

### Update Usage Sites with Context

```rust
// Measure expressions (aggregates allowed)
let measure = ident.clone()
    .then_ignore(just(Token::Eq))
    .then(sql_expr(ExprContext::Measure).map_with(|expr, e| Spanned::new(expr, to_span(e.span()))))
    .then(
        just(Token::Where)
            .ignore_then(sql_expr(ExprContext::Filter).map_with(|expr, e| Spanned::new(expr, to_span(e.span()))))
            .or_not()
    )
    // ...

// Calculated slicer (no aggregates)
let slicer_calculated = ident.clone()
    .then(data_type.map_with(|t, e| Spanned::new(t, to_span(e.span()))))
    .then_ignore(just(Token::Eq))
    .then(sql_expr(ExprContext::CalculatedSlicer))
    // ...

// Report filters (no aggregates)
let report_filter = just(Token::Filter)
    .ignore_then(sql_expr(ExprContext::Filter).map_with(|expr, e| Spanned::new(expr, to_span(e.span()))))
    // ...

// Report inline measure (aggregates allowed)
let inline_measure = ident.clone()
    .then_ignore(just(Token::Eq))
    .then(sql_expr(ExprContext::Measure))
    // ...
```

### Update AST Types

```rust
// src/dsl/ast.rs

use crate::model::expr::Expr;  // Import from model

// Remove SqlExpr struct - no longer needed
// DELETE: pub struct SqlExpr { pub sql: String, pub span: Span }

// Update types to use Expr
pub struct Measure {
    pub name: Spanned<String>,
    pub expr: Spanned<Expr>,  // Changed from Spanned<SqlExpr>
    pub filter: Option<Spanned<Expr>>,  // Changed from Spanned<SqlExpr>
    pub null_handling: Option<Spanned<NullHandling>>,
}

pub enum SlicerKind {
    // ...
    Calculated {
        data_type: DataType,
        expr: Expr,  // Changed from SqlExpr
    },
}

pub struct Report {
    // ...
    pub filters: Vec<Spanned<Expr>>,  // Changed from Vec<Spanned<SqlExpr>>
    // ...
}

pub enum ShowItem {
    // ...
    InlineMeasure {
        name: String,
        expr: Expr,  // Changed from SqlExpr
        label: Option<String>,
    },
}
```

---

## Lowering Phase Validation

### Measure Lowering with Atom Validation

```rust
// src/lowering/measure.rs

pub fn lower_measure_block(
    ast_block: &ast::MeasureBlock,
    model: &Model,
) -> Result<model::MeasureBlock, LoweringError> {
    let table_name = ast_block.table.value.clone();
    
    // Get the table to validate atom references
    let table = model.tables.get(&table_name)
        .ok_or_else(|| LoweringError::UndefinedTable {
            name: table_name.clone(),
            span: ast_block.table.span.clone(),
        })?;
    
    let mut measures = HashMap::new();
    
    for measure_spanned in &ast_block.measures {
        let measure_ast = &measure_spanned.value;
        
        // Validate expression's atom references
        validate_atom_refs(&measure_ast.expr.value, table, &measure_ast.expr.span)?;
        
        // Validate filter's atom references (if present)
        if let Some(filter) = &measure_ast.filter {
            validate_atom_refs(&filter.value, table, &filter.span)?;
        }
        
        let measure = model::Measure {
            name: measure_ast.name.value.clone(),
            expr: measure_ast.expr.value.clone(),
            filter: measure_ast.filter.as_ref().map(|f| f.value.clone()),
            null_handling: measure_ast.null_handling.as_ref().map(|nh| nh.value),
        };
        
        measures.insert(measure.name.clone(), measure);
    }
    
    Ok(model::MeasureBlock {
        table_name,
        measures,
    })
}

/// Validate all atom references in an expression exist in the table.
fn validate_atom_refs(
    expr: &Expr,
    table: &model::Table,
    span: &Span,
) -> Result<(), LoweringError> {
    let atom_refs = expr.atom_refs();
    
    for atom_name in atom_refs {
        if !table.atoms.contains_key(&atom_name) {
            return Err(LoweringError::UndefinedAtom {
                atom: atom_name,
                table: table.name.clone(),
                span: span.clone(),
            });
        }
    }
    
    Ok(())
}
```

### Enhanced Lowering Errors

```rust
// src/lowering/error.rs

#[derive(Debug, thiserror::Error)]
pub enum LoweringError {
    #[error("Undefined table '{name}' at {span:?}")]
    UndefinedTable {
        name: String,
        span: Span,
    },
    
    #[error("Undefined atom '@{atom}' in table '{table}' at {span:?}")]
    UndefinedAtom {
        atom: String,
        table: String,
        span: Span,
    },
    
    #[error("Undefined atom '@{atom}' - not found in any FROM table: {from_tables:?}")]
    UndefinedAtomInReport {
        atom: String,
        from_tables: Vec<String>,
    },
    
    #[error("Undefined dimension '{dimension}' at {span:?}")]
    UndefinedDimension {
        dimension: String,
        span: Span,
    },
    
    // ... other lowering errors
}
```

### Validation Flow Summary

```
DSL Parse Time:
  ✓ SQL syntax validation (via sqlparser)
  ✓ Aggregate context validation (measures vs filters)
  ✓ Supported function whitelist
  ✗ Atom reference validation (can't do yet - no table context)

Lowering Time:
  ✓ Atom reference validation (have table atoms available)
  ✓ Table/dimension existence validation
  ✓ Cross-reference validation
```

---

## Graph Builder Integration

### Update DEPENDS_ON Edge Creation

**Current approach (regex-based):**
```rust
pub(crate) fn create_depends_on_edges(&mut self, model: &Model) -> GraphBuildResult<()> {
    static ATOM_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"@(\w+)").unwrap());
    
    for measure in &measure_block.measures {
        let sql = &measure.value.expr.value.sql;  // Raw SQL string
        for cap in ATOM_PATTERN.captures_iter(sql) {
            let atom_name = &cap[1];
            // Create edge...
        }
    }
}
```

**New approach (AST-based):**
```rust
pub(crate) fn create_depends_on_edges(&mut self, model: &Model) -> GraphBuildResult<()> {
    for item in &model.items {
        if let Item::MeasureBlock(measure_block) = &item.value {
            let entity_name = &measure_block.table.value;
            
            for measure in &measure_block.measures {
                let measure_name = &measure.value.name.value;
                let qualified_measure = format!("{}.{}", entity_name, measure_name);
                
                let measure_idx = self.measure_index.get(&qualified_measure)
                    .ok_or_else(|| GraphBuildError::InvalidReference(qualified_measure.clone()))?;
                
                // Extract atom references from Expr AST
                let atom_refs = measure.value.expr.value.atom_refs();
                
                // Create edges for each referenced atom
                for atom_name in atom_refs {
                    let qualified_col = format!("{}.{}", entity_name, atom_name);
                    
                    if let Some(col_idx) = self.column_index.get(&qualified_col) {
                        let edge = GraphEdge::DependsOn(DependsOnEdge {
                            measure: qualified_measure.clone(),
                            columns: vec![qualified_col],
                        });
                        
                        self.graph.add_edge(*measure_idx, *col_idx, edge);
                    } else {
                        return Err(GraphBuildError::ColumnNotFound(qualified_col));
                    }
                }
                
                // Also handle filter dependencies
                if let Some(filter) = &measure.value.filter {
                    let filter_atom_refs = filter.value.atom_refs();
                    for atom_name in filter_atom_refs {
                        let qualified_col = format!("{}.{}", entity_name, atom_name);
                        if let Some(col_idx) = self.column_index.get(&qualified_col) {
                            let edge = GraphEdge::DependsOn(DependsOnEdge {
                                measure: qualified_measure.clone(),
                                columns: vec![qualified_col],
                            });
                            self.graph.add_edge(*measure_idx, *col_idx, edge);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
```

### Update MeasureNode

```rust
// src/semantic/graph/types.rs

#[derive(Debug, Clone)]
pub struct MeasureNode {
    pub name: String,
    pub entity: String,
    pub aggregation: String,  // "SUM", "COUNT", "AVG", etc.
    pub source_column: Option<String>,
    pub expression: Option<Expr>,  // Changed from Option<String>
    pub metadata: HashMap<String, String>,
}
```

### Update Measure Node Creation

```rust
// src/semantic/graph/builder.rs

pub(crate) fn create_measure_nodes(&mut self, model: &Model) -> GraphBuildResult<()> {
    for item in &model.items {
        if let Item::MeasureBlock(measure_block) = &item.value {
            let entity_name = &measure_block.table.value;
            
            for measure in &measure_block.measures {
                let measure_name = &measure.value.name.value;
                let qualified_name = format!("{}.{}", entity_name, measure_name);
                
                // Extract aggregation type from expression
                let aggregation = extract_aggregation_type(&measure.value.expr.value);
                
                let measure_node = MeasureNode {
                    name: measure_name.clone(),
                    entity: entity_name.clone(),
                    aggregation,
                    source_column: None,
                    expression: Some(measure.value.expr.value.clone()),  // Store Expr AST
                    metadata: HashMap::new(),
                };
                
                let measure_idx = self.graph.add_node(GraphNode::Measure(measure_node));
                self.measure_index.insert(qualified_name.clone(), measure_idx);
                self.node_index.insert(qualified_name, measure_idx);
            }
        }
    }
    Ok(())
}

fn extract_aggregation_type(expr: &Expr) -> String {
    match expr {
        Expr::Function { func: Func::Aggregate(agg), .. } => {
            match agg {
                AggregateFunc::Sum => "SUM".to_string(),
                AggregateFunc::Count => "COUNT".to_string(),
                AggregateFunc::Avg => "AVG".to_string(),
                AggregateFunc::Min => "MIN".to_string(),
                AggregateFunc::Max => "MAX".to_string(),
                AggregateFunc::CountDistinct => "COUNT_DISTINCT".to_string(),
            }
        }
        _ => "CUSTOM".to_string(),
    }
}
```

**Remove regex dependency:** The `ATOM_PATTERN` regex is no longer needed in `builder.rs`.

---

## Comprehensive Testing Suite

### Testing Strategy

This marks the completion of the first full version of the DSL parser. We'll add production-grade testing:

1. **Unit tests** - Individual parser functions (10-15 tests)
2. **Snapshot tests** - Capture complete AST structure using `insta` (50+ tests)
3. **Corpus tests** - Real-world DSL examples (10+ files)
4. **Error message tests** - Validate helpful error output (10-15 tests)
5. **Integration tests** - Full pipeline (parse → lower → graph)

### Test Directory Structure

```
tests/
  dsl_parser/
    snapshots/           # Insta snapshot files (.snap)
    corpus/              # Real-world DSL example files (.mantis)
      invalid/           # Files that should fail parsing
    snapshot_tests.rs    # Snapshot test suite
    corpus_tests.rs      # Corpus test suite
    integration_tests.rs # Full pipeline tests
```

### Snapshot Testing

```rust
// tests/dsl_parser/snapshot_tests.rs

use insta::assert_debug_snapshot;
use mantis::dsl;

#[test]
fn snapshot_complete_model() {
    let input = r#"
        defaults {
            calendar fiscal_calendar;
            fiscal_year_start april;
        }
        
        calendar fiscal_calendar {
            generate day;
            range 2020-01-01 to 2026-12-31;
        }
        
        dimension customers {
            source "dbo.dim_customers";
            key customer_id;
            attributes {
                customer_name string;
                customer_segment string;
            }
        }
        
        table sales {
            source "dbo.fact_sales";
            atoms {
                revenue decimal;
                quantity int;
            }
            times {
                order_date at fiscal_calendar as day;
            }
            slicers {
                customer_id -> customers.customer_id;
            }
        }
        
        measures sales {
            total_revenue = { SUM(@revenue) };
            avg_price = { SUM(@revenue) / NULLIF(SUM(@quantity), 0) };
        }
        
        report sales_summary {
            from sales;
            period last 12 months;
            group by customer_segment;
            show total_revenue, avg_price;
            filter { @revenue > 0 };
            limit 100;
        }
    "#;
    
    let ast = dsl::parse(input).expect("Parse failed");
    assert_debug_snapshot!("complete_model", ast);
}

#[test]
fn snapshot_sql_expressions() {
    let test_cases = vec![
        ("simple_atom", "{ @revenue }"),
        ("simple_aggregate", "{ SUM(@revenue) }"),
        ("division_with_nullif", "{ SUM(@revenue) / NULLIF(SUM(@quantity), 0) }"),
        ("case_expression", "{ CASE WHEN @status = 'active' THEN @revenue ELSE 0 END }"),
        ("complex_nested", "{ SUM(CASE WHEN @region = 'US' THEN @revenue * (1 - @discount / 100) ELSE @revenue END) }"),
    ];
    
    for (name, sql_input) in test_cases {
        let input = format!(
            r#"
            table test {{
                source "test";
                atoms {{ revenue decimal; quantity int; discount decimal; }}
                slicers {{ calc string = {}; }}
            }}
            "#,
            sql_input
        );
        
        let ast = dsl::parse(&input).expect("Parse failed");
        assert_debug_snapshot!(name, ast);
    }
}

#[test]
fn snapshot_all_slicer_types() {
    let input = r#"
        dimension customers {
            source "dbo.dim_customers";
            key customer_id;
        }
        
        table orders {
            source "dbo.fact_orders";
            atoms { amount decimal; }
            slicers {
                customer_id -> customers.customer_id;
                infer product_category;
                delivery_status via shipments.order_id -> statuses.status_id;
                amount_bucket string = { CASE WHEN @amount < 100 THEN 'small' ELSE 'large' END };
            }
        }
    "#;
    
    let ast = dsl::parse(input).expect("Parse failed");
    assert_debug_snapshot!("all_slicer_types", ast);
}
```

### Corpus Testing

```rust
// tests/dsl_parser/corpus_tests.rs

use std::fs;
use std::path::PathBuf;

#[test]
fn test_all_corpus_files_parse() {
    let corpus_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/dsl_parser/corpus");
    
    let mut tested = 0;
    
    for entry in fs::read_dir(&corpus_dir).expect("Failed to read corpus dir") {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) != Some("mantis") {
            continue;
        }
        
        let filename = path.file_name().unwrap().to_str().unwrap();
        let content = fs::read_to_string(&path)
            .expect(&format!("Failed to read {}", filename));
        
        let result = mantis::dsl::parse(&content);
        
        assert!(
            result.is_ok(),
            "Failed to parse corpus file {}: {:?}",
            filename,
            result.err()
        );
        
        tested += 1;
    }
    
    assert!(tested > 0, "No corpus files found in {:?}", corpus_dir);
    println!("✓ Successfully parsed {} corpus files", tested);
}

#[test]
fn test_corpus_full_pipeline() {
    let corpus_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/dsl_parser/corpus");
    
    for entry in fs::read_dir(&corpus_dir).expect("Failed to read corpus dir") {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) != Some("mantis") {
            continue;
        }
        
        let filename = path.file_name().unwrap().to_str().unwrap();
        let content = fs::read_to_string(&path)
            .expect(&format!("Failed to read {}", filename));
        
        // Parse
        let ast = mantis::dsl::parse(&content)
            .expect(&format!("Parse failed for {}", filename));
        
        // Lower
        let model = mantis::lowering::lower(ast)
            .expect(&format!("Lowering failed for {}", filename));
        
        // Validate we can extract SQL expressions
        for (table_name, measure_block) in &model.measures {
            for (measure_name, measure) in &measure_block.measures {
                let atom_refs = measure.expr.atom_refs();
                println!("{}.{} references atoms: {:?}", table_name, measure_name, atom_refs);
            }
        }
    }
}
```

### Example Corpus Files

Create at least 10 corpus files covering:

1. `01-simple-sales.mantis` - Basic sales model
2. `02-ecommerce-full.mantis` - Complete e-commerce model
3. `03-saas-metrics.mantis` - SaaS KPIs and cohorts
4. `04-financial-reporting.mantis` - Complex financial measures
5. `05-retail-inventory.mantis` - Multi-dimensional retail
6. `06-healthcare-analytics.mantis` - Healthcare specific patterns
7. `07-generated-calendars.mantis` - Calendar-heavy model
8. `08-complex-slicers.mantis` - All slicer type examples
9. `09-nested-case-expressions.mantis` - Complex SQL expressions
10. `10-multi-grain-analysis.mantis` - Multiple time grains

Plus invalid examples:
- `invalid/missing-semicolon.mantis`
- `invalid/undefined-atom.mantis`
- `invalid/invalid-sql-syntax.mantis`
- `invalid/duplicate-names.mantis`

---

## Migration Plan

### Phase 1: Add New Code (Non-Breaking)

1. Restore `src/model/expr.rs` from archive with `AtomRef` variant added
2. Add `src/model/expr_parser.rs` with parsing logic
3. Add `src/model/expr_validation.rs` with utilities
4. Update `Cargo.toml` to ensure sqlparser dependency exists (already present)
5. Add `Cargo.toml` dev-dependency: `insta = "1.41"` for snapshot testing
6. Run tests on new modules in isolation

### Phase 2: Update DSL Parser

1. Update `src/dsl/ast.rs` types to use `Expr` instead of `SqlExpr`
2. Update `src/dsl/parser.rs` to parse SQL into `Expr` with context
3. Fix compilation errors in parser
4. Update parser tests

### Phase 3: Update Lowering

1. Update `src/lowering/measure.rs` with atom validation
2. Update `src/lowering/table.rs` with calculated slicer validation
3. Update `src/lowering/report.rs` with inline measure validation
4. Add new error variants to `src/lowering/error.rs`
5. Update lowering tests

### Phase 4: Update Model Types

1. Update `src/model/measure.rs` to use `Expr`
2. Update `src/model/table.rs` to use `Expr` (and remove `SqlExpr`)
3. Update `src/model/report.rs` to use `Expr`
4. Fix compilation errors

### Phase 5: Update Graph Builder

1. Update `src/semantic/graph/types.rs` - `MeasureNode` stores `Expr`
2. Update `src/semantic/graph/builder.rs` - use `atom_refs()` instead of regex
3. Remove `ATOM_PATTERN` regex
4. Update graph integration tests

### Phase 6: Cleanup

1. Delete `src/dsl/sql_expr.rs` (no longer needed)
2. Delete `src/model/table.rs::SqlExpr` struct
3. Remove unused imports
4. Run existing test suite
5. Update documentation

### Phase 7: Comprehensive Testing Suite

1. Create `tests/dsl_parser/` directory structure
2. Create `tests/dsl_parser/corpus/` with 10+ `.mantis` example files
3. Create `tests/dsl_parser/corpus/invalid/` with error case files
4. Add `tests/dsl_parser/snapshot_tests.rs` (50+ snapshot tests)
5. Add `tests/dsl_parser/corpus_tests.rs` (corpus runner)
6. Run snapshot tests and review/accept generated `.snap` files
7. Verify all corpus files parse and lower successfully
8. Verify invalid corpus files fail with helpful errors

### Phase 8: Commit

1. Commit with message: `feat(sql): replace regex parsing with AST-based SQL parsing + comprehensive DSL testing`
2. Include detailed commit message explaining:
   - SQL expression parsing improvements
   - Comprehensive testing suite (snapshot + corpus)
   - First complete version of DSL parser

### Rollback Plan

If issues arise, changes can be rolled back phase-by-phase since each phase is a logical unit. Git history shows clear boundaries between phases.

---

## Success Criteria

✓ All existing tests pass  
✓ New parser unit tests cover edge cases  
✓ **50+ snapshot tests capture full AST structure**  
✓ **10+ corpus files parse and lower successfully**  
✓ **Invalid corpus files fail with helpful errors**  
✓ **Full pipeline (parse → lower → graph) works on all corpus files**  
✓ LSP gets immediate SQL syntax feedback  
✓ Lowering validates atom references with good error messages  
✓ Graph builder correctly extracts dependencies using AST  
✓ No regex-based SQL parsing remains  
✓ Documentation updated  

---

## File Checklist

### New Files

- [ ] `src/model/expr.rs` (restored from archive + AtomRef)
- [ ] `src/model/expr_parser.rs`
- [ ] `src/model/expr_validation.rs`
- [ ] `tests/dsl_parser/snapshot_tests.rs`
- [ ] `tests/dsl_parser/corpus_tests.rs`
- [ ] `tests/dsl_parser/corpus/*.mantis` (10+ files)
- [ ] `tests/dsl_parser/corpus/invalid/*.mantis` (4+ files)

### Modified Files

- [ ] `src/dsl/ast.rs` (use Expr instead of SqlExpr)
- [ ] `src/dsl/parser.rs` (parse SQL to Expr with context)
- [ ] `src/model/measure.rs` (use Expr)
- [ ] `src/model/table.rs` (use Expr for calculated slicers, remove SqlExpr)
- [ ] `src/model/report.rs` (use Expr)
- [ ] `src/lowering/measure.rs` (add atom validation)
- [ ] `src/lowering/table.rs` (add calculated slicer validation)
- [ ] `src/lowering/report.rs` (add inline measure validation)
- [ ] `src/lowering/error.rs` (add new error variants)
- [ ] `src/semantic/graph/types.rs` (MeasureNode uses Expr)
- [ ] `src/semantic/graph/builder.rs` (use atom_refs(), remove regex)
- [ ] `Cargo.toml` (add insta dev-dependency)

### Deleted Files

- [ ] `src/dsl/sql_expr.rs`

---

## Next Steps After This Design

Once this SQL expression parsing is complete:

1. **LSP Integration** - Use the parser for real-time SQL validation
2. **Query Planner** - Use the Expr AST for SQL generation
3. **Type Inference** - Infer data types from expressions
4. **Optimization** - Rewrite expressions for performance

This design represents the foundation for all SQL-related functionality in the semantic layer.
