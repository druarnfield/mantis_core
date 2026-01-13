//! Expression AST - the core of SQL expression building.
//!
//! This module provides a strongly-typed AST for SQL expressions
//! with exhaustive pattern matching enforced by the compiler.

use super::dialect::{Dialect, SqlDialect};
use super::token::{Token, TokenStream};

// =============================================================================
// Expression AST
// =============================================================================

/// A SQL expression.
///
/// Every variant must be handled in `to_tokens()` - the compiler enforces this.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference: optional_table.column
    Column {
        table: Option<String>,
        column: String,
    },

    /// Literal values
    Literal(Literal),

    /// Binary operation: left op right
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },

    /// Unary operation: op expr
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },

    /// Function call: name(args...)
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },

    /// CASE WHEN... THEN... ELSE... END
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },

    /// Subquery: (SELECT ...)
    Subquery(Box<crate::query::Query>),

    /// IN: expr IN (values...)
    In {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },

    /// IN subquery: expr IN (SELECT ...)
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<crate::query::Query>,
        negated: bool,
    },

    /// BETWEEN: expr BETWEEN low AND high
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },

    /// LIKE with ESCAPE: expr LIKE pattern ESCAPE escape_char
    LikeEscape {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: char,
        negated: bool,
    },

    /// Wildcard: * or table.*
    Star { table: Option<String> },

    /// Parenthesized expression
    Paren(Box<Expr>),

    /// Window function expression.
    ///
    /// Example: `SUM(amount) OVER (PARTITION BY region ORDER BY date ROWS UNBOUNDED PRECEDING)`
    WindowFunction {
        /// The function being windowed (usually Expr::Function)
        function: Box<Expr>,
        /// PARTITION BY expressions
        partition_by: Vec<Expr>,
        /// ORDER BY within window
        order_by: Vec<WindowOrderBy>,
        /// Optional frame specification
        frame: Option<WindowFrame>,
    },

    /// Raw SQL expression passed directly to output without escaping.
    ///
    /// # Security Warning
    ///
    /// **Never pass user input to this variant.** Raw SQL is not sanitized
    /// and can lead to SQL injection vulnerabilities. Only use with:
    /// - Trusted, static SQL fragments
    /// - Dialect-specific syntax not covered by structured expressions
    ///
    /// For user-provided values, use `Expr::Literal` variants which properly
    /// escape content for the target dialect.
    ///
    /// TODO(expr-parsing): Replace Raw usages with proper expression parsing.
    /// Tracked usages:
    /// - semantic/planner/report.rs: parse_simple_filter() for report filters
    Raw(String),
}

/// Literal values.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Ne,
    Lt,
    Gt,
    Lte,
    Gte,
    // Logical
    And,
    Or,
    // Arithmetic
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    // String
    Concat,
    Like,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Minus,
}

// =============================================================================
// Window Function Types
// =============================================================================

/// ORDER BY expression within a window specification.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowOrderBy {
    pub expr: Expr,
    pub dir: Option<SortDir>,
    pub nulls: Option<NullsOrder>,
}

impl WindowOrderBy {
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            dir: None,
            nulls: None,
        }
    }

    pub fn asc(expr: Expr) -> Self {
        Self {
            expr,
            dir: Some(SortDir::Asc),
            nulls: None,
        }
    }

    pub fn desc(expr: Expr) -> Self {
        Self {
            expr,
            dir: Some(SortDir::Desc),
            nulls: None,
        }
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls = Some(NullsOrder::First);
        self
    }

    pub fn nulls_last(mut self) -> Self {
        self.nulls = Some(NullsOrder::Last);
        self
    }
}

/// Sort direction (shared with query ORDER BY).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDir {
    #[default]
    Asc,
    Desc,
}

/// NULLS ordering (shared with query ORDER BY).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

/// Window frame specification.
///
/// Examples:
/// - `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
/// - `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING`
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

impl WindowFrame {
    /// Create a frame with just a start bound.
    pub fn new(kind: WindowFrameKind, start: WindowFrameBound) -> Self {
        Self {
            kind,
            start,
            end: None,
        }
    }

    /// Create a frame with BETWEEN start AND end.
    pub fn between(kind: WindowFrameKind, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        Self {
            kind,
            start,
            end: Some(end),
        }
    }

    /// ROWS UNBOUNDED PRECEDING (running aggregate)
    pub fn rows_unbounded_preceding() -> Self {
        Self::new(WindowFrameKind::Rows, WindowFrameBound::UnboundedPreceding)
    }

    /// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    pub fn rows_to_current() -> Self {
        Self::between(
            WindowFrameKind::Rows,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::CurrentRow,
        )
    }

    /// ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (entire partition)
    pub fn rows_entire_partition() -> Self {
        Self::between(
            WindowFrameKind::Rows,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::UnboundedFollowing,
        )
    }

    /// Rolling window: ROWS BETWEEN (periods-1) PRECEDING AND CURRENT ROW
    ///
    /// For a 3-period rolling window, includes: current row + 2 preceding.
    pub fn rolling(periods: u32) -> Self {
        let preceding = periods.saturating_sub(1);
        Self::between(
            WindowFrameKind::Rows,
            WindowFrameBound::Preceding(preceding as u64),
            WindowFrameBound::CurrentRow,
        )
    }
}

/// Frame type: ROWS, RANGE, or GROUPS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameKind {
    /// ROWS - physical row offsets
    Rows,
    /// RANGE - logical value ranges
    Range,
    /// GROUPS - peer groups (PostgreSQL/DuckDB only)
    Groups,
}

/// Frame boundary specification.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// n PRECEDING
    Preceding(u64),
    /// CURRENT ROW
    CurrentRow,
    /// n FOLLOWING
    Following(u64),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
}

// =============================================================================
// Expression to Tokens
// =============================================================================

impl Expr {
    /// Convert this expression to a token stream (dialect-agnostic).
    pub fn to_tokens(&self) -> TokenStream {
        self.to_tokens_for_dialect(Dialect::default())
    }

    /// Convert this expression to a token stream for a specific dialect.
    ///
    /// This handles dialect-specific features like GROUPS frame support.
    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        match self {
            Expr::Column { table, column } => {
                if let Some(t) = table {
                    ts.push(Token::Ident(t.clone()));
                    ts.push(Token::Dot);
                }
                ts.push(Token::Ident(column.clone()));
            }

            Expr::Literal(lit) => {
                ts.push(match lit {
                    Literal::Int(n) => Token::LitInt(*n),
                    Literal::Float(f) => Token::LitFloat(*f),
                    Literal::String(s) => Token::LitString(s.clone()),
                    Literal::Bool(b) => Token::LitBool(*b),
                    Literal::Null => Token::LitNull,
                });
            }

            Expr::BinaryOp { left, op, right } => {
                // Handle CONCAT specially for dialects that don't support || operator
                if *op == BinaryOperator::Concat && !dialect.supports_concat_operator() {
                    // Emit CONCAT(left, right) function instead
                    ts.push(Token::FunctionName("CONCAT".into()));
                    ts.lparen();
                    ts.append(&left.to_tokens_for_dialect(dialect));
                    ts.comma().space();
                    ts.append(&right.to_tokens_for_dialect(dialect));
                    ts.rparen();
                } else {
                    ts.append(&left.to_tokens_for_dialect(dialect));
                    ts.space();
                    ts.push(binary_op_to_token(*op));
                    ts.space();
                    ts.append(&right.to_tokens_for_dialect(dialect));
                }
            }

            Expr::UnaryOp { op, expr } => {
                ts.push(match op {
                    UnaryOperator::Not => Token::Not,
                    UnaryOperator::Minus => Token::Minus,
                });
                ts.space();
                ts.append(&expr.to_tokens());
            }

            Expr::Function {
                name,
                args,
                distinct,
            } => {
                ts.push(Token::FunctionName(name.clone()));
                ts.lparen();
                if *distinct {
                    ts.push(Token::Distinct).space();
                }
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.append(&arg.to_tokens());
                }
                ts.rparen();
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                ts.push(Token::Case);
                if let Some(op) = operand {
                    ts.space().append(&op.to_tokens());
                }
                for (when, then) in when_clauses {
                    ts.space().push(Token::When).space();
                    ts.append(&when.to_tokens());
                    ts.space().push(Token::Then).space();
                    ts.append(&then.to_tokens());
                }
                if let Some(else_expr) = else_clause {
                    ts.space().push(Token::Else).space();
                    ts.append(&else_expr.to_tokens());
                }
                ts.space().push(Token::End);
            }

            Expr::Subquery(query) => {
                ts.lparen();
                ts.append(&query.to_tokens());
                ts.rparen();
            }

            Expr::In {
                expr,
                values,
                negated,
            } => {
                // Empty IN list: "x IN ()" is invalid SQL
                // "x IN ()" should be FALSE, "x NOT IN ()" should be TRUE
                if values.is_empty() {
                    ts.push(if *negated { Token::True } else { Token::False });
                } else {
                    ts.append(&expr.to_tokens());
                    if *negated {
                        ts.space().push(Token::Not);
                    }
                    ts.space().push(Token::In).space().lparen();
                    for (i, val) in values.iter().enumerate() {
                        if i > 0 {
                            ts.comma().space();
                        }
                        ts.append(&val.to_tokens());
                    }
                    ts.rparen();
                }
            }

            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                ts.append(&expr.to_tokens());
                if *negated {
                    ts.space().push(Token::Not);
                }
                ts.space().push(Token::In).space().lparen();
                ts.append(&subquery.to_tokens());
                ts.rparen();
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                ts.append(&expr.to_tokens());
                if *negated {
                    ts.space().push(Token::Not);
                }
                ts.space().push(Token::Between).space();
                ts.append(&low.to_tokens());
                ts.space().push(Token::And).space();
                ts.append(&high.to_tokens());
            }

            Expr::IsNull { expr, negated } => {
                ts.append(&expr.to_tokens());
                ts.space();
                ts.push(if *negated {
                    Token::IsNotNull
                } else {
                    Token::IsNull
                });
            }

            Expr::LikeEscape {
                expr,
                pattern,
                escape_char,
                negated,
            } => {
                ts.append(&expr.to_tokens_for_dialect(dialect));
                if *negated {
                    ts.space().push(Token::Not);
                }
                ts.space()
                    .push(Token::Like)
                    .space()
                    .append(&pattern.to_tokens_for_dialect(dialect))
                    .space()
                    .push(Token::Raw("ESCAPE".into()))
                    .space()
                    .push(Token::LitString(escape_char.to_string()));
            }

            Expr::Star { table } => {
                if let Some(t) = table {
                    ts.push(Token::Ident(t.clone()));
                    ts.push(Token::Dot);
                }
                ts.push(Token::Star);
            }

            Expr::Paren(inner) => {
                ts.lparen();
                ts.append(&inner.to_tokens());
                ts.rparen();
            }

            Expr::WindowFunction {
                function,
                partition_by,
                order_by,
                frame,
            } => {
                // Emit the function first
                ts.append(&function.to_tokens_for_dialect(dialect));

                // OVER (...)
                ts.space().push(Token::Over).space().lparen();

                let mut need_space = false;

                // PARTITION BY
                if !partition_by.is_empty() {
                    ts.push(Token::PartitionBy).space();
                    for (i, expr) in partition_by.iter().enumerate() {
                        if i > 0 {
                            ts.comma().space();
                        }
                        ts.append(&expr.to_tokens_for_dialect(dialect));
                    }
                    need_space = true;
                }

                // ORDER BY
                if !order_by.is_empty() {
                    if need_space {
                        ts.space();
                    }
                    ts.push(Token::OrderBy).space();
                    for (i, ob) in order_by.iter().enumerate() {
                        if i > 0 {
                            ts.comma().space();
                        }
                        ts.append(&ob.expr.to_tokens_for_dialect(dialect));
                        if let Some(dir) = &ob.dir {
                            ts.space().push(match dir {
                                SortDir::Asc => Token::Asc,
                                SortDir::Desc => Token::Desc,
                            });
                        }
                        if let Some(nulls) = &ob.nulls {
                            ts.space().push(match nulls {
                                NullsOrder::First => Token::NullsFirst,
                                NullsOrder::Last => Token::NullsLast,
                            });
                        }
                    }
                    need_space = true;
                }

                // Frame specification
                if let Some(f) = frame {
                    if need_space {
                        ts.space();
                    }
                    // Frame kind - fall back to ROWS if GROUPS not supported
                    ts.push(match f.kind {
                        WindowFrameKind::Rows => Token::Rows,
                        WindowFrameKind::Range => Token::Range,
                        WindowFrameKind::Groups => {
                            if dialect.supports_groups_frame() {
                                Token::Groups
                            } else {
                                // GROUPS not supported, fall back to ROWS
                                Token::Rows
                            }
                        }
                    });
                    ts.space();

                    // If we have an end bound, emit BETWEEN
                    if f.end.is_some() {
                        ts.push(Token::Between).space();
                    }

                    // Start bound
                    emit_frame_bound(&mut ts, &f.start);

                    // End bound
                    if let Some(ref end) = f.end {
                        ts.space().push(Token::And).space();
                        emit_frame_bound(&mut ts, end);
                    }
                }

                ts.rparen();
            }

            Expr::Raw(sql) => {
                ts.push(Token::Raw(sql.clone()));
            }
        }

        ts
    }
}

fn binary_op_to_token(op: BinaryOperator) -> Token {
    match op {
        BinaryOperator::Eq => Token::Eq,
        BinaryOperator::Ne => Token::Ne,
        BinaryOperator::Lt => Token::Lt,
        BinaryOperator::Gt => Token::Gt,
        BinaryOperator::Lte => Token::Lte,
        BinaryOperator::Gte => Token::Gte,
        BinaryOperator::And => Token::And,
        BinaryOperator::Or => Token::Or,
        BinaryOperator::Plus => Token::Plus,
        BinaryOperator::Minus => Token::Minus,
        BinaryOperator::Mul => Token::Mul,
        BinaryOperator::Div => Token::Div,
        BinaryOperator::Mod => Token::Mod,
        BinaryOperator::Concat => Token::Concat,
        BinaryOperator::Like => Token::Like,
    }
}

/// Emit a window frame bound to a token stream.
fn emit_frame_bound(ts: &mut TokenStream, bound: &WindowFrameBound) {
    match bound {
        WindowFrameBound::UnboundedPreceding => {
            ts.push(Token::Unbounded).space().push(Token::Preceding);
        }
        WindowFrameBound::Preceding(n) => {
            ts.push(Token::LitInt(*n as i64))
                .space()
                .push(Token::Preceding);
        }
        WindowFrameBound::CurrentRow => {
            ts.push(Token::CurrentRow);
        }
        WindowFrameBound::Following(n) => {
            ts.push(Token::LitInt(*n as i64))
                .space()
                .push(Token::Following);
        }
        WindowFrameBound::UnboundedFollowing => {
            ts.push(Token::Unbounded).space().push(Token::Following);
        }
    }
}

// =============================================================================
// Expression Constructors
// =============================================================================

/// Create a column reference.
pub fn col(name: &str) -> Expr {
    Expr::Column {
        table: None,
        column: name.into(),
    }
}

/// Create a qualified column reference (table.column).
pub fn table_col(table: &str, column: &str) -> Expr {
    Expr::Column {
        table: Some(table.into()),
        column: column.into(),
    }
}

/// Create an integer literal.
pub fn lit_int(n: i64) -> Expr {
    Expr::Literal(Literal::Int(n))
}

/// Create a float literal.
pub fn lit_float(f: f64) -> Expr {
    Expr::Literal(Literal::Float(f))
}

/// Create a string literal.
pub fn lit_str(s: &str) -> Expr {
    Expr::Literal(Literal::String(s.into()))
}

/// Create a boolean literal.
pub fn lit_bool(b: bool) -> Expr {
    Expr::Literal(Literal::Bool(b))
}

/// Create a NULL literal.
pub fn lit_null() -> Expr {
    Expr::Literal(Literal::Null)
}

/// Create a star (*) expression.
pub fn star() -> Expr {
    Expr::Star { table: None }
}

/// Create a qualified star (table.*) expression.
pub fn table_star(table: &str) -> Expr {
    Expr::Star {
        table: Some(table.into()),
    }
}

// =============================================================================
// Aggregate Functions
// =============================================================================

/// COUNT(expr)
pub fn count(expr: Expr) -> Expr {
    Expr::Function {
        name: "COUNT".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// COUNT(*)
pub fn count_star() -> Expr {
    Expr::Function {
        name: "COUNT".into(),
        args: vec![star()],
        distinct: false,
    }
}

/// COUNT(DISTINCT expr)
pub fn count_distinct(expr: Expr) -> Expr {
    Expr::Function {
        name: "COUNT".into(),
        args: vec![expr],
        distinct: true,
    }
}

/// SUM(expr)
pub fn sum(expr: Expr) -> Expr {
    Expr::Function {
        name: "SUM".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// AVG(expr)
pub fn avg(expr: Expr) -> Expr {
    Expr::Function {
        name: "AVG".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// MIN(expr)
pub fn min(expr: Expr) -> Expr {
    Expr::Function {
        name: "MIN".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// MAX(expr)
pub fn max(expr: Expr) -> Expr {
    Expr::Function {
        name: "MAX".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// COALESCE(args...)
pub fn coalesce(args: Vec<Expr>) -> Expr {
    Expr::Function {
        name: "COALESCE".into(),
        args,
        distinct: false,
    }
}

/// Generic function call.
pub fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function {
        name: name.into(),
        args,
        distinct: false,
    }
}

// =============================================================================
// Window Functions
// =============================================================================

/// ROW_NUMBER() - assigns sequential row numbers.
pub fn row_number() -> Expr {
    Expr::Function {
        name: "ROW_NUMBER".into(),
        args: vec![],
        distinct: false,
    }
}

/// RANK() - assigns rank with gaps for ties.
pub fn rank() -> Expr {
    Expr::Function {
        name: "RANK".into(),
        args: vec![],
        distinct: false,
    }
}

/// DENSE_RANK() - assigns rank without gaps.
pub fn dense_rank() -> Expr {
    Expr::Function {
        name: "DENSE_RANK".into(),
        args: vec![],
        distinct: false,
    }
}

/// NTILE(n) - divides rows into n groups.
pub fn ntile(n: u64) -> Expr {
    Expr::Function {
        name: "NTILE".into(),
        args: vec![lit_int(n as i64)],
        distinct: false,
    }
}

/// LAG(expr) - access previous row value.
pub fn lag(expr: Expr) -> Expr {
    Expr::Function {
        name: "LAG".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// LAG(expr, offset) - access previous row value with offset.
pub fn lag_offset(expr: Expr, offset: i64) -> Expr {
    Expr::Function {
        name: "LAG".into(),
        args: vec![expr, lit_int(offset)],
        distinct: false,
    }
}

/// LAG(expr, offset, default) - access previous row value with offset and default.
pub fn lag_default(expr: Expr, offset: i64, default: Expr) -> Expr {
    Expr::Function {
        name: "LAG".into(),
        args: vec![expr, lit_int(offset), default],
        distinct: false,
    }
}

/// LEAD(expr) - access next row value.
pub fn lead(expr: Expr) -> Expr {
    Expr::Function {
        name: "LEAD".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// LEAD(expr, offset) - access next row value with offset.
pub fn lead_offset(expr: Expr, offset: i64) -> Expr {
    Expr::Function {
        name: "LEAD".into(),
        args: vec![expr, lit_int(offset)],
        distinct: false,
    }
}

/// LEAD(expr, offset, default) - access next row value with offset and default.
pub fn lead_default(expr: Expr, offset: i64, default: Expr) -> Expr {
    Expr::Function {
        name: "LEAD".into(),
        args: vec![expr, lit_int(offset), default],
        distinct: false,
    }
}

/// FIRST_VALUE(expr) - first value in window.
pub fn first_value(expr: Expr) -> Expr {
    Expr::Function {
        name: "FIRST_VALUE".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// LAST_VALUE(expr) - last value in window.
pub fn last_value(expr: Expr) -> Expr {
    Expr::Function {
        name: "LAST_VALUE".into(),
        args: vec![expr],
        distinct: false,
    }
}

/// NTH_VALUE(expr, n) - nth value in window.
pub fn nth_value(expr: Expr, n: i64) -> Expr {
    Expr::Function {
        name: "NTH_VALUE".into(),
        args: vec![expr, lit_int(n)],
        distinct: false,
    }
}

/// PERCENT_RANK() - relative rank as percentage.
pub fn percent_rank() -> Expr {
    Expr::Function {
        name: "PERCENT_RANK".into(),
        args: vec![],
        distinct: false,
    }
}

/// CUME_DIST() - cumulative distribution.
pub fn cume_dist() -> Expr {
    Expr::Function {
        name: "CUME_DIST".into(),
        args: vec![],
        distinct: false,
    }
}

/// Raw SQL expression (pass-through, no parsing).
///
/// # Security Warning
///
/// **Never pass user input to this function.** The SQL is not sanitized
/// and can lead to SQL injection vulnerabilities.
///
/// Use this sparingly for dialect-specific syntax that isn't covered by the builder.
///
/// # Example
/// ```ignore
/// raw_sql("CURRENT_TIMESTAMP")
/// raw_sql("DATE_TRUNC('month', order_date)")
/// ```
pub fn raw_sql(sql: &str) -> Expr {
    Expr::Raw(sql.into())
}

// =============================================================================
// Window Builder
// =============================================================================

/// Builder for creating window function expressions.
#[derive(Debug, Clone)]
#[must_use = "WindowBuilder has no effect until build() is called"]
pub struct WindowBuilder {
    function: Expr,
    partition_by: Vec<Expr>,
    order_by: Vec<WindowOrderBy>,
    frame: Option<WindowFrame>,
}

impl WindowBuilder {
    /// Create a new window builder for the given function.
    pub fn new(function: Expr) -> Self {
        Self {
            function,
            partition_by: vec![],
            order_by: vec![],
            frame: None,
        }
    }

    /// Add PARTITION BY expressions.
    pub fn partition_by(mut self, exprs: Vec<Expr>) -> Self {
        self.partition_by = exprs;
        self
    }

    /// Add ORDER BY expressions.
    pub fn order_by(mut self, exprs: Vec<WindowOrderBy>) -> Self {
        self.order_by = exprs;
        self
    }

    /// Set the window frame.
    pub fn frame(mut self, frame: WindowFrame) -> Self {
        self.frame = Some(frame);
        self
    }

    /// Shorthand: ROWS UNBOUNDED PRECEDING.
    pub fn rows_unbounded_preceding(mut self) -> Self {
        self.frame = Some(WindowFrame::rows_unbounded_preceding());
        self
    }

    /// Shorthand: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    pub fn rows_to_current(mut self) -> Self {
        self.frame = Some(WindowFrame::rows_to_current());
        self
    }

    /// Build the window function expression.
    ///
    /// # Panics
    ///
    /// Panics if a frame is specified without ORDER BY, as this produces
    /// invalid or undefined behavior in most SQL dialects.
    pub fn build(self) -> Expr {
        if self.frame.is_some() && self.order_by.is_empty() {
            panic!(
                "Window frame requires ORDER BY clause. \
                 Add .order_by() before .frame() or remove the frame specification."
            );
        }
        Expr::WindowFunction {
            function: Box::new(self.function),
            partition_by: self.partition_by,
            order_by: self.order_by,
            frame: self.frame,
        }
    }
}

/// Extension trait for adding OVER clause to expressions.
pub trait WindowExt: Sized {
    /// Start building a window function with OVER clause.
    fn over(self) -> WindowBuilder;
}

impl WindowExt for Expr {
    fn over(self) -> WindowBuilder {
        WindowBuilder::new(self)
    }
}

// =============================================================================
// Expression Builder Trait
// =============================================================================

/// Extension trait for building expressions fluently.
pub trait ExprExt: Sized {
    fn into_expr(self) -> Expr;

    // Comparison operators
    fn eq(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Eq,
            right: Box::new(other.into()),
        }
    }

    fn ne(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Ne,
            right: Box::new(other.into()),
        }
    }

    fn gt(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Gt,
            right: Box::new(other.into()),
        }
    }

    fn gte(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Gte,
            right: Box::new(other.into()),
        }
    }

    fn lt(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Lt,
            right: Box::new(other.into()),
        }
    }

    fn lte(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Lte,
            right: Box::new(other.into()),
        }
    }

    // Logical operators
    fn and(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::And,
            right: Box::new(other.into()),
        }
    }

    fn or(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Or,
            right: Box::new(other.into()),
        }
    }

    fn not(self) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(self.into_expr()),
        }
    }

    // Arithmetic operators
    fn add(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Plus,
            right: Box::new(other.into()),
        }
    }

    fn sub(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Minus,
            right: Box::new(other.into()),
        }
    }

    fn mul(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Mul,
            right: Box::new(other.into()),
        }
    }

    fn div(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Div,
            right: Box::new(other.into()),
        }
    }

    // String operators
    fn like(self, pattern: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Like,
            right: Box::new(pattern.into()),
        }
    }

    /// LIKE with ESCAPE clause for matching literal `%` and `_` characters.
    ///
    /// # Example
    /// ```ignore
    /// // Match strings containing literal "100%"
    /// col("discount").like_escape(lit_str("100\\%"), '\\')
    /// ```
    fn like_escape(self, pattern: impl Into<Expr>, escape_char: char) -> Expr {
        Expr::LikeEscape {
            expr: Box::new(self.into_expr()),
            pattern: Box::new(pattern.into()),
            escape_char,
            negated: false,
        }
    }

    /// NOT LIKE with ESCAPE clause.
    fn not_like_escape(self, pattern: impl Into<Expr>, escape_char: char) -> Expr {
        Expr::LikeEscape {
            expr: Box::new(self.into_expr()),
            pattern: Box::new(pattern.into()),
            escape_char,
            negated: true,
        }
    }

    fn concat(self, other: impl Into<Expr>) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self.into_expr()),
            op: BinaryOperator::Concat,
            right: Box::new(other.into()),
        }
    }

    // NULL checks
    #[allow(clippy::wrong_self_convention)]
    fn is_null(self) -> Expr {
        Expr::IsNull {
            expr: Box::new(self.into_expr()),
            negated: false,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn is_not_null(self) -> Expr {
        Expr::IsNull {
            expr: Box::new(self.into_expr()),
            negated: true,
        }
    }

    // IN operator
    fn in_list(self, values: Vec<Expr>) -> Expr {
        Expr::In {
            expr: Box::new(self.into_expr()),
            values,
            negated: false,
        }
    }

    fn not_in_list(self, values: Vec<Expr>) -> Expr {
        Expr::In {
            expr: Box::new(self.into_expr()),
            values,
            negated: true,
        }
    }

    // BETWEEN operator
    fn between(self, low: impl Into<Expr>, high: impl Into<Expr>) -> Expr {
        Expr::Between {
            expr: Box::new(self.into_expr()),
            low: Box::new(low.into()),
            high: Box::new(high.into()),
            negated: false,
        }
    }

    fn not_between(self, low: impl Into<Expr>, high: impl Into<Expr>) -> Expr {
        Expr::Between {
            expr: Box::new(self.into_expr()),
            low: Box::new(low.into()),
            high: Box::new(high.into()),
            negated: true,
        }
    }

    /// Alias this expression (for SELECT list).
    fn alias(self, name: &str) -> crate::query::SelectExpr {
        crate::query::SelectExpr {
            expr: self.into_expr(),
            alias: Some(name.into()),
        }
    }
}

impl ExprExt for Expr {
    fn into_expr(self) -> Expr {
        self
    }
}

// =============================================================================
// Conversions
// =============================================================================

impl From<i64> for Expr {
    fn from(n: i64) -> Self {
        lit_int(n)
    }
}

impl From<i32> for Expr {
    fn from(n: i32) -> Self {
        lit_int(n as i64)
    }
}

impl From<f64> for Expr {
    fn from(f: f64) -> Self {
        lit_float(f)
    }
}

impl From<&str> for Expr {
    fn from(s: &str) -> Self {
        lit_str(s)
    }
}

impl From<String> for Expr {
    fn from(s: String) -> Self {
        Expr::Literal(Literal::String(s))
    }
}

impl From<bool> for Expr {
    fn from(b: bool) -> Self {
        lit_bool(b)
    }
}

impl From<crate::query::Query> for Expr {
    /// Convert a Query into a Subquery expression.
    ///
    /// This enables ergonomic subquery construction:
    /// ```ignore
    /// let subquery = Query::new().select(vec![col("id")]).from(TableRef::new("users"));
    /// let expr = col("id").in_list(vec![subquery.into()]);
    /// ```
    fn from(query: crate::query::Query) -> Self {
        Expr::Subquery(Box::new(query))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::dialect::Dialect;

    #[test]
    fn test_column() {
        let expr = col("name");
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "\"name\"");
    }

    #[test]
    fn test_table_column() {
        let expr = table_col("u", "name");
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "\"u\".\"name\"");
    }

    #[test]
    fn test_binary_op() {
        let expr = col("age").gte(lit_int(18));
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "\"age\" >= 18");
    }

    #[test]
    fn test_chained_and() {
        let expr = col("active").eq(true).and(col("age").gte(18));
        let sql = expr.to_tokens().serialize(Dialect::DuckDb);
        assert!(sql.contains("AND"));
    }

    #[test]
    fn test_function() {
        let expr = sum(col("amount"));
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "SUM(\"amount\")");
    }

    #[test]
    fn test_count_distinct() {
        let expr = count_distinct(col("user_id"));
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "COUNT(DISTINCT \"user_id\")");
    }

    #[test]
    fn test_between() {
        let expr = col("age").between(18, 65);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "\"age\" BETWEEN 18 AND 65");
    }

    #[test]
    fn test_like_escape() {
        // LIKE with escape character for matching literal %
        let expr = col("discount").like_escape(lit_str("100\\%"), '\\');
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("LIKE"), "SQL: {}", sql);
        assert!(sql.contains("ESCAPE"), "SQL: {}", sql);
        // The escape char '\' becomes '\'' in SQL string literal
        assert!(
            sql.contains("ESCAPE '"),
            "SQL should contain ESCAPE clause: {}",
            sql
        );

        // NOT LIKE with escape
        let expr = col("name").not_like_escape(lit_str("test\\_"), '\\');
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("NOT LIKE"), "SQL: {}", sql);
        assert!(sql.contains("ESCAPE"));
    }

    #[test]
    fn test_in_list() {
        let expr = col("status").in_list(vec![lit_str("active"), lit_str("pending")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("IN"));
        assert!(sql.contains("'active'"));
    }

    #[test]
    fn test_in_list_empty() {
        // Empty IN list should produce FALSE
        let expr = col("status").in_list(vec![]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "FALSE");

        // Empty NOT IN list should produce TRUE
        let expr = col("status").not_in_list(vec![]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert_eq!(sql, "TRUE");
    }

    #[test]
    fn test_case() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![
                (col("status").eq("A"), lit_str("Active")),
                (col("status").eq("I"), lit_str("Inactive")),
            ],
            else_clause: Some(Box::new(lit_str("Unknown"))),
        };
        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN"));
        assert!(sql.contains("ELSE"));
        assert!(sql.contains("END"));
    }

    // Window function tests
    #[test]
    fn test_row_number_over_partition() {
        let expr = row_number()
            .over()
            .partition_by(vec![col("department")])
            .order_by(vec![WindowOrderBy::desc(col("salary"))])
            .build();

        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("ROW_NUMBER()"));
        assert!(sql.contains("OVER"));
        assert!(sql.contains("PARTITION BY"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("DESC"));
    }

    #[test]
    fn test_running_sum_with_frame() {
        let expr = sum(col("amount"))
            .over()
            .order_by(vec![WindowOrderBy::asc(col("date"))])
            .rows_to_current()
            .build();

        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("SUM"));
        assert!(sql.contains("OVER"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));
    }

    #[test]
    fn test_window_frame_unbounded() {
        let expr = sum(col("value"))
            .over()
            .partition_by(vec![col("category")])
            .order_by(vec![WindowOrderBy::asc(col("id"))])
            .frame(WindowFrame::rows_entire_partition())
            .build();

        let sql = expr.to_tokens().serialize(Dialect::DuckDb);
        assert!(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"));
    }

    #[test]
    fn test_rank_function() {
        let expr = rank()
            .over()
            .partition_by(vec![col("team")])
            .order_by(vec![WindowOrderBy::desc(col("score"))])
            .build();

        let sql = expr.to_tokens().serialize(Dialect::TSql);
        assert!(sql.contains("RANK()"));
        assert!(sql.contains("OVER"));
        assert!(sql.contains("PARTITION BY"));
    }

    #[test]
    fn test_lag_lead_functions() {
        let lag_expr = lag(col("price"))
            .over()
            .order_by(vec![WindowOrderBy::asc(col("date"))])
            .build();

        let lead_expr = lead_offset(col("price"), 2)
            .over()
            .order_by(vec![WindowOrderBy::asc(col("date"))])
            .build();

        let lag_sql = lag_expr.to_tokens().serialize(Dialect::Postgres);
        let lead_sql = lead_expr.to_tokens().serialize(Dialect::Postgres);

        assert!(lag_sql.contains("LAG"));
        assert!(lead_sql.contains("LEAD"));
        assert!(lead_sql.contains("2"));
    }

    #[test]
    fn test_window_without_partition() {
        // Window with only ORDER BY, no PARTITION BY
        let expr = row_number()
            .over()
            .order_by(vec![WindowOrderBy::asc(col("id"))])
            .build();

        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("OVER (ORDER BY"));
        assert!(!sql.contains("PARTITION BY"));
    }

    #[test]
    fn test_frame_with_offset() {
        let frame = WindowFrame::between(
            WindowFrameKind::Rows,
            WindowFrameBound::Preceding(3),
            WindowFrameBound::Following(1),
        );

        let expr = avg(col("value"))
            .over()
            .order_by(vec![WindowOrderBy::asc(col("time"))])
            .frame(frame)
            .build();

        let sql = expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING"));
    }

    #[test]
    fn test_groups_frame_dialect_fallback() {
        let frame = WindowFrame::between(
            WindowFrameKind::Groups,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::CurrentRow,
        );

        let expr = sum(col("value"))
            .over()
            .order_by(vec![WindowOrderBy::asc(col("id"))])
            .frame(frame)
            .build();

        // PostgreSQL supports GROUPS
        let pg_sql = expr
            .to_tokens_for_dialect(Dialect::Postgres)
            .serialize(Dialect::Postgres);
        assert!(pg_sql.contains("GROUPS BETWEEN"));

        // DuckDB supports GROUPS
        let duck_sql = expr
            .to_tokens_for_dialect(Dialect::DuckDb)
            .serialize(Dialect::DuckDb);
        assert!(duck_sql.contains("GROUPS BETWEEN"));

        // T-SQL doesn't support GROUPS, falls back to ROWS
        let tsql = expr
            .to_tokens_for_dialect(Dialect::TSql)
            .serialize(Dialect::TSql);
        assert!(tsql.contains("ROWS BETWEEN"));
        assert!(!tsql.contains("GROUPS"));

        // MySQL doesn't support GROUPS, falls back to ROWS
        let mysql = expr
            .to_tokens_for_dialect(Dialect::MySql)
            .serialize(Dialect::MySql);
        assert!(mysql.contains("ROWS BETWEEN"));
        assert!(!mysql.contains("GROUPS"));
    }

    #[test]
    fn test_concat_mysql_function() {
        // MySQL should use CONCAT() function, not || operator
        let expr = col("first_name").concat(col("last_name"));

        // PostgreSQL uses ||
        let pg_sql = expr
            .to_tokens_for_dialect(Dialect::Postgres)
            .serialize(Dialect::Postgres);
        assert!(pg_sql.contains("||"));
        assert!(!pg_sql.contains("CONCAT("));

        // DuckDB uses ||
        let duck_sql = expr
            .to_tokens_for_dialect(Dialect::DuckDb)
            .serialize(Dialect::DuckDb);
        assert!(duck_sql.contains("||"));

        // MySQL uses CONCAT() function
        let mysql = expr
            .to_tokens_for_dialect(Dialect::MySql)
            .serialize(Dialect::MySql);
        assert!(mysql.contains("CONCAT("));
        assert!(!mysql.contains("||"));
    }

    #[test]
    #[should_panic(expected = "Window frame requires ORDER BY")]
    fn test_window_frame_without_order_by_panics() {
        // Frame without ORDER BY should panic
        sum(col("amount"))
            .over()
            .partition_by(vec![col("region")])
            .frame(WindowFrame::rows_to_current())
            .build();
    }

    #[test]
    fn test_query_to_expr_subquery() {
        use crate::sql::query::{Query, TableRef};

        // Create a subquery
        let subquery = Query::new()
            .select(vec![col("user_id")])
            .from(TableRef::new("premium_users"));

        // Convert Query to Expr using From trait
        let subquery_expr: Expr = subquery.into();

        // Verify it's a Subquery variant
        let sql = subquery_expr.to_tokens().serialize(Dialect::Postgres);
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("\"user_id\""));
        assert!(sql.contains("\"premium_users\""));
        assert!(sql.starts_with("("));
        assert!(sql.ends_with(")"));
    }
}
