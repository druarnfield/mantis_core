//! Expression types for SQL transformations.
//!
//! This module defines a dialect-agnostic AST for SQL expressions used in
//! table definitions, measures, and calculated slicers.

use serde::{Deserialize, Serialize};

use super::types::DataType;

// =============================================================================
// Core Expression Type
// =============================================================================

/// Logical expression AST - dialect agnostic.
///
/// Expressions can represent @atom references, column references, literals,
/// function calls, operations, CASE expressions, and casts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Reference to an @atom (e.g., @revenue)
    AtomRef(String),

    /// Column reference with optional entity qualifier
    Column {
        entity: Option<String>,
        column: String,
    },

    /// Literal value
    Literal(Literal),

    /// Function call (aggregate or scalar)
    Function { func: Func, args: Vec<Expr> },

    /// Binary operation (e.g., a + b, a > b)
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Unary operation (e.g., -x, NOT x)
    UnaryOp { op: UnaryOp, expr: Box<Expr> },

    /// CASE expression
    Case {
        conditions: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },

    /// CAST expression
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
}

impl Expr {
    /// Create an atom reference.
    pub fn atom_ref(name: impl Into<String>) -> Self {
        Expr::AtomRef(name.into())
    }

    /// Create a column reference.
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column {
            entity: None,
            column: name.into(),
        }
    }

    /// Create a qualified column reference (entity.column).
    pub fn qualified_column(entity: impl Into<String>, column: impl Into<String>) -> Self {
        Expr::Column {
            entity: Some(entity.into()),
            column: column.into(),
        }
    }

    /// Create a literal expression.
    pub fn literal(lit: Literal) -> Self {
        Expr::Literal(lit)
    }

    /// Create an integer literal.
    pub fn int(value: i64) -> Self {
        Expr::Literal(Literal::Int(value))
    }

    /// Create a float literal.
    pub fn float(value: f64) -> Self {
        Expr::Literal(Literal::Float(value))
    }

    /// Create a string literal.
    pub fn string(value: impl Into<String>) -> Self {
        Expr::Literal(Literal::String(value.into()))
    }

    /// Create a boolean literal.
    pub fn bool(value: bool) -> Self {
        Expr::Literal(Literal::Bool(value))
    }

    /// Create a NULL literal.
    pub fn null() -> Self {
        Expr::Literal(Literal::Null)
    }

    /// Create a function call.
    pub fn func(func: Func, args: Vec<Expr>) -> Self {
        Expr::Function { func, args }
    }

    /// Create a binary operation.
    pub fn binary(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Create a unary operation.
    pub fn unary(op: UnaryOp, expr: Expr) -> Self {
        Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }

    /// Create a CAST expression.
    pub fn cast(expr: Expr, data_type: DataType) -> Self {
        Expr::Cast {
            expr: Box::new(expr),
            data_type,
        }
    }

    /// Create a CASE WHEN expression.
    pub fn case_when(conditions: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Self {
        Expr::Case {
            conditions,
            else_expr: else_expr.map(Box::new),
        }
    }

    // === Convenience methods for common operations ===

    /// expr = other
    pub fn eq(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Eq, other)
    }

    /// expr <> other
    pub fn ne(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Ne, other)
    }

    /// expr < other
    pub fn lt(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Lt, other)
    }

    /// expr > other
    pub fn gt(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Gt, other)
    }

    /// expr <= other
    pub fn lte(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Lte, other)
    }

    /// expr >= other
    pub fn gte(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Gte, other)
    }

    /// expr AND other
    pub fn and(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::And, other)
    }

    /// expr OR other
    pub fn or(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Or, other)
    }

    /// expr + other
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Add, other)
    }

    /// expr - other
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Sub, other)
    }

    /// expr * other
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Mul, other)
    }

    /// expr / other
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, other: Expr) -> Self {
        Self::binary(self, BinaryOp::Div, other)
    }

    /// NOT expr
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Self::unary(UnaryOp::Not, self)
    }

    /// expr IS NULL
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOp::IsNull, self)
    }

    /// expr IS NOT NULL
    pub fn is_not_null(self) -> Self {
        Self::unary(UnaryOp::IsNotNull, self)
    }

    // === Validation methods ===

    /// Walk the expression tree, calling visitor on each node.
    pub fn walk<F>(&self, visitor: &mut F)
    where
        F: FnMut(&Expr),
    {
        visitor(self);
        match self {
            Expr::AtomRef(_) | Expr::Column { .. } | Expr::Literal(_) => {}
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.walk(visitor);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                left.walk(visitor);
                right.walk(visitor);
            }
            Expr::UnaryOp { expr, .. } => {
                expr.walk(visitor);
            }
            Expr::Case {
                conditions,
                else_expr,
            } => {
                for (cond, then) in conditions {
                    cond.walk(visitor);
                    then.walk(visitor);
                }
                if let Some(else_e) = else_expr {
                    else_e.walk(visitor);
                }
            }
            Expr::Cast { expr, .. } => {
                expr.walk(visitor);
            }
        }
    }

    /// Validate this expression is appropriate for the given context.
    pub fn validate_context(
        &self,
        context: crate::model::expr_validation::ExprContext,
    ) -> Result<(), crate::model::expr_validation::ValidationError> {
        use crate::model::expr_validation::{ExprContext, ValidationError};

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

    /// Check if this expression contains any aggregate functions.
    pub fn contains_aggregate(&self) -> bool {
        let mut has_agg = false;
        self.walk(&mut |expr| {
            if let Expr::Function {
                func: Func::Aggregate(_),
                ..
            } = expr
            {
                has_agg = true;
            }
        });
        has_agg
    }
}

// =============================================================================
// Literal Values
// =============================================================================

/// Literal values in expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    /// NULL
    Null,
    /// Boolean: true/false
    Bool(bool),
    /// Integer
    Int(i64),
    /// Floating point
    Float(f64),
    /// String
    String(String),
    /// Date: YYYY-MM-DD
    Date(String),
    /// Timestamp: YYYY-MM-DD HH:MM:SS
    Timestamp(String),
    /// Interval: INTERVAL '1' DAY
    Interval { value: String, unit: IntervalUnit },
}

/// Interval units for date/time arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IntervalUnit {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

// =============================================================================
// Operators
// =============================================================================

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,

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

    // String
    Concat,

    // Pattern matching
    Like,
    ILike,

    // Set membership
    In,
    NotIn,

    // Range
    Between,
    NotBetween,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    /// NOT expr
    Not,
    /// -expr (negation)
    Neg,
    /// expr IS NULL
    IsNull,
    /// expr IS NOT NULL
    IsNotNull,
}

// =============================================================================
// Functions
// =============================================================================

/// Function type - either aggregate or scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Func {
    Aggregate(AggregateFunc),
    Scalar(ScalarFunc),
}

impl Func {
    /// Returns true if this is an aggregate function.
    pub const fn is_aggregate(&self) -> bool {
        matches!(self, Func::Aggregate(_))
    }

    /// Infer the return type given argument types.
    pub fn return_type(&self, arg_types: &[DataType]) -> Option<DataType> {
        match self {
            Func::Aggregate(agg) => agg.return_type(arg_types),
            Func::Scalar(scalar) => scalar.return_type(arg_types),
        }
    }
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateFunc {
    /// SUM(expr)
    Sum,
    /// COUNT(*) or COUNT(expr)
    Count,
    /// AVG(expr)
    Avg,
    /// MIN(expr)
    Min,
    /// MAX(expr)
    Max,
    /// COUNT(DISTINCT expr)
    CountDistinct,
    /// STRING_AGG(expr, delimiter)
    StringAgg,
    /// ARRAY_AGG(expr)
    ArrayAgg,
}

impl AggregateFunc {
    /// Infer the return type given argument types.
    pub fn return_type(&self, arg_types: &[DataType]) -> Option<DataType> {
        match self {
            AggregateFunc::Count | AggregateFunc::CountDistinct => Some(DataType::Int),
            AggregateFunc::Sum | AggregateFunc::Min | AggregateFunc::Max => {
                arg_types.first().cloned()
            }
            AggregateFunc::Avg => Some(DataType::Float),
            AggregateFunc::StringAgg => Some(DataType::String),
            AggregateFunc::ArrayAgg => None, // Would need element type
        }
    }
}

/// Scalar functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScalarFunc {
    // === String Functions ===
    /// UPPER(str)
    Upper,
    /// LOWER(str)
    Lower,
    /// INITCAP(str) - capitalize first letter of each word
    InitCap,
    /// TRIM(str)
    Trim,
    /// LTRIM(str)
    LTrim,
    /// RTRIM(str)
    RTrim,
    /// LEFT(str, n)
    Left,
    /// RIGHT(str, n)
    Right,
    /// SUBSTRING(str, start, len)
    Substring,
    /// LENGTH(str)
    Length,
    /// REPLACE(str, from, to)
    Replace,
    /// CONCAT(a, b, ...)
    Concat,
    /// SPLIT_PART(str, delimiter, index)
    SplitPart,
    /// REGEXP_REPLACE(str, pattern, replacement)
    RegexpReplace,
    /// REGEXP_EXTRACT(str, pattern)
    RegexpExtract,

    // === NULL Handling ===
    /// COALESCE(a, b, ...)
    Coalesce,
    /// NULLIF(a, b)
    NullIf,
    /// IFNULL(expr, default)
    IfNull,

    // === Date/Time Functions ===
    /// DATE_TRUNC(unit, date)
    DateTrunc,
    /// EXTRACT(unit FROM date)
    Extract,
    /// DATE_ADD(date, interval, unit)
    DateAdd,
    /// DATE_SUB(date, interval, unit)
    DateSub,
    /// DATE_DIFF(unit, start, end)
    DateDiff,
    /// Current date
    CurrentDate,
    /// Current timestamp
    CurrentTimestamp,
    /// DATE(expr) - extract date from timestamp
    ToDate,
    /// YEAR(date)
    Year,
    /// MONTH(date)
    Month,
    /// DAY(date)
    Day,
    /// HOUR(timestamp)
    Hour,
    /// MINUTE(timestamp)
    Minute,
    /// SECOND(timestamp)
    Second,
    /// DAY_OF_WEEK(date)
    DayOfWeek,
    /// DAY_OF_YEAR(date)
    DayOfYear,
    /// WEEK_OF_YEAR(date)
    WeekOfYear,
    /// QUARTER(date)
    Quarter,
    /// LAST_DAY(date)
    LastDay,
    /// MAKE_DATE(year, month, day)
    MakeDate,
    /// MAKE_TIMESTAMP(year, month, day, hour, minute, second)
    MakeTimestamp,

    // === Numeric Functions ===
    /// ROUND(num, decimals)
    Round,
    /// FLOOR(num)
    Floor,
    /// CEIL(num) or CEILING(num)
    Ceil,
    /// ABS(num)
    Abs,
    /// SIGN(num) - returns -1, 0, or 1
    Sign,
    /// POWER(base, exp) or POW(base, exp)
    Power,
    /// SQRT(num)
    Sqrt,
    /// EXP(num)
    Exp,
    /// LN(num) - natural log
    Ln,
    /// LOG(base, num) or LOG10(num)
    Log,
    /// LOG10(num)
    Log10,
    /// MOD(a, b) or a % b
    Mod,
    /// TRUNCATE(num, decimals)
    Truncate,
    /// RANDOM() or RAND()
    Random,

    // === Conditional ===
    /// IF(cond, then, else)
    If,
    /// GREATEST(a, b, ...)
    Greatest,
    /// LEAST(a, b, ...)
    Least,

    // === Type Conversion ===
    /// CAST(expr AS type)
    Cast,
    /// TRY_CAST(expr AS type) - returns NULL on failure
    TryCast,
    /// TO_CHAR(expr, format) - format to string
    ToChar,
    /// TO_NUMBER(str) - parse string to number
    ToNumber,

    // === Hash Functions ===
    /// MD5(str)
    Md5,
    /// SHA256(str)
    Sha256,
    /// SHA1(str)
    Sha1,

    // === JSON Functions ===
    /// JSON_EXTRACT(json, path)
    JsonExtract,
    /// JSON_EXTRACT_SCALAR(json, path)
    JsonExtractText,
    /// JSON_ARRAY_LENGTH(json)
    JsonArrayLength,
}

impl ScalarFunc {
    /// Infer the return type given argument types.
    pub fn return_type(&self, arg_types: &[DataType]) -> Option<DataType> {
        match self {
            // String functions return String
            ScalarFunc::Upper
            | ScalarFunc::Lower
            | ScalarFunc::InitCap
            | ScalarFunc::Trim
            | ScalarFunc::LTrim
            | ScalarFunc::RTrim
            | ScalarFunc::Left
            | ScalarFunc::Right
            | ScalarFunc::Substring
            | ScalarFunc::Replace
            | ScalarFunc::Concat
            | ScalarFunc::SplitPart
            | ScalarFunc::RegexpReplace
            | ScalarFunc::RegexpExtract
            | ScalarFunc::ToChar => Some(DataType::String),

            ScalarFunc::Length => Some(DataType::Int),

            // Date functions
            ScalarFunc::DateTrunc
            | ScalarFunc::ToDate
            | ScalarFunc::CurrentDate
            | ScalarFunc::LastDay
            | ScalarFunc::MakeDate => Some(DataType::Date),

            ScalarFunc::CurrentTimestamp | ScalarFunc::MakeTimestamp => Some(DataType::Timestamp),

            ScalarFunc::Year
            | ScalarFunc::Month
            | ScalarFunc::Day
            | ScalarFunc::Hour
            | ScalarFunc::Minute
            | ScalarFunc::Second
            | ScalarFunc::DayOfWeek
            | ScalarFunc::DayOfYear
            | ScalarFunc::WeekOfYear
            | ScalarFunc::Quarter => Some(DataType::Int),

            ScalarFunc::DateDiff => Some(DataType::Int),
            ScalarFunc::Extract => Some(DataType::Int),

            // Numeric functions
            ScalarFunc::Abs
            | ScalarFunc::Sign
            | ScalarFunc::Round
            | ScalarFunc::Floor
            | ScalarFunc::Ceil
            | ScalarFunc::Truncate => arg_types.first().cloned(),

            ScalarFunc::Power
            | ScalarFunc::Sqrt
            | ScalarFunc::Exp
            | ScalarFunc::Ln
            | ScalarFunc::Log
            | ScalarFunc::Log10
            | ScalarFunc::Random => Some(DataType::Float),

            ScalarFunc::Mod => arg_types.first().cloned(),

            // Conditional - returns type of first non-null branch
            ScalarFunc::Coalesce
            | ScalarFunc::NullIf
            | ScalarFunc::IfNull
            | ScalarFunc::Greatest
            | ScalarFunc::Least => arg_types.first().cloned(),

            ScalarFunc::If => arg_types.get(1).cloned(), // type of 'then' branch

            // Hash functions return strings
            ScalarFunc::Md5 | ScalarFunc::Sha256 | ScalarFunc::Sha1 => Some(DataType::String),

            // Type conversion - would need target type from Cast expression
            ScalarFunc::Cast | ScalarFunc::TryCast | ScalarFunc::ToNumber => None,

            // Date arithmetic returns date/timestamp
            ScalarFunc::DateAdd | ScalarFunc::DateSub => arg_types.first().cloned(),

            // JSON functions
            ScalarFunc::JsonExtract | ScalarFunc::JsonExtractText => Some(DataType::String),
            ScalarFunc::JsonArrayLength => Some(DataType::Int),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_atom_ref() {
        let atom = Expr::atom_ref("revenue");
        assert!(matches!(atom, Expr::AtomRef(name) if name == "revenue"));
    }

    #[test]
    fn test_expr_column() {
        let col = Expr::column("order_id");
        assert!(matches!(col, Expr::Column { entity: None, column } if column == "order_id"));

        let qual = Expr::qualified_column("orders", "order_id");
        assert!(
            matches!(qual, Expr::Column { entity: Some(e), column } if e == "orders" && column == "order_id")
        );
    }

    #[test]
    fn test_expr_literals() {
        assert!(matches!(Expr::int(42), Expr::Literal(Literal::Int(42))));
        assert!(matches!(Expr::string("hello"), Expr::Literal(Literal::String(s)) if s == "hello"));
        assert!(matches!(Expr::null(), Expr::Literal(Literal::Null)));
    }

    #[test]
    fn test_expr_binary_op() {
        let expr = Expr::column("a").add(Expr::column("b"));
        assert!(matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::Add,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_comparison() {
        let expr = Expr::column("status").eq(Expr::string("active"));
        assert!(matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_logical() {
        let expr = Expr::column("a")
            .gt(Expr::int(0))
            .and(Expr::column("b").lt(Expr::int(100)));
        assert!(matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_function_aggregate() {
        let expr = Expr::func(
            Func::Aggregate(AggregateFunc::Sum),
            vec![Expr::column("amount")],
        );
        match expr {
            Expr::Function { func, args } => {
                assert!(func.is_aggregate());
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function"),
        }
    }

    #[test]
    fn test_expr_function_scalar() {
        let expr = Expr::func(Func::Scalar(ScalarFunc::Upper), vec![Expr::column("name")]);
        match expr {
            Expr::Function { func, args } => {
                assert!(!func.is_aggregate());
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function"),
        }
    }

    #[test]
    fn test_expr_cast() {
        let expr = Expr::cast(Expr::column("amount"), DataType::Decimal);
        assert!(matches!(
            expr,
            Expr::Cast {
                data_type: DataType::Decimal,
                ..
            }
        ));
    }

    #[test]
    fn test_case_when() {
        let expr = Expr::case_when(
            vec![
                (
                    Expr::column("status").eq(Expr::string("active")),
                    Expr::int(1),
                ),
                (
                    Expr::column("status").eq(Expr::string("pending")),
                    Expr::int(2),
                ),
            ],
            Some(Expr::int(0)),
        );
        assert!(matches!(expr, Expr::Case { .. }));
    }

    #[test]
    fn test_func_is_aggregate() {
        assert!(Func::Aggregate(AggregateFunc::Sum).is_aggregate());
        assert!(Func::Aggregate(AggregateFunc::Count).is_aggregate());
        assert!(!Func::Scalar(ScalarFunc::Upper).is_aggregate());
        assert!(!Func::Scalar(ScalarFunc::DateTrunc).is_aggregate());
    }

    #[test]
    fn test_func_return_type() {
        assert_eq!(
            Func::Scalar(ScalarFunc::Upper).return_type(&[]),
            Some(DataType::String)
        );
        assert_eq!(
            Func::Scalar(ScalarFunc::Length).return_type(&[]),
            Some(DataType::Int)
        );
        assert_eq!(
            Func::Aggregate(AggregateFunc::Count).return_type(&[]),
            Some(DataType::Int)
        );
        assert_eq!(
            Func::Scalar(ScalarFunc::Year).return_type(&[]),
            Some(DataType::Int)
        );
    }

    #[test]
    fn test_aggregate_return_type() {
        assert_eq!(
            AggregateFunc::Sum.return_type(&[DataType::Int]),
            Some(DataType::Int)
        );
        assert_eq!(AggregateFunc::Avg.return_type(&[]), Some(DataType::Float));
    }

    #[test]
    fn test_scalar_return_type() {
        assert_eq!(ScalarFunc::Upper.return_type(&[]), Some(DataType::String));
        assert_eq!(
            ScalarFunc::Abs.return_type(&[DataType::Int]),
            Some(DataType::Int)
        );
    }
}
