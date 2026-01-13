//! Expression types for column transformations.
//!
//! This module defines a dialect-agnostic AST for SQL expressions.
//! The emission layer (later) handles dialect-specific SQL generation.

use serde::{Deserialize, Serialize};

use super::types::DataType;

// =============================================================================
// Core Expression Type
// =============================================================================

/// Logical expression AST - dialect agnostic.
///
/// Expressions can represent column references, literals, function calls,
/// operations, CASE expressions, casts, and window functions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Column reference: entity.column or just column
    Column {
        entity: Option<String>,
        column: String,
    },

    /// Literal value
    Literal(Literal),

    /// Function call with typed function
    Function {
        func: Func,
        args: Vec<Expr>,
    },

    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Unary operation
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    /// CASE WHEN expression
    Case {
        /// For CASE expr WHEN value form (simple case)
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expr>>,
    },

    /// CAST expression
    Cast {
        expr: Box<Expr>,
        target_type: DataType,
    },

    /// Window function
    Window {
        func: WindowFunc,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
        frame: Option<WindowFrame>,
    },

    /// Filtered aggregation (aggregate FILTER (WHERE condition))
    ///
    /// Standard SQL:2003 syntax for conditional aggregation.
    /// Example: SUM(amount) FILTER (WHERE status = 'completed')
    FilteredAgg {
        /// The aggregation expression (must be a Function with aggregate func)
        agg: Box<Expr>,
        /// The filter condition
        filter: Box<Expr>,
    },
}

impl Expr {
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
    pub fn cast(expr: Expr, target_type: DataType) -> Self {
        Expr::Cast {
            expr: Box::new(expr),
            target_type,
        }
    }

    /// Create a CASE WHEN expression.
    pub fn case_when(when_clauses: Vec<WhenClause>, else_clause: Option<Expr>) -> Self {
        Expr::Case {
            operand: None,
            when_clauses,
            else_clause: else_clause.map(Box::new),
        }
    }

    /// Create a simple CASE expression (CASE expr WHEN value THEN result).
    pub fn case_simple(operand: Expr, when_clauses: Vec<WhenClause>, else_clause: Option<Expr>) -> Self {
        Expr::Case {
            operand: Some(Box::new(operand)),
            when_clauses,
            else_clause: else_clause.map(Box::new),
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
// CASE Expression
// =============================================================================

/// A WHEN clause in a CASE expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

impl WhenClause {
    pub fn new(condition: Expr, result: Expr) -> Self {
        Self { condition, result }
    }
}

// =============================================================================
// Functions
// =============================================================================

/// Typed function enum - exhaustive list of supported functions.
///
/// Each variant documents dialect mappings in its doc comment.
/// The emission layer uses these to generate dialect-specific SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Func {
    // === Aggregates ===
    /// COUNT(*) or COUNT(expr)
    /// All dialects: COUNT(...)
    Count,
    /// SUM(expr)
    Sum,
    /// AVG(expr)
    Avg,
    /// MIN(expr)
    Min,
    /// MAX(expr)
    Max,
    /// COUNT(DISTINCT expr)
    CountDistinct,

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
    /// T-SQL: SUBSTRING(...), Postgres/DuckDB: SUBSTRING(...) or SUBSTR(...)
    Substring,
    /// LENGTH(str)
    /// T-SQL: LEN(...), Others: LENGTH(...)
    Length,
    /// REPLACE(str, from, to)
    Replace,
    /// CONCAT(a, b, ...)
    /// T-SQL: CONCAT(...) or a + b, Others: CONCAT(...) or a || b
    Concat,
    /// SPLIT_PART(str, delimiter, index)
    SplitPart,
    /// REGEXP_REPLACE(str, pattern, replacement)
    RegexpReplace,
    /// REGEXP_EXTRACT(str, pattern) or REGEXP_SUBSTR
    RegexpExtract,

    // === NULL Handling ===
    /// COALESCE(a, b, ...)
    Coalesce,
    /// NULLIF(a, b)
    NullIf,
    /// IFNULL(expr, default) - alias for COALESCE with 2 args
    /// T-SQL: ISNULL(...), Others: IFNULL(...) or COALESCE(...)
    IfNull,

    // === Date/Time Functions ===
    /// DATE_TRUNC(unit, date)
    /// DuckDB/Postgres: DATE_TRUNC('month', d)
    /// T-SQL (2022+): DATETRUNC(month, d)
    /// T-SQL (older): DATEADD(month, DATEDIFF(month, 0, d), 0)
    DateTrunc,
    /// EXTRACT(unit FROM date)
    /// DuckDB/Postgres: EXTRACT(YEAR FROM d)
    /// T-SQL: DATEPART(year, d)
    Extract,
    /// DATE_ADD(date, interval, unit)
    /// DuckDB: d + INTERVAL '1' DAY
    /// T-SQL: DATEADD(day, 1, d)
    /// Postgres: d + INTERVAL '1 day'
    DateAdd,
    /// DATE_SUB(date, interval, unit)
    DateSub,
    /// DATE_DIFF(unit, start, end)
    /// DuckDB: DATE_DIFF('day', a, b)
    /// T-SQL: DATEDIFF(day, a, b)
    DateDiff,
    /// Current date
    /// DuckDB/Postgres: CURRENT_DATE
    /// T-SQL: CAST(GETDATE() AS DATE)
    CurrentDate,
    /// Current timestamp
    /// DuckDB/Postgres: CURRENT_TIMESTAMP
    /// T-SQL: GETDATE() or SYSDATETIME()
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
    /// DAY_OF_WEEK(date) - 1=Sunday, 7=Saturday (varies by dialect)
    DayOfWeek,
    /// DAY_OF_YEAR(date)
    DayOfYear,
    /// WEEK_OF_YEAR(date)
    WeekOfYear,
    /// QUARTER(date)
    Quarter,
    /// LAST_DAY(date) - last day of month
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
    /// DuckDB: IF(...)
    /// T-SQL: IIF(...)
    /// Postgres: CASE WHEN cond THEN then ELSE else END
    If,
    /// GREATEST(a, b, ...)
    Greatest,
    /// LEAST(a, b, ...)
    Least,

    // === Type Conversion ===
    /// CAST(expr AS type)
    Cast,
    /// TRY_CAST(expr AS type) - returns NULL on failure
    /// DuckDB: TRY_CAST(...)
    /// T-SQL: TRY_CAST(...)
    /// Postgres: custom function or CASE wrapper
    TryCast,
    /// TO_CHAR(expr, format) - format to string
    ToChar,
    /// TO_NUMBER(str) - parse string to number
    ToNumber,

    // === Hash Functions ===
    /// MD5(str) - returns hex string
    Md5,
    /// SHA256(str) or SHA2(str, 256)
    Sha256,
    /// SHA1(str)
    Sha1,

    // === Array/List Functions (where supported) ===
    /// ARRAY_AGG(expr)
    ArrayAgg,
    /// STRING_AGG(expr, delimiter) or LISTAGG or GROUP_CONCAT
    /// DuckDB/Postgres: STRING_AGG(expr, delimiter)
    /// T-SQL: STRING_AGG(expr, delimiter)
    /// MySQL: GROUP_CONCAT(expr SEPARATOR delimiter)
    StringAgg,
    /// ARRAY_LENGTH(arr) or CARDINALITY(arr)
    ArrayLength,

    // === JSON Functions (where supported) ===
    /// JSON_EXTRACT(json, path) or json->path
    JsonExtract,
    /// JSON_EXTRACT_SCALAR(json, path) or json->>path
    JsonExtractText,
    /// JSON_ARRAY_LENGTH(json)
    JsonArrayLength,
}

impl Func {
    /// Infer the return type given argument types.
    ///
    /// Returns `Some(type)` if the return type can be determined,
    /// `None` if it requires runtime inference or is unknown.
    pub fn return_type(&self, arg_types: &[DataType]) -> Option<DataType> {
        match self {
            // String functions return String
            Func::Upper
            | Func::Lower
            | Func::InitCap
            | Func::Trim
            | Func::LTrim
            | Func::RTrim
            | Func::Left
            | Func::Right
            | Func::Substring
            | Func::Replace
            | Func::Concat
            | Func::SplitPart
            | Func::RegexpReplace
            | Func::RegexpExtract
            | Func::ToChar => Some(DataType::String),

            Func::Length => Some(DataType::Int64),

            // Aggregates
            Func::Count | Func::CountDistinct => Some(DataType::Int64),
            Func::Sum | Func::Min | Func::Max => arg_types.first().cloned(),
            Func::Avg => Some(DataType::Float64),

            // Date functions
            Func::DateTrunc | Func::ToDate | Func::CurrentDate | Func::LastDay | Func::MakeDate => {
                Some(DataType::Date)
            }
            Func::CurrentTimestamp | Func::MakeTimestamp => Some(DataType::Timestamp),
            Func::Year
            | Func::Month
            | Func::Day
            | Func::Hour
            | Func::Minute
            | Func::Second
            | Func::DayOfWeek
            | Func::DayOfYear
            | Func::WeekOfYear
            | Func::Quarter => Some(DataType::Int32),
            Func::DateDiff => Some(DataType::Int64),

            // Numeric functions
            Func::Abs | Func::Sign | Func::Round | Func::Floor | Func::Ceil | Func::Truncate => {
                arg_types.first().cloned()
            }
            Func::Power | Func::Sqrt | Func::Exp | Func::Ln | Func::Log | Func::Log10 | Func::Random => {
                Some(DataType::Float64)
            }
            Func::Mod => arg_types.first().cloned(),

            // Conditional - returns type of first non-null branch
            Func::Coalesce | Func::NullIf | Func::IfNull | Func::Greatest | Func::Least => {
                arg_types.first().cloned()
            }
            Func::If => arg_types.get(1).cloned(), // type of 'then' branch

            // Hash functions return strings
            Func::Md5 | Func::Sha256 | Func::Sha1 => Some(DataType::String),

            // Array functions
            Func::StringAgg => Some(DataType::String),
            Func::ArrayLength | Func::JsonArrayLength => Some(DataType::Int64),

            // Type conversion - would need target type from Cast expression
            Func::Cast | Func::TryCast | Func::ToNumber => None,

            // Date arithmetic returns date/timestamp
            Func::DateAdd | Func::DateSub => arg_types.first().cloned(),

            // Extract returns integer
            Func::Extract => Some(DataType::Int32),

            // JSON - usually returns string or varies
            Func::JsonExtract | Func::JsonExtractText => Some(DataType::String),

            // Array aggregate - would need element type
            Func::ArrayAgg => None,
        }
    }

    /// Returns true if this is an aggregate function.
    pub const fn is_aggregate(&self) -> bool {
        matches!(
            self,
            Func::Count
                | Func::CountDistinct
                | Func::Sum
                | Func::Avg
                | Func::Min
                | Func::Max
                | Func::ArrayAgg
                | Func::StringAgg
        )
    }
}

// =============================================================================
// Window Functions
// =============================================================================

/// Window functions for OVER clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFunc {
    // === Ranking ===
    /// ROW_NUMBER()
    RowNumber,
    /// RANK()
    Rank,
    /// DENSE_RANK()
    DenseRank,
    /// NTILE(n)
    NTile,
    /// PERCENT_RANK()
    PercentRank,
    /// CUME_DIST()
    CumeDist,

    // === Offset/Navigation ===
    /// LAG(expr, offset, default)
    Lag,
    /// LEAD(expr, offset, default)
    Lead,
    /// FIRST_VALUE(expr)
    FirstValue,
    /// LAST_VALUE(expr)
    LastValue,
    /// NTH_VALUE(expr, n)
    NthValue,

    // === Aggregates as Window Functions ===
    /// SUM(...) OVER (...)
    Sum,
    /// COUNT(...) OVER (...)
    Count,
    /// AVG(...) OVER (...)
    Avg,
    /// MIN(...) OVER (...)
    Min,
    /// MAX(...) OVER (...)
    Max,
}

impl WindowFunc {
    /// Returns true if this window function requires an ORDER BY clause.
    pub const fn requires_order_by(&self) -> bool {
        matches!(
            self,
            WindowFunc::RowNumber
                | WindowFunc::Rank
                | WindowFunc::DenseRank
                | WindowFunc::NTile
                | WindowFunc::PercentRank
                | WindowFunc::CumeDist
                | WindowFunc::Lag
                | WindowFunc::Lead
                | WindowFunc::FirstValue
                | WindowFunc::LastValue
                | WindowFunc::NthValue
        )
    }
}

// =============================================================================
// ORDER BY
// =============================================================================

/// ORDER BY clause for window functions and sorting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub dir: SortDir,
    pub nulls: Option<NullsOrder>,
}

impl OrderByExpr {
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            dir: SortDir::Asc,
            nulls: None,
        }
    }

    pub fn asc(expr: Expr) -> Self {
        Self {
            expr,
            dir: SortDir::Asc,
            nulls: None,
        }
    }

    pub fn desc(expr: Expr) -> Self {
        Self {
            expr,
            dir: SortDir::Desc,
            nulls: None,
        }
    }

    pub fn with_nulls(mut self, nulls: NullsOrder) -> Self {
        self.nulls = Some(nulls);
        self
    }
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum SortDir {
    #[default]
    Asc,
    Desc,
}

/// NULLS FIRST/LAST ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NullsOrder {
    First,
    Last,
}

// =============================================================================
// Window Frame
// =============================================================================

/// Window frame specification for ROWS/RANGE/GROUPS BETWEEN.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame {
    pub kind: FrameKind,
    pub start: FrameBound,
    pub end: Option<FrameBound>,
}

impl WindowFrame {
    /// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (default for running aggregates)
    pub fn rows_unbounded_preceding() -> Self {
        Self {
            kind: FrameKind::Rows,
            start: FrameBound::UnboundedPreceding,
            end: Some(FrameBound::CurrentRow),
        }
    }

    /// ROWS BETWEEN n PRECEDING AND CURRENT ROW (for moving averages)
    pub fn rows_preceding(n: u32) -> Self {
        Self {
            kind: FrameKind::Rows,
            start: FrameBound::Preceding(n),
            end: Some(FrameBound::CurrentRow),
        }
    }

    /// ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (full partition)
    pub fn rows_full_partition() -> Self {
        Self {
            kind: FrameKind::Rows,
            start: FrameBound::UnboundedPreceding,
            end: Some(FrameBound::UnboundedFollowing),
        }
    }
}

/// Frame type: ROWS, RANGE, or GROUPS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FrameKind {
    /// ROWS - physical row count
    Rows,
    /// RANGE - logical range based on ORDER BY values
    Range,
    /// GROUPS - groups of peer rows (less common)
    Groups,
}

/// Frame boundary specification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// n PRECEDING
    Preceding(u32),
    /// CURRENT ROW
    CurrentRow,
    /// n FOLLOWING
    Following(u32),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
}

// =============================================================================
// Column Definition
// =============================================================================

/// A column definition in a fact, dimension, or intermediate table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnDef {
    /// Simple column passthrough: source_col (keeps same name)
    Simple(String),

    /// Renamed column: source_col -> target_name
    Renamed {
        source: String,
        target: String,
    },

    /// Computed column with expression
    Computed {
        /// Target column name
        name: String,
        /// Expression to compute the value
        expr: Expr,
        /// Optional explicit type (inferred if not specified)
        data_type: Option<DataType>,
    },
}

impl ColumnDef {
    /// Create a simple passthrough column.
    pub fn simple(name: impl Into<String>) -> Self {
        ColumnDef::Simple(name.into())
    }

    /// Create a renamed column.
    pub fn renamed(source: impl Into<String>, target: impl Into<String>) -> Self {
        ColumnDef::Renamed {
            source: source.into(),
            target: target.into(),
        }
    }

    /// Create a computed column.
    pub fn computed(name: impl Into<String>, expr: Expr) -> Self {
        ColumnDef::Computed {
            name: name.into(),
            expr,
            data_type: None,
        }
    }

    /// Create a computed column with explicit type.
    pub fn computed_typed(name: impl Into<String>, expr: Expr, data_type: DataType) -> Self {
        ColumnDef::Computed {
            name: name.into(),
            expr,
            data_type: Some(data_type),
        }
    }

    /// Get the target column name.
    pub fn target_name(&self) -> &str {
        match self {
            ColumnDef::Simple(name) => name,
            ColumnDef::Renamed { target, .. } => target,
            ColumnDef::Computed { name, .. } => name,
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
    fn test_expr_column() {
        let col = Expr::column("order_id");
        assert!(matches!(col, Expr::Column { entity: None, column } if column == "order_id"));

        let qual = Expr::qualified_column("orders", "order_id");
        assert!(matches!(qual, Expr::Column { entity: Some(e), column } if e == "orders" && column == "order_id"));
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
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Add, .. }));
    }

    #[test]
    fn test_expr_comparison() {
        let expr = Expr::column("status").eq(Expr::string("active"));
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Eq, .. }));
    }

    #[test]
    fn test_expr_logical() {
        let expr = Expr::column("a")
            .gt(Expr::int(0))
            .and(Expr::column("b").lt(Expr::int(100)));
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::And, .. }));
    }

    #[test]
    fn test_expr_function() {
        let expr = Expr::func(Func::Upper, vec![Expr::column("name")]);
        match expr {
            Expr::Function { func, args } => {
                assert_eq!(func, Func::Upper);
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function"),
        }
    }

    #[test]
    fn test_expr_cast() {
        let expr = Expr::cast(Expr::column("amount"), DataType::Decimal(10, 2));
        assert!(matches!(expr, Expr::Cast { target_type: DataType::Decimal(10, 2), .. }));
    }

    #[test]
    fn test_case_when() {
        let expr = Expr::case_when(
            vec![
                WhenClause::new(
                    Expr::column("status").eq(Expr::string("active")),
                    Expr::int(1),
                ),
                WhenClause::new(
                    Expr::column("status").eq(Expr::string("pending")),
                    Expr::int(2),
                ),
            ],
            Some(Expr::int(0)),
        );
        assert!(matches!(expr, Expr::Case { operand: None, .. }));
    }

    #[test]
    fn test_func_return_type() {
        assert_eq!(Func::Upper.return_type(&[]), Some(DataType::String));
        assert_eq!(Func::Length.return_type(&[]), Some(DataType::Int64));
        assert_eq!(Func::Count.return_type(&[]), Some(DataType::Int64));
        assert_eq!(Func::Year.return_type(&[]), Some(DataType::Int32));
    }

    #[test]
    fn test_func_is_aggregate() {
        assert!(Func::Sum.is_aggregate());
        assert!(Func::Count.is_aggregate());
        assert!(!Func::Upper.is_aggregate());
        assert!(!Func::DateTrunc.is_aggregate());
    }

    #[test]
    fn test_window_frame() {
        let frame = WindowFrame::rows_preceding(7);
        assert_eq!(frame.kind, FrameKind::Rows);
        assert_eq!(frame.start, FrameBound::Preceding(7));
        assert_eq!(frame.end, Some(FrameBound::CurrentRow));
    }

    #[test]
    fn test_column_def() {
        let simple = ColumnDef::simple("order_id");
        assert_eq!(simple.target_name(), "order_id");

        let renamed = ColumnDef::renamed("cust_id", "customer_id");
        assert_eq!(renamed.target_name(), "customer_id");

        let computed = ColumnDef::computed("full_name", Expr::func(
            Func::Concat,
            vec![Expr::column("first_name"), Expr::string(" "), Expr::column("last_name")],
        ));
        assert_eq!(computed.target_name(), "full_name");
    }

    #[test]
    fn test_order_by() {
        let asc = OrderByExpr::asc(Expr::column("date"));
        assert_eq!(asc.dir, SortDir::Asc);

        let desc = OrderByExpr::desc(Expr::column("amount")).with_nulls(NullsOrder::Last);
        assert_eq!(desc.dir, SortDir::Desc);
        assert_eq!(desc.nulls, Some(NullsOrder::Last));
    }
}
