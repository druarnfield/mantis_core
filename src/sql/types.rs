//! SQL-level data types for DDL generation.
//!
//! This module provides a comprehensive SQL DataType enum that maps to actual
//! database column types. This is distinct from the DSL-level DataType which
//! represents semantic types (String, Int, Decimal, Float, Bool, Date, Timestamp).
//!
//! The SQL-level DataType provides precise control over:
//! - Integer sizes (Int8, Int16, Int32, Int64)
//! - Floating point sizes (Float32, Float64)
//! - Decimal precision and scale
//! - String length constraints (Char, Varchar)
//! - Time types with and without timezone
//! - Binary, JSON, and UUID types

use std::fmt;

/// SQL-level data type for DDL generation.
///
/// This enum represents the actual SQL data types used in CREATE TABLE statements.
/// It provides more granular control than the DSL-level DataType.
///
/// # Examples
///
/// ```ignore
/// use mantis::sql::types::DataType;
///
/// let int_type = DataType::Int64;
/// let varchar_type = DataType::Varchar(255);
/// let decimal_type = DataType::Decimal(18, 2);
///
/// // Parse from strings
/// let parsed = DataType::parse("decimal(10,2)").unwrap();
/// assert_eq!(parsed, DataType::Decimal(10, 2));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Boolean type.
    Bool,

    /// 8-bit signed integer (TINYINT in most databases).
    Int8,

    /// 16-bit signed integer (SMALLINT).
    Int16,

    /// 32-bit signed integer (INT/INTEGER).
    Int32,

    /// 64-bit signed integer (BIGINT).
    Int64,

    /// 32-bit floating point (REAL/FLOAT4).
    Float32,

    /// 64-bit floating point (DOUBLE PRECISION/FLOAT8).
    Float64,

    /// Fixed-precision decimal with precision and scale.
    /// - First parameter: precision (total digits)
    /// - Second parameter: scale (digits after decimal point)
    Decimal(u8, u8),

    /// Variable-length string (TEXT, VARCHAR without limit).
    String,

    /// Fixed-length character string.
    Char(u16),

    /// Variable-length character string with maximum length.
    Varchar(u16),

    /// Date without time.
    Date,

    /// Time without date or timezone.
    Time,

    /// Timestamp without timezone.
    Timestamp,

    /// Timestamp with timezone.
    TimestampTz,

    /// Binary data (BLOB, BYTEA, VARBINARY).
    Binary,

    /// JSON data type.
    Json,

    /// UUID/GUID type.
    Uuid,
}

impl DataType {
    /// Parse a SQL data type from a string.
    ///
    /// Supports common SQL type names and syntax:
    /// - `bool`, `boolean`
    /// - `tinyint`, `int8`
    /// - `smallint`, `int16`
    /// - `int`, `integer`, `int32`
    /// - `bigint`, `int64`
    /// - `real`, `float4`, `float32`
    /// - `double`, `float8`, `float64`, `double precision`
    /// - `decimal(p,s)`, `numeric(p,s)`
    /// - `text`, `string`
    /// - `char(n)`, `character(n)`
    /// - `varchar(n)`, `character varying(n)`
    /// - `date`
    /// - `time`
    /// - `timestamp`
    /// - `timestamptz`, `timestamp with time zone`
    /// - `binary`, `blob`, `bytea`, `varbinary`
    /// - `json`, `jsonb`
    /// - `uuid`, `guid`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use mantis::sql::types::DataType;
    ///
    /// assert_eq!(DataType::parse("bigint"), Some(DataType::Int64));
    /// assert_eq!(DataType::parse("varchar(255)"), Some(DataType::Varchar(255)));
    /// assert_eq!(DataType::parse("decimal(18,2)"), Some(DataType::Decimal(18, 2)));
    /// ```
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().to_lowercase();

        // Handle types with parameters first
        if let Some(inner) = extract_parens(&s, "decimal")
            .or_else(|| extract_parens(&s, "numeric"))
        {
            return parse_decimal_params(&inner);
        }

        if let Some(inner) = extract_parens(&s, "varchar")
            .or_else(|| extract_parens(&s, "character varying"))
            .or_else(|| extract_parens(&s, "nvarchar"))
        {
            return parse_length_param(&inner).map(DataType::Varchar);
        }

        if let Some(inner) = extract_parens(&s, "char")
            .or_else(|| extract_parens(&s, "character"))
            .or_else(|| extract_parens(&s, "nchar"))
        {
            return parse_length_param(&inner).map(DataType::Char);
        }

        // Handle simple type names
        match s.as_str() {
            // Boolean
            "bool" | "boolean" | "bit" => Some(DataType::Bool),

            // Integers
            "tinyint" | "int8" => Some(DataType::Int8),
            "smallint" | "int16" | "int2" => Some(DataType::Int16),
            "int" | "integer" | "int32" | "int4" => Some(DataType::Int32),
            "bigint" | "int64" => Some(DataType::Int64),

            // Floating point
            "real" | "float4" | "float32" => Some(DataType::Float32),
            "double" | "float8" | "float64" | "double precision" | "float" => {
                Some(DataType::Float64)
            }

            // Decimal without parameters (use default precision)
            "decimal" | "numeric" | "number" => Some(DataType::Decimal(18, 2)),

            // String
            "text" | "string" | "clob" | "ntext" => Some(DataType::String),
            "varchar" | "nvarchar" => Some(DataType::String), // Without length = unbounded

            // Date/Time
            "date" => Some(DataType::Date),
            "time" => Some(DataType::Time),
            "timestamp" | "datetime" | "datetime2" => Some(DataType::Timestamp),
            "timestamptz" | "timestamp with time zone" | "datetimeoffset" => {
                Some(DataType::TimestampTz)
            }

            // Binary
            "binary" | "blob" | "bytea" | "varbinary" | "image" => Some(DataType::Binary),

            // JSON
            "json" | "jsonb" => Some(DataType::Json),

            // UUID
            "uuid" | "guid" | "uniqueidentifier" => Some(DataType::Uuid),

            _ => None,
        }
    }

    /// Returns true if this is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal(_, _)
        )
    }

    /// Returns true if this is an integer type.
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    }

    /// Returns true if this is a floating point type.
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    /// Returns true if this is a string/text type.
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::String | DataType::Char(_) | DataType::Varchar(_)
        )
    }

    /// Returns true if this is a temporal (date/time) type.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::TimestampTz
        )
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOLEAN"),
            DataType::Int8 => write!(f, "TINYINT"),
            DataType::Int16 => write!(f, "SMALLINT"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float32 => write!(f, "REAL"),
            DataType::Float64 => write!(f, "DOUBLE PRECISION"),
            DataType::Decimal(p, s) => write!(f, "DECIMAL({}, {})", p, s),
            DataType::String => write!(f, "TEXT"),
            DataType::Char(n) => write!(f, "CHAR({})", n),
            DataType::Varchar(n) => write!(f, "VARCHAR({})", n),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            DataType::Binary => write!(f, "BYTEA"),
            DataType::Json => write!(f, "JSON"),
            DataType::Uuid => write!(f, "UUID"),
        }
    }
}

/// Extract content inside parentheses for a given type prefix.
/// e.g., extract_parens("decimal(10,2)", "decimal") returns Some("10,2")
fn extract_parens(s: &str, prefix: &str) -> Option<String> {
    let s = s.trim();
    if !s.starts_with(prefix) {
        return None;
    }

    let rest = s[prefix.len()..].trim();
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return None;
    }

    Some(rest[1..rest.len() - 1].to_string())
}

/// Parse decimal parameters "precision,scale" or "precision, scale".
fn parse_decimal_params(inner: &str) -> Option<DataType> {
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return None;
    }

    let precision: u8 = parts[0].parse().ok()?;
    let scale: u8 = parts[1].parse().ok()?;

    Some(DataType::Decimal(precision, scale))
}

/// Parse a single length parameter.
fn parse_length_param(inner: &str) -> Option<u16> {
    let inner = inner.trim();
    // Handle "max" keyword used in T-SQL
    if inner.eq_ignore_ascii_case("max") {
        return Some(u16::MAX);
    }
    inner.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_types() {
        assert_eq!(DataType::parse("bool"), Some(DataType::Bool));
        assert_eq!(DataType::parse("boolean"), Some(DataType::Bool));
        assert_eq!(DataType::parse("BOOLEAN"), Some(DataType::Bool));

        assert_eq!(DataType::parse("tinyint"), Some(DataType::Int8));
        assert_eq!(DataType::parse("smallint"), Some(DataType::Int16));
        assert_eq!(DataType::parse("int"), Some(DataType::Int32));
        assert_eq!(DataType::parse("integer"), Some(DataType::Int32));
        assert_eq!(DataType::parse("bigint"), Some(DataType::Int64));

        assert_eq!(DataType::parse("real"), Some(DataType::Float32));
        assert_eq!(DataType::parse("double"), Some(DataType::Float64));
        assert_eq!(DataType::parse("double precision"), Some(DataType::Float64));

        assert_eq!(DataType::parse("text"), Some(DataType::String));
        assert_eq!(DataType::parse("date"), Some(DataType::Date));
        assert_eq!(DataType::parse("timestamp"), Some(DataType::Timestamp));
        assert_eq!(DataType::parse("timestamptz"), Some(DataType::TimestampTz));

        assert_eq!(DataType::parse("json"), Some(DataType::Json));
        assert_eq!(DataType::parse("uuid"), Some(DataType::Uuid));
        assert_eq!(DataType::parse("bytea"), Some(DataType::Binary));
    }

    #[test]
    fn test_parse_parameterized_types() {
        assert_eq!(
            DataType::parse("decimal(10,2)"),
            Some(DataType::Decimal(10, 2))
        );
        assert_eq!(
            DataType::parse("decimal(18, 4)"),
            Some(DataType::Decimal(18, 4))
        );
        assert_eq!(
            DataType::parse("NUMERIC(38,0)"),
            Some(DataType::Decimal(38, 0))
        );

        assert_eq!(DataType::parse("varchar(255)"), Some(DataType::Varchar(255)));
        assert_eq!(DataType::parse("VARCHAR(100)"), Some(DataType::Varchar(100)));
        assert_eq!(DataType::parse("nvarchar(max)"), Some(DataType::Varchar(u16::MAX)));

        assert_eq!(DataType::parse("char(10)"), Some(DataType::Char(10)));
        assert_eq!(DataType::parse("CHAR(1)"), Some(DataType::Char(1)));
    }

    #[test]
    fn test_parse_dialect_specific() {
        // T-SQL types
        assert_eq!(DataType::parse("bit"), Some(DataType::Bool));
        assert_eq!(DataType::parse("datetime2"), Some(DataType::Timestamp));
        assert_eq!(
            DataType::parse("datetimeoffset"),
            Some(DataType::TimestampTz)
        );
        assert_eq!(
            DataType::parse("uniqueidentifier"),
            Some(DataType::Uuid)
        );

        // MySQL types
        assert_eq!(DataType::parse("datetime"), Some(DataType::Timestamp));

        // PostgreSQL types
        assert_eq!(DataType::parse("int4"), Some(DataType::Int32));
        assert_eq!(DataType::parse("int8"), Some(DataType::Int64));
        assert_eq!(DataType::parse("float8"), Some(DataType::Float64));
        assert_eq!(DataType::parse("jsonb"), Some(DataType::Json));
    }

    #[test]
    fn test_parse_invalid() {
        assert_eq!(DataType::parse("unknown_type"), None);
        assert_eq!(DataType::parse(""), None);
        assert_eq!(DataType::parse("decimal(abc)"), None);
        assert_eq!(DataType::parse("varchar()"), None);
    }

    #[test]
    fn test_type_predicates() {
        assert!(DataType::Int64.is_numeric());
        assert!(DataType::Int64.is_integer());
        assert!(!DataType::Int64.is_float());

        assert!(DataType::Float64.is_numeric());
        assert!(DataType::Float64.is_float());
        assert!(!DataType::Float64.is_integer());

        assert!(DataType::Decimal(18, 2).is_numeric());
        assert!(!DataType::Decimal(18, 2).is_integer());
        assert!(!DataType::Decimal(18, 2).is_float());

        assert!(DataType::String.is_string());
        assert!(DataType::Varchar(255).is_string());
        assert!(DataType::Char(10).is_string());

        assert!(DataType::Date.is_temporal());
        assert!(DataType::Timestamp.is_temporal());
        assert!(DataType::TimestampTz.is_temporal());
    }

    #[test]
    fn test_display() {
        assert_eq!(DataType::Bool.to_string(), "BOOLEAN");
        assert_eq!(DataType::Int64.to_string(), "BIGINT");
        assert_eq!(DataType::Decimal(18, 2).to_string(), "DECIMAL(18, 2)");
        assert_eq!(DataType::Varchar(255).to_string(), "VARCHAR(255)");
        assert_eq!(DataType::TimestampTz.to_string(), "TIMESTAMP WITH TIME ZONE");
    }
}
