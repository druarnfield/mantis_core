//! Shared helper functions for SQL dialect implementations.
//!
//! This module provides reusable building blocks that dialects can compose
//! to implement the `SqlDialect` trait with minimal duplication.

// =============================================================================
// Identifier Quoting
// =============================================================================

/// Quote identifier with double quotes (ANSI style).
/// Used by: Postgres, DuckDB, Snowflake, Redshift, BigQuery (standard mode)
pub fn quote_double(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote identifier with backticks.
/// Used by: MySQL, BigQuery (legacy mode), Spark/Databricks
pub fn quote_backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Quote identifier with square brackets.
/// Used by: T-SQL (SQL Server, Azure Synapse)
pub fn quote_bracket(ident: &str) -> String {
    format!("[{}]", ident.replace(']', "]]"))
}

// =============================================================================
// String Quoting
// =============================================================================

/// Quote string with single quotes (standard SQL).
/// Used by: All dialects
pub fn quote_string_single(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Quote string with N prefix for Unicode (T-SQL).
/// Used by: T-SQL for non-ASCII strings
pub fn quote_string_unicode(s: &str) -> String {
    format!("N'{}'", s.replace('\'', "''"))
}

// =============================================================================
// Boolean Formatting
// =============================================================================

/// Format boolean as literal true/false.
/// Used by: Postgres, DuckDB, Snowflake, BigQuery, Spark
pub fn format_bool_literal(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// Format boolean as numeric 1/0.
/// Used by: T-SQL, MySQL
pub fn format_bool_numeric(b: bool) -> &'static str {
    if b {
        "1"
    } else {
        "0"
    }
}

// =============================================================================
// Pagination
// =============================================================================

use super::super::token::{Token, TokenStream};

/// Emit LIMIT ... OFFSET ... (standard SQL).
/// Used by: Postgres, DuckDB, MySQL, Snowflake, BigQuery, Spark
pub fn emit_limit_offset_standard(limit: Option<u64>, offset: Option<u64>) -> TokenStream {
    let mut ts = TokenStream::new();

    if let Some(lim) = limit {
        ts.push(Token::Limit)
            .space()
            .push(Token::LitInt(lim as i64));
    }

    if let Some(off) = offset {
        if limit.is_some() {
            ts.space();
        }
        ts.push(Token::Offset)
            .space()
            .push(Token::LitInt(off as i64));
    }

    ts
}

/// Emit OFFSET ... ROWS FETCH NEXT ... ROWS ONLY (T-SQL style).
/// Used by: T-SQL (SQL Server, Azure Synapse)
/// Note: Requires ORDER BY clause in T-SQL
pub fn emit_limit_offset_tsql(limit: Option<u64>, offset: Option<u64>) -> TokenStream {
    let mut ts = TokenStream::new();

    let off = offset.unwrap_or(0);
    ts.push(Token::Offset)
        .space()
        .push(Token::LitInt(off as i64))
        .space()
        .push(Token::Rows);

    if let Some(lim) = limit {
        ts.space()
            .push(Token::Fetch)
            .space()
            .push(Token::Next)
            .space()
            .push(Token::LitInt(lim as i64))
            .space()
            .push(Token::Rows)
            .space()
            .push(Token::Only);
    }

    ts
}

// =============================================================================
// Function Remapping
// =============================================================================

/// Remap functions for Postgres dialect.
pub fn remap_function_postgres(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "STRFTIME" => Some("TO_CHAR"),
        "DATE_FORMAT" => Some("TO_CHAR"),
        "FORMAT" => Some("TO_CHAR"),
        "NVL" => Some("COALESCE"),
        "IFNULL" => Some("COALESCE"),
        "ISNULL" => Some("COALESCE"),
        _ => None,
    }
}

/// Remap functions for DuckDB dialect.
pub fn remap_function_duckdb(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "TO_CHAR" => Some("STRFTIME"),
        "DATE_FORMAT" => Some("STRFTIME"),
        "FORMAT" => Some("STRFTIME"),
        "NVL" => Some("COALESCE"),
        "IFNULL" => Some("COALESCE"),
        "ISNULL" => Some("COALESCE"),
        _ => None,
    }
}

/// Remap functions for MySQL dialect.
pub fn remap_function_mysql(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "STRFTIME" => Some("DATE_FORMAT"),
        "TO_CHAR" => Some("DATE_FORMAT"),
        "NOW" => None, // NOW() works in MySQL
        "NVL" => Some("IFNULL"),
        "ISNULL" => Some("IFNULL"),
        "SUBSTR" => Some("SUBSTRING"),
        _ => None,
    }
}

/// Remap functions for T-SQL dialect.
pub fn remap_function_tsql(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "LENGTH" => Some("LEN"),
        "SUBSTR" => Some("SUBSTRING"),
        "NOW" => Some("GETDATE"),
        "CURRENT_TIMESTAMP" => Some("GETDATE"),
        "STRFTIME" => Some("FORMAT"),
        "TO_CHAR" => Some("FORMAT"),
        "DATE_FORMAT" => Some("FORMAT"),
        "NVL" => Some("ISNULL"),
        "IFNULL" => Some("ISNULL"),
        _ => None,
    }
}

/// Remap functions for Snowflake dialect.
pub fn remap_function_snowflake(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "STRFTIME" => Some("TO_CHAR"),
        "DATE_FORMAT" => Some("TO_CHAR"),
        "NVL" => None, // NVL is native to Snowflake
        "IFNULL" => Some("NVL"),
        "ISNULL" => Some("NVL"),
        "COALESCE" => None, // Both work
        _ => None,
    }
}

/// Remap functions for BigQuery dialect.
pub fn remap_function_bigquery(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "STRFTIME" => Some("FORMAT_TIMESTAMP"),
        "TO_CHAR" => Some("FORMAT_TIMESTAMP"),
        "DATE_FORMAT" => Some("FORMAT_TIMESTAMP"),
        "NVL" => Some("IFNULL"),
        "ISNULL" => Some("IFNULL"),
        "LENGTH" => Some("CHAR_LENGTH"),
        _ => None,
    }
}

/// Remap functions for Redshift dialect.
/// Redshift is Postgres-based, so we delegate to Postgres remapping.
pub fn remap_function_redshift(name: &str) -> Option<&'static str> {
    remap_function_postgres(name)
}

/// Remap functions for Databricks (Spark SQL) dialect.
pub fn remap_function_databricks(name: &str) -> Option<&'static str> {
    match name.to_uppercase().as_str() {
        "TO_CHAR" => Some("DATE_FORMAT"),
        "STRFTIME" => Some("DATE_FORMAT"),
        "NVL" => Some("COALESCE"),
        "ISNULL" => Some("COALESCE"),
        "IFNULL" => Some("COALESCE"),
        _ => None,
    }
}

// =============================================================================
// Data Type Emission
// =============================================================================

use crate::model::types::DataType;

/// Emit data type for ANSI/Postgres style.
/// Used by: Postgres, DuckDB, Snowflake, Redshift
pub fn emit_data_type_ansi(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "TEXT".into(),
        DataType::Char(len) => format!("CHAR({})", len),
        DataType::Varchar(len) => format!("VARCHAR({})", len),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "TIMESTAMP".into(),
        DataType::TimestampTz => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::Binary => "BYTEA".into(),
        DataType::Json => "JSON".into(),
        DataType::Uuid => "UUID".into(),
    }
}

/// Emit data type for MySQL.
pub fn emit_data_type_mysql(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "TINYINT(1)".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "TEXT".into(),
        DataType::Char(len) => format!("CHAR({})", len),
        DataType::Varchar(len) => format!("VARCHAR({})", len),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "DATETIME".into(),
        DataType::TimestampTz => "DATETIME".into(),
        DataType::Binary => "BLOB".into(),
        DataType::Json => "JSON".into(),
        DataType::Uuid => "CHAR(36)".into(),
    }
}

/// Emit data type for T-SQL.
pub fn emit_data_type_tsql(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BIT".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "FLOAT".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "NVARCHAR(MAX)".into(),
        DataType::Char(len) => format!("NCHAR({})", len),
        DataType::Varchar(len) => format!("NVARCHAR({})", len),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "DATETIME2".into(),
        DataType::TimestampTz => "DATETIMEOFFSET".into(),
        DataType::Binary => "VARBINARY(MAX)".into(),
        DataType::Json => "NVARCHAR(MAX)".into(),
        DataType::Uuid => "UNIQUEIDENTIFIER".into(),
    }
}

/// Emit data type for Snowflake.
pub fn emit_data_type_snowflake(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal(p, s) => format!("NUMBER({}, {})", p, s),
        DataType::String => "VARCHAR".into(),
        DataType::Char(len) => format!("CHAR({})", len),
        DataType::Varchar(len) => format!("VARCHAR({})", len),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "TIMESTAMP_NTZ".into(),
        DataType::TimestampTz => "TIMESTAMP_TZ".into(),
        DataType::Binary => "BINARY".into(),
        DataType::Json => "VARIANT".into(),
        DataType::Uuid => "VARCHAR(36)".into(),
    }
}

/// Emit data type for BigQuery.
pub fn emit_data_type_bigquery(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOL".into(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "INT64".into(),
        DataType::Float32 | DataType::Float64 => "FLOAT64".into(),
        DataType::Decimal(p, s) => format!("NUMERIC({}, {})", p, s),
        DataType::String => "STRING".into(),
        DataType::Char(len) => format!("STRING({})", len),
        DataType::Varchar(len) => format!("STRING({})", len),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "TIMESTAMP".into(),
        DataType::TimestampTz => "TIMESTAMP".into(),
        DataType::Binary => "BYTES".into(),
        DataType::Json => "JSON".into(),
        DataType::Uuid => "STRING".into(),
    }
}

/// Emit data type for Databricks (Spark SQL).
pub fn emit_data_type_databricks(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "STRING".into(),
        DataType::Char(len) => format!("CHAR({})", len),
        DataType::Varchar(len) => format!("VARCHAR({})", len),
        DataType::Date => "DATE".into(),
        DataType::Time => "STRING".into(), // Databricks has no TIME type
        DataType::Timestamp => "TIMESTAMP".into(),
        DataType::TimestampTz => "TIMESTAMP".into(),
        DataType::Binary => "BINARY".into(),
        DataType::Json => "STRING".into(), // Use STRING for JSON
        DataType::Uuid => "STRING".into(),
    }
}

// Redshift uses emit_data_type_ansi (already exists)

// =============================================================================
// Identity / Auto-Increment
// =============================================================================

/// Emit identity for Postgres (GENERATED ALWAYS AS IDENTITY).
pub fn emit_identity_postgres(_start: i64, _increment: i64) -> TokenStream {
    let mut ts = TokenStream::new();
    ts.push(Token::Raw("GENERATED ALWAYS AS IDENTITY".into()));
    ts
}

/// Emit identity for T-SQL (IDENTITY(start, increment)).
pub fn emit_identity_tsql(start: i64, increment: i64) -> TokenStream {
    let mut ts = TokenStream::new();
    ts.push(Token::Raw(format!("IDENTITY({}, {})", start, increment)));
    ts
}

/// Emit identity for MySQL (AUTO_INCREMENT).
pub fn emit_identity_mysql(_start: i64, _increment: i64) -> TokenStream {
    let mut ts = TokenStream::new();
    ts.push(Token::Raw("AUTO_INCREMENT".into()));
    ts
}

/// Emit identity for Snowflake (AUTOINCREMENT).
pub fn emit_identity_snowflake(_start: i64, _increment: i64) -> TokenStream {
    let mut ts = TokenStream::new();
    ts.push(Token::Raw("AUTOINCREMENT".into()));
    ts
}
