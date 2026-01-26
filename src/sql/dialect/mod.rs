//! SQL Dialect definitions and formatting rules.
//!
//! This module provides a trait-based abstraction for SQL dialect differences.
//! Each dialect implements `SqlDialect` to handle its specific syntax:
//!
//! - Identifier quoting: `"` (ANSI/PG/DuckDB), `` ` `` (MySQL), `[]` (T-SQL)
//! - Pagination: LIMIT/OFFSET vs OFFSET FETCH vs TOP
//! - Boolean literals: true/false vs 1/0
//! - String concatenation: `||` vs `+` vs CONCAT()
//! - CTE syntax: WITH RECURSIVE vs WITH
//! - And more...
//!
//! # Usage
//!
//! ```ignore
//! use mantis::dialect::{Dialect, SqlDialect};
//!
//! let dialect = Dialect::Postgres;
//! let quoted = dialect.quote_identifier("user");  // "user"
//! ```
//!
//! # Minimum Version Requirements
//!
//! Some SQL features require specific database versions:
//!
//! | Feature | PostgreSQL | SQL Server | MySQL | DuckDB | Snowflake | BigQuery |
//! |---------|-----------|------------|-------|--------|-----------|----------|
//! | MERGE | 15+ | 2008+ | ❌ | ❌ | ✓ | ✓ |
//! | CTE (WITH) | 8.4+ | 2005+ | 8.0+ | ✓ | ✓ | ✓ |
//! | Recursive CTE | 8.4+ | 2005+ | 8.0+ | ✓ | ✓ | ✓ |
//! | Window Functions | 8.4+ | 2005+ | 8.0+ | ✓ | ✓ | ✓ |
//! | GROUPS Frame | 11+ | ❌ | ❌ | ✓ | ✓ | ❌ |
//! | NULLS FIRST/LAST | 8.3+ | 2022+ | ❌ | ✓ | ✓ | ✓ |
//! | RETURNING | 8.2+ | 2005+ (OUTPUT) | ❌ | ✓ | ❌ | ❌ |
//! | Materialized View | 9.3+ | ❌ (indexed views) | ❌ | ✓ | ✓ | ✓ |
//! | QUALIFY | ❌ | ❌ | ❌ | ✓ | ✓ | ✓ |
//! | DISTINCT ON | ✓ | ❌ | ❌ | ✓ | ❌ | ❌ |
//! | FILTER Clause | 9.4+ | ❌ | ❌ | ✓ | ❌ | ❌ |
//! | Partial Indexes | ✓ | 2008+ | ❌ | ✓ | ❌ | ❌ |
//!
//! Legend: ✓ = supported, ❌ = not supported, version = minimum required
//!
//! Check dialect feature flags (e.g., `supports_merge()`, `supports_groups_frame()`)
//! before generating SQL that uses these features.

mod ansi;
mod bigquery;
mod databricks;
mod duckdb;
pub mod helpers;
mod mysql;
mod postgres;
mod redshift;
mod snowflake;
mod tsql;

// Note: Ansi is exported as a reference implementation for testing and documentation.
// It is NOT included in the Dialect enum because real databases rarely use pure ANSI SQL.
// Use DuckDb, Postgres, TSql, MySql, Snowflake, BigQuery, Redshift, or Databricks for actual query generation.
pub use ansi::Ansi;
pub use bigquery::BigQuery;
pub use databricks::Databricks;
pub use duckdb::DuckDb;
pub use mysql::MySql;
pub use postgres::Postgres;
pub use redshift::Redshift;
pub use snowflake::Snowflake;
pub use tsql::TSql;

use super::token::{Token, TokenStream};

/// SQL dialect trait - defines how SQL constructs are rendered.
///
/// Implementations handle dialect-specific syntax differences.
/// The default implementations follow ANSI SQL where possible.
pub trait SqlDialect: std::fmt::Debug {
    /// Dialect name for display/logging.
    fn name(&self) -> &'static str;

    // =========================================================================
    // Identifier and Literal Quoting
    // =========================================================================

    /// Quote an identifier (table, column, alias).
    ///
    /// - ANSI/PostgreSQL/DuckDB: `"identifier"`
    /// - MySQL: `` `identifier` ``
    /// - T-SQL: `[identifier]`
    fn quote_identifier(&self, ident: &str) -> String;

    /// Quote a string literal.
    ///
    /// All dialects use single quotes with `''` for escaping.
    /// Override for Unicode prefix (T-SQL N'...').
    fn quote_string(&self, s: &str) -> String {
        format!("'{}'", s.replace('\'', "''"))
    }

    /// Format a boolean literal.
    ///
    /// - PostgreSQL/DuckDB: `true`/`false`
    /// - MySQL/T-SQL: `1`/`0`
    fn format_bool(&self, b: bool) -> &'static str;

    /// Format a NULL literal.
    fn format_null(&self) -> &'static str {
        "NULL"
    }

    // =========================================================================
    // Pagination
    // =========================================================================

    /// Emit LIMIT/OFFSET or equivalent pagination clause.
    ///
    /// - ANSI/PostgreSQL/DuckDB/MySQL: `LIMIT n OFFSET m` (default)
    /// - T-SQL: `OFFSET m ROWS FETCH NEXT n ROWS ONLY` (override)
    fn emit_limit_offset(&self, limit: Option<u64>, offset: Option<u64>) -> TokenStream {
        use super::token::Token;

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

    /// Whether this dialect requires ORDER BY for OFFSET/LIMIT.
    ///
    /// T-SQL requires ORDER BY when using OFFSET FETCH.
    fn requires_order_by_for_offset(&self) -> bool {
        false
    }

    // =========================================================================
    // Operators
    // =========================================================================

    /// String concatenation operator or function.
    ///
    /// - ANSI/PostgreSQL/DuckDB: `||`
    /// - T-SQL: `+`
    /// - MySQL: `CONCAT()` (|| is OR by default)
    fn concat_operator(&self) -> &'static str {
        "||"
    }

    /// Whether this dialect supports the `||` concat operator.
    ///
    /// MySQL uses `||` as logical OR by default.
    fn supports_concat_operator(&self) -> bool {
        true
    }

    // =========================================================================
    // CTE (Common Table Expressions)
    // =========================================================================

    /// Whether to emit RECURSIVE keyword for recursive CTEs.
    ///
    /// T-SQL omits the RECURSIVE keyword.
    fn emit_recursive_keyword(&self) -> bool {
        true
    }

    // =========================================================================
    // JOIN Syntax
    // =========================================================================

    /// Whether this dialect supports FULL OUTER JOIN.
    fn supports_full_outer_join(&self) -> bool {
        true
    }

    /// Whether this dialect supports LATERAL joins.
    ///
    /// T-SQL uses CROSS APPLY / OUTER APPLY instead.
    fn supports_lateral(&self) -> bool {
        true
    }

    // =========================================================================
    // NULLS Ordering
    // =========================================================================

    /// Whether this dialect supports NULLS FIRST/LAST in ORDER BY.
    ///
    /// MySQL and older T-SQL versions don't support this.
    fn supports_nulls_ordering(&self) -> bool {
        true
    }

    // =========================================================================
    // Date/Time
    // =========================================================================

    /// Format a date literal.
    ///
    /// - ANSI/PostgreSQL/DuckDB: `DATE 'YYYY-MM-DD'`
    /// - T-SQL: `'YYYY-MM-DD'` (no DATE keyword)
    fn format_date_literal(&self, date: &str) -> String {
        format!("DATE '{}'", date)
    }

    // =========================================================================
    // PIVOT
    // =========================================================================

    /// Whether this dialect has native PIVOT syntax.
    ///
    /// T-SQL and DuckDB have native PIVOT.
    /// PostgreSQL/MySQL use CASE expressions.
    fn supports_native_pivot(&self) -> bool {
        false
    }

    // =========================================================================
    // Misc
    // =========================================================================

    /// Whether this dialect supports RETURNING clause.
    fn supports_returning(&self) -> bool {
        true
    }

    /// Whether this dialect supports DISTINCT ON.
    ///
    /// Only PostgreSQL and DuckDB support this.
    fn supports_distinct_on(&self) -> bool {
        false
    }

    /// Whether this dialect supports the FILTER clause for aggregates.
    ///
    /// PostgreSQL and DuckDB support `COUNT(*) FILTER (WHERE ...)`.
    fn supports_aggregate_filter(&self) -> bool {
        false
    }

    // =========================================================================
    // Window Functions
    // =========================================================================

    /// Whether this dialect supports GROUPS frame type.
    ///
    /// Only PostgreSQL and DuckDB support GROUPS.
    fn supports_groups_frame(&self) -> bool {
        false
    }

    /// Whether this dialect supports QUALIFY clause for window filtering.
    ///
    /// Only DuckDB supports QUALIFY.
    fn supports_qualify(&self) -> bool {
        false
    }

    /// Whether this dialect supports named windows (WINDOW clause).
    ///
    /// PostgreSQL and DuckDB support named windows.
    fn supports_named_windows(&self) -> bool {
        false
    }

    // =========================================================================
    // Function Remapping
    // =========================================================================

    /// Remap a function name for this dialect.
    ///
    /// Different databases use different names for the same functions:
    /// - `STRFTIME` → `TO_CHAR` (PostgreSQL) / `FORMAT` (T-SQL) / `DATE_FORMAT` (MySQL)
    /// - `NOW` → `GETDATE` (T-SQL)
    /// - `LENGTH` → `LEN` (T-SQL)
    ///
    /// Returns `Some(new_name)` if the function should be remapped, `None` to keep original.
    /// The input is matched case-insensitively.
    fn remap_function(&self, name: &str) -> Option<&'static str> {
        // Default: no remapping
        let _ = name;
        None
    }

    // =========================================================================
    // DDL Support
    // =========================================================================

    /// Emit a data type for this dialect.
    ///
    /// Different databases use different type names:
    /// - `Int` → BIGINT (default)
    /// - `String` → TEXT (PostgreSQL/DuckDB), VARCHAR(MAX) (T-SQL), TEXT (MySQL)
    fn emit_data_type(&self, dt: &crate::model::types::DataType) -> String {
        use crate::model::types::DataType;
        // TODO: Update when detailed SQL DataType is available
        // Current DataType only has: String, Int, Decimal, Float, Bool, Date, Timestamp
        match dt {
            DataType::Bool => "BOOLEAN".into(),
            DataType::Int => "BIGINT".into(),
            DataType::Float => "DOUBLE PRECISION".into(),
            DataType::Decimal => "DECIMAL(18, 2)".into(),
            DataType::String => "TEXT".into(),
            DataType::Date => "DATE".into(),
            DataType::Timestamp => "TIMESTAMP".into(),
        }
    }

    /// Emit identity/auto-increment syntax.
    ///
    /// - PostgreSQL: GENERATED ALWAYS AS IDENTITY
    /// - T-SQL: IDENTITY(start, increment)
    /// - MySQL: AUTO_INCREMENT
    /// - DuckDB: No special syntax (uses sequences)
    fn emit_identity(&self, _start: i64, _increment: i64) -> TokenStream {
        // Default: PostgreSQL-style
        let mut ts = TokenStream::new();
        ts.push(Token::Raw("GENERATED ALWAYS AS IDENTITY".into()));
        ts
    }

    /// Whether this dialect supports IF NOT EXISTS for CREATE statements.
    fn supports_if_not_exists(&self) -> bool {
        true
    }

    /// Whether this dialect supports IF EXISTS for DROP statements.
    fn supports_if_exists(&self) -> bool {
        true
    }

    /// Whether this dialect supports CASCADE on DROP TABLE.
    fn supports_drop_cascade(&self) -> bool {
        true
    }

    /// Whether this dialect supports partial indexes (WHERE clause).
    fn supports_partial_indexes(&self) -> bool {
        true
    }

    /// Whether this dialect supports INCLUDE columns in indexes.
    fn supports_include_columns(&self) -> bool {
        true
    }

    // =========================================================================
    // MERGE / TRUNCATE / VIEW Support
    // =========================================================================

    /// Whether this dialect supports native MERGE statement.
    ///
    /// - T-SQL: true (native MERGE)
    /// - PostgreSQL 15+: true (native MERGE)
    /// - MySQL: false (use INSERT...ON DUPLICATE KEY UPDATE)
    /// - DuckDB: false (use INSERT...ON CONFLICT)
    ///
    /// For dialects without MERGE support, use `Insert::on_conflict()` instead.
    fn supports_merge(&self) -> bool {
        false
    }

    /// Whether this dialect supports TRUNCATE TABLE.
    ///
    /// All major databases support TRUNCATE.
    fn supports_truncate(&self) -> bool {
        true
    }

    /// Whether this dialect supports TRUNCATE with CASCADE.
    ///
    /// - PostgreSQL: true
    /// - DuckDB: true
    /// - T-SQL: false
    /// - MySQL: false
    fn supports_truncate_cascade(&self) -> bool {
        false
    }

    /// Whether this dialect supports CREATE OR REPLACE VIEW.
    ///
    /// - PostgreSQL: true
    /// - DuckDB: true
    /// - MySQL: true
    /// - T-SQL: false (use DROP + CREATE or ALTER VIEW)
    fn supports_create_or_replace_view(&self) -> bool {
        true
    }

    /// Whether this dialect supports materialized views.
    ///
    /// - PostgreSQL: true
    /// - DuckDB: true
    /// - T-SQL: false (use indexed views instead)
    /// - MySQL: false
    fn supports_materialized_view(&self) -> bool {
        false
    }
}

/// Supported SQL dialects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    #[default]
    DuckDb,
    TSql,
    MySql,
    Postgres,
    Snowflake,
    BigQuery,
    Redshift,
    Databricks,
}

impl Dialect {
    /// Get the dialect implementation.
    pub fn dialect(&self) -> &'static dyn SqlDialect {
        match self {
            Dialect::DuckDb => &DuckDb,
            Dialect::Postgres => &Postgres,
            Dialect::TSql => &TSql,
            Dialect::MySql => &MySql,
            Dialect::Snowflake => &Snowflake,
            Dialect::BigQuery => &BigQuery,
            Dialect::Redshift => &Redshift,
            Dialect::Databricks => &Databricks,
        }
    }
}

// Implement SqlDialect for Dialect enum by delegating to concrete types
impl SqlDialect for Dialect {
    fn name(&self) -> &'static str {
        self.dialect().name()
    }

    fn quote_identifier(&self, ident: &str) -> String {
        self.dialect().quote_identifier(ident)
    }

    fn quote_string(&self, s: &str) -> String {
        self.dialect().quote_string(s)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        self.dialect().format_bool(b)
    }

    fn emit_limit_offset(&self, limit: Option<u64>, offset: Option<u64>) -> TokenStream {
        self.dialect().emit_limit_offset(limit, offset)
    }

    fn requires_order_by_for_offset(&self) -> bool {
        self.dialect().requires_order_by_for_offset()
    }

    fn concat_operator(&self) -> &'static str {
        self.dialect().concat_operator()
    }

    fn supports_concat_operator(&self) -> bool {
        self.dialect().supports_concat_operator()
    }

    fn emit_recursive_keyword(&self) -> bool {
        self.dialect().emit_recursive_keyword()
    }

    fn supports_full_outer_join(&self) -> bool {
        self.dialect().supports_full_outer_join()
    }

    fn supports_lateral(&self) -> bool {
        self.dialect().supports_lateral()
    }

    fn supports_nulls_ordering(&self) -> bool {
        self.dialect().supports_nulls_ordering()
    }

    fn format_date_literal(&self, date: &str) -> String {
        self.dialect().format_date_literal(date)
    }

    fn supports_native_pivot(&self) -> bool {
        self.dialect().supports_native_pivot()
    }

    fn supports_returning(&self) -> bool {
        self.dialect().supports_returning()
    }

    fn supports_distinct_on(&self) -> bool {
        self.dialect().supports_distinct_on()
    }

    fn supports_aggregate_filter(&self) -> bool {
        self.dialect().supports_aggregate_filter()
    }

    fn supports_groups_frame(&self) -> bool {
        self.dialect().supports_groups_frame()
    }

    fn supports_qualify(&self) -> bool {
        self.dialect().supports_qualify()
    }

    fn supports_named_windows(&self) -> bool {
        self.dialect().supports_named_windows()
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        self.dialect().remap_function(name)
    }

    fn emit_data_type(&self, dt: &crate::model::types::DataType) -> String {
        self.dialect().emit_data_type(dt)
    }

    fn emit_identity(&self, start: i64, increment: i64) -> TokenStream {
        self.dialect().emit_identity(start, increment)
    }

    fn supports_if_not_exists(&self) -> bool {
        self.dialect().supports_if_not_exists()
    }

    fn supports_if_exists(&self) -> bool {
        self.dialect().supports_if_exists()
    }

    fn supports_drop_cascade(&self) -> bool {
        self.dialect().supports_drop_cascade()
    }

    fn supports_partial_indexes(&self) -> bool {
        self.dialect().supports_partial_indexes()
    }

    fn supports_include_columns(&self) -> bool {
        self.dialect().supports_include_columns()
    }

    fn supports_merge(&self) -> bool {
        self.dialect().supports_merge()
    }

    fn supports_truncate(&self) -> bool {
        self.dialect().supports_truncate()
    }

    fn supports_truncate_cascade(&self) -> bool {
        self.dialect().supports_truncate_cascade()
    }

    fn supports_create_or_replace_view(&self) -> bool {
        self.dialect().supports_create_or_replace_view()
    }

    fn supports_materialized_view(&self) -> bool {
        self.dialect().supports_materialized_view()
    }
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.dialect().name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dialect_display() {
        assert_eq!(Dialect::DuckDb.to_string(), "duckdb");
        assert_eq!(Dialect::Postgres.to_string(), "postgres");
        assert_eq!(Dialect::TSql.to_string(), "tsql");
        assert_eq!(Dialect::MySql.to_string(), "mysql");
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(Dialect::DuckDb.quote_identifier("users"), "\"users\"");
        assert_eq!(Dialect::Postgres.quote_identifier("users"), "\"users\"");
        assert_eq!(Dialect::TSql.quote_identifier("users"), "[users]");
        assert_eq!(Dialect::MySql.quote_identifier("users"), "`users`");
    }

    #[test]
    fn test_quote_identifier_escaping() {
        assert_eq!(
            Dialect::DuckDb.quote_identifier("weird\"name"),
            "\"weird\"\"name\""
        );
        assert_eq!(
            Dialect::TSql.quote_identifier("weird]name"),
            "[weird]]name]"
        );
        assert_eq!(
            Dialect::MySql.quote_identifier("weird`name"),
            "`weird``name`"
        );
    }

    #[test]
    fn test_format_bool() {
        assert_eq!(Dialect::DuckDb.format_bool(true), "true");
        assert_eq!(Dialect::Postgres.format_bool(false), "false");
        assert_eq!(Dialect::TSql.format_bool(true), "1");
        assert_eq!(Dialect::MySql.format_bool(false), "0");
    }

    #[test]
    fn test_concat_operator() {
        assert_eq!(Dialect::DuckDb.concat_operator(), "||");
        assert_eq!(Dialect::Postgres.concat_operator(), "||");
        assert_eq!(Dialect::TSql.concat_operator(), "+");
        // MySQL uses CONCAT() function, operator returns || but shouldn't be used
        assert!(!Dialect::MySql.supports_concat_operator());
    }

    #[test]
    fn test_supports_native_pivot() {
        assert!(Dialect::DuckDb.supports_native_pivot());
        assert!(Dialect::TSql.supports_native_pivot());
        assert!(!Dialect::Postgres.supports_native_pivot());
        assert!(!Dialect::MySql.supports_native_pivot());
    }

    #[test]
    fn test_supports_distinct_on() {
        assert!(Dialect::DuckDb.supports_distinct_on());
        assert!(Dialect::Postgres.supports_distinct_on());
        assert!(!Dialect::TSql.supports_distinct_on());
        assert!(!Dialect::MySql.supports_distinct_on());
    }

    #[test]
    fn test_remap_function_datetime() {
        // STRFTIME remapping
        assert_eq!(Dialect::DuckDb.remap_function("STRFTIME"), None); // native
        assert_eq!(
            Dialect::Postgres.remap_function("STRFTIME"),
            Some("TO_CHAR")
        );
        assert_eq!(Dialect::TSql.remap_function("STRFTIME"), Some("FORMAT"));
        assert_eq!(
            Dialect::MySql.remap_function("STRFTIME"),
            Some("DATE_FORMAT")
        );

        // TO_CHAR remapping
        assert_eq!(Dialect::DuckDb.remap_function("TO_CHAR"), Some("STRFTIME"));
        assert_eq!(Dialect::Postgres.remap_function("TO_CHAR"), None); // native
        assert_eq!(Dialect::TSql.remap_function("TO_CHAR"), Some("FORMAT"));
        assert_eq!(
            Dialect::MySql.remap_function("TO_CHAR"),
            Some("DATE_FORMAT")
        );
    }

    #[test]
    fn test_remap_function_null_handling() {
        // NVL (Oracle-style) remapping
        assert_eq!(Dialect::DuckDb.remap_function("NVL"), Some("COALESCE"));
        assert_eq!(Dialect::Postgres.remap_function("NVL"), Some("COALESCE"));
        assert_eq!(Dialect::TSql.remap_function("NVL"), Some("ISNULL"));
        assert_eq!(Dialect::MySql.remap_function("NVL"), Some("IFNULL"));

        // IFNULL remapping
        assert_eq!(Dialect::DuckDb.remap_function("IFNULL"), Some("COALESCE"));
        assert_eq!(Dialect::TSql.remap_function("IFNULL"), Some("ISNULL"));
    }

    #[test]
    fn test_remap_function_string() {
        // LENGTH remapping
        assert_eq!(Dialect::TSql.remap_function("LENGTH"), Some("LEN"));
        assert_eq!(Dialect::Postgres.remap_function("LENGTH"), None); // native
        assert_eq!(Dialect::MySql.remap_function("LENGTH"), None); // native
    }

    #[test]
    fn test_remap_function_case_insensitive() {
        assert_eq!(Dialect::TSql.remap_function("length"), Some("LEN"));
        assert_eq!(Dialect::TSql.remap_function("LENGTH"), Some("LEN"));
        assert_eq!(Dialect::TSql.remap_function("Length"), Some("LEN"));
    }

    #[test]
    fn test_remap_function_unknown() {
        // Unknown functions should return None (no remapping)
        assert_eq!(Dialect::DuckDb.remap_function("CUSTOM_FUNC"), None);
        assert_eq!(Dialect::Postgres.remap_function("CUSTOM_FUNC"), None);
        assert_eq!(Dialect::TSql.remap_function("CUSTOM_FUNC"), None);
        assert_eq!(Dialect::MySql.remap_function("CUSTOM_FUNC"), None);
    }

    // =========================================================================
    // New Dialect Tests
    // =========================================================================

    #[test]
    fn test_new_dialect_display() {
        assert_eq!(Dialect::Snowflake.to_string(), "snowflake");
        assert_eq!(Dialect::BigQuery.to_string(), "bigquery");
        assert_eq!(Dialect::Redshift.to_string(), "redshift");
        assert_eq!(Dialect::Databricks.to_string(), "databricks");
    }

    #[test]
    fn test_new_dialect_quoting() {
        // Snowflake and Redshift use double quotes (ANSI style)
        assert_eq!(Dialect::Snowflake.quote_identifier("users"), "\"users\"");
        assert_eq!(Dialect::Redshift.quote_identifier("users"), "\"users\"");

        // BigQuery and Databricks use backticks
        assert_eq!(Dialect::BigQuery.quote_identifier("users"), "`users`");
        assert_eq!(Dialect::Databricks.quote_identifier("users"), "`users`");
    }

    #[test]
    fn test_new_dialect_quoting_escaping() {
        // Double quote escaping
        assert_eq!(
            Dialect::Snowflake.quote_identifier("weird\"name"),
            "\"weird\"\"name\""
        );
        assert_eq!(
            Dialect::Redshift.quote_identifier("weird\"name"),
            "\"weird\"\"name\""
        );

        // Backtick escaping
        assert_eq!(
            Dialect::BigQuery.quote_identifier("weird`name"),
            "`weird``name`"
        );
        assert_eq!(
            Dialect::Databricks.quote_identifier("weird`name"),
            "`weird``name`"
        );
    }

    #[test]
    fn test_new_dialect_bool_format() {
        // All new dialects use true/false literals
        assert_eq!(Dialect::Snowflake.format_bool(true), "true");
        assert_eq!(Dialect::Snowflake.format_bool(false), "false");
        assert_eq!(Dialect::BigQuery.format_bool(true), "true");
        assert_eq!(Dialect::Redshift.format_bool(true), "true");
        assert_eq!(Dialect::Databricks.format_bool(true), "true");
    }

    #[test]
    fn test_new_dialect_features() {
        // Snowflake features
        assert!(Dialect::Snowflake.supports_qualify());
        assert!(Dialect::Snowflake.supports_native_pivot());
        assert!(Dialect::Snowflake.supports_merge());
        assert!(Dialect::Snowflake.supports_groups_frame());

        // BigQuery features
        assert!(Dialect::BigQuery.supports_qualify());
        assert!(Dialect::BigQuery.supports_merge());
        assert!(Dialect::BigQuery.supports_materialized_view());
        assert!(!Dialect::BigQuery.supports_returning());
        assert!(!Dialect::BigQuery.supports_native_pivot());

        // Redshift features (Postgres-based but limited)
        assert!(Dialect::Redshift.supports_distinct_on());
        assert!(Dialect::Redshift.supports_materialized_view());
        assert!(!Dialect::Redshift.supports_merge());
        assert!(!Dialect::Redshift.supports_returning());
        assert!(!Dialect::Redshift.supports_qualify());
        assert!(!Dialect::Redshift.supports_lateral());

        // Databricks features
        assert!(Dialect::Databricks.supports_native_pivot());
        assert!(Dialect::Databricks.supports_merge());
        assert!(Dialect::Databricks.supports_qualify());
        assert!(!Dialect::Databricks.supports_distinct_on());
        assert!(!Dialect::Databricks.supports_returning());
    }

    #[test]
    fn test_new_dialect_function_remapping() {
        // Snowflake uses NVL natively
        assert_eq!(Dialect::Snowflake.remap_function("NVL"), None);
        assert_eq!(Dialect::Snowflake.remap_function("IFNULL"), Some("NVL"));
        assert_eq!(
            Dialect::Snowflake.remap_function("STRFTIME"),
            Some("TO_CHAR")
        );

        // BigQuery uses FORMAT_TIMESTAMP and IFNULL
        assert_eq!(
            Dialect::BigQuery.remap_function("STRFTIME"),
            Some("FORMAT_TIMESTAMP")
        );
        assert_eq!(Dialect::BigQuery.remap_function("NVL"), Some("IFNULL"));
        assert_eq!(
            Dialect::BigQuery.remap_function("LENGTH"),
            Some("CHAR_LENGTH")
        );

        // Redshift uses Postgres remapping
        assert_eq!(
            Dialect::Redshift.remap_function("STRFTIME"),
            Some("TO_CHAR")
        );
        assert_eq!(Dialect::Redshift.remap_function("NVL"), Some("COALESCE"));

        // Databricks uses DATE_FORMAT and COALESCE
        assert_eq!(
            Dialect::Databricks.remap_function("STRFTIME"),
            Some("DATE_FORMAT")
        );
        assert_eq!(Dialect::Databricks.remap_function("NVL"), Some("COALESCE"));
    }
}
