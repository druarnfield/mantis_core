//! Test utilities for SQL emission validation.
//!
//! Provides helpers for validating that emitted SQL is syntactically correct
//! using sqlparser-rs for roundtrip validation.

use sqlparser::dialect::{
    DuckDbDialect, GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;

use super::dialect::Dialect;

/// Validates that a SQL string is syntactically valid for the given dialect.
///
/// Uses sqlparser-rs to parse the SQL and returns an error if parsing fails.
/// This provides roundtrip validation to ensure emitted SQL is always valid.
///
/// # Example
///
/// ```ignore
/// use crate::sql::test_utils::validate_sql;
/// use crate::sql::dialect::Dialect;
///
/// let sql = "SELECT * FROM users";
/// validate_sql(sql, Dialect::Postgres).unwrap();
/// ```
pub fn validate_sql(sql: &str, dialect: Dialect) -> Result<(), String> {
    let parser_dialect: Box<dyn sqlparser::dialect::Dialect> = match dialect {
        Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        Dialect::DuckDb => Box::new(DuckDbDialect {}),
        Dialect::MySql => Box::new(MySqlDialect {}),
        Dialect::TSql => Box::new(MsSqlDialect {}),
        Dialect::Snowflake => Box::new(SnowflakeDialect {}),
        Dialect::BigQuery => Box::new(GenericDialect {}), // sqlparser has no BigQuery dialect
        Dialect::Redshift => Box::new(PostgreSqlDialect {}), // Redshift is Postgres-like
        Dialect::Databricks => Box::new(GenericDialect {}), // sqlparser has no Databricks dialect
    };

    Parser::parse_sql(&*parser_dialect, sql)
        .map(|_| ())
        .map_err(|e| format!("Invalid SQL for {:?}: {}\nSQL: {}", dialect, e, sql))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_valid_sql() {
        validate_sql("SELECT * FROM users", Dialect::Postgres).unwrap();
        validate_sql("SELECT * FROM users", Dialect::MySql).unwrap();
        validate_sql("SELECT * FROM users", Dialect::DuckDb).unwrap();
    }

    #[test]
    fn test_validate_invalid_sql() {
        let result = validate_sql("SELEC * FORM users", Dialect::Postgres);
        assert!(result.is_err());
    }
}
