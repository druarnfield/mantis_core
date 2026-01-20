//! SQL expression parsing and validation using sqlparser-rs.
//!
//! This module provides optional SQL expression validation for the DSL.
//! It wraps SQL expressions in `SELECT ...` and attempts to parse them
//! using sqlparser-rs to verify they are valid SQL.
//!
//! # Important Notes
//!
//! - **Atom references**: The DSL uses `@column` syntax for atom references
//!   (e.g., `sum(@revenue)`). Interestingly, this IS syntactically valid SQL
//!   in GenericDialect because SQL Server uses `@` for variables. While the
//!   validation will pass, these atom references still need to be transformed
//!   during SQL generation to reference the actual column names.
//!
//! - **GenericDialect**: We use sqlparser's `GenericDialect` since we don't
//!   know what database the user will target.
//!
//! - **Optional validation**: This validation is optional and not integrated
//!   into the main parse pipeline. It's available for users who want to
//!   validate SQL expressions.

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::span::Span;

/// A validated SQL expression.
///
/// Contains the raw SQL text, source location, and validation results.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedSqlExpr {
    /// The raw SQL expression text.
    pub raw: String,
    /// The span in the source code.
    pub span: Span,
    /// Whether the expression is valid SQL.
    pub is_valid: bool,
    /// Error message if validation failed.
    pub error: Option<String>,
}

impl ValidatedSqlExpr {
    /// Validate a SQL expression.
    ///
    /// Wraps the expression in `SELECT ...` and attempts to parse it
    /// using sqlparser-rs. Returns a `ValidatedSqlExpr` with validation results.
    ///
    /// # Example
    ///
    /// ```
    /// use mantis::dsl::sql_expr::ValidatedSqlExpr;
    ///
    /// let expr = ValidatedSqlExpr::validate("sum(amount)".to_string(), 0..12);
    /// assert!(expr.is_valid);
    ///
    /// let bad_expr = ValidatedSqlExpr::validate("sum(".to_string(), 0..4);
    /// assert!(!bad_expr.is_valid);
    /// ```
    pub fn validate(raw: String, span: Span) -> Self {
        let dialect = GenericDialect {};

        // Wrap the expression in SELECT to make it a complete statement
        let sql = format!("SELECT {}", raw);

        // Try to parse the SQL
        match Parser::parse_sql(&dialect, &sql) {
            Ok(_) => {
                // Successfully parsed
                ValidatedSqlExpr {
                    raw,
                    span,
                    is_valid: true,
                    error: None,
                }
            }
            Err(e) => {
                // Parse failed
                ValidatedSqlExpr {
                    raw,
                    span,
                    is_valid: false,
                    error: Some(format!("SQL parse error: {}", e)),
                }
            }
        }
    }
}

/// Extract SQL from source code given a span.
///
/// This helper function extracts the substring from the source that
/// corresponds to the given span.
///
/// # Arguments
///
/// * `source` - The full source code string
/// * `span` - The span indicating which part to extract
///
/// # Returns
///
/// The extracted SQL string, or `None` if the span is out of bounds.
///
/// # Example
///
/// ```
/// use mantis::dsl::sql_expr::extract_sql_from_source;
///
/// let source = "{ sum(amount) }";
/// let sql = extract_sql_from_source(source, &(2..13));
/// assert_eq!(sql, Some("sum(amount)".to_string()));
/// ```
pub fn extract_sql_from_source(source: &str, span: &Span) -> Option<String> {
    if span.end > source.len() || span.start > span.end {
        return None;
    }
    Some(source[span.start..span.end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_simple_expression() {
        // Simple aggregate function should be valid
        let expr = ValidatedSqlExpr::validate("sum(amount)".to_string(), 0..11);
        assert!(expr.is_valid, "sum(amount) should be valid SQL");
        assert!(expr.error.is_none());
        assert_eq!(expr.raw, "sum(amount)");
    }

    #[test]
    fn test_validate_complex_expression() {
        // CASE expression should be valid
        let sql = "case when status = 'active' then 1 else 0 end".to_string();
        let expr = ValidatedSqlExpr::validate(sql.clone(), 0..sql.len());
        assert!(expr.is_valid, "CASE expression should be valid SQL");
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_validate_atom_reference() {
        // NOTE: @ syntax is actually VALID in GenericDialect because SQL Server
        // uses @ for variables. The DSL uses @column for atom references, which
        // happens to be syntactically valid (though semantically wrong for our DSL).
        // This is acceptable - the @ syntax will be transformed during SQL generation.
        let expr = ValidatedSqlExpr::validate("sum(@amount)".to_string(), 0..12);

        // @ is syntactically valid in GenericDialect (SQL Server variables)
        assert!(expr.is_valid, "@column syntax is valid in GenericDialect");
        assert!(expr.error.is_none());

        // However, other DSL-specific syntax should still fail
        // For example, multiple @ symbols in a way that's not valid SQL
        let _expr2 = ValidatedSqlExpr::validate("@@invalid@@".to_string(), 0..11);
        // This might be valid too (SQL Server has @@global variables)
        // So let's test something definitely invalid
        let expr3 = ValidatedSqlExpr::validate("@".to_string(), 0..1);
        assert!(!expr3.is_valid, "Lone @ should be invalid");
        assert!(expr3.error.is_some());
    }

    #[test]
    fn test_validate_measure_math() {
        // Arithmetic between columns should be valid
        let expr = ValidatedSqlExpr::validate("revenue - cost".to_string(), 0..14);
        assert!(expr.is_valid, "Simple arithmetic should be valid SQL");
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_invalid_expression() {
        // Malformed SQL - unclosed parenthesis
        let expr = ValidatedSqlExpr::validate("sum(amount".to_string(), 0..10);
        assert!(!expr.is_valid, "Unclosed parenthesis should be invalid");
        assert!(expr.error.is_some());

        // Invalid SQL - random text
        let expr2 = ValidatedSqlExpr::validate("not valid sql!!!".to_string(), 0..16);
        assert!(!expr2.is_valid);
        assert!(expr2.error.is_some());
    }

    #[test]
    fn test_extract_sql_from_source() {
        // Normal case
        let source = "{ sum(amount) }";
        let sql = extract_sql_from_source(source, &(2..13));
        assert_eq!(sql, Some("sum(amount)".to_string()));

        // Extract from middle of larger source
        let source = "prefix { revenue - cost } suffix";
        let sql = extract_sql_from_source(source, &(9..23));
        assert_eq!(sql, Some("revenue - cost".to_string()));

        // Out of bounds - end beyond source length
        let source = "short";
        let sql = extract_sql_from_source(source, &(0..100));
        assert_eq!(sql, None);

        // Invalid span - start > end
        let source = "test";
        let sql = extract_sql_from_source(source, &(5..2));
        assert_eq!(sql, None);
    }

    #[test]
    fn test_validate_multiple_aggregates() {
        // Multiple aggregates with math
        let expr = ValidatedSqlExpr::validate(
            "sum(revenue) / count(*)".to_string(),
            0..23,
        );
        assert!(expr.is_valid);
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_validate_with_where_clause_fragment() {
        // WHERE clause condition (without WHERE keyword)
        let expr = ValidatedSqlExpr::validate(
            "status = 'active' and region = 'US'".to_string(),
            0..35,
        );
        assert!(expr.is_valid, "Boolean condition should be valid SQL");
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_validate_null_handling() {
        // COALESCE function
        let expr = ValidatedSqlExpr::validate(
            "coalesce(amount, 0)".to_string(),
            0..19,
        );
        assert!(expr.is_valid);
        assert!(expr.error.is_none());

        // Division with NULL handling
        let expr2 = ValidatedSqlExpr::validate(
            "sum(revenue) / nullif(sum(quantity), 0)".to_string(),
            0..39,
        );
        assert!(expr2.is_valid);
        assert!(expr2.error.is_none());
    }

    #[test]
    fn test_validate_cast_expression() {
        // CAST should be valid
        let expr = ValidatedSqlExpr::validate(
            "cast(amount as decimal(10, 2))".to_string(),
            0..30,
        );
        assert!(expr.is_valid);
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_validate_string_functions() {
        // String concatenation and functions
        let expr = ValidatedSqlExpr::validate(
            "concat(first_name, ' ', last_name)".to_string(),
            0..34,
        );
        assert!(expr.is_valid);
        assert!(expr.error.is_none());

        // UPPER function
        let expr2 = ValidatedSqlExpr::validate(
            "upper(name)".to_string(),
            0..11,
        );
        assert!(expr2.is_valid);
        assert!(expr2.error.is_none());
    }

    #[test]
    fn test_validate_date_functions() {
        // Date extraction
        let expr = ValidatedSqlExpr::validate(
            "extract(year from order_date)".to_string(),
            0..29,
        );
        assert!(expr.is_valid);
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_span_preserved() {
        // Ensure span is preserved in the result
        let span = 42..100;
        let expr = ValidatedSqlExpr::validate("sum(x)".to_string(), span.clone());
        assert_eq!(expr.span, span);
    }
}
