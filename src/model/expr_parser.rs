//! SQL expression parser using sqlparser-rs.
//!
//! Parses SQL expressions containing @atom references into our Expr AST.
//!
//! Strategy:
//! 1. Preprocess: @atom → __ATOM__atom (sqlparser marker)
//! 2. Parse with sqlparser-rs (validates SQL syntax)
//! 3. Convert sqlparser AST → our Expr AST
//! 4. Postprocess: __ATOM__atom → Expr::AtomRef(atom)

use once_cell::sync::Lazy;
use regex::Regex;

use crate::dsl::Span;

// These imports will be used in Tasks 3-10 when conversion functions are added
#[allow(unused_imports)]
use sqlparser::ast as sql;
#[allow(unused_imports)]
use sqlparser::dialect::GenericDialect;
#[allow(unused_imports)]
use sqlparser::parser::Parser;

#[allow(unused_imports)]
use super::expr::*;
#[allow(unused_imports)]
use super::types::DataType;

/// Regex pattern for matching @atom references
static ATOM_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"@(\w+)").unwrap());

/// Prefix used for atom substitution
const ATOM_PREFIX: &str = "__ATOM__";

/// Errors that can occur during SQL expression parsing
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL syntax error at {span:?}: {message}")]
    SqlParseError { message: String, span: Span },

    #[error("Unsupported SQL feature '{feature}' at {span:?}")]
    UnsupportedFeature { feature: String, span: Span },

    #[error("Invalid number format '{value}' at {span:?}: {error}")]
    InvalidNumber {
        value: String,
        error: String,
        span: Span,
    },

    #[error("Invalid data type at {span:?}: {message}")]
    InvalidDataType { message: String, span: Span },
}

pub type ParseResult<T> = Result<T, ParseError>;

/// Preprocess SQL by replacing @atom → __ATOM__atom
///
/// This allows sqlparser to parse the SQL (it treats __ATOM__atom as a regular identifier)
/// while preserving information about which identifiers were atom references.
fn preprocess_sql_for_parsing(sql: &str) -> String {
    ATOM_PATTERN.replace_all(sql, "__ATOM__$1").to_string()
}

// Helper functions will be added in subsequent tasks

/// Convert sqlparser literal to our Literal type
fn convert_literal(val: &sql::Value, span: Span) -> ParseResult<Expr> {
    match val {
        sql::Value::Number(n, _) => {
            // Try to parse as int first, then float
            if n.contains('.') || n.contains('e') || n.contains('E') {
                let f = n.parse::<f64>().map_err(|e| ParseError::InvalidNumber {
                    value: n.clone(),
                    error: e.to_string(),
                    span: span.clone(),
                })?;
                Ok(Expr::Literal(Literal::Float(f)))
            } else {
                let i = n.parse::<i64>().map_err(|e| ParseError::InvalidNumber {
                    value: n.clone(),
                    error: e.to_string(),
                    span: span.clone(),
                })?;
                Ok(Expr::Literal(Literal::Int(i)))
            }
        }
        sql::Value::SingleQuotedString(s) | sql::Value::DoubleQuotedString(s) => {
            Ok(Expr::Literal(Literal::String(s.clone())))
        }
        sql::Value::Boolean(b) => Ok(Expr::Literal(Literal::Bool(*b))),
        sql::Value::Null => Ok(Expr::Literal(Literal::Null)),
        unsupported => Err(ParseError::UnsupportedFeature {
            feature: format!("Literal value: {:?}", unsupported),
            span,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_sql() {
        assert_eq!(preprocess_sql_for_parsing("@revenue"), "__ATOM__revenue");

        assert_eq!(
            preprocess_sql_for_parsing("SUM(@revenue * @quantity)"),
            "SUM(__ATOM__revenue * __ATOM__quantity)"
        );

        assert_eq!(preprocess_sql_for_parsing("revenue"), "revenue");
    }

    #[test]
    fn test_atom_pattern() {
        assert!(ATOM_PATTERN.is_match("@revenue"));
        assert!(ATOM_PATTERN.is_match("SUM(@revenue)"));
        assert!(!ATOM_PATTERN.is_match("revenue"));
    }

    #[test]
    fn test_module_loads() {
        // Just verify the module compiles
    }

    #[test]
    fn test_convert_literal_int() {
        let sql_lit = sql::Value::Number("42".to_string(), false);
        let result = convert_literal(&sql_lit, 0..2).unwrap();
        assert_eq!(result, Expr::Literal(Literal::Int(42)));
    }

    #[test]
    fn test_convert_literal_float() {
        let sql_lit = sql::Value::Number("3.14".to_string(), false);
        let result = convert_literal(&sql_lit, 0..4).unwrap();
        assert_eq!(result, Expr::Literal(Literal::Float(3.14)));
    }

    #[test]
    fn test_convert_literal_string() {
        let sql_lit = sql::Value::SingleQuotedString("hello".to_string());
        let result = convert_literal(&sql_lit, 0..7).unwrap();
        assert_eq!(result, Expr::Literal(Literal::String("hello".to_string())));
    }

    #[test]
    fn test_convert_literal_bool() {
        let sql_lit = sql::Value::Boolean(true);
        let result = convert_literal(&sql_lit, 0..4).unwrap();
        assert_eq!(result, Expr::Literal(Literal::Bool(true)));
    }

    #[test]
    fn test_convert_literal_null() {
        let sql_lit = sql::Value::Null;
        let result = convert_literal(&sql_lit, 0..4).unwrap();
        assert_eq!(result, Expr::Literal(Literal::Null));
    }
}
