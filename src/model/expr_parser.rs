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
}
