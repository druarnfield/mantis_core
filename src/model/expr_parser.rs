//! SQL expression parser that converts sqlparser AST into our Expr AST.
//!
//! This module handles parsing SQL expressions with @atom references.

use crate::dsl::Span;
use crate::model::expr::*;
use regex::Regex;
use sqlparser::ast as sql;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::LazyLock;
use thiserror::Error;

/// Pattern for detecting @atom references (e.g., @revenue)
static ATOM_PATTERN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"@(\w+)").unwrap());

/// Prefix used for atom substitution
const ATOM_PREFIX: &str = "__ATOM__";

/// Errors that can occur during SQL parsing
#[derive(Debug, Error, Clone, PartialEq)]
pub enum ParseError {
    #[error("SQL syntax error at {span:?}: {message}")]
    SyntaxError { message: String, span: Span },

    #[error("Unsupported SQL feature at {span:?}: {feature}")]
    UnsupportedFeature { feature: String, span: Span },

    #[error("Invalid expression at {span:?}: {message}")]
    InvalidExpression { message: String, span: Span },
}

pub type ParseResult<T> = Result<T, ParseError>;

// Helper functions will be added in subsequent tasks

#[cfg(test)]
mod tests {
    use super::*;

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
