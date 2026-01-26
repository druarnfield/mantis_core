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

/// Convert sqlparser binary operator to our BinaryOp type
fn convert_binary_op(op: &sql::BinaryOperator, span: Span) -> ParseResult<BinaryOp> {
    match op {
        // Arithmetic
        sql::BinaryOperator::Plus => Ok(BinaryOp::Add),
        sql::BinaryOperator::Minus => Ok(BinaryOp::Sub),
        sql::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        sql::BinaryOperator::Divide => Ok(BinaryOp::Div),
        sql::BinaryOperator::Modulo => Ok(BinaryOp::Mod),

        // Comparison
        sql::BinaryOperator::Eq => Ok(BinaryOp::Eq),
        sql::BinaryOperator::NotEq => Ok(BinaryOp::Ne),
        sql::BinaryOperator::Lt => Ok(BinaryOp::Lt),
        sql::BinaryOperator::LtEq => Ok(BinaryOp::Le),
        sql::BinaryOperator::Gt => Ok(BinaryOp::Gt),
        sql::BinaryOperator::GtEq => Ok(BinaryOp::Ge),

        // Logical
        sql::BinaryOperator::And => Ok(BinaryOp::And),
        sql::BinaryOperator::Or => Ok(BinaryOp::Or),

        // String
        sql::BinaryOperator::Like => Ok(BinaryOp::Like),
        sql::BinaryOperator::NotLike => Ok(BinaryOp::NotLike),

        unsupported => Err(ParseError::UnsupportedFeature {
            feature: format!("Binary operator: {:?}", unsupported),
            span,
        }),
    }
}

/// Convert sqlparser unary operator to our UnaryOp type
fn convert_unary_op(op: &sql::UnaryOperator, span: Span) -> ParseResult<UnaryOp> {
    match op {
        sql::UnaryOperator::Not => Ok(UnaryOp::Not),
        sql::UnaryOperator::Minus => Ok(UnaryOp::Neg),
        sql::UnaryOperator::Plus => {
            // Unary plus is a no-op, but sqlparser doesn't support this directly
            // We'll handle this in convert_expr by ignoring it
            Err(ParseError::UnsupportedFeature {
                feature: "Unary plus operator".to_string(),
                span,
            })
        }
        unsupported => Err(ParseError::UnsupportedFeature {
            feature: format!("Unary operator: {:?}", unsupported),
            span,
        }),
    }
}

/// Convert sqlparser expression to our Expr type
fn convert_expr(sql_expr: &sql::Expr, span: Span) -> ParseResult<Expr> {
    match sql_expr {
        // Simple identifier
        sql::Expr::Identifier(ident) => {
            // Check if this is an atom reference marker
            if let Some(atom_name) = ident.value.strip_prefix("__ATOM__") {
                Ok(Expr::AtomRef(atom_name.to_string()))
            } else {
                Ok(Expr::Column {
                    entity: None,
                    column: ident.value.clone(),
                })
            }
        }

        // Compound identifier (entity.column or schema.table.column)
        sql::Expr::CompoundIdentifier(parts) => {
            if parts.is_empty() {
                return Err(ParseError::SqlParseError {
                    message: "Empty compound identifier".to_string(),
                    span,
                });
            }

            // Check if first part is atom marker
            if parts.len() == 1 {
                if let Some(atom_name) = parts[0].value.strip_prefix("__ATOM__") {
                    return Ok(Expr::AtomRef(atom_name.to_string()));
                }
                return Ok(Expr::Column {
                    entity: None,
                    column: parts[0].value.clone(),
                });
            }

            // Two parts: entity.column
            if parts.len() == 2 {
                return Ok(Expr::Column {
                    entity: Some(parts[0].value.clone()),
                    column: parts[1].value.clone(),
                });
            }

            // Three or more parts: use last two (schema.table.column → table.column)
            let len = parts.len();
            Ok(Expr::Column {
                entity: Some(parts[len - 2].value.clone()),
                column: parts[len - 1].value.clone(),
            })
        }

        // Literals
        sql::Expr::Value(val) => convert_literal(val, span),

        // Nested expression (parentheses)
        sql::Expr::Nested(inner) => convert_expr(inner, span),

        // For now, return error for other types - we'll implement them in next tasks
        unsupported => Err(ParseError::UnsupportedFeature {
            feature: format!("Expression type: {:?}", unsupported),
            span,
        }),
    }
}

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

    #[test]
    fn test_convert_binary_op_arithmetic() {
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Plus, 0..1).unwrap(),
            BinaryOp::Add
        );
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Minus, 0..1).unwrap(),
            BinaryOp::Sub
        );
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Multiply, 0..1).unwrap(),
            BinaryOp::Mul
        );
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Divide, 0..1).unwrap(),
            BinaryOp::Div
        );
    }

    #[test]
    fn test_convert_binary_op_comparison() {
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Eq, 0..1).unwrap(),
            BinaryOp::Eq
        );
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::NotEq, 0..2).unwrap(),
            BinaryOp::Ne
        );
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Lt, 0..1).unwrap(),
            BinaryOp::Lt
        );
        assert_eq!(
            convert_binary_op(&sql::BinaryOperator::Gt, 0..1).unwrap(),
            BinaryOp::Gt
        );
    }

    #[test]
    fn test_convert_unary_op() {
        assert_eq!(
            convert_unary_op(&sql::UnaryOperator::Not, 0..3).unwrap(),
            UnaryOp::Not
        );
        assert_eq!(
            convert_unary_op(&sql::UnaryOperator::Minus, 0..1).unwrap(),
            UnaryOp::Neg
        );
    }

    #[test]
    fn test_convert_atom_ref() {
        let ident = sql::Ident::new("__ATOM__revenue");
        let expr = sql::Expr::Identifier(ident);

        let result = convert_expr(&expr, 0..8).unwrap();
        assert_eq!(result, Expr::AtomRef("revenue".to_string()));
    }

    #[test]
    fn test_convert_regular_column() {
        let ident = sql::Ident::new("customer_id");
        let expr = sql::Expr::Identifier(ident);

        let result = convert_expr(&expr, 0..11).unwrap();
        assert_eq!(
            result,
            Expr::Column {
                entity: None,
                column: "customer_id".to_string(),
            }
        );
    }

    #[test]
    fn test_convert_qualified_column() {
        let idents = vec![sql::Ident::new("sales"), sql::Ident::new("revenue")];
        let expr = sql::Expr::CompoundIdentifier(idents);

        let result = convert_expr(&expr, 0..13).unwrap();
        assert_eq!(
            result,
            Expr::Column {
                entity: Some("sales".to_string()),
                column: "revenue".to_string(),
            }
        );
    }
}
