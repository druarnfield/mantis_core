//! Expression validation and utility functions.
//!
//! Provides context-aware validation and helper methods for traversing
//! and analyzing expression ASTs.

/// Context where an expression is used.
///
/// Different contexts have different validation rules (e.g., aggregates
/// are allowed in measures but not in filters).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprContext {
    /// Measure expression - aggregates allowed
    Measure,
    /// Filter expression - no aggregates
    Filter,
    /// Calculated slicer expression - no aggregates
    CalculatedSlicer,
}

/// Validation errors for expressions
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Aggregate functions not allowed in {context:?} expressions")]
    AggregateNotAllowed { context: ExprContext },

    #[error("Undefined atom reference: @{atom}")]
    UndefinedAtom { atom: String },

    #[error("Undefined column reference: {column}")]
    UndefinedColumn { column: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::expr::*;

    #[test]
    fn test_validate_context_measure_allows_aggregate() {
        let expr = Expr::Function {
            func: Func::Aggregate(AggregateFunc::Sum),
            args: vec![Expr::AtomRef("revenue".to_string())],
        };

        assert!(expr.validate_context(ExprContext::Measure).is_ok());
    }

    #[test]
    fn test_validate_context_filter_rejects_aggregate() {
        let expr = Expr::Function {
            func: Func::Aggregate(AggregateFunc::Sum),
            args: vec![Expr::AtomRef("revenue".to_string())],
        };

        let result = expr.validate_context(ExprContext::Filter);
        assert!(result.is_err());
        match result.unwrap_err() {
            ValidationError::AggregateNotAllowed { context } => {
                assert_eq!(context, ExprContext::Filter);
            }
            _ => panic!("Expected AggregateNotAllowed error"),
        }
    }

    #[test]
    fn test_validate_context_filter_allows_scalar() {
        let expr = Expr::Function {
            func: Func::Scalar(ScalarFunc::Upper),
            args: vec![Expr::Column {
                entity: None,
                column: "name".to_string(),
            }],
        };

        assert!(expr.validate_context(ExprContext::Filter).is_ok());
    }
}
