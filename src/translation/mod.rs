//! Translation of Report model types to SemanticQuery.

use crate::model::{Model, Report};
use crate::semantic::planner::types::SemanticQuery;

/// Translation error.
#[derive(Debug, Clone)]
pub enum TranslationError {
    /// Reference to undefined entity.
    UndefinedReference { entity_type: String, name: String },
    /// Invalid drill path reference.
    InvalidDrillPath {
        source: String,
        path: String,
        level: String,
    },
    /// Invalid measure reference.
    InvalidMeasure { measure: String, table: String },
    /// SQL expression compilation error.
    SqlCompilationError { expression: String, error: String },
}

impl std::fmt::Display for TranslationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslationError::UndefinedReference { entity_type, name } => {
                write!(f, "Undefined {} reference: {}", entity_type, name)
            }
            TranslationError::InvalidDrillPath {
                source,
                path,
                level,
            } => {
                write!(f, "Invalid drill path: {}.{}.{}", source, path, level)
            }
            TranslationError::InvalidMeasure { measure, table } => {
                write!(f, "Invalid measure '{}' in table '{}'", measure, table)
            }
            TranslationError::SqlCompilationError { expression, error } => {
                write!(f, "SQL compilation error in '{}': {}", expression, error)
            }
        }
    }
}

impl std::error::Error for TranslationError {}

/// Translate a Report to a SemanticQuery.
pub fn translate_report(
    report: &Report,
    _model: &Model,
) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();

    // Set from clause
    if !report.from.is_empty() {
        query.from = Some(report.from[0].clone());
    }

    // TODO: Translate group, show, filters, sort, limit

    Ok(query)
}
