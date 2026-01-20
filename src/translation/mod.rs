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

/// Resolve a drill path reference to a FieldRef.
///
/// A drill path reference has three parts:
/// - source: The calendar name (e.g., "dates")
/// - path: The drill path name (e.g., "standard")
/// - level: The grain level (e.g., "month")
///
/// This resolves to a FieldRef pointing to the actual column name.
fn resolve_drill_path_reference(
    source: &str,
    path: &str,
    level: &str,
    model: &Model,
) -> Result<crate::semantic::planner::types::FieldRef, TranslationError> {
    // Get the calendar
    let calendar =
        model
            .calendars
            .get(source)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "calendar".to_string(),
                name: source.to_string(),
            })?;

    // Get drill paths from calendar body
    let drill_paths = match &calendar.body {
        crate::model::CalendarBody::Physical(phys) => &phys.drill_paths,
        crate::model::CalendarBody::Generated { .. } => {
            return Err(TranslationError::InvalidDrillPath {
                source: source.to_string(),
                path: path.to_string(),
                level: level.to_string(),
            });
        }
    };

    // Get the specific drill path
    let drill_path = drill_paths
        .get(path)
        .ok_or_else(|| TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        })?;

    // Parse the level string to GrainLevel
    let grain_level = crate::model::GrainLevel::from_str(level).ok_or_else(|| {
        TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        }
    })?;

    // Verify the grain level is in the drill path
    if !drill_path.levels.contains(&grain_level) {
        return Err(TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        });
    }

    // Get the column name for this grain level
    let grain_mappings = match &calendar.body {
        crate::model::CalendarBody::Physical(phys) => &phys.grain_mappings,
        _ => unreachable!(),
    };

    let column =
        grain_mappings
            .get(&grain_level)
            .ok_or_else(|| TranslationError::InvalidDrillPath {
                source: source.to_string(),
                path: path.to_string(),
                level: level.to_string(),
            })?;

    Ok(crate::semantic::planner::types::FieldRef::new(
        source, column,
    ))
}

/// Translate a Report to a SemanticQuery.
pub fn translate_report(report: &Report, model: &Model) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();

    // Validate and set from clause
    if report.from.is_empty() {
        return Err(TranslationError::UndefinedReference {
            entity_type: "table".to_string(),
            name: "(none specified)".to_string(),
        });
    }

    if report.from.len() > 1 {
        // For now, only single-table queries supported
        // Multi-table support will be added in future tasks
        return Err(TranslationError::UndefinedReference {
            entity_type: "multi-table query".to_string(),
            name: format!("got {} tables, expected 1", report.from.len()),
        });
    }

    query.from = Some(report.from[0].clone());

    // Translate group items
    for group_item in &report.group {
        match group_item {
            crate::model::GroupItem::DrillPathRef {
                source,
                path,
                level,
                ..
            } => {
                let field_ref = resolve_drill_path_reference(source, path, level, model)?;
                query.group_by.push(field_ref);
            }
            crate::model::GroupItem::InlineSlicer { .. } => {
                // TODO: Resolve slicer reference
            }
        }
    }

    // TODO: Translate show, filters, sort, limit

    Ok(query)
}
