//! Translation of Report model types to SemanticQuery.

use crate::model::{Model, Report};
use crate::semantic::planner::types::{SelectField, SemanticQuery};

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

/// Translate a simple measure reference to a SelectField.
///
/// A simple measure is a direct reference to a measure defined in the model,
/// without any time intelligence suffixes or inline expressions.
fn translate_simple_measure(
    measure_name: &str,
    label: Option<String>,
    from_table: &str,
    model: &Model,
) -> Result<SelectField, TranslationError> {
    // Find the measure in the model
    let measure_block =
        model
            .measures
            .get(from_table)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "measure block".to_string(),
                name: from_table.to_string(),
            })?;

    let _measure = measure_block.measures.get(measure_name).ok_or_else(|| {
        TranslationError::InvalidMeasure {
            measure: measure_name.to_string(),
            table: from_table.to_string(),
        }
    })?;

    // Create SelectField
    let mut select_field = SelectField::new(from_table, measure_name);
    if let Some(label) = label {
        select_field = select_field.with_alias(&label);
    }

    Ok(select_field)
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

    let from_table = &report.from[0];
    query.from = Some(from_table.clone());

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

    // Translate show items
    for show_item in &report.show {
        match show_item {
            crate::model::ShowItem::Measure { name, label } => {
                let select_field =
                    translate_simple_measure(name, label.clone(), from_table, model)?;
                query.select.push(select_field);
            }
            crate::model::ShowItem::MeasureWithSuffix { .. } => {
                // TODO: Handle time suffixes
            }
            crate::model::ShowItem::InlineMeasure { .. } => {
                // TODO: Handle inline measures
            }
        }
    }

    // TODO: Translate filters, sort, limit

    Ok(query)
}
