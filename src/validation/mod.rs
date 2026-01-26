//! Validation of semantic models.

use crate::model::Model;

/// Validation error.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// Circular dependency detected.
    CircularDependency {
        entity_type: String,
        cycle: Vec<String>,
    },
    /// Reference to undefined entity.
    UndefinedReference {
        entity_type: String,
        entity_name: String,
        reference_type: String,
        reference_name: String,
    },
    /// Invalid drill path.
    InvalidDrillPath {
        entity_type: String,
        entity_name: String,
        drill_path_name: String,
        issue: String,
    },
    /// Duplicate name detected.
    DuplicateName { entity_type: String, name: String },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::CircularDependency { entity_type, cycle } => {
                write!(
                    f,
                    "Circular dependency in {}: {}",
                    entity_type,
                    cycle.join(" -> ")
                )
            }
            ValidationError::UndefinedReference {
                entity_type,
                entity_name,
                reference_type,
                reference_name,
            } => {
                write!(
                    f,
                    "{} '{}' references undefined {} '{}'",
                    entity_type, entity_name, reference_type, reference_name
                )
            }
            ValidationError::InvalidDrillPath {
                entity_type,
                entity_name,
                drill_path_name,
                issue,
            } => {
                write!(
                    f,
                    "{} '{}' has invalid drill path '{}': {}",
                    entity_type, entity_name, drill_path_name, issue
                )
            }
            ValidationError::DuplicateName { entity_type, name } => {
                write!(f, "Duplicate {} name: '{}'", entity_type, name)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validate a semantic model.
pub fn validate(model: &Model) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    // Validate unique names
    validate_unique_names(model, &mut errors);

    // Validate references
    validate_references(model, &mut errors);

    // Validate circular dependencies
    validate_circular_dependencies(model, &mut errors);

    // Validate drill paths
    validate_drill_paths(model, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn validate_unique_names(model: &Model, errors: &mut Vec<ValidationError>) {
    let mut seen_calendars = std::collections::HashSet::new();
    for calendar_name in model.calendars.keys() {
        if !seen_calendars.insert(calendar_name) {
            errors.push(ValidationError::DuplicateName {
                entity_type: "Calendar".to_string(),
                name: calendar_name.clone(),
            });
        }
    }

    let mut seen_dimensions = std::collections::HashSet::new();
    for dimension_name in model.dimensions.keys() {
        if !seen_dimensions.insert(dimension_name) {
            errors.push(ValidationError::DuplicateName {
                entity_type: "Dimension".to_string(),
                name: dimension_name.clone(),
            });
        }
    }

    let mut seen_tables = std::collections::HashSet::new();
    for table_name in model.tables.keys() {
        if !seen_tables.insert(table_name) {
            errors.push(ValidationError::DuplicateName {
                entity_type: "Table".to_string(),
                name: table_name.clone(),
            });
        }
    }

    let mut seen_measure_blocks = std::collections::HashSet::new();
    for measure_block_name in model.measures.keys() {
        if !seen_measure_blocks.insert(measure_block_name) {
            errors.push(ValidationError::DuplicateName {
                entity_type: "MeasureBlock".to_string(),
                name: measure_block_name.clone(),
            });
        }
    }

    let mut seen_reports = std::collections::HashSet::new();
    for report_name in model.reports.keys() {
        if !seen_reports.insert(report_name) {
            errors.push(ValidationError::DuplicateName {
                entity_type: "Report".to_string(),
                name: report_name.clone(),
            });
        }
    }
}

fn validate_references(model: &Model, errors: &mut Vec<ValidationError>) {
    // Validate table time bindings reference existing calendars
    for (table_name, table) in &model.tables {
        for (_time_name, time_binding) in &table.times {
            if !model.calendars.contains_key(&time_binding.calendar) {
                errors.push(ValidationError::UndefinedReference {
                    entity_type: "Table".to_string(),
                    entity_name: table_name.clone(),
                    reference_type: "calendar".to_string(),
                    reference_name: time_binding.calendar.clone(),
                });
            }
        }

        // Validate slicer foreign keys reference existing dimensions
        for (slicer_name, slicer) in &table.slicers {
            if let crate::model::Slicer::ForeignKey { dimension, .. } = slicer {
                if !model.dimensions.contains_key(dimension) {
                    errors.push(ValidationError::UndefinedReference {
                        entity_type: "Table".to_string(),
                        entity_name: format!("{}.{}", table_name, slicer_name),
                        reference_type: "dimension".to_string(),
                        reference_name: dimension.clone(),
                    });
                }
            }
        }

        // Validate Via slicers reference existing slicers in the same table
        for (slicer_name, slicer) in &table.slicers {
            if let crate::model::Slicer::Via { fk_slicer, .. } = slicer {
                if !table.slicers.contains_key(fk_slicer) {
                    errors.push(ValidationError::UndefinedReference {
                        entity_type: "Table".to_string(),
                        entity_name: format!("{}.{}", table_name, slicer_name),
                        reference_type: "slicer".to_string(),
                        reference_name: fk_slicer.clone(),
                    });
                }
            }
        }
    }

    // Validate measure blocks reference existing tables
    for (table_name, _measure_block) in &model.measures {
        if !model.tables.contains_key(table_name) {
            errors.push(ValidationError::UndefinedReference {
                entity_type: "MeasureBlock".to_string(),
                entity_name: table_name.clone(),
                reference_type: "table".to_string(),
                reference_name: table_name.clone(),
            });
        }
    }

    // Validate reports reference existing tables
    for (report_name, report) in &model.reports {
        for table_ref in &report.from {
            if !model.tables.contains_key(table_ref) {
                errors.push(ValidationError::UndefinedReference {
                    entity_type: "Report".to_string(),
                    entity_name: report_name.clone(),
                    reference_type: "table".to_string(),
                    reference_name: table_ref.clone(),
                });
            }
        }
    }
}

fn validate_circular_dependencies(model: &Model, errors: &mut Vec<ValidationError>) {
    // Check measure dependencies
    for (table_name, measure_block) in &model.measures {
        let mut graph = std::collections::HashMap::new();

        // Build dependency graph
        for (measure_name, measure) in &measure_block.measures {
            // TODO: Implement proper measure reference extraction from Expr AST
            let sql_debug = format!("{:?}", measure.expr);
            let deps = extract_measure_references(&sql_debug);
            graph.insert(measure_name.clone(), deps);
        }

        // Detect cycles
        for measure_name in measure_block.measures.keys() {
            let mut visited = std::collections::HashSet::new();
            let mut path = Vec::new();

            if let Some(cycle) = detect_cycle(measure_name, &graph, &mut visited, &mut path) {
                errors.push(ValidationError::CircularDependency {
                    entity_type: format!("Measure in table '{}'", table_name),
                    cycle,
                });
                break; // Report first cycle found
            }
        }
    }
}

fn validate_drill_paths(model: &Model, errors: &mut Vec<ValidationError>) {
    // Validate dimension drill paths reference existing attributes
    for (dim_name, dimension) in &model.dimensions {
        for (drill_path_name, drill_path) in &dimension.drill_paths {
            for level in &drill_path.levels {
                if !dimension.attributes.contains_key(level) {
                    errors.push(ValidationError::InvalidDrillPath {
                        entity_type: "Dimension".to_string(),
                        entity_name: dim_name.clone(),
                        drill_path_name: drill_path_name.clone(),
                        issue: format!("Attribute '{}' does not exist", level),
                    });
                }
            }
        }
    }

    // Validate calendar drill paths reference valid grain levels
    for (cal_name, calendar) in &model.calendars {
        let grain_mappings = match &calendar.body {
            crate::model::CalendarBody::Physical(phys) => &phys.grain_mappings,
            crate::model::CalendarBody::Generated { grain: _, .. } => {
                // Generated calendars auto-support their grain
                continue;
            }
        };

        let drill_paths = match &calendar.body {
            crate::model::CalendarBody::Physical(phys) => &phys.drill_paths,
            _ => continue,
        };

        for (drill_path_name, drill_path) in drill_paths {
            for level in &drill_path.levels {
                if !grain_mappings.contains_key(level) {
                    errors.push(ValidationError::InvalidDrillPath {
                        entity_type: "Calendar".to_string(),
                        entity_name: cal_name.clone(),
                        drill_path_name: drill_path_name.clone(),
                        issue: format!("Grain level {:?} is not mapped", level),
                    });
                }
            }
        }
    }
}

/// Extract measure references from SQL expression.
fn extract_measure_references(sql: &str) -> Vec<String> {
    let mut refs = Vec::new();

    // Simple regex-like pattern matching for identifiers that are not @atoms
    // This is a simplified implementation - real implementation would use a parser
    let tokens: Vec<&str> = sql
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .collect();

    for token in tokens {
        if !token.is_empty() && !token.starts_with('@') && !is_sql_keyword(token) {
            // Might be a measure reference
            refs.push(token.to_string());
        }
    }

    refs
}

fn is_sql_keyword(s: &str) -> bool {
    matches!(
        s.to_lowercase().as_str(),
        "sum"
            | "avg"
            | "count"
            | "min"
            | "max"
            | "case"
            | "when"
            | "then"
            | "else"
            | "end"
            | "and"
            | "or"
            | "not"
            | "in"
            | "like"
            | "between"
    )
}

fn detect_cycle(
    node: &str,
    graph: &std::collections::HashMap<String, Vec<String>>,
    visited: &mut std::collections::HashSet<String>,
    path: &mut Vec<String>,
) -> Option<Vec<String>> {
    if path.contains(&node.to_string()) {
        // Found cycle - return the cycle
        let cycle_start = path.iter().position(|n| n == node).unwrap();
        let mut cycle = path[cycle_start..].to_vec();
        cycle.push(node.to_string());
        return Some(cycle);
    }

    if visited.contains(node) {
        return None;
    }

    visited.insert(node.to_string());
    path.push(node.to_string());

    if let Some(deps) = graph.get(node) {
        for dep in deps {
            if let Some(cycle) = detect_cycle(dep, graph, visited, path) {
                return Some(cycle);
            }
        }
    }

    path.pop();
    None
}
