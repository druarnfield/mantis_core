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
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validate a semantic model.
pub fn validate(model: &Model) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

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

fn validate_references(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
}

fn validate_circular_dependencies(model: &Model, errors: &mut Vec<ValidationError>) {
    // Check measure dependencies
    for (table_name, measure_block) in &model.measures {
        let mut graph = std::collections::HashMap::new();

        // Build dependency graph
        for (measure_name, measure) in &measure_block.measures {
            let deps = extract_measure_references(&measure.expr.sql);
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

fn validate_drill_paths(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
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
