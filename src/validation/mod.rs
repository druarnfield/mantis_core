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

fn validate_circular_dependencies(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
}

fn validate_drill_paths(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
}
