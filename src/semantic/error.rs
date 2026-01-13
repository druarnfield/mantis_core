//! Unified error types for the semantic layer.
//!
//! This module provides a single error type used by both the ModelGraph
//! (for graph operations like path finding and dependency analysis) and
//! the QueryPlanner (for query resolution and validation).

use std::fmt;

/// Result type for semantic operations.
pub type SemanticResult<T> = Result<T, SemanticError>;

/// Details for a type mismatch error in join validation.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeMismatchDetails {
    pub left_entity: String,
    pub left_column: String,
    pub left_type: String,
    pub right_entity: String,
    pub right_column: String,
    pub right_type: String,
}

/// Unified error type for the semantic layer.
///
/// Covers errors from:
/// - Graph operations (path finding, dependency analysis)
/// - Query planning (resolution, validation, emission)
/// - Model validation
#[derive(Debug, Clone, PartialEq)]
pub enum SemanticError {
    /// Referenced an entity that doesn't exist.
    UnknownEntity(String),

    /// Referenced a field that doesn't exist on an entity.
    UnknownField {
        entity: String,
        field: String,
    },

    /// Could not find a join path between entities.
    NoPath {
        from: String,
        to: String,
    },

    /// The join path would cause row multiplication (fan-out).
    UnsafeJoinPath {
        from: String,
        to: String,
        message: String,
    },

    /// Column used in select without being in GROUP BY.
    UngroupedColumn {
        column: String,
    },

    /// Ambiguous field reference (exists on multiple entities).
    AmbiguousField {
        field: String,
        entities: Vec<String>,
    },

    /// Invalid reference format or usage.
    InvalidReference(String),

    /// Circular dependency detected in the model.
    CyclicDependency(Vec<String>),

    /// Invalid model configuration.
    InvalidModel(String),

    /// Join column types are incompatible.
    TypeMismatch(Box<TypeMismatchDetails>),

    /// Query name not found in model.
    UnknownQuery { name: String },

    /// Measure not found in any fact.
    UnknownMeasure { name: String },

    /// Circular dependencies detected in column lineage.
    ///
    /// This indicates that column definitions form a cycle, which would
    /// cause infinite recursion during query planning.
    ColumnLineageCycle {
        /// The cycles detected, each as a list of "entity.column" strings.
        cycles: Vec<Vec<String>>,
    },

    /// Cannot determine query anchor - no measures and no explicit `from`.
    NoAnchor,

    /// A dimension is not reachable from all anchor facts in a multi-fact query.
    DimensionNotShared {
        /// The dimension that's not reachable from all facts.
        dimension: String,
        /// The fact that cannot reach this dimension.
        unreachable_from: String,
    },

    /// Multiple paths exist to a dimension - disambiguation required.
    AmbiguousPath {
        /// The dimension with multiple paths.
        dimension: String,
        /// The facts that each have different paths.
        facts: Vec<String>,
    },

    /// Ambiguous dimension reference - multiple roles point to this dimension.
    ///
    /// When a fact has multiple foreign keys to the same dimension (role-playing
    /// dimensions), queries must use the role name instead of the dimension name.
    ///
    /// # Example
    ///
    /// If an orders fact has `order_date_id`, `ship_date_id`, and `delivery_date_id`
    /// all pointing to a date dimension, using `date.month` is ambiguous.
    /// The query must specify which role: `order_date.month`, `ship_date.month`, etc.
    AmbiguousDimensionRole {
        /// The dimension that has multiple roles.
        dimension: String,
        /// The available role names to disambiguate.
        available_roles: Vec<String>,
    },

    /// Query planning error.
    ///
    /// Used for errors during query planning that don't fit other categories,
    /// such as virtual fact reconstruction failures.
    QueryPlanError(String),
}

impl fmt::Display for SemanticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SemanticError::UnknownEntity(name) => {
                write!(f, "Unknown entity: '{}'", name)
            }
            SemanticError::UnknownField { entity, field } => {
                write!(f, "Unknown field '{}' on entity '{}'", field, entity)
            }
            SemanticError::NoPath { from, to } => {
                write!(f, "No relationship path from '{}' to '{}'", from, to)
            }
            SemanticError::UnsafeJoinPath { from, to, message } => {
                write!(
                    f,
                    "Unsafe join path from '{}' to '{}': {}",
                    from, to, message
                )
            }
            SemanticError::UngroupedColumn { column } => {
                write!(
                    f,
                    "Column '{}' must be in GROUP BY clause or wrapped in aggregate",
                    column
                )
            }
            SemanticError::AmbiguousField { field, entities } => {
                write!(
                    f,
                    "Ambiguous field '{}' - exists on: {}. Qualify it.",
                    field,
                    entities.join(", ")
                )
            }
            SemanticError::InvalidReference(msg) => {
                write!(f, "Invalid reference: {}", msg)
            }
            SemanticError::CyclicDependency(cycle) => {
                write!(f, "Cyclic dependency detected: {}", cycle.join(" -> "))
            }
            SemanticError::InvalidModel(msg) => {
                write!(f, "Invalid model: {}", msg)
            }
            SemanticError::TypeMismatch(details) => {
                write!(
                    f,
                    "Type mismatch in join: {}.{} ({}) cannot join with {}.{} ({})",
                    details.left_entity, details.left_column, details.left_type,
                    details.right_entity, details.right_column, details.right_type
                )
            }
            SemanticError::UnknownQuery { name } => {
                write!(f, "Unknown query: '{}'. Not found in model.", name)
            }
            SemanticError::UnknownMeasure { name } => {
                write!(f, "Unknown measure: '{}'. Not found in any fact.", name)
            }
            SemanticError::ColumnLineageCycle { cycles } => {
                writeln!(f, "Circular dependencies detected in column lineage:")?;
                for (i, cycle) in cycles.iter().enumerate() {
                    writeln!(f, "  Cycle {}: {} → (back to start)", i + 1, cycle.join(" → "))?;
                }
                Ok(())
            }
            SemanticError::NoAnchor => {
                write!(
                    f,
                    "Cannot determine query anchor. Add a measure or specify 'from'."
                )
            }
            SemanticError::DimensionNotShared {
                dimension,
                unreachable_from,
            } => {
                write!(
                    f,
                    "Dimension '{}' is not reachable from fact '{}'. \
                     Multi-fact queries require all dimensions to be accessible from all facts.",
                    dimension, unreachable_from
                )
            }
            SemanticError::AmbiguousPath { dimension, facts } => {
                write!(
                    f,
                    "Ambiguous path to dimension '{}' from facts: {}. Specify 'via' to disambiguate.",
                    dimension,
                    facts.join(", ")
                )
            }
            SemanticError::AmbiguousDimensionRole {
                dimension,
                available_roles,
            } => {
                write!(
                    f,
                    "Ambiguous reference to dimension '{}'. Multiple roles exist: {}. \
                     Use a role name instead (e.g., '{}.<column>').",
                    dimension,
                    available_roles.join(", "),
                    available_roles.first().map(|s| s.as_str()).unwrap_or("role")
                )
            }
            SemanticError::QueryPlanError(msg) => {
                write!(f, "Query planning error: {}", msg)
            }
        }
    }
}

impl std::error::Error for SemanticError {}

impl From<super::column_lineage::LineageCycleError> for SemanticError {
    fn from(err: super::column_lineage::LineageCycleError) -> Self {
        SemanticError::ColumnLineageCycle {
            cycles: err
                .cycles
                .into_iter()
                .map(|cycle| cycle.into_iter().map(|c| c.to_string()).collect())
                .collect(),
        }
    }
}

// Legacy type aliases for backward compatibility during migration
// TODO: Remove these after full migration
#[allow(dead_code)]
pub type PlanError = SemanticError;
#[allow(dead_code)]
pub type PlanResult<T> = SemanticResult<T>;
#[allow(dead_code)]
pub type GraphError = SemanticError;
#[allow(dead_code)]
pub type GraphResult<T> = SemanticResult<T>;
