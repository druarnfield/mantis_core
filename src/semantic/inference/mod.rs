//! Relationship inference engine for automatic join discovery.
//!
//! This module provides heuristic-based relationship inference between database tables.
//! It analyzes column names, types, and cardinality to suggest potential join relationships.
//!
//! # Architecture
//!
//! The inference system works in two phases:
//!
//! 1. **Heuristic matching** - Apply naming convention rules to find candidate relationships
//! 2. **Cardinality validation** - Use column statistics to score and validate candidates
//!
//! # Example
//!
//! ```ignore
//! use mantis::semantic::inference::{InferenceEngine, InferredRelationship};
//!
//! let engine = InferenceEngine::default();
//! let candidates = engine.infer_relationships(&table_metadata, &all_tables);
//! ```

mod engine;
mod model_integration;
mod rules;
mod scoring;
pub mod signals;

pub use engine::{ColumnInfo, InferenceConfig, InferenceEngine, TableInfo, WeightPreset};
pub use model_integration::{InferenceResult, ModelInferenceConfig};
pub use rules::{default_rules, InferenceRule, RuleMatch};
pub use scoring::{ConfidenceScore, ScoringFactors};

/// Centralized confidence thresholds and adjustment values.
///
/// Using named constants instead of magic numbers improves readability
/// and makes it easier to tune the inference system.
pub mod thresholds {
    /// Confidence levels for different relationship sources.
    pub mod confidence {
        /// Confidence for relationships backed by database FK constraints.
        pub const DB_CONSTRAINT: f64 = 0.98;
        /// Maximum confidence for inferred relationships (never 100% certain).
        pub const INFERENCE_CAP: f64 = 0.95;
        /// Minimum confidence for high-precision mode.
        pub const HIGH_PRECISION_MIN: f64 = 0.70;
        /// Default minimum confidence (balanced mode).
        pub const BALANCED_MIN: f64 = 0.50;
        /// Minimum confidence for high-recall mode.
        pub const HIGH_RECALL_MIN: f64 = 0.40;
    }

    /// Thresholds for value overlap analysis.
    pub mod overlap {
        /// Excellent overlap (near-perfect FK relationship).
        pub const EXCELLENT: f64 = 0.95;
        /// Good overlap (strong FK candidate).
        pub const GOOD: f64 = 0.80;
        /// Acceptable overlap (possible FK).
        pub const ACCEPTABLE: f64 = 0.50;
        /// Low overlap (weak FK candidate).
        pub const LOW: f64 = 0.30;
        /// Very low overlap threshold.
        pub const VERY_LOW: f64 = 0.10;
    }

    /// Confidence score adjustments.
    pub mod adjustment {
        /// Major confidence boost (e.g., high overlap, uniqueness).
        pub const MAJOR_BOOST: f64 = 0.15;
        /// Medium confidence boost.
        pub const MEDIUM_BOOST: f64 = 0.10;
        /// Minor confidence boost.
        pub const MINOR_BOOST: f64 = 0.05;
        /// Tiny confidence boost.
        pub const TINY_BOOST: f64 = 0.03;
        /// Penalty for low overlap.
        pub const LOW_OVERLAP_PENALTY: f64 = -0.15;
        /// Penalty for very low overlap.
        pub const VERY_LOW_OVERLAP_PENALTY: f64 = -0.20;
    }
}

/// Source of a relationship definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RelationshipSource {
    /// Relationship from database FK constraint (highest confidence).
    DatabaseConstraint,
    /// Relationship inferred by heuristics.
    #[default]
    Inferred,
    /// Relationship manually defined by user.
    UserDefined,
}

impl std::fmt::Display for RelationshipSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseConstraint => write!(f, "database"),
            Self::Inferred => write!(f, "inferred"),
            Self::UserDefined => write!(f, "user"),
        }
    }
}

// Re-export Cardinality from model for use in inference
pub use crate::semantic::graph::Cardinality;

/// An inferred relationship between two tables.
#[derive(Debug, Clone)]
pub struct InferredRelationship {
    /// The source table schema
    pub from_schema: String,
    /// The source table name
    pub from_table: String,
    /// The source column name
    pub from_column: String,
    /// The target table schema
    pub to_schema: String,
    /// The target table name
    pub to_table: String,
    /// The target column name
    pub to_column: String,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// The rule that matched (primary signal source)
    pub rule: String,
    /// Inferred cardinality (uses unified Cardinality type from model)
    pub cardinality: Cardinality,
    /// Optional breakdown of signals (for explain mode)
    pub signal_breakdown: Option<Vec<signals::ScoreBreakdown>>,
    /// Source of this relationship (database constraint, inferred, or user-defined).
    pub source: RelationshipSource,
}

impl PartialEq for InferredRelationship {
    fn eq(&self, other: &Self) -> bool {
        self.from_schema == other.from_schema
            && self.from_table == other.from_table
            && self.from_column == other.from_column
            && self.to_schema == other.to_schema
            && self.to_table == other.to_table
            && self.to_column == other.to_column
            && (self.confidence - other.confidence).abs() < 0.001
            && self.rule == other.rule
            && self.cardinality == other.cardinality
    }
}

// InferredCardinality has been unified with model::Cardinality.
// The Cardinality type is re-exported above for backward compatibility.
// Use Cardinality::Unknown for undetermined cardinality during inference.

/// A unique key identifying a relationship by its endpoints.
///
/// Used for deduplication and lookup of relationships. All table/column
/// names are stored in lowercase for case-insensitive comparison.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RelationshipKey {
    /// Source table name (lowercase)
    pub from_table: String,
    /// Source column name (lowercase)
    pub from_column: String,
    /// Target table name (lowercase)
    pub to_table: String,
    /// Target column name (lowercase)
    pub to_column: String,
}

impl RelationshipKey {
    /// Create a new relationship key with normalized (lowercase) names.
    #[must_use]
    pub fn new(from_table: &str, from_column: &str, to_table: &str, to_column: &str) -> Self {
        Self {
            from_table: from_table.to_lowercase(),
            from_column: from_column.to_lowercase(),
            to_table: to_table.to_lowercase(),
            to_column: to_column.to_lowercase(),
        }
    }

    /// Create a key from an inferred relationship.
    #[must_use]
    pub fn from_relationship(rel: &InferredRelationship) -> Self {
        Self::new(
            &rel.from_table,
            &rel.from_column,
            &rel.to_table,
            &rel.to_column,
        )
    }

    /// Get the reversed key (swapping from/to).
    #[must_use]
    pub fn reversed(&self) -> Self {
        Self {
            from_table: self.to_table.clone(),
            from_column: self.to_column.clone(),
            to_table: self.from_table.clone(),
            to_column: self.from_column.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_reverse() {
        assert_eq!(Cardinality::OneToMany.reverse(), Cardinality::ManyToOne);
        assert_eq!(Cardinality::ManyToOne.reverse(), Cardinality::OneToMany);
        assert_eq!(Cardinality::OneToOne.reverse(), Cardinality::OneToOne);
    }

    #[test]
    fn test_cardinality_display() {
        assert_eq!(format!("{}", Cardinality::ManyToOne), "N:1");
        assert_eq!(format!("{}", Cardinality::OneToMany), "1:N");
    }

    #[test]
    fn test_cardinality_from_uniqueness() {
        assert_eq!(
            Cardinality::from_uniqueness(true, true),
            Cardinality::OneToOne
        );
        assert_eq!(
            Cardinality::from_uniqueness(false, true),
            Cardinality::ManyToOne
        );
        assert_eq!(
            Cardinality::from_uniqueness(true, false),
            Cardinality::OneToMany
        );
        assert_eq!(
            Cardinality::from_uniqueness(false, false),
            Cardinality::Unknown
        );
    }

    #[test]
    fn test_relationship_key_new() {
        let key = RelationshipKey::new("Orders", "Customer_ID", "Customers", "ID");
        assert_eq!(key.from_table, "orders");
        assert_eq!(key.from_column, "customer_id");
        assert_eq!(key.to_table, "customers");
        assert_eq!(key.to_column, "id");
    }

    #[test]
    fn test_relationship_key_reversed() {
        let key = RelationshipKey::new("orders", "customer_id", "customers", "id");
        let reversed = key.reversed();
        assert_eq!(reversed.from_table, "customers");
        assert_eq!(reversed.from_column, "id");
        assert_eq!(reversed.to_table, "orders");
        assert_eq!(reversed.to_column, "customer_id");
    }

    #[test]
    fn test_relationship_key_equality() {
        let key1 = RelationshipKey::new("Orders", "Customer_ID", "Customers", "ID");
        let key2 = RelationshipKey::new("orders", "customer_id", "customers", "id");
        assert_eq!(key1, key2); // Case-insensitive comparison
    }
}
