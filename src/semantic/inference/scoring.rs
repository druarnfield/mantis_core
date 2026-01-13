//! Confidence scoring for inferred relationships.
//!
//! This module provides scoring logic that combines base rule confidence with
//! additional factors like type matching and cardinality hints.

use super::{thresholds, Cardinality};

/// Factors that influence confidence scoring.
#[derive(Debug, Clone, Default)]
pub struct ScoringFactors {
    /// Data types match between the two columns
    pub types_match: bool,
    /// One side of the relationship has unique values (is a PK)
    pub one_side_is_unique: bool,
    /// Column name contains the target table name
    pub column_contains_table_name: bool,
    /// High overlap percentage between values (>90%)
    pub high_value_overlap: bool,
    /// Very high overlap percentage (>99%)
    pub very_high_value_overlap: bool,
    /// Cardinality suggests valid relationship direction
    pub valid_cardinality_direction: bool,
}

/// Computed confidence score with breakdown.
#[derive(Debug, Clone)]
pub struct ConfidenceScore {
    /// Final confidence score (0.0 to 1.0)
    pub final_score: f64,
    /// Base score from the matching rule
    pub base_score: f64,
    /// Breakdown of adjustments
    pub adjustments: Vec<ScoreAdjustment>,
}

/// A single adjustment to the confidence score.
#[derive(Debug, Clone)]
pub struct ScoreAdjustment {
    /// Description of this adjustment
    pub reason: &'static str,
    /// Amount added (or subtracted if negative)
    pub delta: f64,
}

impl ConfidenceScore {
    /// Calculate a confidence score given base score and factors.
    #[must_use]
    pub fn calculate(base_score: f64, factors: &ScoringFactors) -> Self {
        let mut adjustments = Vec::new();
        let mut score = base_score;

        // Type matching bonus
        if factors.types_match {
            let delta = thresholds::adjustment::MINOR_BOOST;
            adjustments.push(ScoreAdjustment {
                reason: "Data types match",
                delta,
            });
            score += delta;
        }

        // Uniqueness bonus (strong indicator of PK/FK relationship)
        if factors.one_side_is_unique {
            let delta = thresholds::adjustment::MEDIUM_BOOST;
            adjustments.push(ScoreAdjustment {
                reason: "One side has unique values",
                delta,
            });
            score += delta;
        }

        // Column name contains table name (e.g., customer_id in customers table)
        if factors.column_contains_table_name {
            let delta = thresholds::adjustment::MINOR_BOOST;
            adjustments.push(ScoreAdjustment {
                reason: "Column name contains table name",
                delta,
            });
            score += delta;
        }

        // Value overlap bonuses
        if factors.very_high_value_overlap {
            let delta = thresholds::adjustment::MEDIUM_BOOST;
            adjustments.push(ScoreAdjustment {
                reason: "Very high value overlap (>99%)",
                delta,
            });
            score += delta;
        } else if factors.high_value_overlap {
            let delta = thresholds::adjustment::MINOR_BOOST;
            adjustments.push(ScoreAdjustment {
                reason: "High value overlap (>90%)",
                delta,
            });
            score += delta;
        }

        // Valid cardinality direction
        if factors.valid_cardinality_direction {
            let delta = thresholds::adjustment::TINY_BOOST;
            adjustments.push(ScoreAdjustment {
                reason: "Cardinality suggests valid direction",
                delta,
            });
            score += delta;
        }

        // Cap at inference max (never 100% confident from inference)
        let final_score = score.min(thresholds::confidence::INFERENCE_CAP);

        ConfidenceScore {
            final_score,
            base_score,
            adjustments,
        }
    }

    /// Infer cardinality from column uniqueness.
    ///
    /// Delegates to `Cardinality::from_uniqueness` for consistent logic.
    #[must_use]
    pub fn infer_cardinality(from_is_unique: bool, to_is_unique: bool) -> Cardinality {
        Cardinality::from_uniqueness(from_is_unique, to_is_unique)
    }
}

impl ScoringFactors {
    /// Create factors from overlap statistics.
    pub fn from_overlap_stats(
        overlap_percentage: f64,
        from_is_unique: bool,
        to_is_unique: bool,
        from_column: &str,
        to_table: &str,
    ) -> Self {
        let from_col_lower = from_column.to_lowercase();
        let to_table_lower = to_table.to_lowercase();

        // Check if column name contains table name (e.g., customer_id contains "customer")
        let column_contains_table_name = from_col_lower.contains(&to_table_lower)
            || from_col_lower.contains(&super::rules::singularize(&to_table_lower));

        ScoringFactors {
            types_match: true, // Assume types match for now (would need type info)
            one_side_is_unique: from_is_unique || to_is_unique,
            column_contains_table_name,
            high_value_overlap: overlap_percentage >= 90.0,
            very_high_value_overlap: overlap_percentage >= 99.0,
            valid_cardinality_direction: to_is_unique, // FK -> PK is the valid direction
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_calculation_base_only() {
        let factors = ScoringFactors::default();
        let score = ConfidenceScore::calculate(0.85, &factors);

        assert_eq!(score.final_score, 0.85);
        assert!(score.adjustments.is_empty());
    }

    #[test]
    fn test_confidence_calculation_with_factors() {
        let factors = ScoringFactors {
            types_match: true,
            one_side_is_unique: true,
            column_contains_table_name: true,
            high_value_overlap: true,
            very_high_value_overlap: false,
            valid_cardinality_direction: true,
        };

        let score = ConfidenceScore::calculate(0.70, &factors);

        // 0.70 + 0.05 (types) + 0.10 (unique) + 0.05 (name) + 0.05 (overlap) + 0.03 (cardinality) = 0.95 (capped)
        assert!((score.final_score - 0.95).abs() < 0.001);
        assert_eq!(score.adjustments.len(), 5);
    }

    #[test]
    fn test_confidence_capped_at_95() {
        let factors = ScoringFactors {
            types_match: true,
            one_side_is_unique: true,
            column_contains_table_name: true,
            high_value_overlap: true,
            very_high_value_overlap: true,
            valid_cardinality_direction: true,
        };

        let score = ConfidenceScore::calculate(0.90, &factors);
        assert!((score.final_score - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_infer_cardinality() {
        assert_eq!(
            ConfidenceScore::infer_cardinality(false, true),
            Cardinality::ManyToOne
        );
        assert_eq!(
            ConfidenceScore::infer_cardinality(true, false),
            Cardinality::OneToMany
        );
        assert_eq!(
            ConfidenceScore::infer_cardinality(true, true),
            Cardinality::OneToOne
        );
    }
}
