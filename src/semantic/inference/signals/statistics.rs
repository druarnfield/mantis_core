//! Statistics-based signal collector.
//!
//! Uses column statistics and value overlap to generate signals
//! for relationship inference. These signals require database queries.

use crate::metadata::{ColumnStats, ValueOverlap};
use crate::model::Cardinality;

use super::{Signal, SignalSource};

/// Statistics-based signals for a potential relationship.
#[derive(Debug, Clone)]
pub struct StatisticsSignals {
    /// Uniqueness of the target column (higher = more likely FK target).
    pub uniqueness_score: f64,
    /// Value overlap percentage (higher = more likely valid FK).
    pub overlap_score: f64,
    /// Inferred cardinality from statistics.
    pub cardinality_hint: Option<Cardinality>,
    /// Penalty based on source column null rate.
    pub null_rate_penalty: f64,
    /// Whether target appears to be a superset of source values.
    pub is_valid_fk: bool,
}

impl StatisticsSignals {
    /// Collect statistics signals from column stats and value overlap.
    ///
    /// # Arguments
    /// * `from_stats` - Statistics for the source (FK) column
    /// * `to_stats` - Statistics for the target (PK) column
    /// * `overlap` - Value overlap between the columns
    pub fn from_stats(
        from_stats: &ColumnStats,
        to_stats: &ColumnStats,
        overlap: &ValueOverlap,
    ) -> Self {
        // Uniqueness: target should be unique (PK/unique constraint)
        let uniqueness_score = if to_stats.is_unique {
            1.0
        } else {
            // Partial uniqueness based on distinct ratio
            let distinct_ratio = if to_stats.total_count > 0 {
                to_stats.distinct_count as f64 / to_stats.total_count as f64
            } else {
                0.0
            };
            distinct_ratio * 0.5 // Max 0.5 for non-unique
        };

        // Overlap: source values should exist in target
        let overlap_score = overlap.overlap_percentage / 100.0;

        // Valid FK: right should be superset and have high overlap
        let is_valid_fk = overlap.right_is_superset || overlap.overlap_percentage >= 95.0;

        // Null rate penalty: high null rate in source suggests optional relationship
        let null_rate_penalty = if from_stats.total_count > 0 {
            let null_rate = from_stats.null_count as f64 / from_stats.total_count as f64;
            // Penalty increases with null rate, but capped at 0.3
            (null_rate * 0.5).min(0.3)
        } else {
            0.0
        };

        // Infer cardinality
        let cardinality_hint = Self::infer_cardinality(from_stats, to_stats, overlap);

        Self {
            uniqueness_score,
            overlap_score,
            cardinality_hint,
            null_rate_penalty,
            is_valid_fk,
        }
    }

    /// Infer cardinality from statistics.
    fn infer_cardinality(
        from_stats: &ColumnStats,
        to_stats: &ColumnStats,
        overlap: &ValueOverlap,
    ) -> Option<Cardinality> {
        let from_unique = from_stats.is_unique || overlap.left_is_unique;
        let to_unique = to_stats.is_unique || overlap.right_is_unique;

        match (from_unique, to_unique) {
            (true, true) => Some(Cardinality::OneToOne),
            (false, true) => Some(Cardinality::ManyToOne),
            (true, false) => Some(Cardinality::OneToMany),
            (false, false) => Some(Cardinality::ManyToMany),
        }
    }

    /// Convert to a vector of signals.
    pub fn to_signals(&self) -> Vec<Signal> {
        let mut signals = Vec::new();

        // Uniqueness signal
        if self.uniqueness_score > 0.0 {
            let explanation = if self.uniqueness_score >= 1.0 {
                "Target column is unique (likely PK)".to_string()
            } else {
                format!(
                    "Target column has {:.0}% distinct values",
                    self.uniqueness_score * 200.0 // Scale back from 0.5 max
                )
            };
            signals.push(Signal::positive(
                SignalSource::UniqueConstraint,
                self.uniqueness_score * 0.3, // Weight: uniqueness contributes up to 0.3
                explanation,
            ));
        }

        // Overlap signal
        if self.overlap_score > 0.0 {
            let explanation = format!(
                "{:.1}% of source values exist in target",
                self.overlap_score * 100.0
            );

            // High overlap is a strong positive signal
            let score = if self.overlap_score >= 0.95 {
                0.4 // Very high overlap
            } else if self.overlap_score >= 0.8 {
                0.3
            } else if self.overlap_score >= 0.5 {
                0.2
            } else {
                self.overlap_score * 0.2 // Low overlap, proportional score
            };

            signals.push(Signal::positive(
                SignalSource::ValueOverlap,
                score,
                explanation,
            ));
        } else {
            // Zero overlap is a strong negative signal
            signals.push(Signal::negative(
                SignalSource::ValueOverlap,
                0.8, // Strong penalty
                "No value overlap between columns".to_string(),
            ));
        }

        // Null rate penalty (negative signal)
        if self.null_rate_penalty > 0.1 {
            signals.push(Signal::negative(
                SignalSource::ColumnStatistics,
                self.null_rate_penalty,
                format!(
                    "High null rate in source column ({:.0}%)",
                    self.null_rate_penalty * 200.0 // Approximate
                ),
            ));
        }

        // Invalid FK pattern (strong negative)
        if !self.is_valid_fk && self.overlap_score < 0.5 {
            signals.push(Signal::negative(
                SignalSource::ColumnStatistics,
                0.5,
                "Target is not a superset of source values".to_string(),
            ));
        }

        signals
    }

    /// Get the cardinality hint as a signal explanation.
    pub fn cardinality_explanation(&self) -> Option<String> {
        self.cardinality_hint.and_then(|c| {
            let (from_side, to_side) = match c {
                Cardinality::OneToOne => ("one", "one"),
                Cardinality::OneToMany => ("one", "many"),
                Cardinality::ManyToOne => ("many", "one"),
                Cardinality::ManyToMany => ("many", "many"),
                Cardinality::Unknown => return None, // No explanation for unknown
            };
            Some(format!(
                "Cardinality: {} source rows map to {} target rows",
                from_side, to_side
            ))
        })
    }
}

/// Collector for gathering statistics signals.
///
/// This is used in the async inference path where we have access
/// to a MetadataProvider for database queries.
#[derive(Debug, Clone, Default)]
pub struct StatisticsCollector {
    /// Minimum overlap percentage to consider a relationship valid.
    pub min_overlap_threshold: f64,
    /// Whether to require uniqueness on target column.
    pub require_unique_target: bool,
}

impl StatisticsCollector {
    /// Create a new statistics collector with default settings.
    pub fn new() -> Self {
        Self {
            min_overlap_threshold: 0.5, // 50% minimum overlap
            require_unique_target: false,
        }
    }

    /// Create a strict collector that requires high overlap and unique target.
    pub fn strict() -> Self {
        Self {
            min_overlap_threshold: 0.9,
            require_unique_target: true,
        }
    }

    /// Create a lenient collector for exploratory inference.
    pub fn lenient() -> Self {
        Self {
            min_overlap_threshold: 0.2,
            require_unique_target: false,
        }
    }

    /// Collect signals from statistics.
    ///
    /// Returns `None` if the relationship doesn't meet minimum thresholds.
    pub fn collect(
        &self,
        from_stats: &ColumnStats,
        to_stats: &ColumnStats,
        overlap: &ValueOverlap,
    ) -> Option<StatisticsSignals> {
        // Check minimum overlap threshold
        if overlap.overlap_percentage / 100.0 < self.min_overlap_threshold {
            return None;
        }

        // Check uniqueness requirement
        if self.require_unique_target && !to_stats.is_unique {
            return None;
        }

        Some(StatisticsSignals::from_stats(from_stats, to_stats, overlap))
    }

    /// Quick check if stats suggest a valid FK relationship.
    pub fn is_likely_fk(
        &self,
        from_stats: &ColumnStats,
        to_stats: &ColumnStats,
        overlap: &ValueOverlap,
    ) -> bool {
        // Target should be unique or mostly unique
        let target_ok = to_stats.is_unique
            || (to_stats.total_count > 0
                && to_stats.distinct_count as f64 / to_stats.total_count as f64 > 0.9);

        // Should have good overlap
        let overlap_ok = overlap.overlap_percentage >= 80.0 || overlap.right_is_superset;

        // Source shouldn't be too sparse (high null rate)
        let not_sparse = from_stats.total_count == 0
            || (from_stats.null_count as f64 / from_stats.total_count as f64) < 0.5;

        target_ok && overlap_ok && not_sparse
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_unique_stats(total: i64, nulls: i64) -> ColumnStats {
        ColumnStats {
            total_count: total,
            distinct_count: total - nulls,
            null_count: nulls,
            is_unique: true,
            sample_values: vec![],
        }
    }

    fn make_non_unique_stats(total: i64, distinct: i64, nulls: i64) -> ColumnStats {
        ColumnStats {
            total_count: total,
            distinct_count: distinct,
            null_count: nulls,
            is_unique: false,
            sample_values: vec![],
        }
    }

    fn make_overlap(pct: f64, superset: bool) -> ValueOverlap {
        ValueOverlap {
            left_sample_size: 100,
            left_total_distinct: 100,
            right_total_distinct: 150,
            overlap_count: (pct as i64),
            overlap_percentage: pct,
            right_is_superset: superset,
            left_is_unique: false,
            right_is_unique: true,
        }
    }

    #[test]
    fn test_perfect_fk_relationship() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);
        let overlap = make_overlap(99.0, true);

        let signals = StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap);

        assert_eq!(signals.uniqueness_score, 1.0);
        assert!(signals.overlap_score > 0.95);
        assert!(signals.is_valid_fk);
        assert_eq!(signals.cardinality_hint, Some(Cardinality::ManyToOne));
    }

    #[test]
    fn test_low_overlap_relationship() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);
        let overlap = make_overlap(30.0, false);

        let signals = StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap);

        assert!(!signals.is_valid_fk);
        assert!(signals.overlap_score < 0.5);

        let signal_vec = signals.to_signals();
        // Should have negative signal for low overlap
        assert!(signal_vec.iter().any(|s| s.is_negative()));
    }

    #[test]
    fn test_high_null_rate_penalty() {
        let from_stats = make_non_unique_stats(1000, 100, 800); // 80% nulls
        let to_stats = make_unique_stats(100, 0);
        let overlap = make_overlap(95.0, true);

        let signals = StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap);

        // Should have penalty for high null rate
        assert!(signals.null_rate_penalty > 0.0);
    }

    #[test]
    fn test_cardinality_inference() {
        // Many-to-one: source not unique, target unique
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);
        let overlap = make_overlap(95.0, true);

        let signals = StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap);
        assert_eq!(signals.cardinality_hint, Some(Cardinality::ManyToOne));

        // One-to-one: both unique
        let from_unique = make_unique_stats(100, 0);
        let signals = StatisticsSignals::from_stats(&from_unique, &to_stats, &overlap);
        assert_eq!(signals.cardinality_hint, Some(Cardinality::OneToOne));
    }

    #[test]
    fn test_collector_thresholds() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);

        // High overlap passes
        let high_overlap = make_overlap(80.0, true);
        let collector = StatisticsCollector::new();
        assert!(collector.collect(&from_stats, &to_stats, &high_overlap).is_some());

        // Low overlap filtered out
        let low_overlap = make_overlap(30.0, false);
        assert!(collector.collect(&from_stats, &to_stats, &low_overlap).is_none());
    }

    #[test]
    fn test_strict_collector() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let non_unique_target = make_non_unique_stats(500, 400, 0);
        let overlap = make_overlap(95.0, true);

        let collector = StatisticsCollector::strict();
        // Should reject non-unique target
        assert!(collector.collect(&from_stats, &non_unique_target, &overlap).is_none());
    }

    #[test]
    fn test_to_signals() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);
        let overlap = make_overlap(98.0, true);

        let stats = StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap);
        let signals = stats.to_signals();

        // Should have uniqueness and overlap signals
        assert!(signals.iter().any(|s| matches!(s.source, SignalSource::UniqueConstraint)));
        assert!(signals.iter().any(|s| matches!(s.source, SignalSource::ValueOverlap)));

        // All should be positive for good relationship
        assert!(signals.iter().all(|s| s.is_positive()));
    }

    #[test]
    fn test_zero_overlap_negative_signal() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);
        let overlap = make_overlap(0.0, false);

        let stats = StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap);
        let signals = stats.to_signals();

        // Should have strong negative signal
        let overlap_signals: Vec<_> = signals
            .iter()
            .filter(|s| matches!(s.source, SignalSource::ValueOverlap))
            .collect();
        assert!(!overlap_signals.is_empty());
        assert!(overlap_signals[0].is_negative());
    }

    #[test]
    fn test_is_likely_fk() {
        let from_stats = make_non_unique_stats(1000, 500, 0);
        let to_stats = make_unique_stats(500, 0);

        let collector = StatisticsCollector::new();

        // Good relationship
        let good_overlap = make_overlap(90.0, true);
        assert!(collector.is_likely_fk(&from_stats, &to_stats, &good_overlap));

        // Bad overlap
        let bad_overlap = make_overlap(20.0, false);
        assert!(!collector.is_likely_fk(&from_stats, &to_stats, &bad_overlap));

        // Sparse source (high nulls)
        let sparse_from = make_non_unique_stats(1000, 100, 600);
        assert!(!collector.is_likely_fk(&sparse_from, &to_stats, &good_overlap));
    }
}
