//! Signal aggregation for combining multiple signals into a final confidence score.
//!
//! The aggregator uses configurable weights per signal source type and handles
//! both positive and negative signals to produce a final confidence score.

use std::collections::HashMap;

use super::{AggregatedScore, ScoreBreakdown, SignalCollection, SignalSource};
#[cfg(test)]
use super::Signal;

/// Configuration for signal weights.
#[derive(Debug, Clone)]
pub struct SignalWeights {
    /// Weight multipliers by signal source kind.
    weights: HashMap<&'static str, f64>,
    /// Default weight for unknown sources.
    default_weight: f64,
    /// Minimum confidence threshold (scores below this are rejected).
    pub min_confidence: f64,
    /// Maximum penalty from negative signals (0.0 to 1.0).
    pub max_negative_penalty: f64,
}

impl Default for SignalWeights {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalWeights {
    /// Create default signal weights.
    pub fn new() -> Self {
        let mut weights = HashMap::new();

        // Foreign key constraints are definitive
        weights.insert("fk", 1.0);

        // Naming conventions are strong signals
        weights.insert("naming", 0.85);

        // Type compatibility is important
        weights.insert("type", 0.75);

        // Statistics are valuable but noisy
        weights.insert("stats", 0.65);

        // Unique constraints help validate PKs
        weights.insert("unique", 0.70);

        // Value overlap is strong when available
        weights.insert("overlap", 0.80);

        // Negative signals have high weight (to filter false positives)
        weights.insert("negative", 0.90);

        // Schema conventions are database-specific
        weights.insert("convention", 0.60);

        // Learned patterns depend on quality of training data
        weights.insert("learned", 0.55);

        Self {
            weights,
            default_weight: 0.5,
            min_confidence: 0.5,
            max_negative_penalty: 0.8,
        }
    }

    /// Get the weight for a signal source kind.
    pub fn get(&self, kind: &str) -> f64 {
        *self.weights.get(kind).unwrap_or(&self.default_weight)
    }

    /// Set the weight for a signal source kind.
    pub fn set(&mut self, kind: &'static str, weight: f64) {
        self.weights.insert(kind, weight.clamp(0.0, 1.0));
    }

    /// Create weights optimized for high precision (fewer false positives).
    pub fn high_precision() -> Self {
        let mut weights = Self::new();
        weights.min_confidence = 0.7;
        weights.max_negative_penalty = 0.9;
        weights.set("naming", 0.75);
        weights.set("stats", 0.55);
        weights
    }

    /// Create weights optimized for high recall (fewer false negatives).
    pub fn high_recall() -> Self {
        let mut weights = Self::new();
        weights.min_confidence = 0.4;
        weights.max_negative_penalty = 0.6;
        weights.set("naming", 0.90);
        weights.set("stats", 0.75);
        weights
    }
}

/// Aggregates multiple signals into a final confidence score.
#[derive(Debug, Clone)]
pub struct SignalAggregator {
    weights: SignalWeights,
}

impl Default for SignalAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalAggregator {
    /// Create an aggregator with default weights.
    pub fn new() -> Self {
        Self {
            weights: SignalWeights::new(),
        }
    }

    /// Create an aggregator with custom weights.
    pub fn with_weights(weights: SignalWeights) -> Self {
        Self { weights }
    }

    /// Aggregate signals into a final score.
    ///
    /// The aggregation algorithm:
    /// 1. Separate positive and negative signals
    /// 2. If a definitive signal exists (FK constraint), use it directly
    /// 3. Calculate weighted sum of positive signals
    /// 4. Apply negative signal penalties
    /// 5. Normalize to 0.0-1.0 range
    pub fn aggregate(&self, collection: &SignalCollection) -> AggregatedScore {
        if collection.is_empty() {
            return AggregatedScore {
                confidence: 0.0,
                signals: vec![],
                explanation: "No signals collected".to_string(),
                breakdown: vec![],
            };
        }

        // Check for definitive signals (FK constraints)
        if let Some(fk_signal) = collection.signals().iter().find(|s| {
            matches!(s.source, SignalSource::ForeignKeyConstraint) && s.score > 0.9
        }) {
            return AggregatedScore {
                confidence: fk_signal.score.min(1.0),
                signals: vec![fk_signal.clone()],
                explanation: "Foreign key constraint exists in database".to_string(),
                breakdown: vec![ScoreBreakdown {
                    source: "fk".to_string(),
                    raw_score: fk_signal.score,
                    weight: 1.0,
                    contribution: fk_signal.score,
                    explanation: fk_signal.explanation.clone(),
                }],
            };
        }

        // Separate signals
        let positive_signals: Vec<_> = collection.positive_signals().collect();
        let negative_signals: Vec<_> = collection.negative_signals().collect();

        // Calculate weighted positive score
        let mut breakdown = Vec::new();
        let mut total_weight = 0.0;
        let mut weighted_sum = 0.0;

        for signal in &positive_signals {
            let kind = signal.source.kind();
            let source_weight = self.weights.get(kind);
            let effective_weight = signal.weight * source_weight;

            let contribution = signal.score * effective_weight;
            weighted_sum += contribution;
            total_weight += effective_weight;

            breakdown.push(ScoreBreakdown {
                source: kind.to_string(),
                raw_score: signal.score,
                weight: effective_weight,
                contribution,
                explanation: signal.explanation.clone(),
            });
        }

        // Calculate base confidence
        let base_confidence = if total_weight > 0.0 {
            weighted_sum / total_weight
        } else {
            0.0
        };

        // Apply negative penalties
        let mut penalty_sum = 0.0;
        for signal in &negative_signals {
            let kind = signal.source.kind();
            let source_weight = self.weights.get(kind);
            let effective_weight = signal.weight * source_weight;

            // Negative scores are negative, so this subtracts from penalty_sum
            let contribution = signal.score.abs() * effective_weight;
            penalty_sum += contribution;

            breakdown.push(ScoreBreakdown {
                source: kind.to_string(),
                raw_score: signal.score,
                weight: effective_weight,
                contribution: -contribution,
                explanation: signal.explanation.clone(),
            });
        }

        // Cap the penalty
        let capped_penalty = penalty_sum.min(self.weights.max_negative_penalty);

        // Final confidence
        let final_confidence = (base_confidence - capped_penalty).clamp(0.0, 1.0);

        // Generate explanation
        let explanation = self.generate_explanation(
            final_confidence,
            base_confidence,
            capped_penalty,
            positive_signals.len(),
            negative_signals.len(),
        );

        AggregatedScore {
            confidence: final_confidence,
            signals: collection.signals().to_vec(),
            explanation,
            breakdown,
        }
    }

    /// Check if the aggregated score meets the minimum confidence threshold.
    pub fn is_confident(&self, score: &AggregatedScore) -> bool {
        score.confidence >= self.weights.min_confidence
    }

    /// Get the minimum confidence threshold.
    pub fn min_confidence(&self) -> f64 {
        self.weights.min_confidence
    }

    /// Generate a human-readable explanation of the score.
    fn generate_explanation(
        &self,
        final_score: f64,
        base_score: f64,
        penalty: f64,
        positive_count: usize,
        negative_count: usize,
    ) -> String {
        let confidence_level = match final_score {
            s if s >= 0.9 => "Very High",
            s if s >= 0.75 => "High",
            s if s >= 0.6 => "Medium",
            s if s >= 0.4 => "Low",
            _ => "Very Low",
        };

        let mut parts = vec![format!(
            "{} confidence ({:.0}%)",
            confidence_level,
            final_score * 100.0
        )];

        if positive_count > 0 {
            parts.push(format!(
                "{} positive signal{}",
                positive_count,
                if positive_count == 1 { "" } else { "s" }
            ));
        }

        if negative_count > 0 {
            parts.push(format!(
                "{} negative signal{} (penalty: {:.0}%)",
                negative_count,
                if negative_count == 1 { "" } else { "s" },
                penalty * 100.0
            ));
        }

        if penalty > 0.0 {
            parts.push(format!(
                "Base: {:.0}% → Final: {:.0}%",
                base_score * 100.0,
                final_score * 100.0
            ));
        }

        parts.join(". ")
    }
}

/// Aggregate multiple candidate scores and return top K.
pub fn rank_candidates(
    aggregator: &SignalAggregator,
    candidates: Vec<SignalCollection>,
    top_k: usize,
) -> Vec<AggregatedScore> {
    let mut scores: Vec<_> = candidates
        .iter()
        .map(|c| aggregator.aggregate(c))
        .filter(|s| aggregator.is_confident(s))
        .collect();

    // Sort by confidence (descending)
    scores.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    scores.truncate(top_k);
    scores
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_collection() {
        let aggregator = SignalAggregator::new();
        let collection = SignalCollection::new();

        let score = aggregator.aggregate(&collection);
        assert_eq!(score.confidence, 0.0);
        assert!(score.signals.is_empty());
    }

    #[test]
    fn test_single_positive_signal() {
        let aggregator = SignalAggregator::new();
        let mut collection = SignalCollection::new();
        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Column ends with _id",
        ));

        let score = aggregator.aggregate(&collection);
        assert!(score.confidence > 0.0);
        assert_eq!(score.signals.len(), 1);
    }

    #[test]
    fn test_fk_constraint_is_definitive() {
        let aggregator = SignalAggregator::new();
        let mut collection = SignalCollection::new();

        // Add a naming signal
        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Column ends with _id",
        ));

        // Add FK constraint
        collection.add(Signal::positive(
            SignalSource::ForeignKeyConstraint,
            0.99,
            "FK constraint exists",
        ));

        let score = aggregator.aggregate(&collection);
        // FK constraint should dominate
        assert!(score.confidence >= 0.99);
    }

    #[test]
    fn test_negative_signal_penalty() {
        let aggregator = SignalAggregator::new();
        let mut collection = SignalCollection::new();

        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Column ends with _id",
        ));

        let score_without_negative = aggregator.aggregate(&collection);

        collection.add(Signal::negative(
            SignalSource::negative("timestamp_column"),
            0.7,
            "Column looks like a timestamp",
        ));

        let score_with_negative = aggregator.aggregate(&collection);

        // Negative signal should reduce confidence
        assert!(score_with_negative.confidence < score_without_negative.confidence);
    }

    #[test]
    fn test_multiple_positive_signals() {
        let aggregator = SignalAggregator::new();
        let mut collection = SignalCollection::new();

        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Column ends with _id",
        ));
        collection.add(Signal::positive(
            SignalSource::TypeCompatibility,
            0.9,
            "Types are compatible",
        ));

        let score = aggregator.aggregate(&collection);

        // Multiple positive signals should produce good confidence
        assert!(score.confidence > 0.7);
        assert_eq!(score.breakdown.len(), 2);
    }

    #[test]
    fn test_confidence_threshold() {
        let aggregator = SignalAggregator::new();
        let mut collection = SignalCollection::new();

        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.4,
            "Weak match",
        ));

        let score = aggregator.aggregate(&collection);

        // Default threshold is 0.5
        assert!(!aggregator.is_confident(&score));
    }

    #[test]
    fn test_high_precision_weights() {
        let aggregator = SignalAggregator::with_weights(SignalWeights::high_precision());
        let mut collection = SignalCollection::new();

        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.6,
            "Medium match",
        ));

        let score = aggregator.aggregate(&collection);

        // High precision has higher threshold (0.7)
        assert!(!aggregator.is_confident(&score));
        assert_eq!(aggregator.min_confidence(), 0.7);
    }

    #[test]
    fn test_high_recall_weights() {
        let aggregator = SignalAggregator::with_weights(SignalWeights::high_recall());
        let mut collection = SignalCollection::new();

        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.45,
            "Weak match",
        ));

        let score = aggregator.aggregate(&collection);

        // High recall has lower threshold (0.4)
        assert!(aggregator.is_confident(&score));
        assert_eq!(aggregator.min_confidence(), 0.4);
    }

    #[test]
    fn test_rank_candidates() {
        let aggregator = SignalAggregator::new();

        let mut c1 = SignalCollection::new();
        c1.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Strong match",
        ));

        let mut c2 = SignalCollection::new();
        c2.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.6,
            "Medium match",
        ));

        let mut c3 = SignalCollection::new();
        c3.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.3,
            "Weak match",
        ));

        let ranked = rank_candidates(&aggregator, vec![c2, c1, c3], 2);

        // Should be sorted by confidence, top 2 only
        assert_eq!(ranked.len(), 2);
        assert!(ranked[0].confidence > ranked[1].confidence);
        // c3 should be filtered out (below threshold)
    }

    #[test]
    fn test_explanation_generation() {
        let aggregator = SignalAggregator::new();
        let mut collection = SignalCollection::new();

        collection.add(Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Column ends with _id",
        ));
        collection.add(Signal::negative(
            SignalSource::negative("audit"),
            0.3,
            "Might be audit column",
        ));

        let score = aggregator.aggregate(&collection);

        assert!(score.explanation.contains("confidence"));
        assert!(score.explanation.contains("positive signal"));
        assert!(score.explanation.contains("negative signal"));
    }
}
