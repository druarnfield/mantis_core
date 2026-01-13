//! Signal-based inference system.
//!
//! Each signal contributes a score between -1.0 and 1.0:
//! - **Positive signals** (0.0 to 1.0) increase confidence
//! - **Negative signals** (-1.0 to 0.0) decrease confidence
//!
//! Signals are collected from multiple sources and aggregated
//! with configurable weights to produce a final confidence score.

pub mod aggregator;
pub mod constraints;
pub mod conventions;
pub mod inflection;
pub mod naming;
pub mod negative;
pub mod pipeline;
pub mod statistics;
pub mod types;

// Re-export inflection utilities for convenience
pub use inflection::{pluralize, singularize};

use serde::{Deserialize, Serialize};

/// Source of a signal contributing to relationship inference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SignalSource {
    /// Naming convention rule matched.
    NamingConvention {
        /// Name of the rule that matched.
        rule: String,
    },

    /// Type compatibility check.
    TypeCompatibility,

    /// Column statistics (uniqueness, null rate).
    ColumnStatistics,

    /// Foreign key constraint exists in database.
    ForeignKeyConstraint,

    /// Actual database constraint (FK, unique, PK) - highest confidence.
    DatabaseConstraint,

    /// Unique constraint on target column.
    UniqueConstraint,

    /// Value overlap between columns.
    ValueOverlap,

    /// Negative pattern matched (reduces confidence).
    NegativePattern {
        /// Name of the pattern that matched.
        pattern: String,
    },

    /// Schema convention detected.
    SchemaConvention {
        /// Description of the convention.
        convention: String,
    },

    /// Learned from existing relationships.
    LearnedPattern {
        /// Source relationship that informed this.
        source: String,
    },
}

impl SignalSource {
    /// Create a naming convention signal source.
    pub fn naming(rule: impl Into<String>) -> Self {
        Self::NamingConvention { rule: rule.into() }
    }

    /// Create a negative pattern signal source.
    pub fn negative(pattern: impl Into<String>) -> Self {
        Self::NegativePattern {
            pattern: pattern.into(),
        }
    }

    /// Create a schema convention signal source.
    pub fn convention(convention: impl Into<String>) -> Self {
        Self::SchemaConvention {
            convention: convention.into(),
        }
    }

    /// Get a short identifier for this source type.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NamingConvention { .. } => "naming",
            Self::TypeCompatibility => "type",
            Self::ColumnStatistics => "stats",
            Self::ForeignKeyConstraint => "fk",
            Self::DatabaseConstraint => "db_constraint",
            Self::UniqueConstraint => "unique",
            Self::ValueOverlap => "overlap",
            Self::NegativePattern { .. } => "negative",
            Self::SchemaConvention { .. } => "convention",
            Self::LearnedPattern { .. } => "learned",
        }
    }
}

/// A signal contributing to relationship confidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    /// Source of this signal.
    pub source: SignalSource,

    /// Score contribution (-1.0 to 1.0).
    /// Positive values increase confidence, negative values decrease it.
    pub score: f64,

    /// Weight for this signal (0.0 to 1.0).
    /// Higher weight means more influence on final score.
    pub weight: f64,

    /// Human-readable explanation of why this signal was generated.
    pub explanation: String,
}

impl Signal {
    /// Create a new signal.
    pub fn new(source: SignalSource, score: f64, explanation: impl Into<String>) -> Self {
        Self {
            source,
            score: score.clamp(-1.0, 1.0),
            weight: 1.0,
            explanation: explanation.into(),
        }
    }

    /// Create a positive signal (score 0.0 to 1.0).
    pub fn positive(source: SignalSource, score: f64, explanation: impl Into<String>) -> Self {
        Self::new(source, score.clamp(0.0, 1.0), explanation)
    }

    /// Create a negative signal (score -1.0 to 0.0).
    pub fn negative(source: SignalSource, score: f64, explanation: impl Into<String>) -> Self {
        Self::new(source, -score.abs().clamp(0.0, 1.0), explanation)
    }

    /// Set the weight for this signal.
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight.clamp(0.0, 1.0);
        self
    }

    /// Calculate the weighted contribution of this signal.
    pub fn contribution(&self) -> f64 {
        self.score * self.weight
    }

    /// Is this a positive signal?
    pub fn is_positive(&self) -> bool {
        self.score > 0.0
    }

    /// Is this a negative signal?
    pub fn is_negative(&self) -> bool {
        self.score < 0.0
    }
}

/// Collection of signals for a single candidate relationship.
#[derive(Debug, Clone, Default)]
pub struct SignalCollection {
    signals: Vec<Signal>,
}

impl SignalCollection {
    /// Create an empty signal collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a signal to the collection.
    pub fn add(&mut self, signal: Signal) {
        self.signals.push(signal);
    }

    /// Add multiple signals.
    pub fn extend(&mut self, signals: impl IntoIterator<Item = Signal>) {
        self.signals.extend(signals);
    }

    /// Get all signals.
    pub fn signals(&self) -> &[Signal] {
        &self.signals
    }

    /// Get positive signals only.
    pub fn positive_signals(&self) -> impl Iterator<Item = &Signal> {
        self.signals.iter().filter(|s| s.is_positive())
    }

    /// Get negative signals only.
    pub fn negative_signals(&self) -> impl Iterator<Item = &Signal> {
        self.signals.iter().filter(|s| s.is_negative())
    }

    /// Check if any signal is definitive (database constraint).
    pub fn has_definitive_signal(&self) -> bool {
        self.signals.iter().any(|s| {
            matches!(
                s.source,
                SignalSource::ForeignKeyConstraint | SignalSource::DatabaseConstraint
            ) && s.score > 0.9
        })
    }

    /// Get the strongest positive signal.
    pub fn strongest_positive(&self) -> Option<&Signal> {
        self.positive_signals()
            .max_by(|a, b| a.score.partial_cmp(&b.score).unwrap())
    }

    /// Get the strongest negative signal.
    pub fn strongest_negative(&self) -> Option<&Signal> {
        self.negative_signals()
            .min_by(|a, b| a.score.partial_cmp(&b.score).unwrap())
    }

    /// Count signals by source kind.
    pub fn count_by_kind(&self) -> std::collections::HashMap<&'static str, usize> {
        let mut counts = std::collections::HashMap::new();
        for signal in &self.signals {
            *counts.entry(signal.source.kind()).or_insert(0) += 1;
        }
        counts
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.signals.is_empty()
    }

    /// Number of signals.
    pub fn len(&self) -> usize {
        self.signals.len()
    }
}

/// Aggregated score from multiple signals.
#[derive(Debug, Clone)]
pub struct AggregatedScore {
    /// Final confidence score (0.0 to 1.0).
    pub confidence: f64,

    /// All signals that contributed to this score.
    pub signals: Vec<Signal>,

    /// Human-readable explanation of the score.
    pub explanation: String,

    /// Breakdown of contribution by signal source.
    pub breakdown: Vec<ScoreBreakdown>,
}

/// Breakdown of how a signal contributed to the final score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreBreakdown {
    /// Signal source kind.
    pub source: String,

    /// Raw score from this signal.
    pub raw_score: f64,

    /// Weight applied.
    pub weight: f64,

    /// Final contribution to score.
    pub contribution: f64,

    /// Explanation.
    pub explanation: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_creation() {
        let signal = Signal::positive(
            SignalSource::naming("suffix_id"),
            0.85,
            "Column ends with _id",
        );

        assert_eq!(signal.score, 0.85);
        assert!(signal.is_positive());
        assert!(!signal.is_negative());
    }

    #[test]
    fn test_negative_signal() {
        let signal = Signal::negative(
            SignalSource::negative("timestamp_column"),
            0.8,
            "Column is a timestamp",
        );

        assert_eq!(signal.score, -0.8);
        assert!(signal.is_negative());
    }

    #[test]
    fn test_signal_clamping() {
        let signal = Signal::new(SignalSource::TypeCompatibility, 1.5, "Over max");
        assert_eq!(signal.score, 1.0);

        let signal = Signal::new(SignalSource::TypeCompatibility, -1.5, "Under min");
        assert_eq!(signal.score, -1.0);
    }

    #[test]
    fn test_signal_collection() {
        let mut collection = SignalCollection::new();
        collection.add(Signal::positive(SignalSource::naming("suffix_id"), 0.85, "test"));
        collection.add(Signal::negative(SignalSource::negative("timestamp"), 0.5, "test"));

        assert_eq!(collection.len(), 2);
        assert_eq!(collection.positive_signals().count(), 1);
        assert_eq!(collection.negative_signals().count(), 1);
    }

    #[test]
    fn test_weighted_contribution() {
        let signal = Signal::positive(SignalSource::naming("test"), 0.8, "test").with_weight(0.5);

        assert_eq!(signal.contribution(), 0.4);
    }
}
