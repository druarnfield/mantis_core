//! Type compatibility signal generation.
//!
//! Determines if two columns have compatible types for a foreign key relationship.
//! Uses a compatibility matrix rather than simple string comparison.

use crate::model::DataType;

use super::{Signal, SignalSource};

/// Result of a type compatibility check.
#[derive(Debug, Clone)]
pub struct TypeCompatibility {
    /// Compatibility score (0.0 to 1.0).
    pub score: f64,

    /// Types are exactly the same.
    pub is_exact: bool,

    /// Types can be joined (compatible but not identical).
    pub is_compatible: bool,

    /// Human-readable explanation.
    pub explanation: String,
}

impl TypeCompatibility {
    /// Check compatibility between two data types.
    pub fn check(from: &DataType, to: &DataType) -> Self {
        // Exact match
        if from == to {
            return Self {
                score: 1.0,
                is_exact: true,
                is_compatible: true,
                explanation: format!("{:?} exactly matches {:?}", from, to),
            };
        }

        // Check compatibility by category
        let score = Self::compatibility_score(from, to);

        if score > 0.0 {
            Self {
                score,
                is_exact: false,
                is_compatible: true,
                explanation: format!("{:?} is compatible with {:?}", from, to),
            }
        } else {
            Self {
                score: 0.0,
                is_exact: false,
                is_compatible: false,
                explanation: format!("{:?} is not compatible with {:?}", from, to),
            }
        }
    }

    /// Generate a signal from this compatibility check.
    pub fn to_signal(&self) -> Signal {
        if self.score > 0.0 {
            Signal::positive(
                SignalSource::TypeCompatibility,
                self.score,
                &self.explanation,
            )
        } else {
            Signal::negative(
                SignalSource::TypeCompatibility,
                0.3, // Mild penalty for type mismatch
                &self.explanation,
            )
        }
    }

    /// Calculate compatibility score between two types.
    fn compatibility_score(from: &DataType, to: &DataType) -> f64 {
        // TODO: Implement proper type compatibility when detailed SQL DataType is available
        // Current DataType only has: String, Int, Decimal, Float, Bool, Date, Timestamp
        use DataType::*;

        match (from, to) {
            // Exact matches
            (String, String) => 1.0,
            (Int, Int) => 1.0,
            (Decimal, Decimal) => 1.0,
            (Float, Float) => 1.0,
            (Bool, Bool) => 1.0,
            (Date, Date) => 1.0,
            (Timestamp, Timestamp) => 1.0,

            // Compatible numeric types
            (Int, Decimal) => 0.6,
            (Decimal, Int) => 0.5,
            (Int, Float) => 0.5,
            (Float, Int) => 0.4,
            (Float, Decimal) => 0.4,
            (Decimal, Float) => 0.4,

            // Incompatible types
            (Bool, _) | (_, Bool) => 0.0,
            (Date, _) | (_, Date) => 0.0,
            (Timestamp, _) | (_, Timestamp) => 0.0,

            // Default: incompatible
            _ => 0.0,
        }
    }
}

/// Extension trait for DataType to check type categories.
pub trait DataTypeExt {
    /// Is this an integer type?
    fn is_integer(&self) -> bool;

    /// Is this a string type?
    fn is_string(&self) -> bool;

    /// Is this a floating point type?
    fn is_float(&self) -> bool;

    /// Is this a temporal type?
    fn is_temporal(&self) -> bool;

    /// Is this a UUID type?
    fn is_uuid(&self) -> bool;

    /// Get the bit width for integer types.
    fn integer_bits(&self) -> Option<u8>;
}

impl DataTypeExt for DataType {
    fn is_integer(&self) -> bool {
        matches!(self, DataType::Int)
    }

    fn is_string(&self) -> bool {
        matches!(self, DataType::String)
    }

    fn is_float(&self) -> bool {
        matches!(self, DataType::Float)
    }

    fn is_temporal(&self) -> bool {
        matches!(self, DataType::Date | DataType::Timestamp)
    }

    fn is_uuid(&self) -> bool {
        // Current DataType doesn't have UUID variant
        false
    }

    fn integer_bits(&self) -> Option<u8> {
        // Current DataType doesn't distinguish integer sizes
        match self {
            DataType::Int => Some(64), // Assume 64-bit
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Re-enable tests when detailed SQL DataType is available
    // Current DataType only has: String, Int, Decimal, Float, Bool, Date, Timestamp

    #[test]
    fn test_exact_match() {
        let result = TypeCompatibility::check(&DataType::Int, &DataType::Int);
        assert!(result.is_exact);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_numeric_compatibility() {
        // Int to Decimal should be compatible
        let result = TypeCompatibility::check(&DataType::Int, &DataType::Decimal);
        assert!(result.is_compatible);
        assert!(result.score >= 0.5);

        // Int to Float should be compatible
        let result = TypeCompatibility::check(&DataType::Int, &DataType::Float);
        assert!(result.is_compatible);
        assert!(result.score >= 0.4);
    }

    #[test]
    fn test_string_compatibility() {
        let result = TypeCompatibility::check(&DataType::String, &DataType::String);
        assert!(result.is_compatible);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_incompatible_types() {
        // Bool to Int - incompatible
        let result = TypeCompatibility::check(&DataType::Bool, &DataType::Int);
        assert!(!result.is_compatible);
        assert_eq!(result.score, 0.0);

        // Timestamp to Int - incompatible
        let result = TypeCompatibility::check(&DataType::Timestamp, &DataType::Int);
        assert!(!result.is_compatible);
    }

    #[test]
    fn test_decimal_compatibility() {
        // Exact decimal match
        let result = TypeCompatibility::check(&DataType::Decimal, &DataType::Decimal);
        assert!(result.is_exact);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_signal_generation() {
        let compat = TypeCompatibility::check(&DataType::Int, &DataType::Int);
        let signal = compat.to_signal();
        assert!(signal.is_positive());
        assert_eq!(signal.score, 1.0);

        let compat = TypeCompatibility::check(&DataType::Bool, &DataType::Int);
        let signal = compat.to_signal();
        assert!(signal.is_negative());
    }

    #[test]
    fn test_datatype_ext() {
        assert!(DataType::Int.is_integer());
        assert!(DataType::String.is_string());
        assert!(DataType::Float.is_float());
        assert!(DataType::Timestamp.is_temporal());
        assert!(!DataType::String.is_uuid()); // No UUID in current DataType

        assert_eq!(DataType::Int.integer_bits(), Some(64));
        assert_eq!(DataType::String.integer_bits(), None);
    }
}
