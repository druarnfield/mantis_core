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
            Signal::positive(SignalSource::TypeCompatibility, self.score, &self.explanation)
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
        use DataType::*;

        match (from, to) {
            // === Integer family ===
            // Smaller to larger is better
            (Int8, Int8) => 1.0,
            (Int8, Int16 | Int32 | Int64) => 0.9,
            (Int16, Int16) => 1.0,
            (Int16, Int8) => 0.7,
            (Int16, Int32 | Int64) => 0.9,
            (Int32, Int32) => 1.0,
            (Int32, Int8 | Int16) => 0.6,
            (Int32, Int64) => 0.9,
            (Int64, Int64) => 1.0,
            (Int64, Int8 | Int16 | Int32) => 0.5,

            // Integer to Decimal (common pattern)
            (Int8 | Int16 | Int32 | Int64, Decimal(_, _)) => 0.6,
            (Decimal(_, _), Int8 | Int16 | Int32 | Int64) => 0.5,

            // === String family ===
            (String, String) => 1.0,
            (String, Varchar(_)) => 0.95,
            (String, Char(_)) => 0.85,
            (Varchar(_), String) => 0.95,
            (Varchar(_), Varchar(_)) => 0.95, // Different lengths are ok
            (Varchar(_), Char(_)) => 0.8,
            (Char(_), String) => 0.85,
            (Char(_), Varchar(_)) => 0.8,
            (Char(_), Char(_)) => 0.9, // Different lengths

            // === UUID / String interop ===
            // UUID stored as string is common
            (Uuid, String | Varchar(_)) => 0.7,
            (String | Varchar(_), Uuid) => 0.7,

            // === Float family ===
            (Float32, Float32) => 1.0,
            (Float32, Float64) => 0.9,
            (Float64, Float32) => 0.7,
            (Float64, Float64) => 1.0,

            // Float/Decimal interop (less common for FKs but possible)
            (Float32 | Float64, Decimal(_, _)) => 0.4,
            (Decimal(_, _), Float32 | Float64) => 0.4,

            // === Decimal ===
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // Same precision/scale is best
                if p1 == p2 && s1 == s2 {
                    1.0
                } else if s1 == s2 {
                    // Same scale, different precision
                    0.9
                } else {
                    // Different scale - less compatible
                    0.7
                }
            }

            // === Temporal types - rarely FKs ===
            (Date, Date) => 1.0,
            (Time, Time) => 1.0,
            (Timestamp, Timestamp) => 1.0,
            (TimestampTz, TimestampTz) => 1.0,
            (Timestamp, TimestampTz) => 0.8,
            (TimestampTz, Timestamp) => 0.8,

            // === Boolean - almost never a FK ===
            (Bool, Bool) => 1.0,

            // === Binary ===
            (Binary, Binary) => 1.0,

            // === JSON - rarely a FK ===
            (Json, Json) => 1.0,

            // === Incompatible combinations ===
            // Different categories with no sensible conversion
            (Bool, _) | (_, Bool) => 0.0,
            (Date | Time | Timestamp | TimestampTz, Int8 | Int16 | Int32 | Int64) => 0.0,
            (Int8 | Int16 | Int32 | Int64, Date | Time | Timestamp | TimestampTz) => 0.0,
            (Binary, _) | (_, Binary) => 0.0,
            (Json, _) | (_, Json) => 0.0,

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
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    }

    fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::String | DataType::Varchar(_) | DataType::Char(_)
        )
    }

    fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::TimestampTz
        )
    }

    fn is_uuid(&self) -> bool {
        matches!(self, DataType::Uuid)
    }

    fn integer_bits(&self) -> Option<u8> {
        match self {
            DataType::Int8 => Some(8),
            DataType::Int16 => Some(16),
            DataType::Int32 => Some(32),
            DataType::Int64 => Some(64),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let result = TypeCompatibility::check(&DataType::Int64, &DataType::Int64);
        assert!(result.is_exact);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_integer_promotion() {
        // Int32 to Int64 should be good
        let result = TypeCompatibility::check(&DataType::Int32, &DataType::Int64);
        assert!(result.is_compatible);
        assert!(result.score >= 0.9);

        // Int64 to Int32 is less ideal but still compatible
        let result = TypeCompatibility::check(&DataType::Int64, &DataType::Int32);
        assert!(result.is_compatible);
        assert!(result.score >= 0.5);
    }

    #[test]
    fn test_string_compatibility() {
        let result = TypeCompatibility::check(&DataType::String, &DataType::Varchar(255));
        assert!(result.is_compatible);
        assert!(result.score >= 0.9);

        let result = TypeCompatibility::check(&DataType::Varchar(100), &DataType::Varchar(255));
        assert!(result.is_compatible);
    }

    #[test]
    fn test_uuid_string_compat() {
        let result = TypeCompatibility::check(&DataType::Uuid, &DataType::String);
        assert!(result.is_compatible);
        assert!(result.score >= 0.7);
    }

    #[test]
    fn test_incompatible_types() {
        // Bool to Int - incompatible
        let result = TypeCompatibility::check(&DataType::Bool, &DataType::Int64);
        assert!(!result.is_compatible);
        assert_eq!(result.score, 0.0);

        // Timestamp to Int - incompatible
        let result = TypeCompatibility::check(&DataType::Timestamp, &DataType::Int64);
        assert!(!result.is_compatible);
    }

    #[test]
    fn test_decimal_compatibility() {
        // Same precision and scale
        let result = TypeCompatibility::check(&DataType::Decimal(10, 2), &DataType::Decimal(10, 2));
        assert!(result.is_exact);

        // Different precision, same scale
        let result = TypeCompatibility::check(&DataType::Decimal(10, 2), &DataType::Decimal(18, 2));
        assert!(result.is_compatible);
        assert!(result.score >= 0.9);

        // Different scale
        let result = TypeCompatibility::check(&DataType::Decimal(10, 2), &DataType::Decimal(10, 4));
        assert!(result.is_compatible);
        assert!(result.score >= 0.7);
    }

    #[test]
    fn test_signal_generation() {
        let compat = TypeCompatibility::check(&DataType::Int64, &DataType::Int64);
        let signal = compat.to_signal();
        assert!(signal.is_positive());
        assert_eq!(signal.score, 1.0);

        let compat = TypeCompatibility::check(&DataType::Bool, &DataType::Int64);
        let signal = compat.to_signal();
        assert!(signal.is_negative());
    }

    #[test]
    fn test_datatype_ext() {
        assert!(DataType::Int32.is_integer());
        assert!(DataType::String.is_string());
        assert!(DataType::Float64.is_float());
        assert!(DataType::Timestamp.is_temporal());
        assert!(DataType::Uuid.is_uuid());

        assert_eq!(DataType::Int32.integer_bits(), Some(32));
        assert_eq!(DataType::String.integer_bits(), None);
    }
}
