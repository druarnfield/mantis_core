//! Model type re-exports and utilities.

// Re-export primitive types from DSL
pub use crate::dsl::ast::{AtomType, DataType, GrainLevel, Month, NullHandling, Weekday};

// Model-specific GrainLevel extensions
impl GrainLevel {
    /// Check if this grain level is fiscal.
    pub fn is_fiscal(&self) -> bool {
        matches!(
            self,
            GrainLevel::FiscalMonth | GrainLevel::FiscalQuarter | GrainLevel::FiscalYear
        )
    }

    /// Parse grain level from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "minute" => Some(GrainLevel::Minute),
            "hour" => Some(GrainLevel::Hour),
            "day" => Some(GrainLevel::Day),
            "week" => Some(GrainLevel::Week),
            "month" => Some(GrainLevel::Month),
            "quarter" => Some(GrainLevel::Quarter),
            "year" => Some(GrainLevel::Year),
            "fiscal_month" => Some(GrainLevel::FiscalMonth),
            "fiscal_quarter" => Some(GrainLevel::FiscalQuarter),
            "fiscal_year" => Some(GrainLevel::FiscalYear),
            _ => None,
        }
    }
}
