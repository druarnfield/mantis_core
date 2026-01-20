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
}
