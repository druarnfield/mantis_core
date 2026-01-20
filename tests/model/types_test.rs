//! Integration tests for model types.
//!
//! These tests verify the model types and their utility methods.

use mantis::model::types::GrainLevel;

#[test]
fn test_grain_level_is_fiscal() {
    assert!(GrainLevel::FiscalMonth.is_fiscal());
    assert!(GrainLevel::FiscalQuarter.is_fiscal());
    assert!(GrainLevel::FiscalYear.is_fiscal());
    assert!(!GrainLevel::Month.is_fiscal());
    assert!(!GrainLevel::Day.is_fiscal());
}

#[test]
fn test_grain_level_from_str() {
    assert_eq!(GrainLevel::from_str("day"), Some(GrainLevel::Day));
    assert_eq!(GrainLevel::from_str("month"), Some(GrainLevel::Month));
    assert_eq!(
        GrainLevel::from_str("fiscal_year"),
        Some(GrainLevel::FiscalYear)
    );
    assert_eq!(GrainLevel::from_str("invalid"), None);
}
