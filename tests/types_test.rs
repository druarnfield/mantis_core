//! Test DSL type re-exports in model module.

use mantis::model::{AtomType, DataType, GrainLevel, Month, NullHandling, Weekday};

#[test]
fn test_atom_type_reexport() {
    let _at = AtomType::Sum;
    assert!(true);
}

#[test]
fn test_data_type_reexport() {
    let _dt = DataType::Int64;
    assert!(true);
}

#[test]
fn test_grain_level_reexport() {
    let gl = GrainLevel::Day;
    assert!(!gl.is_fiscal());

    let fiscal = GrainLevel::FiscalYear;
    assert!(fiscal.is_fiscal());
}

#[test]
fn test_grain_level_from_str() {
    assert!(matches!(GrainLevel::from_str("day"), Some(GrainLevel::Day)));

    assert!(matches!(
        GrainLevel::from_str("fiscal_year"),
        Some(GrainLevel::FiscalYear)
    ));

    assert!(GrainLevel::from_str("invalid").is_none());
}

#[test]
fn test_month_reexport() {
    let _m = Month::January;
    assert!(true);
}

#[test]
fn test_weekday_reexport() {
    let _w = Weekday::Monday;
    assert!(true);
}

#[test]
fn test_null_handling_reexport() {
    let _nh = NullHandling::Ignore;
    assert!(true);
}
