//! Integration tests for model Defaults.
//!
//! These tests verify the Defaults type and its functionality.

use mantis::dsl::ast::{Month, NullHandling, Weekday};
use mantis::model::Defaults;

#[test]
fn test_defaults() {
    let defaults = Defaults {
        calendar: Some("dates".to_string()),
        fiscal_year_start: Some(Month::April),
        week_start: Some(Weekday::Monday),
        null_handling: NullHandling::CoalesceZero,
        decimal_places: 2,
    };

    assert_eq!(defaults.calendar, Some("dates".to_string()));
    assert_eq!(defaults.fiscal_year_start, Some(Month::April));
    assert_eq!(defaults.week_start, Some(Weekday::Monday));
    assert_eq!(defaults.null_handling, NullHandling::CoalesceZero);
    assert_eq!(defaults.decimal_places, 2);
}

#[test]
fn test_defaults_default() {
    let defaults = Defaults::default();

    assert_eq!(defaults.calendar, None);
    assert_eq!(defaults.fiscal_year_start, None);
    assert_eq!(defaults.week_start, None);
    assert_eq!(defaults.null_handling, NullHandling::NullOnZero);
    assert_eq!(defaults.decimal_places, 2);
}
