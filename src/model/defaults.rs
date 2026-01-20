//! Model-wide defaults.

use crate::dsl::ast::{Month, NullHandling, Weekday};

/// Model-wide defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Defaults {
    /// Default calendar for time bindings
    pub calendar: Option<String>,
    /// Fiscal year start month
    pub fiscal_year_start: Option<Month>,
    /// First day of week
    pub week_start: Option<Weekday>,
    /// Division NULL handling
    pub null_handling: NullHandling,
    /// Default decimal precision
    pub decimal_places: u8,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            calendar: None,
            fiscal_year_start: None,
            week_start: None,
            null_handling: NullHandling::NullOnZero,
            decimal_places: 2,
        }
    }
}
