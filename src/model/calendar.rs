// src/model/calendar.rs
use crate::model::types::{GrainLevel, Month, Weekday};
use std::collections::HashMap;

/// A calendar (physical dimension table or generated).
#[derive(Debug, Clone, PartialEq)]
pub struct Calendar {
    pub name: String,
    pub body: CalendarBody,
}

impl Calendar {
    /// Check if calendar supports a given grain level.
    pub fn supports_grain(&self, grain: GrainLevel) -> bool {
        match &self.body {
            CalendarBody::Physical(cal) => cal.grain_mappings.contains_key(&grain),
            CalendarBody::Generated {
                grain: gen_grain, ..
            } => grain == *gen_grain,
        }
    }

    /// Get the column name for a given grain level.
    pub fn grain_column(&self, grain: GrainLevel) -> Result<&str, String> {
        match &self.body {
            CalendarBody::Physical(cal) => cal
                .grain_mappings
                .get(&grain)
                .map(|s| s.as_str())
                .ok_or_else(|| format!("Calendar {} does not support grain {}", self.name, grain)),
            CalendarBody::Generated {
                grain: gen_grain, ..
            } => {
                if grain == *gen_grain {
                    Ok("generated_date") // Convention for generated calendars
                } else {
                    Err(format!(
                        "Generated calendar {} only supports grain {}",
                        self.name, gen_grain
                    ))
                }
            }
        }
    }
}

/// Calendar body (physical or generated).
#[derive(Debug, Clone, PartialEq)]
pub enum CalendarBody {
    Physical(PhysicalCalendar),
    Generated {
        grain: GrainLevel,
        from: String, // Start date expression
        to: String,   // End date expression
    },
}

/// Physical calendar (dimension table).
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCalendar {
    /// Source table name
    pub source: String,
    /// Grain level to column name mappings
    pub grain_mappings: HashMap<GrainLevel, String>,
    /// Drill paths
    pub drill_paths: HashMap<String, DrillPath>,
    /// Fiscal year start month
    pub fiscal_year_start: Option<Month>,
    /// Week start day
    pub week_start: Option<Weekday>,
}

/// A drill path through grain levels.
#[derive(Debug, Clone, PartialEq)]
pub struct DrillPath {
    pub name: String,
    pub levels: Vec<GrainLevel>,
}
