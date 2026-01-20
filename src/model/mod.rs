//! DSL-first semantic model types.

pub mod calendar;
pub mod measure;
pub mod table;
pub mod types;

pub use calendar::{Calendar, CalendarBody, DrillPath, PhysicalCalendar};
pub use measure::{Measure, MeasureBlock};
pub use table::{Atom, Slicer, SqlExpr, Table, TimeBinding};
pub use types::{AtomType, DataType, GrainLevel, Month, NullHandling, Weekday};

use std::collections::HashMap;

/// The new DSL-first semantic model.
#[derive(Debug, Clone)]
pub struct Model {
    /// Model-wide defaults
    pub defaults: Option<Defaults>,

    /// Calendars (physical and generated)
    pub calendars: HashMap<String, calendar::Calendar>,

    /// Dimensions (optional rich dimensions with drill paths)
    pub dimensions: HashMap<String, Dimension>,

    /// Tables (universal: sources, facts, wide tables, CSVs)
    pub tables: HashMap<String, table::Table>,

    /// Measure blocks (one per table)
    pub measures: HashMap<String, measure::MeasureBlock>,

    /// Reports
    pub reports: HashMap<String, Report>,
}

// Placeholder types - will implement in later tasks
#[derive(Debug, Clone)]
pub struct Defaults;

#[derive(Debug, Clone)]
pub struct Dimension;

#[derive(Debug, Clone)]
pub struct Report;
