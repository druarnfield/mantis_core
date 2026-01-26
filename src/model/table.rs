// src/model/table.rs
use crate::model::expr::Expr;
use crate::model::types::{AtomType, DataType, GrainLevel};
use std::collections::HashMap;

/// A table (universal: CSV, wide table, fact, etc.).
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    /// Source table/file
    pub source: String,
    /// Atoms (numeric columns for aggregation)
    pub atoms: HashMap<String, Atom>,
    /// Times (date columns bound to calendars)
    pub times: HashMap<String, TimeBinding>,
    /// Slicers (columns for slicing/grouping)
    pub slicers: HashMap<String, Slicer>,
}

/// An atom (numeric column for aggregation).
#[derive(Debug, Clone, PartialEq)]
pub struct Atom {
    pub name: String,
    pub data_type: AtomType,
}

/// A time binding (date column bound to a calendar).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeBinding {
    pub name: String,
    /// Calendar name
    pub calendar: String,
    /// Grain level
    pub grain: GrainLevel,
}

/// A slicer (dimension column).
#[derive(Debug, Clone, PartialEq)]
pub enum Slicer {
    /// Inline slicer (column in the table)
    Inline { name: String, data_type: DataType },
    /// Foreign key to a dimension
    ForeignKey {
        name: String,
        dimension: String,
        key: String,
    },
    /// Via another slicer (inherit relationship)
    Via { name: String, fk_slicer: String },
    /// Calculated slicer (SQL expression)
    Calculated {
        name: String,
        data_type: DataType,
        expr: Expr,
    },
}
