// src/model/measure.rs
use crate::model::table::SqlExpr;
use crate::model::types::NullHandling;
use std::collections::HashMap;

/// A measure block for a table.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureBlock {
    pub table_name: String,
    pub measures: HashMap<String, Measure>,
}

/// A measure definition with @atom syntax preserved.
#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    pub name: String,
    /// SQL expression with @atom references preserved
    pub expr: SqlExpr,
    /// Optional filter condition
    pub filter: Option<SqlExpr>,
    /// Optional NULL handling override
    pub null_handling: Option<NullHandling>,
}
