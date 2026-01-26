// src/model/measure.rs
use crate::model::expr::Expr;
use crate::model::types::NullHandling;
use std::collections::HashMap;

/// A measure block for a table.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureBlock {
    pub table_name: String,
    pub measures: HashMap<String, Measure>,
}

/// A measure definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    pub name: String,
    /// SQL expression (parsed AST with @atom references)
    pub expr: Expr,
    /// Optional filter condition
    pub filter: Option<Expr>,
    /// Optional NULL handling override
    pub null_handling: Option<NullHandling>,
}
