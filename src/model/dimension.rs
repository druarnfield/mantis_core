// src/model/dimension.rs
use crate::model::calendar::DrillPath;
use crate::model::types::DataType;
use std::collections::HashMap;

/// A dimension (optional - for rich dimensions with drill paths).
#[derive(Debug, Clone, PartialEq)]
pub struct Dimension {
    pub name: String,
    /// Source table
    pub source: String,
    /// Primary key column
    pub key: String,
    /// Attributes
    pub attributes: HashMap<String, Attribute>,
    /// Named drill paths
    pub drill_paths: HashMap<String, DrillPath>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
}
