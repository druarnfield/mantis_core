// src/model/dimension.rs
use crate::model::types::DataType;
use std::collections::HashMap;

/// A dimension drill path defining an attribute hierarchy.
#[derive(Debug, Clone, PartialEq)]
pub struct DimensionDrillPath {
    pub name: String,
    /// Ordered attribute names from fine to coarse (e.g., ["city", "state", "country"])
    pub levels: Vec<String>,
}

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
    pub drill_paths: HashMap<String, DimensionDrillPath>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
}
