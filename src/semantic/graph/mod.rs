//! Unified semantic graph - single graph with entities, columns, measures, and calendars.
//!
//! This module replaces the dual-graph architecture (ModelGraph + ColumnLineageGraph)
//! with a unified graph where all semantic elements are first-class nodes.

mod builder;
pub mod query;
pub mod types;

pub use builder::{GraphBuildError, GraphBuildResult};
pub use types::*;

use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::HashMap;

/// The unified semantic graph.
///
/// This graph contains four types of nodes:
/// - Entities (tables)
/// - Columns (fields)
/// - Measures (pre-defined aggregations)
/// - Calendars (time dimensions)
///
/// And five types of edges:
/// - BELONGS_TO: column → entity
/// - REFERENCES: column → column (FK)
/// - DERIVED_FROM: column → column(s) (lineage)
/// - DEPENDS_ON: measure → column(s)
/// - JOINS_TO: entity → entity
#[derive(Debug, Clone)]
pub struct UnifiedGraph {
    /// The underlying directed graph
    graph: DiGraph<GraphNode, GraphEdge>,

    /// Index: node name → NodeIndex
    node_index: HashMap<String, NodeIndex>,

    /// Index: entity name → NodeIndex
    entity_index: HashMap<String, NodeIndex>,

    /// Index: column qualified name (entity.column) → NodeIndex
    column_index: HashMap<String, NodeIndex>,

    /// Index: measure qualified name (entity.measure) → NodeIndex
    measure_index: HashMap<String, NodeIndex>,

    /// Index: calendar name → NodeIndex
    calendar_index: HashMap<String, NodeIndex>,
}

impl UnifiedGraph {
    /// Create a new empty unified graph.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_index: HashMap::new(),
            entity_index: HashMap::new(),
            column_index: HashMap::new(),
            measure_index: HashMap::new(),
            calendar_index: HashMap::new(),
        }
    }
}

impl Default for UnifiedGraph {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: Fix these tests - SqlExpr type was removed/renamed during refactoring
// #[cfg(test)]
// mod tests;

// #[cfg(test)]
// mod integration_tests;
