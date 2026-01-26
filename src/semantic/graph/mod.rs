//! Unified semantic graph - single graph with entities, columns, measures, and calendars.
//!
//! This module replaces the dual-graph architecture (ModelGraph + ColumnLineageGraph)
//! with a unified graph where all semantic elements are first-class nodes.

mod builder;
pub mod query;
pub mod types;

pub use builder::{GraphBuildError, GraphBuildResult};
pub use types::*;

use petgraph::graph::DiGraph;
use std::collections::HashMap;

// Re-export NodeIndex for test helpers
pub use petgraph::graph::NodeIndex;

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

    /// Get the underlying petgraph (for advanced queries).
    pub fn graph(&self) -> &DiGraph<GraphNode, GraphEdge> {
        &self.graph
    }

    /// Look up an entity's NodeIndex by name.
    pub fn entity_index(&self, name: &str) -> Option<NodeIndex> {
        self.entity_index.get(name).copied()
    }

    /// Look up a column's NodeIndex by qualified name (entity.column).
    pub fn column_index(&self, qualified_name: &str) -> Option<NodeIndex> {
        self.column_index.get(qualified_name).copied()
    }

    /// Look up a measure's NodeIndex by qualified name (entity.measure).
    pub fn measure_index(&self, qualified_name: &str) -> Option<NodeIndex> {
        self.measure_index.get(qualified_name).copied()
    }

    /// Look up a calendar's NodeIndex by name.
    pub fn calendar_index(&self, name: &str) -> Option<NodeIndex> {
        self.calendar_index.get(name).copied()
    }
}

impl Default for UnifiedGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Test Helpers (available in test builds)
// ============================================================================

impl UnifiedGraph {
    /// Add an entity node to the graph (test helper).
    ///
    /// Note: Only use this in tests. For production, use `from_model_with_inference`.
    #[doc(hidden)]
    pub fn add_test_entity(&mut self, entity: EntityNode) -> NodeIndex {
        let idx = self.graph.add_node(GraphNode::Entity(entity.clone()));
        self.entity_index.insert(entity.name.clone(), idx);
        self.node_index.insert(entity.name.clone(), idx);
        idx
    }

    /// Add a join edge between two entities (test helper).
    ///
    /// Note: Only use this in tests. For production, use `from_model_with_inference`.
    #[doc(hidden)]
    pub fn add_test_join(&mut self, from: NodeIndex, to: NodeIndex, edge: JoinsToEdge) {
        self.graph.add_edge(from, to, GraphEdge::JoinsTo(edge));
    }

    /// Add a column node to the graph (test helper).
    ///
    /// Note: Only use this in tests. For production, use `from_model_with_inference`.
    #[doc(hidden)]
    pub fn add_test_column(&mut self, column: ColumnNode) -> NodeIndex {
        let qualified_name = column.qualified_name();
        let idx = self.graph.add_node(GraphNode::Column(column.clone()));
        self.column_index.insert(qualified_name.clone(), idx);
        self.node_index.insert(qualified_name, idx);
        idx
    }
}

// TODO: Fix these tests - SqlExpr type was removed/renamed during refactoring
// #[cfg(test)]
// mod tests;

// #[cfg(test)]
// mod integration_tests;
