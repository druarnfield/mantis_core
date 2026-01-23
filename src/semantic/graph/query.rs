//! Query interface for the unified semantic graph.
//!
//! This module provides methods to traverse the graph and answer questions:
//! - Entity-level queries: find join paths between tables
//! - Column-level queries: trace lineage and dependencies
//! - Measure queries: resolve measure dependencies
//! - Calendar queries: find time dimensions

use super::{GraphEdge, GraphNode, UnifiedGraph};
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use std::collections::{HashMap, HashSet, VecDeque};
use thiserror::Error;

/// Errors that can occur during graph queries.
#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Measure not found: {0}")]
    MeasureNotFound(String),

    #[error("Calendar not found: {0}")]
    CalendarNotFound(String),

    #[error("No path found from {from} to {to}")]
    NoPathFound { from: String, to: String },

    #[error("Unsafe join path from {from} to {to}: {reason}")]
    UnsafeJoinPath {
        from: String,
        to: String,
        reason: String,
    },
}

/// Result type for query operations.
pub type QueryResult<T> = Result<T, QueryError>;

/// A join path between two entities.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPath {
    /// The steps in the join path.
    pub steps: Vec<JoinStep>,
}

/// A single step in a join path.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinStep {
    /// The source entity.
    pub from: String,

    /// The target entity.
    pub to: String,

    /// The cardinality of the join.
    pub cardinality: String,
}

impl UnifiedGraph {
    /// Find the shortest join path between two entities.
    ///
    /// Uses BFS to find the shortest path through JOINS_TO edges.
    ///
    /// # Example
    /// ```ignore
    /// let path = graph.find_path("sales", "customers")?;
    /// assert_eq!(path.steps.len(), 1);
    /// ```
    pub fn find_path(&self, from: &str, to: &str) -> QueryResult<JoinPath> {
        // Look up entity nodes
        let from_idx = self
            .entity_index
            .get(from)
            .ok_or_else(|| QueryError::EntityNotFound(from.to_string()))?;

        let to_idx = self
            .entity_index
            .get(to)
            .ok_or_else(|| QueryError::EntityNotFound(to.to_string()))?;

        // BFS to find shortest path
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent: HashMap<NodeIndex, NodeIndex> = HashMap::new();

        queue.push_back(*from_idx);
        visited.insert(*from_idx);

        while let Some(current) = queue.pop_front() {
            if current == *to_idx {
                // Found target - reconstruct path
                return self.reconstruct_join_path(*from_idx, *to_idx, &parent);
            }

            // Explore neighbors via JOINS_TO edges
            for edge in self.graph.edges(current) {
                if matches!(edge.weight(), GraphEdge::JoinsTo(_)) {
                    let neighbor = edge.target();
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        parent.insert(neighbor, current);
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // No path found
        Err(QueryError::NoPathFound {
            from: from.to_string(),
            to: to.to_string(),
        })
    }

    /// Reconstruct a join path from the BFS parent map.
    fn reconstruct_join_path(
        &self,
        from_idx: NodeIndex,
        to_idx: NodeIndex,
        parent: &HashMap<NodeIndex, NodeIndex>,
    ) -> QueryResult<JoinPath> {
        let mut steps = Vec::new();
        let mut current = to_idx;

        // Walk backward from target to source
        while current != from_idx {
            let prev = parent
                .get(&current)
                .ok_or_else(|| QueryError::NoPathFound {
                    from: format!("{:?}", from_idx),
                    to: format!("{:?}", to_idx),
                })?;

            // Find the edge between prev and current
            let edge = self
                .graph
                .find_edge(*prev, current)
                .and_then(|edge_idx| self.graph.edge_weight(edge_idx))
                .ok_or_else(|| QueryError::NoPathFound {
                    from: format!("{:?}", from_idx),
                    to: format!("{:?}", to_idx),
                })?;

            // Extract entity names and cardinality
            if let (Some(GraphNode::Entity(from_entity)), Some(GraphNode::Entity(to_entity))) = (
                self.graph.node_weight(*prev),
                self.graph.node_weight(current),
            ) {
                let cardinality = match edge {
                    GraphEdge::JoinsTo(edge_data) => edge_data.cardinality.to_string(),
                    _ => "Unknown".to_string(),
                };

                steps.push(JoinStep {
                    from: from_entity.name.clone(),
                    to: to_entity.name.clone(),
                    cardinality,
                });
            }

            current = *prev;
        }

        // Reverse to get source → target order
        steps.reverse();

        Ok(JoinPath { steps })
    }

    /// Validate that a join path is safe (no dangerous fan-out).
    ///
    /// Returns an error if the path contains a OneToMany join that could
    /// cause row duplication.
    ///
    /// # Example
    /// ```ignore
    /// graph.validate_safe_path("customers", "orders")?; // Error: OneToMany
    /// graph.validate_safe_path("orders", "customers")?; // OK: ManyToOne
    /// ```
    pub fn validate_safe_path(&self, from: &str, to: &str) -> QueryResult<()> {
        let path = self.find_path(from, to)?;

        for step in &path.steps {
            if step.cardinality.contains("OneToMany") {
                return Err(QueryError::UnsafeJoinPath {
                    from: from.to_string(),
                    to: to.to_string(),
                    reason: format!(
                        "Join from {} to {} is OneToMany and may cause row duplication",
                        step.from, step.to
                    ),
                });
            }
        }

        Ok(())
    }

    /// Infer the grain (most granular entity) from a set of entities.
    ///
    /// The grain is the entity with the highest row count (most detailed).
    ///
    /// # Example
    /// ```ignore
    /// let grain = graph.infer_grain(&["sales", "customers", "products"])?;
    /// assert_eq!(grain, "sales"); // sales has most rows
    /// ```
    pub fn infer_grain(&self, entities: &[&str]) -> QueryResult<String> {
        let mut max_rows = 0;
        let mut grain = None;

        for entity in entities {
            let idx = self
                .entity_index
                .get(*entity)
                .ok_or_else(|| QueryError::EntityNotFound(entity.to_string()))?;

            if let Some(GraphNode::Entity(entity)) = self.graph.node_weight(*idx) {
                if let Some(rows) = entity.row_count {
                    if rows > max_rows {
                        max_rows = rows;
                        grain = Some(entity.name.clone());
                    }
                }
            }
        }

        grain
            .ok_or_else(|| QueryError::EntityNotFound("No entity with row count found".to_string()))
    }
}
