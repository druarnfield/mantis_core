use super::types::*;
use super::UnifiedGraph;
use crate::semantic::types::{ColumnRef, JoinPath, JoinStep};
use petgraph::Direction;
use std::collections::{HashMap, HashSet, VecDeque};

/// Query methods for the unified graph
impl UnifiedGraph {
    // ============================================================================
    // Entity-Level Queries (backward compatibility with ModelGraph)
    // ============================================================================

    /// Find the shortest join path between two entities
    pub fn find_path(&self, from: &str, to: &str) -> Result<JoinPath, GraphError> {
        let from_idx = self
            .entities
            .get(from)
            .ok_or_else(|| GraphError::EntityNotFound(from.to_string()))?;
        let to_idx = self
            .entities
            .get(to)
            .ok_or_else(|| GraphError::EntityNotFound(to.to_string()))?;

        // BFS to find shortest path
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent: HashMap<NodeIndex, (NodeIndex, EdgeIndex)> = HashMap::new();

        queue.push_back(*from_idx);
        visited.insert(*from_idx);

        while let Some(current_idx) = queue.pop_front() {
            if current_idx == *to_idx {
                // Reconstruct path
                return self.reconstruct_join_path(*from_idx, *to_idx, &parent);
            }

            // Explore JOINS_TO edges
            for edge_ref in self.graph.edges_directed(current_idx, Direction::Outgoing) {
                if let GraphEdge::JoinsTo(_) = edge_ref.weight() {
                    let next_idx = edge_ref.target();
                    if !visited.contains(&next_idx) {
                        visited.insert(next_idx);
                        parent.insert(next_idx, (current_idx, edge_ref.id()));
                        queue.push_back(next_idx);
                    }
                }
            }
        }

        Err(GraphError::NoPathFound {
            from: from.to_string(),
            to: to.to_string(),
        })
    }

    /// Validate that a path is safe (no fan-out without aggregation)
    pub fn validate_safe_path(&self, from: &str, to: &str) -> Result<JoinPath, GraphError> {
        let path = self.find_path(from, to)?;

        // Check each step for fan-out (many-to-one relationships going backwards)
        for step in &path.steps {
            if let Some(edge_idx) = self.graph.edge_indices().find(|&e| {
                let edge = &self.graph[e];
                matches!(edge, GraphEdge::JoinsTo(j) if
                        j.from_column_id == step.from_column &&
                        j.to_column_id == step.to_column)
            }) {
                if let GraphEdge::JoinsTo(join_edge) = &self.graph[edge_idx] {
                    // Check if this is a many-to-one going in reverse (dangerous fan-out)
                    match (&join_edge.cardinality, step.direction.as_str()) {
                        (Cardinality::ManyToOne, "outgoing") => {
                            // Safe: many-to-one in forward direction
                        }
                        (Cardinality::ManyToOne, "incoming") => {
                            return Err(GraphError::UnsafePath {
                                from: from.to_string(),
                                to: to.to_string(),
                                reason: format!(
                                    "Fan-out at {} -> {}",
                                    step.from_entity, step.to_entity
                                ),
                            });
                        }
                        (Cardinality::OneToMany, "outgoing") => {
                            return Err(GraphError::UnsafePath {
                                from: from.to_string(),
                                to: to.to_string(),
                                reason: format!(
                                    "Fan-out at {} -> {}",
                                    step.from_entity, step.to_entity
                                ),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(path)
    }

    /// Infer the grain (finest entity) from a set of entities
    pub fn infer_grain(&self, entities: &[String]) -> Option<String> {
        if entities.is_empty() {
            return None;
        }

        // Find the entity with the highest estimated row count (most granular)
        let mut max_rows = 0u64;
        let mut grain_entity = None;

        for entity_name in entities {
            if let Some(&entity_idx) = self.entities.get(entity_name) {
                if let GraphNode::Entity(entity_node) = &self.graph[entity_idx] {
                    if let Some(rows) = entity_node.estimated_rows {
                        if rows > max_rows {
                            max_rows = rows;
                            grain_entity = Some(entity_name.clone());
                        }
                    }
                }
            }
        }

        grain_entity
    }

    // ============================================================================
    // Column-Level Queries (new capabilities)
    // ============================================================================

    /// Get all columns required to compute a measure
    pub fn required_columns(&self, measure_id: &str) -> Result<HashSet<ColumnRef>, GraphError> {
        let measure_idx = self
            .measures
            .get(measure_id)
            .ok_or_else(|| GraphError::MeasureNotFound(measure_id.to_string()))?;

        let mut required = HashSet::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(*measure_idx);
        visited.insert(*measure_idx);

        while let Some(current_idx) = queue.pop_front() {
            // Follow DEPENDS_ON edges to columns
            for edge_ref in self.graph.edges_directed(current_idx, Direction::Outgoing) {
                if matches!(edge_ref.weight(), GraphEdge::DependsOn(_)) {
                    let target_idx = edge_ref.target();

                    if let GraphNode::Column(col_node) = &self.graph[target_idx] {
                        required.insert(ColumnRef {
                            entity: col_node.entity_id.clone(),
                            column: col_node.name.clone(),
                        });
                    }

                    if !visited.contains(&target_idx) {
                        visited.insert(target_idx);
                        queue.push_back(target_idx);
                    }
                }
            }
        }

        Ok(required)
    }

    /// Get the lineage (dependencies) of a column
    pub fn column_lineage(&self, column_id: &str) -> Result<Vec<ColumnRef>, GraphError> {
        let column_idx = self
            .columns
            .get(column_id)
            .ok_or_else(|| GraphError::ColumnNotFound(column_id.to_string()))?;

        let mut lineage = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(*column_idx);
        visited.insert(*column_idx);

        while let Some(current_idx) = queue.pop_front() {
            // Follow DERIVED_FROM edges
            for edge_ref in self.graph.edges_directed(current_idx, Direction::Outgoing) {
                if matches!(edge_ref.weight(), GraphEdge::DerivedFrom(_)) {
                    let target_idx = edge_ref.target();

                    if let GraphNode::Column(col_node) = &self.graph[target_idx] {
                        lineage.push(ColumnRef {
                            entity: col_node.entity_id.clone(),
                            column: col_node.name.clone(),
                        });
                    }

                    if !visited.contains(&target_idx) {
                        visited.insert(target_idx);
                        queue.push_back(target_idx);
                    }
                }
            }
        }

        Ok(lineage)
    }

    /// Check if a column is unique
    pub fn is_column_unique(&self, column_id: &str) -> Result<bool, GraphError> {
        let column_idx = self
            .columns
            .get(column_id)
            .ok_or_else(|| GraphError::ColumnNotFound(column_id.to_string()))?;

        if let GraphNode::Column(col_node) = &self.graph[*column_idx] {
            Ok(col_node.is_unique)
        } else {
            Err(GraphError::InvalidNodeType {
                expected: "Column".to_string(),
                actual: "Other".to_string(),
            })
        }
    }

    /// Check if a column has high cardinality
    pub fn is_high_cardinality(&self, column_id: &str) -> Result<bool, GraphError> {
        let column_idx = self
            .columns
            .get(column_id)
            .ok_or_else(|| GraphError::ColumnNotFound(column_id.to_string()))?;

        if let GraphNode::Column(col_node) = &self.graph[*column_idx] {
            Ok(col_node.high_cardinality)
        } else {
            Err(GraphError::InvalidNodeType {
                expected: "Column".to_string(),
                actual: "Other".to_string(),
            })
        }
    }

    // ============================================================================
    // Hybrid Queries (the power of unified graph!)
    // ============================================================================

    /// Find join path AND required columns in one traversal
    pub fn find_path_with_required_columns(
        &self,
        from: &str,
        to: &str,
        measure_id: &str,
    ) -> Result<(JoinPath, HashSet<ColumnRef>), GraphError> {
        let path = self.find_path(from, to)?;
        let columns = self.required_columns(measure_id)?;
        Ok((path, columns))
    }

    /// Determine the best join strategy based on table sizes and cardinality
    pub fn find_best_join_strategy(&self, path: &JoinPath) -> JoinStrategy {
        let mut strategy = JoinStrategy { steps: Vec::new() };

        for step in &path.steps {
            let from_size = self.get_entity_size(&step.from_entity);
            let to_size = self.get_entity_size(&step.to_entity);

            let join_type = match (from_size, to_size) {
                // Small table first in hash joins
                (
                    Some(SizeCategory::Small) | Some(SizeCategory::Tiny),
                    Some(SizeCategory::Large) | Some(SizeCategory::Huge),
                ) => JoinType::HashJoin { build_left: true },
                // Large table first in hash joins
                (
                    Some(SizeCategory::Large) | Some(SizeCategory::Huge),
                    Some(SizeCategory::Small) | Some(SizeCategory::Tiny),
                ) => JoinType::HashJoin { build_left: false },
                // Similar sizes - let DB choose
                _ => JoinType::Auto,
            };

            strategy.steps.push(JoinStrategyStep {
                from_entity: step.from_entity.clone(),
                to_entity: step.to_entity.clone(),
                join_type,
            });
        }

        strategy
    }

    /// Determine if aggregation should happen before join
    pub fn should_aggregate_before_join(
        &self,
        measure_id: &str,
        entity: &str,
    ) -> Result<bool, GraphError> {
        // Get measure's grain
        let measure_idx = self
            .measures
            .get(measure_id)
            .ok_or_else(|| GraphError::MeasureNotFound(measure_id.to_string()))?;

        if let GraphNode::Measure(measure_node) = &self.graph[*measure_idx] {
            // If measure's grain is finer than the entity we're joining to,
            // we should aggregate first
            let measure_grain_size = self.get_entity_size(&measure_node.grain_entity);
            let target_entity_size = self.get_entity_size(entity);

            match (measure_grain_size, target_entity_size) {
                (Some(grain_size), Some(target_size)) => {
                    // If grain entity is larger (more rows), aggregate first
                    Ok(grain_size as u8 > target_size as u8)
                }
                _ => Ok(false), // Default to no pre-aggregation if sizes unknown
            }
        } else {
            Err(GraphError::InvalidNodeType {
                expected: "Measure".to_string(),
                actual: "Other".to_string(),
            })
        }
    }

    /// Find all measures that can be computed from a given entity
    pub fn available_measures(&self, entity: &str) -> Result<Vec<String>, GraphError> {
        let entity_idx = self
            .entities
            .get(entity)
            .ok_or_else(|| GraphError::EntityNotFound(entity.to_string()))?;

        let mut measures = Vec::new();

        // Find all columns belonging to this entity
        let entity_columns: HashSet<NodeIndex> = self
            .graph
            .edges_directed(*entity_idx, Direction::Incoming)
            .filter(|e| matches!(e.weight(), GraphEdge::BelongsTo(_)))
            .map(|e| e.source())
            .collect();

        // Find measures that depend only on these columns
        for (&measure_id_str, &measure_idx) in &self.measures {
            let required = self.required_columns(measure_id_str)?;

            // Check if all required columns belong to this entity
            let all_available = required.iter().all(|col_ref| col_ref.entity == entity);

            if all_available {
                if let GraphNode::Measure(measure_node) = &self.graph[measure_idx] {
                    measures.push(measure_node.name.clone());
                }
            }
        }

        Ok(measures)
    }

    // ============================================================================
    // Helper Methods
    // ============================================================================

    fn reconstruct_join_path(
        &self,
        from_idx: NodeIndex,
        to_idx: NodeIndex,
        parent: &HashMap<NodeIndex, (NodeIndex, EdgeIndex)>,
    ) -> Result<JoinPath, GraphError> {
        let mut steps = Vec::new();
        let mut current_idx = to_idx;

        while current_idx != from_idx {
            let (parent_idx, edge_idx) = parent
                .get(&current_idx)
                .ok_or_else(|| GraphError::PathReconstructionFailed)?;

            if let GraphEdge::JoinsTo(join_edge) = &self.graph[*edge_idx] {
                let from_entity = if let GraphNode::Entity(e) = &self.graph[*parent_idx] {
                    e.name.clone()
                } else {
                    return Err(GraphError::InvalidNodeType {
                        expected: "Entity".to_string(),
                        actual: "Other".to_string(),
                    });
                };

                let to_entity = if let GraphNode::Entity(e) = &self.graph[current_idx] {
                    e.name.clone()
                } else {
                    return Err(GraphError::InvalidNodeType {
                        expected: "Entity".to_string(),
                        actual: "Other".to_string(),
                    });
                };

                steps.push(JoinStep {
                    from_entity,
                    to_entity,
                    from_column: join_edge.from_column_id.clone(),
                    to_column: join_edge.to_column_id.clone(),
                    cardinality: format!("{:?}", join_edge.cardinality),
                    direction: "outgoing".to_string(),
                });
            }

            current_idx = *parent_idx;
        }

        steps.reverse();

        Ok(JoinPath { steps })
    }

    fn get_entity_size(&self, entity_name: &str) -> Option<SizeCategory> {
        self.entities.get(entity_name).and_then(|&idx| {
            if let GraphNode::Entity(entity_node) = &self.graph[idx] {
                entity_node.size_category
            } else {
                None
            }
        })
    }
}

// ============================================================================
// Supporting Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct JoinStrategy {
    pub steps: Vec<JoinStrategyStep>,
}

#[derive(Debug, Clone)]
pub struct JoinStrategyStep {
    pub from_entity: String,
    pub to_entity: String,
    pub join_type: JoinType,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    HashJoin { build_left: bool },
    Auto,
}

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Measure not found: {0}")]
    MeasureNotFound(String),

    #[error("No path found from {from} to {to}")]
    NoPathFound { from: String, to: String },

    #[error("Unsafe path from {from} to {to}: {reason}")]
    UnsafePath {
        from: String,
        to: String,
        reason: String,
    },

    #[error("Failed to reconstruct path")]
    PathReconstructionFailed,

    #[error("Invalid node type: expected {expected}, got {actual}")]
    InvalidNodeType { expected: String, actual: String },
}

// Re-export NodeIndex and EdgeIndex from petgraph
use petgraph::graph::{EdgeIndex, NodeIndex};
