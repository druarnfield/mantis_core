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

    #[error("Invalid expression for {measure}: {reason}")]
    InvalidExpression { measure: String, reason: String },
}

/// Result type for query operations.
pub type QueryResult<T> = Result<T, QueryError>;

/// Reference to a specific column in an entity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub entity: String,
    pub column: String,
}

impl ColumnRef {
    /// Create a new column reference.
    pub fn new(entity: String, column: String) -> Self {
        Self { entity, column }
    }

    /// Get the qualified name (entity.column).
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.entity, self.column)
    }
}

/// Join strategy hint for query optimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinHint {
    /// Use hash join with this side as build (smaller table)
    HashJoinBuild,
    /// Use hash join with this side as probe (larger table)
    HashJoinProbe,
    /// Consider nested loop join
    NestedLoop,
    /// Use merge join if data is sorted
    MergeJoin,
}

/// Join strategy recommendation for a single step.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinStrategyStep {
    /// Join step from path
    pub step: JoinStep,
    /// Recommended hint for left side
    pub left_hint: JoinHint,
    /// Recommended hint for right side
    pub right_hint: JoinHint,
    /// Explanation of recommendation
    pub reason: String,
}

/// Complete join strategy for a path.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinStrategy {
    /// Strategy steps matching the join path
    pub steps: Vec<JoinStrategyStep>,
}

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

    // ========================================================================
    // Column-Level Query Methods
    // ========================================================================

    /// Find all columns required by a measure.
    ///
    /// Traverses DEPENDS_ON edges from the measure to find all columns it references.
    ///
    /// # Example
    /// ```ignore
    /// let columns = graph.required_columns("sales.total_amount")?;
    /// assert!(columns.contains(&ColumnRef::new("sales".to_string(), "amount".to_string())));
    /// ```
    pub fn required_columns(&self, measure_id: &str) -> QueryResult<Vec<ColumnRef>> {
        let measure_idx = self
            .measure_index
            .get(measure_id)
            .ok_or_else(|| QueryError::MeasureNotFound(measure_id.to_string()))?;

        let mut columns = Vec::new();
        let mut visited = HashSet::new();

        // BFS to find all DEPENDS_ON edges
        let mut queue = VecDeque::new();
        queue.push_back(*measure_idx);
        visited.insert(*measure_idx);

        while let Some(current) = queue.pop_front() {
            for edge in self.graph.edges(current) {
                if let GraphEdge::DependsOn(_) = edge.weight() {
                    let target = edge.target();
                    if !visited.contains(&target) {
                        visited.insert(target);

                        // Extract column information
                        if let Some(GraphNode::Column(col)) = self.graph.node_weight(target) {
                            columns.push(ColumnRef::new(col.entity.clone(), col.name.clone()));
                        }
                    }
                }
            }
        }

        Ok(columns)
    }

    /// Trace column lineage (upstream dependencies).
    ///
    /// Traverses DERIVED_FROM edges backward to find all columns that
    /// this column was derived from.
    ///
    /// # Example
    /// ```ignore
    /// let lineage = graph.column_lineage("sales.total_price")?;
    /// // Returns columns like sales.unit_price, sales.quantity
    /// ```
    pub fn column_lineage(&self, column_id: &str) -> QueryResult<Vec<ColumnRef>> {
        let column_idx = self
            .column_index
            .get(column_id)
            .ok_or_else(|| QueryError::ColumnNotFound(column_id.to_string()))?;

        let mut lineage = Vec::new();
        let mut visited = HashSet::new();

        // BFS to find all DERIVED_FROM edges
        let mut queue = VecDeque::new();
        queue.push_back(*column_idx);
        visited.insert(*column_idx);

        while let Some(current) = queue.pop_front() {
            for edge in self.graph.edges(current) {
                if let GraphEdge::DerivedFrom(_) = edge.weight() {
                    let target = edge.target();
                    if !visited.contains(&target) {
                        visited.insert(target);
                        queue.push_back(target);

                        // Extract column information
                        if let Some(GraphNode::Column(col)) = self.graph.node_weight(target) {
                            lineage.push(ColumnRef::new(col.entity.clone(), col.name.clone()));
                        }
                    }
                }
            }
        }

        Ok(lineage)
    }

    /// Check if a column is unique (has unique constraint or is primary key).
    ///
    /// # Example
    /// ```ignore
    /// let is_unique = graph.is_column_unique("customers.customer_id")?;
    /// assert!(is_unique); // Primary key is unique
    /// ```
    pub fn is_column_unique(&self, column_id: &str) -> QueryResult<bool> {
        let column_idx = self
            .column_index
            .get(column_id)
            .ok_or_else(|| QueryError::ColumnNotFound(column_id.to_string()))?;

        if let Some(GraphNode::Column(col)) = self.graph.node_weight(*column_idx) {
            Ok(col.unique || col.primary_key)
        } else {
            Err(QueryError::ColumnNotFound(column_id.to_string()))
        }
    }

    /// Check if a column has high cardinality (many distinct values).
    ///
    /// Uses metadata to determine if the column is high cardinality.
    /// Returns true if marked as high cardinality in metadata.
    ///
    /// # Example
    /// ```ignore
    /// let is_high_card = graph.is_high_cardinality("sales.transaction_id")?;
    /// ```
    pub fn is_high_cardinality(&self, column_id: &str) -> QueryResult<bool> {
        let column_idx = self
            .column_index
            .get(column_id)
            .ok_or_else(|| QueryError::ColumnNotFound(column_id.to_string()))?;

        if let Some(GraphNode::Column(col)) = self.graph.node_weight(*column_idx) {
            // Check metadata for cardinality marker
            Ok(col
                .metadata
                .get("cardinality")
                .map(|v| v == "high")
                .unwrap_or(false))
        } else {
            Err(QueryError::ColumnNotFound(column_id.to_string()))
        }
    }

    // ========================================================================
    // Hybrid Query Methods
    // ========================================================================

    /// Find a join path and the required columns for a measure.
    ///
    /// This is a hybrid query that combines entity-level path finding with
    /// column-level dependency resolution in one go.
    ///
    /// # Example
    /// ```ignore
    /// let (path, columns) = graph.find_path_with_required_columns(
    ///     "sales",
    ///     "customers",
    ///     "sales.total_amount"
    /// )?;
    /// ```
    pub fn find_path_with_required_columns(
        &self,
        from: &str,
        to: &str,
        measure_id: &str,
    ) -> QueryResult<(JoinPath, Vec<ColumnRef>)> {
        let path = self.find_path(from, to)?;
        let columns = self.required_columns(measure_id)?;
        Ok((path, columns))
    }

    /// Recommend join strategy based on entity size categories.
    ///
    /// Uses size metadata to recommend hash join build/probe sides:
    /// - Small tables should be build side (fits in memory)
    /// - Large tables should be probe side (stream through)
    ///
    /// # Example
    /// ```ignore
    /// let path = graph.find_path("sales", "customers")?;
    /// let strategy = graph.find_best_join_strategy(&path)?;
    /// ```
    pub fn find_best_join_strategy(&self, path: &JoinPath) -> QueryResult<JoinStrategy> {
        let mut steps = Vec::new();

        for join_step in &path.steps {
            let from_size = self.get_entity_size(&join_step.from)?;
            let to_size = self.get_entity_size(&join_step.to)?;

            let (left_hint, right_hint, reason) = match (from_size, to_size) {
                (super::SizeCategory::Small, super::SizeCategory::Large) => (
                    JoinHint::HashJoinBuild,
                    JoinHint::HashJoinProbe,
                    format!(
                        "{} is small (build side), {} is large (probe side)",
                        join_step.from, join_step.to
                    ),
                ),
                (super::SizeCategory::Large, super::SizeCategory::Small) => (
                    JoinHint::HashJoinProbe,
                    JoinHint::HashJoinBuild,
                    format!(
                        "{} is large (probe side), {} is small (build side)",
                        join_step.from, join_step.to
                    ),
                ),
                (super::SizeCategory::Small, super::SizeCategory::Small) => (
                    JoinHint::NestedLoop,
                    JoinHint::NestedLoop,
                    format!(
                        "Both {} and {} are small, nested loop is fine",
                        join_step.from, join_step.to
                    ),
                ),
                (super::SizeCategory::Large, super::SizeCategory::Large) => (
                    JoinHint::HashJoinBuild,
                    JoinHint::HashJoinProbe,
                    format!(
                        "Both {} and {} are large, use hash join with left as build",
                        join_step.from, join_step.to
                    ),
                ),
                (super::SizeCategory::Medium, super::SizeCategory::Small) => (
                    JoinHint::HashJoinProbe,
                    JoinHint::HashJoinBuild,
                    format!(
                        "{} is medium (probe side), {} is small (build side)",
                        join_step.from, join_step.to
                    ),
                ),
                (super::SizeCategory::Small, super::SizeCategory::Medium) => (
                    JoinHint::HashJoinBuild,
                    JoinHint::HashJoinProbe,
                    format!(
                        "{} is small (build side), {} is medium (probe side)",
                        join_step.from, join_step.to
                    ),
                ),
                _ => (
                    JoinHint::HashJoinBuild,
                    JoinHint::HashJoinProbe,
                    format!(
                        "Default hash join strategy for {} to {}",
                        join_step.from, join_step.to
                    ),
                ),
            };

            steps.push(JoinStrategyStep {
                step: join_step.clone(),
                left_hint,
                right_hint,
                reason,
            });
        }

        Ok(JoinStrategy { steps })
    }

    /// Determine if aggregation should happen before or after join.
    ///
    /// Compares entity sizes to recommend if pre-aggregation would reduce
    /// data volume before joining.
    ///
    /// Returns true if aggregating the measure's entity before joining
    /// to the target entity would be beneficial.
    ///
    /// # Example
    /// ```ignore
    /// // Should we aggregate sales before joining to customers?
    /// let should_pre_agg = graph.should_aggregate_before_join(
    ///     "sales.total_amount",
    ///     "customers"
    /// )?;
    /// ```
    pub fn should_aggregate_before_join(
        &self,
        measure_id: &str,
        target_entity: &str,
    ) -> QueryResult<bool> {
        // Get measure's entity
        let measure_idx = self
            .measure_index
            .get(measure_id)
            .ok_or_else(|| QueryError::MeasureNotFound(measure_id.to_string()))?;

        let measure_entity =
            if let Some(GraphNode::Measure(m)) = self.graph.node_weight(*measure_idx) {
                &m.entity
            } else {
                return Err(QueryError::MeasureNotFound(measure_id.to_string()));
            };

        // Get sizes
        let measure_entity_size = self.get_entity_size(measure_entity)?;
        let target_size = self.get_entity_size(target_entity)?;

        // Pre-aggregate if measure's entity is much larger than target
        // This reduces rows before join
        Ok(matches!(
            (measure_entity_size, target_size),
            (super::SizeCategory::Large, super::SizeCategory::Small)
                | (super::SizeCategory::Large, super::SizeCategory::Medium)
                | (super::SizeCategory::Medium, super::SizeCategory::Small)
        ))
    }

    /// Get the size category for an entity.
    ///
    /// Helper method to extract size category from entity node.
    fn get_entity_size(&self, entity_name: &str) -> QueryResult<super::SizeCategory> {
        let entity_idx = self
            .entity_index
            .get(entity_name)
            .ok_or_else(|| QueryError::EntityNotFound(entity_name.to_string()))?;

        if let Some(GraphNode::Entity(entity)) = self.graph.node_weight(*entity_idx) {
            Ok(entity.size_category)
        } else {
            Err(QueryError::EntityNotFound(entity_name.to_string()))
        }
    }
}
