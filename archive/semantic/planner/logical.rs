//! Phase 3: Logical Planning
//!
//! This phase converts a validated query into a logical plan tree.
//! The logical plan represents operations without committing to
//! physical execution details.

use crate::semantic::error::PlanResult;
use crate::semantic::model_graph::{JoinEdge, ModelGraph};

use super::resolved::{
    ResolvedColumn, ResolvedEntity, ResolvedFilter, ResolvedMeasure, ResolvedOrder, ResolvedSelect,
};
use super::validate::ValidatedQuery;

/// A logical plan - tree of logical operations.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a single table.
    Scan(ScanNode),

    /// Join two plans.
    Join(JoinNode),

    /// Filter rows.
    Filter(FilterNode),

    /// Aggregate (GROUP BY).
    Aggregate(AggregateNode),

    /// Project columns (SELECT).
    Project(ProjectNode),

    /// Sort rows (ORDER BY).
    Sort(SortNode),

    /// Limit rows.
    Limit(LimitNode),
}

/// Scan a single entity/table.
#[derive(Debug, Clone)]
pub struct ScanNode {
    /// The entity being scanned.
    pub entity: ResolvedEntity,
}

/// Join two plans.
#[derive(Debug, Clone)]
pub struct JoinNode {
    /// Left input.
    pub left: Box<LogicalPlan>,

    /// Right input.
    pub right: Box<LogicalPlan>,

    /// Join condition (from_entity, from_col, to_entity, to_col).
    pub on: JoinCondition,

    /// Join type.
    pub join_type: LogicalJoinType,
}

/// Join condition.
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_entity: String,
    pub left_column: String,
    pub right_entity: String,
    pub right_column: String,
}

/// Logical join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalJoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Filter node.
#[derive(Debug, Clone)]
pub struct FilterNode {
    /// Input plan.
    pub input: Box<LogicalPlan>,

    /// Filter conditions.
    pub predicates: Vec<ResolvedFilter>,
}

/// Aggregate node (GROUP BY).
#[derive(Debug, Clone)]
pub struct AggregateNode {
    /// Input plan.
    pub input: Box<LogicalPlan>,

    /// GROUP BY columns.
    pub group_by: Vec<ResolvedColumn>,

    /// Aggregate expressions (measures).
    pub aggregates: Vec<ResolvedMeasure>,
}

/// Project node (SELECT).
#[derive(Debug, Clone)]
pub struct ProjectNode {
    /// Input plan.
    pub input: Box<LogicalPlan>,

    /// Columns to project.
    pub projections: Vec<ResolvedSelect>,
}

/// Sort node (ORDER BY).
#[derive(Debug, Clone)]
pub struct SortNode {
    /// Input plan.
    pub input: Box<LogicalPlan>,

    /// Sort expressions.
    pub order_by: Vec<ResolvedOrder>,
}

/// Limit node.
#[derive(Debug, Clone)]
pub struct LimitNode {
    /// Input plan.
    pub input: Box<LogicalPlan>,

    /// Maximum rows to return.
    pub limit: u64,
}

/// Logical planner - handles Phase 3 of query planning.
pub struct LogicalPlanner<'a> {
    /// Reference to the model graph for resolving virtual entities.
    graph: Option<&'a ModelGraph>,
}

impl<'a> LogicalPlanner<'a> {
    /// Create a new logical planner.
    pub fn new() -> Self {
        Self { graph: None }
    }

    /// Create a logical planner with access to the model graph.
    ///
    /// This enables virtual fact reconstruction - when a fact has
    /// `materialized = false`, the planner will expand it into
    /// a join plan from its source entities.
    pub fn with_graph(graph: &'a ModelGraph) -> Self {
        Self { graph: Some(graph) }
    }

    /// Build a logical plan from a validated query.
    pub fn plan(&self, validated: &ValidatedQuery) -> PlanResult<LogicalPlan> {
        let query = &validated.query;
        let join_tree = &validated.join_tree;

        // Check if the from entity is a virtual (unmaterialized) fact
        let mut plan = if !query.from.materialized {
            self.build_virtual_fact_plan(&query.from, validated)?
        } else {
            // Start with the base scan for materialized entities
            LogicalPlan::Scan(ScanNode {
                entity: query.from.clone(),
            })
        };

        // Add joins
        plan = self.add_joins(plan, &join_tree.edges, validated)?;

        // Add filters (WHERE)
        if !query.filters.is_empty() {
            plan = LogicalPlan::Filter(FilterNode {
                input: Box::new(plan),
                predicates: query.filters.clone(),
            });
        }

        // Collect aggregates from select and order_by
        let aggregates = self.collect_aggregates(query);

        // Check if there are any inline aggregates in SELECT
        let has_inline_aggregates = query.select.iter().any(|s| {
            matches!(s, ResolvedSelect::Aggregate { .. })
        });

        // Add aggregation (GROUP BY)
        if !query.group_by.is_empty() || !aggregates.is_empty() || has_inline_aggregates {
            plan = LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(plan),
                group_by: query.group_by.clone(),
                aggregates,
            });
        }

        // Add projection (SELECT)
        plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(plan),
            projections: self.build_projections(query),
        });

        // Add sort (ORDER BY)
        if !query.order_by.is_empty() {
            plan = LogicalPlan::Sort(SortNode {
                input: Box::new(plan),
                order_by: query.order_by.clone(),
            });
        }

        // Add limit
        if let Some(limit) = query.limit {
            plan = LogicalPlan::Limit(LimitNode {
                input: Box::new(plan),
                limit,
            });
        }

        Ok(plan)
    }

    /// Build a plan for a virtual (unmaterialized) fact.
    ///
    /// Instead of scanning the fact's target table, we reconstruct the query
    /// from the fact's source entities by:
    /// 1. Starting with a scan of the grain source entity
    /// 2. Adding joins for any included dimensions
    ///
    /// The column pruning API (if enabled) will optimize which columns are selected.
    fn build_virtual_fact_plan(
        &self,
        fact_entity: &ResolvedEntity,
        _validated: &ValidatedQuery,
    ) -> PlanResult<LogicalPlan> {
        use crate::semantic::SemanticError;

        let graph = self.graph.ok_or_else(|| {
            SemanticError::QueryPlanError(format!(
                "Cannot query virtual fact '{}': model graph not available. \
                 Use LogicalPlanner::with_graph() to enable virtual fact support.",
                fact_entity.name
            ))
        })?;

        // Look up the fact definition
        let fact = graph.get_fact(&fact_entity.name).ok_or_else(|| {
            SemanticError::QueryPlanError(format!(
                "Virtual entity '{}' is not a fact - only virtual facts are supported",
                fact_entity.name
            ))
        })?;

        // Get the primary grain source entity
        // The grain defines the base entity from which the fact is built
        let grain_entity = if let Some(ref from) = fact.from {
            // Use explicit 'from' if provided (could be an intermediate)
            from.clone()
        } else if let Some(first_grain) = fact.grain.first() {
            // Otherwise use the first grain entity
            first_grain.source_entity.clone()
        } else {
            return Err(SemanticError::QueryPlanError(format!(
                "Virtual fact '{}' has no grain or from clause - cannot determine source entity",
                fact_entity.name
            )));
        };

        // Get entity info for the grain source
        let grain_info = graph.get_entity_info(&grain_entity)?;
        let grain_resolved = ResolvedEntity {
            name: grain_info.name.clone(),
            physical_table: grain_info.physical_table.clone(),
            physical_schema: grain_info.physical_schema.clone(),
            materialized: grain_info.materialized,
        };

        // Start with a scan of the grain source
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: grain_resolved.clone(),
        });

        // Add joins for included dimensions
        for (dim_alias, include) in &fact.includes {
            // Get the entity info for the included dimension
            let dim_info = graph.get_entity_info(&include.entity)?;
            let dim_resolved = ResolvedEntity {
                name: dim_alias.clone(), // Use the alias from includes
                physical_table: dim_info.physical_table.clone(),
                physical_schema: dim_info.physical_schema.clone(),
                materialized: dim_info.materialized,
            };

            // Find the relationship between grain entity and included dimension
            if let Some((rel, reversed)) = graph.find_relationship_either_direction(&grain_entity, &include.entity) {
                let (left_col, right_col) = if reversed {
                    (rel.to_column.clone(), rel.from_column.clone())
                } else {
                    (rel.from_column.clone(), rel.to_column.clone())
                };

                let right = LogicalPlan::Scan(ScanNode {
                    entity: dim_resolved,
                });

                plan = LogicalPlan::Join(JoinNode {
                    left: Box::new(plan),
                    right: Box::new(right),
                    on: JoinCondition {
                        left_entity: grain_entity.clone(),
                        left_column: left_col,
                        right_entity: dim_alias.clone(),
                        right_column: right_col,
                    },
                    join_type: LogicalJoinType::Inner,
                });
            }
            // If no direct relationship, skip the join (validated should catch this)
        }

        Ok(plan)
    }

    /// Add joins to the plan.
    fn add_joins(
        &self,
        mut plan: LogicalPlan,
        edges: &[JoinEdge],
        validated: &ValidatedQuery,
    ) -> PlanResult<LogicalPlan> {
        for edge in edges {
            // Look up the target entity's physical info from the entity_info map
            let mut target_entity = validated
                .entity_info
                .get(&edge.to_entity)
                .cloned()
                .unwrap_or_else(|| ResolvedEntity {
                    name: edge.to_entity.clone(),
                    physical_table: edge.to_entity.clone(),
                    physical_schema: None,
                    materialized: true, // Assume materialized if not found
                });

            // If target is a virtual (unmaterialized) dimension, use its source entity
            if !target_entity.materialized {
                if let Some(graph) = self.graph {
                    if let Some(dim) = graph.get_dimension(&edge.to_entity) {
                        // Get the source entity's physical info
                        if let Ok(source_info) = graph.get_entity_info(&dim.source_entity) {
                            target_entity = ResolvedEntity {
                                name: edge.to_entity.clone(), // Keep the alias as dimension name
                                physical_table: source_info.physical_table.clone(),
                                physical_schema: source_info.physical_schema.clone(),
                                materialized: true, // Source is always materialized
                            };
                        }
                    }
                }
            }

            let right = LogicalPlan::Scan(ScanNode {
                entity: target_entity,
            });

            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(right),
                on: JoinCondition {
                    left_entity: edge.from_entity.clone(),
                    left_column: edge.from_column.clone(),
                    right_entity: edge.to_entity.clone(),
                    right_column: edge.to_column.clone(),
                },
                join_type: LogicalJoinType::Inner,
            });
        }

        Ok(plan)
    }

    /// Collect all aggregates (measures) from the query.
    fn collect_aggregates(
        &self,
        query: &super::resolved::ResolvedQuery,
    ) -> Vec<ResolvedMeasure> {
        let mut aggregates = Vec::new();

        for select in &query.select {
            if let ResolvedSelect::Measure { measure, .. } = select {
                if !aggregates.iter().any(|m: &ResolvedMeasure| m.name == measure.name) {
                    aggregates.push(measure.clone());
                }
            }
        }

        for order in &query.order_by {
            if let super::resolved::ResolvedOrderExpr::Measure(measure) = &order.expr {
                if !aggregates.iter().any(|m: &ResolvedMeasure| m.name == measure.name) {
                    aggregates.push(measure.clone());
                }
            }
        }

        aggregates
    }

    /// Build the projection list (SELECT columns + GROUP BY columns).
    fn build_projections(
        &self,
        query: &super::resolved::ResolvedQuery,
    ) -> Vec<ResolvedSelect> {
        let mut projections = Vec::new();

        // First, add GROUP BY columns (they're implicitly selected)
        for col in &query.group_by {
            // Check if already in select
            let already_selected = query.select.iter().any(|s| match s {
                ResolvedSelect::Column { column, .. } => {
                    column.entity_alias == col.entity_alias
                        && column.physical_name == col.physical_name
                }
                _ => false,
            });

            if !already_selected {
                projections.push(ResolvedSelect::Column {
                    column: col.clone(),
                    alias: None,
                });
            }
        }

        // Then add explicit SELECT items
        projections.extend(query.select.clone());

        projections
    }
}

impl Default for LogicalPlanner<'_> {
    fn default() -> Self {
        Self::new()
    }
}
