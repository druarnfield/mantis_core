//! Cost estimation for physical plans.

use crate::planner::physical::{PhysicalPlan, TableScanStrategy};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

/// Multi-objective cost estimate with component breakdown.
#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimate {
    /// Estimated number of output rows
    pub rows_out: usize,
    /// CPU cost component
    pub cpu_cost: f64,
    /// I/O cost component (weighted higher)
    pub io_cost: f64,
    /// Memory cost component (weighted lower)
    pub memory_cost: f64,
}

impl CostEstimate {
    /// Calculate total weighted cost.
    ///
    /// Weights: CPU = 1.0, IO = 10.0 (IO is expensive), Memory = 0.1 (memory is cheap)
    pub fn total(&self) -> f64 {
        (self.cpu_cost * 1.0) + (self.io_cost * 10.0) + (self.memory_cost * 0.1)
    }
}

pub struct CostEstimator<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> CostEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    /// Estimate cost for a physical plan using UnifiedGraph metadata.
    ///
    /// Returns a CostEstimate with detailed breakdown of rows, CPU, IO, and memory costs.
    pub fn estimate(&self, plan: &PhysicalPlan) -> CostEstimate {
        match plan {
            PhysicalPlan::TableScan {
                table, strategy, ..
            } => {
                // Get actual row count from graph
                let row_count = self.get_entity_row_count(table);

                // Calculate IO cost based on scan strategy
                let io_cost = match strategy {
                    TableScanStrategy::FullScan => row_count as f64,
                    TableScanStrategy::IndexScan { .. } => (row_count as f64) * 0.1, // 10% of rows
                };

                CostEstimate {
                    rows_out: row_count,
                    cpu_cost: row_count as f64, // CPU cost for scanning each row
                    io_cost,
                    memory_cost: 0.0, // Table scan doesn't use memory
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                // Get input cost
                let input_cost = self.estimate(input);

                // Estimate filter selectivity
                let selectivity = self.estimate_filter_selectivity(predicate);

                // Calculate output rows after filtering
                let rows_out = ((input_cost.rows_out as f64) * selectivity) as usize;

                CostEstimate {
                    rows_out,
                    // CPU cost: input CPU + evaluating predicate for each input row
                    cpu_cost: input_cost.cpu_cost + (input_cost.rows_out as f64),
                    // IO cost unchanged (filter doesn't add IO)
                    io_cost: input_cost.io_cost,
                    // Memory cost unchanged
                    memory_cost: input_cost.memory_cost,
                }
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                // Estimate costs for both sides
                let left_cost = self.estimate(left);
                let right_cost = self.estimate(right);

                // Estimate join cardinality using graph metadata
                let rows_out = self.estimate_join_cardinality(left, right, &left_cost, &right_cost);

                CostEstimate {
                    rows_out,
                    // CPU cost: scan both sides + build hash table + probe
                    cpu_cost: left_cost.cpu_cost + right_cost.cpu_cost + (rows_out as f64),
                    // IO cost: read both sides
                    io_cost: left_cost.io_cost + right_cost.io_cost,
                    // Memory cost: smaller side for hash table
                    memory_cost: left_cost.rows_out.min(right_cost.rows_out) as f64,
                }
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // Estimate costs for both sides
                let left_cost = self.estimate(left);
                let right_cost = self.estimate(right);

                // Estimate join cardinality using graph metadata
                let rows_out = self.estimate_join_cardinality(left, right, &left_cost, &right_cost);

                CostEstimate {
                    rows_out,
                    // CPU cost: nested loop = left * right comparisons
                    cpu_cost: left_cost.cpu_cost
                        + (left_cost.rows_out as f64 * right_cost.rows_out as f64),
                    // IO cost: read both sides
                    io_cost: left_cost.io_cost + right_cost.io_cost,
                    // Memory cost: no hash table needed
                    memory_cost: 0.0,
                }
            }
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                // Estimate input cost
                let input_cost = self.estimate(input);

                // Estimate GROUP BY cardinality using graph metadata
                let rows_out = self.estimate_group_cardinality(&input_cost, group_by);

                CostEstimate {
                    rows_out,
                    // CPU cost: scan input + hash grouping
                    cpu_cost: input_cost.cpu_cost + (input_cost.rows_out as f64),
                    // IO cost unchanged
                    io_cost: input_cost.io_cost,
                    // Memory cost: hash table for groups
                    memory_cost: rows_out as f64,
                }
            }
            _ => {
                // For other plan types, use simple fallback for now
                CostEstimate {
                    rows_out: 1000,
                    cpu_cost: 1000.0,
                    io_cost: 1000.0,
                    memory_cost: 0.0,
                }
            }
        }
    }

    /// Get row count for an entity from the graph.
    ///
    /// Returns actual row count if available, otherwise fallback to 1 million.
    fn get_entity_row_count(&self, entity_name: &str) -> usize {
        use crate::semantic::graph::GraphNode;

        if let Some(idx) = self.graph.entity_index(entity_name) {
            if let Some(GraphNode::Entity(entity)) = self.graph.graph().node_weight(idx) {
                return entity.row_count.unwrap_or(1_000_000);
            }
        }

        // Fallback for unknown entities
        1_000_000
    }

    /// Estimate filter selectivity using graph metadata.
    ///
    /// Returns a value between 0.0 and 1.0 representing the fraction of rows
    /// that pass the filter.
    fn estimate_filter_selectivity(&self, predicate: &crate::model::expr::Expr) -> f64 {
        use crate::model::expr::{BinaryOp, Expr as ModelExpr};

        match predicate {
            ModelExpr::BinaryOp { op, left, right } => match op {
                // Equality: use column cardinality
                BinaryOp::Eq => {
                    if let Some(col_ref) = Self::extract_column(left) {
                        self.estimate_equality_selectivity(&col_ref)
                    } else if let Some(col_ref) = Self::extract_column(right) {
                        self.estimate_equality_selectivity(&col_ref)
                    } else {
                        0.1 // Default for unknown equality
                    }
                }
                // Range predicates
                BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte => 0.33,
                // Logical AND: multiply selectivities
                BinaryOp::And => {
                    let left_sel = self.estimate_filter_selectivity(left);
                    let right_sel = self.estimate_filter_selectivity(right);
                    left_sel * right_sel
                }
                // Logical OR: probability union
                BinaryOp::Or => {
                    let left_sel = self.estimate_filter_selectivity(left);
                    let right_sel = self.estimate_filter_selectivity(right);
                    left_sel + right_sel - (left_sel * right_sel)
                }
                // Other operators: default estimate
                _ => 0.5,
            },
            // Other expression types: default estimate
            _ => 0.5,
        }
    }

    /// Estimate selectivity for equality predicates based on column cardinality.
    fn estimate_equality_selectivity(&self, col_qualified: &str) -> f64 {
        // Check if column is high or low cardinality
        if let Ok(is_high_card) = self.graph.is_high_cardinality(col_qualified) {
            if is_high_card {
                0.001 // High cardinality: very selective (1 in 1000)
            } else {
                0.1 // Low cardinality: less selective (1 in 10)
            }
        } else {
            0.1 // Default for unknown columns
        }
    }

    /// Extract column reference from an expression.
    ///
    /// Returns the qualified name (entity.column) if the expression is a column reference.
    fn extract_column(expr: &crate::model::expr::Expr) -> Option<String> {
        use crate::model::expr::Expr as ModelExpr;

        match expr {
            ModelExpr::Column { entity, column } => {
                if let Some(e) = entity {
                    Some(format!("{}.{}", e, column))
                } else {
                    Some(column.clone())
                }
            }
            _ => None,
        }
    }

    /// Estimate join cardinality using relationship metadata from UnifiedGraph.
    ///
    /// Uses cardinality information from JOINS_TO edges to estimate output rows:
    /// - 1:1 → max(left, right)
    /// - 1:N → right (many side)
    /// - N:1 → left (many side)
    /// - N:N → (left * right) / 100 (reduced cross product)
    fn estimate_join_cardinality(
        &self,
        left_plan: &PhysicalPlan,
        right_plan: &PhysicalPlan,
        left_cost: &CostEstimate,
        right_cost: &CostEstimate,
    ) -> usize {
        // Extract table names from plans
        let left_table = self.extract_table_name(left_plan);
        let right_table = self.extract_table_name(right_plan);

        if let (Some(left_table), Some(right_table)) = (left_table, right_table) {
            // Try to find join path in graph
            if let Ok(path) = self.graph.find_path(&left_table, &right_table) {
                if let Some(step) = path.steps.first() {
                    // Parse cardinality and apply formula
                    return match step.cardinality.as_str() {
                        "1:1" => left_cost.rows_out.max(right_cost.rows_out),
                        "1:N" => right_cost.rows_out, // Many side
                        "N:1" => left_cost.rows_out,  // Many side
                        "N:N" => {
                            // Many-to-many: reduced cross product
                            ((left_cost.rows_out as f64 * right_cost.rows_out as f64) / 100.0)
                                as usize
                        }
                        _ => left_cost.rows_out.max(right_cost.rows_out), // Unknown: conservative
                    };
                }
            }
        }

        // Fallback: conservative estimate
        left_cost.rows_out.max(right_cost.rows_out)
    }

    /// Extract table name from a physical plan.
    ///
    /// Recursively searches for the base table in the plan tree.
    fn extract_table_name(&self, plan: &PhysicalPlan) -> Option<String> {
        match plan {
            PhysicalPlan::TableScan { table, .. } => Some(table.clone()),
            PhysicalPlan::Filter { input, .. } => self.extract_table_name(input),
            PhysicalPlan::HashJoin { left, .. } => self.extract_table_name(left),
            PhysicalPlan::NestedLoopJoin { left, .. } => self.extract_table_name(left),
            PhysicalPlan::HashAggregate { input, .. } => self.extract_table_name(input),
            PhysicalPlan::Project { input, .. } => self.extract_table_name(input),
            PhysicalPlan::Sort { input, .. } => self.extract_table_name(input),
            PhysicalPlan::Limit { input, .. } => self.extract_table_name(input),
        }
    }

    /// Estimate GROUP BY cardinality using column cardinality metadata.
    ///
    /// Uses column metadata to estimate distinct values:
    /// - High cardinality: 50% of input rows
    /// - Low cardinality: 10% of input rows
    /// - Multiple columns: product of individual selectivities
    fn estimate_group_cardinality(&self, input_cost: &CostEstimate, group_by: &[String]) -> usize {
        if group_by.is_empty() {
            // No grouping: single row output
            return 1;
        }

        let mut selectivity = 1.0;

        for col_qualified in group_by {
            // Check if column is high or low cardinality
            let col_selectivity =
                if let Ok(is_high_card) = self.graph.is_high_cardinality(col_qualified) {
                    if is_high_card {
                        0.5 // High cardinality: 50% of rows
                    } else {
                        0.1 // Low cardinality: 10% of rows
                    }
                } else {
                    0.3 // Default for unknown columns
                };

            // Multiple columns: multiply selectivities
            selectivity *= col_selectivity;
        }

        // Apply selectivity to input rows
        ((input_cost.rows_out as f64) * selectivity).max(1.0) as usize
    }

    pub fn select_best(&self, candidates: Vec<PhysicalPlan>) -> PlanResult<PhysicalPlan> {
        if candidates.is_empty() {
            return Err(PlanError::NoValidPlans);
        }

        // Use new multi-objective cost estimation
        let best = candidates
            .into_iter()
            .min_by_key(|plan| self.estimate(plan).total() as u64)
            .unwrap();

        Ok(best)
    }

    /// Legacy cost estimation method using simple heuristics.
    ///
    /// DEPRECATED: Use `estimate()` instead which provides multi-objective cost estimation
    /// with graph metadata. This method is kept for backwards compatibility but is no longer
    /// used in plan selection.
    #[allow(dead_code)]
    fn estimate_cost(&self, plan: &PhysicalPlan) -> u64 {
        match plan {
            PhysicalPlan::TableScan { estimated_rows, .. } => {
                estimated_rows.unwrap_or(u64::MAX as usize) as u64
            }
            PhysicalPlan::Filter { input, .. } => {
                // Assume filter reduces rows by 10%
                self.estimate_cost(input) / 10
            }
            PhysicalPlan::HashJoin { estimated_rows, .. } => {
                estimated_rows.unwrap_or(u64::MAX as usize) as u64
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // NLJ cost is roughly O(left * right)
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                left_cost.saturating_mul(right_cost)
            }
            PhysicalPlan::HashAggregate { input, .. } => {
                // Hash aggregate roughly same cost as input
                self.estimate_cost(input)
            }
            PhysicalPlan::Sort { input, .. } => {
                // Sort is O(n log n)
                let input_cost = self.estimate_cost(input);
                if input_cost > 0 {
                    let log_factor = (input_cost as f64).log2() as u64;
                    input_cost.saturating_mul(log_factor)
                } else {
                    0
                }
            }
            PhysicalPlan::Project { input, .. } => {
                // Project has same cost as input
                self.estimate_cost(input)
            }
            PhysicalPlan::Limit { input, limit } => {
                // Limit reduces cost
                std::cmp::min(self.estimate_cost(input), *limit as u64)
            }
        }
    }
}
