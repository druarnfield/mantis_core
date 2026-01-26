//! Join order optimization for multi-table queries.

use crate::planner::cost::CostEstimator;
use crate::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
use crate::planner::physical::PhysicalPlan;
use crate::semantic::graph::{Cardinality, UnifiedGraph};
use std::collections::HashSet;

/// Join order optimizer that reorders joins for better performance.
pub struct JoinOrderOptimizer<'a> {
    graph: &'a UnifiedGraph,
}

/// Information needed to construct a join.
struct JoinInfo {
    condition: JoinCondition,
    cardinality: Option<Cardinality>,
}

impl<'a> JoinOrderOptimizer<'a> {
    /// Create a new join order optimizer.
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    /// Extract all table names from a logical plan.
    ///
    /// Recursively walks the plan tree and collects all table names from Scan nodes.
    pub fn extract_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables(plan, &mut tables);
        tables
    }

    /// Enumerate all possible join orders for small joins (≤3 tables).
    ///
    /// For small number of tables, we can try all permutations to find the optimal join order.
    /// Returns a vector of optional LogicalPlan where each represents a different join order.
    /// Plans are sorted by estimated cost (lowest first).
    pub fn enumerate_join_orders(&self, plan: &LogicalPlan) -> Vec<Option<LogicalPlan>> {
        let tables = self.extract_tables(plan);
        let table_vec: Vec<String> = tables.into_iter().collect();

        // Single table - no reordering needed
        if table_vec.len() == 1 {
            return vec![Some(plan.clone())];
        }

        // Generate all permutations of tables
        let mut permutations = Vec::new();
        Self::generate_permutations(&table_vec, &mut permutations);

        // Build a logical plan for each permutation
        let mut candidates: Vec<Option<LogicalPlan>> = permutations
            .into_iter()
            .map(|perm| self.build_join_plan_for_order(&perm))
            .collect();

        // Sort by cost (using cost estimator if available)
        // For now, we just return all candidates
        // TODO: Sort by cost once we have physical plan conversion

        candidates
    }

    /// Generate all permutations of a vector.
    fn generate_permutations(items: &[String], result: &mut Vec<Vec<String>>) {
        if items.len() <= 1 {
            result.push(items.to_vec());
            return;
        }

        for i in 0..items.len() {
            let current = items[i].clone();
            let mut remaining = items.to_vec();
            remaining.remove(i);

            let mut sub_perms = Vec::new();
            Self::generate_permutations(&remaining, &mut sub_perms);

            for mut sub_perm in sub_perms {
                let mut perm = vec![current.clone()];
                perm.append(&mut sub_perm);
                result.push(perm);
            }
        }
    }

    /// Build a logical join plan for a specific table order.
    ///
    /// Takes a list of tables in order and builds left-deep join tree.
    fn build_join_plan_for_order(&self, tables: &[String]) -> Option<LogicalPlan> {
        if tables.is_empty() {
            return None;
        }

        if tables.len() == 1 {
            return Some(LogicalPlan::Scan(ScanNode {
                entity: tables[0].clone(),
            }));
        }

        // Start with first table
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: tables[0].clone(),
        });

        // Join remaining tables in order
        for right_table in &tables[1..] {
            // Get the leftmost table in current plan
            let left_table = Self::get_leftmost_table(&plan)?;

            // Find join path and condition
            let join_info = self.find_join_info(&left_table, right_table)?;

            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan(ScanNode {
                    entity: right_table.clone(),
                })),
                join_type: JoinType::Inner,
                on: join_info.condition,
                cardinality: join_info.cardinality,
            });
        }

        Some(plan)
    }

    /// Get the leftmost (first) table name from a plan tree.
    fn get_leftmost_table(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::Scan(scan) => Some(scan.entity.clone()),
            LogicalPlan::Join(join) => Self::get_leftmost_table(&join.left),
            _ => None,
        }
    }

    /// Find join information between two tables.
    fn find_join_info(&self, from: &str, to: &str) -> Option<JoinInfo> {
        use crate::semantic::graph::GraphEdge;

        // Look up the edge directly from the graph
        // Try from -> to
        if let Some(from_idx) = self.graph.entity_index(from) {
            if let Some(to_idx) = self.graph.entity_index(to) {
                // Check for edge from -> to
                if let Some(edge) = self.graph.graph().find_edge(from_idx, to_idx) {
                    if let Some(GraphEdge::JoinsTo(joins_to)) = self.graph.graph().edge_weight(edge)
                    {
                        let join_columns: Vec<_> = joins_to
                            .join_columns
                            .iter()
                            .map(|(from_col, to_col)| {
                                (
                                    crate::semantic::graph::query::ColumnRef::new(
                                        from.to_string(),
                                        from_col.clone(),
                                    ),
                                    crate::semantic::graph::query::ColumnRef::new(
                                        to.to_string(),
                                        to_col.clone(),
                                    ),
                                )
                            })
                            .collect();

                        return Some(JoinInfo {
                            condition: JoinCondition::Equi(join_columns),
                            cardinality: Some(joins_to.cardinality.clone()),
                        });
                    }
                }

                // Check for edge to -> from (reverse direction)
                if let Some(edge) = self.graph.graph().find_edge(to_idx, from_idx) {
                    if let Some(GraphEdge::JoinsTo(joins_to)) = self.graph.graph().edge_weight(edge)
                    {
                        // Reverse the join columns since we're going opposite direction
                        let join_columns: Vec<_> = joins_to
                            .join_columns
                            .iter()
                            .map(|(to_col, from_col)| {
                                (
                                    crate::semantic::graph::query::ColumnRef::new(
                                        from.to_string(),
                                        from_col.clone(),
                                    ),
                                    crate::semantic::graph::query::ColumnRef::new(
                                        to.to_string(),
                                        to_col.clone(),
                                    ),
                                )
                            })
                            .collect();

                        return Some(JoinInfo {
                            condition: JoinCondition::Equi(join_columns),
                            cardinality: Some(joins_to.cardinality.clone()),
                        });
                    }
                }
            }
        }

        None
    }

    /// Greedy join order optimization for large joins (>3 tables).
    ///
    /// Algorithm:
    /// 1. Find the smallest two-table join pair
    /// 2. Iteratively add the next-best table to the current plan
    /// 3. Return the final plan
    ///
    /// This is O(n²) complexity, much faster than n! enumeration.
    pub fn greedy_join_order(&self, tables: &[&str]) -> Option<LogicalPlan> {
        if tables.is_empty() {
            return None;
        }

        if tables.len() == 1 {
            return Some(LogicalPlan::Scan(ScanNode {
                entity: tables[0].to_string(),
            }));
        }

        // Convert to owned strings for easier manipulation
        let table_strings: Vec<String> = tables.iter().map(|s| s.to_string()).collect();

        // Step 1: Find smallest pair to start with
        let (t1, t2) = self.find_smallest_join_pair(&table_strings)?;

        let mut remaining: Vec<String> = table_strings
            .into_iter()
            .filter(|t| t != &t1 && t != &t2)
            .collect();

        // Build initial join
        let join_info = self.find_join_info(&t1, &t2)?;
        let mut current_plan = LogicalPlan::Join(JoinNode {
            left: Box::new(LogicalPlan::Scan(ScanNode { entity: t1.clone() })),
            right: Box::new(LogicalPlan::Scan(ScanNode { entity: t2.clone() })),
            join_type: JoinType::Inner,
            on: join_info.condition,
            cardinality: join_info.cardinality,
        });

        // Step 2: Iteratively add best next table
        while !remaining.is_empty() {
            let next_table = self.find_best_next_join(&current_plan, &remaining)?;

            // Remove from remaining
            remaining.retain(|t| t != &next_table);

            // Get a table from current plan to join with
            let current_table = Self::get_leftmost_table(&current_plan)?;
            let join_info = self.find_join_info(&current_table, &next_table)?;

            // Add to plan
            current_plan = LogicalPlan::Join(JoinNode {
                left: Box::new(current_plan),
                right: Box::new(LogicalPlan::Scan(ScanNode { entity: next_table })),
                join_type: JoinType::Inner,
                on: join_info.condition,
                cardinality: join_info.cardinality,
            });
        }

        Some(current_plan)
    }

    /// Find the pair of tables with the smallest join cost (Task 12).
    ///
    /// Tries all pairs, estimates cost, returns the pair with lowest cost.
    pub fn find_smallest_join_pair(&self, tables: &[String]) -> Option<(String, String)> {
        if tables.len() < 2 {
            return None;
        }

        let mut best_pair: Option<(String, String)> = None;
        let mut best_cost = f64::MAX;

        // Try all pairs
        for i in 0..tables.len() {
            for j in (i + 1)..tables.len() {
                let t1 = &tables[i];
                let t2 = &tables[j];

                // Check if this pair can be joined
                if self.find_join_info(t1, t2).is_some() {
                    // Estimate cost of joining these two tables
                    let cost = self.estimate_pair_cost(t1, t2);

                    if cost < best_cost {
                        best_cost = cost;
                        best_pair = Some((t1.clone(), t2.clone()));
                    }
                }
            }
        }

        best_pair
    }

    /// Find the best table to join next to the current plan (Task 13).
    ///
    /// Tries each remaining table, estimates cost of adding it, returns the best.
    pub fn find_best_next_join(
        &self,
        current_plan: &LogicalPlan,
        remaining_tables: &[String],
    ) -> Option<String> {
        if remaining_tables.is_empty() {
            return None;
        }

        let mut best_table: Option<String> = None;
        let mut best_cost = f64::MAX;

        // Get tables already in current plan
        let current_tables = self.extract_tables(current_plan);

        // Try each remaining table
        for table in remaining_tables {
            // Check if we can join this table to any table in current plan
            let can_join = current_tables.iter().any(|ct| {
                self.find_join_info(ct, table).is_some() || self.find_join_info(table, ct).is_some()
            });

            if can_join {
                // Estimate cost of adding this table
                let cost = self.estimate_add_cost(current_plan, table);

                if cost < best_cost {
                    best_cost = cost;
                    best_table = Some(table.clone());
                }
            }
        }

        best_table
    }

    /// Estimate cost of joining two tables.
    fn estimate_pair_cost(&self, t1: &str, t2: &str) -> f64 {
        let cost_estimator = CostEstimator::new(self.graph);

        // Create simple scans
        let plan1 = PhysicalPlan::TableScan {
            table: t1.to_string(),
            strategy: crate::planner::physical::TableScanStrategy::FullScan,
            estimated_rows: None,
        };

        let plan2 = PhysicalPlan::TableScan {
            table: t2.to_string(),
            strategy: crate::planner::physical::TableScanStrategy::FullScan,
            estimated_rows: None,
        };

        // Create join
        let join_plan = PhysicalPlan::HashJoin {
            left: Box::new(plan1),
            right: Box::new(plan2),
            on: vec![], // Simplified - actual join columns don't matter for cost
            estimated_rows: None,
        };

        let cost = cost_estimator.estimate(&join_plan);
        cost.total()
    }

    /// Estimate cost of adding a table to current plan.
    fn estimate_add_cost(&self, _current_plan: &LogicalPlan, table: &str) -> f64 {
        // Simplified: cost is based on table size
        // Smaller tables are cheaper to add
        if let Some(idx) = self.graph.entity_index(table) {
            if let Some(crate::semantic::graph::GraphNode::Entity(entity)) =
                self.graph.graph().node_weight(idx)
            {
                return entity.row_count.unwrap_or(1_000_000) as f64;
            }
        }

        1_000_000.0 // Default cost
    }

    /// Recursively collect table names from a logical plan.
    fn collect_tables(&self, plan: &LogicalPlan, tables: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Scan(scan) => {
                tables.insert(scan.entity.clone());
            }
            LogicalPlan::Join(join) => {
                self.collect_tables(&join.left, tables);
                self.collect_tables(&join.right, tables);
            }
            LogicalPlan::Filter(filter) => {
                self.collect_tables(&filter.input, tables);
            }
            LogicalPlan::Aggregate(agg) => {
                self.collect_tables(&agg.input, tables);
            }
            LogicalPlan::TimeMeasure(tm) => {
                self.collect_tables(&tm.input, tables);
            }
            LogicalPlan::DrillPath(dp) => {
                self.collect_tables(&dp.input, tables);
            }
            LogicalPlan::InlineMeasure(im) => {
                self.collect_tables(&im.input, tables);
            }
            LogicalPlan::Project(proj) => {
                self.collect_tables(&proj.input, tables);
            }
            LogicalPlan::Sort(sort) => {
                self.collect_tables(&sort.input, tables);
            }
            LogicalPlan::Limit(limit) => {
                self.collect_tables(&limit.input, tables);
            }
        }
    }
}
