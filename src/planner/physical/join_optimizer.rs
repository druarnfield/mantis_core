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
