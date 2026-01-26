//! Join order optimization for multi-table queries.

use crate::planner::logical::LogicalPlan;
use crate::semantic::graph::UnifiedGraph;
use std::collections::HashSet;

/// Join order optimizer that reorders joins for better performance.
pub struct JoinOrderOptimizer<'a> {
    graph: &'a UnifiedGraph,
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
