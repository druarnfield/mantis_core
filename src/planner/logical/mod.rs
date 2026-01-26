//! Logical planning - converts Report to abstract operation tree.

mod builder;
mod plan;

pub use plan::*;

use crate::model::Report;
use crate::planner::PlanResult;
use crate::semantic::graph::UnifiedGraph;

/// Logical planner.
pub struct LogicalPlanner<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> LogicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn plan(&self, report: &Report) -> PlanResult<LogicalPlan> {
        builder::PlanBuilder::new(self.graph).build(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_node_creation() {
        let scan = LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        });

        assert!(matches!(scan, LogicalPlan::Scan(_)));
    }

    #[test]
    fn test_filter_node_creation() {
        use crate::model::expr::Expr;

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                entity: "sales".to_string(),
            })),
            predicates: vec![Expr::bool(true)],
        });

        assert!(matches!(filter, LogicalPlan::Filter(_)));
    }

    #[test]
    fn test_simple_report_to_logical_plan() {
        use crate::model::{Report, ShowItem};
        use crate::semantic::graph::UnifiedGraph;

        let graph = UnifiedGraph::new();

        let report = Report {
            name: "test".to_string(),
            from: vec!["sales".to_string()],
            use_date: vec![],
            period: None,
            group: vec![],
            show: vec![ShowItem::Measure {
                name: "revenue".to_string(),
                label: None,
            }],
            filters: vec![],
            sort: vec![],
            limit: None,
        };

        let planner = LogicalPlanner::new(&graph);
        let plan = planner.plan(&report).unwrap();

        // Should have Scan → Aggregate → Project structure
        assert!(matches!(plan, LogicalPlan::Project(_)));
    }
}
