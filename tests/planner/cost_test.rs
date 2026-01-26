use mantis::planner::cost::CostEstimator;
use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};
use mantis::semantic::graph::UnifiedGraph;

#[test]
fn test_select_best_plan_from_single() {
    let graph = UnifiedGraph::new();
    let plan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1000),
    };

    let estimator = CostEstimator::new(&graph);
    let best = estimator.select_best(vec![plan.clone()]).unwrap();

    assert_eq!(best, plan);
}

#[test]
fn test_select_best_prefers_smaller_estimated_rows() {
    let graph = UnifiedGraph::new();

    let plan1 = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1000),
    };

    let plan2 = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(100),
    };

    let estimator = CostEstimator::new(&graph);
    // Put plan2 first to ensure we're testing cost-based selection, not just pop()
    let best = estimator.select_best(vec![plan2.clone(), plan1]).unwrap();

    assert_eq!(best, plan2);
}
