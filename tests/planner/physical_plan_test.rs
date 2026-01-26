use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};

#[test]
fn test_table_scan_node_creation() {
    let scan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    assert!(matches!(scan, PhysicalPlan::TableScan { .. }));
}

#[test]
fn test_hash_join_node_creation() {
    // Test will be implemented in later tasks
}

#[test]
fn test_convert_scan_to_physical() {
    use mantis::planner::logical::{LogicalPlan, ScanNode};
    use mantis::planner::physical::PhysicalPlanner;
    use mantis::semantic::graph::UnifiedGraph;

    let graph = UnifiedGraph::new();
    let logical = LogicalPlan::Scan(ScanNode {
        entity: "sales".to_string(),
    });

    let planner = PhysicalPlanner::new(&graph);
    let candidates = planner.generate_candidates(&logical).unwrap();

    assert!(!candidates.is_empty());
    assert!(matches!(candidates[0], PhysicalPlan::TableScan { .. }));
}
