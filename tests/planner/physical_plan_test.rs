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
