//! Tests for TableScan cost estimation using actual row counts

use mantis::planner::cost::CostEstimator;
use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};
use mantis::semantic::graph::{EntityNode, EntityType, SizeCategory, UnifiedGraph};

#[test]
fn test_table_scan_uses_actual_row_count() {
    // Create graph with entity that has specific row count
    let mut graph = UnifiedGraph::new();
    let entity = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(50000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    graph.add_test_entity(entity);

    let plan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&plan);

    // Should use actual row count from graph
    assert_eq!(cost.rows_out, 50000);
    // FullScan IO cost should equal row count
    assert_eq!(cost.io_cost, 50000.0);
    // CPU cost should equal row count (scanning each row)
    assert_eq!(cost.cpu_cost, 50000.0);
}

#[test]
fn test_table_scan_full_scan_vs_index_scan() {
    let mut graph = UnifiedGraph::new();
    let entity = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(10000),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    graph.add_test_entity(entity);

    let full_scan = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let index_scan = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::IndexScan {
            index: "idx_customer_id".to_string(),
        },
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let full_cost = estimator.estimate(&full_scan);
    let index_cost = estimator.estimate(&index_scan);

    // Both should have same rows_out
    assert_eq!(full_cost.rows_out, 10000);
    assert_eq!(index_cost.rows_out, 10000);

    // IndexScan should have lower IO cost (10% of rows)
    assert_eq!(full_cost.io_cost, 10000.0);
    assert_eq!(index_cost.io_cost, 1000.0); // 10% of rows

    // IndexScan total cost should be lower
    assert!(index_cost.total() < full_cost.total());
}

#[test]
fn test_table_scan_fallback_when_no_row_count() {
    let graph = UnifiedGraph::new(); // Empty graph, no row count

    let plan = PhysicalPlan::TableScan {
        table: "unknown_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&plan);

    // Should use default fallback (1 million rows)
    assert_eq!(cost.rows_out, 1_000_000);
    assert_eq!(cost.io_cost, 1_000_000.0);
}

#[test]
fn test_table_scan_cost_varies_with_table_size() {
    let mut graph = UnifiedGraph::new();

    // Small table
    let small_entity = EntityNode {
        name: "small_table".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    graph.add_test_entity(small_entity);

    // Large table
    let large_entity = EntityNode {
        name: "large_table".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    graph.add_test_entity(large_entity);

    let small_plan = PhysicalPlan::TableScan {
        table: "small_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let large_plan = PhysicalPlan::TableScan {
        table: "large_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let small_cost = estimator.estimate(&small_plan);
    let large_cost = estimator.estimate(&large_plan);

    // Large table should have much higher cost
    assert!(large_cost.total() > small_cost.total() * 1000.0);
    assert_eq!(small_cost.rows_out, 100);
    assert_eq!(large_cost.rows_out, 10_000_000);
}
