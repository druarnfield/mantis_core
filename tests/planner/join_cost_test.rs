//! Tests for join cost estimation (Task 7)

use mantis::planner::cost::CostEstimator;
use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};

fn create_test_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Small dimension table
    let customers = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(1_000),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let customers_idx = graph.add_test_entity(customers);

    // Large fact table
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(100_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    // N:1 join (orders → customers)
    let join_edge = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "customers".to_string(),
        join_columns: vec![("customer_id".to_string(), "customer_id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, customers_idx, join_edge);

    graph
}

// Task 7: Join strategy cost estimation

#[test]
fn test_hash_join_has_memory_cost() {
    let graph = create_test_graph();

    let left = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let hash_join = PhysicalPlan::HashJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&hash_join);

    // HashJoin should have memory cost for smaller side (customers: 1000)
    assert_eq!(cost.memory_cost, 1_000.0);

    // CPU cost: scan left + scan right + (rows_out * 11.5 multiplier for hash join)
    // left_cpu (100k) + right_cpu (1k) + (100k * 11.5) = 1,251,000
    assert!(cost.cpu_cost > 1_200_000.0);
    assert!(cost.cpu_cost < 1_300_000.0);

    // IO cost: read both sides + output materialization
    // left_io (100k) + right_io (1k) + rows_out (100k) = 201k
    assert_eq!(cost.io_cost, 201_000.0);
}

#[test]
fn test_nested_loop_has_no_memory_cost() {
    let graph = create_test_graph();

    let left = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let nlj = PhysicalPlan::NestedLoopJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&nlj);

    // NestedLoopJoin should have no memory cost
    assert_eq!(cost.memory_cost, 0.0);

    // CPU cost: left * right comparisons = 100k * 1k = 100M
    assert!(cost.cpu_cost > 100_000_000.0);

    // IO cost: read both sides + output materialization
    // left_io (100k) + right_io (1k) + rows_out (100k) = 201k
    assert_eq!(cost.io_cost, 201_000.0);
}

#[test]
fn test_hash_join_cheaper_than_nested_loop_for_large_tables() {
    let graph = create_test_graph();

    let left = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let hash_join = PhysicalPlan::HashJoin {
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
        on: vec![],
        estimated_rows: None,
    };

    let nlj = PhysicalPlan::NestedLoopJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let hash_cost = estimator.estimate(&hash_join);
    let nlj_cost = estimator.estimate(&nlj);

    // HashJoin should have much lower total cost than NestedLoopJoin
    assert!(hash_cost.total() < nlj_cost.total());

    // HashJoin CPU (~1.25M) should be much lower than NLJ CPU (~100M)
    // At least 50x cheaper
    assert!(hash_cost.cpu_cost < nlj_cost.cpu_cost / 50.0);
}

#[test]
fn test_join_io_cost_includes_both_sides_and_output() {
    let graph = create_test_graph();

    let left = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let hash_join = PhysicalPlan::HashJoin {
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let left_cost = estimator.estimate(&left);
    let right_cost = estimator.estimate(&right);
    let join_cost = estimator.estimate(&hash_join);

    // Join IO should equal sum of both sides plus output row materialization
    // For N:1 join, output rows = left rows (100k)
    let expected_io = left_cost.io_cost + right_cost.io_cost + 100_000.0;
    assert_eq!(join_cost.io_cost, expected_io);
}

#[test]
fn test_hash_join_memory_uses_smaller_side() {
    let graph = create_test_graph();

    // Test both orderings
    let left_small = PhysicalPlan::TableScan {
        table: "customers".to_string(), // Small: 1k rows
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right_large = PhysicalPlan::TableScan {
        table: "orders".to_string(), // Large: 100k rows
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let join1 = PhysicalPlan::HashJoin {
        left: Box::new(left_small.clone()),
        right: Box::new(right_large.clone()),
        on: vec![],
        estimated_rows: None,
    };

    let join2 = PhysicalPlan::HashJoin {
        left: Box::new(right_large),
        right: Box::new(left_small),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost1 = estimator.estimate(&join1);
    let cost2 = estimator.estimate(&join2);

    // Both should use smaller side for memory (1000 rows)
    assert_eq!(cost1.memory_cost, 1_000.0);
    assert_eq!(cost2.memory_cost, 1_000.0);
}
