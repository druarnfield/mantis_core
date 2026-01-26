//! Tests for GROUP BY cardinality estimation (Task 8)

use mantis::planner::cost::CostEstimator;
use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};
use mantis::semantic::graph::{
    ColumnNode, DataType, EntityNode, EntityType, SizeCategory, UnifiedGraph,
};
use std::collections::HashMap;

fn create_test_graph_with_grouping_columns() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Add sales entity
    let sales = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(100_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    graph.add_test_entity(sales);

    // Add high-cardinality column (transaction_id)
    let mut high_card_metadata = HashMap::new();
    high_card_metadata.insert("cardinality".to_string(), "high".to_string());

    let high_card_col = ColumnNode {
        entity: "sales".to_string(),
        name: "transaction_id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        unique: true,
        primary_key: true,
        metadata: high_card_metadata,
    };
    graph.add_test_column(high_card_col);

    // Add low-cardinality column (region)
    let mut low_card_metadata = HashMap::new();
    low_card_metadata.insert("cardinality".to_string(), "low".to_string());

    let low_card_col = ColumnNode {
        entity: "sales".to_string(),
        name: "region".to_string(),
        data_type: DataType::String,
        nullable: false,
        unique: false,
        primary_key: false,
        metadata: low_card_metadata,
    };
    graph.add_test_column(low_card_col);

    // Add another low-cardinality column (status)
    let mut status_metadata = HashMap::new();
    status_metadata.insert("cardinality".to_string(), "low".to_string());

    let status_col = ColumnNode {
        entity: "sales".to_string(),
        name: "status".to_string(),
        data_type: DataType::String,
        nullable: false,
        unique: false,
        primary_key: false,
        metadata: status_metadata,
    };
    graph.add_test_column(status_col);

    // Add column without metadata
    let normal_col = ColumnNode {
        entity: "sales".to_string(),
        name: "customer_id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        unique: false,
        primary_key: false,
        metadata: Default::default(),
    };
    graph.add_test_column(normal_col);

    graph
}

// Task 8: GROUP BY cardinality estimation

#[test]
fn test_group_by_high_cardinality_column() {
    let graph = create_test_graph_with_grouping_columns();

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate = PhysicalPlan::HashAggregate {
        input: Box::new(input),
        group_by: vec!["sales.transaction_id".to_string()], // High cardinality
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&aggregate);

    // High cardinality: 50% of input rows
    // 100,000 * 0.5 = 50,000
    assert_eq!(cost.rows_out, 50_000);

    // Memory cost should be the number of groups
    assert_eq!(cost.memory_cost, 50_000.0);
}

#[test]
fn test_group_by_low_cardinality_column() {
    let graph = create_test_graph_with_grouping_columns();

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate = PhysicalPlan::HashAggregate {
        input: Box::new(input),
        group_by: vec!["sales.region".to_string()], // Low cardinality
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&aggregate);

    // Low cardinality: 10% of input rows
    // 100,000 * 0.1 = 10,000
    assert_eq!(cost.rows_out, 10_000);

    // Memory cost should be the number of groups
    assert_eq!(cost.memory_cost, 10_000.0);
}

#[test]
fn test_group_by_multiple_columns() {
    let graph = create_test_graph_with_grouping_columns();

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate = PhysicalPlan::HashAggregate {
        input: Box::new(input),
        group_by: vec![
            "sales.region".to_string(), // Low cardinality (0.1)
            "sales.status".to_string(), // Low cardinality (0.1)
        ],
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&aggregate);

    // Multiple columns: multiply selectivities
    // 100,000 * 0.1 * 0.1 = 1,000
    assert_eq!(cost.rows_out, 1_000);

    // Memory cost should be the number of groups
    assert_eq!(cost.memory_cost, 1_000.0);
}

#[test]
fn test_group_by_mixed_cardinality() {
    let graph = create_test_graph_with_grouping_columns();

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate = PhysicalPlan::HashAggregate {
        input: Box::new(input),
        group_by: vec![
            "sales.region".to_string(),         // Low cardinality (0.1)
            "sales.transaction_id".to_string(), // High cardinality (0.5)
        ],
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&aggregate);

    // Mixed cardinality: multiply selectivities
    // 100,000 * 0.1 * 0.5 = 5,000
    assert_eq!(cost.rows_out, 5_000);

    // Memory cost should be the number of groups
    assert_eq!(cost.memory_cost, 5_000.0);
}

#[test]
fn test_group_by_unknown_column() {
    let graph = create_test_graph_with_grouping_columns();

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate = PhysicalPlan::HashAggregate {
        input: Box::new(input),
        group_by: vec!["sales.customer_id".to_string()], // No cardinality metadata
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&aggregate);

    // Column without metadata: treated as low cardinality (0.1)
    // 100,000 * 0.1 = 10,000
    assert_eq!(cost.rows_out, 10_000);

    // Memory cost should be the number of groups
    assert_eq!(cost.memory_cost, 10_000.0);
}

#[test]
fn test_group_by_empty_produces_single_row() {
    let graph = create_test_graph_with_grouping_columns();

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate = PhysicalPlan::HashAggregate {
        input: Box::new(input),
        group_by: vec![], // No grouping - just aggregates
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&aggregate);

    // No grouping: single row output
    assert_eq!(cost.rows_out, 1);

    // Memory cost should be 1 for the single group
    assert_eq!(cost.memory_cost, 1.0);
}

#[test]
fn test_aggregate_adds_cpu_cost() {
    let graph = create_test_graph_with_grouping_columns();

    let input_plan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let aggregate_plan = PhysicalPlan::HashAggregate {
        input: Box::new(input_plan.clone()),
        group_by: vec!["sales.region".to_string()],
        aggregates: vec!["SUM(amount)".to_string()],
    };

    let estimator = CostEstimator::new(&graph);
    let input_cost = estimator.estimate(&input_plan);
    let agg_cost = estimator.estimate(&aggregate_plan);

    // Aggregate should add CPU cost for hashing and grouping
    // CPU cost = input CPU + input rows (for hashing)
    assert_eq!(
        agg_cost.cpu_cost,
        input_cost.cpu_cost + input_cost.rows_out as f64
    );

    // IO cost should be unchanged
    assert_eq!(agg_cost.io_cost, input_cost.io_cost);
}
