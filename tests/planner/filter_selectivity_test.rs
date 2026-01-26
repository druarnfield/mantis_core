//! Tests for filter selectivity estimation (Tasks 3, 4, 5)

use mantis::model::expr::{BinaryOp, Expr as ModelExpr, Literal};
use mantis::planner::cost::CostEstimator;
use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};
use mantis::semantic::graph::{
    ColumnNode, DataType, EntityNode, EntityType, SizeCategory, UnifiedGraph,
};
use std::collections::HashMap;

fn create_test_graph_with_cardinality() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Add sales entity
    let sales_entity = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(100_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    graph.add_test_entity(sales_entity);

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

    // Add column without cardinality metadata
    let normal_col = ColumnNode {
        entity: "sales".to_string(),
        name: "amount".to_string(),
        data_type: DataType::Float,
        nullable: false,
        unique: false,
        primary_key: false,
        metadata: Default::default(),
    };
    graph.add_test_column(normal_col);

    graph
}

// Task 3: Equality filter selectivity with cardinality

#[test]
fn test_equality_filter_high_cardinality() {
    let graph = create_test_graph_with_cardinality();

    // Filter: transaction_id = 12345 (high cardinality)
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "transaction_id".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::Int(12345))),
    };

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter = PhysicalPlan::Filter {
        input: Box::new(input),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&filter);

    // High cardinality: selectivity = 0.001
    // Input rows: 100,000 * 0.001 = 100
    assert_eq!(cost.rows_out, 100);

    // CPU cost should include scanning input + evaluating predicate
    assert!(cost.cpu_cost > 100_000.0);
}

#[test]
fn test_equality_filter_low_cardinality() {
    let graph = create_test_graph_with_cardinality();

    // Filter: region = 'WEST' (low cardinality)
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "region".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
    };

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter = PhysicalPlan::Filter {
        input: Box::new(input),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&filter);

    // Low cardinality: selectivity = 0.1
    // Input rows: 100,000 * 0.1 = 10,000
    assert_eq!(cost.rows_out, 10_000);
}

// Task 4: Range and logical predicates

#[test]
fn test_range_filter_selectivity() {
    let graph = create_test_graph_with_cardinality();

    // Filter: amount > 1000 (range predicate)
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::Int(1000))),
    };

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter = PhysicalPlan::Filter {
        input: Box::new(input),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&filter);

    // Range predicate: selectivity = 0.33
    // Input rows: 100,000 * 0.33 = 33,000
    assert_eq!(cost.rows_out, 33_000);
}

#[test]
fn test_and_predicate_combines_multiplicatively() {
    let graph = create_test_graph_with_cardinality();

    // Filter: region = 'WEST' AND amount > 1000
    // Low-card (0.1) AND range (0.33) = 0.033
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "region".to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
        }),
        op: BinaryOp::And,
        right: Box::new(ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(ModelExpr::Literal(Literal::Int(1000))),
        }),
    };

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter = PhysicalPlan::Filter {
        input: Box::new(input),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&filter);

    // AND combines multiplicatively: 0.1 * 0.33 = 0.033
    // Input rows: 100,000 * 0.033 = 3,300
    assert_eq!(cost.rows_out, 3_300);
}

#[test]
fn test_or_predicate_combines_additively() {
    let graph = create_test_graph_with_cardinality();

    // Filter: region = 'WEST' OR region = 'EAST'
    // Low-card (0.1) OR low-card (0.1) = 0.1 + 0.1 - (0.1 * 0.1) = 0.19
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "region".to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
        }),
        op: BinaryOp::Or,
        right: Box::new(ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "region".to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(ModelExpr::Literal(Literal::String("EAST".to_string()))),
        }),
    };

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter = PhysicalPlan::Filter {
        input: Box::new(input),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&filter);

    // OR combines with probability: 0.1 + 0.1 - (0.1 * 0.1) = 0.19
    // Input rows: 100,000 * 0.19 = 19,000
    assert_eq!(cost.rows_out, 19_000);
}

// Task 5: Filter cost estimation

#[test]
fn test_filter_adds_cpu_cost() {
    let graph = create_test_graph_with_cardinality();

    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "region".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
    };

    let input_plan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter_plan = PhysicalPlan::Filter {
        input: Box::new(input_plan.clone()),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let input_cost = estimator.estimate(&input_plan);
    let filter_cost = estimator.estimate(&filter_plan);

    // Filter should add CPU cost for evaluating each input row
    // CPU cost = input CPU + input rows (for evaluation)
    assert!(filter_cost.cpu_cost > input_cost.cpu_cost);
    assert_eq!(
        filter_cost.cpu_cost,
        input_cost.cpu_cost + input_cost.rows_out as f64
    );
}

#[test]
fn test_highly_selective_filter_reduces_rows() {
    let graph = create_test_graph_with_cardinality();

    // Very selective: high-cardinality equality (0.001)
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "transaction_id".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::Int(12345))),
    };

    let input = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let filter = PhysicalPlan::Filter {
        input: Box::new(input.clone()),
        predicate,
    };

    let estimator = CostEstimator::new(&graph);
    let input_cost = estimator.estimate(&input);
    let filter_cost = estimator.estimate(&filter);

    // Filter should significantly reduce output rows
    assert!(filter_cost.rows_out < input_cost.rows_out / 100);
    assert_eq!(filter_cost.rows_out, 100); // 100,000 * 0.001
}
