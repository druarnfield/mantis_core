// tests/planner/cardinality_estimator_test.rs
use mantis::model::expr::{BinaryOp, Expr, Literal};
use mantis::planner::join_optimizer::cardinality::*;
use mantis::planner::join_optimizer::join_graph::JoinEdge;
use mantis::semantic::graph::{Cardinality, UnifiedGraph};

#[test]
fn test_estimate_one_to_one_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    let edge = JoinEdge {
        cardinality: Cardinality::OneToOne,
        join_columns: vec![],
    };

    let output = estimator.estimate_join_output(1000, 500, &edge);

    // 1:1 should return min(left, right)
    assert_eq!(output, 500);
}

#[test]
fn test_estimate_one_to_many_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    let edge = JoinEdge {
        cardinality: Cardinality::OneToMany,
        join_columns: vec![],
    };

    // 100 customers (left) -> 1000 orders (right)
    let output = estimator.estimate_join_output(100, 1000, &edge);

    // 1:N should return "many" side (right)
    assert_eq!(output, 1000);
}

#[test]
fn test_estimate_many_to_one_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    let edge = JoinEdge {
        cardinality: Cardinality::ManyToOne,
        join_columns: vec![],
    };

    // 1000 orders (left) -> 100 customers (right)
    let output = estimator.estimate_join_output(1000, 100, &edge);

    // N:1 should return "many" side (left)
    assert_eq!(output, 1000);
}

#[test]
fn test_estimate_many_to_many_join() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    let edge = JoinEdge {
        cardinality: Cardinality::ManyToMany,
        join_columns: vec![],
    };

    let output = estimator.estimate_join_output(1000, 100, &edge);

    // N:N should be less than cross product (100K)
    // Using sqrt heuristic: 1000 * sqrt(100) = 10K
    assert_eq!(output, 10000);
}

#[test]
fn test_filter_selectivity_equality() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    // WHERE customer_id = 123
    let filter = Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "customer_id".to_string(),
        }),
        right: Box::new(Expr::Literal(Literal::Int(123))),
    };

    let selectivity = estimator.estimate_filter_selectivity(&filter);

    // Equality should be selective (default 0.1 = 10%)
    assert!((selectivity - 0.1).abs() < 0.01);
}

#[test]
fn test_filter_selectivity_range() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    // WHERE amount > 1000
    let filter = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column {
            entity: Some("orders".to_string()),
            column: "amount".to_string(),
        }),
        right: Box::new(Expr::Literal(Literal::Int(1000))),
    };

    let selectivity = estimator.estimate_filter_selectivity(&filter);

    // Range predicates: ~33% selectivity
    assert!((selectivity - 0.33).abs() < 0.01);
}

#[test]
fn test_filter_selectivity_and() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    // WHERE amount > 1000 AND status = 'completed'
    let filter = Expr::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                entity: Some("orders".to_string()),
                column: "amount".to_string(),
            }),
            right: Box::new(Expr::Literal(Literal::Int(1000))),
        }),
        right: Box::new(Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column {
                entity: Some("orders".to_string()),
                column: "status".to_string(),
            }),
            right: Box::new(Expr::Literal(Literal::String("completed".to_string()))),
        }),
    };

    let selectivity = estimator.estimate_filter_selectivity(&filter);

    // AND combines: 0.33 * 0.1 = 0.033 (3.3%)
    assert!((selectivity - 0.033).abs() < 0.01);
}
