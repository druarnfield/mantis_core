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

#[test]
fn test_filter_selectivity_or() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    // WHERE amount > 1000 OR status = 'completed'
    let filter = Expr::BinaryOp {
        op: BinaryOp::Or,
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

    // OR uses probability union: s1 + s2 - (s1 * s2) = 0.33 + 0.1 - (0.33 * 0.1) = 0.397
    assert!((selectivity - 0.397).abs() < 0.01);
}

#[test]
fn test_estimate_join_with_zero_rows() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    let edge = JoinEdge {
        cardinality: Cardinality::OneToOne,
        join_columns: vec![],
    };

    // Joining with zero rows should produce zero rows
    let output = estimator.estimate_join_output(1000, 0, &edge);
    assert_eq!(output, 0);

    // Both sides zero
    let output = estimator.estimate_join_output(0, 0, &edge);
    assert_eq!(output, 0);
}

#[test]
fn test_estimate_unknown_cardinality() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    let edge = JoinEdge {
        cardinality: Cardinality::Unknown,
        join_columns: vec![],
    };

    // Unknown cardinality should use conservative estimate (max of both sides)
    let output = estimator.estimate_join_output(1000, 500, &edge);
    assert_eq!(output, 1000);

    let output = estimator.estimate_join_output(500, 1000, &edge);
    assert_eq!(output, 1000);
}

#[test]
fn test_filter_selectivity_nested_and_or() {
    let graph = UnifiedGraph::new();
    let estimator = CardinalityEstimator::new(&graph);

    // WHERE (amount > 1000 OR amount < 100) AND status = 'completed'
    let filter = Expr::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column {
                    entity: Some("orders".to_string()),
                    column: "amount".to_string(),
                }),
                right: Box::new(Expr::Literal(Literal::Int(1000))),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOp::Lt,
                left: Box::new(Expr::Column {
                    entity: Some("orders".to_string()),
                    column: "amount".to_string(),
                }),
                right: Box::new(Expr::Literal(Literal::Int(100))),
            }),
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

    // Inner OR: 0.33 + 0.33 - (0.33 * 0.33) = 0.5511
    // Outer AND: 0.5511 * 0.1 = 0.05511
    assert!((selectivity - 0.05511).abs() < 0.01);
}
