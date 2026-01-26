// tests/planner/cardinality_estimator_test.rs
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
