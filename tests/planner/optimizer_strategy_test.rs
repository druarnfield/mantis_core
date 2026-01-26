// tests/planner/optimizer_strategy_test.rs
use mantis_core::planner::physical::join_optimizer::*;
use mantis_core::semantic::graph::UnifiedGraph;

#[test]
fn test_strategy_selection_small_query() {
    let graph = UnifiedGraph::new();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // 3 tables - should use DP
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];

    let strategy = optimizer.select_strategy(&tables);

    match strategy {
        OptimizerStrategy::DP => {} // Expected
        _ => panic!("Expected DP for 3 tables"),
    }
}

#[test]
fn test_strategy_selection_large_query() {
    let graph = UnifiedGraph::new();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // 11 tables - should fall back to greedy
    let tables: Vec<_> = (0..11).map(|i| format!("T{}", i)).collect();

    let strategy = optimizer.select_strategy(&tables);

    match strategy {
        OptimizerStrategy::Legacy => {} // Expected
        _ => panic!("Expected Legacy for 11 tables"),
    }
}

#[test]
fn test_strategy_selection_boundary_10_tables() {
    let graph = UnifiedGraph::new();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // 10 tables - should still use DP (≤10)
    let tables: Vec<_> = (0..10).map(|i| format!("T{}", i)).collect();

    let strategy = optimizer.select_strategy(&tables);

    match strategy {
        OptimizerStrategy::DP => {} // Expected
        _ => panic!("Expected DP for 10 tables (boundary)"),
    }
}

#[test]
fn test_can_create_optimizer_with_explicit_strategy() {
    let graph = UnifiedGraph::new();
    let optimizer = JoinOrderOptimizer::with_strategy(&graph, OptimizerStrategy::DP);

    let tables = vec!["A".to_string(), "B".to_string()];

    // Should use DP regardless of table count
    let strategy = optimizer.select_strategy(&tables);

    match strategy {
        OptimizerStrategy::DP => {} // Expected
        _ => panic!("Expected DP when explicitly set"),
    }
}
