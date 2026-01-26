// tests/planner/join_graph_test.rs
use mantis::planner::join_optimizer::join_graph::*;
use mantis::semantic::graph::UnifiedGraph;

#[test]
fn test_join_graph_can_be_created() {
    let graph = UnifiedGraph::new();
    let tables = vec!["orders".to_string(), "customers".to_string()];

    let join_graph = JoinGraph::build(&graph, &tables);

    assert_eq!(join_graph.table_count(), 2);
}
