// tests/planner/join_graph_test.rs
use mantis::planner::join_optimizer::join_graph::*;
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};

#[test]
fn test_join_graph_can_be_created() {
    let graph = UnifiedGraph::new();
    let tables = vec!["orders".to_string(), "customers".to_string()];

    let join_graph = JoinGraph::build(&graph, &tables);

    assert_eq!(join_graph.table_count(), 2);
}

#[test]
fn test_build_join_graph_from_unified_graph() {
    let mut graph = UnifiedGraph::new();

    // Add entities
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    let customers = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let customers_idx = graph.add_test_entity(customers);

    // Add relationship orders.customer_id -> customers.id (N:1)
    let join_edge = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "customers".to_string(),
        join_columns: vec![("customer_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, customers_idx, join_edge);

    let tables = vec!["orders".to_string(), "customers".to_string()];
    let join_graph = JoinGraph::build(&graph, &tables);

    // Should have edge between orders and customers
    assert!(join_graph.are_joinable("orders", "customers"));

    // Should get join edge info
    let edge = join_graph.get_join_edge("orders", "customers");
    assert!(edge.is_some());
    assert_eq!(edge.unwrap().cardinality, Cardinality::ManyToOne);
}
