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

#[test]
fn test_disconnected_tables() {
    let mut graph = UnifiedGraph::new();

    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    graph.add_test_entity(orders);

    let products = EntityNode {
        name: "products".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(500),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    graph.add_test_entity(products);
    // No relationship between orders and products

    let tables = vec!["orders".to_string(), "products".to_string()];
    let join_graph = JoinGraph::build(&graph, &tables);

    // Should NOT be joinable
    assert!(!join_graph.are_joinable("orders", "products"));

    // Should return None for edge
    assert!(join_graph.get_join_edge("orders", "products").is_none());
}

#[test]
fn test_are_table_sets_joinable() {
    let mut graph = UnifiedGraph::new();

    // Chain: A -> B -> C
    let a = EntityNode {
        name: "A".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let a_idx = graph.add_test_entity(a);

    let b = EntityNode {
        name: "B".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(200),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let b_idx = graph.add_test_entity(b);

    let c = EntityNode {
        name: "C".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(300),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let c_idx = graph.add_test_entity(c);

    let ab_join = JoinsToEdge {
        from_entity: "A".to_string(),
        to_entity: "B".to_string(),
        join_columns: vec![("b_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(a_idx, b_idx, ab_join);

    let bc_join = JoinsToEdge {
        from_entity: "B".to_string(),
        to_entity: "C".to_string(),
        join_columns: vec![("c_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(b_idx, c_idx, bc_join);

    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let join_graph = JoinGraph::build(&graph, &tables);

    // {A} and {B} are joinable
    let s1 = vec!["A".to_string()];
    let s2 = vec!["B".to_string()];
    assert!(join_graph.are_sets_joinable(&s1, &s2));

    // {A} and {C} are NOT directly joinable (need B in between)
    let s1 = vec!["A".to_string()];
    let s2 = vec!["C".to_string()];
    assert!(!join_graph.are_sets_joinable(&s1, &s2));

    // {A,B} and {C} are joinable (B connects to C)
    let s1 = vec!["A".to_string(), "B".to_string()];
    let s2 = vec!["C".to_string()];
    assert!(join_graph.are_sets_joinable(&s1, &s2));
}
