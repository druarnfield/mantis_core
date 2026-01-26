//! Tests for join cardinality estimation (Task 6)

use mantis::planner::cost::CostEstimator;
use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};

/// Create test graph with different cardinality relationships
fn create_test_graph_with_joins() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Add entities
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

    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    let order_items = EntityNode {
        name: "order_items".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(50_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let order_items_idx = graph.add_test_entity(order_items);

    let products = EntityNode {
        name: "products".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(500),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let products_idx = graph.add_test_entity(products);

    // Add joins with different cardinalities

    // customers → orders (1:N - one customer has many orders)
    let customer_orders_join = JoinsToEdge {
        from_entity: "customers".to_string(),
        to_entity: "orders".to_string(),
        join_columns: vec![("customer_id".to_string(), "customer_id".to_string())],
        cardinality: Cardinality::OneToMany,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(customers_idx, orders_idx, customer_orders_join);

    // orders → customers (N:1 - many orders belong to one customer)
    let orders_customer_join = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "customers".to_string(),
        join_columns: vec![("customer_id".to_string(), "customer_id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, customers_idx, orders_customer_join);

    // orders → order_items (1:N - one order has many items)
    let order_items_join = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "order_items".to_string(),
        join_columns: vec![("order_id".to_string(), "order_id".to_string())],
        cardinality: Cardinality::OneToMany,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, order_items_idx, order_items_join);

    // products → order_items (N:N - many products in many orders)
    let products_items_join = JoinsToEdge {
        from_entity: "products".to_string(),
        to_entity: "order_items".to_string(),
        join_columns: vec![("product_id".to_string(), "product_id".to_string())],
        cardinality: Cardinality::ManyToMany,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(products_idx, order_items_idx, products_items_join);

    graph
}

// Task 6: Join cardinality estimation

#[test]
fn test_one_to_one_join_cardinality() {
    let mut graph = UnifiedGraph::new();

    // Create two entities with 1:1 relationship
    let users = EntityNode {
        name: "users".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(1_000),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let users_idx = graph.add_test_entity(users);

    let profiles = EntityNode {
        name: "profiles".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(1_000),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let profiles_idx = graph.add_test_entity(profiles);

    // 1:1 join
    let join_edge = JoinsToEdge {
        from_entity: "users".to_string(),
        to_entity: "profiles".to_string(),
        join_columns: vec![("user_id".to_string(), "user_id".to_string())],
        cardinality: Cardinality::OneToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(users_idx, profiles_idx, join_edge);

    let left = PhysicalPlan::TableScan {
        table: "users".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "profiles".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let join = PhysicalPlan::HashJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&join);

    // 1:1 join: output = max(left, right) = max(1000, 1000) = 1000
    assert_eq!(cost.rows_out, 1_000);
}

#[test]
fn test_one_to_many_join_cardinality() {
    let graph = create_test_graph_with_joins();

    let left = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let join = PhysicalPlan::HashJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&join);

    // 1:N join (customers → orders): output = right (many side) = 10,000
    assert_eq!(cost.rows_out, 10_000);
}

#[test]
fn test_many_to_one_join_cardinality() {
    let graph = create_test_graph_with_joins();

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

    let join = PhysicalPlan::HashJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&join);

    // N:1 join (orders → customers): output = left (many side) = 10,000
    assert_eq!(cost.rows_out, 10_000);
}

#[test]
fn test_many_to_many_join_cardinality() {
    let graph = create_test_graph_with_joins();

    let left = PhysicalPlan::TableScan {
        table: "products".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let right = PhysicalPlan::TableScan {
        table: "order_items".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let join = PhysicalPlan::HashJoin {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![],
        estimated_rows: None,
    };

    let estimator = CostEstimator::new(&graph);
    let cost = estimator.estimate(&join);

    // N:N join (products × order_items): output = (left * right) / 100
    // (500 * 50,000) / 100 = 25,000,000 / 100 = 250,000
    assert_eq!(cost.rows_out, 250_000);
}
