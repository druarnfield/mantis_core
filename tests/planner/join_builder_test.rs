use mantis::planner::join_builder::JoinBuilder;
use mantis::planner::logical::{JoinCondition, JoinType, LogicalPlan};
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};

/// Helper to create a test graph with two entities and a join relationship.
fn create_test_graph_two_tables() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Add sales entity
    let sales_idx = graph.add_test_entity(EntityNode {
        name: "sales".to_string(),
        physical_name: Some("sales".to_string()),
        schema: None,
        entity_type: EntityType::Fact,
        row_count: Some(10000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    });

    // Add products entity
    let products_idx = graph.add_test_entity(EntityNode {
        name: "products".to_string(),
        physical_name: Some("products".to_string()),
        schema: None,
        entity_type: EntityType::Dimension,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    });

    // Add join relationship: sales -> products
    graph.add_test_join(
        sales_idx,
        products_idx,
        JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: "products".to_string(),
            join_columns: vec![("product_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph
}

/// Helper to create a test graph with three entities in a chain: sales -> products -> categories
fn create_test_graph_three_tables() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Add sales entity
    let sales_idx = graph.add_test_entity(EntityNode {
        name: "sales".to_string(),
        physical_name: Some("sales".to_string()),
        schema: None,
        entity_type: EntityType::Fact,
        row_count: Some(10000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    });

    // Add products entity
    let products_idx = graph.add_test_entity(EntityNode {
        name: "products".to_string(),
        physical_name: Some("products".to_string()),
        schema: None,
        entity_type: EntityType::Dimension,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    });

    // Add categories entity
    let categories_idx = graph.add_test_entity(EntityNode {
        name: "categories".to_string(),
        physical_name: Some("categories".to_string()),
        schema: None,
        entity_type: EntityType::Dimension,
        row_count: Some(20),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    });

    // Add join relationship: sales -> products
    graph.add_test_join(
        sales_idx,
        products_idx,
        JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: "products".to_string(),
            join_columns: vec![("product_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    // Add join relationship: products -> categories
    graph.add_test_join(
        products_idx,
        categories_idx,
        JoinsToEdge {
            from_entity: "products".to_string(),
            to_entity: "categories".to_string(),
            join_columns: vec![("category_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph
}

/// Helper to create a test graph where sales->customers requires a multi-hop through orders
fn create_test_graph_multihop() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Add sales entity
    let sales_idx = graph.add_test_entity(EntityNode {
        name: "sales".to_string(),
        physical_name: Some("sales".to_string()),
        schema: None,
        entity_type: EntityType::Fact,
        row_count: Some(10000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    });

    // Add orders entity (intermediate)
    let orders_idx = graph.add_test_entity(EntityNode {
        name: "orders".to_string(),
        physical_name: Some("orders".to_string()),
        schema: None,
        entity_type: EntityType::Fact,
        row_count: Some(5000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    });

    // Add customers entity
    let customers_idx = graph.add_test_entity(EntityNode {
        name: "customers".to_string(),
        physical_name: Some("customers".to_string()),
        schema: None,
        entity_type: EntityType::Dimension,
        row_count: Some(1000),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    });

    // Add join relationship: sales -> orders
    graph.add_test_join(
        sales_idx,
        orders_idx,
        JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: "orders".to_string(),
            join_columns: vec![("order_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    // Add join relationship: orders -> customers
    graph.add_test_join(
        orders_idx,
        customers_idx,
        JoinsToEdge {
            from_entity: "orders".to_string(),
            to_entity: "customers".to_string(),
            join_columns: vec![("customer_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    // Note: NO direct sales -> customers relationship
    graph
}

#[test]
fn test_build_single_table() {
    let graph = UnifiedGraph::new();
    let builder = JoinBuilder::new(&graph);

    let plan = builder.build_join_tree(&vec!["sales".to_string()]).unwrap();

    match plan {
        LogicalPlan::Scan(scan) => {
            assert_eq!(scan.entity, "sales");
        }
        _ => panic!("Expected Scan node for single table"),
    }
}

#[test]
fn test_build_two_table_join() {
    let graph = create_test_graph_two_tables();
    let builder = JoinBuilder::new(&graph);

    let plan = builder
        .build_join_tree(&vec!["sales".to_string(), "products".to_string()])
        .unwrap();

    // Should create Join(Scan(sales), Scan(products))
    match plan {
        LogicalPlan::Join(join) => {
            // Verify left is sales
            match *join.left {
                LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "sales"),
                _ => panic!("Expected left to be Scan(sales)"),
            }

            // Verify right is products
            match *join.right {
                LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "products"),
                _ => panic!("Expected right to be Scan(products)"),
            }

            // Verify join condition
            match &join.on {
                JoinCondition::Equi(pairs) => {
                    assert_eq!(pairs.len(), 1);
                    assert_eq!(pairs[0].0.entity, "sales");
                    assert_eq!(pairs[0].0.column, "product_id");
                    assert_eq!(pairs[0].1.entity, "products");
                    assert_eq!(pairs[0].1.column, "id");
                }
                _ => panic!("Expected Equi join condition"),
            }

            // Verify join type
            assert_eq!(join.join_type, JoinType::Inner);

            // Verify cardinality
            assert_eq!(join.cardinality, Some(Cardinality::ManyToOne));
        }
        _ => panic!("Expected Join node"),
    }
}

#[test]
fn test_build_multihop_join() {
    let graph = create_test_graph_multihop();
    let builder = JoinBuilder::new(&graph);

    // Request join from sales to customers (requires going through orders)
    let plan = builder
        .build_join_tree(&vec!["sales".to_string(), "customers".to_string()])
        .unwrap();

    // Should create Join(Join(Scan(sales), Scan(orders)), Scan(customers))
    // because the path is sales -> orders -> customers
    match plan {
        LogicalPlan::Join(outer_join) => {
            // Verify right is customers
            match *outer_join.right {
                LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "customers"),
                _ => panic!("Expected right to be Scan(customers)"),
            }

            // Verify left is a join between sales and orders
            match *outer_join.left {
                LogicalPlan::Join(ref inner_join) => {
                    // Verify inner left is sales
                    match *inner_join.left {
                        LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "sales"),
                        _ => panic!("Expected inner left to be Scan(sales)"),
                    }

                    // Verify inner right is orders
                    match *inner_join.right {
                        LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "orders"),
                        _ => panic!("Expected inner right to be Scan(orders)"),
                    }
                }
                _ => panic!("Expected left to be a Join node"),
            }
        }
        _ => panic!("Expected Join node"),
    }
}

#[test]
fn test_build_three_table_join_chain() {
    let graph = create_test_graph_three_tables();
    let builder = JoinBuilder::new(&graph);

    let plan = builder
        .build_join_tree(&vec![
            "sales".to_string(),
            "products".to_string(),
            "categories".to_string(),
        ])
        .unwrap();

    // Should create Join(Join(Scan(sales), Scan(products)), Scan(categories))
    match plan {
        LogicalPlan::Join(outer_join) => {
            // Verify right is categories
            match *outer_join.right {
                LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "categories"),
                _ => panic!("Expected right to be Scan(categories)"),
            }

            // Verify left is a join between sales and products
            match *outer_join.left {
                LogicalPlan::Join(ref inner_join) => {
                    // Verify inner left is sales
                    match *inner_join.left {
                        LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "sales"),
                        _ => panic!("Expected inner left to be Scan(sales)"),
                    }

                    // Verify inner right is products
                    match *inner_join.right {
                        LogicalPlan::Scan(ref scan) => assert_eq!(scan.entity, "products"),
                        _ => panic!("Expected inner right to be Scan(products)"),
                    }
                }
                _ => panic!("Expected left to be a Join node"),
            }

            // Verify outer join condition is products -> categories
            match &outer_join.on {
                JoinCondition::Equi(pairs) => {
                    assert_eq!(pairs.len(), 1);
                    assert_eq!(pairs[0].0.entity, "products");
                    assert_eq!(pairs[0].0.column, "category_id");
                    assert_eq!(pairs[0].1.entity, "categories");
                    assert_eq!(pairs[0].1.column, "id");
                }
                _ => panic!("Expected Equi join condition"),
            }
        }
        _ => panic!("Expected Join node"),
    }
}
