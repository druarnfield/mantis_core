//! Tests for join order optimizer.

use mantis::model::expr::{BinaryOp, Expr, Literal};
use mantis::planner::logical::{
    FilterNode, JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode,
};
use mantis::planner::physical::join_optimizer::JoinOrderOptimizer;
use mantis::semantic::graph::{
    query::ColumnRef, Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource,
    SizeCategory, UnifiedGraph,
};

/// Helper to create test graph with multiple tables
fn create_test_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Sales (large fact table)
    let sales = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1_000_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let sales_idx = graph.add_test_entity(sales);

    // Products (medium dimension)
    let products = EntityNode {
        name: "products".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let products_idx = graph.add_test_entity(products);

    // Categories (small dimension)
    let categories = EntityNode {
        name: "categories".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let categories_idx = graph.add_test_entity(categories);

    // Sales → Products (N:1)
    let sales_products_join = JoinsToEdge {
        from_entity: "sales".to_string(),
        to_entity: "products".to_string(),
        join_columns: vec![("product_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(sales_idx, products_idx, sales_products_join);

    // Products → Categories (N:1)
    let products_categories_join = JoinsToEdge {
        from_entity: "products".to_string(),
        to_entity: "categories".to_string(),
        join_columns: vec![("category_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(products_idx, categories_idx, products_categories_join);

    graph
}

/// Helper to create a simple 2-table join logical plan
fn create_two_table_join() -> LogicalPlan {
    LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("sales".to_string(), "product_id".to_string()),
            ColumnRef::new("products".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    })
}

/// Helper to create a 3-table join logical plan (sales → products → categories)
fn create_three_table_join() -> LogicalPlan {
    let sales_products = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("sales".to_string(), "product_id".to_string()),
            ColumnRef::new("products".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    LogicalPlan::Join(JoinNode {
        left: Box::new(sales_products),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "categories".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("products".to_string(), "category_id".to_string()),
            ColumnRef::new("categories".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    })
}

#[test]
fn test_extract_tables_from_single_scan() {
    // Test that extract_tables() correctly identifies tables in a scan
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let plan = LogicalPlan::Scan(ScanNode {
        entity: "sales".to_string(),
    });

    let tables = optimizer.extract_tables(&plan);

    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&"sales".to_string()));
}

#[test]
fn test_extract_tables_from_two_table_join() {
    // Test that extract_tables() correctly identifies tables in a 2-table join
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let plan = create_two_table_join();

    let tables = optimizer.extract_tables(&plan);

    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"sales".to_string()));
    assert!(tables.contains(&"products".to_string()));
}

#[test]
fn test_extract_tables_from_three_table_join() {
    // Test that extract_tables() correctly identifies tables in a 3-table join
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let plan = create_three_table_join();

    let tables = optimizer.extract_tables(&plan);

    assert_eq!(tables.len(), 3);
    assert!(tables.contains(&"sales".to_string()));
    assert!(tables.contains(&"products".to_string()));
    assert!(tables.contains(&"categories".to_string()));
}

#[test]
fn test_extract_tables_with_filter() {
    // Test that extract_tables() works through filter nodes
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(Literal::Int(100))),
    };

    let plan = LogicalPlan::Filter(FilterNode {
        input: Box::new(create_two_table_join()),
        predicates: vec![predicate],
    });

    let tables = optimizer.extract_tables(&plan);

    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"sales".to_string()));
    assert!(tables.contains(&"products".to_string()));
}

// ============================================================================
// Task 10: Enumeration Algorithm for Small Joins (≤3 tables)
// ============================================================================

#[test]
fn test_enumerate_two_table_join() {
    // Test that enumerate_join_orders() generates both possible orders for 2 tables
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let plan = create_two_table_join();

    let candidates = optimizer.enumerate_join_orders(&plan);

    // Should generate 2! = 2 permutations
    assert_eq!(
        candidates.len(),
        2,
        "Should generate 2 join orders for 2 tables"
    );

    // Both orders should be valid (different table orderings)
    // We can't easily check the exact order without PhysicalPlan equality,
    // but we can verify all candidates are non-empty
    for candidate in &candidates {
        assert!(candidate.is_some(), "All candidates should be valid plans");
    }
}

#[test]
fn test_enumerate_three_table_join() {
    // Test that enumerate_join_orders() generates all 6 permutations for 3 tables
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let plan = create_three_table_join();

    let candidates = optimizer.enumerate_join_orders(&plan);

    // Should generate 3! = 6 permutations (some may be None if no valid join path)
    assert_eq!(
        candidates.len(),
        6,
        "Should generate 6 join orders for 3 tables"
    );

    // At least some candidates should be valid
    let valid_count = candidates.iter().filter(|c| c.is_some()).count();
    assert!(valid_count > 0, "Should have at least one valid join order");

    // For our test graph (sales → products → categories chain),
    // only certain orders have valid join paths. Example valid orders:
    // - sales, products, categories (original chain)
    // - products, sales, categories (reverse first link)
    // Invalid orders like sales, categories, products won't work
    // because there's no direct sales→categories edge
}

#[test]
fn test_enumerate_single_table() {
    // Test that enumerate_join_orders() handles single table (no joins)
    let graph = create_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let plan = LogicalPlan::Scan(ScanNode {
        entity: "sales".to_string(),
    });

    let candidates = optimizer.enumerate_join_orders(&plan);

    // Single table should return just one "order" (itself)
    assert_eq!(
        candidates.len(),
        1,
        "Single table should return one candidate"
    );
    assert!(
        candidates[0].is_some(),
        "Single table candidate should be valid"
    );
}
