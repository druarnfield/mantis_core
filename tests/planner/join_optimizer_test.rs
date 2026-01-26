//! Tests for join order optimizer.

use mantis::model::expr::{BinaryOp, Expr, Literal};
use mantis::planner::logical::{
    FilterNode, JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode,
};
use mantis::planner::physical::join_optimizer::{JoinOrderOptimizer, OptimizerStrategy};
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

// ============================================================================
// Tasks 11-13: Greedy Algorithm for Large Joins (>3 tables)
// ============================================================================

/// Helper to create a 5-table test graph (large enough to trigger greedy)
fn create_large_test_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Create a star schema: fact table in center, 4 dimensions around it
    // This is a realistic pattern for data warehouses

    // Fact table (large)
    let sales = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000_000), // 10M rows
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let sales_idx = graph.add_test_entity(sales);

    // Small dimension tables
    let tables = vec![
        ("products", 1_000),
        ("customers", 5_000),
        ("stores", 100),
        ("dates", 365),
    ];

    let mut dim_indices = Vec::new();
    for (name, row_count) in tables {
        let entity = EntityNode {
            name: name.to_string(),
            entity_type: EntityType::Dimension,
            physical_name: None,
            schema: None,
            row_count: Some(row_count),
            size_category: SizeCategory::Small,
            metadata: Default::default(),
        };
        let idx = graph.add_test_entity(entity);
        dim_indices.push((name.to_string(), idx));
    }

    // Add joins: sales → each dimension
    for (dim_name, dim_idx) in dim_indices {
        let join = JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: dim_name.clone(),
            join_columns: vec![(format!("{}_id", dim_name), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        };
        graph.add_test_join(sales_idx, dim_idx, join);
    }

    graph
}

#[test]
fn test_greedy_join_order_for_large_query() {
    // Test that greedy_join_order() works for 5+ tables
    let graph = create_large_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // Create a logical plan with all 5 tables
    let tables = vec!["sales", "products", "customers", "stores", "dates"];

    let result = optimizer.greedy_join_order(&tables);

    // Should return a valid plan
    assert!(result.is_some(), "Greedy should return a valid join plan");

    let plan = result.unwrap();

    // Verify all tables are included
    let extracted_tables = optimizer.extract_tables(&plan);
    assert_eq!(extracted_tables.len(), 5, "Should include all 5 tables");
    for table in &tables {
        assert!(
            extracted_tables.contains(&table.to_string()),
            "Should include table: {}",
            table
        );
    }
}

#[test]
fn test_find_smallest_join_pair() {
    // Test that find_smallest_join_pair() returns the pair with lowest cost
    let graph = create_large_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    let tables: Vec<String> = vec![
        "sales".to_string(),
        "products".to_string(),
        "stores".to_string(),
    ];

    let (table1, table2) = optimizer.find_smallest_join_pair(&tables).unwrap();

    // Should return a valid pair that can be joined
    assert_ne!(table1, table2, "Pair should be different tables");
    assert!(tables.contains(&table1), "First table should be in input");
    assert!(tables.contains(&table2), "Second table should be in input");
}

#[test]
fn test_find_best_next_join() {
    // Test that find_best_next_join() returns the best table to add
    let graph = create_large_test_graph();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // Start with a plan: sales joined with stores
    let current_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "stores".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("sales".to_string(), "stores_id".to_string()),
            ColumnRef::new("stores".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    let remaining = vec![
        "products".to_string(),
        "customers".to_string(),
        "dates".to_string(),
    ];

    let next_table = optimizer
        .find_best_next_join(&current_plan, &remaining)
        .unwrap();

    // Should return one of the remaining tables
    assert!(
        remaining.contains(&next_table),
        "Next table should be from remaining tables"
    );
}

// ============================================================================
// Task 14: Optimizer Strategy Selection (Adaptive/DP/Legacy)
// ============================================================================

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
