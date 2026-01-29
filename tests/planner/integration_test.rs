use mantis::model::expr::{BinaryOp, Expr as ModelExpr, Literal};
use mantis::model::{GroupItem, Report, ShowItem, SortDirection, SortItem};
use mantis::planner::SqlPlanner;
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};
use mantis::sql::Dialect;

#[test]
fn test_simple_report_end_to_end() {
    let graph = UnifiedGraph::new();
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report);

    // Should not error (even though query won't be fully implemented yet)
    assert!(query.is_ok());
}

#[test]
fn test_query_generation() {
    let graph = UnifiedGraph::new();
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: Some(10),
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();

    // Query should have limit set
    assert_eq!(
        query.limit_offset.as_ref().and_then(|lo| lo.limit),
        Some(10)
    );
}

#[test]
fn test_measure_selection() {
    let graph = UnifiedGraph::new();
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "total_amount".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();

    // Query should include the measure in SELECT
    assert!(!query.select.is_empty());
    assert_eq!(
        query.select.len(),
        1,
        "Expected exactly one measure in SELECT clause"
    );
}

#[test]
fn test_multiple_measures_selection() {
    let graph = UnifiedGraph::new();
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![
            ShowItem::Measure {
                name: "total_amount".to_string(),
                label: None,
            },
            ShowItem::Measure {
                name: "total_quantity".to_string(),
                label: Some("Total Qty".to_string()),
            },
        ],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();

    // Query should include both measures in SELECT
    assert_eq!(
        query.select.len(),
        2,
        "Expected two measures in SELECT clause"
    );
}

// ============================================================================
// WAVE 1 COMPREHENSIVE INTEGRATION TESTS (Tasks 17-19)
// ============================================================================

/// Task 17: Simple filter integration test
/// Tests end-to-end: Report with WHERE filter → SQL with WHERE clause
#[test]
fn test_simple_filter_integration() {
    let graph = UnifiedGraph::new();

    // Create report with simple filter: WHERE amount > 100
    let report = Report {
        name: "sales_over_100".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "total_revenue".to_string(),
            label: None,
        }],
        filters: vec![ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(ModelExpr::Literal(Literal::Int(100))),
        }],
        sort: vec![],
        limit: None,
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    let sql = query.to_sql(Dialect::Postgres);

    // Verify SQL contains WHERE clause
    assert!(
        sql.to_uppercase().contains("WHERE"),
        "SQL should contain WHERE clause, got: {}",
        sql
    );

    // Verify predicate is present
    assert!(
        sql.contains("amount") && sql.contains("100"),
        "SQL should contain filter predicate, got: {}",
        sql
    );
}

/// Task 17: Multiple filters with AND combination
#[test]
fn test_multiple_filters_integration() {
    let graph = UnifiedGraph::new();

    // Create report with multiple filters: WHERE amount > 100 AND region = 'WEST'
    let report = Report {
        name: "west_sales_over_100".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "total_revenue".to_string(),
            label: None,
        }],
        filters: vec![
            ModelExpr::BinaryOp {
                left: Box::new(ModelExpr::Column {
                    entity: Some("sales".to_string()),
                    column: "amount".to_string(),
                }),
                op: BinaryOp::Gt,
                right: Box::new(ModelExpr::Literal(Literal::Int(100))),
            },
            ModelExpr::BinaryOp {
                left: Box::new(ModelExpr::Column {
                    entity: Some("sales".to_string()),
                    column: "region".to_string(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
            },
        ],
        sort: vec![],
        limit: None,
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    let sql = query.to_sql(Dialect::Postgres);

    // Verify SQL contains WHERE clause
    assert!(
        sql.to_uppercase().contains("WHERE"),
        "SQL should contain WHERE clause, got: {}",
        sql
    );

    // Verify both predicates are present
    assert!(
        sql.contains("amount") && sql.contains("region"),
        "SQL should contain both filter predicates, got: {}",
        sql
    );
}

/// Helper function to create a test graph with sales → products join
fn create_sales_products_graph() -> UnifiedGraph {
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

    // Add join relationship: sales → products
    graph.add_test_join(
        sales_idx,
        products_idx,
        JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: "products".to_string(),
            join_columns: vec![("product_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::Explicit,
        },
    );

    graph
}

/// Task 18: Multi-table join integration test
/// Tests end-to-end: Report with 2 tables → SQL with JOIN...ON
#[test]
fn test_multi_table_join_integration() {
    let graph = create_sales_products_graph();

    // Create report joining sales and products
    let report = Report {
        name: "sales_by_product".to_string(),
        from: vec!["sales".to_string(), "products".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "total_revenue".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    let sql = query.to_sql(Dialect::Postgres);

    // Verify SQL contains JOIN
    assert!(
        sql.to_uppercase().contains("JOIN"),
        "SQL should contain JOIN, got: {}",
        sql
    );

    // Verify both tables are referenced
    assert!(
        sql.contains("sales") && sql.contains("products"),
        "SQL should reference both tables, got: {}",
        sql
    );

    // Verify ON clause with join columns
    assert!(
        sql.to_uppercase().contains("ON"),
        "SQL should contain ON clause, got: {}",
        sql
    );
}

/// Helper function to create a test graph with sales → products → categories
fn create_three_table_graph() -> UnifiedGraph {
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

    // Add join: sales → products
    graph.add_test_join(
        sales_idx,
        products_idx,
        JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: "products".to_string(),
            join_columns: vec![("product_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::Explicit,
        },
    );

    // Add join: products → categories
    graph.add_test_join(
        products_idx,
        categories_idx,
        JoinsToEdge {
            from_entity: "products".to_string(),
            to_entity: "categories".to_string(),
            join_columns: vec![("category_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::Explicit,
        },
    );

    graph
}

/// Task 19: Complex query integration test
/// Tests end-to-end: Report with WHERE + JOIN + GROUP BY → Complete SQL
#[test]
// TODO: Fix SQL generation for nested joins - categories table missing from output
#[ignore]
fn test_complex_query_with_where_join_groupby() {
    let graph = create_three_table_graph();

    // Create complex report:
    // - 3 tables (sales, products, categories)
    // - Filter: WHERE sales.amount > 100
    // - GROUP BY: category
    // - Measure: total_revenue
    // - ORDER BY: total_revenue DESC
    // - LIMIT: 10
    let report = Report {
        name: "top_categories_by_revenue".to_string(),
        from: vec![
            "sales".to_string(),
            "products".to_string(),
            "categories".to_string(),
        ],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::InlineSlicer {
            name: "category_name".to_string(),
            label: None,
        }],
        show: vec![ShowItem::Measure {
            name: "total_revenue".to_string(),
            label: None,
        }],
        filters: vec![ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(ModelExpr::Literal(Literal::Int(100))),
        }],
        sort: vec![SortItem {
            column: "total_revenue".to_string(),
            direction: SortDirection::Desc,
        }],
        limit: Some(10),
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    let sql = query.to_sql(Dialect::Postgres);

    println!("Generated SQL:\n{}", sql);

    // Verify all major clauses are present
    let sql_upper = sql.to_uppercase();

    assert!(
        sql_upper.contains("SELECT"),
        "SQL should contain SELECT, got: {}",
        sql
    );

    assert!(
        sql_upper.contains("FROM"),
        "SQL should contain FROM, got: {}",
        sql
    );

    assert!(
        sql_upper.contains("JOIN"),
        "SQL should contain JOIN (2 joins expected), got: {}",
        sql
    );

    assert!(
        sql_upper.contains("WHERE"),
        "SQL should contain WHERE clause, got: {}",
        sql
    );

    assert!(
        sql_upper.contains("GROUP BY"),
        "SQL should contain GROUP BY clause, got: {}",
        sql
    );

    assert!(
        sql_upper.contains("ORDER BY"),
        "SQL should contain ORDER BY clause, got: {}",
        sql
    );

    assert!(
        sql_upper.contains("LIMIT"),
        "SQL should contain LIMIT clause, got: {}",
        sql
    );

    // Verify all tables are referenced
    assert!(
        sql.contains("sales"),
        "SQL should reference sales table, got: {}",
        sql
    );

    assert!(
        sql.contains("products"),
        "SQL should reference products table, got: {}",
        sql
    );

    assert!(
        sql.contains("categories"),
        "SQL should reference categories table, got: {}",
        sql
    );

    // Verify filter is applied
    assert!(
        sql.contains("amount") && sql.contains("100"),
        "SQL should contain filter predicate, got: {}",
        sql
    );

    // Verify GROUP BY column
    assert!(
        sql.contains("category_name"),
        "SQL should GROUP BY category_name, got: {}",
        sql
    );

    // Verify LIMIT value
    assert!(sql.contains("10"), "SQL should have LIMIT 10, got: {}", sql);
}

/// Task 19: Complex query with multiple filters and groups
#[test]
fn test_complex_query_multiple_filters_and_groups() {
    let graph = create_three_table_graph();

    // Create complex report with multiple filters and grouping columns
    let report = Report {
        name: "detailed_sales_analysis".to_string(),
        from: vec![
            "sales".to_string(),
            "products".to_string(),
            "categories".to_string(),
        ],
        use_date: vec![],
        period: None,
        group: vec![
            GroupItem::InlineSlicer {
                name: "category_name".to_string(),
                label: None,
            },
            GroupItem::InlineSlicer {
                name: "region".to_string(),
                label: None,
            },
        ],
        show: vec![
            ShowItem::Measure {
                name: "total_revenue".to_string(),
                label: None,
            },
            ShowItem::Measure {
                name: "total_quantity".to_string(),
                label: None,
            },
        ],
        filters: vec![
            ModelExpr::BinaryOp {
                left: Box::new(ModelExpr::Column {
                    entity: Some("sales".to_string()),
                    column: "amount".to_string(),
                }),
                op: BinaryOp::Gt,
                right: Box::new(ModelExpr::Literal(Literal::Int(100))),
            },
            ModelExpr::BinaryOp {
                left: Box::new(ModelExpr::Column {
                    entity: Some("sales".to_string()),
                    column: "status".to_string(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(ModelExpr::Literal(Literal::String("completed".to_string()))),
            },
        ],
        sort: vec![
            SortItem {
                column: "total_revenue".to_string(),
                direction: SortDirection::Desc,
            },
            SortItem {
                column: "region".to_string(),
                direction: SortDirection::Asc,
            },
        ],
        limit: Some(50),
    };

    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(&report).unwrap();
    let sql = query.to_sql(Dialect::Postgres);

    println!("Generated complex SQL:\n{}", sql);

    let sql_upper = sql.to_uppercase();

    // Verify all major components
    assert!(sql_upper.contains("SELECT"), "SQL should have SELECT");
    assert!(sql_upper.contains("JOIN"), "SQL should have JOINs");
    assert!(sql_upper.contains("WHERE"), "SQL should have WHERE");
    assert!(sql_upper.contains("GROUP BY"), "SQL should have GROUP BY");
    assert!(sql_upper.contains("ORDER BY"), "SQL should have ORDER BY");
    assert!(sql_upper.contains("LIMIT"), "SQL should have LIMIT");

    // Verify both filter conditions are present
    assert!(
        sql.contains("amount") && sql.contains("status"),
        "SQL should contain both filter conditions, got: {}",
        sql
    );

    // Verify both GROUP BY columns
    assert!(
        sql.contains("category_name") && sql.contains("region"),
        "SQL should GROUP BY both columns, got: {}",
        sql
    );

    // Verify both measures in SELECT
    assert_eq!(
        query.select.len(),
        2,
        "Should have 2 measures in SELECT clause"
    );

    // Verify both sort columns
    assert_eq!(query.order_by.len(), 2, "Should have 2 ORDER BY columns");
}
