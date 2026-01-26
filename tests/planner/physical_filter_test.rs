//! Comprehensive tests for physical plan Filter node (Task 9).

use mantis::model::expr::{BinaryOp, Expr as ModelExpr, Literal, UnaryOp};
use mantis::planner::logical::{FilterNode, LogicalPlan, ScanNode};
use mantis::planner::physical::{PhysicalPlan, PhysicalPlanner, TableScanStrategy};
use mantis::semantic::graph::UnifiedGraph;
use mantis::sql::Dialect;

fn create_test_graph() -> UnifiedGraph {
    UnifiedGraph::new()
}

#[test]
fn test_convert_filter_to_physical() {
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    // Create logical filter: WHERE amount > 100
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::Int(100))),
    };

    let logical_plan = LogicalPlan::Filter(FilterNode {
        input: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        predicates: vec![predicate.clone()],
    });

    // Convert to physical plan
    let result = planner.generate_candidates(&logical_plan);
    assert!(
        result.is_ok(),
        "Converting filter to physical should succeed"
    );

    let physical_plans = result.unwrap();
    assert_eq!(physical_plans.len(), 1, "Should produce one physical plan");

    // Verify structure
    match &physical_plans[0] {
        PhysicalPlan::Filter {
            input,
            predicate: p,
        } => {
            assert_eq!(*p, predicate, "Predicate should be preserved");
            match input.as_ref() {
                PhysicalPlan::TableScan { table, .. } => {
                    assert_eq!(table, "sales");
                }
                _ => panic!("Expected TableScan as input"),
            }
        }
        _ => panic!("Expected Filter node"),
    }
}

#[test]
fn test_filter_predicates_preserved() {
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    // Create complex predicate: WHERE region = 'WEST'
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "region".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
    };

    let logical_plan = LogicalPlan::Filter(FilterNode {
        input: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        predicates: vec![predicate.clone()],
    });

    let physical_plans = planner.generate_candidates(&logical_plan).unwrap();

    match &physical_plans[0] {
        PhysicalPlan::Filter { predicate: p, .. } => {
            // Verify the predicate is exactly as specified
            assert_eq!(*p, predicate);
        }
        _ => panic!("Expected Filter node"),
    }
}

#[test]
fn test_filter_to_query_generates_where_clause() {
    // Create a simple filter: WHERE sales.amount > 100
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::Int(100))),
    };

    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::TableScan {
            table: "sales".to_string(),
            strategy: TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        predicate,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    // Should have WHERE clause
    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
    assert!(
        sql.contains("amount") && sql.contains("100"),
        "Query should contain predicate, got: {}",
        sql
    );
}

#[test]
fn test_filter_with_string_predicate() {
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "region".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
    };

    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::TableScan {
            table: "sales".to_string(),
            strategy: TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        predicate,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
    assert!(
        sql.contains("region"),
        "Query should reference region column, got: {}",
        sql
    );
}

#[test]
fn test_filter_with_boolean_predicate() {
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "is_active".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::Bool(true))),
    };

    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::TableScan {
            table: "sales".to_string(),
            strategy: TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        predicate,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
    assert!(
        sql.contains("is_active"),
        "Query should reference is_active column, got: {}",
        sql
    );
}

#[test]
fn test_filter_with_comparison_operators() {
    // Test different comparison operators
    let operators = vec![
        (BinaryOp::Gt, "greater than"),
        (BinaryOp::Gte, "greater than or equal"),
        (BinaryOp::Lt, "less than"),
        (BinaryOp::Lte, "less than or equal"),
        (BinaryOp::Ne, "not equal"),
    ];

    for (op, desc) in operators {
        let predicate = ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op,
            right: Box::new(ModelExpr::Literal(Literal::Int(100))),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
                strategy: TableScanStrategy::FullScan,
                estimated_rows: None,
            }),
            predicate,
        };

        let query = plan.to_query();
        let sql = query.to_sql(Dialect::Postgres);

        assert!(
            sql.contains("WHERE"),
            "Query for {} should contain WHERE clause, got: {}",
            desc,
            sql
        );
    }
}

#[test]
fn test_filter_with_null_check() {
    let predicate = ModelExpr::UnaryOp {
        op: UnaryOp::IsNull,
        expr: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "notes".to_string(),
        }),
    };

    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::TableScan {
            table: "sales".to_string(),
            strategy: TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        predicate,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
    assert!(
        sql.contains("notes"),
        "Query should reference notes column, got: {}",
        sql
    );
}

#[test]
fn test_filter_with_entity_qualified_columns() {
    // Test that entity names are properly handled in predicates
    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "date".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::String(
            "2024-01-01".to_string(),
        ))),
    };

    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::TableScan {
            table: "sales".to_string(),
            strategy: TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        predicate,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
    // The query should contain the date column reference
    assert!(
        sql.contains("date"),
        "Query should reference date column, got: {}",
        sql
    );
}

#[test]
fn test_filter_over_another_filter() {
    // Test that filters can be stacked (though typically they should be combined)
    let predicate1 = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::Int(100))),
    };

    let predicate2 = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "region".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
    };

    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
                strategy: TableScanStrategy::FullScan,
                estimated_rows: None,
            }),
            predicate: predicate1,
        }),
        predicate: predicate2,
    };

    // Should be able to generate a query (even if not optimal)
    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
}

#[test]
fn test_filter_empty_predicates_error() {
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    // Create filter with no predicates (should error)
    let logical_plan = LogicalPlan::Filter(FilterNode {
        input: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        predicates: vec![],
    });

    let result = planner.generate_candidates(&logical_plan);
    assert!(
        result.is_err(),
        "Filter with no predicates should return an error"
    );
}

#[test]
fn test_filter_structure_correctness() {
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    let predicate = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::Int(100))),
    };

    let logical_plan = LogicalPlan::Filter(FilterNode {
        input: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        predicates: vec![predicate.clone()],
    });

    let physical_plans = planner.generate_candidates(&logical_plan).unwrap();
    let physical_plan = &physical_plans[0];

    // Verify the physical plan structure
    match physical_plan {
        PhysicalPlan::Filter {
            input,
            predicate: p,
        } => {
            // Input should be a TableScan
            assert!(
                matches!(
                    input.as_ref(),
                    PhysicalPlan::TableScan {
                        table,
                        strategy: TableScanStrategy::FullScan,
                        ..
                    } if table == "sales"
                ),
                "Filter input should be a TableScan for sales"
            );

            // Predicate should match
            assert_eq!(*p, predicate, "Predicate should be preserved");
        }
        _ => panic!("Expected PhysicalPlan::Filter"),
    }
}

// Task 10: Test multiple predicates combined with AND
#[test]
fn test_filter_multiple_predicates_combined_with_and() {
    // RED: Test for WHERE with multiple predicates: amount > 100 AND region = 'WEST'
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    let predicate1 = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "amount".to_string(),
        }),
        op: BinaryOp::Gt,
        right: Box::new(ModelExpr::Literal(Literal::Int(100))),
    };

    let predicate2 = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Column {
            entity: Some("sales".to_string()),
            column: "region".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
    };

    let logical_plan = LogicalPlan::Filter(FilterNode {
        input: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        predicates: vec![predicate1, predicate2],
    });

    let physical_plans = planner.generate_candidates(&logical_plan).unwrap();
    let plan = &physical_plans[0];

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    // Should combine predicates with AND
    assert!(
        sql.contains("WHERE"),
        "Query should contain WHERE clause, got: {}",
        sql
    );
    assert!(
        sql.contains("amount") && sql.contains("100"),
        "Query should contain first predicate, got: {}",
        sql
    );
    assert!(
        sql.contains("region") && sql.contains("WEST"),
        "Query should contain second predicate, got: {}",
        sql
    );
    assert!(
        sql.contains("AND"),
        "Query should combine predicates with AND, got: {}",
        sql
    );
}
