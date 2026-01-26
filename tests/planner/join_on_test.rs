//! Tests for JOIN ON clause generation (Task 11).

use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
use mantis::planner::physical::{PhysicalPlan, PhysicalPlanner};
use mantis::semantic::graph::{query::ColumnRef, Cardinality, UnifiedGraph};
use mantis::sql::Dialect;

fn create_test_graph() -> UnifiedGraph {
    UnifiedGraph::new()
}

#[test]
fn test_join_converts_to_physical_hash_join() {
    // RED: Test that LogicalPlan::Join converts to PhysicalPlan::HashJoin
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    // Create logical join: sales INNER JOIN products ON sales.product_id = products.id
    let join_condition = JoinCondition::Equi(vec![(
        ColumnRef::new("sales".to_string(), "product_id".to_string()),
        ColumnRef::new("products".to_string(), "id".to_string()),
    )]);

    let logical_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        on: join_condition,
        join_type: JoinType::Inner,
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Convert to physical plan
    let result = planner.generate_candidates(&logical_plan);
    assert!(result.is_ok(), "Converting join to physical should succeed");

    let physical_plans = result.unwrap();
    assert!(!physical_plans.is_empty(), "Should produce physical plans");

    // Should have at least one HashJoin plan
    let has_hash_join = physical_plans.iter().any(|plan| {
        matches!(plan, PhysicalPlan::HashJoin { .. })
    });
    assert!(has_hash_join, "Should produce HashJoin physical plan");
}

#[test]
fn test_join_on_generates_sql() {
    // RED: Test that HashJoin.to_query() generates SQL with JOIN ON clause
    let plan = PhysicalPlan::HashJoin {
        left: Box::new(PhysicalPlan::TableScan {
            table: "sales".to_string(),
            strategy: mantis::planner::physical::TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        right: Box::new(PhysicalPlan::TableScan {
            table: "products".to_string(),
            strategy: mantis::planner::physical::TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        on: vec![("product_id".to_string(), "id".to_string())],
        estimated_rows: None,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    // Should have JOIN with ON clause
    assert!(
        sql.contains("JOIN") || sql.contains("join"),
        "Query should contain JOIN, got: {}",
        sql
    );
    assert!(
        sql.contains("sales") && sql.contains("products"),
        "Query should reference both tables, got: {}",
        sql
    );
}

#[test]
fn test_join_on_multiple_columns() {
    // RED: Test JOIN ON with multiple column pairs
    let plan = PhysicalPlan::HashJoin {
        left: Box::new(PhysicalPlan::TableScan {
            table: "orders".to_string(),
            strategy: mantis::planner::physical::TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        right: Box::new(PhysicalPlan::TableScan {
            table: "shipments".to_string(),
            strategy: mantis::planner::physical::TableScanStrategy::FullScan,
            estimated_rows: None,
        }),
        on: vec![
            ("order_id".to_string(), "order_id".to_string()),
            ("line_num".to_string(), "line_num".to_string()),
        ],
        estimated_rows: None,
    };

    let query = plan.to_query();
    let sql = query.to_sql(Dialect::Postgres);

    // Should have JOIN with multiple ON conditions combined with AND
    assert!(
        sql.contains("JOIN") || sql.contains("join"),
        "Query should contain JOIN, got: {}",
        sql
    );
    assert!(
        sql.contains("order_id") && sql.contains("line_num"),
        "Query should reference both join columns, got: {}",
        sql
    );
    assert!(
        sql.contains("AND") || sql.contains("and"),
        "Query should combine multiple join conditions with AND, got: {}",
        sql
    );
}
