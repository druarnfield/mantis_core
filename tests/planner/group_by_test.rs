//! Tests for GROUP BY extraction and generation (Task 14).

use mantis::model::{GroupItem, Report, ShowItem};
use mantis::planner::logical::{LogicalPlan, PlanBuilder as LogicalPlanBuilder};
use mantis::planner::physical::PhysicalPlanner;
use mantis::semantic::graph::UnifiedGraph;
use mantis::sql::Dialect;

fn create_test_graph() -> UnifiedGraph {
    UnifiedGraph::new()
}

#[test]
fn test_extract_explicit_group_by() {
    // RED: Test that report.group items become GROUP BY columns
    let graph = create_test_graph();
    let builder = LogicalPlanBuilder::new(&graph);

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::InlineSlicer {
            name: "region".to_string(),
            label: None,
        }],
        show: vec![ShowItem::Measure {
            name: "total_amount".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let logical_plan = builder.build(&report).unwrap();

    // Find the Aggregate node
    let aggregate = find_aggregate_node(&logical_plan);
    assert!(aggregate.is_some(), "Should have Aggregate node");

    let agg = aggregate.unwrap();
    assert_eq!(agg.group_by.len(), 1, "Should have one GROUP BY column");
    assert_eq!(agg.group_by[0].entity, "sales");
    assert_eq!(agg.group_by[0].column, "region");
}

#[test]
fn test_extract_implicit_group_by_from_group_and_show() {
    // RED: Test that report.group items and show columns combine for GROUP BY
    // Note: In current model, dimensions are specified in report.group, not show
    // This test verifies the combination works
    let graph = create_test_graph();
    let builder = LogicalPlanBuilder::new(&graph);

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::InlineSlicer {
            name: "region".to_string(),
            label: None,
        }],
        show: vec![ShowItem::Measure {
            name: "total_amount".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let logical_plan = builder.build(&report).unwrap();

    // Find the Aggregate node
    let aggregate = find_aggregate_node(&logical_plan);
    assert!(aggregate.is_some(), "Should have Aggregate node");

    let agg = aggregate.unwrap();
    assert_eq!(agg.group_by.len(), 1, "Should have one GROUP BY column");
    assert_eq!(agg.group_by[0].column, "region");
}

#[test]
fn test_group_by_generates_sql() {
    // RED: Test that GROUP BY columns generate SQL GROUP BY clause
    let graph = create_test_graph();
    let builder = LogicalPlanBuilder::new(&graph);
    let planner = PhysicalPlanner::new(&graph);

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::InlineSlicer {
            name: "region".to_string(),
            label: None,
        }],
        show: vec![ShowItem::Measure {
            name: "total_amount".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let logical_plan = builder.build(&report).unwrap();
    let physical_plans = planner.generate_candidates(&logical_plan).unwrap();
    let query = physical_plans[0].to_query();
    let sql = query.to_sql(Dialect::Postgres);

    // Should have GROUP BY clause
    assert!(
        sql.contains("GROUP BY") || sql.contains("group by"),
        "Query should contain GROUP BY clause, got: {}",
        sql
    );
    assert!(
        sql.contains("region"),
        "Query should GROUP BY region, got: {}",
        sql
    );
}

#[test]
fn test_multiple_group_by_columns() {
    // RED: Test multiple GROUP BY columns
    let graph = create_test_graph();
    let builder = LogicalPlanBuilder::new(&graph);

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![
            GroupItem::InlineSlicer {
                name: "region".to_string(),
                label: None,
            },
            GroupItem::InlineSlicer {
                name: "product_category".to_string(),
                label: None,
            },
        ],
        show: vec![ShowItem::Measure {
            name: "total_amount".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let logical_plan = builder.build(&report).unwrap();

    let aggregate = find_aggregate_node(&logical_plan);
    assert!(aggregate.is_some(), "Should have Aggregate node");

    let agg = aggregate.unwrap();
    assert_eq!(agg.group_by.len(), 2, "Should have two GROUP BY columns");
}

// Helper function to find Aggregate node in plan tree
fn find_aggregate_node(plan: &LogicalPlan) -> Option<&mantis::planner::logical::AggregateNode> {
    match plan {
        LogicalPlan::Aggregate(agg) => Some(agg),
        LogicalPlan::Project(proj) => find_aggregate_node(&proj.input),
        LogicalPlan::Sort(sort) => find_aggregate_node(&sort.input),
        LogicalPlan::Limit(limit) => find_aggregate_node(&limit.input),
        LogicalPlan::Filter(filter) => find_aggregate_node(&filter.input),
        _ => None,
    }
}
