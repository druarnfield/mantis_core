use mantis::model::{Report, ShowItem};
use mantis::planner::SqlPlanner;
use mantis::semantic::graph::UnifiedGraph;

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
