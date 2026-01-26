use mantis::model::Report;
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
