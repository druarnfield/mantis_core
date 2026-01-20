use mantis::model::{Model, Report};
use mantis::translation;
use std::collections::HashMap;

#[test]
fn test_translate_empty_report() {
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.from, Some("fact_sales".to_string()));
    assert_eq!(query.select.len(), 0);
}

#[test]
fn test_translate_empty_from_returns_error() {
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec![], // Empty!
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_err());

    match result.unwrap_err() {
        translation::TranslationError::UndefinedReference { entity_type, name } => {
            assert_eq!(entity_type, "table");
            assert_eq!(name, "(none specified)");
        }
        _ => panic!("Expected UndefinedReference error"),
    }
}

#[test]
fn test_translate_multiple_tables_returns_error() {
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string(), "fact_orders".to_string()], // Multiple!
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_err());

    match result.unwrap_err() {
        translation::TranslationError::UndefinedReference { entity_type, .. } => {
            assert_eq!(entity_type, "multi-table query");
        }
        _ => panic!("Expected UndefinedReference error for multi-table"),
    }
}
