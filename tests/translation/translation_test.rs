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

#[test]
fn test_translate_drill_path_to_field_ref() {
    use mantis::model::{
        Calendar, CalendarBody, DrillPath, GrainLevel, GroupItem, PhysicalCalendar,
    };

    let mut calendars = HashMap::new();

    let mut grain_mappings = HashMap::new();
    grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
    grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());

    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "standard".to_string(),
        DrillPath {
            name: "standard".to_string(),
            levels: vec![GrainLevel::Day, GrainLevel::Month],
        },
    );

    calendars.insert(
        "dates".to_string(),
        Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings,
                drill_paths,
                fiscal_year_start: None,
                week_start: None,
            }),
        },
    );

    let model = Model {
        defaults: None,
        calendars,
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
        group: vec![GroupItem::DrillPathRef {
            source: "dates".to_string(),
            path: "standard".to_string(),
            level: "month".to_string(),
            label: Some("Month".to_string()),
        }],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.group_by.len(), 1);
    assert_eq!(query.group_by[0].entity, "dates");
    assert_eq!(query.group_by[0].field, "month_start_date");
}
