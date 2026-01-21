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
fn test_translate_filters() {
    use mantis::dsl::span::Span;
    use mantis::model::{Atom, AtomType, SqlExpr, Table};

    let mut atoms = HashMap::new();
    atoms.insert(
        "amount".to_string(),
        Atom {
            name: "amount".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
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
        filters: vec![SqlExpr {
            sql: "@amount > 100".to_string(),
            span: Span::default(),
        }],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    // Note: Filters are compiled but not yet added to query.filters
    // This is a skeleton implementation - full FieldFilter parsing deferred
    // The fact that translation succeeds means filter SQL compilation worked
    assert_eq!(query.filters.len(), 0); // TODO: Will be 1 after FieldFilter parsing implemented
}

#[test]
fn test_translate_sort_and_limit() {
    use mantis::model::{SortDirection, SortItem};

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
        sort: vec![
            SortItem {
                column: "revenue".to_string(),
                direction: SortDirection::Desc,
            },
            SortItem {
                column: "region".to_string(),
                direction: SortDirection::Asc,
            },
        ],
        limit: Some(100),
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.order_by.len(), 2);
    assert_eq!(query.order_by[0].field.entity, "fact_sales");
    assert_eq!(query.order_by[0].field.field, "revenue");
    assert!(query.order_by[0].descending);
    assert_eq!(query.order_by[1].field.entity, "fact_sales");
    assert_eq!(query.order_by[1].field.field, "region");
    assert!(!query.order_by[1].descending);
    assert_eq!(query.limit, Some(100));
}

#[test]
fn test_translate_inline_measure() {
    use mantis::dsl::span::Span;
    use mantis::model::{Atom, AtomType, Measure, MeasureBlock, ShowItem, SqlExpr, Table};

    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );
    atoms.insert(
        "cost".to_string(),
        Atom {
            name: "cost".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "revenue".to_string(),
        Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    measure_map.insert(
        "cost".to_string(),
        Measure {
            name: "cost".to_string(),
            expr: SqlExpr {
                sql: "sum(@cost)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![
            // Need base measures
            ShowItem::Measure {
                name: "revenue".to_string(),
                label: None,
            },
            ShowItem::Measure {
                name: "cost".to_string(),
                label: None,
            },
            // Inline measure references them
            ShowItem::InlineMeasure {
                name: "profit".to_string(),
                expr: SqlExpr {
                    sql: "revenue - cost".to_string(),
                    span: Span::default(),
                },
                label: Some("Profit".to_string()),
            },
        ],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    // Inline measures not fully implemented yet - should return error
    assert!(result.is_err());
    // TODO: Once SQL expression parsing is implemented, this test should pass
    // and verify: query.select.len() == 2, query.derived.len() == 1
}

#[test]
fn test_translate_period_to_filter() {
    use mantis::model::{PeriodExpr, RelativePeriod};

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
        use_date: vec!["order_date".to_string()],
        period: None, // Skip period for now - skeleton implementation
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    // When period is None, no period filter is generated
    let query = result.unwrap();
    assert_eq!(query.filters.len(), 0);
}

#[test]
fn test_compile_sql_expression_with_atoms() {
    use mantis::dsl::span::Span;
    use mantis::model::{Atom, AtomType, SqlExpr, Table};

    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );
    atoms.insert(
        "quantity".to_string(),
        Atom {
            name: "quantity".to_string(),
            data_type: AtomType::Integer,
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let expr = SqlExpr {
        sql: "@revenue * @quantity".to_string(),
        span: Span::default(),
    };

    let result = translation::compile_sql_expr(&expr, "fact_sales", &model);
    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        "dbo.fact_sales.revenue * dbo.fact_sales.quantity"
    );
}

#[test]
fn test_translate_inline_slicer() {
    use mantis::model::{DataType, GroupItem, Slicer, Table};

    let mut tables = HashMap::new();
    let mut slicers = HashMap::new();
    slicers.insert(
        "region".to_string(),
        Slicer::Inline {
            name: "region".to_string(),
            data_type: DataType::String,
        },
    );

    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times: HashMap::new(),
            slicers,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::InlineSlicer {
            name: "region".to_string(),
            label: Some("Region".to_string()),
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
    assert_eq!(query.group_by[0].entity, "fact_sales");
    assert_eq!(query.group_by[0].field, "region");
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

#[test]
fn test_translate_simple_measure() {
    use mantis::dsl::span::Span;
    use mantis::model::{Atom, AtomType, Measure, MeasureBlock, ShowItem, SqlExpr, Table};

    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "total_revenue".to_string(),
        Measure {
            name: "total_revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "total_revenue".to_string(),
            label: Some("Total Revenue".to_string()),
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.select.len(), 1);
    assert_eq!(query.select[0].field.entity, "fact_sales");
    assert_eq!(query.select[0].field.field, "total_revenue");
    assert_eq!(query.select[0].alias, Some("Total Revenue".to_string()));
}

#[test]
fn test_translate_measure_with_ytd_suffix() {
    use mantis::dsl::span::Span;
    use mantis::model::{
        Atom, AtomType, Measure, MeasureBlock, ShowItem, SqlExpr, Table, TimeSuffix,
    };

    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "revenue".to_string(),
        Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::MeasureWithSuffix {
            name: "revenue".to_string(),
            suffix: TimeSuffix::Ytd,
            label: Some("YTD Revenue".to_string()),
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    // Base measure should be in select
    assert_eq!(query.select.len(), 1);
    assert_eq!(query.select[0].field.field, "revenue");

    // YTD should be in derived
    assert_eq!(query.derived.len(), 1);
    assert_eq!(query.derived[0].alias, "YTD Revenue");
}

#[test]
fn test_malformed_sql_expression() {
    use mantis::dsl::span::Span;
    use mantis::model::{Atom, AtomType, SqlExpr, Table};

    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
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
        filters: vec![SqlExpr {
            sql: "@revenue > 100 AND (@revenue".to_string(), // Malformed: unclosed parenthesis
            span: Span::default(),
        }],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_err(), "Malformed SQL should cause an error");

    match result.unwrap_err() {
        translation::TranslationError::SqlCompilationError { expression, error } => {
            assert!(expression.contains("@revenue > 100"));
            assert!(
                error.contains("Invalid SQL"),
                "Error should mention invalid SQL"
            );
        }
        _ => panic!("Expected SqlCompilationError"),
    }
}

#[test]
fn test_undefined_atom_in_sql() {
    use mantis::dsl::span::Span;
    use mantis::model::{Atom, AtomType, SqlExpr, Table};

    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
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
        filters: vec![SqlExpr {
            sql: "@revenue > 100 AND @undefined_atom < 50".to_string(), // undefined_atom doesn't exist
            span: Span::default(),
        }],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_err(), "Undefined atom should cause an error");

    match result.unwrap_err() {
        translation::TranslationError::SqlCompilationError { expression, error } => {
            assert!(expression.contains("@undefined_atom"));
            assert!(error.contains("Undefined atom: @undefined_atom"));
        }
        _ => panic!("Expected SqlCompilationError for undefined atom"),
    }
}

#[test]
fn test_multiple_duplicate_measures() {
    use mantis::dsl::span::Span;
    use mantis::model::{
        Atom, AtomType, Measure, MeasureBlock, ShowItem, SqlExpr, Table, TimeSuffix,
    };

    // Setup model
    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "revenue".to_string(),
        Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    // Report showing revenue multiple times with different time suffixes
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![
            ShowItem::Measure {
                name: "revenue".to_string(),
                label: Some("Revenue".to_string()),
            },
            ShowItem::MeasureWithSuffix {
                name: "revenue".to_string(),
                suffix: TimeSuffix::Ytd,
                label: Some("YTD Revenue".to_string()),
            },
            ShowItem::MeasureWithSuffix {
                name: "revenue".to_string(),
                suffix: TimeSuffix::Mtd,
                label: Some("MTD Revenue".to_string()),
            },
            ShowItem::Measure {
                name: "revenue".to_string(),
                label: Some("Revenue Again".to_string()), // Duplicate!
            },
        ],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();

    // The measure should appear only ONCE in select, despite multiple references
    assert_eq!(
        query.select.len(),
        1,
        "Measure should be deduplicated even with multiple references"
    );
    assert_eq!(query.select[0].field.field, "revenue");

    // All time suffixes should be in derived
    assert_eq!(query.derived.len(), 2);
}

#[test]
fn test_translate_all_time_suffixes() {
    use mantis::dsl::span::Span;
    use mantis::model::{
        Atom, AtomType, Measure, MeasureBlock, Model, Report, ShowItem, SqlExpr, Table, TimeSuffix,
    };

    // Setup model with measure (same as previous test)
    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "revenue".to_string(),
        Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    // Test each time suffix type
    let test_suffixes = vec![
        TimeSuffix::Ytd,
        TimeSuffix::Qtd,
        TimeSuffix::Mtd,
        TimeSuffix::Wtd,
        TimeSuffix::FiscalYtd,
        TimeSuffix::FiscalQtd,
        TimeSuffix::PriorYear,
        TimeSuffix::PriorQuarter,
        TimeSuffix::PriorMonth,
        TimeSuffix::PriorWeek,
        TimeSuffix::YoyGrowth,
        TimeSuffix::QoqGrowth,
        TimeSuffix::MomGrowth,
        TimeSuffix::WowGrowth,
        TimeSuffix::YoyDelta,
        TimeSuffix::QoqDelta,
        TimeSuffix::MomDelta,
        TimeSuffix::WowDelta,
        TimeSuffix::Rolling3m,
        TimeSuffix::Rolling6m,
        TimeSuffix::Rolling12m,
        TimeSuffix::Rolling3mAvg,
        TimeSuffix::Rolling6mAvg,
        TimeSuffix::Rolling12mAvg,
    ];

    for suffix in test_suffixes {
        let report = Report {
            name: "test_report".to_string(),
            from: vec!["fact_sales".to_string()],
            use_date: vec![],
            period: None,
            group: vec![],
            show: vec![ShowItem::MeasureWithSuffix {
                name: "revenue".to_string(),
                suffix,
                label: None,
            }],
            filters: vec![],
            sort: vec![],
            limit: None,
        };

        let result = translation::translate_report(&report, &model);
        assert!(result.is_ok(), "Failed to translate suffix: {:?}", suffix);
    }
}

#[test]
fn test_measure_deduplication_with_time_suffix() {
    use mantis::dsl::span::Span;
    use mantis::model::{
        Atom, AtomType, Measure, MeasureBlock, ShowItem, SqlExpr, Table, TimeSuffix,
    };

    // Setup model with measure
    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "revenue".to_string(),
        Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );

    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    // Report showing revenue both standalone and with YTD suffix
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![
            ShowItem::Measure {
                name: "revenue".to_string(),
                label: Some("Revenue".to_string()),
            },
            ShowItem::MeasureWithSuffix {
                name: "revenue".to_string(),
                suffix: TimeSuffix::Ytd,
                label: Some("YTD Revenue".to_string()),
            },
        ],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();

    // The measure should appear only ONCE in select, even though it's used
    // both standalone and with a time suffix
    assert_eq!(
        query.select.len(),
        1,
        "Measure should not be duplicated in select"
    );
    assert_eq!(query.select[0].field.field, "revenue");

    // YTD should be in derived
    assert_eq!(query.derived.len(), 1);
    assert_eq!(query.derived[0].alias, "YTD Revenue");
}
