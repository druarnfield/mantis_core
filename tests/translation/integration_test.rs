use mantis::dsl::span::Span;
use mantis::model::{
    Atom, AtomType, Calendar, CalendarBody, DataType, DrillPath, GrainLevel, GroupItem, Measure,
    MeasureBlock, Model, PhysicalCalendar, Report, ShowItem, Slicer, SortDirection, SortItem,
    SqlExpr, Table, TimeSuffix,
};
use mantis::translation;
use std::collections::HashMap;

#[test]
fn test_complete_sales_report_translation() {
    // Build a complete model with calendar, table, slicers, and measures

    // Calendar setup
    let mut calendars = HashMap::new();
    let mut grain_mappings = HashMap::new();
    grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
    grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());
    grain_mappings.insert(GrainLevel::Year, "year".to_string());

    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "standard".to_string(),
        DrillPath {
            name: "standard".to_string(),
            levels: vec![GrainLevel::Year, GrainLevel::Month, GrainLevel::Day],
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

    // Table setup
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

    let mut slicers = HashMap::new();
    slicers.insert(
        "region".to_string(),
        Slicer::Inline {
            name: "region".to_string(),
            data_type: DataType::String,
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
            slicers,
        },
    );

    // Measures setup
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
    measure_map.insert(
        "total_quantity".to_string(),
        Measure {
            name: "total_quantity".to_string(),
            expr: SqlExpr {
                sql: "sum(@quantity)".to_string(),
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
        calendars,
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };

    // Create a realistic report:
    // "Show total revenue, YTD revenue, and quantity by region and month, sorted by revenue desc, top 100"
    let report = Report {
        name: "sales_by_region_month".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![
            GroupItem::InlineSlicer {
                name: "region".to_string(),
                label: Some("Region".to_string()),
            },
            GroupItem::DrillPathRef {
                source: "dates".to_string(),
                path: "standard".to_string(),
                level: "month".to_string(),
                label: Some("Month".to_string()),
            },
        ],
        show: vec![
            ShowItem::Measure {
                name: "total_revenue".to_string(),
                label: Some("Revenue".to_string()),
            },
            ShowItem::MeasureWithSuffix {
                name: "total_revenue".to_string(),
                suffix: TimeSuffix::Ytd,
                label: Some("YTD Revenue".to_string()),
            },
            ShowItem::Measure {
                name: "total_quantity".to_string(),
                label: Some("Quantity".to_string()),
            },
        ],
        filters: vec![],
        sort: vec![SortItem {
            column: "Revenue".to_string(),
            direction: SortDirection::Desc,
        }],
        limit: Some(100),
    };

    // Translate the report
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok(), "Translation failed: {:?}", result.err());

    let query = result.unwrap();

    // Verify translation results
    assert_eq!(query.from, Some("fact_sales".to_string()));

    // Group by: region, month
    assert_eq!(query.group_by.len(), 2);
    assert_eq!(query.group_by[0].entity, "fact_sales");
    assert_eq!(query.group_by[0].field, "region");
    assert_eq!(query.group_by[1].entity, "dates");
    assert_eq!(query.group_by[1].field, "month_start_date");

    // Select: total_revenue (base for YTD), total_quantity
    assert_eq!(query.select.len(), 2);
    assert_eq!(query.select[0].field.field, "total_revenue");
    assert_eq!(query.select[1].field.field, "total_quantity");

    // Derived: YTD revenue
    assert_eq!(query.derived.len(), 1);
    assert_eq!(query.derived[0].alias, "YTD Revenue");

    // Sort: Revenue desc
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(query.order_by[0].field.field, "Revenue");
    assert!(query.order_by[0].descending);

    // Limit: 100
    assert_eq!(query.limit, Some(100));
}

#[test]
fn test_simple_grouped_report() {
    // Simpler test: just group by slicer and show one measure
    let mut slicers = HashMap::new();
    slicers.insert(
        "category".to_string(),
        Slicer::Inline {
            name: "category".to_string(),
            data_type: DataType::String,
        },
    );

    let mut atoms = HashMap::new();
    atoms.insert(
        "sales".to_string(),
        Atom {
            name: "sales".to_string(),
            data_type: AtomType::Decimal,
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "fact_orders".to_string(),
        Table {
            name: "fact_orders".to_string(),
            source: "dbo.fact_orders".to_string(),
            atoms,
            times: HashMap::new(),
            slicers,
        },
    );

    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "total_sales".to_string(),
        Measure {
            name: "total_sales".to_string(),
            expr: SqlExpr {
                sql: "sum(@sales)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "fact_orders".to_string(),
        MeasureBlock {
            table_name: "fact_orders".to_string(),
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
        name: "sales_by_category".to_string(),
        from: vec!["fact_orders".to_string()],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::InlineSlicer {
            name: "category".to_string(),
            label: None,
        }],
        show: vec![ShowItem::Measure {
            name: "total_sales".to_string(),
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.group_by.len(), 1);
    assert_eq!(query.select.len(), 1);
}
