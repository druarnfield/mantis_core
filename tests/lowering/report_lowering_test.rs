use mantis::dsl::ast;
use mantis::dsl::span::{Span, Spanned};
use mantis::lowering;
use mantis::model::{GroupItem, ShowItem, TimeSuffix};

#[test]
fn test_lower_report() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Report(ast::Report {
                name: Spanned {
                    value: "monthly_sales".to_string(),
                    span: Span::default(),
                },
                from: vec![Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                }],
                use_date: vec![Spanned {
                    value: "order_date_id".to_string(),
                    span: Span::default(),
                }],
                period: None,
                group: vec![Spanned {
                    value: ast::GroupItem::DrillPathRef(ast::DrillPathRef {
                        source: "dates".to_string(),
                        path: "standard".to_string(),
                        level: "month".to_string(),
                        label: None,
                    }),
                    span: Span::default(),
                }],
                show: vec![Spanned {
                    value: ast::ShowItem::Measure {
                        name: "total_revenue".to_string(),
                        label: None,
                    },
                    span: Span::default(),
                }],
                filter: None,
                sort: vec![],
                limit: None,
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.reports.len(), 1);

    let report = model.reports.get("monthly_sales").unwrap();
    assert_eq!(report.name, "monthly_sales");
    assert_eq!(report.from, vec!["fact_sales"]);
    assert_eq!(report.use_date, vec!["order_date_id"]);
    assert_eq!(report.group.len(), 1);
    assert_eq!(report.show.len(), 1);

    match &report.group[0] {
        GroupItem::DrillPathRef {
            source,
            path,
            level,
            ..
        } => {
            assert_eq!(source, "dates");
            assert_eq!(path, "standard");
            assert_eq!(level, "month");
        }
        _ => panic!("Expected DrillPathRef"),
    }
}

#[test]
fn test_lower_report_with_time_suffix() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Report(ast::Report {
                name: Spanned {
                    value: "ytd_report".to_string(),
                    span: Span::default(),
                },
                from: vec![Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                }],
                use_date: vec![Spanned {
                    value: "order_date_id".to_string(),
                    span: Span::default(),
                }],
                period: None,
                group: vec![],
                show: vec![Spanned {
                    value: ast::ShowItem::MeasureWithSuffix {
                        name: "revenue".to_string(),
                        suffix: ast::TimeSuffix::Ytd,
                        label: Some("YTD Revenue".to_string()),
                    },
                    span: Span::default(),
                }],
                filter: None,
                sort: vec![],
                limit: None,
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let report = model.reports.get("ytd_report").unwrap();

    match &report.show[0] {
        ShowItem::MeasureWithSuffix {
            name,
            suffix,
            label,
        } => {
            assert_eq!(name, "revenue");
            assert_eq!(*suffix, TimeSuffix::Ytd);
            assert_eq!(label.as_deref(), Some("YTD Revenue"));
        }
        _ => panic!("Expected MeasureWithSuffix"),
    }
}
