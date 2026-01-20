use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;

#[test]
fn test_lower_measure_block() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::MeasureBlock(ast::MeasureBlock {
                table: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                measures: vec![Spanned {
                    value: ast::Measure {
                        name: Spanned {
                            value: "total_revenue".to_string(),
                            span: Span::default(),
                        },
                        expr: Spanned {
                            value: ast::SqlExpr::new("sum(@revenue)".to_string(), Span::default()),
                            span: Span::default(),
                        },
                        filter: None,
                        null_handling: None,
                    },
                    span: Span::default(),
                }],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.measures.len(), 1);

    let measure_block = model.measures.get("fact_sales").unwrap();
    assert_eq!(measure_block.table_name, "fact_sales");
    assert_eq!(measure_block.measures.len(), 1);

    let measure = measure_block.measures.get("total_revenue").unwrap();
    assert_eq!(measure.name, "total_revenue");
    assert_eq!(measure.expr.sql, "sum(@revenue)");
    assert!(measure.filter.is_none());
}

#[test]
fn test_lower_measure_with_filter() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::MeasureBlock(ast::MeasureBlock {
                table: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                measures: vec![Spanned {
                    value: ast::Measure {
                        name: Spanned {
                            value: "enterprise_revenue".to_string(),
                            span: Span::default(),
                        },
                        expr: Spanned {
                            value: ast::SqlExpr::new("sum(@amount)".to_string(), Span::default()),
                            span: Span::default(),
                        },
                        filter: Some(Spanned {
                            value: ast::SqlExpr::new(
                                "segment = 'Enterprise'".to_string(),
                                Span::default(),
                            ),
                            span: Span::default(),
                        }),
                        null_handling: Some(Spanned {
                            value: ast::NullHandling::CoalesceZero,
                            span: Span::default(),
                        }),
                    },
                    span: Span::default(),
                }],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let measure_block = model.measures.get("fact_sales").unwrap();
    let measure = measure_block.measures.get("enterprise_revenue").unwrap();

    assert!(measure.filter.is_some());
    assert_eq!(
        measure.filter.as_ref().unwrap().sql,
        "segment = 'Enterprise'"
    );
    assert_eq!(
        measure.null_handling,
        Some(mantis_core::model::NullHandling::CoalesceZero)
    );
}
