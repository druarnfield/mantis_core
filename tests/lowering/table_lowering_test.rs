use mantis::dsl::ast;
use mantis::dsl::span::{Span, Spanned};
use mantis::lowering;
use mantis::model::expr::{Expr, Func, ScalarFunc};
use mantis::model::{Slicer, Table};

#[test]
fn test_lower_table_with_atoms() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Table(ast::Table {
                name: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.fact_sales".to_string(),
                    span: Span::default(),
                },
                atoms: vec![Spanned {
                    value: ast::Atom {
                        name: Spanned {
                            value: "revenue".to_string(),
                            span: Span::default(),
                        },
                        atom_type: Spanned {
                            value: ast::AtomType::Decimal,
                            span: Span::default(),
                        },
                    },
                    span: Span::default(),
                }],
                times: vec![],
                slicers: vec![],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.tables.len(), 1);

    let table = model.tables.get("fact_sales").unwrap();
    assert_eq!(table.name, "fact_sales");
    assert_eq!(table.source, "dbo.fact_sales");
    assert_eq!(table.atoms.len(), 1);

    let atom = table.atoms.get("revenue").unwrap();
    assert_eq!(atom.name, "revenue");
    assert_eq!(atom.data_type, mantis::model::AtomType::Decimal);
}

#[test]
fn test_lower_table_with_all_components() {
    // Create a YEAR(order_date) function call expression
    let year_expr = Expr::Function {
        func: Func::Scalar(ScalarFunc::Year),
        args: vec![Expr::Column {
            entity: None,
            column: "order_date".to_string(),
        }],
    };

    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Table(ast::Table {
                name: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.fact_sales".to_string(),
                    span: Span::default(),
                },
                atoms: vec![],
                times: vec![Spanned {
                    value: ast::TimeBinding {
                        name: Spanned {
                            value: "order_date_id".to_string(),
                            span: Span::default(),
                        },
                        calendar: Spanned {
                            value: "dates".to_string(),
                            span: Span::default(),
                        },
                        grain: Spanned {
                            value: ast::GrainLevel::Day,
                            span: Span::default(),
                        },
                    },
                    span: Span::default(),
                }],
                slicers: vec![
                    Spanned {
                        value: ast::Slicer {
                            name: Spanned {
                                value: "customer_id".to_string(),
                                span: Span::default(),
                            },
                            kind: Spanned {
                                value: ast::SlicerKind::ForeignKey {
                                    dimension: "customers".to_string(),
                                    key_column: "customer_id".to_string(),
                                },
                                span: Span::default(),
                            },
                        },
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::Slicer {
                            name: Spanned {
                                value: "segment".to_string(),
                                span: Span::default(),
                            },
                            kind: Spanned {
                                value: ast::SlicerKind::Inline {
                                    data_type: ast::DataType::String,
                                },
                                span: Span::default(),
                            },
                        },
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::Slicer {
                            name: Spanned {
                                value: "region".to_string(),
                                span: Span::default(),
                            },
                            kind: Spanned {
                                value: ast::SlicerKind::Via {
                                    fk_slicer: "customer_id".to_string(),
                                },
                                span: Span::default(),
                            },
                        },
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::Slicer {
                            name: Spanned {
                                value: "order_year".to_string(),
                                span: Span::default(),
                            },
                            kind: Spanned {
                                value: ast::SlicerKind::Calculated {
                                    data_type: ast::DataType::Int,
                                    expr: year_expr,
                                },
                                span: Span::default(),
                            },
                        },
                        span: Span::default(),
                    },
                ],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let table = model.tables.get("fact_sales").unwrap();

    assert_eq!(table.times.len(), 1);
    let time = table.times.get("order_date_id").unwrap();
    assert_eq!(time.calendar, "dates");

    assert_eq!(table.slicers.len(), 4);
    assert!(matches!(
        table.slicers.get("customer_id"),
        Some(Slicer::ForeignKey { .. })
    ));
    assert!(matches!(
        table.slicers.get("segment"),
        Some(Slicer::Inline { .. })
    ));
    assert!(matches!(
        table.slicers.get("region"),
        Some(Slicer::Via { .. })
    ));
    assert!(matches!(
        table.slicers.get("order_year"),
        Some(Slicer::Calculated { .. })
    ));
}
