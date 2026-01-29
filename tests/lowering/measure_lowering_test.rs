use mantis::dsl::ast;
use mantis::dsl::span::{Span, Spanned};
use mantis::lowering;
use mantis::model::expr::{AggregateFunc, BinaryOp, Expr, Func, Literal};

/// Helper to create a simple aggregate expression like sum(@atom)
fn sum_atom(atom_name: &str) -> Expr {
    Expr::Function {
        func: Func::Aggregate(AggregateFunc::Sum),
        args: vec![Expr::AtomRef(atom_name.to_string())],
    }
}

/// Helper to create a simple comparison expression like column = 'value'
fn column_eq_string(column: &str, value: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Column {
            entity: None,
            column: column.to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::String(value.to_string()))),
    }
}

#[test]
fn test_lower_measure_block() {
    // First, we need a table for the measure block to reference
    let table_ast = ast::Table {
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
                    value: mantis::model::AtomType::Decimal,
                    span: Span::default(),
                },
            },
            span: Span::default(),
        }],
        times: vec![],
        slicers: vec![],
    };

    let measure_ast = ast::MeasureBlock {
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
                    value: sum_atom("revenue"),
                    span: Span::default(),
                },
                filter: None,
                null_handling: None,
            },
            span: Span::default(),
        }],
    };

    let ast = ast::Model {
        defaults: None,
        items: vec![
            Spanned {
                value: ast::Item::Table(table_ast),
                span: Span::default(),
            },
            Spanned {
                value: ast::Item::MeasureBlock(measure_ast),
                span: Span::default(),
            },
        ],
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

    // Check the expression structure
    match &measure.expr {
        Expr::Function { func, args } => {
            assert!(matches!(func, Func::Aggregate(AggregateFunc::Sum)));
            assert_eq!(args.len(), 1);
            assert!(matches!(&args[0], Expr::AtomRef(name) if name == "revenue"));
        }
        _ => panic!("Expected Function expression, got {:?}", measure.expr),
    }

    assert!(measure.filter.is_none());
}

#[test]
fn test_lower_measure_with_filter() {
    // First, we need a table for the measure block to reference
    let table_ast = ast::Table {
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
                    value: "amount".to_string(),
                    span: Span::default(),
                },
                atom_type: Spanned {
                    value: mantis::model::AtomType::Decimal,
                    span: Span::default(),
                },
            },
            span: Span::default(),
        }],
        times: vec![],
        slicers: vec![],
    };

    let measure_ast = ast::MeasureBlock {
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
                    value: sum_atom("amount"),
                    span: Span::default(),
                },
                filter: Some(Spanned {
                    value: column_eq_string("segment", "Enterprise"),
                    span: Span::default(),
                }),
                null_handling: Some(Spanned {
                    value: ast::NullHandling::CoalesceZero,
                    span: Span::default(),
                }),
            },
            span: Span::default(),
        }],
    };

    let ast = ast::Model {
        defaults: None,
        items: vec![
            Spanned {
                value: ast::Item::Table(table_ast),
                span: Span::default(),
            },
            Spanned {
                value: ast::Item::MeasureBlock(measure_ast),
                span: Span::default(),
            },
        ],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let measure_block = model.measures.get("fact_sales").unwrap();
    let measure = measure_block.measures.get("enterprise_revenue").unwrap();

    assert!(measure.filter.is_some());

    // Check the filter expression structure
    let filter = measure.filter.as_ref().unwrap();
    match filter {
        Expr::BinaryOp { left, op, right } => {
            assert!(matches!(op, BinaryOp::Eq));
            assert!(matches!(
                left.as_ref(),
                Expr::Column { entity: None, column } if column == "segment"
            ));
            assert!(matches!(
                right.as_ref(),
                Expr::Literal(Literal::String(s)) if s == "Enterprise"
            ));
        }
        _ => panic!("Expected BinaryOp expression, got {:?}", filter),
    }

    assert_eq!(
        measure.null_handling,
        Some(mantis::model::NullHandling::CoalesceZero)
    );
}
