use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;
use mantis_core::model::NullHandling;

#[test]
fn test_lower_defaults() {
    let ast = ast::Model {
        defaults: Some(Spanned {
            value: ast::Defaults {
                settings: vec![
                    Spanned {
                        value: ast::DefaultSetting::Calendar(Spanned {
                            value: "dates".to_string(),
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::DefaultSetting::FiscalYearStart(Spanned {
                            value: ast::Month::April,
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::DefaultSetting::NullHandling(Spanned {
                            value: ast::NullHandling::CoalesceZero,
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::DefaultSetting::DecimalPlaces(Spanned {
                            value: 3,
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                ],
            },
            span: Span::default(),
        }),
        items: vec![],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert!(model.defaults.is_some());

    let defaults = model.defaults.unwrap();
    assert_eq!(defaults.calendar, Some("dates".to_string()));
    assert_eq!(defaults.fiscal_year_start, Some(ast::Month::April));
    assert_eq!(defaults.null_handling, NullHandling::CoalesceZero);
    assert_eq!(defaults.decimal_places, 3);
}

#[test]
fn test_lower_empty_defaults() {
    let ast = ast::Model {
        defaults: Some(Spanned {
            value: ast::Defaults { settings: vec![] },
            span: Span::default(),
        }),
        items: vec![],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert!(model.defaults.is_some());

    let defaults = model.defaults.unwrap();
    // Should use Default trait values
    assert_eq!(defaults.calendar, None);
    assert_eq!(defaults.fiscal_year_start, None);
    assert_eq!(defaults.null_handling, NullHandling::NullOnZero);
    assert_eq!(defaults.decimal_places, 2);
}
