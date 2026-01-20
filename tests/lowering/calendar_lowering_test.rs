use mantis::dsl::ast;
use mantis::dsl::span::{Span, Spanned};
use mantis::lowering;
use mantis::model::CalendarBody;

#[test]
fn test_lower_physical_calendar() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Calendar(ast::Calendar {
                name: Spanned {
                    value: "dates".to_string(),
                    span: Span::default(),
                },
                body: Spanned {
                    value: ast::CalendarBody::Physical(ast::PhysicalCalendar {
                        source: Spanned {
                            value: "dbo.dim_date".to_string(),
                            span: Span::default(),
                        },
                        grain_mappings: vec![
                            Spanned {
                                value: ast::GrainMapping {
                                    level: Spanned {
                                        value: ast::GrainLevel::Day,
                                        span: Span::default(),
                                    },
                                    column: Spanned {
                                        value: "date_key".to_string(),
                                        span: Span::default(),
                                    },
                                },
                                span: Span::default(),
                            },
                            Spanned {
                                value: ast::GrainMapping {
                                    level: Spanned {
                                        value: ast::GrainLevel::Month,
                                        span: Span::default(),
                                    },
                                    column: Spanned {
                                        value: "month_start_date".to_string(),
                                        span: Span::default(),
                                    },
                                },
                                span: Span::default(),
                            },
                        ],
                        drill_paths: vec![],
                        fiscal_year_start: None,
                        week_start: None,
                    }),
                    span: Span::default(),
                },
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.calendars.len(), 1);

    let calendar = model.calendars.get("dates").unwrap();
    assert_eq!(calendar.name, "dates");

    match &calendar.body {
        CalendarBody::Physical(phys) => {
            assert_eq!(phys.source, "dbo.dim_date");
            assert_eq!(phys.grain_mappings.len(), 2);
            assert_eq!(
                phys.grain_mappings
                    .get(&mantis::model::GrainLevel::Day)
                    .unwrap(),
                "date_key"
            );
        }
        _ => panic!("Expected Physical calendar"),
    }
}

#[test]
fn test_lower_generated_calendar() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Calendar(ast::Calendar {
                name: Spanned {
                    value: "auto_dates".to_string(),
                    span: Span::default(),
                },
                body: Spanned {
                    value: ast::CalendarBody::Generated(ast::GeneratedCalendar {
                        base_grain: Spanned {
                            value: ast::GrainLevel::Day,
                            span: Span::default(),
                        },
                        fiscal: None,
                        range: Some(Spanned {
                            value: ast::CalendarRange::Explicit {
                                start: Spanned {
                                    value: ast::DateLiteral::new(2020, 1, 1),
                                    span: Span::default(),
                                },
                                end: Spanned {
                                    value: ast::DateLiteral::new(2025, 12, 31),
                                    span: Span::default(),
                                },
                            },
                            span: Span::default(),
                        }),
                        drill_paths: vec![],
                        week_start: None,
                    }),
                    span: Span::default(),
                },
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let calendar = model.calendars.get("auto_dates").unwrap();

    match &calendar.body {
        CalendarBody::Generated { grain, from, to } => {
            assert_eq!(*grain, mantis::model::GrainLevel::Day);
            assert_eq!(from, "2020-01-01");
            assert_eq!(to, "2025-12-31");
        }
        _ => panic!("Expected Generated calendar"),
    }
}
