use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;

#[test]
fn test_lower_dimension() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Dimension(ast::Dimension {
                name: Spanned {
                    value: "customers".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.dim_customers".to_string(),
                    span: Span::default(),
                },
                key: Spanned {
                    value: "customer_id".to_string(),
                    span: Span::default(),
                },
                attributes: vec![
                    ast::Attribute {
                        name: Spanned {
                            value: "customer_name".to_string(),
                            span: Span::default(),
                        },
                        data_type: Spanned {
                            value: ast::DataType::String,
                            span: Span::default(),
                        },
                    },
                    ast::Attribute {
                        name: Spanned {
                            value: "region".to_string(),
                            span: Span::default(),
                        },
                        data_type: Spanned {
                            value: ast::DataType::String,
                            span: Span::default(),
                        },
                    },
                ],
                drill_paths: vec![],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.dimensions.len(), 1);

    let dimension = model.dimensions.get("customers").unwrap();
    assert_eq!(dimension.name, "customers");
    assert_eq!(dimension.source, "dbo.dim_customers");
    assert_eq!(dimension.key, "customer_id");
    assert_eq!(dimension.attributes.len(), 2);
    assert!(dimension.attributes.contains_key("customer_name"));
    assert!(dimension.attributes.contains_key("region"));
}

#[test]
fn test_lower_dimension_with_drill_paths() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Dimension(ast::Dimension {
                name: Spanned {
                    value: "geography".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.dim_geo".to_string(),
                    span: Span::default(),
                },
                key: Spanned {
                    value: "geo_id".to_string(),
                    span: Span::default(),
                },
                attributes: vec![],
                drill_paths: vec![ast::DimensionDrillPath {
                    name: Spanned {
                        value: "geographic".to_string(),
                        span: Span::default(),
                    },
                    levels: vec![
                        Spanned {
                            value: "city".to_string(),
                            span: Span::default(),
                        },
                        Spanned {
                            value: "state".to_string(),
                            span: Span::default(),
                        },
                        Spanned {
                            value: "country".to_string(),
                            span: Span::default(),
                        },
                    ],
                }],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let dimension = model.dimensions.get("geography").unwrap();
    assert_eq!(dimension.drill_paths.len(), 1);

    let drill_path = dimension.drill_paths.get("geographic").unwrap();
    assert_eq!(drill_path.levels, vec!["city", "state", "country"]);
}
