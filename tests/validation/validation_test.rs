use mantis_core::dsl::span::Span;
use mantis_core::model;
use mantis_core::validation;
use std::collections::HashMap;

#[test]
fn test_validate_empty_model() {
    let model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let result = validation::validate(&model);
    assert!(result.is_ok());
}

#[test]
fn test_detect_undefined_calendar_reference() {
    use mantis_core::model::{GrainLevel, Table, TimeBinding};

    let mut times = HashMap::new();
    times.insert(
        "order_date_id".to_string(),
        TimeBinding {
            name: "order_date_id".to_string(),
            calendar: "nonexistent_calendar".to_string(), // Undefined!
            grain: GrainLevel::Day,
        },
    );

    let table = Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms: HashMap::new(),
        times,
        slicers: HashMap::new(),
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(), // Empty - no calendars defined
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.tables.insert("fact_sales".to_string(), table);

    let result = validation::validate(&model);
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);

    match &errors[0] {
        validation::ValidationError::UndefinedReference { reference_name, .. } => {
            assert_eq!(reference_name, "nonexistent_calendar");
        }
        _ => panic!("Expected UndefinedReference error"),
    }
}

#[test]
fn test_detect_undefined_dimension_reference() {
    use mantis_core::model::{Slicer, Table};

    let mut slicers = HashMap::new();
    slicers.insert(
        "customer_id".to_string(),
        Slicer::ForeignKey {
            name: "customer_id".to_string(),
            dimension: "nonexistent_dimension".to_string(), // Undefined!
            key: "customer_id".to_string(),
        },
    );

    let table = Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms: HashMap::new(),
        times: HashMap::new(),
        slicers,
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(), // Empty - no dimensions defined
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.tables.insert("fact_sales".to_string(), table);

    let result = validation::validate(&model);
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| matches!(
        e,
        validation::ValidationError::UndefinedReference {
            reference_name,
            ..
        } if reference_name == "nonexistent_dimension"
    )));
}

#[test]
fn test_validation_error_display() {
    let error = validation::ValidationError::UndefinedReference {
        entity_type: "Table".to_string(),
        entity_name: "fact_sales".to_string(),
        reference_type: "calendar".to_string(),
        reference_name: "dates".to_string(),
    };

    let message = error.to_string();
    assert!(message.contains("fact_sales"));
    assert!(message.contains("dates"));
    assert!(message.contains("calendar"));
}

#[test]
fn test_detect_circular_measure_dependency() {
    use mantis_core::model::{Measure, MeasureBlock, SqlExpr};

    let mut measures = HashMap::new();

    // Create circular dependency: a -> b -> a
    measures.insert(
        "a".to_string(),
        Measure {
            name: "a".to_string(),
            expr: SqlExpr {
                sql: "b + 1".to_string(), // References 'b'
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "b".to_string(),
        Measure {
            name: "b".to_string(),
            expr: SqlExpr {
                sql: "a * 2".to_string(), // References 'a'
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    let measure_block = MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures,
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model
        .measures
        .insert("fact_sales".to_string(), measure_block);

    let result = validation::validate(&model);
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    assert!(matches!(
        &errors[0],
        validation::ValidationError::CircularDependency { .. }
    ));
}

#[test]
fn test_no_circular_dependency_linear_chain() {
    use mantis_core::model::{Measure, MeasureBlock, SqlExpr};

    let mut measures = HashMap::new();

    // Create linear chain: a -> b -> c (no cycle)
    measures.insert(
        "a".to_string(),
        Measure {
            name: "a".to_string(),
            expr: SqlExpr {
                sql: "b + 1".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "b".to_string(),
        Measure {
            name: "b".to_string(),
            expr: SqlExpr {
                sql: "c * 2".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "c".to_string(),
        Measure {
            name: "c".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(), // Base measure
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );

    let measure_block = MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures,
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model
        .measures
        .insert("fact_sales".to_string(), measure_block);

    let result = validation::validate(&model);
    assert!(result.is_ok());
}

#[test]
fn test_detect_invalid_dimension_drill_path() {
    use mantis_core::model::{Attribute, DataType, Dimension, DimensionDrillPath};

    let mut attributes = HashMap::new();
    attributes.insert(
        "city".to_string(),
        Attribute {
            name: "city".to_string(),
            data_type: DataType::String,
        },
    );
    attributes.insert(
        "state".to_string(),
        Attribute {
            name: "state".to_string(),
            data_type: DataType::String,
        },
    );

    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "geographic".to_string(),
        DimensionDrillPath {
            name: "geographic".to_string(),
            levels: vec![
                "city".to_string(),
                "state".to_string(),
                "nonexistent_attribute".to_string(), // Undefined!
            ],
        },
    );

    let dimension = Dimension {
        name: "geography".to_string(),
        source: "dbo.dim_geo".to_string(),
        key: "geo_id".to_string(),
        attributes,
        drill_paths,
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.dimensions.insert("geography".to_string(), dimension);

    let result = validation::validate(&model);
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert!(errors
        .iter()
        .any(|e| matches!(e, validation::ValidationError::InvalidDrillPath { .. })));
}

#[test]
fn test_valid_dimension_drill_path() {
    use mantis_core::model::{Attribute, DataType, Dimension, DimensionDrillPath};

    let mut attributes = HashMap::new();
    attributes.insert(
        "city".to_string(),
        Attribute {
            name: "city".to_string(),
            data_type: DataType::String,
        },
    );
    attributes.insert(
        "state".to_string(),
        Attribute {
            name: "state".to_string(),
            data_type: DataType::String,
        },
    );
    attributes.insert(
        "country".to_string(),
        Attribute {
            name: "country".to_string(),
            data_type: DataType::String,
        },
    );

    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "geographic".to_string(),
        DimensionDrillPath {
            name: "geographic".to_string(),
            levels: vec![
                "city".to_string(),
                "state".to_string(),
                "country".to_string(), // All valid!
            ],
        },
    );

    let dimension = Dimension {
        name: "geography".to_string(),
        source: "dbo.dim_geo".to_string(),
        key: "geo_id".to_string(),
        attributes,
        drill_paths,
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.dimensions.insert("geography".to_string(), dimension);

    let result = validation::validate(&model);
    assert!(result.is_ok());
}
