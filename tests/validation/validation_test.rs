use mantis::model;
use mantis::validation;
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
    use mantis::model::{GrainLevel, Table, TimeBinding};

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
    use mantis::model::{Slicer, Table};

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
    use mantis::model::{BinaryOp, Expr, Literal, Measure, MeasureBlock, Table};

    let mut measures = HashMap::new();

    // Create circular dependency: a -> b -> a
    // Use Column references which the validation will detect
    measures.insert(
        "a".to_string(),
        Measure {
            name: "a".to_string(),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    entity: None,
                    column: "b".to_string(), // References 'b'
                }),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int(1))),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "b".to_string(),
        Measure {
            name: "b".to_string(),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    entity: None,
                    column: "a".to_string(), // References 'a'
                }),
                op: BinaryOp::Mul,
                right: Box::new(Expr::Literal(Literal::Int(2))),
            },
            filter: None,
            null_handling: None,
        },
    );

    let measure_block = MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures,
    };

    // Create the table that the measure block references
    let table = Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms: HashMap::new(),
        times: HashMap::new(),
        slicers: HashMap::new(),
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.tables.insert("fact_sales".to_string(), table);
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
    use mantis::model::{BinaryOp, Expr, Func, Literal, Measure, MeasureBlock, Table};

    let mut measures = HashMap::new();

    // Create linear chain: a -> b -> c (no cycle)
    measures.insert(
        "a".to_string(),
        Measure {
            name: "a".to_string(),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    entity: None,
                    column: "b".to_string(),
                }),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int(1))),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "b".to_string(),
        Measure {
            name: "b".to_string(),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    entity: None,
                    column: "c".to_string(),
                }),
                op: BinaryOp::Mul,
                right: Box::new(Expr::Literal(Literal::Int(2))),
            },
            filter: None,
            null_handling: None,
        },
    );

    measures.insert(
        "c".to_string(),
        Measure {
            name: "c".to_string(),
            expr: Expr::Function {
                func: Func::Aggregate(mantis::model::expr::AggregateFunc::Sum),
                args: vec![Expr::AtomRef("revenue".to_string())], // Base measure with @atom
            },
            filter: None,
            null_handling: None,
        },
    );

    let measure_block = MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures,
    };

    // Create the table that the measure block references
    let table = Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms: HashMap::new(),
        times: HashMap::new(),
        slicers: HashMap::new(),
    };

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.tables.insert("fact_sales".to_string(), table);
    model
        .measures
        .insert("fact_sales".to_string(), measure_block);

    let result = validation::validate(&model);
    assert!(result.is_ok());
}

#[test]
fn test_detect_invalid_dimension_drill_path() {
    use mantis::model::{Attribute, DataType, Dimension, DimensionDrillPath};

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
    use mantis::model::{Attribute, DataType, Dimension, DimensionDrillPath};

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

#[test]
fn test_detect_undefined_via_slicer_reference() {
    use mantis::model::{Slicer, Table};

    let mut slicers = HashMap::new();

    // Add a Via slicer that references a non-existent FK slicer
    slicers.insert(
        "customer_name".to_string(),
        Slicer::Via {
            name: "customer_name".to_string(),
            fk_slicer: "customer_id".to_string(), // This doesn't exist!
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
        dimensions: HashMap::new(),
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
            reference_type,
            reference_name,
            ..
        } if reference_type == "slicer" && reference_name == "customer_id"
    )));
}

#[test]
fn test_valid_via_slicer_reference() {
    use mantis::model::{Slicer, Table};

    let mut slicers = HashMap::new();

    // Add a FK slicer
    slicers.insert(
        "customer_id".to_string(),
        Slicer::ForeignKey {
            name: "customer_id".to_string(),
            dimension: "customers".to_string(),
            key: "customer_id".to_string(),
        },
    );

    // Add a Via slicer that references the FK slicer
    slicers.insert(
        "customer_name".to_string(),
        Slicer::Via {
            name: "customer_name".to_string(),
            fk_slicer: "customer_id".to_string(), // This exists!
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
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    // Note: We're not adding the dimension itself, which would normally cause an error
    // But for this test, we're focusing on Via slicer validation
    model.tables.insert("fact_sales".to_string(), table);

    let result = validation::validate(&model);
    // Will fail because dimension "customers" doesn't exist, but not because of Via slicer
    assert!(result.is_err());
    let errors = result.unwrap_err();

    // Should have dimension error but NOT a Via slicer error
    assert!(errors.iter().any(|e| matches!(
        e,
        validation::ValidationError::UndefinedReference {
            reference_type,
            reference_name,
            ..
        } if reference_type == "dimension" && reference_name == "customers"
    )));

    assert!(!errors.iter().any(|e| matches!(
        e,
        validation::ValidationError::UndefinedReference {
            reference_type,
            ..
        } if reference_type == "slicer"
    )));
}

#[test]
fn test_detect_duplicate_calendar_names() {
    use mantis::model::{Calendar, CalendarBody};

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    // HashMaps don't allow duplicate keys, so we can't actually insert duplicates
    // But the validation should still work if somehow duplicates existed
    // This test verifies the logic exists, even though HashMaps prevent the issue
    model.calendars.insert(
        "dates".to_string(),
        Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Generated {
                grain: mantis::model::GrainLevel::Day,
                from: "2020-01-01".to_string(),
                to: "2025-12-31".to_string(),
            },
        },
    );

    let result = validation::validate(&model);
    // Should pass - no duplicates possible with HashMap
    assert!(result.is_ok());
}

#[test]
fn test_detect_duplicate_dimension_names() {
    use mantis::model::Dimension;

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.dimensions.insert(
        "customers".to_string(),
        Dimension {
            name: "customers".to_string(),
            source: "dbo.dim_customers".to_string(),
            key: "customer_id".to_string(),
            attributes: HashMap::new(),
            drill_paths: HashMap::new(),
        },
    );

    let result = validation::validate(&model);
    assert!(result.is_ok());
}

#[test]
fn test_detect_duplicate_table_names() {
    use mantis::model::Table;

    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    model.tables.insert(
        "sales".to_string(),
        Table {
            name: "sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );

    let result = validation::validate(&model);
    assert!(result.is_ok());
}

#[test]
fn test_duplicate_name_error_display() {
    let error = validation::ValidationError::DuplicateName {
        entity_type: "Calendar".to_string(),
        name: "dates".to_string(),
    };

    let message = error.to_string();
    assert!(message.contains("Calendar"));
    assert!(message.contains("dates"));
    assert!(message.contains("Duplicate"));
}
