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
