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
