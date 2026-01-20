//! Integration tests for lowering DSL AST to semantic model.
//!
//! These tests verify the lowering infrastructure.

use mantis::dsl::ast;
use mantis::lowering;

#[test]
fn test_lower_empty_model() {
    let ast = ast::Model {
        defaults: None,
        items: vec![],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert!(model.calendars.is_empty());
    assert!(model.tables.is_empty());
    assert!(model.measures.is_empty());
}
