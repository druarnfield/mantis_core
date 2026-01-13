//! textDocument/definition handler
//!
//! Provides go-to-definition for entity references in Mantis DSL.
//! Allows jumping from entity references (like `:from("orders")`) to
//! their definitions (like `source("orders")`).

use tower_lsp::lsp_types::{GotoDefinitionResponse, Location, Position};

use crate::lsp::analysis::document::DocumentState;
use crate::lsp::project::ProjectState;

/// Get definition location for symbol at position.
pub fn get_definition(
    project: &ProjectState,
    doc: &DocumentState,
    position: Position,
) -> Option<GotoDefinitionResponse> {
    // Find what entity is referenced at the cursor
    let entity_name = find_entity_reference(doc, position)?;

    // Look up the entity in the project
    let (uri, entity) = project.find_entity(&entity_name)?;

    Some(GotoDefinitionResponse::Scalar(Location {
        uri,
        range: entity.range,
    }))
}

/// Find entity name referenced at position.
///
/// This looks for string literals that appear in contexts where they
/// reference entities (e.g., `:from("orders")`, `:source("orders")`,
/// `:include("customers")`).
fn find_entity_reference(doc: &DocumentState, position: Position) -> Option<String> {
    let node = doc.node_at_position(position)?;

    // Walk up the tree to find a string node
    let mut current = Some(node);
    while let Some(n) = current {
        if n.kind() == "string" || n.kind() == "string_content" {
            let text = &doc.source[n.start_byte()..n.end_byte()];
            let content = text.trim_matches('"').trim_matches('\'');

            // Check if parent context suggests this is an entity reference
            if is_entity_reference_context(n, &doc.source) {
                return Some(content.to_string());
            }
        }
        current = n.parent();
    }

    None
}

/// Check if a string node is in a context where it references an entity.
///
/// Entity references occur in method calls like:
/// - `:from("entity_name")` - source reference in facts/dimensions
/// - `:source("entity_name")` - source reference
/// - `:include("entity_name")` - dimension includes
///
/// We need to verify the string is a DIRECT argument to a method call,
/// not just anywhere in the AST ancestry.
fn is_entity_reference_context(node: tree_sitter::Node, source: &str) -> bool {
    // Walk up to find the containing function_call
    // The path should be: string -> arguments -> function_call
    let mut current = node.parent();

    // Skip string_content to get to string if necessary
    if let Some(n) = current {
        if n.kind() == "string" {
            current = n.parent();
        }
    }

    // Should now be at arguments
    let arguments = match current {
        Some(n) if n.kind() == "arguments" => n,
        _ => return false,
    };

    // Parent of arguments should be function_call
    let function_call = match arguments.parent() {
        Some(n) if n.kind() == "function_call" => n,
        _ => return false,
    };

    // Check if this function_call is a method call (first child is method_index_expression)
    if let Some(first) = function_call.child(0) {
        if first.kind() == "method_index_expression" {
            if let Some(method_name) = get_method_name(first, source) {
                return matches!(method_name.as_str(), "from" | "source" | "include");
            }
        }
    }

    false
}

/// Get method name from method_index_expression.
///
/// A method_index_expression has the form: `<object>:<identifier>`
/// We want to extract the identifier part (the method name).
fn get_method_name(method_expr: tree_sitter::Node, source: &str) -> Option<String> {
    let mut cursor = method_expr.walk();
    let mut last_identifier: Option<tree_sitter::Node> = None;

    for child in method_expr.children(&mut cursor) {
        if child.kind() == "identifier" {
            last_identifier = Some(child);
        }
    }

    last_identifier.map(|n| source[n.start_byte()..n.end_byte()].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tower_lsp::lsp_types::Url;

    #[test]
    fn test_find_entity_reference_in_from() {
        let source = r#"fact("sales"):from("orders"):measures({})"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "orders" string (character 21 is inside the string)
        // fact("sales"):from("orders")
        // 0         1         2
        // 0123456789012345678901234567
        let position = Position {
            line: 0,
            character: 22,
        };
        let result = find_entity_reference(&doc, position);

        assert_eq!(result, Some("orders".to_string()));
    }

    #[test]
    fn test_find_entity_reference_in_source() {
        let source = r#"dimension("customers"):source("users"):columns({})"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "users" string
        // dimension("customers"):source("users")
        // 0         1         2         3
        // 0123456789012345678901234567890123456
        let position = Position {
            line: 0,
            character: 33,
        };
        let result = find_entity_reference(&doc, position);

        assert_eq!(result, Some("users".to_string()));
    }

    #[test]
    fn test_find_entity_reference_in_include() {
        let source = r#"fact("sales"):include("products"):measures({})"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "products" string
        let position = Position {
            line: 0,
            character: 25,
        };
        let result = find_entity_reference(&doc, position);

        assert_eq!(result, Some("products".to_string()));
    }

    #[test]
    fn test_no_entity_reference_in_entity_definition() {
        let source = r#"source("orders"):from("raw.orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "orders" in source("orders") - this is a definition, not a reference
        let position = Position {
            line: 0,
            character: 10,
        };
        let result = find_entity_reference(&doc, position);

        // Should be None because source("orders") is a definition, not a reference
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_definition_returns_location() {
        let project = ProjectState::new(PathBuf::from("/test"));

        // Add a source entity
        let source_uri = Url::parse("file:///test/sources.lua").unwrap();
        project.update_document(
            source_uri.clone(),
            1,
            r#"source("orders"):from("raw.orders"):columns({ id = pk(int64) })"#.to_string(),
        );

        // Add a fact that references the source
        let fact_uri = Url::parse("file:///test/facts.lua").unwrap();
        let fact_source = r#"fact("sales"):from("orders"):measures({})"#;
        project.update_document(fact_uri.clone(), 1, fact_source.to_string());

        let doc = project.get_document(&fact_uri).unwrap();

        // Position inside "orders" in :from("orders")
        let position = Position {
            line: 0,
            character: 22,
        };

        let result = get_definition(&project, &doc, position);

        assert!(result.is_some());
        if let Some(GotoDefinitionResponse::Scalar(location)) = result {
            assert_eq!(location.uri, source_uri);
            // The range should point to the source definition
            assert_eq!(location.range.start.line, 0);
        } else {
            panic!("Expected Scalar response");
        }
    }

    #[test]
    fn test_get_definition_returns_none_for_unknown_entity() {
        let project = ProjectState::new(PathBuf::from("/test"));

        let uri = Url::parse("file:///test/facts.lua").unwrap();
        let source = r#"fact("sales"):from("nonexistent"):measures({})"#;
        project.update_document(uri.clone(), 1, source.to_string());

        let doc = project.get_document(&uri).unwrap();

        let position = Position {
            line: 0,
            character: 22,
        };

        let result = get_definition(&project, &doc, position);

        assert!(result.is_none());
    }

    #[test]
    fn test_get_definition_returns_none_outside_reference() {
        let project = ProjectState::new(PathBuf::from("/test"));

        let uri = Url::parse("file:///test/sources.lua").unwrap();
        let source = r#"source("orders"):from("raw.orders")"#;
        project.update_document(uri.clone(), 1, source.to_string());

        let doc = project.get_document(&uri).unwrap();

        // Position in whitespace or non-reference context
        let position = Position {
            line: 0,
            character: 0,
        };

        let result = get_definition(&project, &doc, position);

        assert!(result.is_none());
    }
}
