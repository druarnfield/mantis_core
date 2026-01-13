//! textDocument/references handler
//!
//! Finds all references to an entity across all open documents.
//! This includes both the entity definition and all places where
//! the entity is referenced (e.g., in `:from()`, `:source()`, `:include()` calls).

use tower_lsp::lsp_types::{Location, Position, Range};

use crate::lsp::analysis::document::DocumentState;
use crate::lsp::project::ProjectState;

/// Find all references to an entity.
pub fn find_references(
    project: &ProjectState,
    doc: &DocumentState,
    position: Position,
    include_declaration: bool,
) -> Vec<Location> {
    let mut locations = Vec::new();

    // Find entity name at cursor
    let Some(entity_name) = find_entity_at_position(doc, position) else {
        return locations;
    };

    // Include declaration if requested
    if include_declaration {
        if let Some((uri, entity)) = project.find_entity(&entity_name) {
            locations.push(Location {
                uri,
                range: entity.range,
            });
        }
    }

    // Scan all documents for references
    for entry in project.documents.iter() {
        let doc_uri = entry.key().clone();
        let doc_source = &entry.value().source;

        // Find string occurrences of the entity name
        let refs = find_string_references(doc_source, &entity_name);
        for range in refs {
            locations.push(Location {
                uri: doc_uri.clone(),
                range,
            });
        }
    }

    locations
}

/// Find entity name at position (from definition or reference).
fn find_entity_at_position(doc: &DocumentState, position: Position) -> Option<String> {
    let node = doc.node_at_position(position)?;

    let mut current = Some(node);
    while let Some(n) = current {
        if n.kind() == "string" || n.kind() == "string_content" {
            let text = &doc.source[n.start_byte()..n.end_byte()];
            return Some(text.trim_matches('"').trim_matches('\'').to_string());
        }
        current = n.parent();
    }

    None
}

/// Find all string references to an entity name in source.
fn find_string_references(source: &str, entity_name: &str) -> Vec<Range> {
    let mut ranges = Vec::new();
    let search = format!("\"{}\"", entity_name);

    let mut line: u32 = 0;
    let mut col: u32 = 0;
    let mut pos = 0;

    while let Some(found) = source[pos..].find(&search) {
        let abs_pos = pos + found;

        // Calculate line/col by scanning from last position
        for ch in source[pos..abs_pos].chars() {
            if ch == '\n' {
                line += 1;
                col = 0;
            } else {
                col += 1;
            }
        }

        let start_col = col + 1; // Skip opening quote
        let end_col = start_col + entity_name.len() as u32;

        ranges.push(Range {
            start: Position {
                line,
                character: start_col,
            },
            end: Position {
                line,
                character: end_col,
            },
        });

        pos = abs_pos + search.len();
        col += search.len() as u32;
    }

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tower_lsp::lsp_types::Url;

    #[test]
    fn test_find_string_references() {
        let source = r#"
source("orders"):from("raw.orders")
fact("sales"):from("orders")
"#;
        let refs = find_string_references(source, "orders");
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn test_find_string_references_positions() {
        let source = r#"source("test"):from("test")"#;
        let refs = find_string_references(source, "test");
        assert_eq!(refs.len(), 2);
        // First "test" starts at column 8 (after `source("`)
        assert_eq!(refs[0].start.character, 8);
        // Second "test" starts at column 21 (after `:from("`)
        assert_eq!(refs[1].start.character, 21);
    }

    #[test]
    fn test_find_entity_at_position() {
        let source = r#"fact("sales"):from("orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "orders" string (character 22 is inside)
        let position = Position {
            line: 0,
            character: 22,
        };
        let result = find_entity_at_position(&doc, position);
        assert_eq!(result, Some("orders".to_string()));
    }

    #[test]
    fn test_find_entity_at_position_in_definition() {
        let source = r#"source("orders"):from("raw.orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "orders" in source("orders")
        let position = Position {
            line: 0,
            character: 10,
        };
        let result = find_entity_at_position(&doc, position);
        assert_eq!(result, Some("orders".to_string()));
    }

    #[test]
    fn test_find_references_includes_declaration() {
        let project = ProjectState::new(PathBuf::from("/test"));

        let uri = Url::parse("file:///test/model.lua").unwrap();
        let source = r#"source("orders"):from("raw.orders")
fact("sales"):from("orders")"#;
        project.update_document(uri.clone(), 1, source.to_string());

        let doc = project.get_document(&uri).unwrap();

        // Position inside "orders" in source("orders")
        let position = Position {
            line: 0,
            character: 10,
        };

        let refs = find_references(&project, &doc, position, true);

        // Should find: declaration + 2 string references (source and fact)
        assert!(refs.len() >= 2);
    }

    #[test]
    fn test_find_references_excludes_declaration() {
        let project = ProjectState::new(PathBuf::from("/test"));

        let uri = Url::parse("file:///test/model.lua").unwrap();
        let source = r#"source("orders"):from("raw.orders")
fact("sales"):from("orders")"#;
        project.update_document(uri.clone(), 1, source.to_string());

        let doc = project.get_document(&uri).unwrap();

        // Position inside "orders" in source("orders")
        let position = Position {
            line: 0,
            character: 10,
        };

        let refs_with_decl = find_references(&project, &doc, position, true);
        let refs_without_decl = find_references(&project, &doc, position, false);

        // Without declaration should have one fewer result
        assert_eq!(refs_with_decl.len(), refs_without_decl.len() + 1);
    }

    #[test]
    fn test_find_references_across_documents() {
        let project = ProjectState::new(PathBuf::from("/test"));

        // First document with source definition
        let uri1 = Url::parse("file:///test/sources.lua").unwrap();
        project.update_document(
            uri1.clone(),
            1,
            r#"source("orders"):from("raw.orders")"#.to_string(),
        );

        // Second document that references the source
        let uri2 = Url::parse("file:///test/facts.lua").unwrap();
        project.update_document(
            uri2.clone(),
            1,
            r#"fact("sales"):from("orders")"#.to_string(),
        );

        let doc = project.get_document(&uri1).unwrap();

        // Position inside "orders" in source("orders")
        let position = Position {
            line: 0,
            character: 10,
        };

        let refs = find_references(&project, &doc, position, true);

        // Should find references in both documents
        let uris: Vec<_> = refs.iter().map(|r| r.uri.clone()).collect();
        assert!(uris.contains(&uri1));
        assert!(uris.contains(&uri2));
    }

    #[test]
    fn test_find_references_multiline() {
        let source = r#"source("orders"):from("raw.orders")
dimension("customers"):source("orders")
fact("sales"):from("orders")"#;

        let refs = find_string_references(source, "orders");
        assert_eq!(refs.len(), 3);

        // Check line numbers
        assert_eq!(refs[0].start.line, 0);
        assert_eq!(refs[1].start.line, 1);
        assert_eq!(refs[2].start.line, 2);
    }

    #[test]
    fn test_find_references_no_match() {
        let project = ProjectState::new(PathBuf::from("/test"));

        let uri = Url::parse("file:///test/model.lua").unwrap();
        let source = r#"source("orders"):from("raw.orders")"#;
        project.update_document(uri.clone(), 1, source.to_string());

        let doc = project.get_document(&uri).unwrap();

        // Position outside any string
        let position = Position {
            line: 0,
            character: 0,
        };

        let refs = find_references(&project, &doc, position, true);
        assert!(refs.is_empty());
    }
}
