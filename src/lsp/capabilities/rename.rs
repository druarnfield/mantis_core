//! textDocument/rename and textDocument/prepareRename handlers
//!
//! Provides rename refactoring for entities across all open documents.
//! Renames the entity definition and all references in `:from()`, `:source()`, etc.

use std::collections::HashMap;

use tower_lsp::lsp_types::{Position, PrepareRenameResponse, Range, TextEdit, Url, WorkspaceEdit};

use crate::lsp::analysis::document::DocumentState;
use crate::lsp::project::ProjectState;

/// Prepare rename - check if rename is valid at position and return the range.
pub fn prepare_rename(doc: &DocumentState, position: Position) -> Option<PrepareRenameResponse> {
    let (range, name) = find_string_at_position(doc, position)?;

    // Return the range and placeholder text
    Some(PrepareRenameResponse::RangeWithPlaceholder {
        range,
        placeholder: name,
    })
}

/// Perform rename - return workspace edit with all text changes.
pub fn rename(
    project: &ProjectState,
    doc: &DocumentState,
    position: Position,
    new_name: &str,
) -> Option<WorkspaceEdit> {
    let (_, old_name) = find_string_at_position(doc, position)?;

    let mut changes: HashMap<Url, Vec<TextEdit>> = HashMap::new();

    // Find and update all references across all documents
    for entry in project.documents.iter() {
        let doc_uri = entry.key().clone();
        let doc_source = &entry.value().source;

        let edits = find_and_replace_references(doc_source, &old_name, new_name);
        if !edits.is_empty() {
            changes.insert(doc_uri, edits);
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(WorkspaceEdit {
            changes: Some(changes),
            document_changes: None,
            change_annotations: None,
        })
    }
}

/// Find the string (including quotes) at position and return range and content.
fn find_string_at_position(doc: &DocumentState, position: Position) -> Option<(Range, String)> {
    let node = doc.node_at_position(position)?;

    let mut current = Some(node);
    while let Some(n) = current {
        if n.kind() == "string" {
            let text = &doc.source[n.start_byte()..n.end_byte()];
            let name = text.trim_matches('"').trim_matches('\'').to_string();

            // Return range of just the content (excluding quotes)
            let range = Range {
                start: Position {
                    line: n.start_position().row as u32,
                    character: n.start_position().column as u32 + 1, // Skip opening quote
                },
                end: Position {
                    line: n.end_position().row as u32,
                    character: n.end_position().column as u32 - 1, // Skip closing quote
                },
            };

            return Some((range, name));
        }
        if n.kind() == "string_content" {
            let text = &doc.source[n.start_byte()..n.end_byte()];
            let range = Range {
                start: Position {
                    line: n.start_position().row as u32,
                    character: n.start_position().column as u32,
                },
                end: Position {
                    line: n.end_position().row as u32,
                    character: n.end_position().column as u32,
                },
            };
            return Some((range, text.to_string()));
        }
        current = n.parent();
    }

    None
}

/// Find all string references and create text edits to rename them.
fn find_and_replace_references(source: &str, old_name: &str, new_name: &str) -> Vec<TextEdit> {
    let mut edits = Vec::new();
    let search = format!("\"{}\"", old_name);

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
        let end_col = start_col + old_name.len() as u32;

        edits.push(TextEdit {
            range: Range {
                start: Position {
                    line,
                    character: start_col,
                },
                end: Position {
                    line,
                    character: end_col,
                },
            },
            new_text: new_name.to_string(),
        });

        pos = abs_pos + search.len();
        col += search.len() as u32;
    }

    edits
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_prepare_rename() {
        let source = r#"source("orders"):from("raw.orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "orders"
        let position = Position {
            line: 0,
            character: 10,
        };

        let result = prepare_rename(&doc, position);
        assert!(result.is_some());

        if let Some(PrepareRenameResponse::RangeWithPlaceholder { range, placeholder }) = result {
            assert_eq!(placeholder, "orders");
            assert_eq!(range.start.character, 8); // After opening quote
            assert_eq!(range.end.character, 14); // Before closing quote
        } else {
            panic!("Expected RangeWithPlaceholder");
        }
    }

    #[test]
    fn test_prepare_rename_outside_string() {
        let source = r#"source("orders"):from("raw.orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position on "source" keyword
        let position = Position {
            line: 0,
            character: 2,
        };

        let result = prepare_rename(&doc, position);
        assert!(result.is_none());
    }

    #[test]
    fn test_rename_single_document() {
        let project = ProjectState::new(PathBuf::from("/test"));

        let uri = Url::parse("file:///test/model.lua").unwrap();
        let source = r#"source("orders"):from("raw.orders")
fact("sales"):from("orders")"#;
        project.update_document(uri.clone(), 1, source.to_string());

        let doc = project.get_document(&uri).unwrap();

        // Position inside "orders"
        let position = Position {
            line: 0,
            character: 10,
        };

        let result = rename(&project, &doc, position, "order_items");
        assert!(result.is_some());

        let edit = result.unwrap();
        let changes = edit.changes.unwrap();

        // Should have edits for this document
        assert!(changes.contains_key(&uri));

        let edits = &changes[&uri];
        // Should rename both occurrences of "orders"
        assert_eq!(edits.len(), 2);

        for edit in edits {
            assert_eq!(edit.new_text, "order_items");
        }
    }

    #[test]
    fn test_rename_across_documents() {
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

        // Position inside "orders"
        let position = Position {
            line: 0,
            character: 10,
        };

        let result = rename(&project, &doc, position, "order_items");
        assert!(result.is_some());

        let edit = result.unwrap();
        let changes = edit.changes.unwrap();

        // Should have edits for both documents
        assert!(changes.contains_key(&uri1));
        assert!(changes.contains_key(&uri2));
    }

    #[test]
    fn test_find_and_replace_references() {
        let source = r#"source("test"):from("test")"#;
        let edits = find_and_replace_references(source, "test", "new_test");

        assert_eq!(edits.len(), 2);
        assert_eq!(edits[0].new_text, "new_test");
        assert_eq!(edits[1].new_text, "new_test");
    }

    #[test]
    fn test_find_and_replace_multiline() {
        let source = r#"source("orders"):from("raw.orders")
dimension("customers"):source("orders")
fact("sales"):from("orders")"#;

        let edits = find_and_replace_references(source, "orders", "order_items");

        assert_eq!(edits.len(), 3);
        assert_eq!(edits[0].range.start.line, 0);
        assert_eq!(edits[1].range.start.line, 1);
        assert_eq!(edits[2].range.start.line, 2);
    }

    #[test]
    fn test_rename_no_match() {
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

        let result = rename(&project, &doc, position, "new_name");
        assert!(result.is_none());
    }
}
