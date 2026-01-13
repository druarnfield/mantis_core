//! textDocument/inlayHint handler
//!
//! Provides inline hints showing entity types after entity definitions.

use tower_lsp::lsp_types::{InlayHint, InlayHintKind, InlayHintLabel, Position, Range};

use crate::lsp::analysis::document::DocumentState;
use crate::lsp::analysis::entities::{extract_entities, EntityKind};

/// Get inlay hints for a document range.
pub fn get_inlay_hints(doc: &DocumentState, range: Range) -> Vec<InlayHint> {
    let entities = extract_entities(&doc.source);

    entities
        .into_iter()
        .filter(|entity| {
            // Only include entities within the requested range
            entity.range.start.line >= range.start.line && entity.range.end.line <= range.end.line
        })
        .map(|entity| {
            let type_label = match entity.kind {
                EntityKind::Source => "source",
                EntityKind::Table => "table",
                EntityKind::Fact => "fact",
                EntityKind::Dimension => "dim",
                EntityKind::Query => "query",
                EntityKind::Report => "report",
            };

            // Position the hint right after the entity name string
            // We'll put it at the end of the first line of the entity definition
            let hint_position = Position {
                line: entity.range.start.line,
                // Place after the opening definition, e.g., source("orders") -> after "orders")"
                character: find_name_end_position(&doc.source, entity.range.start, &entity.name),
            };

            InlayHint {
                position: hint_position,
                label: InlayHintLabel::String(format!(": {}", type_label)),
                kind: Some(InlayHintKind::TYPE),
                text_edits: None,
                tooltip: Some(tower_lsp::lsp_types::InlayHintTooltip::String(format!(
                    "{} entity '{}'",
                    capitalize(type_label),
                    entity.name
                ))),
                padding_left: Some(false),
                padding_right: Some(true),
                data: None,
            }
        })
        .collect()
}

/// Find the character position after the entity name in the source.
fn find_name_end_position(source: &str, start: Position, name: &str) -> u32 {
    let lines: Vec<&str> = source.lines().collect();
    let line_idx = start.line as usize;

    if line_idx >= lines.len() {
        return start.character;
    }

    let line = lines[line_idx];

    // Look for the pattern: entity_type("name")
    // We want to position after the closing quote and paren
    if let Some(name_start) = line.find(&format!("\"{}\"", name)) {
        // Position after the closing quote and paren: "name")
        (name_start + name.len() + 3) as u32
    } else if let Some(name_start) = line.find(&format!("'{}'", name)) {
        // Single quote variant
        (name_start + name.len() + 3) as u32
    } else {
        // Fallback: position at end of line
        line.len() as u32
    }
}

/// Capitalize the first letter of a string.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_lsp::lsp_types::Url;

    fn make_full_range() -> Range {
        Range {
            start: Position {
                line: 0,
                character: 0,
            },
            end: Position {
                line: 1000,
                character: 0,
            },
        }
    }

    #[test]
    fn test_inlay_hints_basic() {
        let source = r#"source("orders"):from("raw.orders"):columns({ id = pk(int64) })"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let hints = get_inlay_hints(&doc, make_full_range());

        assert_eq!(hints.len(), 1);
        match &hints[0].label {
            InlayHintLabel::String(s) => assert_eq!(s, ": source"),
            _ => panic!("Expected string label"),
        }
        assert_eq!(hints[0].kind, Some(InlayHintKind::TYPE));
    }

    #[test]
    fn test_inlay_hints_multiple_entities() {
        let source = r#"source("orders"):from("raw.orders")
fact("sales"):source("orders"):measures({ total = sum("amount") })
dimension("customers"):source("raw_customers")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let hints = get_inlay_hints(&doc, make_full_range());

        assert_eq!(hints.len(), 3);

        // Check labels
        let labels: Vec<_> = hints
            .iter()
            .map(|h| match &h.label {
                InlayHintLabel::String(s) => s.clone(),
                _ => String::new(),
            })
            .collect();

        assert!(labels.contains(&": source".to_string()));
        assert!(labels.contains(&": fact".to_string()));
        assert!(labels.contains(&": dim".to_string()));
    }

    #[test]
    fn test_inlay_hints_all_entity_types() {
        let source = r#"source("src"):from("raw.src")
table("tbl"):from("raw.tbl")
fact("fct"):source("src")
dimension("dim"):source("src")
query("qry"):from("src")
report("rpt"):title("Report")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let hints = get_inlay_hints(&doc, make_full_range());

        assert_eq!(hints.len(), 6);

        let labels: Vec<_> = hints
            .iter()
            .map(|h| match &h.label {
                InlayHintLabel::String(s) => s.clone(),
                _ => String::new(),
            })
            .collect();

        assert!(labels.contains(&": source".to_string()));
        assert!(labels.contains(&": table".to_string()));
        assert!(labels.contains(&": fact".to_string()));
        assert!(labels.contains(&": dim".to_string()));
        assert!(labels.contains(&": query".to_string()));
        assert!(labels.contains(&": report".to_string()));
    }

    #[test]
    fn test_inlay_hints_range_filter() {
        let source = r#"source("first"):from("raw.first")
source("second"):from("raw.second")
source("third"):from("raw.third")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Only request hints for line 1
        let range = Range {
            start: Position {
                line: 1,
                character: 0,
            },
            end: Position {
                line: 1,
                character: 100,
            },
        };

        let hints = get_inlay_hints(&doc, range);

        assert_eq!(hints.len(), 1);
        // Should only get the hint for "second"
        assert_eq!(hints[0].position.line, 1);
    }

    #[test]
    fn test_inlay_hints_empty() {
        let source = "-- just a comment\nlocal x = 1";
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let hints = get_inlay_hints(&doc, make_full_range());

        assert!(hints.is_empty());
    }

    #[test]
    fn test_inlay_hint_position() {
        let source = r#"source("orders"):from("raw.orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let hints = get_inlay_hints(&doc, make_full_range());

        assert_eq!(hints.len(), 1);
        // Should be positioned after source("orders")
        // source("orders") is 16 characters, so position should be at 16
        assert_eq!(hints[0].position.character, 16);
    }

    #[test]
    fn test_capitalize() {
        assert_eq!(capitalize("source"), "Source");
        assert_eq!(capitalize("dim"), "Dim");
        assert_eq!(capitalize(""), "");
    }
}
