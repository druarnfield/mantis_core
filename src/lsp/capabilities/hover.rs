//! textDocument/hover handler
//!
//! Provides hover information for Mantis DSL entities, functions, types, and constants.

use tower_lsp::lsp_types::{Hover, HoverContents, MarkupContent, MarkupKind, Position, Url};

use crate::lsp::analysis::document::DocumentState;
use crate::lsp::analysis::entities::{EntityKind, LocalEntity};
use crate::lsp::analysis::symbols_generated::{BLOCKS, CONSTANTS, FUNCTIONS, METHODS, TYPES};
use crate::lsp::project::ProjectState;

#[derive(Debug, Clone)]
pub struct HoverInfo {
    pub kind: &'static str,
    pub name: String,
    pub from_table: Option<String>,
    pub columns: Vec<String>,
    pub description: Option<String>,
    pub defined_in: String,
}

impl HoverInfo {
    pub fn from_entity(entity: &LocalEntity, uri: &Url) -> Self {
        let kind = match entity.kind {
            EntityKind::Source => "source",
            EntityKind::Table => "table",
            EntityKind::Fact => "fact",
            EntityKind::Dimension => "dimension",
            EntityKind::Query => "query",
            EntityKind::Report => "report",
        };

        let defined_in = uri
            .path_segments()
            .and_then(|s| s.last())
            .unwrap_or("unknown")
            .to_string();

        Self {
            kind,
            name: entity.name.clone(),
            from_table: entity.from_table.clone(),
            columns: entity.columns.clone(),
            description: None,
            defined_in,
        }
    }
}

pub fn format_hover_markdown(info: &HoverInfo) -> String {
    let mut lines = Vec::new();
    lines.push(format!("**{} \"{}\"**", info.kind, info.name));
    lines.push("---".to_string());

    if let Some(ref table) = info.from_table {
        lines.push(format!("**From:** `{}`", table));
    }

    if !info.columns.is_empty() {
        lines.push(format!("**Columns:** {}", info.columns.join(", ")));
    }

    lines.push(format!("*Defined in {}*", info.defined_in));

    if let Some(ref desc) = info.description {
        lines.push(String::new());
        lines.push(desc.clone());
    }

    lines.join("\n")
}

pub fn entity_name_at_position(doc: &DocumentState, position: Position) -> Option<String> {
    let node = doc.node_at_position(position)?;

    let mut current = Some(node);
    while let Some(n) = current {
        if n.kind() == "string" || n.kind() == "string_content" {
            let text = &doc.source[n.start_byte()..n.end_byte()];
            let content = text.trim_matches('"').trim_matches('\'');

            if is_entity_context(n, &doc.source) {
                return Some(content.to_string());
            }
        }
        current = n.parent();
    }

    None
}

fn is_entity_context(node: tree_sitter::Node, source: &str) -> bool {
    let mut current = node.parent();
    while let Some(n) = current {
        if n.kind() == "function_call" {
            if let Some(first_child) = n.child(0) {
                if first_child.kind() == "identifier" {
                    let name = &source[first_child.start_byte()..first_child.end_byte()];
                    if matches!(
                        name,
                        "source" | "fact" | "dimension" | "table" | "query" | "report"
                    ) {
                        return true;
                    }
                }
            }
        }
        current = n.parent();
    }
    false
}

pub fn get_hover(project: &ProjectState, doc: &DocumentState, position: Position) -> Option<Hover> {
    // First, try to get hover for an identifier (function, type, constant, method)
    if let Some(hover) = get_identifier_hover(doc, position) {
        return Some(hover);
    }

    // Then, try to get hover for an entity reference (string in entity context)
    let name = entity_name_at_position(doc, position)?;
    let (uri, entity) = project.find_entity(&name)?;
    let info = HoverInfo::from_entity(&entity, &uri);
    let markdown = format_hover_markdown(&info);

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Get hover for DSL identifiers (functions, types, constants, methods).
fn get_identifier_hover(doc: &DocumentState, position: Position) -> Option<Hover> {
    let node = doc.node_at_position(position)?;

    // Walk up to find an identifier
    let mut current = Some(node);
    while let Some(n) = current {
        if n.kind() == "identifier" {
            let name = &doc.source[n.start_byte()..n.end_byte()];

            // Check if this is a method call (preceded by :)
            let is_method = n
                .parent()
                .map(|p| p.kind() == "method_index_expression")
                .unwrap_or(false);

            if is_method {
                if let Some(hover) = get_method_hover(name) {
                    return Some(hover);
                }
            } else {
                // Try functions, types, constants, blocks in order
                if let Some(hover) = get_function_hover(name) {
                    return Some(hover);
                }
                if let Some(hover) = get_type_hover(name) {
                    return Some(hover);
                }
                if let Some(hover) = get_constant_hover(name) {
                    return Some(hover);
                }
                if let Some(hover) = get_block_hover(name) {
                    return Some(hover);
                }
            }
        }
        current = n.parent();
    }

    None
}

/// Get hover for a DSL function.
fn get_function_hover(name: &str) -> Option<Hover> {
    let func = FUNCTIONS.iter().find(|f| f.name == name)?;

    let markdown = format!(
        "**{}** *({})*\n\n---\n\n{}",
        func.name, func.category, func.description
    );

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Get hover for a DSL type.
fn get_type_hover(name: &str) -> Option<Hover> {
    let typ = TYPES.iter().find(|t| t.name == name)?;

    let mut markdown = format!("**{}** *type*\n\n---\n\n{}", typ.name, typ.description);

    if let Some(template) = typ.template {
        markdown.push_str(&format!(
            "\n\n**Syntax:** `{}`",
            template
                .replace("${1:", "")
                .replace("${2:", "")
                .replace("}", "")
        ));
    }

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Get hover for a DSL constant.
fn get_constant_hover(name: &str) -> Option<Hover> {
    let constant = CONSTANTS.iter().find(|c| c.name == name)?;

    let markdown = format!(
        "**{}** *({})*\n\n---\n\n{}",
        constant.name, constant.category, constant.description
    );

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Get hover for a DSL method.
fn get_method_hover(name: &str) -> Option<Hover> {
    let method = METHODS.iter().find(|m| m.name == name)?;

    let entity_types = if method.entity_types.is_empty() {
        "all".to_string()
    } else {
        method.entity_types.join(", ")
    };

    let markdown = format!(
        "**:{}** *method*\n\n---\n\n{}\n\n**Available on:** {}",
        method.name, method.description, entity_types
    );

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Get hover for a DSL block (top-level construct).
fn get_block_hover(name: &str) -> Option<Hover> {
    let block = BLOCKS.iter().find(|b| b.name == name)?;

    let markdown = format!(
        "**{}** *entity type*\n\n---\n\n{}",
        block.name, block.description
    );

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_hover() {
        let hover = get_function_hover("sum");
        assert!(hover.is_some());

        if let Some(Hover {
            contents: HoverContents::Markup(content),
            ..
        }) = hover
        {
            assert!(content.value.contains("**sum**"));
            assert!(content.value.contains("aggregation"));
            assert!(content.value.contains("Sum aggregation measure"));
        }
    }

    #[test]
    fn test_type_hover() {
        let hover = get_type_hover("int64");
        assert!(hover.is_some());

        if let Some(Hover {
            contents: HoverContents::Markup(content),
            ..
        }) = hover
        {
            assert!(content.value.contains("**int64**"));
            assert!(content.value.contains("type"));
        }
    }

    #[test]
    fn test_type_hover_with_template() {
        let hover = get_type_hover("decimal");
        assert!(hover.is_some());

        if let Some(Hover {
            contents: HoverContents::Markup(content),
            ..
        }) = hover
        {
            assert!(content.value.contains("**decimal**"));
            assert!(content.value.contains("Syntax:"));
        }
    }

    #[test]
    fn test_constant_hover() {
        let hover = get_constant_hover("MANY_TO_ONE");
        assert!(hover.is_some());

        if let Some(Hover {
            contents: HoverContents::Markup(content),
            ..
        }) = hover
        {
            assert!(content.value.contains("**MANY_TO_ONE**"));
            assert!(content.value.contains("cardinality"));
        }
    }

    #[test]
    fn test_method_hover() {
        let hover = get_method_hover("from");
        assert!(hover.is_some());

        if let Some(Hover {
            contents: HoverContents::Markup(content),
            ..
        }) = hover
        {
            assert!(content.value.contains("**:from**"));
            assert!(content.value.contains("method"));
            assert!(content.value.contains("Available on:"));
        }
    }

    #[test]
    fn test_block_hover() {
        let hover = get_block_hover("source");
        assert!(hover.is_some());

        if let Some(Hover {
            contents: HoverContents::Markup(content),
            ..
        }) = hover
        {
            assert!(content.value.contains("**source**"));
            assert!(content.value.contains("entity type"));
        }
    }

    #[test]
    fn test_identifier_hover_on_function() {
        let source = r#"total = sum("amount")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position on "sum"
        let position = Position {
            line: 0,
            character: 9,
        };

        let hover = get_identifier_hover(&doc, position);
        assert!(hover.is_some());
    }

    #[test]
    fn test_identifier_hover_on_type() {
        let source = r#"id = pk(int64)"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position on "int64"
        let position = Position {
            line: 0,
            character: 10,
        };

        let hover = get_identifier_hover(&doc, position);
        assert!(hover.is_some());
    }

    #[test]
    fn test_format_hover_with_all_fields() {
        let info = HoverInfo {
            kind: "source",
            name: "orders".to_string(),
            from_table: Some("raw.orders".to_string()),
            columns: vec!["id".to_string(), "amount".to_string()],
            description: Some("Customer orders".to_string()),
            defined_in: "model.lua".to_string(),
        };

        let markdown = format_hover_markdown(&info);

        assert!(markdown.contains("**source \"orders\"**"));
        assert!(markdown.contains("**From:** `raw.orders`"));
        assert!(markdown.contains("**Columns:** id, amount"));
        assert!(markdown.contains("*Defined in model.lua*"));
        assert!(markdown.contains("Customer orders"));
    }

    #[test]
    fn test_format_hover_minimal() {
        let info = HoverInfo {
            kind: "fact",
            name: "sales".to_string(),
            from_table: None,
            columns: vec![],
            description: None,
            defined_in: "facts.lua".to_string(),
        };

        let markdown = format_hover_markdown(&info);

        assert!(markdown.contains("**fact \"sales\"**"));
        assert!(!markdown.contains("**From:**"));
        assert!(!markdown.contains("**Columns:**"));
    }

    #[test]
    fn test_hover_info_from_entity() {
        use crate::lsp::analysis::entities::{EntityKind, LocalEntity};
        use tower_lsp::lsp_types::Range;

        let entity = LocalEntity {
            name: "customers".to_string(),
            kind: EntityKind::Dimension,
            range: Range::default(),
            columns: vec!["id".to_string(), "name".to_string()],
            from_table: Some("raw.customers".to_string()),
        };

        let uri = Url::parse("file:///project/dims.lua").unwrap();
        let info = HoverInfo::from_entity(&entity, &uri);

        assert_eq!(info.kind, "dimension");
        assert_eq!(info.name, "customers");
        assert_eq!(info.from_table, Some("raw.customers".to_string()));
        assert_eq!(info.defined_in, "dims.lua");
    }
}
