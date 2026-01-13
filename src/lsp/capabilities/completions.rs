//! textDocument/completion handler
//!
//! Provides completion items based on the detected context.

use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, Documentation, MarkupContent, MarkupKind,
};

use crate::lsp::analysis::context::{CompletionContext, StringContext};
use crate::lsp::analysis::document::DocumentState;
use crate::lsp::analysis::entities::EntityKind;
use crate::lsp::analysis::symbols_generated::{
    self, methods_for_entity, BLOCKS, CONSTANTS, FUNCTIONS, TYPES,
};
use crate::lsp::project::ProjectState;

/// Get completion items for the given context.
pub fn get_completions(
    context: &CompletionContext,
    _document: &DocumentState,
    project: &ProjectState,
) -> Vec<CompletionItem> {
    match context {
        CompletionContext::Global => complete_globals(),
        CompletionContext::BuilderChain { builder_type } => complete_builder_methods(builder_type),
        CompletionContext::ColumnBlock => complete_column_context(),
        CompletionContext::MeasuresBlock => complete_measures_context(),
        CompletionContext::IncludesBlock => complete_includes_context(project),
        CompletionContext::TableConstructor => complete_table_constructor(),
        CompletionContext::StringLiteral { content: _, kind } => {
            complete_string_context(kind, project)
        }
        CompletionContext::TypeExpression => complete_types(),
        CompletionContext::Unknown => complete_globals(), // Fallback to globals
    }
}

/// Complete at global scope - offer block definitions and top-level functions.
fn complete_globals() -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Add block definitions (source, table, fact, dimension, etc.)
    for block in BLOCKS.iter() {
        items.push(block.to_completion_item());
    }

    // Add standalone functions useful at top level
    for func in FUNCTIONS.iter() {
        // Filter to functions that make sense at top level
        if matches!(
            func.category,
            "utility" | "comparison" // link, link_as, AND, OR, NOT
        ) {
            items.push(func.to_completion_item());
        }
    }

    // Add constants (cardinality, materialization, etc.)
    for constant in CONSTANTS.iter() {
        items.push(constant.to_completion_item());
    }

    items
}

/// Complete methods for a specific builder type.
fn complete_builder_methods(builder_type: &str) -> Vec<CompletionItem> {
    methods_for_entity(builder_type)
        .into_iter()
        .map(|m| m.to_completion_item())
        .collect()
}

/// Complete inside a :columns({...}) block.
fn complete_column_context() -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Column helper functions: pk, required, nullable, describe, rename, compute
    for func in FUNCTIONS.iter() {
        if func.category == "column" {
            items.push(func.to_completion_item());
        }
    }

    // Types for column definitions
    for type_def in TYPES.iter() {
        items.push(type_def.to_completion_item());
    }

    items
}

/// Complete inside a :measures({...}) block.
fn complete_measures_context() -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Aggregation functions: sum, count, avg, min, max, count_distinct
    for func in FUNCTIONS.iter() {
        if func.category == "aggregation" {
            items.push(func.to_completion_item());
        }
    }

    // Time intelligence functions
    for func in FUNCTIONS.iter() {
        if func.category == "time_intel" {
            items.push(func.to_completion_item());
        }
    }

    // Derived measure helper
    if let Some(derived) = symbols_generated::find_function("derived") {
        items.push(derived.to_completion_item());
    }

    items
}

/// Complete inside an :includes({...}) block.
fn complete_includes_context(project: &ProjectState) -> Vec<CompletionItem> {
    // Inside includes, we reference dimension entities
    let mut items = Vec::new();

    for (name, _, entity) in project.entities_of_kind(EntityKind::Dimension) {
        items.push(CompletionItem {
            label: name.clone(),
            kind: Some(CompletionItemKind::CLASS),
            detail: Some("dimension".to_string()),
            documentation: entity.from_table.as_ref().map(|t| {
                Documentation::MarkupContent(MarkupContent {
                    kind: MarkupKind::Markdown,
                    value: format!("Dimension from `{}`", t),
                })
            }),
            ..Default::default()
        });
    }

    // Also include sources that might be used directly
    for (name, _, entity) in project.entities_of_kind(EntityKind::Source) {
        items.push(CompletionItem {
            label: name.clone(),
            kind: Some(CompletionItemKind::CLASS),
            detail: Some("source".to_string()),
            documentation: entity.from_table.as_ref().map(|t| {
                Documentation::MarkupContent(MarkupContent {
                    kind: MarkupKind::Markdown,
                    value: format!("Source from `{}`", t),
                })
            }),
            ..Default::default()
        });
    }

    items
}

/// Complete inside a general table constructor.
fn complete_table_constructor() -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Common items in table constructors
    for func in FUNCTIONS.iter() {
        if matches!(func.category, "column" | "expression") {
            items.push(func.to_completion_item());
        }
    }

    for type_def in TYPES.iter() {
        items.push(type_def.to_completion_item());
    }

    items
}

/// Complete inside a string literal based on context.
fn complete_string_context(kind: &StringContext, project: &ProjectState) -> Vec<CompletionItem> {
    match kind {
        StringContext::TableReference => {
            // TODO: Query worker for available database tables
            Vec::new()
        }
        StringContext::TargetReference => {
            // TODO: Query worker for available schemas/tables
            Vec::new()
        }
        StringContext::ColumnReference => {
            // TODO: Provide columns from the current entity's source
            Vec::new()
        }
        StringContext::EntityReference => {
            // Provide all defined entities from the model
            complete_entity_references(project)
        }
        StringContext::Other => Vec::new(),
    }
}

/// Complete entity references (for :from(), :source(), etc.)
fn complete_entity_references(project: &ProjectState) -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Add all entities with their type info
    for entry in project.entities.iter() {
        let name = entry.key();
        let (_, entity) = entry.value();

        let (kind_str, icon) = match entity.kind {
            EntityKind::Source => ("source", CompletionItemKind::CLASS),
            EntityKind::Table => ("table", CompletionItemKind::STRUCT),
            EntityKind::Fact => ("fact", CompletionItemKind::MODULE),
            EntityKind::Dimension => ("dimension", CompletionItemKind::INTERFACE),
            EntityKind::Query => ("query", CompletionItemKind::FUNCTION),
            EntityKind::Report => ("report", CompletionItemKind::FILE),
        };

        let mut doc_parts = vec![format!("**{}** `{}`", kind_str, name)];
        if let Some(ref from) = entity.from_table {
            doc_parts.push(format!("From: `{}`", from));
        }
        if !entity.columns.is_empty() {
            doc_parts.push(format!("Columns: {}", entity.columns.join(", ")));
        }

        items.push(CompletionItem {
            label: name.clone(),
            kind: Some(icon),
            detail: Some(kind_str.to_string()),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: doc_parts.join("\n\n"),
            })),
            ..Default::default()
        });
    }

    items
}

/// Complete type expressions (inside pk(), required(), etc.).
fn complete_types() -> Vec<CompletionItem> {
    TYPES.iter().map(|t| t.to_completion_item()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_completions_include_blocks() {
        let items = complete_globals();
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"source"), "Should include source block");
        assert!(labels.contains(&"table"), "Should include table block");
        assert!(labels.contains(&"fact"), "Should include fact block");
        assert!(
            labels.contains(&"dimension"),
            "Should include dimension block"
        );
    }

    #[test]
    fn test_global_completions_include_constants() {
        let items = complete_globals();
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(
            labels.contains(&"MANY_TO_ONE"),
            "Should include cardinality constants"
        );
        assert!(
            labels.contains(&"TABLE"),
            "Should include materialization constants"
        );
    }

    #[test]
    fn test_column_context_completions() {
        let items = complete_column_context();
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"pk"), "Should include pk function");
        assert!(
            labels.contains(&"required"),
            "Should include required function"
        );
        assert!(labels.contains(&"int64"), "Should include int64 type");
        assert!(labels.contains(&"string"), "Should include string type");
    }

    #[test]
    fn test_measures_context_completions() {
        let items = complete_measures_context();
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"sum"), "Should include sum aggregation");
        assert!(
            labels.contains(&"count"),
            "Should include count aggregation"
        );
        assert!(labels.contains(&"avg"), "Should include avg aggregation");
    }

    #[test]
    fn test_builder_methods_for_source() {
        let items = complete_builder_methods("source");
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"from"), "Source should have :from method");
        assert!(
            labels.contains(&"columns"),
            "Source should have :columns method"
        );
    }

    #[test]
    fn test_builder_methods_for_fact() {
        let items = complete_builder_methods("fact");
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(
            labels.contains(&"measures"),
            "Fact should have :measures method"
        );
        assert!(labels.contains(&"grain"), "Fact should have :grain method");
    }

    #[test]
    fn test_type_completions() {
        let items = complete_types();
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"int64"), "Should include int64");
        assert!(labels.contains(&"string"), "Should include string");
        assert!(labels.contains(&"decimal"), "Should include decimal");
        assert!(labels.contains(&"timestamp"), "Should include timestamp");
    }

    #[test]
    fn test_entity_reference_completions() {
        use std::path::PathBuf;
        use tower_lsp::lsp_types::Url;

        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            Url::parse("file:///test/model.lua").unwrap(),
            1,
            r#"source("orders"):from("raw.orders"):columns({ id = pk(int64) })
dimension("customers"):source("orders")"#
                .to_string(),
        );

        let items = complete_entity_references(&project);
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"orders"), "Should include orders source");
        assert!(
            labels.contains(&"customers"),
            "Should include customers dimension"
        );
    }

    #[test]
    fn test_includes_context_completions() {
        use std::path::PathBuf;
        use tower_lsp::lsp_types::Url;

        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            Url::parse("file:///test/model.lua").unwrap(),
            1,
            r#"source("orders"):from("raw.orders")
dimension("customers"):source("orders")
dimension("products"):source("orders")"#
                .to_string(),
        );

        let items = complete_includes_context(&project);
        let labels: Vec<_> = items.iter().map(|i| i.label.as_str()).collect();

        // Should include dimensions
        assert!(
            labels.contains(&"customers"),
            "Should include customers dimension"
        );
        assert!(
            labels.contains(&"products"),
            "Should include products dimension"
        );
        // Should also include source
        assert!(labels.contains(&"orders"), "Should include orders source");
    }
}
