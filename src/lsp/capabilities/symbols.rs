//! textDocument/documentSymbol and workspace/symbol handlers

use tower_lsp::lsp_types::{
    DocumentSymbol, DocumentSymbolResponse, Location, SymbolInformation, SymbolKind,
};

use crate::lsp::analysis::document::DocumentState;
use crate::lsp::analysis::entities::{extract_entities, EntityKind};
use crate::lsp::project::ProjectState;

/// Get document symbols for outline view.
pub fn get_document_symbols(doc: &DocumentState) -> DocumentSymbolResponse {
    let entities = extract_entities(&doc.source);

    let symbols: Vec<DocumentSymbol> = entities
        .into_iter()
        .map(|entity| {
            let kind = match entity.kind {
                EntityKind::Source => SymbolKind::CLASS,
                EntityKind::Table => SymbolKind::STRUCT,
                EntityKind::Fact => SymbolKind::MODULE,
                EntityKind::Dimension => SymbolKind::INTERFACE,
                EntityKind::Query => SymbolKind::FUNCTION,
                EntityKind::Report => SymbolKind::FILE,
            };

            // Build children for columns
            let children: Vec<DocumentSymbol> = entity
                .columns
                .iter()
                .map(|col| {
                    #[allow(deprecated)]
                    DocumentSymbol {
                        name: col.clone(),
                        detail: None,
                        kind: SymbolKind::FIELD,
                        tags: None,
                        deprecated: None,
                        range: entity.range,
                        selection_range: entity.range,
                        children: None,
                    }
                })
                .collect();

            #[allow(deprecated)]
            DocumentSymbol {
                name: entity.name,
                detail: entity.from_table,
                kind,
                tags: None,
                deprecated: None,
                range: entity.range,
                selection_range: entity.range,
                children: if children.is_empty() {
                    None
                } else {
                    Some(children)
                },
            }
        })
        .collect();

    DocumentSymbolResponse::Nested(symbols)
}

/// Search for symbols across the workspace.
pub fn get_workspace_symbols(project: &ProjectState, query: &str) -> Vec<SymbolInformation> {
    let query_lower = query.to_lowercase();

    project
        .entities
        .iter()
        .filter(|entry| query.is_empty() || entry.key().to_lowercase().contains(&query_lower))
        .map(|entry| {
            let (uri, entity) = entry.value();
            let kind = match entity.kind {
                EntityKind::Source => SymbolKind::CLASS,
                EntityKind::Table => SymbolKind::STRUCT,
                EntityKind::Fact => SymbolKind::MODULE,
                EntityKind::Dimension => SymbolKind::INTERFACE,
                EntityKind::Query => SymbolKind::FUNCTION,
                EntityKind::Report => SymbolKind::FILE,
            };

            #[allow(deprecated)]
            SymbolInformation {
                name: entity.name.clone(),
                kind,
                tags: None,
                deprecated: None,
                location: Location {
                    uri: uri.clone(),
                    range: entity.range,
                },
                container_name: None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_lsp::lsp_types::Url;

    #[test]
    fn test_document_symbols_basic() {
        let source = r#"
source("orders"):from("raw.orders"):columns({ id = pk(int64), amount = decimal })
fact("sales"):source("orders"):measures({ total = sum("amount") })
"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let response = get_document_symbols(&doc);

        if let DocumentSymbolResponse::Nested(symbols) = response {
            assert_eq!(symbols.len(), 2);
            assert_eq!(symbols[0].name, "orders");
            assert_eq!(symbols[0].kind, SymbolKind::CLASS);
            assert_eq!(symbols[1].name, "sales");
            assert_eq!(symbols[1].kind, SymbolKind::MODULE);
        } else {
            panic!("Expected nested response");
        }
    }

    #[test]
    fn test_document_symbols_with_columns() {
        let source = r#"source("orders"):from("raw.orders"):columns({ id = pk(int64), customer_id = int64, amount = decimal })"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let response = get_document_symbols(&doc);

        if let DocumentSymbolResponse::Nested(symbols) = response {
            assert_eq!(symbols.len(), 1);
            assert_eq!(symbols[0].name, "orders");
            assert_eq!(symbols[0].detail, Some("raw.orders".to_string()));

            let children = symbols[0].children.as_ref().expect("Expected children");
            assert_eq!(children.len(), 3);
            assert!(children.iter().any(|c| c.name == "id"));
            assert!(children.iter().any(|c| c.name == "customer_id"));
            assert!(children.iter().any(|c| c.name == "amount"));
        } else {
            panic!("Expected nested response");
        }
    }

    #[test]
    fn test_document_symbols_all_entity_kinds() {
        let source = r#"
source("src"):from("raw.src"):columns({ id = pk(int64) })
table("tbl"):from("raw.tbl"):columns({ id = pk(int64) })
fact("fct"):source("src"):measures({ total = sum("value") })
dimension("dim"):source("src"):columns({ name = string })
query("qry"):from("src"):select({ "id" })
report("rpt"):title("Report")
"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let response = get_document_symbols(&doc);

        if let DocumentSymbolResponse::Nested(symbols) = response {
            assert_eq!(symbols.len(), 6);

            // Check kinds mapping
            let find_symbol = |name: &str| symbols.iter().find(|s| s.name == name).unwrap();

            assert_eq!(find_symbol("src").kind, SymbolKind::CLASS);
            assert_eq!(find_symbol("tbl").kind, SymbolKind::STRUCT);
            assert_eq!(find_symbol("fct").kind, SymbolKind::MODULE);
            assert_eq!(find_symbol("dim").kind, SymbolKind::INTERFACE);
            assert_eq!(find_symbol("qry").kind, SymbolKind::FUNCTION);
            assert_eq!(find_symbol("rpt").kind, SymbolKind::FILE);
        } else {
            panic!("Expected nested response");
        }
    }

    #[test]
    fn test_document_symbols_empty() {
        let source = "-- just a comment\nlocal x = 1";
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let response = get_document_symbols(&doc);

        if let DocumentSymbolResponse::Nested(symbols) = response {
            assert!(symbols.is_empty());
        } else {
            panic!("Expected nested response");
        }
    }

    #[test]
    fn test_workspace_symbols_filter() {
        use std::path::PathBuf;
        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            Url::parse("file:///test/a.lua").unwrap(),
            1,
            r#"source("orders"):from("raw.orders")"#.to_string(),
        );
        project.update_document(
            Url::parse("file:///test/b.lua").unwrap(),
            1,
            r#"source("customers"):from("raw.customers")"#.to_string(),
        );

        // Empty query returns all
        let all = get_workspace_symbols(&project, "");
        assert_eq!(all.len(), 2);

        // Filter by name
        let filtered = get_workspace_symbols(&project, "order");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "orders");
    }
}
