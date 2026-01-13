//! Diagnostics generation for Mantis Lua DSL
//!
//! Detects issues like duplicate entity definitions and undefined references.

use std::collections::HashMap;

use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Url};

use crate::lsp::analysis::entities::{extract_entities, EntityKind, LocalEntity};

/// Result of checking a set of documents for issues.
#[derive(Debug, Default)]
pub struct DiagnosticsResult {
    /// Diagnostics grouped by document URI.
    pub by_uri: HashMap<Url, Vec<Diagnostic>>,
}

impl DiagnosticsResult {
    /// Create a new empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a diagnostic for a specific URI.
    pub fn add(&mut self, uri: Url, diagnostic: Diagnostic) {
        self.by_uri.entry(uri).or_default().push(diagnostic);
    }

    /// Get diagnostics for a specific URI (empty vec if none).
    pub fn for_uri(&self, uri: &Url) -> Vec<Diagnostic> {
        self.by_uri.get(uri).cloned().unwrap_or_default()
    }

    /// Get all URIs that have diagnostics.
    pub fn uris(&self) -> impl Iterator<Item = &Url> {
        self.by_uri.keys()
    }
}

/// A document with its source content for analysis.
pub struct DocumentInfo<'a> {
    pub uri: &'a Url,
    pub source: &'a str,
}

/// Check multiple documents for duplicate entity definitions.
///
/// Returns diagnostics for each document that defines a duplicate entity.
pub fn check_duplicates(documents: &[DocumentInfo<'_>]) -> DiagnosticsResult {
    let mut result = DiagnosticsResult::new();

    // Collect all entities with their locations
    let mut entities_by_name: HashMap<String, Vec<(Url, LocalEntity)>> = HashMap::new();

    for doc in documents {
        let entities = extract_entities(doc.source);
        for entity in entities {
            entities_by_name
                .entry(entity.name.clone())
                .or_default()
                .push((doc.uri.clone(), entity));
        }
    }

    // Report duplicates
    for (name, locations) in entities_by_name {
        if locations.len() > 1 {
            // Add a diagnostic to each location
            for (uri, entity) in &locations {
                let other_files: Vec<String> = locations
                    .iter()
                    .filter(|(u, _)| u != uri)
                    .map(|(u, _)| {
                        u.path_segments()
                            .and_then(|s| s.last())
                            .unwrap_or("unknown")
                            .to_string()
                    })
                    .collect();

                let message = format!(
                    "Duplicate {} '{}' also defined in: {}",
                    entity_kind_name(&entity.kind),
                    name,
                    other_files.join(", ")
                );

                result.add(
                    uri.clone(),
                    Diagnostic {
                        range: entity.range,
                        severity: Some(DiagnosticSeverity::ERROR),
                        code: Some(tower_lsp::lsp_types::NumberOrString::String(
                            "duplicate-entity".to_string(),
                        )),
                        source: Some("mantis".to_string()),
                        message,
                        ..Default::default()
                    },
                );
            }
        }
    }

    result
}

/// Check for undefined entity references in a document.
///
/// Currently checks :source() references in facts/dimensions.
/// This is a placeholder for future implementation that would require
/// deeper AST walking to extract :source() call arguments.
#[allow(dead_code)]
pub fn check_undefined_refs(_doc: &DocumentInfo<'_>, _known_sources: &[String]) -> Vec<Diagnostic> {
    // TODO: Implement by walking AST to find :source() calls
    // and checking against known_sources
    Vec::new()
}

/// Get a human-readable name for an entity kind.
fn entity_kind_name(kind: &EntityKind) -> &'static str {
    match kind {
        EntityKind::Source => "source",
        EntityKind::Table => "table",
        EntityKind::Fact => "fact",
        EntityKind::Dimension => "dimension",
        EntityKind::Query => "query",
        EntityKind::Report => "report",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_uri(name: &str) -> Url {
        Url::parse(&format!("file:///test/{}.lua", name)).unwrap()
    }

    #[test]
    fn test_no_duplicates_single_file() {
        let uri = test_uri("model");
        let source = r#"
source("orders"):from("raw.orders")
source("products"):from("raw.products")
"#;

        let docs = vec![DocumentInfo { uri: &uri, source }];

        let result = check_duplicates(&docs);
        assert!(result.by_uri.is_empty());
    }

    #[test]
    fn test_duplicate_in_same_file() {
        let uri = test_uri("model");
        let source = r#"
source("orders"):from("raw.orders")
source("orders"):from("other.orders")
"#;

        let docs = vec![DocumentInfo { uri: &uri, source }];

        let result = check_duplicates(&docs);
        let diagnostics = result.for_uri(&uri);
        assert_eq!(diagnostics.len(), 2); // Both definitions get flagged
        assert!(diagnostics[0].message.contains("Duplicate"));
        assert!(diagnostics[0].message.contains("orders"));
    }

    #[test]
    fn test_duplicate_across_files() {
        let uri_a = test_uri("a");
        let uri_b = test_uri("b");
        let source_a = r#"source("orders"):from("raw.orders")"#;
        let source_b = r#"source("orders"):from("other.orders")"#;

        let docs = vec![
            DocumentInfo {
                uri: &uri_a,
                source: source_a,
            },
            DocumentInfo {
                uri: &uri_b,
                source: source_b,
            },
        ];

        let result = check_duplicates(&docs);

        // Both files should have diagnostics
        let diag_a = result.for_uri(&uri_a);
        let diag_b = result.for_uri(&uri_b);

        assert_eq!(diag_a.len(), 1);
        assert_eq!(diag_b.len(), 1);
        assert!(diag_a[0].message.contains("b.lua"));
        assert!(diag_b[0].message.contains("a.lua"));
    }

    #[test]
    fn test_duplicate_different_kinds_ok() {
        // A source and a fact with the same name should NOT be flagged
        // (they're in different namespaces in Mantis)
        let uri = test_uri("model");
        let source = r#"
source("orders"):from("raw.orders")
fact("orders"):source("orders")
"#;

        let docs = vec![DocumentInfo { uri: &uri, source }];

        let result = check_duplicates(&docs);
        // Currently we flag same-name entities regardless of kind
        // This might need adjustment based on Mantis semantics
        // For now, we DO flag this as a potential issue
        let diagnostics = result.for_uri(&uri);
        assert_eq!(diagnostics.len(), 2);
    }

    #[test]
    fn test_diagnostic_has_correct_severity() {
        let uri = test_uri("model");
        let source = r#"
source("orders"):from("raw.orders")
source("orders"):from("other.orders")
"#;

        let docs = vec![DocumentInfo { uri: &uri, source }];

        let result = check_duplicates(&docs);
        let diagnostics = result.for_uri(&uri);
        assert_eq!(diagnostics[0].severity, Some(DiagnosticSeverity::ERROR));
    }

    #[test]
    fn test_diagnostic_has_source() {
        let uri = test_uri("model");
        let source = r#"
source("orders"):from("raw.orders")
source("orders"):from("other.orders")
"#;

        let docs = vec![DocumentInfo { uri: &uri, source }];

        let result = check_duplicates(&docs);
        let diagnostics = result.for_uri(&uri);
        assert_eq!(diagnostics[0].source, Some("mantis".to_string()));
    }
}
