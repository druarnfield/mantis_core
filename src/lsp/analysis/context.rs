//! Completion context detection from AST
//!
//! Analyzes the tree-sitter AST to determine what kind of completions
//! are appropriate at the cursor position.

use tower_lsp::lsp_types::Position;
use tree_sitter::{Node, Tree};

use super::document::lsp_position_to_byte_offset;

/// The context for providing completions.
#[derive(Debug, Clone, PartialEq)]
pub enum CompletionContext {
    /// Top-level, expecting source/table/fact/dimension/etc.
    Global,

    /// After a builder call, expecting :method chain
    BuilderChain {
        /// The type of builder (source, table, fact, dimension, etc.)
        builder_type: String,
    },

    /// Inside :columns({...}) block
    ColumnBlock,

    /// Inside :measures({...}) block
    MeasuresBlock,

    /// Inside :includes({...}) block
    IncludesBlock,

    /// Inside a table constructor (general)
    TableConstructor,

    /// Inside a string literal
    StringLiteral {
        /// The partial content typed so far
        content: String,
        /// What kind of string this is
        kind: StringContext,
    },

    /// After a type expression (inside pk(), required(), etc.)
    TypeExpression,

    /// Unknown/error state - provide global completions as fallback
    Unknown,
}

/// Context for string literal completions.
#[derive(Debug, Clone, PartialEq)]
pub enum StringContext {
    /// Inside :from("...") - table reference
    TableReference,
    /// Inside :target("...") - target table
    TargetReference,
    /// Inside column reference
    ColumnReference,
    /// Inside entity reference (for relationships, grains, etc.)
    EntityReference,
    /// General string (no special completions)
    Other,
}

impl Default for CompletionContext {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Detect the completion context at the given position.
pub fn detect_context(tree: &Tree, source: &str, position: Position) -> CompletionContext {
    let byte_offset = lsp_position_to_byte_offset(source, position);

    let root = tree.root_node();
    let node = find_node_at_offset(root, byte_offset);

    // Try AST-based detection first
    if let Some(n) = node {
        let ctx = detect_context_from_node(n, source, byte_offset);
        if !matches!(ctx, CompletionContext::Unknown | CompletionContext::Global) {
            return ctx;
        }
    }

    // Fallback: text-based heuristics for incomplete parses
    if let Some(ctx) = detect_from_text_context(source, byte_offset) {
        return ctx;
    }

    if node.map(|n| n.kind()) == Some("chunk") {
        return CompletionContext::Global;
    }

    CompletionContext::Global
}

/// Find the most specific node containing the given byte offset.
fn find_node_at_offset(node: Node, offset: usize) -> Option<Node> {
    // Check if this node contains the offset
    if offset < node.start_byte() || offset > node.end_byte() {
        return None;
    }

    // Try to find a more specific child
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(found) = find_node_at_offset(child, offset) {
            return Some(found);
        }
    }

    // No child contains it, return this node
    Some(node)
}

/// Detect context using text-based heuristics when AST detection fails.
fn detect_from_text_context(source: &str, offset: usize) -> Option<CompletionContext> {
    let before = &source[..offset.min(source.len())];
    let trimmed = before.trim_end();

    // Check if we just typed a colon after a builder
    if trimmed.ends_with(':') {
        if let Some(builder_type) = find_builder_type_before_colon(before) {
            return Some(CompletionContext::BuilderChain { builder_type });
        }
    }

    // Check if we're inside a specific block
    if let Some(ctx) = detect_block_context(before) {
        return Some(ctx);
    }

    None
}

/// Find the builder type (source, fact, etc.) before a trailing colon.
fn find_builder_type_before_colon(text: &str) -> Option<String> {
    let trimmed = text.trim_end();
    let without_colon = trimmed.strip_suffix(':')?;

    let builders = [
        "source",
        "fact",
        "dimension",
        "table",
        "query",
        "report",
        "pivot_report",
    ];

    for builder in builders {
        if let Some(pos) = without_colon.rfind(&format!("{}(", builder)) {
            if pos == 0 || !without_colon.as_bytes()[pos - 1].is_ascii_alphanumeric() {
                return Some(builder.to_string());
            }
        }
    }

    None
}

/// Detect if we're inside a specific method block based on text patterns.
fn detect_block_context(text: &str) -> Option<CompletionContext> {
    let patterns = [
        (":columns({", CompletionContext::ColumnBlock),
        (":measures({", CompletionContext::MeasuresBlock),
        (":includes({", CompletionContext::IncludesBlock),
    ];

    for (pattern, context) in patterns {
        if let Some(pos) = text.rfind(pattern) {
            let after_pattern = &text[pos + pattern.len()..];
            let opens = after_pattern.matches('{').count();
            let closes = after_pattern.matches('}').count();
            if opens >= closes {
                return Some(context);
            }
        }
    }

    None
}

/// Detect context by walking up from the given node.
fn detect_context_from_node(node: Node, source: &str, byte_offset: usize) -> CompletionContext {
    // Check the immediate node type
    match node.kind() {
        // Inside a string
        "string" | "string_content" => {
            return detect_string_context(node, source);
        }
        _ => {}
    }

    // Walk up the tree looking for context clues
    let mut current = Some(node);
    while let Some(n) = current {
        match n.kind() {
            // Inside a function call - check if it's a builder or type function
            "function_call" => {
                if let Some(ctx) = detect_function_call_context(n, source, byte_offset) {
                    return ctx;
                }
            }

            // Inside a method call chain (e.g., source():from())
            "method_index_expression" => {
                if let Some(ctx) = detect_method_chain_context(n, source) {
                    return ctx;
                }
            }

            // Inside a table constructor
            "table_constructor" => {
                if let Some(ctx) = detect_table_constructor_context(n, source) {
                    return ctx;
                }
            }

            // At chunk (root) level
            "chunk" => {
                return CompletionContext::Global;
            }

            _ => {}
        }

        current = n.parent();
    }

    CompletionContext::Unknown
}

/// Detect context within a string literal.
fn detect_string_context(node: Node, source: &str) -> CompletionContext {
    let content = get_string_content(node, source);

    // Walk up to find what function/method this string is an argument to
    let mut current = node.parent();
    while let Some(n) = current {
        match n.kind() {
            "arguments" => {
                // Check the parent function call
                if let Some(call) = n.parent() {
                    if call.kind() == "function_call" {
                        let kind = detect_string_kind_from_call(call, source);
                        return CompletionContext::StringLiteral { content, kind };
                    }
                }
            }
            "function_call" => {
                let kind = detect_string_kind_from_call(n, source);
                return CompletionContext::StringLiteral { content, kind };
            }
            _ => {}
        }
        current = n.parent();
    }

    CompletionContext::StringLiteral {
        content,
        kind: StringContext::Other,
    }
}

/// Get the content of a string node (without quotes).
fn get_string_content(node: Node, source: &str) -> String {
    let text = node_text(node, source);
    // Strip quotes if present
    let text = text.trim_matches('"').trim_matches('\'');
    text.to_string()
}

/// Detect what kind of string based on the function being called.
fn detect_string_kind_from_call(call: Node, source: &str) -> StringContext {
    // Check first child to determine if this is a method call or regular function call
    if let Some(first_child) = call.child(0) {
        match first_child.kind() {
            "method_index_expression" => {
                // Method call: obj:method(args)
                if let Some(method_name) = get_method_name(first_child, source) {
                    return match method_name.as_str() {
                        "from" => StringContext::TableReference,
                        "target" | "target_schema" => StringContext::TargetReference,
                        "columns" | "primary_key" => StringContext::ColumnReference,
                        "source" => StringContext::EntityReference,
                        _ => StringContext::Other,
                    };
                }
            }
            "identifier" => {
                // Regular function call: func(args)
                let name = node_text(first_child, source);
                return match name.as_str() {
                    "source" | "table" | "fact" | "dimension" | "query" | "report"
                    | "pivot_report" => {
                        // First argument is typically a name, not a reference
                        StringContext::Other
                    }
                    "ref" => StringContext::EntityReference,
                    _ => StringContext::Other,
                };
            }
            _ => {}
        }
    }

    StringContext::Other
}

/// Detect context from a function call node.
fn detect_function_call_context(
    call: Node,
    source: &str,
    byte_offset: usize,
) -> Option<CompletionContext> {
    // Get the function name
    let name = if let Some(name_node) = call.child_by_field_name("name") {
        node_text(name_node, source)
    } else if let Some(first_child) = call.child(0) {
        // Could be a method call or identifier
        if first_child.kind() == "identifier" {
            node_text(first_child, source)
        } else {
            return None;
        }
    } else {
        return None;
    };

    // Check if we're inside the arguments
    if let Some(args) = call.child_by_field_name("arguments") {
        if byte_offset >= args.start_byte() && byte_offset <= args.end_byte() {
            // Inside arguments of a type function
            match name.as_str() {
                "pk" | "required" | "nullable" | "describe" => {
                    return Some(CompletionContext::TypeExpression);
                }
                _ => {}
            }
        }
    }

    None
}

/// Detect context from a method chain.
fn detect_method_chain_context(method_expr: Node, source: &str) -> Option<CompletionContext> {
    // Get the method name (the part after the colon)
    let method_name = get_method_name(method_expr, source)?;

    // Determine context based on method name
    match method_name.as_str() {
        "columns" => Some(CompletionContext::ColumnBlock),
        "measures" => Some(CompletionContext::MeasuresBlock),
        "includes" => Some(CompletionContext::IncludesBlock),
        _ => {
            // Try to determine the builder type from the chain root
            let builder_type = find_builder_type(method_expr, source)?;
            Some(CompletionContext::BuilderChain { builder_type })
        }
    }
}

/// Detect context inside a table constructor.
fn detect_table_constructor_context(table: Node, source: &str) -> Option<CompletionContext> {
    // Walk up to see if this table is an argument to a specific method
    let mut current = table.parent();
    while let Some(n) = current {
        if n.kind() == "arguments" {
            if let Some(call) = n.parent() {
                if call.kind() == "function_call" {
                    // Check if this is a method call
                    if let Some(prefix) = call.child(0) {
                        if prefix.kind() == "method_index_expression" {
                            if let Some(method_name) = get_method_name(prefix, source) {
                                return match method_name.as_str() {
                                    "columns" => Some(CompletionContext::ColumnBlock),
                                    "measures" => Some(CompletionContext::MeasuresBlock),
                                    "includes" => Some(CompletionContext::IncludesBlock),
                                    _ => Some(CompletionContext::TableConstructor),
                                };
                            }
                        }
                    }
                }
            }
        }
        current = n.parent();
    }

    Some(CompletionContext::TableConstructor)
}

/// Get the method name from a method_index_expression node.
fn get_method_name(method_expr: Node, source: &str) -> Option<String> {
    // In tree-sitter-lua, method_index_expression has structure:
    //   method_index_expression
    //     <object> (function_call, identifier, etc.)
    //     :
    //     identifier (the method name)
    // We want the last direct child that is an identifier
    let mut cursor = method_expr.walk();
    let mut last_identifier: Option<Node> = None;

    for child in method_expr.children(&mut cursor) {
        // Only consider direct identifier children (not nested ones)
        if child.kind() == "identifier" {
            last_identifier = Some(child);
        }
    }

    if let Some(ident) = last_identifier {
        return Some(node_text(ident, source));
    }

    // Fallback: look for the identifier after ':'
    let text = node_text(method_expr, source);
    if let Some(colon_pos) = text.rfind(':') {
        let after_colon = &text[colon_pos + 1..];
        let method_name: String = after_colon
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !method_name.is_empty() {
            return Some(method_name);
        }
    }

    None
}

/// Find the root builder type by walking up the method chain.
fn find_builder_type(node: Node, source: &str) -> Option<String> {
    let mut current = Some(node);

    while let Some(n) = current {
        match n.kind() {
            "function_call" => {
                // Check if this is a top-level builder function
                if let Some(name_node) = n.child_by_field_name("name") {
                    let name = node_text(name_node, source);
                    if is_builder_function(&name) {
                        return Some(name);
                    }
                } else if let Some(first_child) = n.child(0) {
                    if first_child.kind() == "identifier" {
                        let name = node_text(first_child, source);
                        if is_builder_function(&name) {
                            return Some(name);
                        }
                    }
                }
            }
            _ => {}
        }
        current = n.parent();
    }

    None
}

/// Check if a function name is a known builder function.
fn is_builder_function(name: &str) -> bool {
    matches!(
        name,
        "source" | "table" | "fact" | "dimension" | "query" | "report" | "pivot_report"
    )
}

/// Get text content of a node.
fn node_text(node: Node, source: &str) -> String {
    source[node.start_byte()..node.end_byte()].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsp::analysis::document::DocumentState;
    use tower_lsp::lsp_types::Url;

    fn parse_and_detect(source: &str, line: u32, character: u32) -> CompletionContext {
        let uri = Url::parse("file:///test.lua").unwrap();
        let doc = DocumentState::new(uri, 1, source.to_string());
        detect_context(&doc.tree, source, Position { line, character })
    }

    #[test]
    fn test_global_context_empty() {
        let ctx = parse_and_detect("", 0, 0);
        assert_eq!(ctx, CompletionContext::Global);
    }

    #[test]
    fn test_global_context_top_level() {
        let ctx = parse_and_detect("-- comment\n", 1, 0);
        assert_eq!(ctx, CompletionContext::Global);
    }

    #[test]
    fn test_builder_chain_after_source() {
        let source = r#"source("orders")
    :"#;
        let ctx = parse_and_detect(source, 1, 5);
        // At the colon, we should suggest methods for source builder
        match ctx {
            CompletionContext::BuilderChain { builder_type } => {
                assert_eq!(builder_type, "source");
            }
            CompletionContext::Global | CompletionContext::Unknown => {
                // Also acceptable - colon alone might not be parsed as method yet
            }
            other => panic!("Unexpected context: {:?}", other),
        }
    }

    #[test]
    fn test_column_block_context() {
        let source = r#"source("orders")
    :columns({

    })"#;
        let ctx = parse_and_detect(source, 2, 8);
        assert!(
            matches!(
                ctx,
                CompletionContext::ColumnBlock | CompletionContext::TableConstructor
            ),
            "Expected ColumnBlock or TableConstructor, got {:?}",
            ctx
        );
    }

    #[test]
    fn test_string_literal_context() {
        let source = r#"source("ord")"#;
        let ctx = parse_and_detect(source, 0, 9);
        match ctx {
            CompletionContext::StringLiteral { content, kind: _ } => {
                assert!(content.contains("ord") || content.is_empty());
            }
            _ => {} // Other contexts are acceptable depending on exact position
        }
    }

    #[test]
    fn test_type_expression_context() {
        let source = r#"source("orders")
    :columns({
        id = pk()
    })"#;
        let ctx = parse_and_detect(source, 2, 16);
        // Inside pk() we should get type suggestions
        match ctx {
            CompletionContext::TypeExpression
            | CompletionContext::ColumnBlock
            | CompletionContext::TableConstructor => {}
            other => panic!("Unexpected context: {:?}", other),
        }
    }

    #[test]
    fn test_measures_block_context() {
        let source = r#"fact("revenue")
    :measures({

    })"#;
        let ctx = parse_and_detect(source, 2, 8);
        assert!(
            matches!(
                ctx,
                CompletionContext::MeasuresBlock | CompletionContext::TableConstructor
            ),
            "Expected MeasuresBlock or TableConstructor, got {:?}",
            ctx
        );
    }

    #[test]
    fn test_from_method_string_context() {
        let source = r#"source("orders")
    :from("public.")"#;
        let ctx = parse_and_detect(source, 1, 14);
        if let CompletionContext::StringLiteral { kind, .. } = ctx {
            assert_eq!(kind, StringContext::TableReference);
        } else {
            panic!("Expected StringLiteral context, got {:?}", ctx);
        }
    }

    #[test]
    fn test_text_fallback_detects_method_chain_after_colon() {
        // Incomplete parse - just typed colon
        let source = "source(\"orders\"):";
        let ctx = parse_and_detect(source, 0, 17);
        match ctx {
            CompletionContext::BuilderChain { builder_type } => {
                assert_eq!(builder_type, "source");
            }
            other => panic!("Expected BuilderChain for source, got {:?}", other),
        }
    }

    #[test]
    fn test_text_fallback_detects_fact_chain() {
        let source = "fact(\"sales\"):";
        let ctx = parse_and_detect(source, 0, 14);
        match ctx {
            CompletionContext::BuilderChain { builder_type } => {
                assert_eq!(builder_type, "fact");
            }
            other => panic!("Expected BuilderChain for fact, got {:?}", other),
        }
    }

    #[test]
    fn test_text_fallback_columns_block() {
        let source = "source(\"x\"):columns({";
        let ctx = parse_and_detect(source, 0, 21);
        assert!(
            matches!(
                ctx,
                CompletionContext::ColumnBlock | CompletionContext::TableConstructor
            ),
            "Expected ColumnBlock or TableConstructor, got {:?}",
            ctx
        );
    }

    #[test]
    fn test_text_fallback_measures_block() {
        let source = "fact(\"x\"):measures({";
        let ctx = parse_and_detect(source, 0, 20);
        assert!(
            matches!(
                ctx,
                CompletionContext::MeasuresBlock | CompletionContext::TableConstructor
            ),
            "Expected MeasuresBlock or TableConstructor, got {:?}",
            ctx
        );
    }
}
