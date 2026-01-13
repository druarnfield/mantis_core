//! Entity extraction from AST
//!
//! Extracts Source, Table, Fact, Dimension, Query, and Report entities
//! from tree-sitter AST without full Lua evaluation.

use tower_lsp::lsp_types::Range;

/// The kind of entity being defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityKind {
    Source,
    Table,
    Fact,
    Dimension,
    Query,
    Report,
}

impl EntityKind {
    /// Parse entity kind from function name.
    pub fn from_function_name(name: &str) -> Option<Self> {
        match name {
            "source" => Some(Self::Source),
            "table" => Some(Self::Table),
            "fact" => Some(Self::Fact),
            "dimension" => Some(Self::Dimension),
            "query" => Some(Self::Query),
            "report" | "pivot_report" => Some(Self::Report),
            _ => None,
        }
    }
}

/// An entity extracted from the AST.
#[derive(Debug, Clone)]
pub struct LocalEntity {
    /// The entity name (first string argument)
    pub name: String,
    /// The kind of entity
    pub kind: EntityKind,
    /// The source range of the entity definition
    pub range: Range,
    /// Column names defined in :columns({...}) if any
    pub columns: Vec<String>,
    /// The :from() table reference if present
    pub from_table: Option<String>,
}

/// Extract all entities from source code.
pub fn extract_entities(source: &str) -> Vec<LocalEntity> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_lua::LANGUAGE.into())
        .expect("Failed to set Lua language");

    let Some(tree) = parser.parse(source, None) else {
        return Vec::new();
    };

    let mut entities = Vec::new();
    extract_from_node(tree.root_node(), source, &mut entities);
    entities
}

/// Recursively extract entities from AST nodes.
fn extract_from_node(node: tree_sitter::Node, source: &str, entities: &mut Vec<LocalEntity>) {
    if node.kind() == "function_call" {
        if let Some(entity) = try_extract_entity(node, source) {
            entities.push(entity);
            // Don't recurse into entity definitions - we've already processed the whole chain
            return;
        }
    }

    // Recurse into children
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        extract_from_node(child, source, entities);
    }
}

/// Try to extract an entity from a function call node.
fn try_extract_entity(call: tree_sitter::Node, source: &str) -> Option<LocalEntity> {
    // Find the root of the method chain
    let root_call = find_chain_root(call, source)?;

    // Get the function name
    let func_name = get_function_name(root_call, source)?;
    let kind = EntityKind::from_function_name(&func_name)?;

    // Get the entity name (first string argument)
    let name = get_first_string_arg(root_call, source)?;

    // Get the range of the entire chain
    let range = node_to_range(call);

    // Extract columns and from_table by walking the chain
    let (columns, from_table) = extract_chain_info(call, source);

    Some(LocalEntity {
        name,
        kind,
        range,
        columns,
        from_table,
    })
}

/// Find the root function call in a method chain.
fn find_chain_root<'a>(node: tree_sitter::Node<'a>, source: &str) -> Option<tree_sitter::Node<'a>> {
    // If this is a method call (first child is method_index_expression),
    // we need to find the innermost function_call
    let first_child = node.child(0)?;

    if first_child.kind() == "method_index_expression" {
        // Walk down to find the root
        find_root_in_chain(first_child, source)
    } else if first_child.kind() == "identifier" {
        // This is the root function call
        let name = node_text(first_child, source);
        if EntityKind::from_function_name(&name).is_some() {
            Some(node)
        } else {
            None
        }
    } else {
        None
    }
}

/// Find the root function call within a method chain.
fn find_root_in_chain<'a>(
    method_expr: tree_sitter::Node<'a>,
    source: &str,
) -> Option<tree_sitter::Node<'a>> {
    // method_index_expression structure:
    //   <object> (function_call or another method_index_expression)
    //   :
    //   identifier
    let object = method_expr.child(0)?;

    match object.kind() {
        "function_call" => {
            // Check if this function_call is a builder
            let first = object.child(0)?;
            if first.kind() == "identifier" {
                let name = node_text(first, source);
                if EntityKind::from_function_name(&name).is_some() {
                    return Some(object);
                }
            }
            // This might be a chained call, go deeper
            if first.kind() == "method_index_expression" {
                return find_root_in_chain(first, source);
            }
            None
        }
        "method_index_expression" => find_root_in_chain(object, source),
        _ => None,
    }
}

/// Get the function name from a function_call node.
fn get_function_name(call: tree_sitter::Node, source: &str) -> Option<String> {
    let first = call.child(0)?;
    if first.kind() == "identifier" {
        Some(node_text(first, source))
    } else {
        None
    }
}

/// Get the first string argument from a function call.
fn get_first_string_arg(call: tree_sitter::Node, source: &str) -> Option<String> {
    // Find the arguments node
    let mut cursor = call.walk();
    for child in call.children(&mut cursor) {
        if child.kind() == "arguments" {
            // Find the first string in arguments
            let mut arg_cursor = child.walk();
            for arg in child.children(&mut arg_cursor) {
                if arg.kind() == "string" {
                    return Some(get_string_content(arg, source));
                }
            }
        }
    }
    None
}

/// Get string content without quotes.
fn get_string_content(node: tree_sitter::Node, source: &str) -> String {
    let text = node_text(node, source);
    text.trim_matches('"').trim_matches('\'').to_string()
}

/// Extract columns and from_table from the method chain.
fn extract_chain_info(node: tree_sitter::Node, source: &str) -> (Vec<String>, Option<String>) {
    let mut columns = Vec::new();
    let mut from_table = None;

    // Walk the chain looking for :columns() and :from() calls
    walk_chain(node, source, &mut |method_name, args_node| {
        match method_name {
            "from" => {
                // Get the string argument
                if let Some(table) = get_first_string_in_node(args_node, source) {
                    from_table = Some(table);
                }
            }
            "columns" => {
                // Get column names from the table constructor
                if let Some(table_ctor) = find_table_constructor(args_node) {
                    columns = extract_column_names(table_ctor, source);
                }
            }
            _ => {}
        }
    });

    (columns, from_table)
}

/// Walk a method chain, calling the callback for each method.
fn walk_chain<F>(node: tree_sitter::Node, source: &str, callback: &mut F)
where
    F: FnMut(&str, tree_sitter::Node),
{
    // If this is a function_call with method_index_expression prefix
    if node.kind() == "function_call" {
        if let Some(first) = node.child(0) {
            if first.kind() == "method_index_expression" {
                // Get method name
                if let Some(method_name) = get_method_name(first, source) {
                    // Get arguments
                    if let Some(args) = find_arguments(node) {
                        callback(&method_name, args);
                    }
                }
                // Continue walking up the chain
                if let Some(inner_call) = first.child(0) {
                    if inner_call.kind() == "function_call" {
                        walk_chain(inner_call, source, callback);
                    }
                }
            }
        }
    }
}

/// Get method name from method_index_expression.
fn get_method_name(method_expr: tree_sitter::Node, source: &str) -> Option<String> {
    let mut cursor = method_expr.walk();
    let mut last_identifier: Option<tree_sitter::Node> = None;

    for child in method_expr.children(&mut cursor) {
        if child.kind() == "identifier" {
            last_identifier = Some(child);
        }
    }

    last_identifier.map(|n| node_text(n, source))
}

/// Find the arguments node in a function_call.
fn find_arguments(call: tree_sitter::Node) -> Option<tree_sitter::Node> {
    let mut cursor = call.walk();
    for child in call.children(&mut cursor) {
        if child.kind() == "arguments" {
            return Some(child);
        }
    }
    None
}

/// Get first string in a node subtree.
fn get_first_string_in_node(node: tree_sitter::Node, source: &str) -> Option<String> {
    if node.kind() == "string" {
        return Some(get_string_content(node, source));
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(s) = get_first_string_in_node(child, source) {
            return Some(s);
        }
    }
    None
}

/// Find a table_constructor in a node subtree.
fn find_table_constructor(node: tree_sitter::Node) -> Option<tree_sitter::Node> {
    if node.kind() == "table_constructor" {
        return Some(node);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(tc) = find_table_constructor(child) {
            return Some(tc);
        }
    }
    None
}

/// Extract column names from a table constructor.
fn extract_column_names(table: tree_sitter::Node, source: &str) -> Vec<String> {
    let mut columns = Vec::new();

    let mut cursor = table.walk();
    for child in table.children(&mut cursor) {
        if child.kind() == "field" {
            // Field structure: name = value
            if let Some(name_node) = child.child_by_field_name("name") {
                columns.push(node_text(name_node, source));
            } else if let Some(first) = child.child(0) {
                // For fields like `name = value`, first child is identifier
                if first.kind() == "identifier" {
                    columns.push(node_text(first, source));
                }
            }
        }
    }

    columns
}

/// Get text content of a node.
fn node_text(node: tree_sitter::Node, source: &str) -> String {
    source[node.start_byte()..node.end_byte()].to_string()
}

/// Convert tree-sitter node to LSP Range.
fn node_to_range(node: tree_sitter::Node) -> Range {
    Range {
        start: tower_lsp::lsp_types::Position {
            line: node.start_position().row as u32,
            character: node.start_position().column as u32,
        },
        end: tower_lsp::lsp_types::Position {
            line: node.end_position().row as u32,
            character: node.end_position().column as u32,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_source_entity() {
        let source = r#"source("orders")
    :from("raw.orders")
    :columns({
        id = pk(int64),
        customer_id = int64,
    })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "orders");
        assert_eq!(entities[0].kind, EntityKind::Source);
    }

    #[test]
    fn test_extract_table_entity() {
        let source = r#"table("customers")
    :from("raw.customers")
    :columns({
        id = pk(int64),
        name = string,
    })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "customers");
        assert_eq!(entities[0].kind, EntityKind::Table);
    }

    #[test]
    fn test_extract_fact_entity() {
        let source = r#"fact("sales")
    :source("orders")
    :measures({
        total_amount = sum("amount"),
    })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "sales");
        assert_eq!(entities[0].kind, EntityKind::Fact);
    }

    #[test]
    fn test_extract_dimension_entity() {
        let source = r#"dimension("product")
    :source("products")
    :columns({
        id = pk(int64),
        name = string,
    })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "product");
        assert_eq!(entities[0].kind, EntityKind::Dimension);
    }

    #[test]
    fn test_extract_query_entity() {
        let source = r#"query("top_customers")
    :select({ "customer_id", "total_sales" })
    :from("sales")"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "top_customers");
        assert_eq!(entities[0].kind, EntityKind::Query);
    }

    #[test]
    fn test_extract_report_entity() {
        let source = r#"report("monthly_sales")
    :title("Monthly Sales Report")"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "monthly_sales");
        assert_eq!(entities[0].kind, EntityKind::Report);
    }

    #[test]
    fn test_extract_multiple_entities() {
        let source = r#"source("orders"):from("raw.orders"):columns({ id = pk(int64) })
source("products"):from("raw.products"):columns({ id = pk(int64) })
fact("sales"):source("orders"):measures({ count = count() })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 3);
        assert_eq!(entities[0].name, "orders");
        assert_eq!(entities[1].name, "products");
        assert_eq!(entities[2].name, "sales");
    }

    #[test]
    fn test_extract_columns() {
        let source = r#"source("orders")
    :from("raw.orders")
    :columns({
        id = pk(int64),
        customer_id = int64,
        amount = decimal,
    })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].columns.len(), 3);
        assert!(entities[0].columns.contains(&"id".to_string()));
        assert!(entities[0].columns.contains(&"customer_id".to_string()));
        assert!(entities[0].columns.contains(&"amount".to_string()));
    }

    #[test]
    fn test_extract_from_table() {
        let source = r#"source("orders"):from("public.orders"):columns({ id = pk(int64) })"#;

        let entities = extract_entities(source);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].from_table, Some("public.orders".to_string()));
    }

    #[test]
    fn test_no_entities_in_empty_source() {
        let entities = extract_entities("");
        assert!(entities.is_empty());
    }

    #[test]
    fn test_no_entities_for_non_builder_functions() {
        let source = r#"print("hello")
local x = some_function("arg")"#;

        let entities = extract_entities(source);
        assert!(entities.is_empty());
    }
}
