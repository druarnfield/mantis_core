//! Per-document state and tree-sitter parsing

use tower_lsp::lsp_types::{Position, Range, Url};

/// State for a single open document
pub struct DocumentState {
    /// Document URI
    pub uri: Url,
    /// Document version (incremented on each change)
    pub version: i32,
    /// Full source text
    pub source: String,
    /// Parsed tree-sitter tree
    pub tree: tree_sitter::Tree,
}

impl DocumentState {
    /// Create a new document state by parsing the source
    pub fn new(uri: Url, version: i32, source: String) -> Self {
        let tree = parse_lua(&source);
        Self {
            uri,
            version,
            source,
            tree,
        }
    }

    /// Update the document with new content
    pub fn update(&mut self, version: i32, source: String) {
        self.version = version;
        self.source = source;
        self.tree = parse_lua(&self.source);
    }

    /// Get the tree-sitter node at the given LSP position
    pub fn node_at_position(&self, pos: Position) -> Option<tree_sitter::Node<'_>> {
        let point = tree_sitter::Point {
            row: pos.line as usize,
            column: lsp_column_to_byte_column(&self.source, pos),
        };
        self.tree
            .root_node()
            .descendant_for_point_range(point, point)
    }
}

/// Parse Lua source into a tree-sitter tree
fn parse_lua(source: &str) -> tree_sitter::Tree {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_lua::LANGUAGE.into())
        .expect("Failed to load Lua grammar");
    parser.parse(source, None).expect("Failed to parse")
}

/// Convert LSP position (line, character as UTF-16 code units) to byte offset
pub fn lsp_position_to_byte_offset(source: &str, pos: Position) -> usize {
    let mut offset = 0;
    for (line_num, line) in source.lines().enumerate() {
        if line_num == pos.line as usize {
            // LSP uses UTF-16 code units for character position
            offset += utf16_offset_to_byte_offset(line, pos.character as usize);
            break;
        }
        offset += line.len() + 1; // +1 for newline
    }
    offset
}

/// Convert byte offset to LSP position
pub fn byte_offset_to_lsp_position(source: &str, offset: usize) -> Position {
    let mut line = 0;
    let mut col_bytes = 0;
    let mut current_offset = 0;

    for (line_num, line_text) in source.lines().enumerate() {
        let line_end = current_offset + line_text.len();
        if offset <= line_end {
            line = line_num;
            col_bytes = offset - current_offset;
            break;
        }
        current_offset = line_end + 1; // +1 for newline
    }

    // Convert byte column to UTF-16 code units
    let line_text = source.lines().nth(line).unwrap_or("");
    let character = byte_offset_to_utf16_offset(line_text, col_bytes);

    Position {
        line: line as u32,
        character: character as u32,
    }
}

/// Convert tree-sitter node to LSP range
pub fn node_to_lsp_range(node: &tree_sitter::Node, source: &str) -> Range {
    let start = byte_offset_to_lsp_position(source, node.start_byte());
    let end = byte_offset_to_lsp_position(source, node.end_byte());
    Range { start, end }
}

/// Convert UTF-16 offset to byte offset within a line
fn utf16_offset_to_byte_offset(line: &str, utf16_offset: usize) -> usize {
    let mut utf16_count = 0;
    for (byte_idx, ch) in line.char_indices() {
        if utf16_count >= utf16_offset {
            return byte_idx;
        }
        utf16_count += ch.len_utf16();
    }
    line.len()
}

/// Convert byte offset to UTF-16 offset within a line
fn byte_offset_to_utf16_offset(line: &str, byte_offset: usize) -> usize {
    line[..byte_offset.min(line.len())]
        .chars()
        .map(|c| c.len_utf16())
        .sum()
}

/// Convert LSP column (UTF-16) to byte column for tree-sitter
fn lsp_column_to_byte_column(source: &str, pos: Position) -> usize {
    let line = source.lines().nth(pos.line as usize).unwrap_or("");
    utf16_offset_to_byte_offset(line, pos.character as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_source() {
        let source = r#"source("orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );
        assert!(!doc.tree.root_node().has_error());
    }

    #[test]
    fn test_position_conversion_ascii() {
        let source = "line1\nline2\nline3";

        // Start of line 2
        let pos = Position {
            line: 1,
            character: 0,
        };
        let offset = lsp_position_to_byte_offset(source, pos);
        assert_eq!(offset, 6); // "line1\n" = 6 bytes

        // Round-trip
        let back = byte_offset_to_lsp_position(source, offset);
        assert_eq!(back, pos);
    }

    #[test]
    fn test_position_conversion_unicode() {
        let source = "hello\nworld";

        // 'e' is 1 byte, 1 UTF-16 code unit
        // Position after 'h' and 'e' (character 2 in UTF-16)
        let pos = Position {
            line: 0,
            character: 2,
        };
        let offset = lsp_position_to_byte_offset(source, pos);
        assert_eq!(offset, 2); // 'h' (1) + 'e' (1) = 2 bytes
    }

    #[test]
    fn test_position_conversion_unicode_multibyte() {
        // Test with actual multi-byte unicode
        let source = "ab\ncd";

        let pos = Position {
            line: 0,
            character: 2,
        };
        let offset = lsp_position_to_byte_offset(source, pos);
        assert_eq!(offset, 2);

        // Round-trip
        let back = byte_offset_to_lsp_position(source, offset);
        assert_eq!(back, pos);
    }

    #[test]
    fn test_utf16_conversion() {
        // Test UTF-16 offset to byte offset
        let line = "hello";
        assert_eq!(utf16_offset_to_byte_offset(line, 0), 0);
        assert_eq!(utf16_offset_to_byte_offset(line, 1), 1);
        assert_eq!(utf16_offset_to_byte_offset(line, 5), 5);
    }

    #[test]
    fn test_node_at_position() {
        let source = r#"source("orders"):columns({ pk("id") })"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position inside "orders" string (character 8 is the 'o' in orders)
        // tree-sitter-lua returns "string_content" for text inside quotes
        let pos = Position {
            line: 0,
            character: 8,
        };
        let node = doc.node_at_position(pos);
        assert!(node.is_some());
        let node = node.unwrap();
        assert_eq!(node.kind(), "string_content");

        // Verify the parent is a string node
        let parent = node.parent();
        assert!(parent.is_some());
        assert_eq!(parent.unwrap().kind(), "string");
    }

    #[test]
    fn test_node_to_lsp_range() {
        let source = r#"source("orders")"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let root = doc.tree.root_node();
        let range = node_to_lsp_range(&root, &doc.source);

        assert_eq!(range.start.line, 0);
        assert_eq!(range.start.character, 0);
        assert_eq!(range.end.line, 0);
        assert_eq!(range.end.character, 16);
    }

    #[test]
    fn test_document_update() {
        let mut doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            "source(\"orders\")".to_string(),
        );

        assert_eq!(doc.version, 1);
        assert!(!doc.tree.root_node().has_error());

        doc.update(2, "source(\"customers\")".to_string());

        assert_eq!(doc.version, 2);
        assert!(!doc.tree.root_node().has_error());
        assert!(doc.source.contains("customers"));
    }
}
