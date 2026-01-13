//! Lua formatting utilities for model emission.
//!
//! Provides string escaping, identifier quoting, and indentation management.

/// Lua reserved words that require quoting when used as identifiers.
const LUA_RESERVED: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "goto", "if", "in",
    "local", "nil", "not", "or", "repeat", "return", "then", "true", "until", "while",
];

/// Escape a string for use in a Lua string literal.
///
/// Handles backslashes, quotes, and control characters.
#[must_use]
pub fn escape_lua_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '"' => result.push_str("\\\""),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\0' => result.push_str("\\0"),
            c => result.push(c),
        }
    }
    result
}

/// Check if a string is a valid Lua identifier.
///
/// Valid identifiers start with a letter or underscore, followed by
/// letters, digits, or underscores.
#[must_use]
pub fn is_valid_lua_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Check if a string is a Lua reserved word.
#[must_use]
pub fn is_lua_reserved(s: &str) -> bool {
    LUA_RESERVED.contains(&s)
}

/// Quote an identifier if needed (reserved words, special chars).
///
/// Returns the identifier as-is if it's a valid Lua identifier,
/// otherwise wraps it in bracket notation: `["name"]`.
#[must_use]
pub fn quote_identifier(name: &str) -> String {
    if is_valid_lua_identifier(name) && !is_lua_reserved(name) {
        name.to_string()
    } else {
        format!("[\"{}\"]", escape_lua_string(name))
    }
}

/// Quote a string literal with double quotes.
#[must_use]
pub fn quote_string(s: &str) -> String {
    format!("\"{}\"", escape_lua_string(s))
}

/// Indentation style for emitted Lua.
#[derive(Debug, Clone, Default)]
pub enum Indent {
    /// Use tabs for indentation (default).
    #[default]
    Tabs,
    /// Use spaces for indentation.
    Spaces(usize),
}

impl Indent {
    /// Get the string representation of one indent level.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Indent::Tabs => "\t",
            Indent::Spaces(2) => "  ",
            Indent::Spaces(4) => "    ",
            // For other sizes, we'll handle dynamically in IndentWriter
            _ => "    ",
        }
    }

    /// Get the indent string (may allocate for non-standard sizes).
    #[must_use]
    pub fn to_string_owned(&self) -> String {
        match self {
            Indent::Tabs => "\t".to_string(),
            Indent::Spaces(n) => " ".repeat(*n),
        }
    }
}

/// A writer that manages indentation for Lua output.
pub struct IndentWriter {
    buffer: String,
    indent_str: String,
    current_indent: usize,
    at_line_start: bool,
}

impl IndentWriter {
    /// Create a new indent writer with the specified indentation style.
    #[must_use]
    pub fn new(indent: Indent) -> Self {
        Self {
            buffer: String::new(),
            indent_str: indent.to_string_owned(),
            current_indent: 0,
            at_line_start: true,
        }
    }

    /// Increase indentation level.
    pub fn indent(&mut self) {
        self.current_indent += 1;
    }

    /// Decrease indentation level.
    pub fn dedent(&mut self) {
        self.current_indent = self.current_indent.saturating_sub(1);
    }

    /// Write the current indentation if at line start.
    fn write_indent_if_needed(&mut self) {
        if self.at_line_start && self.current_indent > 0 {
            for _ in 0..self.current_indent {
                self.buffer.push_str(&self.indent_str);
            }
            self.at_line_start = false;
        }
    }

    /// Write a complete line (with newline at end).
    pub fn write_line(&mut self, s: &str) {
        self.write_indent_if_needed();
        self.buffer.push_str(s);
        self.buffer.push('\n');
        self.at_line_start = true;
    }

    /// Write inline content (no automatic newline).
    pub fn write_inline(&mut self, s: &str) {
        self.write_indent_if_needed();
        self.buffer.push_str(s);
        self.at_line_start = false;
    }

    /// Write a blank line.
    pub fn blank_line(&mut self) {
        self.buffer.push('\n');
        self.at_line_start = true;
    }

    /// Write a comment line.
    pub fn write_comment(&mut self, comment: &str) {
        self.write_line(&format!("-- {}", comment));
    }

    /// Write a section header with separators.
    pub fn write_section_header(&mut self, title: &str) {
        self.blank_line();
        self.write_line("-- ============================================================================");
        self.write_line(&format!("-- {}", title));
        self.write_line("-- ============================================================================");
        self.blank_line();
    }

    /// Consume the writer and return the final string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.buffer
    }

    /// Get a reference to the current buffer.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.buffer
    }
}

impl Default for IndentWriter {
    fn default() -> Self {
        Self::new(Indent::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_lua_string() {
        assert_eq!(escape_lua_string("hello"), "hello");
        assert_eq!(escape_lua_string("he\"llo"), "he\\\"llo");
        assert_eq!(escape_lua_string("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_lua_string("path\\to\\file"), "path\\\\to\\\\file");
        assert_eq!(escape_lua_string("tab\there"), "tab\\there");
    }

    #[test]
    fn test_is_valid_lua_identifier() {
        assert!(is_valid_lua_identifier("foo"));
        assert!(is_valid_lua_identifier("_bar"));
        assert!(is_valid_lua_identifier("baz123"));
        assert!(is_valid_lua_identifier("_"));
        assert!(is_valid_lua_identifier("CamelCase"));

        assert!(!is_valid_lua_identifier("123abc"));
        assert!(!is_valid_lua_identifier("foo-bar"));
        assert!(!is_valid_lua_identifier("foo.bar"));
        assert!(!is_valid_lua_identifier(""));
        assert!(!is_valid_lua_identifier("foo bar"));
    }

    #[test]
    fn test_is_lua_reserved() {
        assert!(is_lua_reserved("and"));
        assert!(is_lua_reserved("function"));
        assert!(is_lua_reserved("nil"));
        assert!(is_lua_reserved("true"));

        assert!(!is_lua_reserved("foo"));
        assert!(!is_lua_reserved("And")); // Case-sensitive
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("foo"), "foo");
        assert_eq!(quote_identifier("_bar"), "_bar");
        assert_eq!(quote_identifier("and"), "[\"and\"]");
        assert_eq!(quote_identifier("foo-bar"), "[\"foo-bar\"]");
        assert_eq!(quote_identifier("123abc"), "[\"123abc\"]");
        assert_eq!(quote_identifier("name\"quoted"), "[\"name\\\"quoted\"]");
    }

    #[test]
    fn test_quote_string() {
        assert_eq!(quote_string("hello"), "\"hello\"");
        assert_eq!(quote_string("hello\"world"), "\"hello\\\"world\"");
    }

    #[test]
    fn test_indent_writer_basic() {
        let mut w = IndentWriter::new(Indent::Spaces(2));
        w.write_line("line1");
        w.indent();
        w.write_line("line2");
        w.dedent();
        w.write_line("line3");

        assert_eq!(w.into_string(), "line1\n  line2\nline3\n");
    }

    #[test]
    fn test_indent_writer_nested() {
        let mut w = IndentWriter::new(Indent::Tabs);
        w.write_line("table {");
        w.indent();
        w.write_line("nested = {");
        w.indent();
        w.write_line("value = 1,");
        w.dedent();
        w.write_line("},");
        w.dedent();
        w.write_line("}");

        let expected = "table {\n\tnested = {\n\t\tvalue = 1,\n\t},\n}\n";
        assert_eq!(w.into_string(), expected);
    }

    #[test]
    fn test_indent_writer_inline() {
        let mut w = IndentWriter::new(Indent::Spaces(4));
        w.write_inline("a = ");
        w.write_inline("1");
        w.write_line(",");

        assert_eq!(w.into_string(), "a = 1,\n");
    }

    #[test]
    fn test_indent_writer_blank_line() {
        let mut w = IndentWriter::default();
        w.write_line("section1");
        w.blank_line();
        w.write_line("section2");

        assert_eq!(w.into_string(), "section1\n\nsection2\n");
    }

    #[test]
    fn test_indent_writer_comment() {
        let mut w = IndentWriter::default();
        w.indent();
        w.write_comment("This is a comment");

        assert_eq!(w.into_string(), "\t-- This is a comment\n");
    }

    #[test]
    fn test_indent_writer_section_header() {
        let mut w = IndentWriter::default();
        w.write_section_header("SOURCES");

        let output = w.into_string();
        assert!(output.contains("-- SOURCES"));
        assert!(output.contains("============"));
    }

    #[test]
    fn test_dedent_saturates() {
        let mut w = IndentWriter::default();
        w.dedent(); // Should not panic
        w.dedent(); // Still should not panic
        w.write_line("no indent");

        assert_eq!(w.into_string(), "no indent\n");
    }
}
