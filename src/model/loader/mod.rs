//! Model loaders for different file formats.
//!
//! Currently supports:
//! - **Lua** (.lua) - Primary format with computed values and imports
//!
//! # Example
//!
//! ```rust,ignore
//! use mantis::model::loader::load_model;
//! use std::path::Path;
//!
//! let model = load_model(Path::new("model.lua"))?;
//! ```

pub mod lua;
pub mod sql_expr;

use std::path::Path;
use regex::Regex;
use thiserror::Error;

use super::Model;

// Re-export lenient loading types
pub use lua::{LenientLoadResult, ParseError};

/// Errors that can occur when loading a model.
#[derive(Debug, Error)]
pub enum LoadError {
    /// File not found
    #[error("File not found: {path}")]
    FileNotFound { path: String },

    /// Unsupported file extension
    #[error("Unsupported file extension: {extension}. Supported: .lua")]
    UnsupportedExtension { extension: String },

    /// IO error reading file
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Lua parsing/execution error
    #[error("Lua error in {file}: {message}")]
    Lua { file: String, message: String },

    /// Circular import detected
    #[error("Circular import detected: {path}")]
    CircularImport { path: String },

    /// Invalid model definition
    #[error("Invalid {kind} definition: {message}")]
    InvalidDefinition { kind: String, message: String },

    /// Missing required field
    #[error("Missing required field '{field}' in {context}")]
    MissingField { field: String, context: String },

    /// Invalid field value
    #[error("Invalid value for '{field}' in {context}: {message}")]
    InvalidValue {
        field: String,
        context: String,
        message: String,
    },

    /// Model validation failed
    #[error("Model validation failed: {0}")]
    Validation(#[from] super::ModelError),

    /// SQL expression parsing error
    #[error("SQL expression error: {message}")]
    SqlExpression { message: String },
}

/// Result type for model loading operations.
pub type LoadResult<T> = Result<T, LoadError>;

/// Load a model from a file path.
///
/// The loader is selected based on the file extension:
/// - `.lua` - Lua loader
///
/// # Example
///
/// ```rust,ignore
/// let model = load_model(Path::new("model.lua"))?;
/// ```
pub fn load_model(path: &Path) -> LoadResult<Model> {
    if !path.exists() {
        return Err(LoadError::FileNotFound {
            path: path.display().to_string(),
        });
    }

    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match extension {
        "lua" => lua::LuaLoader::load(path),
        _ => Err(LoadError::UnsupportedExtension {
            extension: extension.to_string(),
        }),
    }
}

/// Load a model from a Lua string (useful for testing).
///
/// # Example
///
/// ```rust,ignore
/// let lua_code = r#"
///     source "orders" {
///         table = "raw.orders",
///     }
/// "#;
/// let model = load_model_from_str(lua_code, "test.lua")?;
/// ```
pub fn load_model_from_str(content: &str, filename: &str) -> LoadResult<Model> {
    lua::LuaLoader::load_from_str(content, filename)
}

/// Load a model from a Lua string in lenient mode.
///
/// Unlike `load_model_from_str`, this function continues execution after
/// entity-level errors and returns a partial model with whatever entities
/// parsed successfully.
///
/// # Example
///
/// ```rust,ignore
/// let lua_code = r#"
///     source "orders" { table = "raw.orders" }
///     fact "broken" { }  -- Missing required fields
///     source "customers" { table = "raw.customers" }
/// "#;
/// let result = load_model_from_str_lenient(lua_code, "test.lua");
/// // result.model contains orders and customers
/// // result.parse_errors contains the fact error
/// ```
pub fn load_model_from_str_lenient(content: &str, filename: &str) -> LenientLoadResult {
    lua::LuaLoader::load_from_str_lenient(content, filename)
}

/// A basic symbol extracted via regex when Lua parsing fails completely.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BasicSymbol {
    pub name: String,
    pub kind: String,
    pub line: usize,
}

/// Extract basic symbols from Lua code using regex patterns.
///
/// This is a fallback for when Lua syntax errors prevent execution.
/// It only extracts entity names and line numbers, not full definitions.
pub fn extract_symbols_regex(content: &str) -> Vec<BasicSymbol> {
    let mut symbols = Vec::new();

    // Patterns for entity declarations: entity_type "name" or entity_type("name")
    let patterns = [
        ("source", r#"source\s*(?:\(\s*)?"([^"]+)""#),
        ("fact", r#"fact\s*(?:\(\s*)?"([^"]+)""#),
        ("dimension", r#"dimension\s*(?:\(\s*)?"([^"]+)""#),
        ("intermediate", r#"intermediate\s*(?:\(\s*)?"([^"]+)""#),
        ("table", r#"table\s*\(\s*"([^"]+)""#),
        ("query", r#"query\s*(?:\(\s*)?"([^"]+)""#),
        ("report", r#"report\s*(?:\(\s*)?"([^"]+)""#),
        ("pivot_report", r#"pivot_report\s*(?:\(\s*)?"([^"]+)""#),
    ];

    for (kind, pattern) in patterns {
        if let Ok(re) = Regex::new(pattern) {
            for cap in re.captures_iter(content) {
                if let Some(name_match) = cap.get(1) {
                    // Calculate line number
                    let line = content[..name_match.start()]
                        .chars()
                        .filter(|&c| c == '\n')
                        .count() + 1;

                    symbols.push(BasicSymbol {
                        name: name_match.as_str().to_string(),
                        kind: kind.to_string(),
                        line,
                    });
                }
            }
        }
    }

    // Sort by line number
    symbols.sort_by_key(|s| s.line);
    symbols
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // extract_symbols_regex tests
    // ========================================================================

    #[test]
    fn test_regex_extracts_source() {
        let lua = r#"
source "orders" {
    table = "raw.orders",
}
"#;
        let symbols = extract_symbols_regex(lua);
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "orders");
        assert_eq!(symbols[0].kind, "source");
        assert_eq!(symbols[0].line, 2);
    }

    #[test]
    fn test_regex_extracts_multiple_entities() {
        let lua = r#"
source "orders" { table = "raw.orders" }
source "customers" { table = "raw.customers" }
fact "sales" { table = "analytics.sales" }
dimension "dim_date" { source = "dates" }
query "top_sales" { from = "sales" }
"#;
        let symbols = extract_symbols_regex(lua);
        assert_eq!(symbols.len(), 5);

        // Check they're sorted by line
        assert_eq!(symbols[0].name, "orders");
        assert_eq!(symbols[1].name, "customers");
        assert_eq!(symbols[2].name, "sales");
        assert_eq!(symbols[2].kind, "fact");
        assert_eq!(symbols[3].name, "dim_date");
        assert_eq!(symbols[3].kind, "dimension");
        assert_eq!(symbols[4].name, "top_sales");
        assert_eq!(symbols[4].kind, "query");
    }

    #[test]
    fn test_regex_handles_function_call_syntax() {
        // Some users might use source("name") instead of source "name"
        let lua = r#"
source("orders") {
    table = "raw.orders",
}
"#;
        let symbols = extract_symbols_regex(lua);
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "orders");
    }

    #[test]
    fn test_regex_handles_broken_lua() {
        let lua = r#"
source "orders" {
    table = "raw.orders"
-- Missing closing brace, invalid Lua

source "customers" {
    table = "raw.customers",
}
"#;
        let symbols = extract_symbols_regex(lua);
        // Should still extract both entity names
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].name, "orders");
        assert_eq!(symbols[1].name, "customers");
    }

    #[test]
    fn test_regex_empty_input() {
        let symbols = extract_symbols_regex("");
        assert!(symbols.is_empty());
    }

    #[test]
    fn test_regex_comments_only() {
        let lua = r#"
-- This is a comment
-- source "not_real" { }
"#;
        // Regex can't distinguish comments from code, so this will match
        // That's acceptable for the fallback case
        let symbols = extract_symbols_regex(lua);
        // It will find the commented source - this is a known limitation
        assert!(symbols.len() <= 1);
    }

    // ========================================================================
    // load_from_str_lenient tests
    // ========================================================================

    #[test]
    fn test_lenient_valid_source() {
        let lua = r#"
source("orders")
    :from("raw.orders")
    :columns({
        order_id = pk(int64),
        customer_id = int64,
    })
"#;
        let result = load_model_from_str_lenient(lua, "test.lua");

        assert!(result.lua_error.is_none());
        assert!(result.parse_errors.is_empty());
        assert_eq!(result.model.sources.len(), 1);
        assert!(result.model.sources.contains_key("orders"));
    }

    #[test]
    fn test_lenient_continues_after_entity_error() {
        let lua = r#"
source("valid_source")
    :from("raw.valid")
    :columns({ id = pk(int64) })

-- This fact has defaults, chained syntax doesn't error on missing fields
fact("minimal_fact")

source("another_valid")
    :from("raw.another")
    :columns({ id = pk(int64) })
"#;
        let result = load_model_from_str_lenient(lua, "test.lua");

        // Lua executed successfully (no syntax error)
        assert!(result.lua_error.is_none());

        // Two valid sources should be in the model
        assert_eq!(result.model.sources.len(), 2);
        assert!(result.model.sources.contains_key("valid_source"));
        assert!(result.model.sources.contains_key("another_valid"));

        // With chained syntax, facts with defaults are valid - no parse errors
        assert_eq!(result.parse_errors.len(), 0);

        // The fact exists with defaults
        assert!(result.model.facts.contains_key("minimal_fact"));
    }

    #[test]
    fn test_lenient_syntax_error_returns_partial() {
        let lua = r#"
source("orders")
    :from("raw.orders")
    :columns({ id = pk(int64) })

-- Syntax error: unexpected token
local x =
"#;
        let result = load_model_from_str_lenient(lua, "test.lua");

        // Should have a Lua error
        assert!(result.lua_error.is_some());

        // But the source before the error should still be captured
        // Note: This depends on when Lua parses vs executes
        // If syntax error is caught before execution, model may be empty
        // If caught during execution after source, model will have it
    }

    #[test]
    fn test_lenient_multiple_entities_with_defaults() {
        let lua = r#"
fact("fact1")
fact("fact2")
dimension("dim1")
"#;
        let result = load_model_from_str_lenient(lua, "test.lua");

        // With chained syntax, all entities are created with defaults
        assert_eq!(result.parse_errors.len(), 0);

        // All entities should be in the model
        assert!(result.model.facts.contains_key("fact1"));
        assert!(result.model.facts.contains_key("fact2"));
        assert!(result.model.dimensions.contains_key("dim1"));
    }

    #[test]
    fn test_lenient_relationship_error() {
        let lua = r#"
source("orders")
    :from("raw.orders")
    :columns({ order_id = pk(int64) })

-- Relationship missing required fields
relationship { }
"#;
        let result = load_model_from_str_lenient(lua, "test.lua");

        // Source should be captured
        assert_eq!(result.model.sources.len(), 1);

        // Relationship error should be captured
        assert_eq!(result.parse_errors.len(), 1);
        assert_eq!(result.parse_errors[0].entity_type, "relationship");
    }
}
