//! Lua model emitter - serializes Model to Lua source code.
//!
//! This is the reverse of the `loader` module - instead of parsing Lua to Model,
//! we serialize Model to Lua for bootstrapping and documentation.
//!
//! # Example
//!
//! ```ignore
//! use mantis::model::emitter::{LuaEmitter, EmitConfig};
//!
//! let emitter = LuaEmitter::new(EmitConfig::default());
//! let lua_code = emitter.emit(&model);
//! ```

mod format;
mod relationship;
mod source;

pub use format::{escape_lua_string, quote_identifier, quote_string, Indent, IndentWriter};

use crate::model::{Model, Relationship, SourceEntity};
use format::IndentWriter as Writer;
use std::io;
use std::path::Path;

/// Configuration for Lua emission.
#[derive(Debug, Clone)]
pub struct EmitConfig {
    /// Indentation style (tabs or spaces).
    pub indent: Indent,
    /// Include metadata comments (e.g., data types, schema info).
    pub include_comments: bool,
    /// Group sources by schema in output.
    pub group_by_schema: bool,
    /// Minimum confidence for including inferred relationships.
    pub min_relationship_confidence: f64,
    /// Include inferred relationships (vs only FK-based).
    pub include_inferred_relationships: bool,
}

impl Default for EmitConfig {
    fn default() -> Self {
        Self {
            indent: Indent::Tabs,
            include_comments: true,
            group_by_schema: true,
            min_relationship_confidence: 0.5,
            include_inferred_relationships: true,
        }
    }
}

impl EmitConfig {
    /// Create a minimal config without comments.
    #[must_use]
    pub fn minimal() -> Self {
        Self {
            include_comments: false,
            group_by_schema: false,
            ..Default::default()
        }
    }

    /// Create a verbose config with all comments.
    #[must_use]
    pub fn verbose() -> Self {
        Self {
            include_comments: true,
            group_by_schema: true,
            include_inferred_relationships: true,
            ..Default::default()
        }
    }
}

/// Main Lua emitter for serializing Model to Lua source code.
pub struct LuaEmitter {
    config: EmitConfig,
}

impl LuaEmitter {
    /// Create a new emitter with the given configuration.
    #[must_use]
    pub fn new(config: EmitConfig) -> Self {
        Self { config }
    }

    /// Get the emitter configuration.
    #[must_use]
    pub fn config(&self) -> &EmitConfig {
        &self.config
    }

    /// Emit the entire model to a Lua string.
    #[must_use]
    pub fn emit(&self, model: &Model) -> String {
        let mut w = Writer::new(self.config.indent.clone());

        // Header
        self.emit_header(&mut w, model);

        // Sources section
        self.emit_sources_section(&mut w, model);

        // Relationships section
        self.emit_relationships_section(&mut w, model);

        // Facts placeholder/section
        if model.facts.is_empty() {
            self.emit_facts_placeholder(&mut w);
        } else {
            self.emit_facts_section(&mut w, model);
        }

        // Dimensions placeholder/section
        if model.dimensions.is_empty() {
            self.emit_dimensions_placeholder(&mut w);
        } else {
            self.emit_dimensions_section(&mut w, model);
        }

        w.into_string()
    }

    /// Emit only sources to a Lua string.
    #[must_use]
    pub fn emit_sources(&self, sources: &[&SourceEntity]) -> String {
        let mut w = Writer::new(self.config.indent.clone());
        for source in sources {
            source::emit_source(&mut w, source, &self.config);
            w.blank_line();
        }
        w.into_string()
    }

    /// Emit only relationships to a Lua string.
    #[must_use]
    pub fn emit_relationships(&self, relationships: &[&Relationship]) -> String {
        let mut w = Writer::new(self.config.indent.clone());
        for rel in relationships {
            relationship::emit_relationship(&mut w, rel, &self.config);
        }
        w.into_string()
    }

    /// Emit the model to a file.
    pub fn emit_to_file(&self, model: &Model, path: &Path) -> io::Result<()> {
        let content = self.emit(model);
        std::fs::write(path, content)
    }

    fn emit_header(&self, w: &mut Writer, _model: &Model) {
        w.write_line("-- ============================================================================");
        w.write_line("-- Mantis Model");
        w.write_line("-- ============================================================================");
    }

    fn emit_sources_section(&self, w: &mut Writer, model: &Model) {
        if model.sources.is_empty() {
            return;
        }

        w.write_section_header("SOURCES");

        if self.config.group_by_schema {
            self.emit_sources_grouped_by_schema(w, model);
        } else {
            for source in model.sources.values() {
                source::emit_source(w, source, &self.config);
                w.blank_line();
            }
        }
    }

    fn emit_sources_grouped_by_schema(&self, w: &mut Writer, model: &Model) {
        use std::collections::BTreeMap;

        // Group sources by schema
        let mut by_schema: BTreeMap<Option<&str>, Vec<&SourceEntity>> = BTreeMap::new();
        for source in model.sources.values() {
            by_schema
                .entry(source.schema.as_deref())
                .or_default()
                .push(source);
        }

        for (schema, sources) in by_schema {
            if self.config.include_comments {
                match schema {
                    Some(s) => w.write_comment(&format!("Schema: {}", s)),
                    None => w.write_comment("Schema: (default)"),
                }
                w.blank_line();
            }

            for source in sources {
                source::emit_source(w, source, &self.config);
                w.blank_line();
            }
        }
    }

    fn emit_relationships_section(&self, w: &mut Writer, model: &Model) {
        if model.relationships.is_empty() {
            return;
        }

        w.write_section_header("RELATIONSHIPS");

        for rel in &model.relationships {
            relationship::emit_relationship(w, rel, &self.config);
        }
    }

    fn emit_facts_placeholder(&self, w: &mut Writer) {
        w.write_section_header("FACTS (define your analytics tables)");

        if self.config.include_comments {
            w.write_line("-- TODO: Define fact tables");
            w.write_line("-- fact \"fact_orders\" {");
            w.write_line("--     grain = { orders.order_id },");
            w.write_line("--     measures = { revenue = sum \"total\" },");
            w.write_line("-- }");
        }
    }

    fn emit_facts_section(&self, w: &mut Writer, _model: &Model) {
        w.write_section_header("FACTS");
        // TODO: Implement fact emission when FactDefinition is finalized
        w.write_comment("Fact emission not yet implemented");
    }

    fn emit_dimensions_placeholder(&self, w: &mut Writer) {
        w.write_section_header("DIMENSIONS (define your dimension tables)");

        if self.config.include_comments {
            w.write_line("-- TODO: Define dimension tables");
            w.write_line("-- dimension \"dim_customers\" {");
            w.write_line("--     source = \"customers\",");
            w.write_line("--     columns = { \"customer_id\", \"name\" },");
            w.write_line("-- }");
        }
    }

    fn emit_dimensions_section(&self, w: &mut Writer, _model: &Model) {
        w.write_section_header("DIMENSIONS");
        // TODO: Implement dimension emission when DimensionDefinition is finalized
        w.write_comment("Dimension emission not yet implemented");
    }
}

impl Default for LuaEmitter {
    fn default() -> Self {
        Self::new(EmitConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::types::DataType;
    use crate::model::{Cardinality, Relationship};

    #[test]
    fn test_emit_config_default() {
        let config = EmitConfig::default();
        assert!(config.include_comments);
        assert!(config.group_by_schema);
        assert!(config.include_inferred_relationships);
    }

    #[test]
    fn test_emit_config_minimal() {
        let config = EmitConfig::minimal();
        assert!(!config.include_comments);
        assert!(!config.group_by_schema);
    }

    #[test]
    fn test_emitter_empty_model() {
        let model = Model::new();
        let emitter = LuaEmitter::default();
        let output = emitter.emit(&model);

        assert!(output.contains("Mantis Model"));
        assert!(output.contains("====="));
    }

    #[test]
    fn test_emitter_with_sources() {
        let mut model = Model::new();
        model.add_source(
            SourceEntity::new("orders", "orders")
                .with_schema("public")
                .with_required_column("id", DataType::Int64)
                .with_primary_key(vec!["id"]),
        );

        let emitter = LuaEmitter::default();
        let output = emitter.emit(&model);

        assert!(output.contains("SOURCES"));
        // New chained syntax
        assert!(output.contains("source(\"orders\")"));
    }

    #[test]
    fn test_emit_sources_only() {
        let source = SourceEntity::new("users", "users")
            .with_required_column("user_id", DataType::Int64)
            .with_primary_key(vec!["user_id"]);

        let emitter = LuaEmitter::default();
        let output = emitter.emit_sources(&[&source]);

        // New chained syntax
        assert!(output.contains("source(\"users\")"));
        assert!(output.contains("user_id"));
    }

    #[test]
    fn test_emit_relationships_only() {
        let rel = Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        );

        let emitter = LuaEmitter::new(EmitConfig::minimal());
        let output = emitter.emit_relationships(&[&rel]);

        assert!(output.contains("link(orders.customer_id, customers.customer_id)"));
    }

    #[test]
    fn test_round_trip_sources() {
        use crate::model::loader::load_model_from_str;

        // Create a model with sources
        let mut model = Model::new();
        model.add_source(
            SourceEntity::new("orders", "orders")
                .with_schema("public")
                .with_required_column("order_id", DataType::Int64)
                .with_required_column("customer_id", DataType::Int64)
                .with_nullable_column("total", DataType::Decimal(10, 2))
                .with_primary_key(vec!["order_id"]),
        );
        model.add_source(
            SourceEntity::new("customers", "customers")
                .with_schema("public")
                .with_required_column("customer_id", DataType::Int64)
                .with_required_column("name", DataType::String)
                .with_primary_key(vec!["customer_id"]),
        );

        // Emit to Lua
        let emitter = LuaEmitter::new(EmitConfig::minimal());
        let lua = emitter.emit(&model);

        // Parse back
        let parsed = load_model_from_str(&lua, "test.lua").expect("Failed to parse emitted Lua");

        // Verify sources were round-tripped
        assert_eq!(parsed.sources.len(), model.sources.len());
        assert!(parsed.sources.contains_key("orders"));
        assert!(parsed.sources.contains_key("customers"));

        // Verify column counts
        let orders = parsed.sources.get("orders").unwrap();
        assert_eq!(orders.columns.len(), 3);
        assert!(orders.columns.contains_key("order_id"));
        assert!(orders.columns.contains_key("customer_id"));
        assert!(orders.columns.contains_key("total"));
    }

    #[test]
    fn test_full_model_output_format() {
        let mut model = Model::new();
        model.add_source(
            SourceEntity::new("orders", "orders")
                .with_schema("public")
                .with_required_column("id", DataType::Int64)
                .with_primary_key(vec!["id"]),
        );
        model.add_relationship(Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        ));

        let emitter = LuaEmitter::default();
        let output = emitter.emit(&model);

        // Verify section structure
        assert!(output.contains("-- Mantis Model"));
        assert!(output.contains("-- SOURCES"));
        assert!(output.contains("-- RELATIONSHIPS"));
        assert!(output.contains("-- FACTS"));
        assert!(output.contains("-- DIMENSIONS"));
    }
}
