//! SourceEntity → Lua emission.

use super::format::{quote_identifier, IndentWriter};
use super::EmitConfig;
use crate::model::types::DataType;
use crate::model::{ChangeTracking, SourceEntity};

/// Convert a DataType to its Lua representation.
fn datatype_to_lua(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Decimal(p, s) => format!("decimal({}, {})", p, s),
        DataType::String => "string".to_string(),
        DataType::Char(len) => format!("char({})", len),
        DataType::Varchar(len) => format!("varchar({})", len),
        DataType::Date => "date".to_string(),
        DataType::Time => "time".to_string(),
        DataType::Timestamp => "timestamp".to_string(),
        DataType::TimestampTz => "timestamptz".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::Json => "json".to_string(),
        DataType::Uuid => "uuid".to_string(),
    }
}

/// Emit a column definition.
fn emit_column(col_name: &str, col: &crate::model::SourceColumn, is_pk: bool) -> String {
    let type_str = datatype_to_lua(&col.data_type);

    let wrapped = if is_pk {
        format!("pk({})", type_str)
    } else if !col.nullable {
        format!("required({})", type_str)
    } else {
        format!("nullable({})", type_str)
    };

    format!("{} = {}", quote_identifier(col_name), wrapped)
}

/// Emit a SourceEntity to Lua using chained syntax.
/// Example output:
/// ```lua
/// source("orders")
///     :from("raw.orders")
///     :columns({
///         order_id = pk(int64),
///         customer_id = int64,
///     })
/// ```
pub fn emit_source(w: &mut IndentWriter, source: &SourceEntity, config: &EmitConfig) {
    // Comment with metadata
    if config.include_comments {
        if let Some(schema) = &source.schema {
            w.write_comment(&format!("Source: {} (schema: {})", source.name, schema));
        } else {
            w.write_comment(&format!("Source: {}", source.name));
        }
    }

    // Opening - source("name")
    w.write_line(&format!("source(\"{}\")", source.name));
    w.indent();

    // :from("schema.table")
    w.write_line(&format!(":from(\"{}\")", source.qualified_table_name()));

    // :columns({ ... })
    if !source.columns.is_empty() {
        w.write_line(":columns({");
        w.indent();

        // Sort columns for consistent output (primary key first, then alphabetically)
        let mut column_names: Vec<&String> = source.columns.keys().collect();
        column_names.sort_by(|a, b| {
            let a_is_pk = source.primary_key.contains(*a);
            let b_is_pk = source.primary_key.contains(*b);
            match (a_is_pk, b_is_pk) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a.cmp(b),
            }
        });

        for name in column_names {
            let col = &source.columns[name];
            let is_pk = source.primary_key.contains(name);
            w.write_line(&format!("{},", emit_column(name, col, is_pk)));
        }
        w.dedent();
        w.write_line("})");
    }

    // :metadata({ ... }) - for change tracking
    if let Some(tracking) = &source.change_tracking {
        w.write_line(":metadata({");
        w.indent();

        match tracking {
            ChangeTracking::AppendOnly { timestamp_column } => {
                w.write_line("change_tracking = APPEND_ONLY,");
                w.write_line(&format!("timestamp_column = \"{}\",", timestamp_column));
            }
            ChangeTracking::CDC {
                operation_column,
                timestamp_column,
            } => {
                w.write_line("change_tracking = CDC,");
                w.write_line(&format!("operation_column = \"{}\",", operation_column));
                w.write_line(&format!("timestamp_column = \"{}\",", timestamp_column));
            }
            ChangeTracking::FullSnapshot => {
                w.write_line("change_tracking = FULL_SNAPSHOT,");
            }
        }

        w.dedent();
        w.write_line("})");
    }

    w.dedent();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::emitter::format::Indent;
    use crate::model::{SourceColumn, SourceEntity};

    fn source_entity_fixture() -> SourceEntity {
        SourceEntity::new("orders", "orders")
            .with_schema("raw")
            .with_required_column("order_id", DataType::Int64)
            .with_required_column("customer_id", DataType::Int64)
            .with_nullable_column("total", DataType::Decimal(10, 2))
            .with_nullable_column("status", DataType::String)
            .with_primary_key(vec!["order_id"])
    }

    #[test]
    fn test_datatype_to_lua() {
        assert_eq!(datatype_to_lua(&DataType::Int64), "int64");
        assert_eq!(datatype_to_lua(&DataType::String), "string");
        assert_eq!(datatype_to_lua(&DataType::Decimal(10, 2)), "decimal(10, 2)");
        assert_eq!(datatype_to_lua(&DataType::Varchar(255)), "varchar(255)");
        assert_eq!(datatype_to_lua(&DataType::Bool), "bool");
    }

    #[test]
    fn test_emit_column_pk() {
        let col = SourceColumn::new("id", DataType::Int64, false);
        let result = emit_column("id", &col, true);
        assert_eq!(result, "id = pk(int64)");
    }

    #[test]
    fn test_emit_column_required() {
        let col = SourceColumn::new("name", DataType::String, false);
        let result = emit_column("name", &col, false);
        assert_eq!(result, "name = required(string)");
    }

    #[test]
    fn test_emit_column_nullable() {
        let col = SourceColumn::new("description", DataType::String, true);
        let result = emit_column("description", &col, false);
        assert_eq!(result, "description = nullable(string)");
    }

    #[test]
    fn test_emit_column_quoted_name() {
        let col = SourceColumn::new("order-id", DataType::Int64, false);
        let result = emit_column("order-id", &col, true);
        assert_eq!(result, "[\"order-id\"] = pk(int64)");
    }

    #[test]
    fn test_emit_source() {
        let source = source_entity_fixture();
        let config = EmitConfig::default();
        let mut w = IndentWriter::new(Indent::Spaces(4));

        emit_source(&mut w, &source, &config);
        let output = w.into_string();

        // New chained syntax
        assert!(output.contains("source(\"orders\")"));
        assert!(output.contains(":from(\"raw.orders\")"));
        assert!(output.contains(":columns({"));
        assert!(output.contains("order_id = pk(int64)"));
        assert!(output.contains("customer_id = required(int64)"));
        assert!(output.contains("total = nullable(decimal(10, 2))"));
    }

    #[test]
    fn test_emit_source_without_comments() {
        let source = source_entity_fixture();
        let config = EmitConfig::minimal();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_source(&mut w, &source, &config);
        let output = w.into_string();

        assert!(!output.contains("-- Source:"));
        // New chained syntax
        assert!(output.contains("source(\"orders\")"));
    }

    #[test]
    fn test_emit_source_with_change_tracking() {
        let source = SourceEntity::new("events", "events")
            .with_required_column("id", DataType::Int64)
            .with_required_column("created_at", DataType::Timestamp)
            .with_primary_key(vec!["id"])
            .with_change_tracking(ChangeTracking::AppendOnly {
                timestamp_column: "created_at".into(),
            });

        let config = EmitConfig::minimal();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_source(&mut w, &source, &config);
        let output = w.into_string();

        // New chained syntax with :metadata({ ... })
        assert!(output.contains(":metadata({"));
        assert!(output.contains("change_tracking = APPEND_ONLY"));
        assert!(output.contains("timestamp_column = \"created_at\""));
    }

    #[test]
    fn test_emit_source_multi_column_pk() {
        let source = SourceEntity::new("order_items", "order_items")
            .with_required_column("order_id", DataType::Int64)
            .with_required_column("item_id", DataType::Int64)
            .with_required_column("quantity", DataType::Int32)
            .with_primary_key(vec!["order_id", "item_id"]);

        let config = EmitConfig::minimal();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_source(&mut w, &source, &config);
        let output = w.into_string();

        // New chained syntax - primary key columns get pk() wrapper
        assert!(output.contains("order_id = pk(int64)"));
        assert!(output.contains("item_id = pk(int64)"));
        // No separate primary_key field in chained syntax (tracked via pk() wrapper)
    }
}
