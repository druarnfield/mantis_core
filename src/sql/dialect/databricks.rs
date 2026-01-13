//! Databricks (Spark SQL) dialect.
//!
//! Databricks features:
//! - Backtick identifier quoting
//! - Delta Lake table format
//! - Unity Catalog for governance
//! - MERGE INTO support
//! - QUALIFY clause (Databricks SQL)
//! - Native PIVOT/UNPIVOT

use super::helpers;
use super::SqlDialect;

/// Databricks (Spark SQL) dialect.
#[derive(Debug, Clone, Copy)]
pub struct Databricks;

impl SqlDialect for Databricks {
    fn name(&self) -> &'static str {
        "databricks"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_backtick(ident)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_literal(b)
    }

    fn supports_native_pivot(&self) -> bool {
        true
    }

    fn supports_qualify(&self) -> bool {
        true
    }

    fn supports_groups_frame(&self) -> bool {
        true
    }

    fn supports_named_windows(&self) -> bool {
        true
    }

    fn supports_merge(&self) -> bool {
        true
    }

    fn supports_create_or_replace_view(&self) -> bool {
        true
    }

    // Databricks limitations
    fn supports_returning(&self) -> bool {
        false
    }

    fn supports_distinct_on(&self) -> bool {
        false
    }

    fn supports_truncate_cascade(&self) -> bool {
        false
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_databricks(name)
    }

    fn emit_data_type(&self, dt: &crate::model::types::DataType) -> String {
        helpers::emit_data_type_databricks(dt)
    }
}
