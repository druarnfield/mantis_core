//! BigQuery SQL dialect.
//!
//! BigQuery features:
//! - Backtick identifier quoting (standard SQL mode uses double quotes)
//! - No native PIVOT (use CASE expressions)
//! - QUALIFY clause support
//! - Nested and repeated fields (STRUCT, ARRAY)
//! - Partitioned and clustered tables

use super::helpers;
use super::SqlDialect;

/// BigQuery SQL dialect.
#[derive(Debug, Clone, Copy)]
pub struct BigQuery;

impl SqlDialect for BigQuery {
    fn name(&self) -> &'static str {
        "bigquery"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_backtick(ident)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_literal(b)
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

    fn supports_materialized_view(&self) -> bool {
        true
    }

    // BigQuery lacks these features
    fn supports_returning(&self) -> bool {
        false
    }

    fn supports_truncate_cascade(&self) -> bool {
        false
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_bigquery(name)
    }

    fn emit_data_type(&self, dt: &crate::model::types::DataType) -> String {
        helpers::emit_data_type_bigquery(dt)
    }
}
