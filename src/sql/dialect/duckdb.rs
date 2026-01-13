//! DuckDB SQL dialect.
//!
//! DuckDB is PostgreSQL-compatible with extensions:
//! - ANSI identifier quoting (`"`)
//! - Native PIVOT syntax
//! - DISTINCT ON support
//! - QUALIFY clause for window functions
//! - TRY_CAST for safe casting

use super::helpers;
use super::SqlDialect;

/// DuckDB SQL dialect.
#[derive(Debug, Clone, Copy)]
pub struct DuckDb;

impl SqlDialect for DuckDb {
    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_double(ident)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_literal(b)
    }

    // Uses default emit_limit_offset (LIMIT ... OFFSET ...)

    fn supports_native_pivot(&self) -> bool {
        true
    }

    fn supports_distinct_on(&self) -> bool {
        true
    }

    fn supports_aggregate_filter(&self) -> bool {
        true
    }

    fn supports_groups_frame(&self) -> bool {
        true
    }

    fn supports_qualify(&self) -> bool {
        true
    }

    fn supports_named_windows(&self) -> bool {
        true
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_duckdb(name)
    }

    fn supports_include_columns(&self) -> bool {
        // DuckDB doesn't support INCLUDE in indexes
        false
    }

    fn supports_truncate_cascade(&self) -> bool {
        true
    }

    fn supports_materialized_view(&self) -> bool {
        true
    }
}
