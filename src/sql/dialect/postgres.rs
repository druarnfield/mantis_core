//! PostgreSQL SQL dialect.
//!
//! PostgreSQL features:
//! - ANSI identifier quoting (`"`)
//! - Lowercase case folding for unquoted identifiers
//! - Native boolean type (true/false)
//! - RETURNING clause
//! - ON CONFLICT for upserts
//! - Dollar quoting for strings ($$text$$)
//! - DISTINCT ON
//! - FILTER clause for aggregates

use super::helpers;
use super::SqlDialect;

/// PostgreSQL SQL dialect.
#[derive(Debug, Clone, Copy)]
pub struct Postgres;

impl SqlDialect for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_double(ident)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_literal(b)
    }

    // Uses default emit_limit_offset (LIMIT ... OFFSET ...)

    fn supports_distinct_on(&self) -> bool {
        true
    }

    fn supports_aggregate_filter(&self) -> bool {
        true
    }

    fn supports_groups_frame(&self) -> bool {
        true
    }

    fn supports_named_windows(&self) -> bool {
        true
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_postgres(name)
    }

    fn supports_merge(&self) -> bool {
        // PostgreSQL 15+ has native MERGE support
        true
    }

    fn supports_truncate_cascade(&self) -> bool {
        true
    }

    fn supports_materialized_view(&self) -> bool {
        true
    }
}
