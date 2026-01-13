//! Amazon Redshift SQL dialect.
//!
//! Redshift features:
//! - PostgreSQL-based syntax
//! - ANSI identifier quoting (`"`)
//! - Distribution and sort keys
//! - No RETURNING clause
//! - Limited window function support compared to Postgres
//! - COPY command for bulk loading

use super::helpers;
use super::SqlDialect;

/// Amazon Redshift SQL dialect.
#[derive(Debug, Clone, Copy)]
pub struct Redshift;

impl SqlDialect for Redshift {
    fn name(&self) -> &'static str {
        "redshift"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_double(ident)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_literal(b)
    }

    // Redshift supports most Postgres features
    fn supports_distinct_on(&self) -> bool {
        true
    }

    fn supports_named_windows(&self) -> bool {
        true
    }

    fn supports_create_or_replace_view(&self) -> bool {
        true
    }

    fn supports_materialized_view(&self) -> bool {
        true
    }

    // Redshift limitations
    fn supports_returning(&self) -> bool {
        false
    }

    fn supports_merge(&self) -> bool {
        false // Use DELETE + INSERT
    }

    fn supports_qualify(&self) -> bool {
        false
    }

    fn supports_groups_frame(&self) -> bool {
        false
    }

    fn supports_aggregate_filter(&self) -> bool {
        false
    }

    fn supports_lateral(&self) -> bool {
        false
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_redshift(name)
    }
}
