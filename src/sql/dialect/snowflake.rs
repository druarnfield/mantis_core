//! Snowflake SQL dialect.
//!
//! Snowflake features:
//! - ANSI identifier quoting (`"`)
//! - Native QUALIFY clause
//! - Native PIVOT/UNPIVOT
//! - MERGE support
//! - VARIANT type for semi-structured data
//! - FLATTEN for array/object expansion

use super::helpers;
use super::SqlDialect;
use crate::sql::token::TokenStream;

/// Snowflake SQL dialect.
#[derive(Debug, Clone, Copy)]
pub struct Snowflake;

impl SqlDialect for Snowflake {
    fn name(&self) -> &'static str {
        "snowflake"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_double(ident)
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

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_snowflake(name)
    }

    fn emit_data_type(&self, dt: &crate::model::types::DataType) -> String {
        helpers::emit_data_type_snowflake(dt)
    }

    fn emit_identity(&self, start: i64, increment: i64) -> TokenStream {
        helpers::emit_identity_snowflake(start, increment)
    }
}
