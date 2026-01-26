//! MySQL SQL dialect.
//!
//! MySQL differences from ANSI:
//! - Backtick identifier quoting (`` `name` ``)
//! - Boolean is TINYINT(1), returns 1/0
//! - `||` is logical OR by default (use CONCAT())
//! - LIMIT ... OFFSET ... for pagination
//! - ON DUPLICATE KEY UPDATE for upserts
//! - No RETURNING clause (use LAST_INSERT_ID())
//! - LATERAL supported in 8.0.14+
//! - No NULLS FIRST/LAST
//! - No native PIVOT (use CASE expressions)

use super::helpers;
use super::SqlDialect;
use crate::sql::token::TokenStream;

/// MySQL SQL dialect.
#[derive(Debug, Clone, Copy)]
pub struct MySql;

impl SqlDialect for MySql {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_backtick(ident)
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_numeric(b)
    }

    // Uses default emit_limit_offset (LIMIT ... OFFSET ...)

    fn concat_operator(&self) -> &'static str {
        // MySQL || is OR by default, but we return it anyway
        // Callers should check supports_concat_operator()
        "||"
    }

    fn supports_concat_operator(&self) -> bool {
        // MySQL || is OR by default, use CONCAT() instead
        false
    }

    fn supports_nulls_ordering(&self) -> bool {
        false
    }

    fn supports_returning(&self) -> bool {
        // MySQL has no RETURNING, use LAST_INSERT_ID()
        false
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_mysql(name)
    }

    fn emit_data_type(&self, dt: &super::super::types::DataType) -> String {
        helpers::emit_data_type_mysql(dt)
    }

    fn emit_identity(&self, start: i64, increment: i64) -> TokenStream {
        helpers::emit_identity_mysql(start, increment)
    }

    fn supports_partial_indexes(&self) -> bool {
        false
    }

    fn supports_include_columns(&self) -> bool {
        false
    }
}
