//! T-SQL (SQL Server / Azure SQL) dialect.
//!
//! T-SQL has significant differences from ANSI:
//! - Square bracket identifier quoting (`[name]`)
//! - No native boolean in SELECT (must use IIF/CASE)
//! - OFFSET FETCH for pagination (requires ORDER BY)
//! - TOP for simple limiting
//! - N'...' prefix for Unicode strings
//! - OUTPUT instead of RETURNING
//! - CROSS APPLY / OUTER APPLY instead of LATERAL
//! - No RECURSIVE keyword for recursive CTEs
//! - Native PIVOT/UNPIVOT syntax
//! - String concatenation with `+`

use super::helpers;
use super::SqlDialect;
use crate::sql::token::TokenStream;

/// T-SQL (SQL Server) dialect.
#[derive(Debug, Clone, Copy)]
pub struct TSql;

impl SqlDialect for TSql {
    fn name(&self) -> &'static str {
        "tsql"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        helpers::quote_bracket(ident)
    }

    fn quote_string(&self, s: &str) -> String {
        // T-SQL uses N'...' for Unicode strings
        // For safety, always use N prefix for non-ASCII
        if !s.is_ascii() {
            helpers::quote_string_unicode(s)
        } else {
            helpers::quote_string_single(s)
        }
    }

    fn format_bool(&self, b: bool) -> &'static str {
        helpers::format_bool_numeric(b)
    }

    fn emit_limit_offset(&self, limit: Option<u64>, offset: Option<u64>) -> TokenStream {
        helpers::emit_limit_offset_tsql(limit, offset)
    }

    fn requires_order_by_for_offset(&self) -> bool {
        true
    }

    fn concat_operator(&self) -> &'static str {
        "+"
    }

    fn emit_recursive_keyword(&self) -> bool {
        // T-SQL doesn't use RECURSIVE keyword
        false
    }

    fn supports_lateral(&self) -> bool {
        // T-SQL uses CROSS APPLY / OUTER APPLY instead
        false
    }

    fn supports_nulls_ordering(&self) -> bool {
        // T-SQL 2022+ supports NULLS FIRST/LAST, but older versions don't
        // Being conservative here
        false
    }

    fn format_date_literal(&self, date: &str) -> String {
        // T-SQL doesn't support DATE 'YYYY-MM-DD' syntax
        format!("'{}'", date)
    }

    fn supports_native_pivot(&self) -> bool {
        true
    }

    fn supports_returning(&self) -> bool {
        // T-SQL uses OUTPUT instead of RETURNING
        false
    }

    fn remap_function(&self, name: &str) -> Option<&'static str> {
        helpers::remap_function_tsql(name)
    }

    fn emit_data_type(&self, dt: &crate::model::types::DataType) -> String {
        helpers::emit_data_type_tsql(dt)
    }

    fn emit_identity(&self, start: i64, increment: i64) -> TokenStream {
        helpers::emit_identity_tsql(start, increment)
    }

    fn supports_if_not_exists(&self) -> bool {
        // T-SQL doesn't support IF NOT EXISTS in CREATE TABLE
        // (but can use IF NOT EXISTS (...) pattern)
        false
    }

    fn supports_if_exists(&self) -> bool {
        // T-SQL 2016+ supports DROP TABLE IF EXISTS
        true
    }

    fn supports_drop_cascade(&self) -> bool {
        // T-SQL doesn't support CASCADE on DROP TABLE
        false
    }

    fn supports_include_columns(&self) -> bool {
        true
    }

    fn supports_merge(&self) -> bool {
        // T-SQL has native MERGE support
        true
    }

    fn supports_create_or_replace_view(&self) -> bool {
        // T-SQL doesn't support CREATE OR REPLACE VIEW
        // Use DROP + CREATE or ALTER VIEW instead
        false
    }
}
