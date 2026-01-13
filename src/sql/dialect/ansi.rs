//! ANSI SQL dialect - base reference implementation.
//!
//! This provides the ANSI SQL standard behavior as a reference.
//! Most dialects derive from ANSI with specific overrides.

use super::super::token::{Token, TokenStream};

use super::SqlDialect;

/// ANSI SQL dialect (reference implementation).
#[derive(Debug, Clone, Copy)]
pub struct Ansi;

impl SqlDialect for Ansi {
    fn name(&self) -> &'static str {
        "ansi"
    }

    fn quote_identifier(&self, ident: &str) -> String {
        // ANSI uses double quotes, escape by doubling
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn format_bool(&self, b: bool) -> &'static str {
        if b { "TRUE" } else { "FALSE" }
    }

    fn emit_limit_offset(&self, limit: Option<u64>, offset: Option<u64>) -> TokenStream {
        // ANSI SQL uses FETCH FIRST / OFFSET
        let mut ts = TokenStream::new();

        if let Some(off) = offset {
            ts.push(Token::Offset)
                .space()
                .push(Token::LitInt(off as i64))
                .space()
                .push(Token::Rows);
        }

        if let Some(lim) = limit {
            if offset.is_some() {
                ts.space();
            }
            ts.push(Token::Fetch)
                .space()
                .push(Token::First)
                .space()
                .push(Token::LitInt(lim as i64))
                .space()
                .push(Token::Rows)
                .space()
                .push(Token::Only);
        }

        ts
    }
}
