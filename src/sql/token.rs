//! SQL Tokens - the atomic units of SQL output.
//!
//! Tokens are dialect-agnostic representations that serialize
//! to dialect-specific strings.

use super::dialect::{Dialect, SqlDialect};

/// SQL Token - every possible element in a SQL statement.
///
/// Adding a new variant here will cause compile errors everywhere
/// it needs to be handled (exhaustive matching).
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // === Keywords ===
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    On,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    Cross,
    GroupBy,
    Having,
    OrderBy,
    Asc,
    Desc,
    NullsFirst,
    NullsLast,
    Limit,
    Offset,
    Fetch,
    Next,
    First,
    Rows,
    Only,
    Top,
    Case,
    When,
    Then,
    Else,
    End,
    In,
    Between,
    Like,
    IsNull,
    IsNotNull,
    Distinct,
    All,
    Union,
    Intersect,
    Except,
    With,
    Recursive,
    Null,
    True,
    False,

    // === Window Function Keywords ===
    Over,
    PartitionBy,
    Range,
    Groups,
    Unbounded,
    Preceding,
    Following,
    CurrentRow,

    // === DDL Keywords ===
    Create,
    Alter,
    Drop,
    Table,
    Column,
    Index,
    Constraint,
    Primary,
    Key,
    Foreign,
    References,
    Unique,
    Check,
    Default,
    Cascade,
    Restrict,
    NoAction,
    SetNull,
    SetDefault,
    Add,
    If,
    Exists,
    Truncate,
    View,
    Materialized,
    Replace,

    // === DML Keywords ===
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Returning,
    Output,
    Inserted,
    Deleted,
    Using,
    Conflict,
    Do,
    Nothing,
    Merge,
    Matched,

    // === Punctuation ===
    Comma,
    Dot,
    Star,
    LParen,
    RParen,

    // === Operators ===
    Eq,
    Ne,
    Lt,
    Gt,
    Lte,
    Gte,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    Concat,

    // === Whitespace / Formatting ===
    Space,
    Newline,
    Indent(usize),

    // === Dynamic Content ===
    /// Simple identifier (table, column, alias)
    Ident(String),
    /// Qualified identifier: schema.table or just table
    QualifiedIdent {
        schema: Option<String>,
        name: String,
    },
    /// Integer literal
    LitInt(i64),
    /// Float literal
    LitFloat(f64),
    /// String literal
    LitString(String),
    /// Boolean literal
    LitBool(bool),
    /// NULL literal
    LitNull,

    // === Function Names ===
    /// Function name - currently rendered as-is, but allows future dialect remapping
    /// (e.g., STRFTIME → DATE_FORMAT for MySQL)
    FunctionName(String),

    // === Escape Hatch ===
    /// Raw SQL passed directly to output without escaping.
    ///
    /// # Security Warning
    ///
    /// **Never pass user input to this variant.** Raw SQL is not sanitized
    /// and can lead to SQL injection vulnerabilities. Only use with:
    /// - Trusted, static SQL fragments
    /// - Dialect-specific syntax not covered by other tokens
    ///
    /// For user-provided values, use `Token::LitString`, `Token::LitInt`, etc.
    /// which properly escape content for the target dialect.
    Raw(String),
}

impl Token {
    /// Serialize this token to a string for the given dialect.
    pub fn serialize(&self, dialect: Dialect) -> String {
        match self {
            // Keywords
            Token::Select => "SELECT".into(),
            Token::From => "FROM".into(),
            Token::Where => "WHERE".into(),
            Token::And => "AND".into(),
            Token::Or => "OR".into(),
            Token::Not => "NOT".into(),
            Token::As => "AS".into(),
            Token::On => "ON".into(),
            Token::Join => "JOIN".into(),
            Token::Inner => "INNER".into(),
            Token::Left => "LEFT".into(),
            Token::Right => "RIGHT".into(),
            Token::Full => "FULL".into(),
            Token::Outer => "OUTER".into(),
            Token::Cross => "CROSS".into(),
            Token::GroupBy => "GROUP BY".into(),
            Token::Having => "HAVING".into(),
            Token::OrderBy => "ORDER BY".into(),
            Token::Asc => "ASC".into(),
            Token::Desc => "DESC".into(),
            Token::NullsFirst => "NULLS FIRST".into(),
            Token::NullsLast => "NULLS LAST".into(),
            Token::Limit => "LIMIT".into(),
            Token::Offset => "OFFSET".into(),
            Token::Fetch => "FETCH".into(),
            Token::Next => "NEXT".into(),
            Token::First => "FIRST".into(),
            Token::Rows => "ROWS".into(),
            Token::Only => "ONLY".into(),
            Token::Top => "TOP".into(),
            Token::Case => "CASE".into(),
            Token::When => "WHEN".into(),
            Token::Then => "THEN".into(),
            Token::Else => "ELSE".into(),
            Token::End => "END".into(),
            Token::In => "IN".into(),
            Token::Between => "BETWEEN".into(),
            Token::Like => "LIKE".into(),
            Token::IsNull => "IS NULL".into(),
            Token::IsNotNull => "IS NOT NULL".into(),
            Token::Distinct => "DISTINCT".into(),
            Token::All => "ALL".into(),
            Token::Union => "UNION".into(),
            Token::Intersect => "INTERSECT".into(),
            Token::Except => "EXCEPT".into(),
            Token::With => "WITH".into(),
            Token::Recursive => "RECURSIVE".into(),
            Token::Null => "NULL".into(),
            Token::True => "TRUE".into(),
            Token::False => "FALSE".into(),

            // Window function keywords
            Token::Over => "OVER".into(),
            Token::PartitionBy => "PARTITION BY".into(),
            Token::Range => "RANGE".into(),
            Token::Groups => "GROUPS".into(),
            Token::Unbounded => "UNBOUNDED".into(),
            Token::Preceding => "PRECEDING".into(),
            Token::Following => "FOLLOWING".into(),
            Token::CurrentRow => "CURRENT ROW".into(),

            // DDL keywords
            Token::Create => "CREATE".into(),
            Token::Alter => "ALTER".into(),
            Token::Drop => "DROP".into(),
            Token::Table => "TABLE".into(),
            Token::Column => "COLUMN".into(),
            Token::Index => "INDEX".into(),
            Token::Constraint => "CONSTRAINT".into(),
            Token::Primary => "PRIMARY".into(),
            Token::Key => "KEY".into(),
            Token::Foreign => "FOREIGN".into(),
            Token::References => "REFERENCES".into(),
            Token::Unique => "UNIQUE".into(),
            Token::Check => "CHECK".into(),
            Token::Default => "DEFAULT".into(),
            Token::Cascade => "CASCADE".into(),
            Token::Restrict => "RESTRICT".into(),
            Token::NoAction => "NO ACTION".into(),
            Token::SetNull => "SET NULL".into(),
            Token::SetDefault => "SET DEFAULT".into(),
            Token::Add => "ADD".into(),
            Token::If => "IF".into(),
            Token::Exists => "EXISTS".into(),
            Token::Truncate => "TRUNCATE".into(),
            Token::View => "VIEW".into(),
            Token::Materialized => "MATERIALIZED".into(),
            Token::Replace => "REPLACE".into(),

            // DML keywords
            Token::Insert => "INSERT".into(),
            Token::Into => "INTO".into(),
            Token::Values => "VALUES".into(),
            Token::Update => "UPDATE".into(),
            Token::Set => "SET".into(),
            Token::Delete => "DELETE".into(),
            Token::Returning => "RETURNING".into(),
            Token::Output => "OUTPUT".into(),
            Token::Inserted => "INSERTED".into(),
            Token::Deleted => "DELETED".into(),
            Token::Using => "USING".into(),
            Token::Conflict => "CONFLICT".into(),
            Token::Do => "DO".into(),
            Token::Nothing => "NOTHING".into(),
            Token::Merge => "MERGE".into(),
            Token::Matched => "MATCHED".into(),

            // Punctuation
            Token::Comma => ",".into(),
            Token::Dot => ".".into(),
            Token::Star => "*".into(),
            Token::LParen => "(".into(),
            Token::RParen => ")".into(),

            // Operators
            Token::Eq => "=".into(),
            Token::Ne => "<>".into(),
            Token::Lt => "<".into(),
            Token::Gt => ">".into(),
            Token::Lte => "<=".into(),
            Token::Gte => ">=".into(),
            Token::Plus => "+".into(),
            Token::Minus => "-".into(),
            Token::Mul => "*".into(),
            Token::Div => "/".into(),
            Token::Mod => "%".into(),
            Token::Concat => dialect.concat_operator().into(),

            // Whitespace
            Token::Space => " ".into(),
            Token::Newline => "\n".into(),
            Token::Indent(n) => "  ".repeat(*n),

            // Dynamic - dialect-specific formatting
            Token::Ident(name) => dialect.quote_identifier(name),
            Token::QualifiedIdent { schema, name } => match schema {
                Some(s) => format!(
                    "{}.{}",
                    dialect.quote_identifier(s),
                    dialect.quote_identifier(name)
                ),
                None => dialect.quote_identifier(name),
            },
            Token::LitInt(n) => n.to_string(),
            Token::LitFloat(f) => {
                if f.is_nan() {
                    panic!("Cannot serialize NaN to SQL")
                }
                if f.is_infinite() {
                    panic!("Cannot serialize Infinity to SQL")
                }
                // Use ryu for fast, accurate float formatting
                let mut buffer = ryu::Buffer::new();
                buffer.format(*f).to_string()
            }
            Token::LitString(s) => dialect.quote_string(s),
            Token::LitBool(b) => dialect.format_bool(*b).into(),
            Token::LitNull => "NULL".into(),

            // Function names with dialect-specific remapping
            Token::FunctionName(name) => match dialect.remap_function(name) {
                Some(remapped) => remapped.to_uppercase(),
                None => name.to_uppercase(),
            },

            // Escape hatch
            Token::Raw(s) => s.clone(),
        }
    }
}

/// A stream of tokens that can be serialized to SQL.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TokenStream {
    tokens: Vec<Token>,
}

impl TokenStream {
    /// Create an empty token stream.
    pub fn new() -> Self {
        Self { tokens: vec![] }
    }

    /// Push a single token.
    pub fn push(&mut self, token: Token) -> &mut Self {
        self.tokens.push(token);
        self
    }

    /// Extend with multiple tokens.
    pub fn extend(&mut self, tokens: impl IntoIterator<Item = Token>) -> &mut Self {
        self.tokens.extend(tokens);
        self
    }

    /// Append another token stream.
    pub fn append(&mut self, other: &TokenStream) -> &mut Self {
        self.tokens.extend(other.tokens.iter().cloned());
        self
    }

    /// Serialize all tokens to a SQL string.
    pub fn serialize(&self, dialect: Dialect) -> String {
        self.tokens.iter().map(|t| t.serialize(dialect)).collect()
    }

    // Convenience methods for common tokens
    pub fn space(&mut self) -> &mut Self {
        self.push(Token::Space)
    }
    pub fn newline(&mut self) -> &mut Self {
        self.push(Token::Newline)
    }
    pub fn indent(&mut self, n: usize) -> &mut Self {
        self.push(Token::Indent(n))
    }
    pub fn comma(&mut self) -> &mut Self {
        self.push(Token::Comma)
    }
    pub fn lparen(&mut self) -> &mut Self {
        self.push(Token::LParen)
    }
    pub fn rparen(&mut self) -> &mut Self {
        self.push(Token::RParen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_serialize() {
        assert_eq!(Token::Select.serialize(Dialect::DuckDb), "SELECT");
        assert_eq!(Token::GroupBy.serialize(Dialect::TSql), "GROUP BY");
    }

    #[test]
    fn test_ident_serialize() {
        let tok = Token::Ident("users".into());
        assert_eq!(tok.serialize(Dialect::DuckDb), "\"users\"");
        assert_eq!(tok.serialize(Dialect::TSql), "[users]");
        assert_eq!(tok.serialize(Dialect::MySql), "`users`");
    }

    #[test]
    fn test_qualified_ident() {
        let tok = Token::QualifiedIdent {
            schema: Some("dbo".into()),
            name: "users".into(),
        };
        assert_eq!(tok.serialize(Dialect::TSql), "[dbo].[users]");
    }

    #[test]
    fn test_token_stream() {
        let mut ts = TokenStream::new();
        ts.push(Token::Select)
            .space()
            .push(Token::Ident("name".into()))
            .space()
            .push(Token::From)
            .space()
            .push(Token::Ident("users".into()));

        assert_eq!(
            ts.serialize(Dialect::Postgres),
            "SELECT \"name\" FROM \"users\""
        );
    }

    #[test]
    fn test_concat_dialect() {
        assert_eq!(Token::Concat.serialize(Dialect::DuckDb), "||");
        assert_eq!(Token::Concat.serialize(Dialect::TSql), "+");
    }

    #[test]
    fn test_float_serialize() {
        // Normal floats
        assert_eq!(Token::LitFloat(3.14).serialize(Dialect::DuckDb), "3.14");
        assert_eq!(Token::LitFloat(1.0).serialize(Dialect::DuckDb), "1.0");
        assert_eq!(Token::LitFloat(-42.5).serialize(Dialect::DuckDb), "-42.5");

        // Very small and large numbers maintain precision
        let small = Token::LitFloat(0.000000001).serialize(Dialect::DuckDb);
        assert!(
            small.contains("1"),
            "Small float should be readable: {}",
            small
        );

        let large = Token::LitFloat(1234567890.123456).serialize(Dialect::DuckDb);
        assert!(large.starts_with("1234567890"), "Large float: {}", large);
    }

    #[test]
    #[should_panic(expected = "Cannot serialize NaN")]
    fn test_float_nan_panics() {
        Token::LitFloat(f64::NAN).serialize(Dialect::DuckDb);
    }

    #[test]
    #[should_panic(expected = "Cannot serialize Infinity")]
    fn test_float_infinity_panics() {
        Token::LitFloat(f64::INFINITY).serialize(Dialect::DuckDb);
    }

    #[test]
    #[should_panic(expected = "Cannot serialize Infinity")]
    fn test_float_neg_infinity_panics() {
        Token::LitFloat(f64::NEG_INFINITY).serialize(Dialect::DuckDb);
    }
}
