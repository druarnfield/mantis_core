//! Lexer for the Mantis DSL.
//!
//! This module provides lexical analysis (tokenization) for the Mantis DSL,
//! converting source text into a sequence of tokens with span information.

use chumsky::prelude::*;

/// A token in the Mantis DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum Token<'src> {
    // ========================================================================
    // Main Keywords
    // ========================================================================
    Defaults,
    Calendar,
    Dimension,
    Table,
    Measures,
    Report,

    // ========================================================================
    // Structure Keywords
    // ========================================================================
    Source,
    Key,
    Atoms,
    Times,
    Slicers,
    Attributes,
    DrillPath,

    // ========================================================================
    // Calendar Keywords
    // ========================================================================
    Generate,
    Include,
    Fiscal,
    Range,
    Infer,
    Min,
    Max,

    // ========================================================================
    // Report Keywords
    // ========================================================================
    From,
    UseDate,
    Period,
    Group,
    Show,
    Filter,
    Sort,
    Limit,
    Where,
    As,
    Via,
    To,
    Null,

    // ========================================================================
    // Type Keywords
    // ========================================================================
    Int,
    Decimal,
    Float,
    String,
    Bool,
    Date,
    Timestamp,

    // ========================================================================
    // Grain Keywords
    // ========================================================================
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    FiscalMonth,
    FiscalQuarter,
    FiscalYear,

    // ========================================================================
    // Null Handling Keywords
    // ========================================================================
    CoalesceZero,
    NullOnZero,
    ErrorOnZero,
    NullHandling,

    // ========================================================================
    // Weekday Keywords
    // ========================================================================
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,

    // ========================================================================
    // Month Keywords
    // ========================================================================
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,

    // ========================================================================
    // Period Keywords
    // ========================================================================
    Today,
    Yesterday,
    ThisWeek,
    LastWeek,
    ThisMonth,
    LastMonth,
    ThisQuarter,
    LastQuarter,
    ThisYear,
    LastYear,
    Ytd,
    Qtd,
    Mtd,
    Wtd,
    FiscalYtd,
    FiscalQtd,

    // ========================================================================
    // Fiscal Period Keywords
    // ========================================================================
    ThisFiscalYear,
    LastFiscalYear,
    ThisFiscalQuarter,
    LastFiscalQuarter,

    // ========================================================================
    // Time Suffix Keywords
    // ========================================================================
    PriorYear,
    PriorQuarter,
    PriorMonth,
    PriorWeek,
    YoyGrowth,
    QoqGrowth,
    MomGrowth,
    WowGrowth,
    YoyDelta,
    QoqDelta,
    MomDelta,
    WowDelta,
    Rolling3m,
    Rolling6m,
    Rolling12m,
    Rolling3mAvg,
    Rolling6mAvg,
    Rolling12mAvg,

    // ========================================================================
    // Sort Keywords
    // ========================================================================
    Asc,
    Desc,

    // ========================================================================
    // Settings Keywords
    // ========================================================================
    FiscalYearStart,
    WeekStart,
    DecimalPlaces,

    // ========================================================================
    // Literals
    // ========================================================================
    /// An identifier (not a keyword).
    Ident(&'src str),
    /// A string literal (contents without quotes).
    StringLit(&'src str),
    /// A number (integer, decimal, or date literal).
    Number(&'src str),

    // ========================================================================
    // Symbols
    // ========================================================================
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `;`
    Semicolon,
    /// `,`
    Comma,
    /// `.`
    Dot,
    /// `->`
    Arrow,
    /// `=`
    Eq,
    /// `+`
    Plus,
    /// `@`
    At,
    /// `*`
    Star,
    /// `/`
    Slash,
    /// `-`
    Minus,
    /// `:`
    Colon,
}

impl<'src> std::fmt::Display for Token<'src> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Main keywords
            Token::Defaults => write!(f, "defaults"),
            Token::Calendar => write!(f, "calendar"),
            Token::Dimension => write!(f, "dimension"),
            Token::Table => write!(f, "table"),
            Token::Measures => write!(f, "measures"),
            Token::Report => write!(f, "report"),

            // Structure keywords
            Token::Source => write!(f, "source"),
            Token::Key => write!(f, "key"),
            Token::Atoms => write!(f, "atoms"),
            Token::Times => write!(f, "times"),
            Token::Slicers => write!(f, "slicers"),
            Token::Attributes => write!(f, "attributes"),
            Token::DrillPath => write!(f, "drill_path"),

            // Calendar keywords
            Token::Generate => write!(f, "generate"),
            Token::Include => write!(f, "include"),
            Token::Fiscal => write!(f, "fiscal"),
            Token::Range => write!(f, "range"),
            Token::Infer => write!(f, "infer"),
            Token::Min => write!(f, "min"),
            Token::Max => write!(f, "max"),

            // Report keywords
            Token::From => write!(f, "from"),
            Token::UseDate => write!(f, "use_date"),
            Token::Period => write!(f, "period"),
            Token::Group => write!(f, "group"),
            Token::Show => write!(f, "show"),
            Token::Filter => write!(f, "filter"),
            Token::Sort => write!(f, "sort"),
            Token::Limit => write!(f, "limit"),
            Token::Where => write!(f, "where"),
            Token::As => write!(f, "as"),
            Token::Via => write!(f, "via"),
            Token::To => write!(f, "to"),
            Token::Null => write!(f, "null"),

            // Type keywords
            Token::Int => write!(f, "int"),
            Token::Decimal => write!(f, "decimal"),
            Token::Float => write!(f, "float"),
            Token::String => write!(f, "string"),
            Token::Bool => write!(f, "bool"),
            Token::Date => write!(f, "date"),
            Token::Timestamp => write!(f, "timestamp"),

            // Grain keywords
            Token::Minute => write!(f, "minute"),
            Token::Hour => write!(f, "hour"),
            Token::Day => write!(f, "day"),
            Token::Week => write!(f, "week"),
            Token::Month => write!(f, "month"),
            Token::Quarter => write!(f, "quarter"),
            Token::Year => write!(f, "year"),
            Token::FiscalMonth => write!(f, "fiscal_month"),
            Token::FiscalQuarter => write!(f, "fiscal_quarter"),
            Token::FiscalYear => write!(f, "fiscal_year"),

            // Null handling keywords
            Token::CoalesceZero => write!(f, "coalesce_zero"),
            Token::NullOnZero => write!(f, "null_on_zero"),
            Token::ErrorOnZero => write!(f, "error_on_zero"),
            Token::NullHandling => write!(f, "null_handling"),

            // Weekday keywords
            Token::Monday => write!(f, "Monday"),
            Token::Tuesday => write!(f, "Tuesday"),
            Token::Wednesday => write!(f, "Wednesday"),
            Token::Thursday => write!(f, "Thursday"),
            Token::Friday => write!(f, "Friday"),
            Token::Saturday => write!(f, "Saturday"),
            Token::Sunday => write!(f, "Sunday"),

            // Month keywords
            Token::January => write!(f, "January"),
            Token::February => write!(f, "February"),
            Token::March => write!(f, "March"),
            Token::April => write!(f, "April"),
            Token::May => write!(f, "May"),
            Token::June => write!(f, "June"),
            Token::July => write!(f, "July"),
            Token::August => write!(f, "August"),
            Token::September => write!(f, "September"),
            Token::October => write!(f, "October"),
            Token::November => write!(f, "November"),
            Token::December => write!(f, "December"),

            // Period keywords
            Token::Today => write!(f, "today"),
            Token::Yesterday => write!(f, "yesterday"),
            Token::ThisWeek => write!(f, "this_week"),
            Token::LastWeek => write!(f, "last_week"),
            Token::ThisMonth => write!(f, "this_month"),
            Token::LastMonth => write!(f, "last_month"),
            Token::ThisQuarter => write!(f, "this_quarter"),
            Token::LastQuarter => write!(f, "last_quarter"),
            Token::ThisYear => write!(f, "this_year"),
            Token::LastYear => write!(f, "last_year"),
            Token::Ytd => write!(f, "ytd"),
            Token::Qtd => write!(f, "qtd"),
            Token::Mtd => write!(f, "mtd"),
            Token::Wtd => write!(f, "wtd"),
            Token::FiscalYtd => write!(f, "fiscal_ytd"),
            Token::FiscalQtd => write!(f, "fiscal_qtd"),

            // Fiscal period keywords
            Token::ThisFiscalYear => write!(f, "this_fiscal_year"),
            Token::LastFiscalYear => write!(f, "last_fiscal_year"),
            Token::ThisFiscalQuarter => write!(f, "this_fiscal_quarter"),
            Token::LastFiscalQuarter => write!(f, "last_fiscal_quarter"),

            // Time suffix keywords
            Token::PriorYear => write!(f, "prior_year"),
            Token::PriorQuarter => write!(f, "prior_quarter"),
            Token::PriorMonth => write!(f, "prior_month"),
            Token::PriorWeek => write!(f, "prior_week"),
            Token::YoyGrowth => write!(f, "yoy_growth"),
            Token::QoqGrowth => write!(f, "qoq_growth"),
            Token::MomGrowth => write!(f, "mom_growth"),
            Token::WowGrowth => write!(f, "wow_growth"),
            Token::YoyDelta => write!(f, "yoy_delta"),
            Token::QoqDelta => write!(f, "qoq_delta"),
            Token::MomDelta => write!(f, "mom_delta"),
            Token::WowDelta => write!(f, "wow_delta"),
            Token::Rolling3m => write!(f, "rolling_3m"),
            Token::Rolling6m => write!(f, "rolling_6m"),
            Token::Rolling12m => write!(f, "rolling_12m"),
            Token::Rolling3mAvg => write!(f, "rolling_3m_avg"),
            Token::Rolling6mAvg => write!(f, "rolling_6m_avg"),
            Token::Rolling12mAvg => write!(f, "rolling_12m_avg"),

            // Sort keywords
            Token::Asc => write!(f, "asc"),
            Token::Desc => write!(f, "desc"),

            // Settings keywords
            Token::FiscalYearStart => write!(f, "fiscal_year_start"),
            Token::WeekStart => write!(f, "week_start"),
            Token::DecimalPlaces => write!(f, "decimal_places"),

            // Literals
            Token::Ident(s) => write!(f, "{}", s),
            Token::StringLit(s) => write!(f, "\"{}\"", s),
            Token::Number(s) => write!(f, "{}", s),

            // Symbols
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::Semicolon => write!(f, ";"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::Arrow => write!(f, "->"),
            Token::Eq => write!(f, "="),
            Token::Plus => write!(f, "+"),
            Token::At => write!(f, "@"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Minus => write!(f, "-"),
            Token::Colon => write!(f, ":"),
        }
    }
}

/// Map an identifier string to a keyword token or return Ident.
fn keyword_or_ident(s: &str) -> Token<'_> {
    match s {
        // Main keywords
        "defaults" => Token::Defaults,
        "calendar" => Token::Calendar,
        "dimension" => Token::Dimension,
        "table" => Token::Table,
        "measures" => Token::Measures,
        "report" => Token::Report,

        // Structure keywords
        "source" => Token::Source,
        "key" => Token::Key,
        "atoms" => Token::Atoms,
        "times" => Token::Times,
        "slicers" => Token::Slicers,
        "attributes" => Token::Attributes,
        "drill_path" => Token::DrillPath,

        // Calendar keywords
        "generate" => Token::Generate,
        "include" => Token::Include,
        "fiscal" => Token::Fiscal,
        "range" => Token::Range,
        "infer" => Token::Infer,
        "min" => Token::Min,
        "max" => Token::Max,

        // Report keywords
        "from" => Token::From,
        "use_date" => Token::UseDate,
        "period" => Token::Period,
        "group" => Token::Group,
        "show" => Token::Show,
        "filter" => Token::Filter,
        "sort" => Token::Sort,
        "limit" => Token::Limit,
        "where" => Token::Where,
        "as" => Token::As,
        "via" => Token::Via,
        "to" => Token::To,
        "null" => Token::Null,

        // Type keywords
        "int" => Token::Int,
        "decimal" => Token::Decimal,
        "float" => Token::Float,
        "string" => Token::String,
        "bool" => Token::Bool,
        "date" => Token::Date,
        "timestamp" => Token::Timestamp,

        // Grain keywords
        "minute" => Token::Minute,
        "hour" => Token::Hour,
        "day" => Token::Day,
        "week" => Token::Week,
        "month" => Token::Month,
        "quarter" => Token::Quarter,
        "year" => Token::Year,
        "fiscal_month" => Token::FiscalMonth,
        "fiscal_quarter" => Token::FiscalQuarter,
        "fiscal_year" => Token::FiscalYear,

        // Null handling keywords
        "coalesce_zero" => Token::CoalesceZero,
        "null_on_zero" => Token::NullOnZero,
        "error_on_zero" => Token::ErrorOnZero,
        "null_handling" => Token::NullHandling,

        // Weekday keywords
        "Monday" => Token::Monday,
        "Tuesday" => Token::Tuesday,
        "Wednesday" => Token::Wednesday,
        "Thursday" => Token::Thursday,
        "Friday" => Token::Friday,
        "Saturday" => Token::Saturday,
        "Sunday" => Token::Sunday,

        // Month keywords
        "January" => Token::January,
        "February" => Token::February,
        "March" => Token::March,
        "April" => Token::April,
        "May" => Token::May,
        "June" => Token::June,
        "July" => Token::July,
        "August" => Token::August,
        "September" => Token::September,
        "October" => Token::October,
        "November" => Token::November,
        "December" => Token::December,

        // Period keywords
        "today" => Token::Today,
        "yesterday" => Token::Yesterday,
        "this_week" => Token::ThisWeek,
        "last_week" => Token::LastWeek,
        "this_month" => Token::ThisMonth,
        "last_month" => Token::LastMonth,
        "this_quarter" => Token::ThisQuarter,
        "last_quarter" => Token::LastQuarter,
        "this_year" => Token::ThisYear,
        "last_year" => Token::LastYear,
        "ytd" => Token::Ytd,
        "qtd" => Token::Qtd,
        "mtd" => Token::Mtd,
        "wtd" => Token::Wtd,
        "fiscal_ytd" => Token::FiscalYtd,
        "fiscal_qtd" => Token::FiscalQtd,

        // Fiscal period keywords
        "this_fiscal_year" => Token::ThisFiscalYear,
        "last_fiscal_year" => Token::LastFiscalYear,
        "this_fiscal_quarter" => Token::ThisFiscalQuarter,
        "last_fiscal_quarter" => Token::LastFiscalQuarter,

        // Time suffix keywords
        "prior_year" => Token::PriorYear,
        "prior_quarter" => Token::PriorQuarter,
        "prior_month" => Token::PriorMonth,
        "prior_week" => Token::PriorWeek,
        "yoy_growth" => Token::YoyGrowth,
        "qoq_growth" => Token::QoqGrowth,
        "mom_growth" => Token::MomGrowth,
        "wow_growth" => Token::WowGrowth,
        "yoy_delta" => Token::YoyDelta,
        "qoq_delta" => Token::QoqDelta,
        "mom_delta" => Token::MomDelta,
        "wow_delta" => Token::WowDelta,
        "rolling_3m" => Token::Rolling3m,
        "rolling_6m" => Token::Rolling6m,
        "rolling_12m" => Token::Rolling12m,
        "rolling_3m_avg" => Token::Rolling3mAvg,
        "rolling_6m_avg" => Token::Rolling6mAvg,
        "rolling_12m_avg" => Token::Rolling12mAvg,

        // Sort keywords
        "asc" => Token::Asc,
        "desc" => Token::Desc,

        // Settings keywords
        "fiscal_year_start" => Token::FiscalYearStart,
        "week_start" => Token::WeekStart,
        "decimal_places" => Token::DecimalPlaces,

        // Not a keyword - return as identifier
        _ => Token::Ident(s),
    }
}

/// Create a lexer for the Mantis DSL.
///
/// Returns a parser that tokenizes the input string into a sequence of
/// tokens with span information, skipping whitespace and comments.
pub fn lexer<'src>(
) -> impl Parser<'src, &'src str, Vec<(Token<'src>, SimpleSpan)>, extra::Err<Rich<'src, char>>> {
    // Identifiers: start with letter or underscore, followed by alphanumeric or underscore
    let ident = text::ident().map(keyword_or_ident);

    // String literals: "..."
    let string_lit = just('"')
        .ignore_then(none_of('"').repeated().to_slice())
        .then_ignore(just('"'))
        .map(Token::StringLit);

    // Numbers: integers, decimals, and date literals (YYYY-MM-DD)
    // We parse a broad pattern and let the parser handle validation
    // Use text::digits instead of text::int to parse all consecutive digits including leading zeros
    let number = text::digits(10)
        .then(just('.').then(text::digits(10)).or_not())
        .to_slice()
        .map(Token::Number);

    // Symbols (multi-char first, then single-char)
    let symbol = choice((
        just("->").to(Token::Arrow),
        just('{').to(Token::LBrace),
        just('}').to(Token::RBrace),
        just('(').to(Token::LParen),
        just(')').to(Token::RParen),
        just('[').to(Token::LBracket),
        just(']').to(Token::RBracket),
        just(';').to(Token::Semicolon),
        just(',').to(Token::Comma),
        just('.').to(Token::Dot),
        just('=').to(Token::Eq),
        just('+').to(Token::Plus),
        just('@').to(Token::At),
        just('*').to(Token::Star),
        just('/').to(Token::Slash),
        just('-').to(Token::Minus),
        just(':').to(Token::Colon),
    ));

    // Single-line comments: // ... until newline
    let single_line_comment = just("//")
        .then(any().and_is(just('\n').not()).repeated())
        .ignored();

    // Multi-line comments: /* ... */
    let multi_line_comment = just("/*")
        .then(any().and_is(just("*/").not()).repeated())
        .then(just("*/"))
        .ignored();

    // Combined comment parser
    let comment = single_line_comment.or(multi_line_comment);

    // A single token with span
    let token = choice((ident, string_lit, number, symbol)).map_with(|tok, e| (tok, e.span()));

    // Tokens separated by whitespace and comments, then expect end of input
    token
        .padded_by(comment.padded().repeated())
        .padded()
        .repeated()
        .collect()
        .padded_by(comment.padded().repeated())
        .padded()
        .then_ignore(end())
}

/// Lex a source string into tokens.
///
/// Returns Ok with the token list on success, or Err with the parse errors.
pub fn lex(source: &str) -> Result<Vec<(Token<'_>, SimpleSpan)>, Vec<Rich<'_, char>>> {
    let (tokens, errs) = lexer().parse(source).into_output_errors();
    if errs.is_empty() {
        Ok(tokens.unwrap_or_default())
    } else {
        Err(errs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to extract just the tokens (without spans) for easier testing.
    fn tokens_only(tokens: Vec<(Token<'_>, SimpleSpan)>) -> Vec<Token<'_>> {
        tokens.into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn test_lex_keywords() {
        let source = "table calendar dimension measures report defaults";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Table,
                Token::Calendar,
                Token::Dimension,
                Token::Measures,
                Token::Report,
                Token::Defaults,
            ]
        );
    }

    #[test]
    fn test_lex_structure_keywords() {
        let source = "source key atoms times slicers attributes drill_path";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Source,
                Token::Key,
                Token::Atoms,
                Token::Times,
                Token::Slicers,
                Token::Attributes,
                Token::DrillPath,
            ]
        );
    }

    #[test]
    fn test_lex_type_keywords() {
        let source = "int decimal float string bool date timestamp";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Int,
                Token::Decimal,
                Token::Float,
                Token::String,
                Token::Bool,
                Token::Date,
                Token::Timestamp,
            ]
        );
    }

    #[test]
    fn test_lex_grain_keywords() {
        let source = "minute hour day week month quarter year fiscal_month fiscal_quarter fiscal_year";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Minute,
                Token::Hour,
                Token::Day,
                Token::Week,
                Token::Month,
                Token::Quarter,
                Token::Year,
                Token::FiscalMonth,
                Token::FiscalQuarter,
                Token::FiscalYear,
            ]
        );
    }

    #[test]
    fn test_lex_weekday_keywords() {
        let source = "Monday Tuesday Wednesday Thursday Friday Saturday Sunday";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Monday,
                Token::Tuesday,
                Token::Wednesday,
                Token::Thursday,
                Token::Friday,
                Token::Saturday,
                Token::Sunday,
            ]
        );
    }

    #[test]
    fn test_lex_month_keywords() {
        let source = "January February March April May June July August September October November December";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::January,
                Token::February,
                Token::March,
                Token::April,
                Token::May,
                Token::June,
                Token::July,
                Token::August,
                Token::September,
                Token::October,
                Token::November,
                Token::December,
            ]
        );
    }

    #[test]
    fn test_lex_period_keywords() {
        let source = "today yesterday this_week last_week this_month last_month ytd qtd mtd";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Today,
                Token::Yesterday,
                Token::ThisWeek,
                Token::LastWeek,
                Token::ThisMonth,
                Token::LastMonth,
                Token::Ytd,
                Token::Qtd,
                Token::Mtd,
            ]
        );
    }

    #[test]
    fn test_lex_time_suffix_keywords() {
        let source = "prior_year yoy_growth rolling_12m_avg";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![Token::PriorYear, Token::YoyGrowth, Token::Rolling12mAvg,]
        );
    }

    #[test]
    fn test_lex_string_literal() {
        let source = r#""hello world" "data/sales.csv""#;
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::StringLit("hello world"),
                Token::StringLit("data/sales.csv"),
            ]
        );
    }

    #[test]
    fn test_lex_identifiers() {
        let source = "my_table revenue_total amount123 _private";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Ident("my_table"),
                Token::Ident("revenue_total"),
                Token::Ident("amount123"),
                Token::Ident("_private"),
            ]
        );
    }

    #[test]
    fn test_lex_numbers() {
        let source = "123 3.14 0 42.0";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Number("123"),
                Token::Number("3.14"),
                Token::Number("0"),
                Token::Number("42.0"),
            ]
        );
    }

    #[test]
    fn test_lex_symbols() {
        let source = "{ } ( ) [ ] ; , . -> = + @ * / - :";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::LBrace,
                Token::RBrace,
                Token::LParen,
                Token::RParen,
                Token::LBracket,
                Token::RBracket,
                Token::Semicolon,
                Token::Comma,
                Token::Dot,
                Token::Arrow,
                Token::Eq,
                Token::Plus,
                Token::At,
                Token::Star,
                Token::Slash,
                Token::Minus,
                Token::Colon,
            ]
        );
    }

    #[test]
    fn test_lex_with_comments() {
        let source = r#"
            // This is a single-line comment
            table sales {
                /* This is a
                   multi-line comment */
                source "data.csv";
            }
        "#;
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Table,
                Token::Ident("sales"),
                Token::LBrace,
                Token::Source,
                Token::StringLit("data.csv"),
                Token::Semicolon,
                Token::RBrace,
            ]
        );
    }

    #[test]
    fn test_lex_mixed_content() {
        let source = r#"
            table sales {
                source "dbo.fact_sales";
                atoms { amount decimal; }
                times { sale_date -> dates.day; }
            }
        "#;
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Table,
                Token::Ident("sales"),
                Token::LBrace,
                Token::Source,
                Token::StringLit("dbo.fact_sales"),
                Token::Semicolon,
                Token::Atoms,
                Token::LBrace,
                Token::Ident("amount"),
                Token::Decimal,
                Token::Semicolon,
                Token::RBrace,
                Token::Times,
                Token::LBrace,
                Token::Ident("sale_date"),
                Token::Arrow,
                Token::Ident("dates"),
                Token::Dot,
                Token::Day,
                Token::Semicolon,
                Token::RBrace,
                Token::RBrace,
            ]
        );
    }

    #[test]
    fn test_lex_null_handling_keywords() {
        let source = "coalesce_zero null_on_zero error_on_zero null_handling";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::CoalesceZero,
                Token::NullOnZero,
                Token::ErrorOnZero,
                Token::NullHandling,
            ]
        );
    }

    #[test]
    fn test_lex_settings_keywords() {
        let source = "fiscal_year_start week_start decimal_places";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![Token::FiscalYearStart, Token::WeekStart, Token::DecimalPlaces,]
        );
    }

    #[test]
    fn test_lex_report_keywords() {
        let source = "from use_date period group show filter sort limit where as via to null";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::From,
                Token::UseDate,
                Token::Period,
                Token::Group,
                Token::Show,
                Token::Filter,
                Token::Sort,
                Token::Limit,
                Token::Where,
                Token::As,
                Token::Via,
                Token::To,
                Token::Null,
            ]
        );
    }

    #[test]
    fn test_lex_calendar_keywords() {
        let source = "generate include fiscal range infer min max";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::Generate,
                Token::Include,
                Token::Fiscal,
                Token::Range,
                Token::Infer,
                Token::Min,
                Token::Max,
            ]
        );
    }

    #[test]
    fn test_lex_fiscal_period_keywords() {
        let source = "this_fiscal_year last_fiscal_year this_fiscal_quarter last_fiscal_quarter fiscal_ytd fiscal_qtd";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(
            tokens,
            vec![
                Token::ThisFiscalYear,
                Token::LastFiscalYear,
                Token::ThisFiscalQuarter,
                Token::LastFiscalQuarter,
                Token::FiscalYtd,
                Token::FiscalQtd,
            ]
        );
    }

    #[test]
    fn test_lex_sort_keywords() {
        let source = "asc desc";
        let result = lex(source).expect("lexing should succeed");
        let tokens = tokens_only(result);

        assert_eq!(tokens, vec![Token::Asc, Token::Desc,]);
    }

    #[test]
    fn test_lex_empty_input() {
        let source = "";
        let result = lex(source).expect("lexing should succeed");
        assert!(result.is_empty());
    }

    #[test]
    fn test_lex_whitespace_only() {
        let source = "   \n\t\r\n   ";
        let result = lex(source).expect("lexing should succeed");
        assert!(result.is_empty());
    }

    #[test]
    fn test_lex_comment_only() {
        let source = "// just a comment\n/* another comment */";
        let result = lex(source).expect("lexing should succeed");
        assert!(result.is_empty());
    }

    #[test]
    fn test_lex_spans() {
        let source = "table sales";
        let result = lex(source).expect("lexing should succeed");

        assert_eq!(result.len(), 2);

        // "table" starts at 0, ends at 5
        assert_eq!(result[0].0, Token::Table);
        assert_eq!(result[0].1.start, 0);
        assert_eq!(result[0].1.end, 5);

        // "sales" starts at 6, ends at 11
        assert_eq!(result[1].0, Token::Ident("sales"));
        assert_eq!(result[1].1.start, 6);
        assert_eq!(result[1].1.end, 11);
    }

    #[test]
    fn test_token_display() {
        assert_eq!(format!("{}", Token::Table), "table");
        assert_eq!(format!("{}", Token::Arrow), "->");
        assert_eq!(format!("{}", Token::Ident("foo")), "foo");
        assert_eq!(format!("{}", Token::StringLit("bar")), "\"bar\"");
        assert_eq!(format!("{}", Token::Number("123")), "123");
    }
}
