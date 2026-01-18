# Phase 1: DSL Parser Foundation

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a working parser for the Mantis DSL that can parse all constructs from the spec with error recovery and span tracking.

**Architecture:** Chumsky parser combinators for the DSL grammar, sqlparser-rs for SQL expressions inside `{ }` blocks. Two-pass design: parse → validate. All AST nodes carry spans for future LSP integration.

**Tech Stack:** chumsky 1.x, sqlparser 0.53 (already in deps), ariadne for error display

**Reference:** `docs/semantic-model-dsl-spec-v3.1.md` for DSL syntax

---

## Task 1: Add Dependencies

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add chumsky and ariadne dependencies**

Add to `[dependencies]` section in `Cargo.toml`:

```toml
# DSL Parser
chumsky = "1.0.0-alpha.7"
ariadne = "0.5"
```

**Step 2: Verify dependencies resolve**

Run: `cargo check`
Expected: Compiles successfully (warnings OK)

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "deps: add chumsky and ariadne for DSL parser"
```

---

## Task 2: Create DSL Module Structure

**Files:**
- Create: `src/dsl/mod.rs`
- Create: `src/dsl/ast.rs`
- Create: `src/dsl/span.rs`
- Modify: `src/lib.rs`

**Step 1: Create the span types**

Create `src/dsl/span.rs`:

```rust
//! Span and source location types for error reporting and LSP.

use std::ops::Range;

/// A span in the source code.
pub type Span = Range<usize>;

/// A value with an associated source span.
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned<T> {
    pub value: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Spanned<U> {
        Spanned {
            value: f(self.value),
            span: self.span,
        }
    }

    pub fn as_ref(&self) -> Spanned<&T> {
        Spanned {
            value: &self.value,
            span: self.span.clone(),
        }
    }
}

impl<T> std::ops::Deref for Spanned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
```

**Step 2: Create the AST types (skeleton)**

Create `src/dsl/ast.rs`:

```rust
//! Abstract Syntax Tree types for the Mantis DSL.

use super::span::{Span, Spanned};

/// A complete model file.
#[derive(Debug, Clone, PartialEq)]
pub struct Model {
    pub items: Vec<Spanned<Item>>,
}

/// Top-level items in a model file.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Defaults(Defaults),
    Calendar(Calendar),
    Dimension(Dimension),
    Table(Table),
    Measures(MeasureBlock),
    Report(Report),
}

/// Model-wide defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Defaults {
    pub settings: Vec<Spanned<DefaultSetting>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DefaultSetting {
    Calendar(String),
    FiscalYearStart(Month),
    WeekStart(Weekday),
    NullHandling(NullHandling),
    DecimalPlaces(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Month {
    January, February, March, April, May, June,
    July, August, September, October, November, December,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Weekday {
    Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullHandling {
    CoalesceZero,
    NullOnZero,
    ErrorOnZero,
}

/// Calendar definition (physical or generated).
#[derive(Debug, Clone, PartialEq)]
pub struct Calendar {
    pub name: Spanned<String>,
    pub source: Option<Spanned<String>>,  // Physical calendar source table
    pub body: CalendarBody,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CalendarBody {
    Physical(PhysicalCalendar),
    Generated(GeneratedCalendar),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCalendar {
    pub grain_mappings: Vec<Spanned<GrainMapping>>,
    pub drill_paths: Vec<Spanned<DrillPath>>,
    pub fiscal_year_start: Option<Month>,
    pub week_start: Option<Weekday>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrainMapping {
    pub grain: GrainLevel,
    pub column: Spanned<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrainLevel {
    Minute, Hour, Day, Week, Month, Quarter, Year,
    FiscalMonth, FiscalQuarter, FiscalYear,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedCalendar {
    pub base_grain: GrainLevel,
    pub include_fiscal: Option<Month>,
    pub range: CalendarRange,
    pub drill_paths: Vec<Spanned<DrillPath>>,
    pub week_start: Option<Weekday>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CalendarRange {
    Explicit { start: String, end: String },
    Infer { min: Option<String>, max: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DrillPath {
    pub name: Spanned<String>,
    pub levels: Vec<Spanned<GrainLevel>>,
}

/// Dimension definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Dimension {
    pub name: Spanned<String>,
    pub source: Spanned<String>,
    pub key: Spanned<String>,
    pub attributes: Vec<Spanned<Attribute>>,
    pub drill_paths: Vec<Spanned<DrillPath>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: Spanned<String>,
    pub data_type: Spanned<DataType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    Int,
    Decimal,
    Float,
    Bool,
    Date,
    Timestamp,
}

/// Table definition (the universal data container).
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: Spanned<String>,
    pub source: Spanned<String>,
    pub atoms: Vec<Spanned<Atom>>,
    pub times: Vec<Spanned<TimeBinding>>,
    pub slicers: Vec<Spanned<Slicer>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Atom {
    pub name: Spanned<String>,
    pub data_type: Spanned<AtomType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomType {
    Int,
    Decimal,
    Float,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeBinding {
    pub name: Spanned<String>,
    pub calendar: Spanned<String>,
    pub grain: Spanned<GrainLevel>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Slicer {
    Inline {
        name: Spanned<String>,
        data_type: Spanned<DataType>,
    },
    ForeignKey {
        name: Spanned<String>,
        dimension: Spanned<String>,
        key: Spanned<String>,
    },
    Via {
        name: Spanned<String>,
        via_slicer: Spanned<String>,
    },
    Calculated {
        name: Spanned<String>,
        data_type: Spanned<DataType>,
        expr: Spanned<SqlExpr>,
    },
}

/// SQL expression (validated by sqlparser).
#[derive(Debug, Clone, PartialEq)]
pub struct SqlExpr {
    pub raw: String,
    pub span: Span,
}

/// Measure block for a table.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureBlock {
    pub table_name: Spanned<String>,
    pub measures: Vec<Spanned<Measure>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    pub name: Spanned<String>,
    pub expr: Spanned<SqlExpr>,
    pub filter: Option<Spanned<SqlExpr>>,
    pub null_handling: Option<NullHandling>,
}

/// Report definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub name: Spanned<String>,
    pub from: Vec<Spanned<String>>,
    pub use_date: Vec<Spanned<String>>,
    pub period: Option<Spanned<PeriodExpr>>,
    pub group: Vec<Spanned<GroupItem>>,
    pub show: Vec<Spanned<ShowItem>>,
    pub filter: Option<Spanned<SqlExpr>>,
    pub sort: Vec<Spanned<SortItem>>,
    pub limit: Option<Spanned<u64>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeriodExpr {
    Relative(RelativePeriod),
    Trailing { count: u32, unit: PeriodUnit },
    Absolute { start: String, end: String },
    Named { name: String, arg: Option<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelativePeriod {
    Today, Yesterday,
    ThisWeek, LastWeek,
    ThisMonth, LastMonth,
    ThisQuarter, LastQuarter,
    ThisYear, LastYear,
    Ytd, Qtd, Mtd,
    ThisFiscalYear, LastFiscalYear,
    ThisFiscalQuarter, LastFiscalQuarter,
    FiscalYtd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodUnit {
    Days, Weeks, Months, Quarters, Years,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupItem {
    pub path: Spanned<DrillPathRef>,
    pub label: Option<Spanned<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DrillPathRef {
    pub source: String,        // calendar or dimension name
    pub path_name: String,     // drill path name
    pub level: String,         // level in the path
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowItem {
    Measure {
        name: Spanned<String>,
        suffix: Option<TimeSuffix>,
        label: Option<Spanned<String>>,
    },
    Inline {
        name: Spanned<String>,
        expr: Spanned<SqlExpr>,
        label: Option<Spanned<String>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSuffix {
    // Accumulations
    Ytd, Qtd, Mtd, Wtd, FiscalYtd, FiscalQtd,
    // Prior period
    PriorYear, PriorQuarter, PriorMonth, PriorWeek,
    // Growth (percentage)
    YoyGrowth, QoqGrowth, MomGrowth, WowGrowth,
    // Delta (absolute)
    YoyDelta, QoqDelta, MomDelta, WowDelta,
    // Rolling
    Rolling3m, Rolling6m, Rolling12m,
    Rolling3mAvg, Rolling6mAvg, Rolling12mAvg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    pub column: Spanned<String>,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}
```

**Step 3: Create the module entry point**

Create `src/dsl/mod.rs`:

```rust
//! DSL parser for the Mantis semantic model language.
//!
//! # Example
//!
//! ```ignore
//! use mantis::dsl::{parse, ParseResult};
//!
//! let source = r#"
//!     table sales {
//!         source "sales.csv";
//!         atoms { revenue decimal; }
//!     }
//! "#;
//!
//! let result = parse(source);
//! for diag in &result.diagnostics {
//!     eprintln!("{}", diag);
//! }
//! if let Some(model) = result.model {
//!     // Use the parsed model
//! }
//! ```

pub mod ast;
pub mod span;

pub use ast::*;
pub use span::{Span, Spanned};

/// Result of parsing a DSL source.
#[derive(Debug)]
pub struct ParseResult {
    /// The parsed model (may be partial if there were errors).
    pub model: Option<Model>,
    /// Parse diagnostics (errors and warnings).
    pub diagnostics: Vec<Diagnostic>,
}

/// A diagnostic message with source location.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub span: Span,
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let level = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        write!(f, "{}: {} (at {:?})", level, self.message, self.span)
    }
}

/// Parse a DSL source string.
///
/// Returns a `ParseResult` containing the parsed model (if successful)
/// and any diagnostics.
pub fn parse(source: &str) -> ParseResult {
    // TODO: Implement parser
    ParseResult {
        model: None,
        diagnostics: vec![Diagnostic {
            span: 0..0,
            severity: Severity::Error,
            message: "Parser not yet implemented".to_string(),
        }],
    }
}

/// Parse a DSL source file.
pub fn parse_file(path: &std::path::Path) -> std::io::Result<ParseResult> {
    let source = std::fs::read_to_string(path)?;
    Ok(parse(&source))
}
```

**Step 4: Register the module in lib.rs**

Add to `src/lib.rs` after the existing module declarations:

```rust
pub mod dsl;
```

**Step 5: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/dsl/ src/lib.rs
git commit -m "feat(dsl): add AST types and module structure"
```

---

## Task 3: Implement Lexer/Token Types

**Files:**
- Create: `src/dsl/lexer.rs`
- Modify: `src/dsl/mod.rs`

**Step 1: Create the lexer with keyword recognition**

Create `src/dsl/lexer.rs`:

```rust
//! Lexer for the Mantis DSL.
//!
//! Tokenizes source into keywords, identifiers, literals, and symbols.

use chumsky::prelude::*;

/// Token types for the DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum Token<'src> {
    // Keywords
    Defaults,
    Calendar,
    Dimension,
    Table,
    Measures,
    Report,
    Source,
    Key,
    Atoms,
    Times,
    Slicers,
    Attributes,
    DrillPath,
    Generate,
    Include,
    Fiscal,
    Range,
    Infer,
    Min,
    Max,
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

    // Type keywords
    Int,
    Decimal,
    Float,
    String_,
    Bool,
    Date,
    Timestamp,

    // Grain keywords
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

    // Null handling
    CoalesceZero,
    NullOnZero,
    ErrorOnZero,

    // Day/Month names
    Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday,
    January, February, March, April, May, June,
    July, August, September, October, November, December,

    // Period keywords
    Today, Yesterday,
    ThisWeek, LastWeek,
    ThisMonth, LastMonth,
    ThisQuarter, LastQuarter,
    ThisYear, LastYear,
    Ytd, Qtd, Mtd, Wtd,
    ThisFiscalYear, LastFiscalYear,
    ThisFiscalQuarter, LastFiscalQuarter,
    FiscalYtd, FiscalQtd,

    // Time suffixes (used in show clauses)
    PriorYear, PriorQuarter, PriorMonth, PriorWeek,
    YoyGrowth, QoqGrowth, MomGrowth, WowGrowth,
    YoyDelta, QoqDelta, MomDelta, WowDelta,
    Rolling3m, Rolling6m, Rolling12m,
    Rolling3mAvg, Rolling6mAvg, Rolling12mAvg,

    // Literals
    Ident(&'src str),
    StringLit(&'src str),
    Number(&'src str),

    // Symbols
    LBrace,      // {
    RBrace,      // }
    LParen,      // (
    RParen,      // )
    LBracket,    // [
    RBracket,    // ]
    Semi,        // ;
    Comma,       // ,
    Dot,         // .
    Arrow,       // ->
    Eq,          // =
    Plus,        // + (for grain specifier like day+)

    // Sort direction
    Asc,
    Desc,
}

/// Create the lexer.
pub fn lexer<'src>() -> impl Parser<'src, &'src str, Vec<(Token<'src>, SimpleSpan)>, extra::Err<Rich<'src, char>>> {
    let keyword_or_ident = text::ident().map(|s: &str| match s {
        // Main keywords
        "defaults" => Token::Defaults,
        "calendar" => Token::Calendar,
        "dimension" => Token::Dimension,
        "table" => Token::Table,
        "measures" => Token::Measures,
        "report" => Token::Report,
        "source" => Token::Source,
        "key" => Token::Key,
        "atoms" => Token::Atoms,
        "times" => Token::Times,
        "slicers" => Token::Slicers,
        "attributes" => Token::Attributes,
        "drill_path" => Token::DrillPath,
        "generate" => Token::Generate,
        "include" => Token::Include,
        "fiscal" => Token::Fiscal,
        "range" => Token::Range,
        "infer" => Token::Infer,
        "min" => Token::Min,
        "max" => Token::Max,
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

        // Types
        "int" => Token::Int,
        "decimal" => Token::Decimal,
        "float" => Token::Float,
        "string" => Token::String_,
        "bool" => Token::Bool,
        "date" => Token::Date,
        "timestamp" => Token::Timestamp,

        // Grains
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

        // Null handling
        "coalesce_zero" => Token::CoalesceZero,
        "null_on_zero" => Token::NullOnZero,
        "error_on_zero" => Token::ErrorOnZero,

        // Days
        "Monday" => Token::Monday,
        "Tuesday" => Token::Tuesday,
        "Wednesday" => Token::Wednesday,
        "Thursday" => Token::Thursday,
        "Friday" => Token::Friday,
        "Saturday" => Token::Saturday,
        "Sunday" => Token::Sunday,

        // Months
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

        // Periods
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
        "this_fiscal_year" => Token::ThisFiscalYear,
        "last_fiscal_year" => Token::LastFiscalYear,
        "this_fiscal_quarter" => Token::ThisFiscalQuarter,
        "last_fiscal_quarter" => Token::LastFiscalQuarter,
        "fiscal_ytd" => Token::FiscalYtd,
        "fiscal_qtd" => Token::FiscalQtd,

        // Time suffixes
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

        // Sort
        "asc" => Token::Asc,
        "desc" => Token::Desc,

        // Otherwise it's an identifier
        _ => Token::Ident(s),
    });

    let string_lit = just('"')
        .ignore_then(none_of('"').repeated().to_slice())
        .then_ignore(just('"'))
        .map(Token::StringLit);

    let number = text::int(10)
        .then(just('.').then(text::digits(10)).or_not())
        .to_slice()
        .map(Token::Number);

    let symbol = choice((
        just("->").to(Token::Arrow),
        just('{').to(Token::LBrace),
        just('}').to(Token::RBrace),
        just('(').to(Token::LParen),
        just(')').to(Token::RParen),
        just('[').to(Token::LBracket),
        just(']').to(Token::RBracket),
        just(';').to(Token::Semi),
        just(',').to(Token::Comma),
        just('.').to(Token::Dot),
        just('=').to(Token::Eq),
        just('+').to(Token::Plus),
    ));

    let single_line_comment = just("//")
        .then(any().and_is(just('\n').not()).repeated())
        .padded();

    let multi_line_comment = just("/*")
        .then(any().and_is(just("*/").not()).repeated())
        .then(just("*/"))
        .padded();

    let comment = single_line_comment.or(multi_line_comment);

    let token = choice((
        keyword_or_ident,
        string_lit,
        number,
        symbol,
    ))
    .map_with(|tok, e| (tok, e.span()));

    token
        .padded_by(comment.repeated())
        .padded()
        .repeated()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lex_keywords() {
        let input = "table atoms times slicers";
        let result = lexer().parse(input).into_result().unwrap();
        let tokens: Vec<_> = result.into_iter().map(|(t, _)| t).collect();
        assert_eq!(tokens, vec![
            Token::Table,
            Token::Atoms,
            Token::Times,
            Token::Slicers,
        ]);
    }

    #[test]
    fn test_lex_string_literal() {
        let input = r#""hello world""#;
        let result = lexer().parse(input).into_result().unwrap();
        assert_eq!(result[0].0, Token::StringLit("hello world"));
    }

    #[test]
    fn test_lex_identifiers() {
        let input = "my_table customer_id";
        let result = lexer().parse(input).into_result().unwrap();
        let tokens: Vec<_> = result.into_iter().map(|(t, _)| t).collect();
        assert_eq!(tokens, vec![
            Token::Ident("my_table"),
            Token::Ident("customer_id"),
        ]);
    }

    #[test]
    fn test_lex_symbols() {
        let input = "{ } -> ; .";
        let result = lexer().parse(input).into_result().unwrap();
        let tokens: Vec<_> = result.into_iter().map(|(t, _)| t).collect();
        assert_eq!(tokens, vec![
            Token::LBrace,
            Token::RBrace,
            Token::Arrow,
            Token::Semi,
            Token::Dot,
        ]);
    }

    #[test]
    fn test_lex_with_comments() {
        let input = r#"
            table // this is a comment
            atoms /* multi
            line */ times
        "#;
        let result = lexer().parse(input).into_result().unwrap();
        let tokens: Vec<_> = result.into_iter().map(|(t, _)| t).collect();
        assert_eq!(tokens, vec![Token::Table, Token::Atoms, Token::Times]);
    }
}
```

**Step 2: Add lexer module to mod.rs**

Add to `src/dsl/mod.rs` after the existing module declarations:

```rust
pub mod lexer;
```

**Step 3: Run lexer tests**

Run: `cargo test dsl::lexer`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/dsl/lexer.rs src/dsl/mod.rs
git commit -m "feat(dsl): implement lexer with keyword recognition"
```

---

## Task 4: Implement Core Parser - Table Definition

**Files:**
- Create: `src/dsl/parser.rs`
- Modify: `src/dsl/mod.rs`

**Step 1: Create parser skeleton with table parsing**

Create `src/dsl/parser.rs`:

```rust
//! Parser for the Mantis DSL using chumsky.

use chumsky::prelude::*;

use super::ast::*;
use super::lexer::Token;
use super::span::{Span, Spanned};

type ParserInput<'tokens, 'src> = chumsky::input::SpannedInput<Token<'src>, Span, &'tokens [(Token<'src>, Span)]>;

/// Create the main parser.
pub fn parser<'tokens, 'src: 'tokens>() -> impl Parser<'tokens, ParserInput<'tokens, 'src>, Model, extra::Err<Rich<'tokens, Token<'src>, Span>>> {
    let ident = select! {
        Token::Ident(s) => s.to_string(),
    }.labelled("identifier");

    let string_lit = select! {
        Token::StringLit(s) => s.to_string(),
    }.labelled("string literal");

    let atom_type = select! {
        Token::Int => AtomType::Int,
        Token::Decimal => AtomType::Decimal,
        Token::Float => AtomType::Float,
    }.labelled("atom type (int, decimal, float)");

    let data_type = select! {
        Token::String_ => DataType::String,
        Token::Int => DataType::Int,
        Token::Decimal => DataType::Decimal,
        Token::Float => DataType::Float,
        Token::Bool => DataType::Bool,
        Token::Date => DataType::Date,
        Token::Timestamp => DataType::Timestamp,
    }.labelled("data type");

    let grain_level = select! {
        Token::Minute => GrainLevel::Minute,
        Token::Hour => GrainLevel::Hour,
        Token::Day => GrainLevel::Day,
        Token::Week => GrainLevel::Week,
        Token::Month => GrainLevel::Month,
        Token::Quarter => GrainLevel::Quarter,
        Token::Year => GrainLevel::Year,
        Token::FiscalMonth => GrainLevel::FiscalMonth,
        Token::FiscalQuarter => GrainLevel::FiscalQuarter,
        Token::FiscalYear => GrainLevel::FiscalYear,
    }.labelled("grain level");

    // Helper to wrap value with span
    let spanned = |p: &_| p.clone().map_with(|v, e| Spanned::new(v, e.span()));

    // atoms { name type; ... }
    let atom = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then(atom_type.map_with(|t, e| Spanned::new(t, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|(name, data_type)| Atom { name, data_type });

    let atoms_block = just(Token::Atoms)
        .ignore_then(
            atom.map_with(|a, e| Spanned::new(a, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // times { name -> calendar.grain; ... }
    let time_binding = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then_ignore(just(Token::Arrow))
        .then(ident.map_with(|c, e| Spanned::new(c, e.span())))
        .then_ignore(just(Token::Dot))
        .then(grain_level.map_with(|g, e| Spanned::new(g, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|((name, calendar), grain)| TimeBinding { name, calendar, grain });

    let times_block = just(Token::Times)
        .ignore_then(
            time_binding.map_with(|t, e| Spanned::new(t, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // slicers { ... }
    // Inline: name type;
    // FK: name -> dimension.key;
    // Via: name via other_slicer;
    // Calculated: name type = { sql };
    let slicer_inline = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then(data_type.map_with(|t, e| Spanned::new(t, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|(name, data_type)| Slicer::Inline { name, data_type });

    let slicer_fk = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then_ignore(just(Token::Arrow))
        .then(ident.map_with(|d, e| Spanned::new(d, e.span())))
        .then_ignore(just(Token::Dot))
        .then(ident.map_with(|k, e| Spanned::new(k, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|((name, dimension), key)| Slicer::ForeignKey { name, dimension, key });

    let slicer_via = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then_ignore(just(Token::Via))
        .then(ident.map_with(|v, e| Spanned::new(v, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|(name, via_slicer)| Slicer::Via { name, via_slicer });

    // For calculated slicers, we need to capture the SQL block
    // SQL expressions are { ... } - we capture raw content for now
    let sql_expr = just(Token::LBrace)
        .ignore_then(
            none_of([Token::RBrace])
                .repeated()
                .collect::<Vec<_>>()
        )
        .then_ignore(just(Token::RBrace))
        .map_with(|_tokens, e| {
            // For now, just capture the span - we'll extract raw text later
            SqlExpr {
                raw: String::new(), // Will be populated from source
                span: e.span(),
            }
        });

    let slicer_calc = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then(data_type.map_with(|t, e| Spanned::new(t, e.span())))
        .then_ignore(just(Token::Eq))
        .then(sql_expr.map_with(|s, e| Spanned::new(s, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|((name, data_type), expr)| Slicer::Calculated { name, data_type, expr });

    let slicer = choice((
        slicer_calc,  // Must come before inline (both start with ident type)
        slicer_via,
        slicer_fk,
        slicer_inline,
    ));

    let slicers_block = just(Token::Slicers)
        .ignore_then(
            slicer.map_with(|s, e| Spanned::new(s, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // table name { source "..."; atoms { ... } times { ... } slicers { ... } }
    let table = just(Token::Table)
        .ignore_then(ident.map_with(|n, e| Spanned::new(n, e.span())))
        .then(
            just(Token::Source)
                .ignore_then(string_lit.map_with(|s, e| Spanned::new(s, e.span())))
                .then_ignore(just(Token::Semi))
                .then(atoms_block.or_not())
                .then(times_block.or_not())
                .then(slicers_block.or_not())
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(name, (((source, atoms), times), slicers))| {
            Table {
                name,
                source,
                atoms: atoms.unwrap_or_default(),
                times: times.unwrap_or_default(),
                slicers: slicers.unwrap_or_default(),
            }
        });

    // For now, only parse tables
    let item = table.map(Item::Table);

    item.map_with(|i, e| Spanned::new(i, e.span()))
        .repeated()
        .collect::<Vec<_>>()
        .map(|items| Model { items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::lexer::lexer;

    fn parse_str(input: &str) -> Result<Model, Vec<Rich<'_, Token<'_>, Span>>> {
        let tokens = lexer().parse(input).into_result().unwrap();
        let token_span = tokens.as_slice().map((tokens.len()..tokens.len()).into());
        parser().parse(token_span).into_result()
    }

    #[test]
    fn test_parse_simple_table() {
        let input = r#"
            table sales {
                source "sales.csv";
                atoms {
                    revenue decimal;
                    quantity int;
                }
            }
        "#;

        let result = parse_str(input).unwrap();
        assert_eq!(result.items.len(), 1);

        if let Item::Table(table) = &result.items[0].value {
            assert_eq!(table.name.value, "sales");
            assert_eq!(table.source.value, "sales.csv");
            assert_eq!(table.atoms.len(), 2);
            assert_eq!(table.atoms[0].value.name.value, "revenue");
        } else {
            panic!("Expected table");
        }
    }

    #[test]
    fn test_parse_table_with_times() {
        let input = r#"
            table orders {
                source "dbo.orders";
                times {
                    order_date -> calendar.day;
                }
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Table(table) = &result.items[0].value {
            assert_eq!(table.times.len(), 1);
            assert_eq!(table.times[0].value.name.value, "order_date");
            assert_eq!(table.times[0].value.calendar.value, "calendar");
            assert_eq!(table.times[0].value.grain.value, GrainLevel::Day);
        } else {
            panic!("Expected table");
        }
    }

    #[test]
    fn test_parse_table_with_slicers() {
        let input = r#"
            table orders {
                source "dbo.orders";
                slicers {
                    region string;
                    customer_id -> customers.customer_id;
                    segment via customer_id;
                }
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Table(table) = &result.items[0].value {
            assert_eq!(table.slicers.len(), 3);

            // Check inline slicer
            if let Slicer::Inline { name, data_type } = &table.slicers[0].value {
                assert_eq!(name.value, "region");
                assert_eq!(data_type.value, DataType::String);
            } else {
                panic!("Expected inline slicer");
            }

            // Check FK slicer
            if let Slicer::ForeignKey { name, dimension, key } = &table.slicers[1].value {
                assert_eq!(name.value, "customer_id");
                assert_eq!(dimension.value, "customers");
                assert_eq!(key.value, "customer_id");
            } else {
                panic!("Expected FK slicer");
            }

            // Check via slicer
            if let Slicer::Via { name, via_slicer } = &table.slicers[2].value {
                assert_eq!(name.value, "segment");
                assert_eq!(via_slicer.value, "customer_id");
            } else {
                panic!("Expected via slicer");
            }
        } else {
            panic!("Expected table");
        }
    }
}
```

**Step 2: Add parser module to mod.rs**

Add to `src/dsl/mod.rs`:

```rust
pub mod parser;
```

**Step 3: Run parser tests**

Run: `cargo test dsl::parser`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/dsl/parser.rs src/dsl/mod.rs
git commit -m "feat(dsl): implement parser for table definitions"
```

---

## Task 5: Add Calendar and Dimension Parsing

**Files:**
- Modify: `src/dsl/parser.rs`

**Step 1: Add calendar parser**

Add the following parsers to `src/dsl/parser.rs` inside the `parser()` function, before the `item` definition:

```rust
    let month = select! {
        Token::January => Month::January,
        Token::February => Month::February,
        Token::March => Month::March,
        Token::April => Month::April,
        Token::May => Month::May,
        Token::June => Month::June,
        Token::July => Month::July,
        Token::August => Month::August,
        Token::September => Month::September,
        Token::October => Month::October,
        Token::November => Month::November,
        Token::December => Month::December,
    }.labelled("month name");

    let weekday = select! {
        Token::Monday => Weekday::Monday,
        Token::Tuesday => Weekday::Tuesday,
        Token::Wednesday => Weekday::Wednesday,
        Token::Thursday => Weekday::Thursday,
        Token::Friday => Weekday::Friday,
        Token::Saturday => Weekday::Saturday,
        Token::Sunday => Weekday::Sunday,
    }.labelled("weekday name");

    // drill_path name { level -> level -> ... };
    let drill_path = just(Token::DrillPath)
        .ignore_then(ident.map_with(|n, e| Spanned::new(n, e.span())))
        .then(
            grain_level.map_with(|g, e| Spanned::new(g, e.span()))
                .separated_by(just(Token::Arrow))
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .then_ignore(just(Token::Semi))
        .map(|(name, levels)| DrillPath { name, levels });

    // Generated calendar: calendar name { generate grain+; ... }
    let generated_calendar = just(Token::Generate)
        .ignore_then(grain_level)
        .then_ignore(just(Token::Plus))
        .then(
            just(Token::Include)
                .ignore_then(just(Token::Fiscal))
                .ignore_then(month.delimited_by(just(Token::LBracket), just(Token::RBracket)))
                .or_not()
        )
        .then_ignore(just(Token::Semi))
        .then(
            just(Token::Range)
                .ignore_then(choice((
                    // range infer [min DATE] [max DATE];
                    just(Token::Infer)
                        .ignore_then(
                            just(Token::Min).ignore_then(ident).or_not()
                        )
                        .then(
                            just(Token::Max).ignore_then(ident).or_not()
                        )
                        .map(|(min, max)| CalendarRange::Infer {
                            min: min.map(|s| s.to_string()),
                            max: max.map(|s| s.to_string()),
                        }),
                    // range DATE to DATE;
                    ident
                        .then_ignore(just(Token::To))
                        .then(ident)
                        .map(|(start, end)| CalendarRange::Explicit {
                            start: start.to_string(),
                            end: end.to_string(),
                        }),
                )))
                .then_ignore(just(Token::Semi))
        )
        .then(
            drill_path.map_with(|d, e| Spanned::new(d, e.span()))
                .repeated()
                .collect::<Vec<_>>()
        )
        .then(
            just(Token::WeekStart)
                .ignore_then(weekday)
                .then_ignore(just(Token::Semi))
                .or_not()
        )
        .map(|((((base_grain, include_fiscal), range), drill_paths), week_start)| {
            CalendarBody::Generated(GeneratedCalendar {
                base_grain,
                include_fiscal,
                range,
                drill_paths,
                week_start,
            })
        });

    // Physical calendar: calendar name "source" { day = col; ... }
    let grain_mapping = grain_level
        .then_ignore(just(Token::Eq))
        .then(ident.map_with(|c, e| Spanned::new(c, e.span())))
        .then_ignore(just(Token::Semi))
        .map_with(|(grain, column), e| Spanned::new(GrainMapping { grain, column }, e.span()));

    let physical_calendar = grain_mapping
        .repeated()
        .collect::<Vec<_>>()
        .then(
            drill_path.map_with(|d, e| Spanned::new(d, e.span()))
                .repeated()
                .collect::<Vec<_>>()
        )
        .then(
            just(Token::FiscalYearStart)
                .ignore_then(month)
                .then_ignore(just(Token::Semi))
                .or_not()
        )
        .then(
            just(Token::WeekStart)
                .ignore_then(weekday)
                .then_ignore(just(Token::Semi))
                .or_not()
        )
        .map(|(((grain_mappings, drill_paths), fiscal_year_start), week_start)| {
            CalendarBody::Physical(PhysicalCalendar {
                grain_mappings,
                drill_paths,
                fiscal_year_start,
                week_start,
            })
        });

    // calendar name ["source"] { ... }
    let calendar = just(Token::Calendar)
        .ignore_then(ident.map_with(|n, e| Spanned::new(n, e.span())))
        .then(string_lit.map_with(|s, e| Spanned::new(s, e.span())).or_not())
        .then(
            choice((generated_calendar, physical_calendar))
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|((name, source), body)| Calendar { name, source, body });
```

**Step 2: Add dimension parser**

Add after the calendar parser:

```rust
    // attributes { name type; ... }
    let attribute = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then(data_type.map_with(|t, e| Spanned::new(t, e.span())))
        .then_ignore(just(Token::Semi))
        .map(|(name, data_type)| Attribute { name, data_type });

    let attributes_block = just(Token::Attributes)
        .ignore_then(
            attribute.map_with(|a, e| Spanned::new(a, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // dimension name { source "..."; key col; attributes { ... } drill_path { ... } }
    let dimension = just(Token::Dimension)
        .ignore_then(ident.map_with(|n, e| Spanned::new(n, e.span())))
        .then(
            just(Token::Source)
                .ignore_then(string_lit.map_with(|s, e| Spanned::new(s, e.span())))
                .then_ignore(just(Token::Semi))
                .then(
                    just(Token::Key)
                        .ignore_then(ident.map_with(|k, e| Spanned::new(k, e.span())))
                        .then_ignore(just(Token::Semi))
                )
                .then(attributes_block)
                .then(
                    drill_path.map_with(|d, e| Spanned::new(d, e.span()))
                        .repeated()
                        .collect::<Vec<_>>()
                )
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(name, (((source, key), attributes), drill_paths))| {
            Dimension { name, source, key, attributes, drill_paths }
        });
```

**Step 3: Update item parser to include calendar and dimension**

Replace the `item` definition:

```rust
    let item = choice((
        calendar.map(Item::Calendar),
        dimension.map(Item::Dimension),
        table.map(Item::Table),
    ));
```

**Step 4: Add tests for calendar and dimension**

Add to the tests module:

```rust
    #[test]
    fn test_parse_generated_calendar() {
        let input = r#"
            calendar auto {
                generate day+;
                range infer min 2020-01-01 max 2030-12-31;
                drill_path standard { day -> week -> month -> quarter -> year };
                week_start Monday;
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Calendar(cal) = &result.items[0].value {
            assert_eq!(cal.name.value, "auto");
            assert!(cal.source.is_none());
            if let CalendarBody::Generated(gen) = &cal.body {
                assert_eq!(gen.base_grain, GrainLevel::Day);
                assert_eq!(gen.drill_paths.len(), 1);
                assert_eq!(gen.week_start, Some(Weekday::Monday));
            } else {
                panic!("Expected generated calendar");
            }
        } else {
            panic!("Expected calendar");
        }
    }

    #[test]
    fn test_parse_dimension() {
        let input = r#"
            dimension customers {
                source "dbo.dim_customers";
                key customer_id;
                attributes {
                    customer_name string;
                    segment string;
                    region string;
                }
                drill_path geo { region };
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Dimension(dim) = &result.items[0].value {
            assert_eq!(dim.name.value, "customers");
            assert_eq!(dim.source.value, "dbo.dim_customers");
            assert_eq!(dim.key.value, "customer_id");
            assert_eq!(dim.attributes.len(), 3);
            assert_eq!(dim.drill_paths.len(), 1);
        } else {
            panic!("Expected dimension");
        }
    }
```

**Step 5: Run tests**

Run: `cargo test dsl::parser`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/dsl/parser.rs
git commit -m "feat(dsl): add calendar and dimension parsing"
```

---

## Task 6: Add Measures and Defaults Parsing

**Files:**
- Modify: `src/dsl/parser.rs`
- Modify: `src/dsl/lexer.rs`

**Step 1: Add missing keywords to lexer**

Add these keywords to `src/dsl/lexer.rs` in the Token enum:

```rust
    // Additional keywords for defaults
    FiscalYearStart,
    WeekStart,
    DecimalPlaces,
```

And in the match block:

```rust
        "fiscal_year_start" => Token::FiscalYearStart,
        "week_start" => Token::WeekStart,
        "decimal_places" => Token::DecimalPlaces,
```

**Step 2: Add measures and defaults parsers to parser.rs**

Add after the dimension parser:

```rust
    let null_handling = select! {
        Token::CoalesceZero => NullHandling::CoalesceZero,
        Token::NullOnZero => NullHandling::NullOnZero,
        Token::ErrorOnZero => NullHandling::ErrorOnZero,
    }.labelled("null handling option");

    // defaults { calendar name; fiscal_year_start Month; ... }
    let default_setting = choice((
        just(Token::Calendar)
            .ignore_then(ident)
            .then_ignore(just(Token::Semi))
            .map(|s| DefaultSetting::Calendar(s.to_string())),
        just(Token::FiscalYearStart)
            .ignore_then(month)
            .then_ignore(just(Token::Semi))
            .map(DefaultSetting::FiscalYearStart),
        just(Token::WeekStart)
            .ignore_then(weekday)
            .then_ignore(just(Token::Semi))
            .map(DefaultSetting::WeekStart),
        just(Token::NullHandling)
            .ignore_then(null_handling)
            .then_ignore(just(Token::Semi))
            .map(DefaultSetting::NullHandling),
        just(Token::DecimalPlaces)
            .ignore_then(select! { Token::Number(n) => n.parse::<u8>().unwrap_or(2) })
            .then_ignore(just(Token::Semi))
            .map(DefaultSetting::DecimalPlaces),
    ));

    let defaults = just(Token::Defaults)
        .ignore_then(
            default_setting.map_with(|s, e| Spanned::new(s, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|settings| Defaults { settings });

    // measure: name = { expr } [where { cond }] [null handling];
    let measure = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then_ignore(just(Token::Eq))
        .then(sql_expr.clone().map_with(|s, e| Spanned::new(s, e.span())))
        .then(
            just(Token::Where)
                .ignore_then(sql_expr.clone().map_with(|s, e| Spanned::new(s, e.span())))
                .or_not()
        )
        .then(
            just(Token::Null)
                .ignore_then(null_handling)
                .or_not()
        )
        .then_ignore(just(Token::Semi))
        .map(|(((name, expr), filter), null_handling)| {
            Measure { name, expr, filter, null_handling }
        });

    // measures table_name { measure; ... }
    let measures_block = just(Token::Measures)
        .ignore_then(ident.map_with(|n, e| Spanned::new(n, e.span())))
        .then(
            measure.map_with(|m, e| Spanned::new(m, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(table_name, measures)| MeasureBlock { table_name, measures });
```

**Step 3: Add NullHandling token to lexer**

Add to Token enum:

```rust
    NullHandling,
```

And in the match:

```rust
        "null_handling" => Token::NullHandling,
```

**Step 4: Update item parser**

```rust
    let item = choice((
        defaults.map(Item::Defaults),
        calendar.map(Item::Calendar),
        dimension.map(Item::Dimension),
        table.map(Item::Table),
        measures_block.map(Item::Measures),
    ));
```

**Step 5: Add tests**

```rust
    #[test]
    fn test_parse_defaults() {
        let input = r#"
            defaults {
                calendar dates;
                fiscal_year_start July;
                week_start Monday;
                null_handling coalesce_zero;
                decimal_places 2;
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Defaults(def) = &result.items[0].value {
            assert_eq!(def.settings.len(), 5);
        } else {
            panic!("Expected defaults");
        }
    }

    #[test]
    fn test_parse_measures() {
        let input = r#"
            measures fact_sales {
                revenue = { sum(@revenue) };
                margin = { revenue - cost };
                enterprise_rev = { sum(@revenue) } where { segment = 'Enterprise' };
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Measures(meas) = &result.items[0].value {
            assert_eq!(meas.table_name.value, "fact_sales");
            assert_eq!(meas.measures.len(), 3);
        } else {
            panic!("Expected measures");
        }
    }
```

**Step 6: Run tests**

Run: `cargo test dsl`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/dsl/
git commit -m "feat(dsl): add measures and defaults parsing"
```

---

## Task 7: Add Report Parsing

**Files:**
- Modify: `src/dsl/parser.rs`

**Step 1: Add period expression parser**

Add to `parser()`:

```rust
    let relative_period = select! {
        Token::Today => RelativePeriod::Today,
        Token::Yesterday => RelativePeriod::Yesterday,
        Token::ThisWeek => RelativePeriod::ThisWeek,
        Token::LastWeek => RelativePeriod::LastWeek,
        Token::ThisMonth => RelativePeriod::ThisMonth,
        Token::LastMonth => RelativePeriod::LastMonth,
        Token::ThisQuarter => RelativePeriod::ThisQuarter,
        Token::LastQuarter => RelativePeriod::LastQuarter,
        Token::ThisYear => RelativePeriod::ThisYear,
        Token::LastYear => RelativePeriod::LastYear,
        Token::Ytd => RelativePeriod::Ytd,
        Token::Qtd => RelativePeriod::Qtd,
        Token::Mtd => RelativePeriod::Mtd,
        Token::ThisFiscalYear => RelativePeriod::ThisFiscalYear,
        Token::LastFiscalYear => RelativePeriod::LastFiscalYear,
        Token::ThisFiscalQuarter => RelativePeriod::ThisFiscalQuarter,
        Token::LastFiscalQuarter => RelativePeriod::LastFiscalQuarter,
        Token::FiscalYtd => RelativePeriod::FiscalYtd,
    }.labelled("relative period");

    let period_unit = select! {
        Token::Day => PeriodUnit::Days,
        Token::Week => PeriodUnit::Weeks,
        Token::Month => PeriodUnit::Months,
        Token::Quarter => PeriodUnit::Quarters,
        Token::Year => PeriodUnit::Years,
    };

    // last_N_units pattern (e.g., last_12_months)
    // We parse this as an identifier and decode it
    let trailing_period = select! {
        Token::Ident(s) if s.starts_with("last_") => s.to_string(),
    }.try_map(|s, span| {
        // Parse "last_12_months" pattern
        let parts: Vec<&str> = s.split('_').collect();
        if parts.len() == 3 && parts[0] == "last" {
            if let Ok(count) = parts[1].parse::<u32>() {
                let unit = match parts[2] {
                    "days" => Some(PeriodUnit::Days),
                    "weeks" => Some(PeriodUnit::Weeks),
                    "months" => Some(PeriodUnit::Months),
                    "quarters" => Some(PeriodUnit::Quarters),
                    "years" => Some(PeriodUnit::Years),
                    _ => None,
                };
                if let Some(unit) = unit {
                    return Ok(PeriodExpr::Trailing { count, unit });
                }
            }
        }
        Err(Rich::custom(span, format!("Invalid trailing period: {}", s)))
    });

    let period_expr = choice((
        relative_period.map(PeriodExpr::Relative),
        trailing_period,
        // Could add range(date, date), month(2024-03), etc. later
    ));

    let time_suffix = select! {
        Token::Ytd => TimeSuffix::Ytd,
        Token::Qtd => TimeSuffix::Qtd,
        Token::Mtd => TimeSuffix::Mtd,
        Token::Wtd => TimeSuffix::Wtd,
        Token::FiscalYtd => TimeSuffix::FiscalYtd,
        Token::FiscalQtd => TimeSuffix::FiscalQtd,
        Token::PriorYear => TimeSuffix::PriorYear,
        Token::PriorQuarter => TimeSuffix::PriorQuarter,
        Token::PriorMonth => TimeSuffix::PriorMonth,
        Token::PriorWeek => TimeSuffix::PriorWeek,
        Token::YoyGrowth => TimeSuffix::YoyGrowth,
        Token::QoqGrowth => TimeSuffix::QoqGrowth,
        Token::MomGrowth => TimeSuffix::MomGrowth,
        Token::WowGrowth => TimeSuffix::WowGrowth,
        Token::YoyDelta => TimeSuffix::YoyDelta,
        Token::QoqDelta => TimeSuffix::QoqDelta,
        Token::MomDelta => TimeSuffix::MomDelta,
        Token::WowDelta => TimeSuffix::WowDelta,
        Token::Rolling3m => TimeSuffix::Rolling3m,
        Token::Rolling6m => TimeSuffix::Rolling6m,
        Token::Rolling12m => TimeSuffix::Rolling12m,
        Token::Rolling3mAvg => TimeSuffix::Rolling3mAvg,
        Token::Rolling6mAvg => TimeSuffix::Rolling6mAvg,
        Token::Rolling12mAvg => TimeSuffix::Rolling12mAvg,
    };
```

**Step 2: Add group, show, sort parsers**

```rust
    // group { source.path.level as "Label"; ... }
    let drill_path_ref = ident
        .then_ignore(just(Token::Dot))
        .then(ident)
        .then_ignore(just(Token::Dot))
        .then(ident)
        .map(|((source, path_name), level)| DrillPathRef {
            source: source.to_string(),
            path_name: path_name.to_string(),
            level: level.to_string(),
        });

    let group_item = drill_path_ref.map_with(|p, e| Spanned::new(p, e.span()))
        .then(
            just(Token::As)
                .ignore_then(string_lit.map_with(|s, e| Spanned::new(s, e.span())))
                .or_not()
        )
        .then_ignore(just(Token::Semi))
        .map(|(path, label)| GroupItem { path, label });

    let group_block = just(Token::Group)
        .ignore_then(
            group_item.map_with(|g, e| Spanned::new(g, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // show { measure.suffix as "Label"; name = { expr }; ... }
    let show_measure = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then(
            just(Token::Dot)
                .ignore_then(time_suffix)
                .or_not()
        )
        .then(
            just(Token::As)
                .ignore_then(string_lit.map_with(|s, e| Spanned::new(s, e.span())))
                .or_not()
        )
        .then_ignore(just(Token::Semi))
        .map(|((name, suffix), label)| ShowItem::Measure { name, suffix, label });

    let show_inline = ident.map_with(|n, e| Spanned::new(n, e.span()))
        .then_ignore(just(Token::Eq))
        .then(sql_expr.clone().map_with(|s, e| Spanned::new(s, e.span())))
        .then(
            just(Token::As)
                .ignore_then(string_lit.map_with(|s, e| Spanned::new(s, e.span())))
                .or_not()
        )
        .then_ignore(just(Token::Semi))
        .map(|((name, expr), label)| ShowItem::Inline { name, expr, label });

    let show_item = choice((show_inline, show_measure));

    let show_block = just(Token::Show)
        .ignore_then(
            show_item.map_with(|s, e| Spanned::new(s, e.span()))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // sort column.asc, column.desc;
    let sort_dir = select! {
        Token::Asc => SortDirection::Asc,
        Token::Desc => SortDirection::Desc,
    };

    let sort_item = ident.map_with(|c, e| Spanned::new(c, e.span()))
        .then_ignore(just(Token::Dot))
        .then(sort_dir)
        .map(|(column, direction)| SortItem { column, direction });

    let sort_clause = just(Token::Sort)
        .ignore_then(
            sort_item.map_with(|s, e| Spanned::new(s, e.span()))
                .separated_by(just(Token::Comma))
                .collect::<Vec<_>>()
        )
        .then_ignore(just(Token::Semi));
```

**Step 3: Add report parser**

```rust
    // report name { from table; use_date col; period expr; group { }; show { }; ... }
    let report = just(Token::Report)
        .ignore_then(ident.map_with(|n, e| Spanned::new(n, e.span())))
        .then(
            // from table [, table];
            just(Token::From)
                .ignore_then(
                    ident.map_with(|t, e| Spanned::new(t, e.span()))
                        .separated_by(just(Token::Comma))
                        .collect::<Vec<_>>()
                )
                .then_ignore(just(Token::Semi))
                // use_date col [, col];
                .then(
                    just(Token::UseDate)
                        .ignore_then(
                            ident.map_with(|c, e| Spanned::new(c, e.span()))
                                .separated_by(just(Token::Comma))
                                .collect::<Vec<_>>()
                        )
                        .then_ignore(just(Token::Semi))
                )
                // period expr;
                .then(
                    just(Token::Period)
                        .ignore_then(period_expr.map_with(|p, e| Spanned::new(p, e.span())))
                        .then_ignore(just(Token::Semi))
                        .or_not()
                )
                // group { }
                .then(group_block.or_not())
                // show { }
                .then(show_block.or_not())
                // filter { };
                .then(
                    just(Token::Filter)
                        .ignore_then(sql_expr.clone().map_with(|s, e| Spanned::new(s, e.span())))
                        .then_ignore(just(Token::Semi).or_not())
                        .or_not()
                )
                // sort col.dir, ...;
                .then(sort_clause.or_not())
                // limit N;
                .then(
                    just(Token::Limit)
                        .ignore_then(select! { Token::Number(n) => n.parse::<u64>().unwrap_or(0) }
                            .map_with(|n, e| Spanned::new(n, e.span())))
                        .then_ignore(just(Token::Semi))
                        .or_not()
                )
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(name, (((((((from, use_date), period), group), show), filter), sort), limit))| {
            Report {
                name,
                from,
                use_date,
                period,
                group: group.unwrap_or_default(),
                show: show.unwrap_or_default(),
                filter,
                sort: sort.unwrap_or_default(),
                limit,
            }
        });
```

**Step 4: Update item parser**

```rust
    let item = choice((
        defaults.map(Item::Defaults),
        calendar.map(Item::Calendar),
        dimension.map(Item::Dimension),
        table.map(Item::Table),
        measures_block.map(Item::Measures),
        report.map(Item::Report),
    ));
```

**Step 5: Add report test**

```rust
    #[test]
    fn test_parse_report() {
        let input = r#"
            report quarterly_review {
                from fact_sales;
                use_date order_date;
                period last_12_months;
                group {
                    dates.standard.month as "Month";
                    customers.geo.region as "Region";
                }
                show {
                    revenue as "Revenue";
                    revenue.yoy_growth as "YoY Growth";
                    margin_pct as "Margin %";
                }
                sort revenue.desc;
                limit 20;
            }
        "#;

        let result = parse_str(input).unwrap();
        if let Item::Report(rep) = &result.items[0].value {
            assert_eq!(rep.name.value, "quarterly_review");
            assert_eq!(rep.from.len(), 1);
            assert_eq!(rep.from[0].value, "fact_sales");
            assert_eq!(rep.group.len(), 2);
            assert_eq!(rep.show.len(), 3);
            assert!(rep.limit.is_some());
        } else {
            panic!("Expected report");
        }
    }
```

**Step 6: Run tests**

Run: `cargo test dsl`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/dsl/
git commit -m "feat(dsl): add report parsing with periods, groups, and show clauses"
```

---

## Task 8: Wire Up Parse Function and Add Integration Test

**Files:**
- Modify: `src/dsl/mod.rs`
- Create: `tests/dsl_integration.rs`

**Step 1: Implement the parse function**

Replace the stub `parse` function in `src/dsl/mod.rs`:

```rust
use chumsky::prelude::*;

/// Parse a DSL source string.
pub fn parse(source: &str) -> ParseResult {
    // First, lex the source
    let (tokens, lex_errors) = lexer::lexer().parse(source).into_output_errors();

    let mut diagnostics: Vec<Diagnostic> = lex_errors
        .into_iter()
        .map(|e| Diagnostic {
            span: e.span().into_range(),
            severity: Severity::Error,
            message: e.to_string(),
        })
        .collect();

    let tokens = match tokens {
        Some(t) => t,
        None => {
            return ParseResult {
                model: None,
                diagnostics,
            };
        }
    };

    // Then parse the tokens
    let eoi = tokens.len()..tokens.len();
    let token_stream = tokens.as_slice().spanned(eoi.into());

    let (model, parse_errors) = parser::parser().parse(token_stream).into_output_errors();

    diagnostics.extend(parse_errors.into_iter().map(|e| Diagnostic {
        span: e.span().into_range(),
        severity: Severity::Error,
        message: e.to_string(),
    }));

    ParseResult { model, diagnostics }
}
```

**Step 2: Create integration test**

Create `tests/dsl_integration.rs`:

```rust
//! Integration tests for the DSL parser.

use mantis::dsl::{self, Item, GrainLevel, DataType, AtomType};

/// Test parsing Example 1 from the DSL spec (Simple CSV Report)
#[test]
fn test_spec_example_1_simple_csv() {
    let input = r#"
        calendar auto {
            generate day+;
            range infer min 2020-01-01 max 2030-12-31;
            drill_path standard { day -> week -> month -> quarter -> year };
            week_start Monday;
        }

        table sales_export {
            source "exports/q4_sales.csv";

            atoms {
                deal_value decimal;
                quantity int;
            }

            times {
                close_date -> auto.day;
            }

            slicers {
                sales_rep string;
                region string;
                product_name string;
                deal_stage string;
            }
        }

        measures sales_export {
            revenue = { sum(@deal_value) };
            deals = { count(*) };
            avg_deal = { revenue / deals };
        }

        report rep_performance {
            from sales_export;
            use_date close_date;
            period this_quarter;

            group {
                auto.standard.month as "Month";
            }

            show {
                revenue as "Revenue";
                deals as "Deals";
                avg_deal as "Avg Deal Size";
            }

            sort revenue.desc;
            limit 20;
        }
    "#;

    let result = dsl::parse(input);

    // Print any errors for debugging
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.diagnostics.is_empty(), "Expected no parse errors");
    let model = result.model.expect("Expected model to be parsed");

    // Check we have all expected items
    assert_eq!(model.items.len(), 4);

    // Check calendar
    assert!(matches!(&model.items[0].value, Item::Calendar(_)));

    // Check table
    if let Item::Table(table) = &model.items[1].value {
        assert_eq!(table.name.value, "sales_export");
        assert_eq!(table.atoms.len(), 2);
        assert_eq!(table.times.len(), 1);
        assert_eq!(table.slicers.len(), 4);
    } else {
        panic!("Expected table");
    }

    // Check measures
    if let Item::Measures(measures) = &model.items[2].value {
        assert_eq!(measures.table_name.value, "sales_export");
        assert_eq!(measures.measures.len(), 3);
    } else {
        panic!("Expected measures");
    }

    // Check report
    if let Item::Report(report) = &model.items[3].value {
        assert_eq!(report.name.value, "rep_performance");
    } else {
        panic!("Expected report");
    }
}

/// Test that parse errors are collected without crashing
#[test]
fn test_parse_errors_collected() {
    let input = r#"
        table broken {
            source "test.csv";
            atoms {
                bad_syntax @@@;
            }
        }
    "#;

    let result = dsl::parse(input);

    // Should have errors but not panic
    assert!(!result.diagnostics.is_empty());
}

/// Test empty input
#[test]
fn test_parse_empty() {
    let result = dsl::parse("");
    assert!(result.diagnostics.is_empty());
    assert!(result.model.is_some());
    assert!(result.model.unwrap().items.is_empty());
}
```

**Step 3: Run integration tests**

Run: `cargo test dsl_integration`
Expected: Tests pass (some may need adjustment based on parser behavior)

**Step 4: Commit**

```bash
git add src/dsl/mod.rs tests/dsl_integration.rs
git commit -m "feat(dsl): wire up parse function with integration tests"
```

---

## Task 9: Add SQL Expression Validation with sqlparser

**Files:**
- Create: `src/dsl/sql_expr.rs`
- Modify: `src/dsl/mod.rs`
- Modify: `src/dsl/parser.rs`

**Step 1: Create SQL expression validator**

Create `src/dsl/sql_expr.rs`:

```rust
//! SQL expression parsing and validation using sqlparser-rs.

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::span::Span;

/// A validated SQL expression.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedSqlExpr {
    /// Original raw SQL text.
    pub raw: String,
    /// Span in the source file.
    pub span: Span,
    /// Whether this is a valid SQL expression.
    pub is_valid: bool,
    /// Validation error message if invalid.
    pub error: Option<String>,
}

impl ValidatedSqlExpr {
    /// Validate a SQL expression string.
    pub fn validate(raw: String, span: Span) -> Self {
        let dialect = GenericDialect {};

        // Try to parse as a standalone expression
        // sqlparser needs a complete statement, so we wrap in SELECT
        let test_sql = format!("SELECT {}", raw);

        match Parser::parse_sql(&dialect, &test_sql) {
            Ok(_) => Self {
                raw,
                span,
                is_valid: true,
                error: None,
            },
            Err(e) => Self {
                raw,
                span,
                is_valid: false,
                error: Some(e.to_string()),
            },
        }
    }
}

/// Extract the raw SQL text from source given a span.
///
/// The span should point to the `{ }` block. This extracts
/// the content between the braces.
pub fn extract_sql_from_source(source: &str, span: &Span) -> Option<String> {
    let block = source.get(span.clone())?;

    // Find the opening and closing braces
    let start = block.find('{')? + 1;
    let end = block.rfind('}')?;

    if start < end {
        Some(block[start..end].trim().to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_simple_expression() {
        let expr = ValidatedSqlExpr::validate("sum(amount)".to_string(), 0..11);
        assert!(expr.is_valid);
        assert!(expr.error.is_none());
    }

    #[test]
    fn test_validate_complex_expression() {
        let expr = ValidatedSqlExpr::validate(
            "case when amount > 1000 then 'Large' else 'Small' end".to_string(),
            0..50,
        );
        assert!(expr.is_valid);
    }

    #[test]
    fn test_validate_atom_reference() {
        // @amount is our DSL syntax, not valid SQL directly
        // We'd need to transform this before validation
        let expr = ValidatedSqlExpr::validate("sum(@amount)".to_string(), 0..12);
        // This will fail because @ is not valid SQL
        assert!(!expr.is_valid);
    }

    #[test]
    fn test_validate_measure_math() {
        let expr = ValidatedSqlExpr::validate("revenue - cost".to_string(), 0..14);
        assert!(expr.is_valid);
    }

    #[test]
    fn test_invalid_expression() {
        let expr = ValidatedSqlExpr::validate("sum((((".to_string(), 0..7);
        assert!(!expr.is_valid);
        assert!(expr.error.is_some());
    }
}
```

**Step 2: Add module to mod.rs**

```rust
pub mod sql_expr;
```

**Step 3: Run tests**

Run: `cargo test dsl::sql_expr`
Expected: Tests pass

**Step 4: Commit**

```bash
git add src/dsl/sql_expr.rs src/dsl/mod.rs
git commit -m "feat(dsl): add SQL expression validation with sqlparser-rs"
```

---

## Task 10: Add Basic Validation Pass

**Files:**
- Create: `src/dsl/validation.rs`
- Modify: `src/dsl/mod.rs`

**Step 1: Create validation module**

Create `src/dsl/validation.rs`:

```rust
//! Semantic validation for parsed DSL models.
//!
//! Checks for:
//! - Undefined references (tables, dimensions, calendars)
//! - Duplicate names
//! - Type mismatches

use std::collections::{HashMap, HashSet};

use super::ast::*;
use super::span::{Span, Spanned};
use super::{Diagnostic, Severity};

/// Validate a parsed model and return diagnostics.
pub fn validate(model: &Model) -> Vec<Diagnostic> {
    let mut validator = Validator::new();
    validator.validate_model(model);
    validator.diagnostics
}

struct Validator {
    diagnostics: Vec<Diagnostic>,

    // Symbol tables for reference checking
    calendars: HashMap<String, Span>,
    dimensions: HashMap<String, Span>,
    tables: HashMap<String, Span>,
    measures: HashMap<String, HashMap<String, Span>>, // table -> measure -> span
}

impl Validator {
    fn new() -> Self {
        Self {
            diagnostics: Vec::new(),
            calendars: HashMap::new(),
            dimensions: HashMap::new(),
            tables: HashMap::new(),
            measures: HashMap::new(),
        }
    }

    fn error(&mut self, span: Span, message: impl Into<String>) {
        self.diagnostics.push(Diagnostic {
            span,
            severity: Severity::Error,
            message: message.into(),
        });
    }

    fn warning(&mut self, span: Span, message: impl Into<String>) {
        self.diagnostics.push(Diagnostic {
            span,
            severity: Severity::Warning,
            message: message.into(),
        });
    }

    fn validate_model(&mut self, model: &Model) {
        // First pass: collect all definitions
        for item in &model.items {
            match &item.value {
                Item::Calendar(cal) => {
                    self.register_calendar(cal);
                }
                Item::Dimension(dim) => {
                    self.register_dimension(dim);
                }
                Item::Table(table) => {
                    self.register_table(table);
                }
                Item::Measures(measures) => {
                    self.register_measures(measures);
                }
                Item::Defaults(_) | Item::Report(_) => {}
            }
        }

        // Second pass: validate references
        for item in &model.items {
            match &item.value {
                Item::Table(table) => {
                    self.validate_table(table);
                }
                Item::Measures(measures) => {
                    self.validate_measures(measures);
                }
                Item::Report(report) => {
                    self.validate_report(report);
                }
                _ => {}
            }
        }
    }

    fn register_calendar(&mut self, cal: &Calendar) {
        let name = &cal.name.value;
        if let Some(existing) = self.calendars.get(name) {
            self.error(
                cal.name.span.clone(),
                format!("Duplicate calendar '{}', first defined at {:?}", name, existing),
            );
        } else {
            self.calendars.insert(name.clone(), cal.name.span.clone());
        }
    }

    fn register_dimension(&mut self, dim: &Dimension) {
        let name = &dim.name.value;
        if let Some(existing) = self.dimensions.get(name) {
            self.error(
                dim.name.span.clone(),
                format!("Duplicate dimension '{}', first defined at {:?}", name, existing),
            );
        } else {
            self.dimensions.insert(name.clone(), dim.name.span.clone());
        }
    }

    fn register_table(&mut self, table: &Table) {
        let name = &table.name.value;
        if let Some(existing) = self.tables.get(name) {
            self.error(
                table.name.span.clone(),
                format!("Duplicate table '{}', first defined at {:?}", name, existing),
            );
        } else {
            self.tables.insert(name.clone(), table.name.span.clone());
        }
    }

    fn register_measures(&mut self, block: &MeasureBlock) {
        let table_name = &block.table_name.value;
        let entry = self.measures.entry(table_name.clone()).or_default();

        for measure in &block.measures {
            let name = &measure.value.name.value;
            if let Some(existing) = entry.get(name) {
                self.error(
                    measure.value.name.span.clone(),
                    format!("Duplicate measure '{}' in table '{}', first defined at {:?}",
                        name, table_name, existing),
                );
            } else {
                entry.insert(name.clone(), measure.value.name.span.clone());
            }
        }
    }

    fn validate_table(&mut self, table: &Table) {
        // Check time bindings reference valid calendars
        for time in &table.times {
            let cal_name = &time.value.calendar.value;
            if !self.calendars.contains_key(cal_name) {
                self.error(
                    time.value.calendar.span.clone(),
                    format!("Unknown calendar '{}' in time binding", cal_name),
                );
            }
        }

        // Check FK slicers reference valid dimensions
        for slicer in &table.slicers {
            if let Slicer::ForeignKey { dimension, .. } = &slicer.value {
                if !self.dimensions.contains_key(&dimension.value) {
                    self.error(
                        dimension.span.clone(),
                        format!("Unknown dimension '{}' in slicer", dimension.value),
                    );
                }
            }
        }

        // Check via slicers reference valid slicers in same table
        let slicer_names: HashSet<_> = table.slicers.iter()
            .filter_map(|s| match &s.value {
                Slicer::Inline { name, .. }
                | Slicer::ForeignKey { name, .. }
                | Slicer::Calculated { name, .. } => Some(name.value.clone()),
                Slicer::Via { .. } => None,
            })
            .collect();

        for slicer in &table.slicers {
            if let Slicer::Via { via_slicer, .. } = &slicer.value {
                if !slicer_names.contains(&via_slicer.value) {
                    self.error(
                        via_slicer.span.clone(),
                        format!("Unknown slicer '{}' in via reference", via_slicer.value),
                    );
                }
            }
        }
    }

    fn validate_measures(&mut self, block: &MeasureBlock) {
        let table_name = &block.table_name.value;

        // Check table exists
        if !self.tables.contains_key(table_name) {
            self.error(
                block.table_name.span.clone(),
                format!("Measures block references unknown table '{}'", table_name),
            );
        }
    }

    fn validate_report(&mut self, report: &Report) {
        // Check from tables exist
        for table in &report.from {
            if !self.tables.contains_key(&table.value) {
                self.error(
                    table.span.clone(),
                    format!("Report references unknown table '{}'", table.value),
                );
            }
        }

        // Check show items reference valid measures
        for show in &report.show {
            if let ShowItem::Measure { name, .. } = &show.value {
                let mut found = false;
                for table in &report.from {
                    if let Some(measures) = self.measures.get(&table.value) {
                        if measures.contains_key(&name.value) {
                            found = true;
                            break;
                        }
                    }
                }
                if !found {
                    self.warning(
                        name.span.clone(),
                        format!("Measure '{}' not found in source tables", name.value),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl;

    #[test]
    fn test_validate_undefined_calendar() {
        let input = r#"
            table orders {
                source "orders.csv";
                times {
                    order_date -> undefined_cal.day;
                }
            }
        "#;

        let result = dsl::parse(input);
        let model = result.model.unwrap();
        let diagnostics = validate(&model);

        assert!(!diagnostics.is_empty());
        assert!(diagnostics[0].message.contains("Unknown calendar"));
    }

    #[test]
    fn test_validate_undefined_dimension() {
        let input = r#"
            table orders {
                source "orders.csv";
                slicers {
                    customer_id -> undefined_dim.customer_id;
                }
            }
        "#;

        let result = dsl::parse(input);
        let model = result.model.unwrap();
        let diagnostics = validate(&model);

        assert!(!diagnostics.is_empty());
        assert!(diagnostics[0].message.contains("Unknown dimension"));
    }

    #[test]
    fn test_validate_duplicate_table() {
        let input = r#"
            table orders {
                source "orders1.csv";
            }
            table orders {
                source "orders2.csv";
            }
        "#;

        let result = dsl::parse(input);
        let model = result.model.unwrap();
        let diagnostics = validate(&model);

        assert!(!diagnostics.is_empty());
        assert!(diagnostics[0].message.contains("Duplicate table"));
    }

    #[test]
    fn test_validate_valid_model() {
        let input = r#"
            calendar dates {
                generate day+;
                range infer;
            }

            dimension customers {
                source "dim_customers";
                key customer_id;
                attributes {
                    name string;
                }
            }

            table orders {
                source "orders.csv";
                times {
                    order_date -> dates.day;
                }
                slicers {
                    customer_id -> customers.customer_id;
                }
            }
        "#;

        let result = dsl::parse(input);
        let model = result.model.unwrap();
        let diagnostics = validate(&model);

        // Should have no errors
        let errors: Vec<_> = diagnostics.iter()
            .filter(|d| d.severity == Severity::Error)
            .collect();
        assert!(errors.is_empty(), "Unexpected errors: {:?}", errors);
    }
}
```

**Step 2: Add module and integrate into parse**

Add to `src/dsl/mod.rs`:

```rust
pub mod validation;

pub use validation::validate;
```

Update the `parse` function to include validation:

```rust
/// Parse and validate a DSL source string.
pub fn parse(source: &str) -> ParseResult {
    // ... existing lexing and parsing code ...

    // Validate if we have a model
    if let Some(ref model) = model {
        let validation_diagnostics = validation::validate(model);
        diagnostics.extend(validation_diagnostics);
    }

    ParseResult { model, diagnostics }
}
```

**Step 3: Run tests**

Run: `cargo test dsl::validation`
Expected: Tests pass

**Step 4: Commit**

```bash
git add src/dsl/validation.rs src/dsl/mod.rs
git commit -m "feat(dsl): add semantic validation for references and duplicates"
```

---

## Summary

After completing all tasks, you will have:

1. **Chumsky-based parser** with error recovery
2. **Complete AST types** for all DSL constructs
3. **Lexer** with keyword recognition and comment handling
4. **Parsers** for: defaults, calendar, dimension, table, measures, report
5. **SQL expression validation** via sqlparser-rs
6. **Semantic validation** for undefined references and duplicates
7. **Integration tests** against DSL spec examples

The parser is LSP-ready with span information on all nodes. Phase 2 will connect this to the semantic model for query planning.
