//! DSL parser for the Mantis semantic model language.
//!
//! This module provides parsing and AST types for the Mantis DSL, which is used
//! to define semantic data models for analytics. The DSL supports:
//!
//! - **Defaults**: Model-wide settings (calendar, fiscal year, null handling)
//! - **Calendars**: Physical (from existing tables) or generated (ephemeral CTE)
//! - **Dimensions**: Lookup tables with attributes and drill paths
//! - **Tables**: Data sources with atoms (numbers), times (dates), and slicers
//! - **Measures**: Aggregations and calculations over atoms
//! - **Reports**: Query definitions with grouping, filtering, and time intelligence
//!
//! # Example
//!
//! ```ignore
//! use mantis::dsl;
//!
//! let source = r#"
//!     calendar auto {
//!         generate day+;
//!         range infer min 2020-01-01 max 2030-12-31;
//!         drill_path standard { day -> week -> month -> quarter -> year };
//!     }
//!
//!     table sales {
//!         source "data/sales.csv";
//!         atoms { amount decimal; }
//!         times { sale_date -> auto.day; }
//!         slicers { region string; }
//!     }
//!
//!     measures sales {
//!         revenue = { sum(@amount) };
//!     }
//!
//!     report summary {
//!         from sales;
//!         use_date sale_date;
//!         period last_12_months;
//!         group { auto.standard.month; region; }
//!         show { revenue; }
//!     }
//! "#;
//!
//! let result = dsl::parse(source);
//! if let Some(model) = result.model {
//!     println!("Parsed {} items", model.items.len());
//! }
//! for diag in &result.diagnostics {
//!     eprintln!("{}", diag);
//! }
//! ```

pub mod ast;
pub mod lexer;
pub mod parser;
pub mod span;

pub use ast::*;
pub use span::{Span, Spanned};

/// Result of parsing a DSL source.
#[derive(Debug)]
pub struct ParseResult {
    /// The parsed model, if parsing succeeded.
    pub model: Option<Model>,
    /// Diagnostic messages (errors and warnings).
    pub diagnostics: Vec<Diagnostic>,
}

impl ParseResult {
    /// Returns true if parsing succeeded without errors.
    pub fn is_ok(&self) -> bool {
        self.model.is_some()
            && !self
                .diagnostics
                .iter()
                .any(|d| d.severity == Severity::Error)
    }

    /// Returns true if there are any errors.
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == Severity::Error)
    }

    /// Returns true if there are any warnings.
    pub fn has_warnings(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == Severity::Warning)
    }

    /// Returns only the error diagnostics.
    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
    }

    /// Returns only the warning diagnostics.
    pub fn warnings(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Warning)
    }
}

/// A diagnostic message with source location.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// The span in the source where the diagnostic applies.
    pub span: Span,
    /// The severity level.
    pub severity: Severity,
    /// The diagnostic message.
    pub message: String,
}

impl Diagnostic {
    /// Create a new error diagnostic.
    pub fn error(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            severity: Severity::Error,
            message: message.into(),
        }
    }

    /// Create a new warning diagnostic.
    pub fn warning(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            severity: Severity::Warning,
            message: message.into(),
        }
    }
}

/// Diagnostic severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// A fatal error that prevents compilation.
    Error,
    /// A warning that doesn't prevent compilation.
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

impl std::error::Error for Diagnostic {}

/// Parse a DSL source string.
///
/// Returns a `ParseResult` containing the parsed model (if successful) and
/// any diagnostic messages.
///
/// # Example
///
/// ```ignore
/// use mantis::dsl;
///
/// let result = dsl::parse(r#"
///     table test {
///         source "data.csv";
///         atoms { value decimal; }
///         times { }
///         slicers { }
///     }
/// "#);
///
/// if result.is_ok() {
///     println!("Parsing succeeded!");
/// }
/// ```
pub fn parse(source: &str) -> ParseResult {
    use chumsky::error::Rich;
    use chumsky::input::Input;
    use chumsky::span::SimpleSpan;
    use chumsky::span::Span as _;
    use chumsky::Parser as _;

    // Step 1: Lexical analysis
    let (tokens, lex_errs) = lexer::lexer().parse(source).into_output_errors();

    // Collect lexer errors as diagnostics
    let mut diagnostics: Vec<Diagnostic> = lex_errs
        .into_iter()
        .map(|e: Rich<'_, char>| {
            let span = e.span();
            Diagnostic::error(span.start()..span.end(), e.to_string())
        })
        .collect();

    // If lexing failed completely, return early
    let tokens: Vec<(lexer::Token<'_>, SimpleSpan)> = match tokens {
        Some(t) => t,
        None => {
            return ParseResult {
                model: None,
                diagnostics,
            };
        }
    };

    // Step 2: Parsing
    let len = source.len();
    let eoi: SimpleSpan = (len..len).into();
    let token_stream = tokens.as_slice().map(
        eoi,
        |(tok, span): &(lexer::Token<'_>, SimpleSpan)| (tok, span),
    );

    let (model, parse_errs) = parser::parser().parse(token_stream).into_output_errors();

    // Collect parser errors as diagnostics
    diagnostics.extend(
        parse_errs
            .into_iter()
            .map(|e: Rich<'_, lexer::Token<'_>, SimpleSpan>| {
                let span = e.span();
                Diagnostic::error(span.start()..span.end(), e.to_string())
            }),
    );

    ParseResult { model, diagnostics }
}

/// Parse a DSL source file.
///
/// Reads the file at the given path and parses it as DSL source.
///
/// # Errors
///
/// Returns an `io::Error` if the file cannot be read.
///
/// # Example
///
/// ```ignore
/// use mantis::dsl;
/// use std::path::Path;
///
/// let result = dsl::parse_file(Path::new("model.mantis"))?;
/// if result.is_ok() {
///     println!("Model parsed successfully!");
/// }
/// ```
pub fn parse_file(path: &std::path::Path) -> std::io::Result<ParseResult> {
    let source = std::fs::read_to_string(path)?;
    Ok(parse(&source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_table() {
        let result = parse(r#"
            table sales {
                source "sales.csv";
                atoms { amount decimal; }
            }
        "#);
        assert!(result.model.is_some());
        assert!(result.diagnostics.is_empty());
        let model = result.model.unwrap();
        assert_eq!(model.items.len(), 1);
    }

    #[test]
    fn test_parse_error_on_invalid_input() {
        let result = parse("invalid { syntax");
        // Should have parse errors
        assert!(result.has_errors() || result.model.is_none());
    }

    #[test]
    fn test_parse_result_helpers() {
        let result = ParseResult {
            model: Some(Model {
                defaults: None,
                items: vec![],
            }),
            diagnostics: vec![Diagnostic::warning(0..1, "test warning")],
        };

        assert!(result.is_ok());
        assert!(!result.has_errors());
        assert!(result.has_warnings());
        assert_eq!(result.errors().count(), 0);
        assert_eq!(result.warnings().count(), 1);
    }

    #[test]
    fn test_diagnostic_display() {
        let diag = Diagnostic::error(10..20, "test error");
        let display = format!("{}", diag);
        assert!(display.contains("error"));
        assert!(display.contains("test error"));
        assert!(display.contains("10..20"));
    }

    #[test]
    fn test_grain_level_from_str() {
        assert_eq!(GrainLevel::from_str("day"), Some(GrainLevel::Day));
        assert_eq!(GrainLevel::from_str("MONTH"), Some(GrainLevel::Month));
        assert_eq!(
            GrainLevel::from_str("fiscal_quarter"),
            Some(GrainLevel::FiscalQuarter)
        );
        assert_eq!(GrainLevel::from_str("invalid"), None);
    }

    #[test]
    fn test_grain_level_and_coarser() {
        let day_and_coarser = GrainLevel::Day.and_coarser();
        assert!(day_and_coarser.contains(&GrainLevel::Day));
        assert!(day_and_coarser.contains(&GrainLevel::Month));
        assert!(day_and_coarser.contains(&GrainLevel::Year));
        assert!(!day_and_coarser.contains(&GrainLevel::Hour));

        let month_and_coarser = GrainLevel::Month.and_coarser();
        assert!(!month_and_coarser.contains(&GrainLevel::Day));
        assert!(month_and_coarser.contains(&GrainLevel::Month));
        assert!(month_and_coarser.contains(&GrainLevel::Year));
    }

    #[test]
    fn test_data_type_from_str() {
        assert_eq!(DataType::from_str("string"), Some(DataType::String));
        assert_eq!(DataType::from_str("INT"), Some(DataType::Int));
        assert_eq!(DataType::from_str("timestamp"), Some(DataType::Timestamp));
        assert_eq!(DataType::from_str("unknown"), None);
    }

    #[test]
    fn test_time_suffix_from_str() {
        assert_eq!(TimeSuffix::from_str("ytd"), Some(TimeSuffix::Ytd));
        assert_eq!(TimeSuffix::from_str("YOY_GROWTH"), Some(TimeSuffix::YoyGrowth));
        assert_eq!(
            TimeSuffix::from_str("rolling_12m_avg"),
            Some(TimeSuffix::Rolling12mAvg)
        );
        assert_eq!(TimeSuffix::from_str("invalid"), None);
    }

    #[test]
    fn test_month_from_str() {
        assert_eq!(Month::from_str("January"), Some(Month::January));
        assert_eq!(Month::from_str("jul"), Some(Month::July));
        assert_eq!(Month::from_str("DECEMBER"), Some(Month::December));
        assert_eq!(Month::from_str("invalid"), None);
    }

    #[test]
    fn test_weekday_from_str() {
        assert_eq!(Weekday::from_str("Monday"), Some(Weekday::Monday));
        assert_eq!(Weekday::from_str("fri"), Some(Weekday::Friday));
        assert_eq!(Weekday::from_str("invalid"), None);
    }

    #[test]
    fn test_null_handling_from_str() {
        assert_eq!(
            NullHandling::from_str("coalesce_zero"),
            Some(NullHandling::CoalesceZero)
        );
        assert_eq!(
            NullHandling::from_str("NULL_ON_ZERO"),
            Some(NullHandling::NullOnZero)
        );
        assert_eq!(NullHandling::from_str("invalid"), None);
    }

    #[test]
    fn test_spanned() {
        let spanned = Spanned::new("test", 0..4);
        assert_eq!(spanned.value, "test");
        assert_eq!(spanned.span, 0..4);
        assert_eq!(*spanned, "test"); // Deref

        let mapped = spanned.clone().map(|s| s.len());
        assert_eq!(mapped.value, 4);
        assert_eq!(mapped.span, 0..4);

        let as_ref = spanned.as_ref();
        assert_eq!(*as_ref.value, "test");
    }

    #[test]
    fn test_date_literal() {
        let date = DateLiteral::new(2024, 7, 15);
        assert_eq!(date.year, 2024);
        assert_eq!(date.month, 7);
        assert_eq!(date.day, 15);
        assert_eq!(format!("{}", date), "2024-07-15");
    }
}
