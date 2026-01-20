//! Parser for the Mantis DSL using chumsky.
//!
//! This module provides a parser that transforms a token stream from the lexer
//! into an AST (Abstract Syntax Tree) for semantic model definitions.

use chumsky::prelude::*;
use chumsky::input::ValueInput;

use super::ast::*;
use super::lexer::Token;
use super::span::Spanned;

/// Convert a SimpleSpan to our Span type (Range<usize>)
fn to_span(span: SimpleSpan) -> std::ops::Range<usize> {
    span.start..span.end
}

/// Create the main parser for the Mantis DSL.
///
/// Returns a parser that transforms a token stream into a Model AST.
/// The parser is generic over the input type, accepting any `ValueInput`
/// that produces `Token` values with `SimpleSpan` spans.
pub fn parser<'tokens, 'src: 'tokens, I>() -> impl Parser<'tokens, I, Model, extra::Err<Rich<'tokens, Token<'src>, SimpleSpan>>>
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = SimpleSpan>,
{
    // ==========================================================================
    // Basic token parsers
    // ==========================================================================

    // Parse an identifier (also allow keywords as identifiers in certain contexts)
    let ident = select! {
        Token::Ident(s) => s.to_string(),
    }.labelled("identifier");

    // Parse an identifier or keyword that can be used as a name
    // This allows reserved words like "fiscal" to be used as calendar/table names
    let ident_or_keyword = select! {
        Token::Ident(s) => s.to_string(),
        Token::Fiscal => "fiscal".to_string(),
        Token::Min => "min".to_string(),
        Token::Max => "max".to_string(),
        Token::Generate => "generate".to_string(),
        Token::Include => "include".to_string(),
        Token::Range => "range".to_string(),
        Token::Infer => "infer".to_string(),
        // Grain levels (for drill path levels)
        Token::Minute => "minute".to_string(),
        Token::Hour => "hour".to_string(),
        Token::Day => "day".to_string(),
        Token::Week => "week".to_string(),
        Token::Month => "month".to_string(),
        Token::Quarter => "quarter".to_string(),
        Token::Year => "year".to_string(),
        Token::FiscalMonth => "fiscal_month".to_string(),
        Token::FiscalQuarter => "fiscal_quarter".to_string(),
        Token::FiscalYear => "fiscal_year".to_string(),
    }.labelled("identifier or keyword");

    // Parse a string literal
    let string_lit = select! {
        Token::StringLit(s) => s.to_string(),
    }.labelled("string literal");

    // Parse an atom type (int, decimal, float)
    let atom_type = select! {
        Token::Int => AtomType::Int,
        Token::Decimal => AtomType::Decimal,
        Token::Float => AtomType::Float,
    }.labelled("atom type (int, decimal, float)");

    // Parse a data type (string, int, decimal, float, bool, date, timestamp)
    let data_type = select! {
        Token::String => DataType::String,
        Token::Int => DataType::Int,
        Token::Decimal => DataType::Decimal,
        Token::Float => DataType::Float,
        Token::Bool => DataType::Bool,
        Token::Date => DataType::Date,
        Token::Timestamp => DataType::Timestamp,
    }.labelled("data type");

    // Parse a grain level
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

    // Parse a month
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
    }.labelled("month");

    // Parse a weekday
    let weekday = select! {
        Token::Monday => Weekday::Monday,
        Token::Tuesday => Weekday::Tuesday,
        Token::Wednesday => Weekday::Wednesday,
        Token::Thursday => Weekday::Thursday,
        Token::Friday => Weekday::Friday,
        Token::Saturday => Weekday::Saturday,
        Token::Sunday => Weekday::Sunday,
    }.labelled("weekday");

    // Parse NULL handling mode
    let null_handling = select! {
        Token::CoalesceZero => NullHandling::CoalesceZero,
        Token::NullOnZero => NullHandling::NullOnZero,
        Token::ErrorOnZero => NullHandling::ErrorOnZero,
    }.labelled("null handling mode");

    // ==========================================================================
    // SQL expression parser (reusable)
    // ==========================================================================

    // Parse any token that's not RBrace, then reconstruct the SQL string from tokens
    let sql_token = any()
        .filter(|t: &Token| !matches!(t, Token::RBrace))
        .map_with(|t: Token, e| (t.to_string(), to_span(e.span())));

    // SQL expression parser: { sql_tokens }
    // Parses tokens between braces and reconstructs SQL string
    // Note: whitespace handling is basic - joins tokens with single space
    // This is sufficient for SQL expressions but doesn't handle all edge cases
    let sql_expr = just(Token::LBrace)
        .map_with(|_, e| to_span(e.span()))
        .then(
            sql_token.clone()
                .repeated()
                .collect::<Vec<_>>()
        )
        .then(
            just(Token::RBrace)
                .map_with(|_, e| to_span(e.span()))
        )
        .map(|((lbrace_span, tokens), rbrace_span)| {
            // Reconstruct SQL from tokens, joining with spaces
            let sql = tokens.iter()
                .map(|(s, _)| s.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            // Span covers from LBrace start to RBrace end
            let span = lbrace_span.start..rbrace_span.end;
            SqlExpr::new(sql, span)
        });

    // Parse a date literal from a Number token (YYYY-MM-DD format)
    // The lexer produces numbers like "2024" or "2024.01" but dates are YYYY-MM-DD
    // We need to parse sequences: Number("-")Number("-")Number
    let date_literal = select! {
        Token::Number(s) => s,
    }
    .then_ignore(just(Token::Minus))
    .then(select! { Token::Number(s) => s })
    .then_ignore(just(Token::Minus))
    .then(select! { Token::Number(s) => s })
    .try_map(|((year_str, month_str), day_str), span| {
        let year: u16 = year_str.parse().map_err(|_| Rich::custom(span, "invalid year"))?;
        let month: u8 = month_str.parse().map_err(|_| Rich::custom(span, "invalid month"))?;
        let day: u8 = day_str.parse().map_err(|_| Rich::custom(span, "invalid day"))?;
        Ok(DateLiteral::new(year, month, day))
    })
    .labelled("date literal (YYYY-MM-DD)");

    // ==========================================================================
    // Defaults block
    // ==========================================================================

    // Parse a default setting
    let default_setting_calendar = just(Token::Calendar)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DefaultSetting::Calendar);

    let default_setting_fiscal_year_start = just(Token::FiscalYearStart)
        .ignore_then(
            month.clone()
                .map_with(|m, e| Spanned::new(m, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DefaultSetting::FiscalYearStart);

    let default_setting_week_start = just(Token::WeekStart)
        .ignore_then(
            weekday.clone()
                .map_with(|w, e| Spanned::new(w, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DefaultSetting::WeekStart);

    let default_setting_null_handling = just(Token::NullHandling)
        .ignore_then(
            null_handling.clone()
                .map_with(|nh, e| Spanned::new(nh, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DefaultSetting::NullHandling);

    let default_setting_decimal_places = just(Token::DecimalPlaces)
        .ignore_then(
            select! { Token::Number(s) => s }
                .try_map(|s, span| {
                    s.parse::<u8>().map_err(|_| Rich::custom(span, "invalid decimal places"))
                })
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DefaultSetting::DecimalPlaces);

    let default_setting = choice((
        default_setting_calendar,
        default_setting_fiscal_year_start,
        default_setting_week_start,
        default_setting_null_handling,
        default_setting_decimal_places,
    ));

    // defaults { ... }
    let defaults = just(Token::Defaults)
        .ignore_then(
            default_setting
                .map_with(|s, e| Spanned::new(s, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|settings| Defaults { settings })
        .map_with(|d, e| Spanned::new(d, to_span(e.span())));

    // ==========================================================================
    // Atoms block: atoms { name type; ... }
    // ==========================================================================

    let atom = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then(
            atom_type
                .map_with(|t, e| Spanned::new(t, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(|(name, atom_type)| Atom { name, atom_type });

    let atoms_block = just(Token::Atoms)
        .ignore_then(
            atom
                .map_with(|a, e| Spanned::new(a, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // ==========================================================================
    // Times block: times { name -> calendar.grain; ... }
    // ==========================================================================

    let time_binding = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then_ignore(just(Token::Arrow))
        .then(
            ident.clone()
                .map_with(|c, e| Spanned::new(c, to_span(e.span())))
        )
        .then_ignore(just(Token::Dot))
        .then(
            grain_level
                .map_with(|g, e| Spanned::new(g, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(|((name, calendar), grain)| TimeBinding { name, calendar, grain });

    let times_block = just(Token::Times)
        .ignore_then(
            time_binding
                .map_with(|t, e| Spanned::new(t, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // ==========================================================================
    // Slicers block: slicers { ... }
    // ==========================================================================

    // Inline slicer: name type;
    let slicer_inline = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then(
            data_type
                .map_with(|t, e| Spanned::new(t, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map_with(|(name, data_type_spanned), e| {
            let kind = SlicerKind::Inline { data_type: data_type_spanned.value };
            Slicer {
                name,
                kind: Spanned::new(kind, to_span(e.span())),
            }
        });

    // FK slicer: name -> dimension.key;
    let slicer_fk = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then_ignore(just(Token::Arrow))
        .then(ident.clone())
        .then_ignore(just(Token::Dot))
        .then(ident.clone())
        .then_ignore(just(Token::Semicolon))
        .map_with(|((name, dimension), key_column), e| {
            let kind = SlicerKind::ForeignKey { dimension, key_column };
            Slicer {
                name,
                kind: Spanned::new(kind, to_span(e.span())),
            }
        });

    // Via slicer: name via other_slicer;
    let slicer_via = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then_ignore(just(Token::Via))
        .then(ident.clone())
        .then_ignore(just(Token::Semicolon))
        .map_with(|(name, fk_slicer), e| {
            let kind = SlicerKind::Via { fk_slicer };
            Slicer {
                name,
                kind: Spanned::new(kind, to_span(e.span())),
            }
        });

    // Calculated slicer: name type = { sql_expr };
    let slicer_calculated = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then(
            data_type
                .map_with(|t, e| Spanned::new(t, to_span(e.span())))
        )
        .then_ignore(just(Token::Eq))
        .then(sql_expr.clone())
        .then_ignore(just(Token::Semicolon))
        .map_with(|((name, data_type_spanned), expr), e| {
            let kind = SlicerKind::Calculated {
                data_type: data_type_spanned.value,
                expr,
            };
            Slicer {
                name,
                kind: Spanned::new(kind, to_span(e.span())),
            }
        });

    // Order matters: try FK and Via first (they have distinguishing tokens),
    // then calculated (has = { }), then fall back to inline
    let slicer = choice((
        slicer_fk,
        slicer_via,
        slicer_calculated,
        slicer_inline,
    ));

    let slicers_block = just(Token::Slicers)
        .ignore_then(
            slicer
                .map_with(|s, e| Spanned::new(s, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // ==========================================================================
    // Drill Path: drill_path name { level1 -> level2 -> ... };
    // ==========================================================================

    // Parse a drill path level (identifier or grain keyword treated as identifier)
    let drill_level = ident.clone()
        .or(grain_level.clone().map(|g| g.to_string()))
        .map_with(|s, e| Spanned::new(s, to_span(e.span())));

    // drill_path name { level -> level -> ... };
    let drill_path = just(Token::DrillPath)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            drill_level.clone()
                .separated_by(just(Token::Arrow))
                .at_least(1)
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .then_ignore(just(Token::Semicolon))
        .map(|(name, levels)| DrillPath { name, levels });

    // ==========================================================================
    // Calendar definitions (Generated and Physical)
    // ==========================================================================

    // Generate statement: generate grain+;
    // e.g., generate day+;
    let generate_stmt = just(Token::Generate)
        .ignore_then(
            grain_level.clone()
                .map_with(|g, e| Spanned::new(g, to_span(e.span())))
        )
        .then_ignore(just(Token::Plus))
        .then_ignore(just(Token::Semicolon));

    // Range statement: range infer min DATE max DATE; or range DATE to DATE;
    // For generated calendars: range infer min 2020-01-01 max 2030-12-31;
    let range_infer = just(Token::Range)
        .ignore_then(just(Token::Infer))
        .then(
            just(Token::Min)
                .ignore_then(
                    date_literal.clone()
                        .map_with(|d, e| Spanned::new(d, to_span(e.span())))
                )
                .or_not()
        )
        .then(
            just(Token::Max)
                .ignore_then(
                    date_literal.clone()
                        .map_with(|d, e| Spanned::new(d, to_span(e.span())))
                )
                .or_not()
        )
        .then_ignore(just(Token::Semicolon))
        .map(|((_, min), max)| CalendarRange::Infer { min, max });

    let range_explicit = just(Token::Range)
        .ignore_then(
            date_literal.clone()
                .map_with(|d, e| Spanned::new(d, to_span(e.span())))
        )
        .then_ignore(just(Token::To))
        .then(
            date_literal.clone()
                .map_with(|d, e| Spanned::new(d, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(|(start, end)| CalendarRange::Explicit { start, end });

    let range_stmt = choice((range_infer, range_explicit))
        .map_with(|r, e| Spanned::new(r, to_span(e.span())));

    // week_start statement: week_start Monday;
    let week_start_stmt = just(Token::WeekStart)
        .ignore_then(
            weekday.clone()
                .map_with(|w, e| Spanned::new(w, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon));

    // fiscal_year_start statement: fiscal_year_start July;
    let fiscal_year_start_stmt = just(Token::FiscalYearStart)
        .ignore_then(
            month.clone()
                .map_with(|m, e| Spanned::new(m, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon));

    // include fiscal statement: include fiscal[January];
    let include_fiscal_stmt = just(Token::Include)
        .ignore_then(just(Token::Fiscal))
        .ignore_then(just(Token::LBracket))
        .ignore_then(
            month.clone()
                .map_with(|m, e| Spanned::new(m, to_span(e.span())))
        )
        .then_ignore(just(Token::RBracket))
        .then_ignore(just(Token::Semicolon));

    // Grain mapping for physical calendar: grain = column;
    // e.g., day = date_key;
    let grain_mapping = grain_level.clone()
        .map_with(|g, e| Spanned::new(g, to_span(e.span())))
        .then_ignore(just(Token::Eq))
        .then(
            ident.clone()
                .map_with(|c, e| Spanned::new(c, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(|(level, column)| GrainMapping { level, column });

    // Generated calendar body (inside braces):
    // - generate grain+;
    // - range ...;  (optional)
    // - drill_path ... { ... };  (zero or more)
    // - week_start ...;  (optional)
    // - include fiscal[Month];  (optional)
    // We need a flexible parser that handles these in any order
    #[derive(Clone)]
    enum GenCalPart {
        Generate(Spanned<GrainLevel>),
        Range(Spanned<CalendarRange>),
        DrillPath(Spanned<DrillPath>),
        WeekStart(Spanned<Weekday>),
        Fiscal(Spanned<Month>),
    }

    let gen_cal_part = choice((
        generate_stmt.clone().map(GenCalPart::Generate),
        range_stmt.clone().map(GenCalPart::Range),
        drill_path.clone()
            .map_with(|dp, e| Spanned::new(dp, to_span(e.span())))
            .map(GenCalPart::DrillPath),
        week_start_stmt.clone().map(GenCalPart::WeekStart),
        include_fiscal_stmt.clone().map(GenCalPart::Fiscal),
    ));

    // Generated calendar: calendar name { generate ...; ... }
    // Detected by having no source string after name
    let generated_calendar_body = gen_cal_part
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|parts, span| {
            let mut base_grain: Option<Spanned<GrainLevel>> = None;
            let mut range: Option<Spanned<CalendarRange>> = None;
            let mut drill_paths: Vec<Spanned<DrillPath>> = Vec::new();
            let mut week_start: Option<Spanned<Weekday>> = None;
            let mut fiscal: Option<Spanned<Month>> = None;

            for part in parts {
                match part {
                    GenCalPart::Generate(g) => {
                        if base_grain.is_some() {
                            return Err(Rich::custom(span, "duplicate generate statement"));
                        }
                        base_grain = Some(g);
                    }
                    GenCalPart::Range(r) => {
                        if range.is_some() {
                            return Err(Rich::custom(span, "duplicate range statement"));
                        }
                        range = Some(r);
                    }
                    GenCalPart::DrillPath(dp) => {
                        drill_paths.push(dp);
                    }
                    GenCalPart::WeekStart(w) => {
                        if week_start.is_some() {
                            return Err(Rich::custom(span, "duplicate week_start statement"));
                        }
                        week_start = Some(w);
                    }
                    GenCalPart::Fiscal(f) => {
                        if fiscal.is_some() {
                            return Err(Rich::custom(span, "duplicate include fiscal statement"));
                        }
                        fiscal = Some(f);
                    }
                }
            }

            let base_grain = base_grain.ok_or_else(|| {
                Rich::custom(span, "generated calendar requires generate statement")
            })?;

            Ok(GeneratedCalendar {
                base_grain,
                fiscal,
                range,
                drill_paths,
                week_start,
            })
        });

    // Physical calendar body (inside braces):
    // - grain = column;  (one or more)
    // - drill_path ... { ... };  (zero or more)
    // - fiscal_year_start ...;  (optional)
    // - week_start ...;  (optional)
    #[derive(Clone)]
    enum PhysCalPart {
        GrainMapping(Spanned<GrainMapping>),
        DrillPath(Spanned<DrillPath>),
        FiscalYearStart(Spanned<Month>),
        WeekStart(Spanned<Weekday>),
    }

    let phys_cal_part = choice((
        grain_mapping.clone()
            .map_with(|gm, e| Spanned::new(gm, to_span(e.span())))
            .map(PhysCalPart::GrainMapping),
        drill_path.clone()
            .map_with(|dp, e| Spanned::new(dp, to_span(e.span())))
            .map(PhysCalPart::DrillPath),
        fiscal_year_start_stmt.clone().map(PhysCalPart::FiscalYearStart),
        week_start_stmt.clone().map(PhysCalPart::WeekStart),
    ));

    let physical_calendar_body = phys_cal_part
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|parts, span| {
            let mut grain_mappings: Vec<Spanned<GrainMapping>> = Vec::new();
            let mut drill_paths: Vec<Spanned<DrillPath>> = Vec::new();
            let mut fiscal_year_start: Option<Spanned<Month>> = None;
            let mut week_start: Option<Spanned<Weekday>> = None;

            for part in parts {
                match part {
                    PhysCalPart::GrainMapping(gm) => {
                        grain_mappings.push(gm);
                    }
                    PhysCalPart::DrillPath(dp) => {
                        drill_paths.push(dp);
                    }
                    PhysCalPart::FiscalYearStart(f) => {
                        if fiscal_year_start.is_some() {
                            return Err(Rich::custom(span, "duplicate fiscal_year_start statement"));
                        }
                        fiscal_year_start = Some(f);
                    }
                    PhysCalPart::WeekStart(w) => {
                        if week_start.is_some() {
                            return Err(Rich::custom(span, "duplicate week_start statement"));
                        }
                        week_start = Some(w);
                    }
                }
            }

            if grain_mappings.is_empty() {
                return Err(Rich::custom(span, "physical calendar requires at least one grain mapping"));
            }

            Ok((grain_mappings, drill_paths, fiscal_year_start, week_start))
        });

    // Physical calendar: calendar name "source" { ... }
    // Has a source string after the name
    let physical_calendar = just(Token::Calendar)
        .ignore_then(
            ident_or_keyword.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            string_lit.clone()
                .map_with(|s, e| Spanned::new(s, to_span(e.span())))
        )
        .then(
            physical_calendar_body
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
                .map_with(|body, e| (body, to_span(e.span())))
        )
        .map(|((name, source), ((grain_mappings, drill_paths, fiscal_year_start, week_start), body_span))| {
            let body = CalendarBody::Physical(PhysicalCalendar {
                source,
                grain_mappings,
                drill_paths,
                fiscal_year_start,
                week_start,
            });
            Calendar {
                name,
                body: Spanned::new(body, body_span),
            }
        });

    // Generated calendar: calendar name { generate ...; ... }
    // No source string after the name
    let generated_calendar = just(Token::Calendar)
        .ignore_then(
            ident_or_keyword.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            generated_calendar_body
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
                .map_with(|body, e| (body, to_span(e.span())))
        )
        .map(|(name, (gen_cal, body_span))| {
            let body = CalendarBody::Generated(gen_cal);
            Calendar {
                name,
                body: Spanned::new(body, body_span),
            }
        });

    // Calendar: try physical first (has string lit), then generated
    let calendar = physical_calendar
        .or(generated_calendar);

    // ==========================================================================
    // Dimension definition
    // ==========================================================================

    // Attribute: name type;
    let attribute = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then(
            data_type.clone()
                .map_with(|t, e| Spanned::new(t, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(|(name, data_type)| Attribute { name, data_type });

    // Attributes block: attributes { name type; ... }
    let attributes_block = just(Token::Attributes)
        .ignore_then(
            attribute
                .map_with(|a, e| Spanned::new(a, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // Dimension body parts
    #[derive(Clone)]
    enum DimPart {
        Source(Spanned<String>),
        Key(Spanned<String>),
        Attributes(Vec<Spanned<Attribute>>),
        DrillPath(Spanned<DrillPath>),
    }

    let dim_source = just(Token::Source)
        .ignore_then(
            string_lit.clone()
                .map_with(|s, e| Spanned::new(s, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DimPart::Source);

    let dim_key = just(Token::Key)
        .ignore_then(
            ident.clone()
                .map_with(|k, e| Spanned::new(k, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(DimPart::Key);

    let dim_attributes = attributes_block.clone().map(DimPart::Attributes);

    let dim_drill_path = drill_path.clone()
        .map_with(|dp, e| Spanned::new(dp, to_span(e.span())))
        .map(DimPart::DrillPath);

    let dim_part = choice((
        dim_source,
        dim_key,
        dim_attributes,
        dim_drill_path,
    ));

    let dimension_body = dim_part
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|parts, span| {
            let mut source: Option<Spanned<String>> = None;
            let mut key: Option<Spanned<String>> = None;
            let mut attributes: Vec<Spanned<Attribute>> = Vec::new();
            let mut drill_paths: Vec<Spanned<DrillPath>> = Vec::new();

            for part in parts {
                match part {
                    DimPart::Source(s) => {
                        if source.is_some() {
                            return Err(Rich::custom(span, "duplicate source statement"));
                        }
                        source = Some(s);
                    }
                    DimPart::Key(k) => {
                        if key.is_some() {
                            return Err(Rich::custom(span, "duplicate key statement"));
                        }
                        key = Some(k);
                    }
                    DimPart::Attributes(attrs) => {
                        attributes.extend(attrs);
                    }
                    DimPart::DrillPath(dp) => {
                        drill_paths.push(dp);
                    }
                }
            }

            let source = source.ok_or_else(|| {
                Rich::custom(span, "dimension requires source statement")
            })?;
            let key = key.ok_or_else(|| {
                Rich::custom(span, "dimension requires key statement")
            })?;

            Ok((source, key, attributes, drill_paths))
        });

    // dimension name { source "..."; key ...; attributes { ... } drill_path ... { ... }; }
    let dimension = just(Token::Dimension)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            dimension_body
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(name, (source, key, attributes, drill_paths))| {
            Dimension {
                name,
                source,
                key,
                attributes,
                drill_paths,
            }
        });

    // ==========================================================================
    // Table definition
    // ==========================================================================

    // table name { source "..."; atoms { ... } times { ... } slicers { ... } }
    let table = just(Token::Table)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            // Inside the table braces
            just(Token::Source)
                .ignore_then(
                    string_lit
                        .map_with(|s, e| Spanned::new(s, to_span(e.span())))
                )
                .then_ignore(just(Token::Semicolon))
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

    // ==========================================================================
    // Measures block
    // ==========================================================================

    // Measure: name = { expr } [where { cond }] [null mode];
    let measure = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then_ignore(just(Token::Eq))
        .then(
            sql_expr.clone()
                .map_with(|expr, e| Spanned::new(expr, to_span(e.span())))
        )
        .then(
            just(Token::Where)
                .ignore_then(
                    sql_expr.clone()
                        .map_with(|expr, e| Spanned::new(expr, to_span(e.span())))
                )
                .or_not()
        )
        .then(
            just(Token::Null)
                .ignore_then(
                    null_handling.clone()
                        .map_with(|nh, e| Spanned::new(nh, to_span(e.span())))
                )
                .or_not()
        )
        .then_ignore(just(Token::Semicolon))
        .map(|(((name, expr), filter), null_handling)| Measure {
            name,
            expr,
            filter,
            null_handling,
        });

    // measures table_name { measure; ... }
    let measures_block = just(Token::Measures)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            measure
                .map_with(|m, e| Spanned::new(m, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(table, measures)| MeasureBlock { table, measures });

    // ==========================================================================
    // Report definition
    // ==========================================================================

    // Parse a relative period keyword
    let relative_period = select! {
        Token::Today => RelativePeriod::Today,
        Token::Yesterday => RelativePeriod::Yesterday,
        Token::ThisWeek => RelativePeriod::ThisWeek,
        Token::ThisMonth => RelativePeriod::ThisMonth,
        Token::ThisQuarter => RelativePeriod::ThisQuarter,
        Token::ThisYear => RelativePeriod::ThisYear,
        Token::LastWeek => RelativePeriod::LastWeek,
        Token::LastMonth => RelativePeriod::LastMonth,
        Token::LastQuarter => RelativePeriod::LastQuarter,
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

    // Parse trailing period pattern: last_N_<unit>
    // Pattern is tokenized as Token::Ident, so we need to parse and validate it
    let trailing_period = ident.clone()
        .try_map(|s, span| {
            // Expected format: last_<N>_<unit> (e.g., last_12_months)
            if !s.starts_with("last_") {
                return Err(Rich::custom(span, format!("invalid trailing period: expected 'last_N_unit', got '{}'", s)));
            }

            let parts: Vec<&str> = s.split('_').collect();
            if parts.len() != 3 {
                return Err(Rich::custom(span, format!("invalid trailing period format: expected 'last_N_unit', got '{}'", s)));
            }

            let count = parts[1].parse::<u32>()
                .map_err(|_| Rich::custom(span, format!("invalid count in trailing period: '{}'", parts[1])))?;

            let unit = PeriodUnit::from_str(parts[2])
                .ok_or_else(|| Rich::custom(span, format!("invalid unit in trailing period: '{}' (expected days, weeks, months, quarters, or years)", parts[2])))?;

            Ok(RelativePeriod::Trailing { count, unit })
        })
        .labelled("trailing period (last_N_unit)");

    // Period expression parser: relative period or trailing period
    let period_expr = relative_period
        .or(trailing_period)
        .map(PeriodExpr::Relative)
        .labelled("period expression");

    // Parse time suffix
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
    }.labelled("time suffix");

    // Parse drill path reference: source.path.level
    // Use ident_or_keyword for level to allow grain levels like "month", "quarter", etc.
    let drill_path_ref = ident.clone()
        .then_ignore(just(Token::Dot))
        .then(ident.clone())
        .then_ignore(just(Token::Dot))
        .then(ident_or_keyword.clone())
        .map(|((source, path), level)| DrillPathRef {
            source,
            path,
            level,
            label: None,
        });

    // Parse group item: drill_path_ref [as "Label"];
    let group_item = drill_path_ref
        .then(
            just(Token::As)
                .ignore_then(string_lit.clone())
                .or_not()
        )
        .map(|(mut drill_ref, label)| {
            drill_ref.label = label;
            GroupItem::DrillPathRef(drill_ref)
        })
        .then_ignore(just(Token::Semicolon))
        .labelled("group item");

    // Parse group block: group { ... }
    let group_block = just(Token::Group)
        .ignore_then(
            group_item
                .map_with(|g, e| Spanned::new(g, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // Parse inline measure: name = { expr } [as "Label"];
    let inline_measure = ident.clone()
        .then_ignore(just(Token::Eq))
        .then(sql_expr.clone())
        .then(
            just(Token::As)
                .ignore_then(string_lit.clone())
                .or_not()
        )
        .map(|((name, expr), label)| ShowItem::InlineMeasure { name, expr, label });

    // Parse measure with suffix: name.suffix [as "Label"];
    let measure_with_suffix = ident.clone()
        .then_ignore(just(Token::Dot))
        .then(time_suffix)
        .then(
            just(Token::As)
                .ignore_then(string_lit.clone())
                .or_not()
        )
        .map(|((name, suffix), label)| ShowItem::MeasureWithSuffix { name, suffix, label });

    // Parse basic measure: name [as "Label"];
    let basic_measure = ident.clone()
        .then(
            just(Token::As)
                .ignore_then(string_lit.clone())
                .or_not()
        )
        .map(|(name, label)| ShowItem::Measure { name, label });

    // Parse show item (order matters: try inline first, then suffix, then basic)
    let show_item = choice((
        inline_measure,
        measure_with_suffix,
        basic_measure,
    ))
    .then_ignore(just(Token::Semicolon))
    .labelled("show item");

    // Parse show block: show { ... }
    let show_block = just(Token::Show)
        .ignore_then(
            show_item
                .map_with(|s, e| Spanned::new(s, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        );

    // Parse sort direction
    let sort_direction = select! {
        Token::Asc => SortDirection::Asc,
        Token::Desc => SortDirection::Desc,
    };

    // Parse sort item: column.direction
    let sort_item = ident.clone()
        .then_ignore(just(Token::Dot))
        .then(sort_direction)
        .map(|(column, direction)| SortItem { column, direction })
        .labelled("sort item");

    // Parse sort clause: sort item, item, ...;
    let sort_clause = just(Token::Sort)
        .ignore_then(
            sort_item
                .map_with(|s, e| Spanned::new(s, to_span(e.span())))
                .separated_by(just(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>()
        )
        .then_ignore(just(Token::Semicolon));

    // Report body parts
    #[derive(Clone)]
    enum ReportPart {
        From(Vec<Spanned<String>>),
        UseDate(Vec<Spanned<String>>),
        Period(Spanned<PeriodExpr>),
        Group(Vec<Spanned<GroupItem>>),
        Show(Vec<Spanned<ShowItem>>),
        Filter(Spanned<SqlExpr>),
        Sort(Vec<Spanned<SortItem>>),
        Limit(Spanned<u64>),
    }

    // Parse from clause: from table1, table2, ...;
    let report_from = just(Token::From)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
                .separated_by(just(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>()
        )
        .then_ignore(just(Token::Semicolon))
        .map(ReportPart::From);

    // Parse use_date clause: use_date col1, col2, ...;
    let report_use_date = just(Token::UseDate)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
                .separated_by(just(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>()
        )
        .then_ignore(just(Token::Semicolon))
        .map(ReportPart::UseDate);

    // Parse period clause: period expr;
    let report_period = just(Token::Period)
        .ignore_then(
            period_expr
                .map_with(|p, e| Spanned::new(p, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(ReportPart::Period);

    // Parse filter clause: filter { condition };
    let report_filter = just(Token::Filter)
        .ignore_then(
            sql_expr.clone()
                .map_with(|expr, e| Spanned::new(expr, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(ReportPart::Filter);

    // Parse limit clause: limit N;
    let report_limit = just(Token::Limit)
        .ignore_then(
            select! { Token::Number(s) => s }
                .try_map(|s, span| {
                    s.parse::<u64>().map_err(|_| Rich::custom(span, "invalid limit value"))
                })
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then_ignore(just(Token::Semicolon))
        .map(ReportPart::Limit);

    let report_part = choice((
        report_from,
        report_use_date,
        report_period,
        group_block.map(ReportPart::Group),
        show_block.map(ReportPart::Show),
        report_filter,
        sort_clause.map(ReportPart::Sort),
        report_limit,
    ));

    // Parse report body
    let report_body = report_part
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|parts, span| {
            let mut from: Option<Vec<Spanned<String>>> = None;
            let mut use_date: Option<Vec<Spanned<String>>> = None;
            let mut period: Option<Spanned<PeriodExpr>> = None;
            let mut group: Vec<Spanned<GroupItem>> = Vec::new();
            let mut show: Vec<Spanned<ShowItem>> = Vec::new();
            let mut filter: Option<Spanned<SqlExpr>> = None;
            let mut sort: Vec<Spanned<SortItem>> = Vec::new();
            let mut limit: Option<Spanned<u64>> = None;

            for part in parts {
                match part {
                    ReportPart::From(f) => {
                        if from.is_some() {
                            return Err(Rich::custom(span, "duplicate from clause"));
                        }
                        from = Some(f);
                    }
                    ReportPart::UseDate(u) => {
                        if use_date.is_some() {
                            return Err(Rich::custom(span, "duplicate use_date clause"));
                        }
                        use_date = Some(u);
                    }
                    ReportPart::Period(p) => {
                        if period.is_some() {
                            return Err(Rich::custom(span, "duplicate period clause"));
                        }
                        period = Some(p);
                    }
                    ReportPart::Group(g) => {
                        group.extend(g);
                    }
                    ReportPart::Show(s) => {
                        show.extend(s);
                    }
                    ReportPart::Filter(f) => {
                        if filter.is_some() {
                            return Err(Rich::custom(span, "duplicate filter clause"));
                        }
                        filter = Some(f);
                    }
                    ReportPart::Sort(s) => {
                        sort.extend(s);
                    }
                    ReportPart::Limit(l) => {
                        if limit.is_some() {
                            return Err(Rich::custom(span, "duplicate limit clause"));
                        }
                        limit = Some(l);
                    }
                }
            }

            let from = from.ok_or_else(|| {
                Rich::custom(span, "report requires from clause")
            })?;
            let use_date = use_date.ok_or_else(|| {
                Rich::custom(span, "report requires use_date clause")
            })?;

            Ok((from, use_date, period, group, show, filter, sort, limit))
        });

    // report name { ... }
    let report = just(Token::Report)
        .ignore_then(
            ident.clone()
                .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        )
        .then(
            report_body
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(name, (from, use_date, period, group, show, filter, sort, limit))| {
            Report {
                name,
                from,
                use_date,
                period,
                group,
                show,
                filter,
                sort,
                limit,
            }
        });

    // ==========================================================================
    // Top-level items
    // ==========================================================================

    // Parse any top-level item: calendar, dimension, table, measures block, or report
    let item = choice((
        calendar.map(Item::Calendar),
        dimension.map(Item::Dimension),
        table.map(Item::Table),
        measures_block.map(Item::MeasureBlock),
        report.map(Item::Report),
    ));

    // The model is an optional defaults block followed by items
    // Parse optional defaults first (must come before all items)
    let model = defaults.or_not()
        .then(
            item
                .map_with(|i, e| Spanned::new(i, to_span(e.span())))
                .repeated()
                .collect::<Vec<_>>()
        )
        .map(|(defaults, items)| Model { defaults, items });

    model
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::lexer::lex;
    use chumsky::input::Input;

    /// Helper to parse a source string and return the Model or panic with errors.
    fn parse_str(input: &str) -> Model {
        let tokens = lex(input).expect("lexing should succeed");
        let len = input.len();
        // Create a mapped input from the token slice
        // The eoi_span is the span at the end of input
        let token_stream = tokens.as_slice().map(
            (len..len).into(),
            |(tok, span): &(Token<'_>, SimpleSpan)| (tok, span),
        );
        let result = parser()
            .parse(token_stream)
            .into_result()
            .expect("parsing should succeed");
        result
    }

    /// Helper that returns parse result for testing error cases.
    #[allow(dead_code)]
    fn try_parse_str(input: &str) -> Result<Model, String> {
        let tokens = lex(input).expect("lexing should succeed");
        let len = input.len();
        let token_stream = tokens.as_slice().map(
            (len..len).into(),
            |(tok, span): &(Token<'_>, SimpleSpan)| (tok, span),
        );
        let result = parser()
            .parse(token_stream)
            .into_result()
            .map_err(|errs| format!("{:?}", errs));
        result
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
        let model = parse_str(input);

        // Should have 1 item
        assert_eq!(model.items.len(), 1);

        // Item should be a Table
        let item = &model.items[0].value;
        match item {
            Item::Table(table) => {
                assert_eq!(table.name.value, "sales");
                assert_eq!(table.source.value, "sales.csv");

                // Check atoms
                assert_eq!(table.atoms.len(), 2);
                assert_eq!(table.atoms[0].value.name.value, "revenue");
                assert_eq!(table.atoms[0].value.atom_type.value, AtomType::Decimal);
                assert_eq!(table.atoms[1].value.name.value, "quantity");
                assert_eq!(table.atoms[1].value.atom_type.value, AtomType::Int);

                // No times or slicers
                assert!(table.times.is_empty());
                assert!(table.slicers.is_empty());
            }
            _ => panic!("Expected Table item"),
        }
    }

    #[test]
    fn test_parse_table_with_times() {
        let input = r#"
            table orders {
                source "dbo.orders";
                times {
                    order_date -> dates.day;
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.name.value, "orders");
                assert_eq!(table.source.value, "dbo.orders");

                // Check times
                assert_eq!(table.times.len(), 1);
                let time = &table.times[0].value;
                assert_eq!(time.name.value, "order_date");
                assert_eq!(time.calendar.value, "dates");
                assert_eq!(time.grain.value, GrainLevel::Day);
            }
            _ => panic!("Expected Table item"),
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
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.name.value, "orders");

                // Check slicers
                assert_eq!(table.slicers.len(), 3);

                // Inline slicer: region string;
                match &table.slicers[0].value.kind.value {
                    SlicerKind::Inline { data_type } => {
                        assert_eq!(table.slicers[0].value.name.value, "region");
                        assert_eq!(*data_type, DataType::String);
                    }
                    _ => panic!("Expected Inline slicer for region"),
                }

                // FK slicer: customer_id -> customers.customer_id;
                match &table.slicers[1].value.kind.value {
                    SlicerKind::ForeignKey {
                        dimension,
                        key_column,
                    } => {
                        assert_eq!(table.slicers[1].value.name.value, "customer_id");
                        assert_eq!(dimension, "customers");
                        assert_eq!(key_column, "customer_id");
                    }
                    _ => panic!("Expected ForeignKey slicer for customer_id"),
                }

                // Via slicer: segment via customer_id;
                match &table.slicers[2].value.kind.value {
                    SlicerKind::Via { fk_slicer } => {
                        assert_eq!(table.slicers[2].value.name.value, "segment");
                        assert_eq!(fk_slicer, "customer_id");
                    }
                    _ => panic!("Expected Via slicer for segment"),
                }
            }
            _ => panic!("Expected Table item"),
        }
    }

    #[test]
    fn test_parse_table_with_all_sections() {
        let input = r#"
            table sales {
                source "fact_sales";
                atoms {
                    amount decimal;
                }
                times {
                    sale_date -> dates.day;
                }
                slicers {
                    category string;
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.name.value, "sales");
                assert_eq!(table.source.value, "fact_sales");
                assert_eq!(table.atoms.len(), 1);
                assert_eq!(table.times.len(), 1);
                assert_eq!(table.slicers.len(), 1);
            }
            _ => panic!("Expected Table item"),
        }
    }

    #[test]
    fn test_parse_multiple_tables() {
        let input = r#"
            table orders {
                source "orders";
                atoms {
                    total decimal;
                }
            }
            table customers {
                source "customers";
                atoms {
                    lifetime_value decimal;
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 2);

        match &model.items[0].value {
            Item::Table(t) => assert_eq!(t.name.value, "orders"),
            _ => panic!("Expected Table"),
        }
        match &model.items[1].value {
            Item::Table(t) => assert_eq!(t.name.value, "customers"),
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_parse_table_minimal() {
        // A table with just source (no atoms, times, or slicers)
        let input = r#"
            table empty {
                source "empty.csv";
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.name.value, "empty");
                assert_eq!(table.source.value, "empty.csv");
                assert!(table.atoms.is_empty());
                assert!(table.times.is_empty());
                assert!(table.slicers.is_empty());
            }
            _ => panic!("Expected Table item"),
        }
    }

    #[test]
    fn test_parse_empty_model() {
        let input = "";
        let model = parse_str(input);
        assert!(model.items.is_empty());
        assert!(model.defaults.is_none());
    }

    #[test]
    fn test_parse_atom_types() {
        let input = r#"
            table types_test {
                source "test";
                atoms {
                    a int;
                    b decimal;
                    c float;
                }
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.atoms[0].value.atom_type.value, AtomType::Int);
                assert_eq!(table.atoms[1].value.atom_type.value, AtomType::Decimal);
                assert_eq!(table.atoms[2].value.atom_type.value, AtomType::Float);
            }
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_parse_slicer_data_types() {
        let input = r#"
            table types_test {
                source "test";
                slicers {
                    a string;
                    b int;
                    c bool;
                    d date;
                    e timestamp;
                }
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Table(table) => {
                let check_inline = |idx: usize, expected: DataType| {
                    match &table.slicers[idx].value.kind.value {
                        SlicerKind::Inline { data_type } => {
                            assert_eq!(*data_type, expected)
                        }
                        _ => panic!("Expected Inline slicer"),
                    }
                };
                check_inline(0, DataType::String);
                check_inline(1, DataType::Int);
                check_inline(2, DataType::Bool);
                check_inline(3, DataType::Date);
                check_inline(4, DataType::Timestamp);
            }
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_parse_time_grain_levels() {
        let input = r#"
            table grains_test {
                source "test";
                times {
                    a -> cal.day;
                    b -> cal.week;
                    c -> cal.month;
                    d -> cal.quarter;
                    e -> cal.year;
                }
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.times[0].value.grain.value, GrainLevel::Day);
                assert_eq!(table.times[1].value.grain.value, GrainLevel::Week);
                assert_eq!(table.times[2].value.grain.value, GrainLevel::Month);
                assert_eq!(table.times[3].value.grain.value, GrainLevel::Quarter);
                assert_eq!(table.times[4].value.grain.value, GrainLevel::Year);
            }
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_parse_calculated_slicer() {
        let input = r#"
            table orders {
                source "dbo.orders";
                slicers {
                    region_code string = { UPPER(SUBSTRING(region, 1, 2)) };
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Table(table) => {
                assert_eq!(table.slicers.len(), 1);
                match &table.slicers[0].value.kind.value {
                    SlicerKind::Calculated { data_type, expr } => {
                        assert_eq!(table.slicers[0].value.name.value, "region_code");
                        assert_eq!(*data_type, DataType::String);
                        assert!(expr.sql.contains("UPPER"));
                    }
                    _ => panic!("Expected Calculated slicer"),
                }
            }
            _ => panic!("Expected Table item"),
        }
    }

    // ==========================================================================
    // Calendar parsing tests
    // ==========================================================================

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
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Calendar(cal) => {
                assert_eq!(cal.name.value, "auto");

                match &cal.body.value {
                    CalendarBody::Generated(gen) => {
                        // Check base grain
                        assert_eq!(gen.base_grain.value, GrainLevel::Day);

                        // Check range
                        assert!(gen.range.is_some());
                        match &gen.range.as_ref().unwrap().value {
                            CalendarRange::Infer { min, max } => {
                                assert!(min.is_some());
                                let min_date = &min.as_ref().unwrap().value;
                                assert_eq!(min_date.year, 2020);
                                assert_eq!(min_date.month, 1);
                                assert_eq!(min_date.day, 1);

                                assert!(max.is_some());
                                let max_date = &max.as_ref().unwrap().value;
                                assert_eq!(max_date.year, 2030);
                                assert_eq!(max_date.month, 12);
                                assert_eq!(max_date.day, 31);
                            }
                            _ => panic!("Expected Infer range"),
                        }

                        // Check drill_path
                        assert_eq!(gen.drill_paths.len(), 1);
                        let dp = &gen.drill_paths[0].value;
                        assert_eq!(dp.name.value, "standard");
                        assert_eq!(dp.levels.len(), 5);
                        assert_eq!(dp.levels[0].value, "day");
                        assert_eq!(dp.levels[1].value, "week");
                        assert_eq!(dp.levels[2].value, "month");
                        assert_eq!(dp.levels[3].value, "quarter");
                        assert_eq!(dp.levels[4].value, "year");

                        // Check week_start
                        assert!(gen.week_start.is_some());
                        assert_eq!(gen.week_start.as_ref().unwrap().value, Weekday::Monday);
                    }
                    _ => panic!("Expected Generated calendar"),
                }
            }
            _ => panic!("Expected Calendar item"),
        }
    }

    #[test]
    fn test_parse_generated_calendar_minimal() {
        let input = r#"
            calendar dates {
                generate day+;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Calendar(cal) => {
                assert_eq!(cal.name.value, "dates");

                match &cal.body.value {
                    CalendarBody::Generated(gen) => {
                        assert_eq!(gen.base_grain.value, GrainLevel::Day);
                        assert!(gen.range.is_none());
                        assert!(gen.drill_paths.is_empty());
                        assert!(gen.week_start.is_none());
                    }
                    _ => panic!("Expected Generated calendar"),
                }
            }
            _ => panic!("Expected Calendar item"),
        }
    }

    #[test]
    fn test_parse_generated_calendar_with_fiscal() {
        let input = r#"
            calendar fiscal_auto {
                generate day+;
                include fiscal[January];
                range infer;
            }
        "#;

        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Calendar(cal) => {
                assert_eq!(cal.name.value, "fiscal_auto");

                match &cal.body.value {
                    CalendarBody::Generated(gen) => {
                        assert!(gen.fiscal.is_some());
                        assert_eq!(gen.fiscal.as_ref().unwrap().value, Month::January);
                    }
                    _ => panic!("Expected generated calendar"),
                }
            }
            _ => panic!("Expected Calendar item"),
        }
    }

    #[test]
    fn test_parse_physical_calendar() {
        let input = r#"
            calendar fiscal "dbo.dim_date" {
                day = date_key;
                week = week_start_date;
                month = month_start_date;
                drill_path standard { day -> week -> month };
                fiscal_year_start July;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Calendar(cal) => {
                assert_eq!(cal.name.value, "fiscal");

                match &cal.body.value {
                    CalendarBody::Physical(phys) => {
                        // Check source
                        assert_eq!(phys.source.value, "dbo.dim_date");

                        // Check grain mappings
                        assert_eq!(phys.grain_mappings.len(), 3);
                        assert_eq!(phys.grain_mappings[0].value.level.value, GrainLevel::Day);
                        assert_eq!(phys.grain_mappings[0].value.column.value, "date_key");
                        assert_eq!(phys.grain_mappings[1].value.level.value, GrainLevel::Week);
                        assert_eq!(phys.grain_mappings[1].value.column.value, "week_start_date");
                        assert_eq!(phys.grain_mappings[2].value.level.value, GrainLevel::Month);
                        assert_eq!(phys.grain_mappings[2].value.column.value, "month_start_date");

                        // Check drill_path
                        assert_eq!(phys.drill_paths.len(), 1);
                        let dp = &phys.drill_paths[0].value;
                        assert_eq!(dp.name.value, "standard");
                        assert_eq!(dp.levels.len(), 3);

                        // Check fiscal_year_start
                        assert!(phys.fiscal_year_start.is_some());
                        assert_eq!(phys.fiscal_year_start.as_ref().unwrap().value, Month::July);

                        // No week_start
                        assert!(phys.week_start.is_none());
                    }
                    _ => panic!("Expected Physical calendar"),
                }
            }
            _ => panic!("Expected Calendar item"),
        }
    }

    #[test]
    fn test_parse_physical_calendar_minimal() {
        let input = r#"
            calendar dates "dim_date" {
                day = date_key;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Calendar(cal) => {
                assert_eq!(cal.name.value, "dates");

                match &cal.body.value {
                    CalendarBody::Physical(phys) => {
                        assert_eq!(phys.source.value, "dim_date");
                        assert_eq!(phys.grain_mappings.len(), 1);
                        assert_eq!(phys.grain_mappings[0].value.level.value, GrainLevel::Day);
                        assert_eq!(phys.grain_mappings[0].value.column.value, "date_key");
                        assert!(phys.drill_paths.is_empty());
                        assert!(phys.fiscal_year_start.is_none());
                        assert!(phys.week_start.is_none());
                    }
                    _ => panic!("Expected Physical calendar"),
                }
            }
            _ => panic!("Expected Calendar item"),
        }
    }

    // ==========================================================================
    // Dimension parsing tests
    // ==========================================================================

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
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Dimension(dim) => {
                assert_eq!(dim.name.value, "customers");
                assert_eq!(dim.source.value, "dbo.dim_customers");
                assert_eq!(dim.key.value, "customer_id");

                // Check attributes
                assert_eq!(dim.attributes.len(), 3);
                assert_eq!(dim.attributes[0].value.name.value, "customer_name");
                assert_eq!(dim.attributes[0].value.data_type.value, DataType::String);
                assert_eq!(dim.attributes[1].value.name.value, "segment");
                assert_eq!(dim.attributes[1].value.data_type.value, DataType::String);
                assert_eq!(dim.attributes[2].value.name.value, "region");
                assert_eq!(dim.attributes[2].value.data_type.value, DataType::String);

                // Check drill_path
                assert_eq!(dim.drill_paths.len(), 1);
                let dp = &dim.drill_paths[0].value;
                assert_eq!(dp.name.value, "geo");
                assert_eq!(dp.levels.len(), 1);
                assert_eq!(dp.levels[0].value, "region");
            }
            _ => panic!("Expected Dimension item"),
        }
    }

    #[test]
    fn test_parse_dimension_minimal() {
        let input = r#"
            dimension products {
                source "dim_products";
                key product_id;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Dimension(dim) => {
                assert_eq!(dim.name.value, "products");
                assert_eq!(dim.source.value, "dim_products");
                assert_eq!(dim.key.value, "product_id");
                assert!(dim.attributes.is_empty());
                assert!(dim.drill_paths.is_empty());
            }
            _ => panic!("Expected Dimension item"),
        }
    }

    #[test]
    fn test_parse_dimension_with_multiple_types() {
        let input = r#"
            dimension orders {
                source "dim_orders";
                key order_id;
                attributes {
                    order_date date;
                    is_priority bool;
                    order_count int;
                    total_amount decimal;
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Dimension(dim) => {
                assert_eq!(dim.attributes.len(), 4);
                assert_eq!(dim.attributes[0].value.data_type.value, DataType::Date);
                assert_eq!(dim.attributes[1].value.data_type.value, DataType::Bool);
                assert_eq!(dim.attributes[2].value.data_type.value, DataType::Int);
                assert_eq!(dim.attributes[3].value.data_type.value, DataType::Decimal);
            }
            _ => panic!("Expected Dimension item"),
        }
    }

    // ==========================================================================
    // Mixed item tests
    // ==========================================================================

    #[test]
    fn test_parse_mixed_items() {
        let input = r#"
            calendar dates {
                generate day+;
            }

            dimension customers {
                source "dim_customers";
                key customer_id;
            }

            table sales {
                source "fact_sales";
                atoms {
                    amount decimal;
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 3);

        match &model.items[0].value {
            Item::Calendar(cal) => assert_eq!(cal.name.value, "dates"),
            _ => panic!("Expected Calendar"),
        }
        match &model.items[1].value {
            Item::Dimension(dim) => assert_eq!(dim.name.value, "customers"),
            _ => panic!("Expected Dimension"),
        }
        match &model.items[2].value {
            Item::Table(t) => assert_eq!(t.name.value, "sales"),
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_parse_calendar_with_explicit_range() {
        let input = r#"
            calendar dates {
                generate month+;
                range 2024-01-01 to 2024-12-31;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Calendar(cal) => {
                match &cal.body.value {
                    CalendarBody::Generated(gen) => {
                        assert_eq!(gen.base_grain.value, GrainLevel::Month);
                        match &gen.range.as_ref().unwrap().value {
                            CalendarRange::Explicit { start, end } => {
                                assert_eq!(start.value.year, 2024);
                                assert_eq!(start.value.month, 1);
                                assert_eq!(start.value.day, 1);
                                assert_eq!(end.value.year, 2024);
                                assert_eq!(end.value.month, 12);
                                assert_eq!(end.value.day, 31);
                            }
                            _ => panic!("Expected Explicit range"),
                        }
                    }
                    _ => panic!("Expected Generated calendar"),
                }
            }
            _ => panic!("Expected Calendar item"),
        }
    }

    #[test]
    fn test_parse_calendar_weekdays() {
        // Test all weekday values
        for (weekday_str, expected) in [
            ("Monday", Weekday::Monday),
            ("Tuesday", Weekday::Tuesday),
            ("Wednesday", Weekday::Wednesday),
            ("Thursday", Weekday::Thursday),
            ("Friday", Weekday::Friday),
            ("Saturday", Weekday::Saturday),
            ("Sunday", Weekday::Sunday),
        ] {
            let input = format!(
                r#"
                calendar dates {{
                    generate day+;
                    week_start {};
                }}
            "#,
                weekday_str
            );
            let model = parse_str(&input);

            match &model.items[0].value {
                Item::Calendar(cal) => match &cal.body.value {
                    CalendarBody::Generated(gen) => {
                        assert_eq!(gen.week_start.as_ref().unwrap().value, expected);
                    }
                    _ => panic!("Expected Generated calendar"),
                },
                _ => panic!("Expected Calendar item"),
            }
        }
    }

    #[test]
    fn test_parse_physical_calendar_months() {
        // Test a few month values
        for (month_str, expected) in [
            ("January", Month::January),
            ("July", Month::July),
            ("December", Month::December),
        ] {
            let input = format!(
                r#"
                calendar fiscal "dim_date" {{
                    day = date_key;
                    fiscal_year_start {};
                }}
            "#,
                month_str
            );
            let model = parse_str(&input);

            match &model.items[0].value {
                Item::Calendar(cal) => match &cal.body.value {
                    CalendarBody::Physical(phys) => {
                        assert_eq!(phys.fiscal_year_start.as_ref().unwrap().value, expected);
                    }
                    _ => panic!("Expected Physical calendar"),
                },
                _ => panic!("Expected Calendar item"),
            }
        }
    }

    // ==========================================================================
    // Defaults parsing tests
    // ==========================================================================

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
        let model = parse_str(input);

        // Should have defaults
        assert!(model.defaults.is_some());
        let defaults = &model.defaults.unwrap().value;
        assert_eq!(defaults.settings.len(), 5);

        // Check calendar setting
        match &defaults.settings[0].value {
            DefaultSetting::Calendar(name) => {
                assert_eq!(name.value, "dates");
            }
            _ => panic!("Expected Calendar setting"),
        }

        // Check fiscal_year_start
        match &defaults.settings[1].value {
            DefaultSetting::FiscalYearStart(month) => {
                assert_eq!(month.value, Month::July);
            }
            _ => panic!("Expected FiscalYearStart setting"),
        }

        // Check week_start
        match &defaults.settings[2].value {
            DefaultSetting::WeekStart(weekday) => {
                assert_eq!(weekday.value, Weekday::Monday);
            }
            _ => panic!("Expected WeekStart setting"),
        }

        // Check null_handling
        match &defaults.settings[3].value {
            DefaultSetting::NullHandling(nh) => {
                assert_eq!(nh.value, NullHandling::CoalesceZero);
            }
            _ => panic!("Expected NullHandling setting"),
        }

        // Check decimal_places
        match &defaults.settings[4].value {
            DefaultSetting::DecimalPlaces(dp) => {
                assert_eq!(dp.value, 2);
            }
            _ => panic!("Expected DecimalPlaces setting"),
        }
    }

    #[test]
    fn test_parse_defaults_minimal() {
        let input = r#"
            defaults {
                calendar dates;
            }
        "#;
        let model = parse_str(input);

        assert!(model.defaults.is_some());
        let defaults = &model.defaults.unwrap().value;
        assert_eq!(defaults.settings.len(), 1);
    }

    #[test]
    fn test_parse_defaults_and_items() {
        let input = r#"
            defaults {
                calendar dates;
            }

            table sales {
                source "sales.csv";
            }
        "#;
        let model = parse_str(input);

        // Should have both defaults and items
        assert!(model.defaults.is_some());
        assert_eq!(model.items.len(), 1);

        match &model.items[0].value {
            Item::Table(t) => assert_eq!(t.name.value, "sales"),
            _ => panic!("Expected Table"),
        }
    }

    // ==========================================================================
    // Measures parsing tests
    // ==========================================================================

    #[test]
    fn test_parse_measures() {
        let input = r#"
            measures fact_sales {
                revenue = { sum(@revenue) };
                margin = { revenue - cost };
                enterprise_rev = { sum(@revenue) } where { segment = "Enterprise" };
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::MeasureBlock(mb) => {
                assert_eq!(mb.table.value, "fact_sales");
                assert_eq!(mb.measures.len(), 3);

                // Check first measure
                let m1 = &mb.measures[0].value;
                assert_eq!(m1.name.value, "revenue");
                assert!(m1.expr.value.sql.contains("sum"));
                assert!(m1.filter.is_none());
                assert!(m1.null_handling.is_none());

                // Check second measure
                let m2 = &mb.measures[1].value;
                assert_eq!(m2.name.value, "margin");
                assert!(m2.expr.value.sql.contains("revenue"));
                assert!(m2.expr.value.sql.contains("cost"));
                assert!(m2.filter.is_none());
                assert!(m2.null_handling.is_none());

                // Check third measure with WHERE clause
                let m3 = &mb.measures[2].value;
                assert_eq!(m3.name.value, "enterprise_rev");
                assert!(m3.expr.value.sql.contains("sum"));
                assert!(m3.filter.is_some());
                let filter = &m3.filter.as_ref().unwrap().value;
                assert!(filter.sql.contains("segment"));
                assert!(m3.null_handling.is_none());
            }
            _ => panic!("Expected MeasureBlock"),
        }
    }

    #[test]
    fn test_parse_measure_with_null_handling() {
        let input = r#"
            measures sales {
                safe_ratio = { numerator / denominator } null coalesce_zero;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::MeasureBlock(mb) => {
                assert_eq!(mb.measures.len(), 1);
                let m = &mb.measures[0].value;
                assert_eq!(m.name.value, "safe_ratio");
                assert!(m.null_handling.is_some());
                assert_eq!(m.null_handling.as_ref().unwrap().value, NullHandling::CoalesceZero);
            }
            _ => panic!("Expected MeasureBlock"),
        }
    }

    #[test]
    fn test_parse_measure_with_where_and_null() {
        let input = r#"
            measures sales {
                safe_enterprise = { sum(@revenue) } where { segment = "Enterprise" } null null_on_zero;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::MeasureBlock(mb) => {
                assert_eq!(mb.measures.len(), 1);
                let m = &mb.measures[0].value;
                assert_eq!(m.name.value, "safe_enterprise");
                assert!(m.filter.is_some());
                assert!(m.null_handling.is_some());
                assert_eq!(m.null_handling.as_ref().unwrap().value, NullHandling::NullOnZero);
            }
            _ => panic!("Expected MeasureBlock"),
        }
    }

    #[test]
    fn test_parse_defaults_with_all_null_handling_modes() {
        // Test all null handling modes
        for (mode_str, expected) in [
            ("coalesce_zero", NullHandling::CoalesceZero),
            ("null_on_zero", NullHandling::NullOnZero),
            ("error_on_zero", NullHandling::ErrorOnZero),
        ] {
            let input = format!(
                r#"
                defaults {{
                    null_handling {};
                }}
            "#,
                mode_str
            );
            let model = parse_str(&input);

            assert!(model.defaults.is_some());
            let defaults = &model.defaults.unwrap().value;
            match &defaults.settings[0].value {
                DefaultSetting::NullHandling(nh) => {
                    assert_eq!(nh.value, expected);
                }
                _ => panic!("Expected NullHandling setting"),
            }
        }
    }

    #[test]
    fn test_sql_expr_whitespace_handling() {
        // Test that the SQL expression parser correctly reconstructs SQL from tokens
        // Note: whitespace handling is basic - joins tokens with single space
        let input = r#"
            table test {
                source "test";
                slicers {
                    calc1 string = { UPPER(SUBSTRING(region, 1, 2)) };
                }
            }
            measures test {
                total = { sum(@revenue) };
                ratio = { numerator / denominator };
            }
        "#;
        let model = parse_str(input);

        // Check calculated slicer SQL
        match &model.items[0].value {
            Item::Table(table) => {
                match &table.slicers[0].value.kind.value {
                    SlicerKind::Calculated { expr, .. } => {
                        // Verify SQL contains expected tokens
                        assert!(expr.sql.contains("UPPER"));
                        assert!(expr.sql.contains("SUBSTRING"));
                        assert!(expr.sql.contains("region"));
                    }
                    _ => panic!("Expected Calculated slicer"),
                }
            }
            _ => panic!("Expected Table item"),
        }

        // Check measure SQL
        match &model.items[1].value {
            Item::MeasureBlock(mb) => {
                // First measure: sum(@revenue)
                let m1 = &mb.measures[0].value;
                assert!(m1.expr.value.sql.contains("sum"));
                assert!(m1.expr.value.sql.contains("@"));
                assert!(m1.expr.value.sql.contains("revenue"));

                // Second measure: numerator / denominator
                let m2 = &mb.measures[1].value;
                assert!(m2.expr.value.sql.contains("numerator"));
                assert!(m2.expr.value.sql.contains("denominator"));
            }
            _ => panic!("Expected MeasureBlock"),
        }
    }

    // ==========================================================================
    // Report parsing tests
    // ==========================================================================

    #[test]
    fn test_parse_report_minimal() {
        let input = r#"
            report simple {
                from sales;
                use_date order_date;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.name.value, "simple");
                assert_eq!(report.from.len(), 1);
                assert_eq!(report.from[0].value, "sales");
                assert_eq!(report.use_date.len(), 1);
                assert_eq!(report.use_date[0].value, "order_date");
                assert!(report.period.is_none());
                assert!(report.group.is_empty());
                assert!(report.show.is_empty());
                assert!(report.filter.is_none());
                assert!(report.sort.is_empty());
                assert!(report.limit.is_none());
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_report_full() {
        let input = r#"
            report quarterly_review {
                from fact_sales;
                use_date order_date;
                period last_12_months;
                group {
                    dates.standard.month as "Month";
                }
                show {
                    revenue as "Revenue";
                    revenue.yoy_growth as "YoY Growth";
                }
                sort revenue.desc;
                limit 20;
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.name.value, "quarterly_review");

                // Check from
                assert_eq!(report.from.len(), 1);
                assert_eq!(report.from[0].value, "fact_sales");

                // Check use_date
                assert_eq!(report.use_date.len(), 1);
                assert_eq!(report.use_date[0].value, "order_date");

                // Check period
                assert!(report.period.is_some());
                match &report.period.as_ref().unwrap().value {
                    PeriodExpr::Relative(RelativePeriod::Trailing { count, unit }) => {
                        assert_eq!(*count, 12);
                        assert_eq!(*unit, PeriodUnit::Months);
                    }
                    _ => panic!("Expected Trailing period"),
                }

                // Check group
                assert_eq!(report.group.len(), 1);
                match &report.group[0].value {
                    GroupItem::DrillPathRef(drill_ref) => {
                        assert_eq!(drill_ref.source, "dates");
                        assert_eq!(drill_ref.path, "standard");
                        assert_eq!(drill_ref.level, "month");
                        assert_eq!(drill_ref.label.as_deref(), Some("Month"));
                    }
                    _ => panic!("Expected DrillPathRef"),
                }

                // Check show
                assert_eq!(report.show.len(), 2);
                match &report.show[0].value {
                    ShowItem::Measure { name, label } => {
                        assert_eq!(name, "revenue");
                        assert_eq!(label.as_deref(), Some("Revenue"));
                    }
                    _ => panic!("Expected Measure"),
                }
                match &report.show[1].value {
                    ShowItem::MeasureWithSuffix { name, suffix, label } => {
                        assert_eq!(name, "revenue");
                        assert_eq!(*suffix, TimeSuffix::YoyGrowth);
                        assert_eq!(label.as_deref(), Some("YoY Growth"));
                    }
                    _ => panic!("Expected MeasureWithSuffix"),
                }

                // Check sort
                assert_eq!(report.sort.len(), 1);
                assert_eq!(report.sort[0].value.column, "revenue");
                assert_eq!(report.sort[0].value.direction, SortDirection::Desc);

                // Check limit
                assert!(report.limit.is_some());
                assert_eq!(report.limit.as_ref().unwrap().value, 20);
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_period_expressions() {
        // Test relative periods
        let test_cases = vec![
            ("today", RelativePeriod::Today),
            ("yesterday", RelativePeriod::Yesterday),
            ("this_week", RelativePeriod::ThisWeek),
            ("this_month", RelativePeriod::ThisMonth),
            ("this_quarter", RelativePeriod::ThisQuarter),
            ("this_year", RelativePeriod::ThisYear),
            ("last_week", RelativePeriod::LastWeek),
            ("last_month", RelativePeriod::LastMonth),
            ("last_quarter", RelativePeriod::LastQuarter),
            ("last_year", RelativePeriod::LastYear),
            ("ytd", RelativePeriod::Ytd),
            ("qtd", RelativePeriod::Qtd),
            ("mtd", RelativePeriod::Mtd),
        ];

        for (period_str, expected_period) in test_cases {
            let input = format!(
                r#"
                report test {{
                    from sales;
                    use_date order_date;
                    period {};
                }}
            "#,
                period_str
            );
            let model = parse_str(&input);

            match &model.items[0].value {
                Item::Report(report) => {
                    assert!(report.period.is_some());
                    match &report.period.as_ref().unwrap().value {
                        PeriodExpr::Relative(period) => {
                            assert_eq!(*period, expected_period);
                        }
                        _ => panic!("Expected Relative period"),
                    }
                }
                _ => panic!("Expected Report item"),
            }
        }
    }

    #[test]
    fn test_parse_trailing_periods() {
        let test_cases = vec![
            ("last_7_days", 7, PeriodUnit::Days),
            ("last_4_weeks", 4, PeriodUnit::Weeks),
            ("last_12_months", 12, PeriodUnit::Months),
            ("last_4_quarters", 4, PeriodUnit::Quarters),
            ("last_3_years", 3, PeriodUnit::Years),
        ];

        for (period_str, expected_count, expected_unit) in test_cases {
            let input = format!(
                r#"
                report test {{
                    from sales;
                    use_date order_date;
                    period {};
                }}
            "#,
                period_str
            );
            let model = parse_str(&input);

            match &model.items[0].value {
                Item::Report(report) => {
                    assert!(report.period.is_some());
                    match &report.period.as_ref().unwrap().value {
                        PeriodExpr::Relative(RelativePeriod::Trailing { count, unit }) => {
                            assert_eq!(*count, expected_count);
                            assert_eq!(*unit, expected_unit);
                        }
                        _ => panic!("Expected Trailing period"),
                    }
                }
                _ => panic!("Expected Report item"),
            }
        }
    }

    #[test]
    fn test_parse_report_with_filter() {
        let input = r#"
            report filtered {
                from sales;
                use_date order_date;
                filter { region = "North America" };
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 1);
        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.name.value, "filtered");
                assert!(report.filter.is_some());
                let filter = &report.filter.as_ref().unwrap().value;
                assert!(filter.sql.contains("region"));
                assert!(filter.sql.contains("North America"));
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_report_multiple_sort() {
        let input = r#"
            report multi_sort {
                from sales;
                use_date order_date;
                sort revenue.desc, margin.asc, created_at.asc;
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.sort.len(), 3);
                assert_eq!(report.sort[0].value.column, "revenue");
                assert_eq!(report.sort[0].value.direction, SortDirection::Desc);
                assert_eq!(report.sort[1].value.column, "margin");
                assert_eq!(report.sort[1].value.direction, SortDirection::Asc);
                assert_eq!(report.sort[2].value.column, "created_at");
                assert_eq!(report.sort[2].value.direction, SortDirection::Asc);
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_report_multiple_groups() {
        let input = r#"
            report multi_group {
                from sales;
                use_date order_date;
                group {
                    dates.standard.month as "Month";
                    customers.geo.region as "Region";
                    products.hierarchy.category;
                }
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.group.len(), 3);

                match &report.group[0].value {
                    GroupItem::DrillPathRef(drill_ref) => {
                        assert_eq!(drill_ref.source, "dates");
                        assert_eq!(drill_ref.path, "standard");
                        assert_eq!(drill_ref.level, "month");
                        assert_eq!(drill_ref.label.as_deref(), Some("Month"));
                    }
                    _ => panic!("Expected DrillPathRef"),
                }

                match &report.group[1].value {
                    GroupItem::DrillPathRef(drill_ref) => {
                        assert_eq!(drill_ref.source, "customers");
                        assert_eq!(drill_ref.path, "geo");
                        assert_eq!(drill_ref.level, "region");
                        assert_eq!(drill_ref.label.as_deref(), Some("Region"));
                    }
                    _ => panic!("Expected DrillPathRef"),
                }

                match &report.group[2].value {
                    GroupItem::DrillPathRef(drill_ref) => {
                        assert_eq!(drill_ref.source, "products");
                        assert_eq!(drill_ref.path, "hierarchy");
                        assert_eq!(drill_ref.level, "category");
                        assert!(drill_ref.label.is_none());
                    }
                    _ => panic!("Expected DrillPathRef"),
                }
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_report_inline_measure() {
        let input = r#"
            report with_inline {
                from sales;
                use_date order_date;
                show {
                    revenue;
                    margin_pct = { revenue / cost } as "Margin %";
                }
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.show.len(), 2);

                match &report.show[0].value {
                    ShowItem::Measure { name, label } => {
                        assert_eq!(name, "revenue");
                        assert!(label.is_none());
                    }
                    _ => panic!("Expected Measure"),
                }

                match &report.show[1].value {
                    ShowItem::InlineMeasure { name, expr, label } => {
                        assert_eq!(name, "margin_pct");
                        assert!(expr.sql.contains("revenue"));
                        assert!(expr.sql.contains("cost"));
                        assert_eq!(label.as_deref(), Some("Margin %"));
                    }
                    _ => panic!("Expected InlineMeasure"),
                }
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_report_time_suffixes() {
        let input = r#"
            report time_intel {
                from sales;
                use_date order_date;
                show {
                    revenue.ytd;
                    revenue.prior_year as "Last Year";
                    revenue.yoy_growth;
                    revenue.rolling_12m as "12M Rolling";
                }
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.show.len(), 4);

                match &report.show[0].value {
                    ShowItem::MeasureWithSuffix { name, suffix, label } => {
                        assert_eq!(name, "revenue");
                        assert_eq!(*suffix, TimeSuffix::Ytd);
                        assert!(label.is_none());
                    }
                    _ => panic!("Expected MeasureWithSuffix"),
                }

                match &report.show[1].value {
                    ShowItem::MeasureWithSuffix { name, suffix, label } => {
                        assert_eq!(name, "revenue");
                        assert_eq!(*suffix, TimeSuffix::PriorYear);
                        assert_eq!(label.as_deref(), Some("Last Year"));
                    }
                    _ => panic!("Expected MeasureWithSuffix"),
                }

                match &report.show[2].value {
                    ShowItem::MeasureWithSuffix { name, suffix, label } => {
                        assert_eq!(name, "revenue");
                        assert_eq!(*suffix, TimeSuffix::YoyGrowth);
                        assert!(label.is_none());
                    }
                    _ => panic!("Expected MeasureWithSuffix"),
                }

                match &report.show[3].value {
                    ShowItem::MeasureWithSuffix { name, suffix, label } => {
                        assert_eq!(name, "revenue");
                        assert_eq!(*suffix, TimeSuffix::Rolling12m);
                        assert_eq!(label.as_deref(), Some("12M Rolling"));
                    }
                    _ => panic!("Expected MeasureWithSuffix"),
                }
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_report_multiple_tables() {
        let input = r#"
            report combined {
                from sales, returns;
                use_date sale_date, return_date;
            }
        "#;
        let model = parse_str(input);

        match &model.items[0].value {
            Item::Report(report) => {
                assert_eq!(report.from.len(), 2);
                assert_eq!(report.from[0].value, "sales");
                assert_eq!(report.from[1].value, "returns");
                assert_eq!(report.use_date.len(), 2);
                assert_eq!(report.use_date[0].value, "sale_date");
                assert_eq!(report.use_date[1].value, "return_date");
            }
            _ => panic!("Expected Report item"),
        }
    }

    #[test]
    fn test_parse_mixed_with_report() {
        let input = r#"
            table sales {
                source "fact_sales";
            }

            measures sales {
                revenue = { sum(@amount) };
            }

            report quarterly {
                from sales;
                use_date order_date;
                period this_quarter;
                show {
                    revenue;
                }
            }
        "#;
        let model = parse_str(input);

        assert_eq!(model.items.len(), 3);

        match &model.items[0].value {
            Item::Table(t) => assert_eq!(t.name.value, "sales"),
            _ => panic!("Expected Table"),
        }
        match &model.items[1].value {
            Item::MeasureBlock(mb) => assert_eq!(mb.table.value, "sales"),
            _ => panic!("Expected MeasureBlock"),
        }
        match &model.items[2].value {
            Item::Report(r) => assert_eq!(r.name.value, "quarterly"),
            _ => panic!("Expected Report"),
        }
    }
}
