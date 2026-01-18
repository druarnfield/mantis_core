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

    // Parse an identifier
    let ident = select! {
        Token::Ident(s) => s.to_string(),
    }.labelled("identifier");

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
    // Parse any token that's not RBrace, then reconstruct the SQL string from tokens
    let sql_token = any()
        .filter(|t: &Token| !matches!(t, Token::RBrace))
        .map_with(|t: Token, e| (t.to_string(), to_span(e.span())));

    let slicer_calculated = ident.clone()
        .map_with(|n, e| Spanned::new(n, to_span(e.span())))
        .then(
            data_type
                .map_with(|t, e| Spanned::new(t, to_span(e.span())))
        )
        .then_ignore(just(Token::Eq))
        .then(
            just(Token::LBrace)
                .map_with(|_, e| to_span(e.span()))
                .then(
                    sql_token
                        .repeated()
                        .collect::<Vec<_>>()
                )
                .then(
                    just(Token::RBrace)
                        .map_with(|_, e| to_span(e.span()))
                )
                .map(|((lbrace_span, tokens), rbrace_span)| {
                    // Reconstruct SQL from tokens
                    let sql = tokens.iter()
                        .map(|(s, _)| s.as_str())
                        .collect::<Vec<_>>()
                        .join(" ");
                    // Span covers from LBrace start to RBrace end
                    let span = lbrace_span.start..rbrace_span.end;
                    SqlExpr::new(sql, span)
                })
        )
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
    // Top-level items
    // ==========================================================================

    // For now, only parse tables
    let item = table.map(Item::Table);

    // The model is a list of items
    item
        .map_with(|i, e| Spanned::new(i, to_span(e.span())))
        .repeated()
        .collect::<Vec<_>>()
        .map(|items| Model {
            defaults: None,
            items,
        })
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
}
