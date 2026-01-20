//! Semantic validation for parsed DSL models.
//!
//! This module provides semantic validation that checks:
//! - All referenced calendars, dimensions, and tables are defined
//! - No duplicate definitions exist
//! - Via slicers reference valid slicers in the same table
//! - Measure blocks reference valid tables
//! - Reports reference valid tables and measures (warning for undefined measures)

use std::collections::HashMap;

use super::ast::*;
use super::span::{Span, Spanned};
use super::{Diagnostic, Severity};

/// Validate a parsed model for semantic correctness.
///
/// Returns a list of diagnostics (errors and warnings) found during validation.
/// An empty list indicates a semantically valid model.
///
/// # Example
///
/// ```ignore
/// use mantis::dsl::{parse, validation};
///
/// let result = parse(source);
/// if let Some(model) = result.model {
///     let diagnostics = validation::validate(&model);
///     for diag in diagnostics {
///         eprintln!("{}", diag);
///     }
/// }
/// ```
pub fn validate(model: &Model) -> Vec<Diagnostic> {
    let mut validator = Validator::new();
    validator.validate_model(model);
    validator.diagnostics
}

/// Internal validator state that tracks symbol definitions and collects diagnostics.
struct Validator {
    diagnostics: Vec<Diagnostic>,
    calendars: HashMap<String, Span>,
    dimensions: HashMap<String, Span>,
    tables: HashMap<String, Span>,
    // Table name -> (measure name -> span)
    measures: HashMap<String, HashMap<String, Span>>,
    // Table name -> (slicer name -> span)
    slicers: HashMap<String, HashMap<String, Span>>,
}

impl Validator {
    fn new() -> Self {
        Self {
            diagnostics: Vec::new(),
            calendars: HashMap::new(),
            dimensions: HashMap::new(),
            tables: HashMap::new(),
            measures: HashMap::new(),
            slicers: HashMap::new(),
        }
    }

    // ========================================================================
    // Diagnostic helpers
    // ========================================================================

    fn error(&mut self, span: Span, message: impl Into<String>) {
        self.diagnostics.push(Diagnostic::error(span, message));
    }

    fn warning(&mut self, span: Span, message: impl Into<String>) {
        self.diagnostics.push(Diagnostic::warning(span, message));
    }

    // ========================================================================
    // Top-level validation (two passes)
    // ========================================================================

    fn validate_model(&mut self, model: &Model) {
        // First pass: Register all definitions
        for item in &model.items {
            match &item.value {
                Item::Calendar(cal) => self.register_calendar(cal),
                Item::Dimension(dim) => self.register_dimension(dim),
                Item::Table(table) => self.register_table(table),
                Item::MeasureBlock(block) => self.register_measures(block),
                Item::Report(_) => {} // Reports don't define anything
            }
        }

        // Second pass: Validate references
        for item in &model.items {
            match &item.value {
                Item::Table(table) => self.validate_table(table),
                Item::MeasureBlock(block) => self.validate_measure_block(block),
                Item::Report(report) => self.validate_report(report),
                _ => {}
            }
        }
    }

    // ========================================================================
    // First pass: Registration
    // ========================================================================

    fn register_calendar(&mut self, cal: &Calendar) {
        let name = cal.name.value.clone();
        let span = cal.name.span.clone();

        if let Some(prev_span) = self.calendars.insert(name.clone(), span.clone()) {
            self.error(
                span,
                format!("Duplicate calendar definition '{}' (first defined at {:?})", name, prev_span),
            );
        }
    }

    fn register_dimension(&mut self, dim: &Dimension) {
        let name = dim.name.value.clone();
        let span = dim.name.span.clone();

        if let Some(prev_span) = self.dimensions.insert(name.clone(), span.clone()) {
            self.error(
                span,
                format!("Duplicate dimension definition '{}' (first defined at {:?})", name, prev_span),
            );
        }
    }

    fn register_table(&mut self, table: &Table) {
        let name = table.name.value.clone();
        let span = table.name.span.clone();

        if let Some(prev_span) = self.tables.insert(name.clone(), span.clone()) {
            self.error(
                span,
                format!("Duplicate table definition '{}' (first defined at {:?})", name, prev_span),
            );
        }

        // Register slicers for this table
        let mut table_slicers = HashMap::new();
        for slicer in &table.slicers {
            let slicer_name = slicer.value.name.value.clone();
            let slicer_span = slicer.value.name.span.clone();

            if let Some(prev_span) = table_slicers.insert(slicer_name.clone(), slicer_span.clone()) {
                self.error(
                    slicer_span,
                    format!(
                        "Duplicate slicer '{}' in table '{}' (first defined at {:?})",
                        slicer_name, name, prev_span
                    ),
                );
            }
        }
        self.slicers.insert(name, table_slicers);
    }

    fn register_measures(&mut self, block: &MeasureBlock) {
        let table_name = block.table.value.clone();

        // Collect duplicates first to avoid borrowing issues
        let mut duplicates = Vec::new();
        let table_measures = self.measures.entry(table_name.clone()).or_insert_with(HashMap::new);

        for measure in &block.measures {
            let measure_name = measure.value.name.value.clone();
            let measure_span = measure.value.name.span.clone();

            if let Some(prev_span) = table_measures.insert(measure_name.clone(), measure_span.clone()) {
                duplicates.push((measure_name, measure_span, prev_span));
            }
        }

        // Now emit errors
        for (measure_name, measure_span, prev_span) in duplicates {
            self.error(
                measure_span,
                format!(
                    "Duplicate measure '{}' in table '{}' (first defined at {:?})",
                    measure_name, table_name, prev_span
                ),
            );
        }
    }

    // ========================================================================
    // Second pass: Validation
    // ========================================================================

    fn validate_table(&mut self, table: &Table) {
        let table_name = &table.name.value;

        // Validate time bindings reference valid calendars
        for time in &table.times {
            let calendar_name = &time.value.calendar.value;
            let calendar_span = time.value.calendar.span.clone();

            if !self.calendars.contains_key(calendar_name) {
                self.error(
                    calendar_span,
                    format!("Undefined calendar '{}' in time binding", calendar_name),
                );
            }
        }

        // Validate slicers
        for slicer in &table.slicers {
            match &slicer.value.kind.value {
                SlicerKind::ForeignKey { dimension, .. } => {
                    // Check that dimension exists
                    if !self.dimensions.contains_key(dimension) {
                        self.error(
                            slicer.value.kind.span.clone(),
                            format!("Undefined dimension '{}' in foreign key slicer", dimension),
                        );
                    }
                }
                SlicerKind::Via { fk_slicer } => {
                    // Check that referenced slicer exists in this table
                    if let Some(table_slicers) = self.slicers.get(table_name) {
                        if !table_slicers.contains_key(fk_slicer) {
                            self.error(
                                slicer.value.kind.span.clone(),
                                format!(
                                    "Undefined slicer '{}' referenced in via slicer (in table '{}')",
                                    fk_slicer, table_name
                                ),
                            );
                        } else {
                            // Additional check: ensure the referenced slicer is a FK, not another via
                            // We need to find the actual slicer to check its kind
                            for candidate in &table.slicers {
                                if candidate.value.name.value == *fk_slicer {
                                    if matches!(candidate.value.kind.value, SlicerKind::Via { .. }) {
                                        self.error(
                                            slicer.value.kind.span.clone(),
                                            format!(
                                                "Via slicer '{}' cannot reference another via slicer '{}'",
                                                slicer.value.name.value, fk_slicer
                                            ),
                                        );
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                SlicerKind::Inline { .. } | SlicerKind::Calculated { .. } => {
                    // No validation needed for inline or calculated slicers
                }
            }
        }
    }

    fn validate_measure_block(&mut self, block: &MeasureBlock) {
        let table_name = &block.table.value;
        let table_span = block.table.span.clone();

        // Check that the table exists
        if !self.tables.contains_key(table_name) {
            self.error(
                table_span,
                format!("Measure block references undefined table '{}'", table_name),
            );
        }
    }

    fn validate_report(&mut self, report: &Report) {
        // Validate FROM tables
        for table_ref in &report.from {
            let table_name = &table_ref.value;
            let table_span = table_ref.span.clone();

            if !self.tables.contains_key(table_name) {
                self.error(
                    table_span,
                    format!("Report references undefined table '{}' in FROM clause", table_name),
                );
            }
        }

        // Validate SHOW measures (warnings only, since inline measures are allowed)
        for show_item in &report.show {
            match &show_item.value {
                ShowItem::Measure { name, .. } | ShowItem::MeasureWithSuffix { name, .. } => {
                    // Check if this measure exists in any of the FROM tables
                    let mut found = false;
                    for table_ref in &report.from {
                        let table_name = &table_ref.value;
                        if let Some(table_measures) = self.measures.get(table_name) {
                            if table_measures.contains_key(name) {
                                found = true;
                                break;
                            }
                        }
                    }

                    if !found {
                        self.warning(
                            show_item.span.clone(),
                            format!(
                                "Measure '{}' not found in any of the FROM tables ({})",
                                name,
                                report.from.iter().map(|t| t.value.as_str()).collect::<Vec<_>>().join(", ")
                            ),
                        );
                    }
                }
                ShowItem::InlineMeasure { .. } => {
                    // Inline measures are always valid (they define their own expression)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_undefined_calendar() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::Table(Table {
                    name: Spanned::new("sales".to_string(), 0..5),
                    source: Spanned::new("sales.csv".to_string(), 6..15),
                    atoms: vec![],
                    times: vec![Spanned::new(
                        TimeBinding {
                            name: Spanned::new("date".to_string(), 16..20),
                            calendar: Spanned::new("unknown_cal".to_string(), 21..31),
                            grain: Spanned::new(GrainLevel::Day, 32..35),
                        },
                        16..35,
                    )],
                    slicers: vec![],
                }),
                0..40,
            )],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Undefined calendar"));
        assert!(diagnostics[0].message.contains("unknown_cal"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_undefined_dimension() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::Table(Table {
                    name: Spanned::new("sales".to_string(), 0..5),
                    source: Spanned::new("sales.csv".to_string(), 6..15),
                    atoms: vec![],
                    times: vec![],
                    slicers: vec![Spanned::new(
                        Slicer {
                            name: Spanned::new("customer".to_string(), 16..24),
                            kind: Spanned::new(
                                SlicerKind::ForeignKey {
                                    dimension: "customers".to_string(),
                                    key_column: "id".to_string(),
                                },
                                25..40,
                            ),
                        },
                        16..40,
                    )],
                }),
                0..45,
            )],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Undefined dimension"));
        assert!(diagnostics[0].message.contains("customers"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_duplicate_table() {
        let model = Model {
            defaults: None,
            items: vec![
                Spanned::new(
                    Item::Table(Table {
                        name: Spanned::new("sales".to_string(), 0..5),
                        source: Spanned::new("sales1.csv".to_string(), 6..16),
                        atoms: vec![],
                        times: vec![],
                        slicers: vec![],
                    }),
                    0..20,
                ),
                Spanned::new(
                    Item::Table(Table {
                        name: Spanned::new("sales".to_string(), 21..26),
                        source: Spanned::new("sales2.csv".to_string(), 27..37),
                        atoms: vec![],
                        times: vec![],
                        slicers: vec![],
                    }),
                    21..40,
                ),
            ],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Duplicate table"));
        assert!(diagnostics[0].message.contains("sales"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_undefined_via_slicer() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::Table(Table {
                    name: Spanned::new("sales".to_string(), 0..5),
                    source: Spanned::new("sales.csv".to_string(), 6..15),
                    atoms: vec![],
                    times: vec![],
                    slicers: vec![Spanned::new(
                        Slicer {
                            name: Spanned::new("region".to_string(), 16..22),
                            kind: Spanned::new(
                                SlicerKind::Via {
                                    fk_slicer: "nonexistent".to_string(),
                                },
                                23..35,
                            ),
                        },
                        16..35,
                    )],
                }),
                0..40,
            )],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Undefined slicer"));
        assert!(diagnostics[0].message.contains("nonexistent"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_via_slicer_referencing_via_slicer() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::Table(Table {
                    name: Spanned::new("sales".to_string(), 0..5),
                    source: Spanned::new("sales.csv".to_string(), 6..15),
                    atoms: vec![],
                    times: vec![],
                    slicers: vec![
                        Spanned::new(
                            Slicer {
                                name: Spanned::new("customer_via".to_string(), 16..28),
                                kind: Spanned::new(
                                    SlicerKind::Via {
                                        fk_slicer: "some_fk".to_string(),
                                    },
                                    29..40,
                                ),
                            },
                            16..40,
                        ),
                        Spanned::new(
                            Slicer {
                                name: Spanned::new("invalid_via".to_string(), 41..52),
                                kind: Spanned::new(
                                    SlicerKind::Via {
                                        fk_slicer: "customer_via".to_string(),
                                    },
                                    53..70,
                                ),
                            },
                            41..70,
                        ),
                    ],
                }),
                0..75,
            )],
        };

        let diagnostics = validate(&model);
        // Should have two errors: "some_fk" doesn't exist, and via can't reference via
        assert_eq!(diagnostics.len(), 2);
        let has_undefined = diagnostics.iter().any(|d| d.message.contains("Undefined slicer") && d.message.contains("some_fk"));
        let has_via_error = diagnostics.iter().any(|d| d.message.contains("cannot reference another via slicer"));
        assert!(has_undefined);
        assert!(has_via_error);
    }

    #[test]
    fn test_validate_measure_block_undefined_table() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::MeasureBlock(MeasureBlock {
                    table: Spanned::new("nonexistent".to_string(), 0..11),
                    measures: vec![],
                }),
                0..15,
            )],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("undefined table"));
        assert!(diagnostics[0].message.contains("nonexistent"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_report_undefined_table() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::Report(Report {
                    name: Spanned::new("test_report".to_string(), 0..11),
                    from: vec![Spanned::new("nonexistent".to_string(), 12..23)],
                    use_date: vec![],
                    period: None,
                    group: vec![],
                    show: vec![],
                    filter: None,
                    sort: vec![],
                    limit: None,
                }),
                0..30,
            )],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("undefined table"));
        assert!(diagnostics[0].message.contains("nonexistent"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_report_undefined_measure_warning() {
        let model = Model {
            defaults: None,
            items: vec![
                Spanned::new(
                    Item::Table(Table {
                        name: Spanned::new("sales".to_string(), 0..5),
                        source: Spanned::new("sales.csv".to_string(), 6..15),
                        atoms: vec![],
                        times: vec![],
                        slicers: vec![],
                    }),
                    0..20,
                ),
                Spanned::new(
                    Item::Report(Report {
                        name: Spanned::new("test_report".to_string(), 21..32),
                        from: vec![Spanned::new("sales".to_string(), 33..38)],
                        use_date: vec![],
                        period: None,
                        group: vec![],
                        show: vec![Spanned::new(
                            ShowItem::Measure {
                                name: "revenue".to_string(),
                                label: None,
                            },
                            39..46,
                        )],
                        filter: None,
                        sort: vec![],
                        limit: None,
                    }),
                    21..50,
                ),
            ],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Measure 'revenue' not found"));
        assert_eq!(diagnostics[0].severity, Severity::Warning);
    }

    #[test]
    fn test_validate_valid_model() {
        let model = Model {
            defaults: None,
            items: vec![
                Spanned::new(
                    Item::Calendar(Calendar {
                        name: Spanned::new("auto".to_string(), 0..4),
                        body: Spanned::new(
                            CalendarBody::Generated(GeneratedCalendar {
                                base_grain: Spanned::new(GrainLevel::Day, 5..8),
                                fiscal: None,
                                range: None,
                                drill_paths: vec![],
                                week_start: None,
                            }),
                            5..10,
                        ),
                    }),
                    0..10,
                ),
                Spanned::new(
                    Item::Dimension(Dimension {
                        name: Spanned::new("customers".to_string(), 11..20),
                        source: Spanned::new("dim_customers".to_string(), 21..34),
                        key: Spanned::new("customer_id".to_string(), 35..46),
                        attributes: vec![],
                        drill_paths: vec![],
                    }),
                    11..50,
                ),
                Spanned::new(
                    Item::Table(Table {
                        name: Spanned::new("sales".to_string(), 51..56),
                        source: Spanned::new("sales.csv".to_string(), 57..66),
                        atoms: vec![Spanned::new(
                            Atom {
                                name: Spanned::new("amount".to_string(), 67..73),
                                atom_type: Spanned::new(AtomType::Decimal, 74..81),
                            },
                            67..81,
                        )],
                        times: vec![Spanned::new(
                            TimeBinding {
                                name: Spanned::new("date".to_string(), 82..86),
                                calendar: Spanned::new("auto".to_string(), 87..91),
                                grain: Spanned::new(GrainLevel::Day, 92..95),
                            },
                            82..95,
                        )],
                        slicers: vec![
                            Spanned::new(
                                Slicer {
                                    name: Spanned::new("customer_id".to_string(), 96..107),
                                    kind: Spanned::new(
                                        SlicerKind::ForeignKey {
                                            dimension: "customers".to_string(),
                                            key_column: "customer_id".to_string(),
                                        },
                                        108..130,
                                    ),
                                },
                                96..130,
                            ),
                            Spanned::new(
                                Slicer {
                                    name: Spanned::new("customer_name".to_string(), 131..144),
                                    kind: Spanned::new(
                                        SlicerKind::Via {
                                            fk_slicer: "customer_id".to_string(),
                                        },
                                        145..160,
                                    ),
                                },
                                131..160,
                            ),
                        ],
                    }),
                    51..165,
                ),
                Spanned::new(
                    Item::MeasureBlock(MeasureBlock {
                        table: Spanned::new("sales".to_string(), 166..171),
                        measures: vec![Spanned::new(
                            Measure {
                                name: Spanned::new("revenue".to_string(), 172..179),
                                expr: Spanned::new(
                                    SqlExpr::new("sum(@amount)".to_string(), 180..192),
                                    180..192,
                                ),
                                filter: None,
                                null_handling: None,
                            },
                            172..192,
                        )],
                    }),
                    166..195,
                ),
                Spanned::new(
                    Item::Report(Report {
                        name: Spanned::new("summary".to_string(), 196..203),
                        from: vec![Spanned::new("sales".to_string(), 204..209)],
                        use_date: vec![],
                        period: None,
                        group: vec![],
                        show: vec![Spanned::new(
                            ShowItem::Measure {
                                name: "revenue".to_string(),
                                label: None,
                            },
                            210..217,
                        )],
                        filter: None,
                        sort: vec![],
                        limit: None,
                    }),
                    196..220,
                ),
            ],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 0, "Expected no diagnostics for valid model, but got: {:?}", diagnostics);
    }

    #[test]
    fn test_validate_duplicate_calendar() {
        let model = Model {
            defaults: None,
            items: vec![
                Spanned::new(
                    Item::Calendar(Calendar {
                        name: Spanned::new("auto".to_string(), 0..4),
                        body: Spanned::new(
                            CalendarBody::Generated(GeneratedCalendar {
                                base_grain: Spanned::new(GrainLevel::Day, 5..8),
                                fiscal: None,
                                range: None,
                                drill_paths: vec![],
                                week_start: None,
                            }),
                            5..10,
                        ),
                    }),
                    0..10,
                ),
                Spanned::new(
                    Item::Calendar(Calendar {
                        name: Spanned::new("auto".to_string(), 11..15),
                        body: Spanned::new(
                            CalendarBody::Generated(GeneratedCalendar {
                                base_grain: Spanned::new(GrainLevel::Day, 16..19),
                                fiscal: None,
                                range: None,
                                drill_paths: vec![],
                                week_start: None,
                            }),
                            16..20,
                        ),
                    }),
                    11..20,
                ),
            ],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Duplicate calendar"));
        assert!(diagnostics[0].message.contains("auto"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_duplicate_dimension() {
        let model = Model {
            defaults: None,
            items: vec![
                Spanned::new(
                    Item::Dimension(Dimension {
                        name: Spanned::new("customers".to_string(), 0..9),
                        source: Spanned::new("dim_customers".to_string(), 10..23),
                        key: Spanned::new("id".to_string(), 24..26),
                        attributes: vec![],
                        drill_paths: vec![],
                    }),
                    0..30,
                ),
                Spanned::new(
                    Item::Dimension(Dimension {
                        name: Spanned::new("customers".to_string(), 31..40),
                        source: Spanned::new("dim_customers2".to_string(), 41..55),
                        key: Spanned::new("id".to_string(), 56..58),
                        attributes: vec![],
                        drill_paths: vec![],
                    }),
                    31..60,
                ),
            ],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Duplicate dimension"));
        assert!(diagnostics[0].message.contains("customers"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_duplicate_slicer() {
        let model = Model {
            defaults: None,
            items: vec![Spanned::new(
                Item::Table(Table {
                    name: Spanned::new("sales".to_string(), 0..5),
                    source: Spanned::new("sales.csv".to_string(), 6..15),
                    atoms: vec![],
                    times: vec![],
                    slicers: vec![
                        Spanned::new(
                            Slicer {
                                name: Spanned::new("region".to_string(), 16..22),
                                kind: Spanned::new(
                                    SlicerKind::Inline {
                                        data_type: DataType::String,
                                    },
                                    23..30,
                                ),
                            },
                            16..30,
                        ),
                        Spanned::new(
                            Slicer {
                                name: Spanned::new("region".to_string(), 31..37),
                                kind: Spanned::new(
                                    SlicerKind::Inline {
                                        data_type: DataType::String,
                                    },
                                    38..45,
                                ),
                            },
                            31..45,
                        ),
                    ],
                }),
                0..50,
            )],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Duplicate slicer"));
        assert!(diagnostics[0].message.contains("region"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_duplicate_measure() {
        let model = Model {
            defaults: None,
            items: vec![
                Spanned::new(
                    Item::Table(Table {
                        name: Spanned::new("sales".to_string(), 0..5),
                        source: Spanned::new("sales.csv".to_string(), 6..15),
                        atoms: vec![],
                        times: vec![],
                        slicers: vec![],
                    }),
                    0..20,
                ),
                Spanned::new(
                    Item::MeasureBlock(MeasureBlock {
                        table: Spanned::new("sales".to_string(), 21..26),
                        measures: vec![
                            Spanned::new(
                                Measure {
                                    name: Spanned::new("revenue".to_string(), 27..34),
                                    expr: Spanned::new(
                                        SqlExpr::new("sum(@amount)".to_string(), 35..47),
                                        35..47,
                                    ),
                                    filter: None,
                                    null_handling: None,
                                },
                                27..47,
                            ),
                            Spanned::new(
                                Measure {
                                    name: Spanned::new("revenue".to_string(), 48..55),
                                    expr: Spanned::new(
                                        SqlExpr::new("sum(@price)".to_string(), 56..67),
                                        56..67,
                                    ),
                                    filter: None,
                                    null_handling: None,
                                },
                                48..67,
                            ),
                        ],
                    }),
                    21..70,
                ),
            ],
        };

        let diagnostics = validate(&model);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("Duplicate measure"));
        assert!(diagnostics[0].message.contains("revenue"));
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }
}
