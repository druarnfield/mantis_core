//! Translation of Report model types to SemanticQuery.
//!
//! This module provides functions to translate high-level Report structures from the
//! semantic model DSL into SemanticQuery structures that can be executed by the query planner.
//!
//! ## Future Improvements
//!
//! ### Code Quality
//! - **TODO**: Add `#[must_use]` attribute to public functions that return `Result`.
//!   Functions like `translate_report`, `compile_sql_expr`, etc. should have `#[must_use]`
//!   to ensure callers don't accidentally ignore errors. This is a Rust best practice
//!   for functions where ignoring the return value is likely a programming error.
//!
//! ### Known Limitations
//! - Filters are compiled but not added to `query.filters` (requires SQL parsing to FieldFilter)
//! - Period expressions are not yet implemented (requires calendar integration)
//! - Inline measures require SQL expression parsing to DerivedExpr
//! - WTD (Week-to-Date) uses MonthToDate as a placeholder
//! - PriorMonth and PriorWeek lack granularity distinction

use crate::model::{Model, Report};
use crate::semantic::planner::types::{
    DerivedExpr, DerivedField, OrderField, SelectField, SemanticQuery, TimeFunction,
};
use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Regex pattern for matching @atom references in SQL expressions.
/// Compiled once at startup for performance.
static ATOM_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"@(\w+)").unwrap());

/// Translation error.
#[derive(Debug, Clone)]
pub enum TranslationError {
    /// Reference to undefined entity.
    UndefinedReference { entity_type: String, name: String },
    /// Invalid drill path reference.
    InvalidDrillPath {
        source: String,
        path: String,
        level: String,
    },
    /// Invalid measure reference.
    InvalidMeasure { measure: String, table: String },
    /// SQL expression compilation error.
    SqlCompilationError { expression: String, error: String },
}

impl std::fmt::Display for TranslationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslationError::UndefinedReference { entity_type, name } => {
                write!(f, "Undefined {} reference: {}", entity_type, name)
            }
            TranslationError::InvalidDrillPath {
                source,
                path,
                level,
            } => {
                write!(f, "Invalid drill path: {}.{}.{}", source, path, level)
            }
            TranslationError::InvalidMeasure { measure, table } => {
                write!(f, "Invalid measure '{}' in table '{}'", measure, table)
            }
            TranslationError::SqlCompilationError { expression, error } => {
                write!(f, "SQL compilation error in '{}': {}", expression, error)
            }
        }
    }
}

impl std::error::Error for TranslationError {}

/// Resolve an inline slicer reference to a FieldRef.
///
/// An inline slicer is a grouping dimension defined directly on a table.
/// The slicer name IS the column name for inline slicers.
///
/// This resolves to a FieldRef pointing to the table and column.
fn resolve_inline_slicer(
    slicer_name: &str,
    from_table: &str,
    model: &Model,
) -> Result<crate::semantic::planner::types::FieldRef, TranslationError> {
    // Get the table
    let table =
        model
            .tables
            .get(from_table)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "table".to_string(),
                name: from_table.to_string(),
            })?;

    // Verify the slicer exists in the table
    let _slicer =
        table
            .slicers
            .get(slicer_name)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "slicer".to_string(),
                name: slicer_name.to_string(),
            })?;

    // For inline slicers, the field name is the slicer name
    Ok(crate::semantic::planner::types::FieldRef::new(
        from_table,
        slicer_name,
    ))
}

/// Resolve a drill path reference to a FieldRef.
///
/// A drill path reference has three parts:
/// - source: The calendar name (e.g., "dates")
/// - path: The drill path name (e.g., "standard")
/// - level: The grain level (e.g., "month")
///
/// This resolves to a FieldRef pointing to the actual column name.
fn resolve_drill_path_reference(
    source: &str,
    path: &str,
    level: &str,
    model: &Model,
) -> Result<crate::semantic::planner::types::FieldRef, TranslationError> {
    // Get the calendar
    let calendar =
        model
            .calendars
            .get(source)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "calendar".to_string(),
                name: source.to_string(),
            })?;

    // Get drill paths from calendar body
    let drill_paths = match &calendar.body {
        crate::model::CalendarBody::Physical(phys) => &phys.drill_paths,
        crate::model::CalendarBody::Generated { .. } => {
            return Err(TranslationError::InvalidDrillPath {
                source: source.to_string(),
                path: path.to_string(),
                level: level.to_string(),
            });
        }
    };

    // Get the specific drill path
    let drill_path = drill_paths
        .get(path)
        .ok_or_else(|| TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        })?;

    // Parse the level string to GrainLevel
    let grain_level = crate::model::GrainLevel::from_str(level).ok_or_else(|| {
        TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        }
    })?;

    // Verify the grain level is in the drill path
    if !drill_path.levels.contains(&grain_level) {
        return Err(TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        });
    }

    // Get the column name for this grain level
    let grain_mappings = match &calendar.body {
        crate::model::CalendarBody::Physical(phys) => &phys.grain_mappings,
        _ => unreachable!(),
    };

    let column =
        grain_mappings
            .get(&grain_level)
            .ok_or_else(|| TranslationError::InvalidDrillPath {
                source: source.to_string(),
                path: path.to_string(),
                level: level.to_string(),
            })?;

    Ok(crate::semantic::planner::types::FieldRef::new(
        source, column,
    ))
}

/// Translate a simple measure reference to a SelectField.
///
/// A simple measure is a direct reference to a measure defined in the model,
/// without any time intelligence suffixes or inline expressions.
fn translate_simple_measure(
    measure_name: &str,
    label: Option<String>,
    from_table: &str,
    model: &Model,
) -> Result<SelectField, TranslationError> {
    // Find the measure in the model
    let measure_block =
        model
            .measures
            .get(from_table)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "measure block".to_string(),
                name: from_table.to_string(),
            })?;

    let _measure = measure_block.measures.get(measure_name).ok_or_else(|| {
        TranslationError::InvalidMeasure {
            measure: measure_name.to_string(),
            table: from_table.to_string(),
        }
    })?;

    // Create SelectField
    let mut select_field = SelectField::new(from_table, measure_name);
    if let Some(label) = label {
        select_field = select_field.with_alias(&label);
    }

    Ok(select_field)
}

/// Translate a measure with a time suffix to a SelectField and DerivedField.
///
/// Time suffixes like `.ytd` expand into:
/// 1. A base SelectField for the measure (added to query.select)
/// 2. A DerivedField with a TimeFunction (added to query.derived)
///
/// The DerivedField references the base measure to calculate time-based values.
fn translate_time_suffix(
    measure_name: &str,
    suffix: crate::model::TimeSuffix,
    label: Option<String>,
    from_table: &str,
    model: &Model,
) -> Result<(SelectField, DerivedField), TranslationError> {
    // First, ensure the base measure exists
    let measure_block =
        model
            .measures
            .get(from_table)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "measure block".to_string(),
                name: from_table.to_string(),
            })?;

    let _measure = measure_block.measures.get(measure_name).ok_or_else(|| {
        TranslationError::InvalidMeasure {
            measure: measure_name.to_string(),
            table: from_table.to_string(),
        }
    })?;

    // Create base measure select field
    let base_select = SelectField::new(from_table, measure_name);

    // Create derived field based on suffix
    let derived_alias = label.unwrap_or_else(|| format!("{}_{:?}", measure_name, suffix));

    let derived_expr = match suffix {
        // Accumulations
        crate::model::TimeSuffix::Ytd => DerivedExpr::TimeFunction(TimeFunction::YearToDate {
            measure: measure_name.to_string(),
            year_column: None,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Qtd => DerivedExpr::TimeFunction(TimeFunction::QuarterToDate {
            measure: measure_name.to_string(),
            year_column: None,
            quarter_column: None,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Mtd => DerivedExpr::TimeFunction(TimeFunction::MonthToDate {
            measure: measure_name.to_string(),
            year_column: None,
            month_column: None,
            day_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Wtd => {
            // TODO: WTD currently uses MonthToDate which is semantically incorrect.
            // This is a known limitation. Proper implementation requires:
            // - A WeekToDate TimeFunction variant
            // - Week boundary logic (week start day from calendar)
            // - Week number calculations for filtering
            DerivedExpr::TimeFunction(TimeFunction::MonthToDate {
                measure: measure_name.to_string(),
                year_column: None,
                month_column: None,
                day_column: None,
                via: None,
            })
        }
        crate::model::TimeSuffix::FiscalYtd => {
            DerivedExpr::TimeFunction(TimeFunction::YearToDate {
                measure: measure_name.to_string(),
                year_column: None,
                period_column: None,
                via: Some("fiscal".to_string()),
            })
        }
        crate::model::TimeSuffix::FiscalQtd => {
            DerivedExpr::TimeFunction(TimeFunction::QuarterToDate {
                measure: measure_name.to_string(),
                year_column: None,
                quarter_column: None,
                period_column: None,
                via: Some("fiscal".to_string()),
            })
        }

        // Prior periods
        crate::model::TimeSuffix::PriorYear => DerivedExpr::TimeFunction(TimeFunction::PriorYear {
            measure: measure_name.to_string(),
            via: None,
        }),
        crate::model::TimeSuffix::PriorQuarter => {
            DerivedExpr::TimeFunction(TimeFunction::PriorQuarter {
                measure: measure_name.to_string(),
                via: None,
            })
        }
        crate::model::TimeSuffix::PriorMonth => {
            // TODO: PriorMonth and PriorWeek both map to PriorPeriod with periods_back: 1,
            // but they lack granularity distinction (month vs week). Proper implementation requires:
            // - Adding a period_type parameter to PriorPeriod (Month, Week, Day, etc.)
            // - Or separate PriorMonth and PriorWeek TimeFunction variants
            // For now, both behave identically which may cause issues in query generation.
            DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods_back: 1,
                via: None,
            })
        }
        crate::model::TimeSuffix::PriorWeek => {
            // TODO: See PriorMonth comment above - same limitation applies here.
            DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods_back: 1,
                via: None,
            })
        }

        // Growth calculations
        crate::model::TimeSuffix::YoyGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorYear {
                measure: measure_name.to_string(),
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        crate::model::TimeSuffix::QoqGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorQuarter {
                measure: measure_name.to_string(),
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        crate::model::TimeSuffix::MomGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods_back: 1,
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        crate::model::TimeSuffix::WowGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods_back: 1,
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }

        // Delta calculations
        crate::model::TimeSuffix::YoyDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorYear {
                measure: measure_name.to_string(),
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        crate::model::TimeSuffix::QoqDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorQuarter {
                measure: measure_name.to_string(),
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        crate::model::TimeSuffix::MomDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods_back: 1,
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        crate::model::TimeSuffix::WowDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods_back: 1,
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }

        // Rolling windows
        crate::model::TimeSuffix::Rolling3m => {
            DerivedExpr::TimeFunction(TimeFunction::RollingSum {
                measure: measure_name.to_string(),
                periods: 3,
                via: None,
            })
        }
        crate::model::TimeSuffix::Rolling6m => {
            DerivedExpr::TimeFunction(TimeFunction::RollingSum {
                measure: measure_name.to_string(),
                periods: 6,
                via: None,
            })
        }
        crate::model::TimeSuffix::Rolling12m => {
            DerivedExpr::TimeFunction(TimeFunction::RollingSum {
                measure: measure_name.to_string(),
                periods: 12,
                via: None,
            })
        }
        crate::model::TimeSuffix::Rolling3mAvg => {
            DerivedExpr::TimeFunction(TimeFunction::RollingAvg {
                measure: measure_name.to_string(),
                periods: 3,
                via: None,
            })
        }
        crate::model::TimeSuffix::Rolling6mAvg => {
            DerivedExpr::TimeFunction(TimeFunction::RollingAvg {
                measure: measure_name.to_string(),
                periods: 6,
                via: None,
            })
        }
        crate::model::TimeSuffix::Rolling12mAvg => {
            DerivedExpr::TimeFunction(TimeFunction::RollingAvg {
                measure: measure_name.to_string(),
                periods: 12,
                via: None,
            })
        }
    };

    let derived_field = DerivedField {
        alias: derived_alias,
        expression: derived_expr,
    };

    Ok((base_select, derived_field))
}

/// Translate a Report to a SemanticQuery.
pub fn translate_report(report: &Report, model: &Model) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();

    // Validate and set from clause
    if report.from.is_empty() {
        return Err(TranslationError::UndefinedReference {
            entity_type: "table".to_string(),
            name: "(none specified)".to_string(),
        });
    }

    if report.from.len() > 1 {
        // For now, only single-table queries supported
        // Multi-table support will be added in future tasks
        return Err(TranslationError::UndefinedReference {
            entity_type: "multi-table query".to_string(),
            name: format!("got {} tables, expected 1", report.from.len()),
        });
    }

    let from_table = &report.from[0];
    query.from = Some(from_table.clone());

    // Translate group items
    for group_item in &report.group {
        match group_item {
            crate::model::GroupItem::DrillPathRef {
                source,
                path,
                level,
                ..
            } => {
                let field_ref = resolve_drill_path_reference(source, path, level, model)?;
                query.group_by.push(field_ref);
            }
            crate::model::GroupItem::InlineSlicer { name, .. } => {
                let field_ref = resolve_inline_slicer(name, from_table, model)?;
                query.group_by.push(field_ref);
            }
        }
    }

    // Translate show items
    // Track measures already added to select to avoid duplicates
    let mut added_measures = std::collections::HashSet::new();

    for show_item in &report.show {
        match show_item {
            crate::model::ShowItem::Measure { name, label } => {
                let select_field =
                    translate_simple_measure(name, label.clone(), from_table, model)?;
                // Only add if not already present
                if added_measures.insert(name.clone()) {
                    query.select.push(select_field);
                }
            }
            crate::model::ShowItem::MeasureWithSuffix {
                name,
                suffix,
                label,
            } => {
                let (base_select, derived_field) =
                    translate_time_suffix(name, *suffix, label.clone(), from_table, model)?;
                // Only add base measure if not already present
                if added_measures.insert(name.clone()) {
                    query.select.push(base_select);
                }
                query.derived.push(derived_field);
            }
            crate::model::ShowItem::InlineMeasure { name, expr, label } => {
                let derived_field = translate_inline_measure(name, expr, label.clone())?;
                query.derived.push(derived_field);
            }
        }
    }

    // Translate period (if specified)
    // This will generate a date range filter on use_date columns
    let _period_filter = translate_period(&report.period, &report.use_date)?;
    // TODO: Add period_filter to query.filters when implemented

    // Translate filters (compile SQL expressions)
    // Note: For now, we compile the SQL but store as raw strings
    // TODO: Parse compiled SQL into FieldFilter structures
    let _compiled_filters = translate_filters(&report.filters, from_table, model)?;
    // Filters are compiled but not yet added to query - full parsing comes later

    // Translate sort items
    query.order_by = translate_sort_items(&report.sort, from_table);

    // Translate limit
    query.limit = report.limit;

    Ok(query)
}

/// Translate period expressions into date range filters.
///
/// Period expressions like "this_month", "last_year", "ytd" need to be converted
/// into actual date range filters based on:
/// 1. The current date (or a reference date)
/// 2. Calendar logic (fiscal vs. calendar year, week start day, etc.)
/// 3. The use_date columns to filter on
///
/// This is a skeleton implementation. Full implementation requires:
/// 1. Date arithmetic to compute start/end dates for relative periods
/// 2. Integration with calendar logic (fiscal year start, week start, etc.)
/// 3. Support for absolute date ranges
/// 4. Generation of FieldFilter objects with date comparisons
///
/// For now, this returns None if period is None, or an error if period is specified.
fn translate_period(
    period: &Option<crate::model::PeriodExpr>,
    _use_date: &[String],
) -> Result<Option<String>, TranslationError> {
    if period.is_some() {
        // TODO: Implement period to date range conversion
        // This is a complex feature requiring:
        // - Date arithmetic (chrono or similar)
        // - Calendar integration (fiscal year logic)
        // - Filter generation (FieldFilter with date comparisons)
        return Err(TranslationError::SqlCompilationError {
            expression: format!("{:?}", period),
            error: "Period translation not yet implemented - requires calendar integration"
                .to_string(),
        });
    }

    Ok(None)
}

/// Translate filter expressions by compiling SQL with @atom substitution.
///
/// For now, this compiles the SQL expressions but does not parse them into
/// FieldFilter structures. That will require a SQL expression parser.
///
/// Future enhancement: Parse compiled SQL into FieldFilter with:
/// - field: FieldRef
/// - op: FilterOp (Eq, Gt, etc.)
/// - value: FilterValue
fn translate_filters(
    filters: &[crate::model::table::SqlExpr],
    from_table: &str,
    model: &Model,
) -> Result<Vec<String>, TranslationError> {
    let mut compiled_filters = Vec::new();

    for filter in filters {
        let compiled = compile_sql_expr(filter, from_table, model)?;
        compiled_filters.push(compiled);
    }

    Ok(compiled_filters)
}

/// Translate sort items from Report to SemanticQuery OrderField.
///
/// Maps column names (aliases) to OrderField with descending flag.
/// The column name references the alias of a field in the select list.
/// The entity is set to the from_table for proper field resolution.
fn translate_sort_items(
    sort_items: &[crate::model::SortItem],
    from_table: &str,
) -> Vec<OrderField> {
    sort_items
        .iter()
        .map(|item| OrderField {
            field: crate::semantic::planner::types::FieldRef::new(from_table, &item.column),
            descending: matches!(item.direction, crate::model::SortDirection::Desc),
        })
        .collect()
}

/// Translate an inline measure expression to a DerivedField.
///
/// Inline measures are SQL expressions that reference other measures by name.
/// Example: "revenue - cost" creates a derived measure from two base measures.
///
/// This is a skeleton implementation. Full implementation requires:
/// 1. SQL expression parser to parse "revenue - cost" into an AST
/// 2. AST transformer to convert to DerivedExpr tree
/// 3. Validation that referenced measures exist
///
/// For now, this returns an error indicating the feature is not yet implemented.
fn translate_inline_measure(
    _name: &str,
    expr: &crate::model::table::SqlExpr,
    _label: Option<String>,
) -> Result<DerivedField, TranslationError> {
    // TODO: Parse SQL expressions like "revenue - cost" into DerivedExpr::BinaryOp
    // This requires a SQL expression parser that can handle:
    // - Binary operations: +, -, *, /, %
    // - Unary operations: -, NOT
    // - Function calls: COALESCE, NULLIF, etc.
    // - Literals: numbers, strings, booleans
    // - Measure references: column names that resolve to measures
    //
    // The parser should build a DerivedExpr tree that can be used by the
    // semantic planner to generate the final SQL.

    Err(TranslationError::SqlCompilationError {
        expression: expr.sql.clone(),
        error: "Inline measure SQL parsing not yet implemented - needs DerivedExpr parser"
            .to_string(),
    })
}

/// Compile a SQL expression by replacing @atom references with qualified table.column.
///
/// Strategy:
/// 1. Use regex to replace @atom → qualified column (simple, reliable)
/// 2. Validate the result with sqlparser to ensure it's valid SQL
///
/// Note: sqlparser parses @atoms as valid SQL (SQL Server variable syntax), but
/// for our purposes, simple text replacement is clearer and more maintainable.
/// A future enhancement could use AST transformation if needed for complex cases.
///
/// Example: "@revenue * @quantity" → "dbo.fact_sales.revenue * dbo.fact_sales.quantity"
pub fn compile_sql_expr(
    expr: &crate::model::table::SqlExpr,
    from_table: &str,
    model: &Model,
) -> Result<String, TranslationError> {
    // Get the table
    let table =
        model
            .tables
            .get(from_table)
            .ok_or_else(|| TranslationError::UndefinedReference {
                entity_type: "table".to_string(),
                name: from_table.to_string(),
            })?;

    let mut result = expr.sql.clone();
    let mut atoms_found = Vec::new();

    // First pass: collect all @atoms and verify they exist
    for cap in ATOM_PATTERN.captures_iter(&expr.sql) {
        let atom_name = cap[1].to_string();

        // Verify atom exists in the table
        if !table.atoms.contains_key(&atom_name) {
            return Err(TranslationError::SqlCompilationError {
                expression: expr.sql.clone(),
                error: format!("Undefined atom: @{}", atom_name),
            });
        }

        atoms_found.push(atom_name);
    }

    // Second pass: replace each @atom with qualified table.column
    for atom_name in atoms_found {
        let qualified = format!("{}.{}", table.source, atom_name);
        result = result.replace(&format!("@{}", atom_name), &qualified);
    }

    // Validate the compiled SQL using sqlparser
    // This ensures the result is syntactically valid SQL
    let dialect = GenericDialect {};
    let test_sql = format!("SELECT {}", result);

    Parser::parse_sql(&dialect, &test_sql).map_err(|e| TranslationError::SqlCompilationError {
        expression: expr.sql.clone(),
        error: format!("Invalid SQL after @atom compilation: {}", e),
    })?;

    Ok(result)
}
