//! Lowering DSL AST to semantic model.

use crate::dsl::ast;
use crate::dsl::span::{Span, Spanned};
use crate::model;
use crate::model::expr::Expr;

/// Lower DSL AST to semantic model.
pub fn lower(ast: ast::Model) -> Result<model::Model, LoweringError> {
    let mut model = model::Model {
        defaults: None,
        calendars: std::collections::HashMap::new(),
        dimensions: std::collections::HashMap::new(),
        tables: std::collections::HashMap::new(),
        measures: std::collections::HashMap::new(),
        reports: std::collections::HashMap::new(),
    };

    // Lower defaults
    if let Some(defaults_ast) = ast.defaults {
        model.defaults = Some(lower_defaults(defaults_ast)?);
    }

    // Lower items
    for item in ast.items {
        match item.value {
            ast::Item::Calendar(cal) => {
                let calendar = lower_calendar(cal)?;
                model.calendars.insert(calendar.name.clone(), calendar);
            }
            ast::Item::Dimension(dim) => {
                let dimension = lower_dimension(dim)?;
                model.dimensions.insert(dimension.name.clone(), dimension);
            }
            ast::Item::Table(tbl) => {
                let table = lower_table(tbl)?;
                model.tables.insert(table.name.clone(), table);
            }
            ast::Item::MeasureBlock(meas) => {
                let measure_block = lower_measure_block(meas, &model)?;
                model
                    .measures
                    .insert(measure_block.table_name.clone(), measure_block);
            }
            ast::Item::Report(rep) => {
                let report = lower_report(rep)?;
                model.reports.insert(report.name.clone(), report);
            }
        }
    }

    Ok(model)
}

fn lower_defaults(defaults: Spanned<ast::Defaults>) -> Result<model::Defaults, LoweringError> {
    let mut result = model::Defaults::default();

    for setting in defaults.value.settings {
        match setting.value {
            ast::DefaultSetting::Calendar(cal) => {
                result.calendar = Some(cal.value);
            }
            ast::DefaultSetting::FiscalYearStart(month) => {
                result.fiscal_year_start = Some(month.value);
            }
            ast::DefaultSetting::WeekStart(weekday) => {
                result.week_start = Some(weekday.value);
            }
            ast::DefaultSetting::NullHandling(nh) => {
                result.null_handling = nh.value;
            }
            ast::DefaultSetting::DecimalPlaces(dp) => {
                result.decimal_places = dp.value;
            }
        }
    }

    Ok(result)
}

fn lower_calendar(calendar: ast::Calendar) -> Result<model::Calendar, LoweringError> {
    let name = calendar.name.value;

    let body = match calendar.body.value {
        ast::CalendarBody::Physical(phys) => {
            let source = phys.source.value;

            // Convert grain mappings from Vec<Spanned<GrainMapping>> to HashMap
            let mut grain_mappings = std::collections::HashMap::new();
            for mapping in phys.grain_mappings {
                grain_mappings.insert(mapping.value.level.value, mapping.value.column.value);
            }

            // Convert drill paths
            let mut drill_paths = std::collections::HashMap::new();
            for drill_path in phys.drill_paths {
                let path_name = drill_path.value.name.value.clone();
                // Drill paths contain GrainLevel enums, not strings - need to convert
                let mut levels = Vec::new();
                for level_str in drill_path.value.levels {
                    match ast::GrainLevel::from_str(&level_str.value) {
                        Some(grain_level) => levels.push(grain_level),
                        None => {
                            return Err(LoweringError::InvalidGrainLevel {
                                calendar_name: name.clone(),
                                drill_path_name: path_name.clone(),
                                invalid_level: level_str.value,
                            });
                        }
                    }
                }

                drill_paths.insert(
                    path_name.clone(),
                    model::calendar::DrillPath {
                        name: path_name,
                        levels,
                    },
                );
            }

            model::CalendarBody::Physical(model::calendar::PhysicalCalendar {
                source,
                grain_mappings,
                drill_paths,
                fiscal_year_start: phys.fiscal_year_start.map(|s| s.value),
                week_start: phys.week_start.map(|s| s.value),
            })
        }
        ast::CalendarBody::Generated(gen) => {
            // Extract from/to from the range
            let (from, to) = match gen.range {
                Some(range) => match range.value {
                    ast::CalendarRange::Explicit { start, end } => {
                        (start.value.to_string(), end.value.to_string())
                    }
                    ast::CalendarRange::Infer { min, max } => {
                        // For inferred ranges, use min/max if provided, otherwise use placeholders
                        let from = min
                            .map(|d| d.value.to_string())
                            .unwrap_or_else(|| "INFER_MIN".to_string());
                        let to = max
                            .map(|d| d.value.to_string())
                            .unwrap_or_else(|| "INFER_MAX".to_string());
                        (from, to)
                    }
                },
                None => ("INFER_MIN".to_string(), "INFER_MAX".to_string()),
            };

            model::CalendarBody::Generated {
                grain: gen.base_grain.value,
                from,
                to,
            }
        }
    };

    Ok(model::Calendar { name, body })
}

fn lower_dimension(dimension: ast::Dimension) -> Result<model::Dimension, LoweringError> {
    let name = dimension.name.value;
    let source = dimension.source.value;
    let key = dimension.key.value;

    // Convert attributes from Vec<Spanned<Attribute>>
    let mut attributes = std::collections::HashMap::new();
    for attr in dimension.attributes {
        let attr_name = attr.value.name.value.clone();
        attributes.insert(
            attr_name.clone(),
            model::dimension::Attribute {
                name: attr_name,
                data_type: attr.value.data_type.value,
            },
        );
    }

    // Convert drill paths from Vec<Spanned<DrillPath>>
    let mut drill_paths = std::collections::HashMap::new();
    for drill_path in dimension.drill_paths {
        let path_name = drill_path.value.name.value.clone();
        drill_paths.insert(
            path_name.clone(),
            model::dimension::DimensionDrillPath {
                name: path_name,
                levels: drill_path
                    .value
                    .levels
                    .into_iter()
                    .map(|l| l.value)
                    .collect(),
            },
        );
    }

    Ok(model::Dimension {
        name,
        source,
        key,
        attributes,
        drill_paths,
    })
}

fn lower_table(table: ast::Table) -> Result<model::Table, LoweringError> {
    let name = table.name.value;
    let source = table.source.value;

    // Convert atoms from Vec<Spanned<Atom>>
    let mut atoms = std::collections::HashMap::new();
    for atom in table.atoms {
        let atom_name = atom.value.name.value.clone();
        atoms.insert(
            atom_name.clone(),
            model::table::Atom {
                name: atom_name,
                data_type: atom.value.atom_type.value,
            },
        );
    }

    // Convert times from Vec<Spanned<TimeBinding>>
    let mut times = std::collections::HashMap::new();
    for time in table.times {
        let time_name = time.value.name.value.clone();
        times.insert(
            time_name.clone(),
            model::table::TimeBinding {
                name: time_name,
                calendar: time.value.calendar.value,
                grain: time.value.grain.value,
            },
        );
    }

    // Convert slicers from Vec<Spanned<Slicer>>
    let mut slicers = std::collections::HashMap::new();
    for slicer in table.slicers {
        let slicer_name = slicer.value.name.value.clone();

        let model_slicer = match slicer.value.kind.value {
            ast::SlicerKind::Inline { data_type } => model::table::Slicer::Inline {
                name: slicer_name.clone(),
                data_type,
            },
            ast::SlicerKind::ForeignKey {
                dimension,
                key_column,
            } => model::table::Slicer::ForeignKey {
                name: slicer_name.clone(),
                dimension,
                key: key_column,
            },
            ast::SlicerKind::Via { fk_slicer } => model::table::Slicer::Via {
                name: slicer_name.clone(),
                fk_slicer,
            },
            ast::SlicerKind::Calculated { data_type, expr } => model::table::Slicer::Calculated {
                name: slicer_name.clone(),
                data_type,
                expr: expr.clone(),
            },
        };

        slicers.insert(slicer_name, model_slicer);
    }

    Ok(model::Table {
        name,
        source,
        atoms,
        times,
        slicers,
    })
}

/// Validate all atom references in an expression exist in the table.
fn validate_atom_refs(expr: &Expr, table: &model::Table, span: &Span) -> Result<(), LoweringError> {
    let atom_refs = expr.atom_refs();

    for atom_name in atom_refs {
        if !table.atoms.contains_key(&atom_name) {
            return Err(LoweringError::UndefinedAtom {
                atom: atom_name,
                table: table.name.clone(),
                span: span.clone(),
            });
        }
    }

    Ok(())
}

fn lower_measure_block(
    measure_block: ast::MeasureBlock,
    model: &model::Model,
) -> Result<model::MeasureBlock, LoweringError> {
    let table_name = measure_block.table.value.clone();

    // Get the table to validate atom references
    let table = model
        .tables
        .get(&table_name)
        .ok_or_else(|| LoweringError::UndefinedTable {
            name: table_name.clone(),
            span: measure_block.table.span.clone(),
        })?;

    // Convert measures from Vec<Spanned<Measure>>
    let mut measures = std::collections::HashMap::new();
    for measure in measure_block.measures {
        let measure_name = measure.value.name.value.clone();

        // Validate expression's atom references
        validate_atom_refs(&measure.value.expr.value, table, &measure.value.expr.span)?;

        // Validate filter's atom references (if present)
        if let Some(filter) = &measure.value.filter {
            validate_atom_refs(&filter.value, table, &filter.span)?;
        }

        measures.insert(
            measure_name.clone(),
            model::measure::Measure {
                name: measure_name,
                expr: measure.value.expr.value.clone(),
                filter: measure.value.filter.map(|f| f.value.clone()),
                null_handling: measure.value.null_handling.map(|nh| nh.value),
            },
        );
    }

    Ok(model::MeasureBlock {
        table_name,
        measures,
    })
}

fn lower_report(report: ast::Report) -> Result<model::Report, LoweringError> {
    let name = report.name.value;
    let from: Vec<String> = report.from.into_iter().map(|s| s.value).collect();
    let use_date: Vec<String> = report.use_date.into_iter().map(|s| s.value).collect();

    // Convert period
    let period = report
        .period
        .map(|p| lower_period_expr(p.value))
        .transpose()?;

    // Convert group items
    let mut group = Vec::new();
    for group_item in report.group {
        let model_group_item = match group_item.value {
            ast::GroupItem::DrillPathRef(drill_path_ref) => model::GroupItem::DrillPathRef {
                source: drill_path_ref.source,
                path: drill_path_ref.path,
                level: drill_path_ref.level,
                label: drill_path_ref.label,
            },
            ast::GroupItem::InlineSlicer { name } => {
                model::GroupItem::InlineSlicer { name, label: None }
            }
        };
        group.push(model_group_item);
    }

    // Convert show items
    let mut show = Vec::new();
    for show_item in report.show {
        let model_show_item = match show_item.value {
            ast::ShowItem::Measure { name, label } => model::ShowItem::Measure { name, label },
            ast::ShowItem::MeasureWithSuffix {
                name,
                suffix,
                label,
            } => model::ShowItem::MeasureWithSuffix {
                name,
                suffix: lower_time_suffix(suffix),
                label,
            },
            ast::ShowItem::InlineMeasure { name, expr, label } => model::ShowItem::InlineMeasure {
                name,
                expr: expr.clone(),
                label,
            },
        };
        show.push(model_show_item);
    }

    // Convert filters
    let mut filters = Vec::new();
    if let Some(filter) = report.filter {
        filters.push(filter.value.clone());
    }

    // Convert sort
    let mut sort = Vec::new();
    for sort_item in report.sort {
        sort.push(model::report::SortItem {
            column: sort_item.value.column,
            direction: match sort_item.value.direction {
                ast::SortDirection::Asc => model::report::SortDirection::Asc,
                ast::SortDirection::Desc => model::report::SortDirection::Desc,
            },
        });
    }

    let limit = report.limit.map(|l| l.value);

    Ok(model::Report {
        name,
        from,
        use_date,
        period,
        group,
        show,
        filters,
        sort,
        limit,
    })
}

fn lower_time_suffix(suffix: ast::TimeSuffix) -> model::TimeSuffix {
    match suffix {
        ast::TimeSuffix::Ytd => model::TimeSuffix::Ytd,
        ast::TimeSuffix::Qtd => model::TimeSuffix::Qtd,
        ast::TimeSuffix::Mtd => model::TimeSuffix::Mtd,
        ast::TimeSuffix::Wtd => model::TimeSuffix::Wtd,
        ast::TimeSuffix::FiscalYtd => model::TimeSuffix::FiscalYtd,
        ast::TimeSuffix::FiscalQtd => model::TimeSuffix::FiscalQtd,
        ast::TimeSuffix::PriorYear => model::TimeSuffix::PriorYear,
        ast::TimeSuffix::PriorQuarter => model::TimeSuffix::PriorQuarter,
        ast::TimeSuffix::PriorMonth => model::TimeSuffix::PriorMonth,
        ast::TimeSuffix::PriorWeek => model::TimeSuffix::PriorWeek,
        ast::TimeSuffix::YoyGrowth => model::TimeSuffix::YoyGrowth,
        ast::TimeSuffix::QoqGrowth => model::TimeSuffix::QoqGrowth,
        ast::TimeSuffix::MomGrowth => model::TimeSuffix::MomGrowth,
        ast::TimeSuffix::WowGrowth => model::TimeSuffix::WowGrowth,
        ast::TimeSuffix::YoyDelta => model::TimeSuffix::YoyDelta,
        ast::TimeSuffix::QoqDelta => model::TimeSuffix::QoqDelta,
        ast::TimeSuffix::MomDelta => model::TimeSuffix::MomDelta,
        ast::TimeSuffix::WowDelta => model::TimeSuffix::WowDelta,
        ast::TimeSuffix::Rolling3m => model::TimeSuffix::Rolling3m,
        ast::TimeSuffix::Rolling6m => model::TimeSuffix::Rolling6m,
        ast::TimeSuffix::Rolling12m => model::TimeSuffix::Rolling12m,
        ast::TimeSuffix::Rolling3mAvg => model::TimeSuffix::Rolling3mAvg,
        ast::TimeSuffix::Rolling6mAvg => model::TimeSuffix::Rolling6mAvg,
        ast::TimeSuffix::Rolling12mAvg => model::TimeSuffix::Rolling12mAvg,
    }
}

fn lower_period_expr(period: ast::PeriodExpr) -> Result<model::report::PeriodExpr, LoweringError> {
    match period {
        ast::PeriodExpr::Relative(rel) => Ok(model::report::PeriodExpr::Relative(
            lower_relative_period(rel),
        )),
        ast::PeriodExpr::Range { start, end } => Ok(model::report::PeriodExpr::Range {
            start: start.to_string(),
            end: end.to_string(),
        }),
        ast::PeriodExpr::Month { year, month } => {
            Ok(model::report::PeriodExpr::Month { year, month })
        }
        ast::PeriodExpr::Quarter { year, quarter } => {
            Ok(model::report::PeriodExpr::Quarter { year, quarter })
        }
        ast::PeriodExpr::Year { year } => Ok(model::report::PeriodExpr::Year { year }),
    }
}

fn lower_relative_period(rel: ast::RelativePeriod) -> model::report::RelativePeriod {
    match rel {
        ast::RelativePeriod::Today => model::report::RelativePeriod::Today,
        ast::RelativePeriod::Yesterday => model::report::RelativePeriod::Yesterday,
        ast::RelativePeriod::ThisWeek => model::report::RelativePeriod::ThisWeek,
        ast::RelativePeriod::ThisMonth => model::report::RelativePeriod::ThisMonth,
        ast::RelativePeriod::ThisQuarter => model::report::RelativePeriod::ThisQuarter,
        ast::RelativePeriod::ThisYear => model::report::RelativePeriod::ThisYear,
        ast::RelativePeriod::LastWeek => model::report::RelativePeriod::LastWeek,
        ast::RelativePeriod::LastMonth => model::report::RelativePeriod::LastMonth,
        ast::RelativePeriod::LastQuarter => model::report::RelativePeriod::LastQuarter,
        ast::RelativePeriod::LastYear => model::report::RelativePeriod::LastYear,
        ast::RelativePeriod::Ytd => model::report::RelativePeriod::Ytd,
        ast::RelativePeriod::Qtd => model::report::RelativePeriod::Qtd,
        ast::RelativePeriod::Mtd => model::report::RelativePeriod::Mtd,
        ast::RelativePeriod::ThisFiscalYear => model::report::RelativePeriod::ThisFiscalYear,
        ast::RelativePeriod::LastFiscalYear => model::report::RelativePeriod::LastFiscalYear,
        ast::RelativePeriod::ThisFiscalQuarter => model::report::RelativePeriod::ThisFiscalQuarter,
        ast::RelativePeriod::LastFiscalQuarter => model::report::RelativePeriod::LastFiscalQuarter,
        ast::RelativePeriod::FiscalYtd => model::report::RelativePeriod::FiscalYtd,
        ast::RelativePeriod::Trailing { count, unit } => model::report::RelativePeriod::Trailing {
            count,
            unit: lower_period_unit(unit),
        },
    }
}

fn lower_period_unit(unit: ast::PeriodUnit) -> model::report::PeriodUnit {
    match unit {
        ast::PeriodUnit::Days => model::report::PeriodUnit::Days,
        ast::PeriodUnit::Weeks => model::report::PeriodUnit::Weeks,
        ast::PeriodUnit::Months => model::report::PeriodUnit::Months,
        ast::PeriodUnit::Quarters => model::report::PeriodUnit::Quarters,
        ast::PeriodUnit::Years => model::report::PeriodUnit::Years,
    }
}

#[derive(Debug, Clone)]
pub enum LoweringError {
    NotImplemented(String),
    InvalidGrainLevel {
        calendar_name: String,
        drill_path_name: String,
        invalid_level: String,
    },
    UndefinedAtom {
        atom: String,
        table: String,
        span: Span,
    },
    UndefinedTable {
        name: String,
        span: Span,
    },
}

impl std::fmt::Display for LoweringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoweringError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
            LoweringError::InvalidGrainLevel {
                calendar_name,
                drill_path_name,
                invalid_level,
            } => write!(
                f,
                "Invalid grain level '{}' in drill path '{}' of calendar '{}'",
                invalid_level, drill_path_name, calendar_name
            ),
            LoweringError::UndefinedAtom { atom, table, span } => {
                write!(
                    f,
                    "Undefined atom '@{}' in table '{}' at {:?}",
                    atom, table, span
                )
            }
            LoweringError::UndefinedTable { name, span } => {
                write!(f, "Undefined table '{}' at {:?}", name, span)
            }
        }
    }
}

impl std::error::Error for LoweringError {}
