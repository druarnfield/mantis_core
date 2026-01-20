//! Lowering DSL AST to semantic model.

use crate::dsl::ast;
use crate::dsl::span::Spanned;
use crate::model;

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
                let measure_block = lower_measure_block(meas)?;
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

fn lower_defaults(_defaults: Spanned<ast::Defaults>) -> Result<model::Defaults, LoweringError> {
    // Placeholder - will implement properly later
    Ok(model::Defaults::default())
}

fn lower_calendar(calendar: ast::Calendar) -> Result<model::Calendar, LoweringError> {
    let name = calendar.name.value;

    let body = match calendar.body {
        ast::CalendarBody::Physical(phys) => {
            let source = phys.source.value;

            // Convert grain mappings
            let mut grain_mappings = std::collections::HashMap::new();
            for (grain_level, column) in phys.grain_mappings {
                grain_mappings.insert(grain_level, column.value);
            }

            // Convert drill paths
            let mut drill_paths = std::collections::HashMap::new();
            for drill_path in phys.drill_paths {
                let path_name = drill_path.name.value.clone();
                drill_paths.insert(
                    path_name.clone(),
                    model::calendar::DrillPath {
                        name: path_name,
                        levels: drill_path.levels.into_iter().map(|l| l.value).collect(),
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
        ast::CalendarBody::Generated(gen) => model::CalendarBody::Generated {
            grain: gen.grain.value,
            from: gen.from.value,
            to: gen.to.value,
        },
    };

    Ok(model::Calendar { name, body })
}

fn lower_dimension(_dimension: ast::Dimension) -> Result<model::Dimension, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("Dimension".to_string()))
}

fn lower_table(_table: ast::Table) -> Result<model::Table, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("Table".to_string()))
}

fn lower_measure_block(
    _measure_block: ast::MeasureBlock,
) -> Result<model::MeasureBlock, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("MeasureBlock".to_string()))
}

fn lower_report(_report: ast::Report) -> Result<model::Report, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("Report".to_string()))
}

#[derive(Debug, Clone)]
pub enum LoweringError {
    NotImplemented(String),
}

impl std::fmt::Display for LoweringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoweringError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
        }
    }
}

impl std::error::Error for LoweringError {}
