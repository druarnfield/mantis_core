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

fn lower_calendar(_calendar: ast::Calendar) -> Result<model::Calendar, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("Calendar".to_string()))
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
