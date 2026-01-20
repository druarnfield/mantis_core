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
                let levels: Vec<model::types::GrainLevel> = drill_path
                    .value
                    .levels
                    .into_iter()
                    .filter_map(|level_str| {
                        // Parse string level names to GrainLevel enums
                        ast::GrainLevel::from_str(&level_str.value)
                    })
                    .collect();

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
