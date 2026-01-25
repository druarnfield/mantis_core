//! Pivot Report Planner.
//!
//! Plans pivot reports with row/column dimensions and value measures.

use crate::model::{Model, PivotColumns, PivotReport, PivotValue};
use crate::semantic::error::{PlanError, PlanResult};
use crate::semantic::model_graph::ModelGraph;

use super::planner::aggregation_to_sql;

/// Planned pivot report - ready for SQL emission.
#[derive(Debug, Clone)]
pub struct PivotPlan {
    /// The pivot report being planned.
    pub report_name: String,

    /// Row dimensions (left side of pivot).
    pub row_dimensions: Vec<PivotDimension>,

    /// Column dimension (values become column headers).
    pub column_dimension: PivotDimension,

    /// Pivot column values (explicit or dynamic).
    pub column_values: PivotColumnValues,

    /// Values/measures to aggregate.
    pub value_measures: Vec<PivotMeasure>,

    /// Filters to apply.
    pub filters: Vec<String>,

    /// Totals configuration.
    pub totals: PivotTotals,

    /// Sort configuration.
    pub sort: Option<PivotSortPlan>,

    /// The source fact for joins.
    pub source_fact: String,

    /// Fact's physical table.
    pub source_table: String,
}

/// A dimension used in the pivot.
#[derive(Debug, Clone)]
pub struct PivotDimension {
    /// Entity name (e.g., "customers").
    pub entity: String,
    /// Column name (e.g., "region").
    pub column: String,
    /// Physical table (for joins).
    pub physical_table: Option<String>,
}

/// Pivot column values.
#[derive(Debug, Clone)]
pub enum PivotColumnValues {
    /// Dynamic: query distinct values at runtime.
    Dynamic,
    /// Explicit: fixed list of values.
    Explicit(Vec<String>),
}

/// A measure in the pivot.
#[derive(Debug, Clone)]
pub struct PivotMeasure {
    /// Output alias.
    pub alias: String,
    /// Aggregation type.
    pub aggregation: String,
    /// Source column expression.
    pub source_expr: String,
}

/// Totals configuration.
#[derive(Debug, Clone, Default)]
pub struct PivotTotals {
    pub rows: bool,
    pub columns: bool,
    pub grand: bool,
}

/// Sort configuration.
#[derive(Debug, Clone)]
pub struct PivotSortPlan {
    pub by_measure: String,
    pub descending: bool,
}

/// Planner for PivotReport entities.
pub struct PivotPlanner<'a> {
    model: &'a Model,
    #[allow(dead_code)]
    graph: &'a ModelGraph,
}

impl<'a> PivotPlanner<'a> {
    pub fn new(model: &'a Model, graph: &'a ModelGraph) -> Self {
        Self { model, graph }
    }

    /// Plan a pivot report into a PivotPlan.
    pub fn plan(&self, pivot: &PivotReport) -> PlanResult<PivotPlan> {
        // Determine source fact from values
        let source_fact = self.determine_source_fact(pivot)?;
        let fact = self.model.get_fact(&source_fact).ok_or_else(|| {
            PlanError::UnknownEntity(source_fact.clone())
        })?;

        // Parse row dimensions
        let row_dimensions: Vec<PivotDimension> = pivot
            .rows
            .iter()
            .map(|r| self.parse_dimension(r))
            .collect::<Result<_, _>>()?;

        // Parse column dimension
        let (column_dimension, column_values) = self.parse_column_dimension(&pivot.columns)?;

        // Parse value measures
        let value_measures = self.parse_value_measures(&source_fact, &pivot.values)?;

        // Parse totals
        let totals = match &pivot.totals {
            Some(t) => PivotTotals {
                rows: t.rows,
                columns: t.columns,
                grand: t.grand,
            },
            None => PivotTotals::default(),
        };

        // Parse sort
        let sort = pivot.sort.as_ref().map(|s| PivotSortPlan {
            by_measure: s.by.clone(),
            descending: matches!(s.direction, crate::model::SortDirection::Desc),
        });

        Ok(PivotPlan {
            report_name: pivot.name.clone(),
            row_dimensions,
            column_dimension,
            column_values,
            value_measures,
            filters: pivot.filters.clone(),
            totals,
            sort,
            source_fact,
            source_table: fact.target_table.clone(),
        })
    }

    fn determine_source_fact(&self, pivot: &PivotReport) -> PlanResult<String> {
        // Get fact from first measure's MeasureRef
        if let Some(first_value) = pivot.values.first() {
            return Ok(first_value.measure.fact.clone());
        }
        Err(PlanError::InvalidModel(
            "Pivot report must have at least one value measure".to_string(),
        ))
    }

    fn parse_dimension(&self, dim_ref: &str) -> PlanResult<PivotDimension> {
        let (entity, column) = dim_ref.split_once('.').ok_or_else(|| {
            PlanError::InvalidReference(format!(
                "Dimension reference must be 'entity.column': {}",
                dim_ref
            ))
        })?;

        Ok(PivotDimension {
            entity: entity.to_string(),
            column: column.to_string(),
            physical_table: self.get_physical_table(entity),
        })
    }

    fn parse_column_dimension(
        &self,
        columns: &PivotColumns,
    ) -> PlanResult<(PivotDimension, PivotColumnValues)> {
        match columns {
            PivotColumns::Dynamic(dim_ref) => {
                let dim = self.parse_dimension(dim_ref)?;
                Ok((dim, PivotColumnValues::Dynamic))
            }
            PivotColumns::Explicit { dimension, values } => {
                let dim = self.parse_dimension(dimension)?;
                Ok((dim, PivotColumnValues::Explicit(values.clone())))
            }
        }
    }

    fn parse_value_measures(
        &self,
        fact_name: &str,
        values: &[PivotValue],
    ) -> PlanResult<Vec<PivotMeasure>> {
        let fact = self.model.get_fact(fact_name).ok_or_else(|| {
            PlanError::UnknownEntity(fact_name.to_string())
        })?;

        let mut measures = Vec::new();
        for value in values {
            // Get measure name from MeasureRef
            let measure_name = &value.measure.measure;

            let measure_def = fact.measures.get(measure_name).ok_or_else(|| {
                PlanError::UnknownField {
                    entity: fact_name.to_string(),
                    field: measure_name.to_string(),
                }
            })?;

            measures.push(PivotMeasure {
                alias: value.name.clone(),
                aggregation: aggregation_to_sql(&measure_def.aggregation),
                source_expr: measure_def.source_column.clone(),
            });
        }

        Ok(measures)
    }

    fn get_physical_table(&self, entity: &str) -> Option<String> {
        if let Some(source) = self.model.get_source(entity) {
            return Some(source.table.clone());
        }
        if let Some(dim) = self.model.get_dimension(entity) {
            return Some(dim.target_table.clone());
        }
        if let Some(fact) = self.model.get_fact(entity) {
            return Some(fact.target_table.clone());
        }
        None
    }
}
