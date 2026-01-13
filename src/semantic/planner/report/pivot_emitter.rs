//! Pivot SQL Emitter.
//!
//! Generates dialect-specific SQL for pivot operations.

use crate::dialect::Dialect;
use crate::semantic::error::{PlanError, PlanResult};

use super::pivot_planner::{PivotColumnValues, PivotPlan};

/// Emitter for Pivot SQL generation.
///
/// Generates dialect-specific SQL for pivot operations.
pub struct PivotEmitter {
    #[allow(dead_code)]
    default_schema: String,
}

impl Default for PivotEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl PivotEmitter {
    pub fn new() -> Self {
        Self {
            default_schema: "dbo".to_string(),
        }
    }

    pub fn with_default_schema(mut self, schema: &str) -> Self {
        self.default_schema = schema.to_string();
        self
    }

    /// Emit SQL for a pivot plan.
    ///
    /// Returns raw SQL string because pivot syntax varies significantly
    /// between dialects and doesn't fit well into the Query builder.
    pub fn emit(&self, plan: &PivotPlan, dialect: Dialect) -> PlanResult<String> {
        match dialect {
            Dialect::DuckDb => self.emit_duckdb(plan),
            Dialect::Postgres => self.emit_postgres(plan),
            Dialect::TSql => self.emit_tsql(plan),
            Dialect::MySql => self.emit_postgres(plan), // MySQL uses conditional aggregation
            Dialect::Snowflake => self.emit_duckdb(plan), // Snowflake has native PIVOT similar to DuckDB
            Dialect::Databricks => self.emit_duckdb(plan), // Databricks has native PIVOT similar to DuckDB
            Dialect::BigQuery => self.emit_postgres(plan), // BigQuery uses conditional aggregation
            Dialect::Redshift => self.emit_postgres(plan), // Redshift uses conditional aggregation
        }
    }

    /// Emit DuckDB PIVOT syntax.
    fn emit_duckdb(&self, plan: &PivotPlan) -> PlanResult<String> {
        let mut sql = String::new();

        // Build source subquery
        sql.push_str("PIVOT (\n    SELECT\n");

        // Row dimensions
        for (i, dim) in plan.row_dimensions.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }
            sql.push_str(&format!("        {}.{}", dim.entity, dim.column));
        }

        // Column dimension
        sql.push_str(&format!(
            ",\n        {}.{} AS pivot_col",
            plan.column_dimension.entity, plan.column_dimension.column
        ));

        // Measures
        for measure in &plan.value_measures {
            sql.push_str(&format!(",\n        {}", measure.source_expr));
        }

        sql.push_str(&format!("\n    FROM {}", plan.source_table));

        // TODO: Add JOINs for dimensions

        // Filters
        if !plan.filters.is_empty() {
            sql.push_str("\n    WHERE ");
            sql.push_str(&plan.filters.join(" AND "));
        }

        sql.push_str("\n)\n");

        // ON clause for pivot
        sql.push_str("ON pivot_col\n");

        // USING clause with aggregations
        sql.push_str("USING ");
        let usings: Vec<String> = plan
            .value_measures
            .iter()
            .map(|m| format!("{}({}) AS {}", m.aggregation, m.source_expr, m.alias))
            .collect();
        sql.push_str(&usings.join(", "));

        // GROUP BY row dimensions
        if !plan.row_dimensions.is_empty() {
            sql.push_str("\nGROUP BY ");
            let group_cols: Vec<String> = plan
                .row_dimensions
                .iter()
                .map(|d| format!("{}.{}", d.entity, d.column))
                .collect();
            sql.push_str(&group_cols.join(", "));
        }

        Ok(sql)
    }

    /// Emit PostgreSQL conditional aggregation syntax.
    fn emit_postgres(&self, plan: &PivotPlan) -> PlanResult<String> {
        let column_values = match &plan.column_values {
            PivotColumnValues::Explicit(values) => values.clone(),
            PivotColumnValues::Dynamic => {
                return Err(PlanError::InvalidModel(
                    "PostgreSQL pivot requires explicit column values".to_string(),
                ));
            }
        };

        let mut sql = String::new();
        sql.push_str("SELECT\n");

        // Row dimensions
        for (i, dim) in plan.row_dimensions.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }
            sql.push_str(&format!("    {}.{}", dim.entity, dim.column));
        }

        // Generate CASE WHEN for each column value and measure
        for value in &column_values {
            for measure in &plan.value_measures {
                sql.push_str(&format!(
                    ",\n    {}(CASE WHEN {}.{} = '{}' THEN {} END) AS {}_{} ",
                    measure.aggregation,
                    plan.column_dimension.entity,
                    plan.column_dimension.column,
                    value,
                    measure.source_expr,
                    value.replace("-", "_").replace(" ", "_"),
                    measure.alias
                ));
            }
        }

        // Total columns if requested
        if plan.totals.rows {
            for measure in &plan.value_measures {
                sql.push_str(&format!(
                    ",\n    {}({}) AS Total_{}",
                    measure.aggregation, measure.source_expr, measure.alias
                ));
            }
        }

        sql.push_str(&format!("\nFROM {}", plan.source_table));

        // TODO: Add JOINs for dimensions

        // Filters
        if !plan.filters.is_empty() {
            sql.push_str("\nWHERE ");
            sql.push_str(&plan.filters.join(" AND "));
        }

        // GROUP BY row dimensions
        if !plan.row_dimensions.is_empty() {
            sql.push_str("\nGROUP BY ");
            let group_cols: Vec<String> = plan
                .row_dimensions
                .iter()
                .map(|d| format!("{}.{}", d.entity, d.column))
                .collect();
            sql.push_str(&group_cols.join(", "));
        }

        // ORDER BY
        if let Some(sort) = &plan.sort {
            sql.push_str(&format!(
                "\nORDER BY Total_{} {}",
                sort.by_measure,
                if sort.descending { "DESC" } else { "ASC" }
            ));
        }

        Ok(sql)
    }

    /// Emit T-SQL PIVOT syntax.
    fn emit_tsql(&self, plan: &PivotPlan) -> PlanResult<String> {
        let column_values = match &plan.column_values {
            PivotColumnValues::Explicit(values) => values.clone(),
            PivotColumnValues::Dynamic => {
                return Err(PlanError::InvalidModel(
                    "T-SQL pivot requires explicit column values".to_string(),
                ));
            }
        };

        let mut sql = String::new();

        // SELECT clause with pivot columns
        sql.push_str("SELECT ");
        let row_cols: Vec<String> = plan
            .row_dimensions
            .iter()
            .map(|d| d.column.clone())
            .collect();
        sql.push_str(&row_cols.join(", "));

        // Bracketed column values
        for value in &column_values {
            sql.push_str(&format!(", [{}]", value));
        }

        // FROM subquery
        sql.push_str("\nFROM (\n    SELECT ");

        // Row dimensions
        for dim in &plan.row_dimensions {
            sql.push_str(&format!("{}.{}, ", dim.entity, dim.column));
        }

        // Pivot column
        sql.push_str(&format!(
            "{}.{}, ",
            plan.column_dimension.entity, plan.column_dimension.column
        ));

        // Value column (first measure)
        if let Some(first_measure) = plan.value_measures.first() {
            sql.push_str(&first_measure.source_expr);
        }

        sql.push_str(&format!("\n    FROM {}", plan.source_table));

        // Filters
        if !plan.filters.is_empty() {
            sql.push_str("\n    WHERE ");
            sql.push_str(&plan.filters.join(" AND "));
        }

        sql.push_str("\n) src\n");

        // PIVOT clause
        if let Some(first_measure) = plan.value_measures.first() {
            sql.push_str(&format!(
                "PIVOT ({}({}) FOR {} IN (",
                first_measure.aggregation, first_measure.source_expr, plan.column_dimension.column
            ));

            let bracketed: Vec<String> = column_values.iter().map(|v| format!("[{}]", v)).collect();
            sql.push_str(&bracketed.join(", "));
            sql.push_str(")) pvt");
        }

        // ORDER BY
        if let Some(sort) = &plan.sort {
            sql.push_str(&format!(
                "\nORDER BY {} {}",
                sort.by_measure,
                if sort.descending { "DESC" } else { "ASC" }
            ));
        }

        Ok(sql)
    }
}
