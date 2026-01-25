//! Time Intelligence SQL Emitter
//!
//! Generates SQL for time intelligence functions like YTD, prior period,
//! rolling windows, and period-over-period comparisons.
//!
//! # Window Function Patterns
//!
//! - **YTD/QTD/MTD**: Cumulative sum partitioned by period boundary
//! - **Prior Period/Year**: LAG function with appropriate offset
//! - **Rolling Sum/Avg**: Moving window aggregation

use crate::expr::{col, func, lag_offset, sum, table_col, Expr, WindowExt, WindowFrame, WindowOrderBy};

use super::types::TimeFunction;

/// A qualified column reference with table alias and column name.
pub type QualifiedColumn<'a> = (&'a str, &'a str);

/// Time intelligence SQL emitter.
///
/// Generates window functions for temporal calculations.
pub struct TimeEmitter;

impl TimeEmitter {
    /// Emit SQL expression for a time function.
    ///
    /// The `measure_expr` is the resolved measure expression (e.g., `SUM(amount)`).
    /// The `group_by_columns` are the qualified dimension columns `(table_alias, column_name)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let time_fn = TimeFunction::YearToDate {
    ///     measure: "revenue".to_string(),
    ///     year_column: Some("year".to_string()),
    ///     period_column: Some("month".to_string()),
    ///     via: None,
    /// };
    ///
    /// let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("dates", "year"), ("dates", "month")]);
    /// // Generates: SUM(revenue) OVER (PARTITION BY dates.year ORDER BY dates.month ROWS UNBOUNDED PRECEDING)
    /// ```
    pub fn emit(time_fn: &TimeFunction, measure_expr: Expr, group_by_columns: &[QualifiedColumn]) -> Expr {
        match time_fn {
            TimeFunction::YearToDate {
                year_column,
                period_column,
                ..
            } => {
                // Find qualified columns matching year and period
                let year_col = Self::find_column(group_by_columns, year_column.as_deref().unwrap_or("year"));
                let period_col = Self::find_column(group_by_columns, period_column.as_deref().unwrap_or("month"));
                Self::emit_cumulative(measure_expr, year_col, period_col)
            }

            TimeFunction::QuarterToDate {
                year_column,
                quarter_column,
                period_column,
                ..
            } => {
                // Partition by year + quarter, order by period (month)
                let year_col = Self::find_column(group_by_columns, year_column.as_deref().unwrap_or("year"));
                let quarter_col = Self::find_column(group_by_columns, quarter_column.as_deref().unwrap_or("quarter"));
                let period_col = Self::find_column(group_by_columns, period_column.as_deref().unwrap_or("month"));

                sum(measure_expr)
                    .over()
                    .partition_by(vec![Self::to_expr(year_col), Self::to_expr(quarter_col)])
                    .order_by(vec![WindowOrderBy::asc(Self::to_expr(period_col))])
                    .rows_to_current()
                    .build()
            }

            TimeFunction::MonthToDate {
                year_column,
                month_column,
                day_column,
                ..
            } => {
                // Partition by year + month, order by day
                let year_col = Self::find_column(group_by_columns, year_column.as_deref().unwrap_or("year"));
                let month_col = Self::find_column(group_by_columns, month_column.as_deref().unwrap_or("month"));
                let day_col = Self::find_column(group_by_columns, day_column.as_deref().unwrap_or("day"));

                sum(measure_expr)
                    .over()
                    .partition_by(vec![Self::to_expr(year_col), Self::to_expr(month_col)])
                    .order_by(vec![WindowOrderBy::asc(Self::to_expr(day_col))])
                    .rows_to_current()
                    .build()
            }

            TimeFunction::PriorPeriod { periods_back, .. } => {
                // LAG with specified offset
                Self::emit_lag(measure_expr, *periods_back, group_by_columns)
            }

            TimeFunction::PriorYear { .. } => {
                // For monthly grain, prior year = 12 periods back
                // This is a simplification - in practice we'd need to detect grain
                Self::emit_lag(measure_expr, 12, group_by_columns)
            }

            TimeFunction::PriorQuarter { .. } => {
                // For monthly grain, prior quarter = 3 periods back
                Self::emit_lag(measure_expr, 3, group_by_columns)
            }

            TimeFunction::RollingSum { periods, .. } => {
                Self::emit_rolling_sum(measure_expr, *periods, group_by_columns)
            }

            TimeFunction::RollingAvg { periods, .. } => {
                Self::emit_rolling_avg(measure_expr, *periods, group_by_columns)
            }
        }
    }

    /// Find a qualified column by its column name.
    ///
    /// Returns the matching (table_alias, column_name) or a fallback with empty alias.
    fn find_column<'a>(columns: &[QualifiedColumn<'a>], name: &'a str) -> QualifiedColumn<'a> {
        columns
            .iter()
            .find(|(_, col_name)| *col_name == name)
            .copied()
            .unwrap_or(("", name))
    }

    /// Convert a qualified column to an Expr.
    fn to_expr(qc: QualifiedColumn) -> Expr {
        if qc.0.is_empty() {
            col(qc.1)
        } else {
            table_col(qc.0, qc.1)
        }
    }

    /// Emit year-to-date cumulative sum.
    fn emit_cumulative(measure_expr: Expr, year_col: QualifiedColumn, period_col: QualifiedColumn) -> Expr {
        sum(measure_expr)
            .over()
            .partition_by(vec![Self::to_expr(year_col)])
            .order_by(vec![WindowOrderBy::asc(Self::to_expr(period_col))])
            .rows_to_current()
            .build()
    }

    /// Emit LAG function for prior period access.
    fn emit_lag(measure_expr: Expr, periods_back: u32, group_by_columns: &[QualifiedColumn]) -> Expr {
        // Order by all time-related columns for proper chronological ordering
        // e.g., ORDER BY dates.year, dates.month ensures correct sequencing across year boundaries
        let order_cols = Self::build_order_by(group_by_columns);

        lag_offset(measure_expr, periods_back as i64)
            .over()
            .order_by(order_cols)
            .build()
    }

    /// Emit rolling sum over N periods.
    fn emit_rolling_sum(measure_expr: Expr, periods: u32, group_by_columns: &[QualifiedColumn]) -> Expr {
        let order_cols = Self::build_order_by(group_by_columns);

        // ROWS BETWEEN (periods-1) PRECEDING AND CURRENT ROW
        let frame = WindowFrame::rolling(periods);

        sum(measure_expr)
            .over()
            .order_by(order_cols)
            .frame(frame)
            .build()
    }

    /// Emit rolling average over N periods.
    fn emit_rolling_avg(measure_expr: Expr, periods: u32, group_by_columns: &[QualifiedColumn]) -> Expr {
        let order_cols = Self::build_order_by(group_by_columns);

        // ROWS BETWEEN (periods-1) PRECEDING AND CURRENT ROW
        let frame = WindowFrame::rolling(periods);

        // Use AVG function
        func("AVG", vec![measure_expr])
            .over()
            .order_by(order_cols)
            .frame(frame)
            .build()
    }

    /// Build ORDER BY clause from qualified group_by columns.
    ///
    /// Uses all columns for proper chronological ordering (e.g., dates.year, dates.month).
    fn build_order_by(group_by_columns: &[QualifiedColumn]) -> Vec<WindowOrderBy> {
        if group_by_columns.is_empty() {
            vec![WindowOrderBy::asc(col("period"))]
        } else {
            group_by_columns
                .iter()
                .map(|qc| WindowOrderBy::asc(Self::to_expr(*qc)))
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::Dialect;

    #[test]
    fn test_ytd_emission() {
        let time_fn = TimeFunction::YearToDate {
            measure: "revenue".to_string(),
            year_column: Some("year".to_string()),
            period_column: Some("month".to_string()),
            via: None,
        };

        let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("dates", "year"), ("dates", "month")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);

        assert!(sql.contains("SUM"), "Expected SUM, got: {}", sql);
        assert!(sql.contains("OVER"), "Expected OVER, got: {}", sql);
        assert!(sql.contains("PARTITION BY"), "Expected PARTITION BY, got: {}", sql);
        assert!(sql.contains("ORDER BY"), "Expected ORDER BY, got: {}", sql);
        assert!(sql.contains("ROWS"), "Expected ROWS, got: {}", sql);
        // Verify qualified column reference
        assert!(sql.contains("\"dates\".\"year\""), "Expected qualified year column, got: {}", sql);
    }

    #[test]
    fn test_qtd_emission() {
        let time_fn = TimeFunction::QuarterToDate {
            measure: "revenue".to_string(),
            year_column: Some("year".to_string()),
            quarter_column: Some("quarter".to_string()),
            period_column: Some("month".to_string()),
            via: None,
        };

        let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("d", "year"), ("d", "quarter"), ("d", "month")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);

        assert!(sql.contains("SUM"), "Expected SUM, got: {}", sql);
        assert!(sql.contains("PARTITION BY"), "Expected PARTITION BY, got: {}", sql);
        assert!(sql.contains("ORDER BY"), "Expected ORDER BY, got: {}", sql);
        // Verify qualified column reference
        assert!(sql.contains("\"d\".\"year\""), "Expected qualified year column, got: {}", sql);
    }

    #[test]
    fn test_prior_year_emission() {
        let time_fn = TimeFunction::PriorYear {
            measure: "revenue".to_string(),
            via: None,
        };

        let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("dates", "month")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);

        assert!(sql.contains("LAG"), "Expected LAG, got: {}", sql);
        assert!(sql.contains("12"), "Expected 12 periods back, got: {}", sql);
        assert!(sql.contains("OVER"), "Expected OVER, got: {}", sql);
        // Verify qualified column reference
        assert!(sql.contains("\"dates\".\"month\""), "Expected qualified month column, got: {}", sql);
    }

    #[test]
    fn test_prior_period_emission() {
        let time_fn = TimeFunction::PriorPeriod {
            measure: "revenue".to_string(),
            periods_back: 1,
            via: None,
        };

        let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("t", "month")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);

        assert!(sql.contains("LAG"), "Expected LAG, got: {}", sql);
        assert!(sql.contains(", 1)"), "Expected 1 period back, got: {}", sql);
        assert!(sql.contains("\"t\".\"month\""), "Expected qualified month column, got: {}", sql);
    }

    #[test]
    fn test_rolling_sum_emission() {
        let time_fn = TimeFunction::RollingSum {
            measure: "revenue".to_string(),
            periods: 3,
            via: None,
        };

        let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("d", "year"), ("d", "month")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);

        assert!(sql.contains("SUM"), "Expected SUM, got: {}", sql);
        assert!(sql.contains("OVER"), "Expected OVER, got: {}", sql);
        assert!(sql.contains("2 PRECEDING"), "Expected 2 PRECEDING, got: {}", sql);
        // Verify qualified column references in ORDER BY
        assert!(sql.contains("\"d\".\"year\""), "Expected qualified year column, got: {}", sql);
        assert!(sql.contains("\"d\".\"month\""), "Expected qualified month column, got: {}", sql);
    }

    #[test]
    fn test_rolling_avg_emission() {
        let time_fn = TimeFunction::RollingAvg {
            measure: "revenue".to_string(),
            periods: 6,
            via: None,
        };

        let expr = TimeEmitter::emit(&time_fn, col("revenue"), &[("dates", "month")]);
        let sql = expr.to_tokens().serialize(Dialect::Postgres);

        assert!(sql.contains("AVG"), "Expected AVG, got: {}", sql);
        assert!(sql.contains("OVER"), "Expected OVER, got: {}", sql);
        assert!(sql.contains("5 PRECEDING"), "Expected 5 PRECEDING, got: {}", sql);
        assert!(sql.contains("\"dates\".\"month\""), "Expected qualified month column, got: {}", sql);
    }
}
