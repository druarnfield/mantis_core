//! Multi-fact query emitter - generates symmetric aggregate pattern.
//!
//! For multi-fact queries (measures from multiple facts), this generates:
//!
//! ```sql
//! WITH orders_agg AS (
//!     SELECT date_id, customer_id, SUM(revenue) as revenue
//!     FROM fact_orders
//!     GROUP BY date_id, customer_id
//! ),
//! returns_agg AS (
//!     SELECT date_id, customer_id, SUM(return_amount) as return_amount
//!     FROM fact_returns
//!     GROUP BY date_id, customer_id
//! )
//! SELECT
//!     d.month,
//!     c.region,
//!     COALESCE(o.revenue, 0) as revenue,
//!     COALESCE(r.return_amount, 0) as return_amount
//! FROM orders_agg o
//! FULL OUTER JOIN returns_agg r
//!     ON o.date_id = r.date_id AND o.customer_id = r.customer_id
//! JOIN date d ON COALESCE(o.date_id, r.date_id) = d.date_id
//! JOIN customers c ON COALESCE(o.customer_id, r.customer_id) = c.customer_id
//! ORDER BY d.month
//! LIMIT 100
//! ```

use crate::expr::{
    coalesce, count_distinct, count_star, func, lit_int, max, min, sum, table_col, Expr, ExprExt,
};
use crate::model::AggregationType;
use crate::query::{Cte, OrderByExpr, Query, SelectExpr, TableRef};

use super::resolved::{
    FactAggregate, MultiFactQuery, ResolvedMeasure, ResolvedOrderExpr, SharedDimension,
};
#[cfg(test)]
use super::resolved::ResolvedColumn;
use super::types::{FilterOp, FilterValue};

/// Emitter for multi-fact queries.
pub struct MultiFactEmitter<'a> {
    query: &'a MultiFactQuery,
}

impl<'a> MultiFactEmitter<'a> {
    pub fn new(query: &'a MultiFactQuery) -> Self {
        Self { query }
    }

    /// Emit the complete SQL query.
    pub fn emit(&self) -> Query {
        // 1. Generate CTEs for each fact
        let ctes = self.emit_ctes();

        // 2. Build the main query
        let mut main_query = self.emit_main_query();

        // 3. Add CTEs to the query
        for cte in ctes {
            main_query = main_query.with_cte(cte);
        }

        // 4. Add ORDER BY
        let order_exprs: Vec<OrderByExpr> = self
            .query
            .order_by
            .iter()
            .map(|o| {
                let expr = match &o.expr {
                    ResolvedOrderExpr::Column(col) => table_col("", &col.physical_name),
                    ResolvedOrderExpr::Measure(m) => table_col("", &m.name),
                };
                if o.descending {
                    OrderByExpr::desc(expr)
                } else {
                    OrderByExpr::asc(expr)
                }
            })
            .collect();

        if !order_exprs.is_empty() {
            main_query = main_query.order_by(order_exprs);
        }

        // 5. Add LIMIT
        if let Some(limit) = self.query.limit {
            main_query = main_query.limit(limit);
        }

        main_query
    }

    /// Generate CTEs for each fact aggregate.
    fn emit_ctes(&self) -> Vec<Cte> {
        self.query
            .fact_aggregates
            .iter()
            .map(|fa| self.emit_fact_cte(fa))
            .collect()
    }

    /// Generate a CTE for a single fact.
    fn emit_fact_cte(&self, fact_agg: &FactAggregate) -> Cte {
        let table_ref = if let Some(ref schema) = fact_agg.fact.physical_schema {
            TableRef::new(&format!("{}.{}", schema, fact_agg.fact.physical_table))
                .with_alias(&fact_agg.fact.name)
        } else {
            TableRef::new(&fact_agg.fact.physical_table).with_alias(&fact_agg.fact.name)
        };

        let mut subquery = Query::new().from(table_ref);

        // Collect SELECT and GROUP BY expressions
        let mut select_exprs: Vec<SelectExpr> = Vec::new();
        let mut group_exprs: Vec<Expr> = Vec::new();

        // Add join key columns to SELECT and GROUP BY
        for key in &fact_agg.join_keys {
            if !key.fact_column.is_empty() {
                let col_expr = table_col(&fact_agg.fact.name, &key.fact_column);
                select_exprs.push(SelectExpr::new(col_expr.clone()).with_alias(&key.fact_column));
                group_exprs.push(col_expr);
            }
        }

        // Add measures
        for measure in &fact_agg.measures {
            let agg_expr = self.emit_aggregate_expr(measure, &fact_agg.fact.name);
            select_exprs.push(SelectExpr::new(agg_expr).with_alias(&measure.name));
        }

        subquery = subquery.select(select_exprs);

        if !group_exprs.is_empty() {
            subquery = subquery.group_by(group_exprs);
        }

        // Add filters
        for filter in &fact_agg.fact_filters {
            let filter_expr = self.emit_filter_expr(filter, &fact_agg.fact.name);
            subquery = subquery.filter(filter_expr);
        }

        // Add global filters
        for filter in &self.query.global_filters {
            let filter_expr = self.emit_filter_expr(filter, &fact_agg.fact.name);
            subquery = subquery.filter(filter_expr);
        }

        Cte::new(&fact_agg.cte_alias, subquery)
    }

    /// Generate the main query with FULL OUTER JOINs.
    fn emit_main_query(&self) -> Query {
        let cte_aliases: Vec<_> = self.query.cte_aliases().into_iter().collect();

        // Start from first CTE
        let first_alias = cte_aliases[0];
        let mut query = Query::new().from(TableRef::new(first_alias).with_alias(first_alias));

        // FULL OUTER JOIN remaining CTEs
        for (i, cte_alias) in cte_aliases.iter().skip(1).enumerate() {
            let join_condition = self.emit_cte_join_condition(cte_aliases[i], cte_alias);
            query = query.full_join(
                TableRef::new(cte_alias).with_alias(cte_alias),
                join_condition,
            );
        }

        // JOIN dimension tables
        for dim in &self.query.shared_dimensions {
            let join_condition = self.emit_dimension_join_condition(dim, &cte_aliases);
            let dim_ref = if let Some(ref schema) = dim.dimension.physical_schema {
                TableRef::new(&format!("{}.{}", schema, dim.dimension.physical_table))
                    .with_alias(&dim.dimension.name)
            } else {
                TableRef::new(&dim.dimension.physical_table).with_alias(&dim.dimension.name)
            };
            query = query.inner_join(dim_ref, join_condition);
        }

        // Build SELECT expressions
        let mut select_exprs: Vec<SelectExpr> = Vec::new();

        // SELECT dimension columns
        for dim in &self.query.shared_dimensions {
            for col in &dim.columns {
                let col_expr = table_col(&dim.dimension.name, &col.physical_name);
                select_exprs.push(SelectExpr::new(col_expr).with_alias(&col.logical_name));
            }
        }

        // SELECT measures with COALESCE
        for fact_agg in &self.query.fact_aggregates {
            for measure in &fact_agg.measures {
                let measure_col = table_col(&fact_agg.cte_alias, &measure.name);
                let coalesced = coalesce(vec![measure_col, lit_int(0)]);
                select_exprs.push(SelectExpr::new(coalesced).with_alias(&measure.name));
            }
        }

        query.select(select_exprs)
    }

    /// Generate join condition between two CTEs.
    fn emit_cte_join_condition(&self, left_cte: &str, right_cte: &str) -> Expr {
        let mut conditions: Vec<Expr> = Vec::new();

        for dim in &self.query.shared_dimensions {
            // Find the join keys for left and right CTEs
            let left_key = self.find_cte_join_key(dim, left_cte);
            let right_key = self.find_cte_join_key(dim, right_cte);

            if let (Some(lk), Some(rk)) = (left_key, right_key) {
                if !lk.fact_column.is_empty() && !rk.fact_column.is_empty() {
                    let left_col = table_col(left_cte, &lk.fact_column);
                    let right_col = table_col(right_cte, &rk.fact_column);
                    conditions.push(left_col.eq(right_col));
                }
            }
        }

        if conditions.is_empty() {
            lit_int(1).eq(lit_int(1)) // True condition
        } else {
            conditions.into_iter().reduce(|a, b| a.and(b)).unwrap()
        }
    }

    /// Find the join key for a CTE in a shared dimension.
    fn find_cte_join_key<'b>(
        &self,
        dim: &'b SharedDimension,
        cte_alias: &str,
    ) -> Option<&'b super::resolved::FactJoinKey> {
        dim.paths.iter().find_map(|(fact_name, key)| {
            self.query
                .fact_aggregates
                .iter()
                .find(|fa| fa.cte_alias == cte_alias && fa.fact.name == *fact_name)
                .map(|_| key)
        })
    }

    /// Generate join condition for a dimension table.
    fn emit_dimension_join_condition(
        &self,
        dim: &SharedDimension,
        cte_aliases: &[&str],
    ) -> Expr {
        // Build COALESCE of all CTE join keys
        let coalesce_args: Vec<Expr> = cte_aliases
            .iter()
            .filter_map(|cte_alias| {
                self.find_cte_join_key(dim, cte_alias)
                    .filter(|key| !key.fact_column.is_empty())
                    .map(|key| table_col(cte_alias, &key.fact_column))
            })
            .collect();

        if coalesce_args.is_empty() || dim.paths.is_empty() {
            return lit_int(1).eq(lit_int(1)); // True condition
        }

        let dim_column = &dim.paths[0].1.dimension_column;
        let coalesce_expr = if coalesce_args.len() == 1 {
            coalesce_args.into_iter().next().unwrap()
        } else {
            coalesce(coalesce_args)
        };

        coalesce_expr.eq(table_col(&dim.dimension.name, dim_column))
    }

    /// Generate aggregate expression for a measure.
    fn emit_aggregate_expr(&self, measure: &ResolvedMeasure, entity_alias: &str) -> Expr {
        match measure.aggregation {
            AggregationType::Sum => sum(table_col(entity_alias, &measure.source_column)),
            AggregationType::Count => {
                if measure.source_column == "*" {
                    count_star()
                } else {
                    func("COUNT", vec![table_col(entity_alias, &measure.source_column)])
                }
            }
            AggregationType::CountDistinct => {
                count_distinct(table_col(entity_alias, &measure.source_column))
            }
            AggregationType::Avg => func("AVG", vec![table_col(entity_alias, &measure.source_column)]),
            AggregationType::Min => min(table_col(entity_alias, &measure.source_column)),
            AggregationType::Max => max(table_col(entity_alias, &measure.source_column)),
        }
    }

    /// Emit a filter expression.
    fn emit_filter_expr(
        &self,
        filter: &super::resolved::ResolvedFilter,
        entity_alias: &str,
    ) -> Expr {
        let col_expr = table_col(entity_alias, &filter.column.physical_name);
        emit_filter_op(col_expr, filter.op, &filter.value)
    }
}

/// Apply a filter operator to an expression.
pub fn emit_filter_op(col: Expr, op: FilterOp, value: &FilterValue) -> Expr {
    match op {
        FilterOp::Eq => col.eq(emit_filter_value(value)),
        FilterOp::Ne => col.ne(emit_filter_value(value)),
        FilterOp::Gt => col.gt(emit_filter_value(value)),
        FilterOp::Gte => col.gte(emit_filter_value(value)),
        FilterOp::Lt => col.lt(emit_filter_value(value)),
        FilterOp::Lte => col.lte(emit_filter_value(value)),
        FilterOp::Like => col.like(emit_filter_value(value)),
        FilterOp::In => {
            if let FilterValue::List(values) = value {
                let exprs: Vec<Expr> = values.iter().map(emit_filter_value).collect();
                col.in_list(exprs)
            } else {
                col.eq(emit_filter_value(value))
            }
        }
        FilterOp::IsNull => col.is_null(),
        FilterOp::IsNotNull => col.is_not_null(),
    }
}

/// Convert a filter value to an expression.
fn emit_filter_value(value: &FilterValue) -> Expr {
    use crate::expr::{lit_bool, lit_float, lit_null, lit_str};

    match value {
        FilterValue::String(s) => lit_str(s),
        FilterValue::Int(i) => lit_int(*i),
        FilterValue::Float(f) => lit_float(*f),
        FilterValue::Bool(b) => lit_bool(*b),
        FilterValue::Null => lit_null(),
        FilterValue::List(_) => lit_null(), // Lists handled specially in emit_filter_op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::Dialect;
    use crate::model::AggregationType;
    use crate::semantic::planner::resolved::{FactJoinKey, ResolvedEntity};

    fn sample_multi_fact_query() -> MultiFactQuery {
        MultiFactQuery {
            fact_aggregates: vec![
                FactAggregate {
                    fact: ResolvedEntity {
                        name: "orders".into(),
                        physical_table: "fact_orders".into(),
                        physical_schema: None,
                        materialized: true,
                    },
                    cte_alias: "orders_agg".into(),
                    join_keys: vec![FactJoinKey {
                        fact_column: "date_id".into(),
                        dimension: "date".into(),
                        dimension_column: "date_id".into(),
                    }],
                    measures: vec![ResolvedMeasure {
                        entity_alias: "orders".into(),
                        name: "revenue".into(),
                        aggregation: AggregationType::Sum,
                        source_column: "amount".into(),
                        filter: None,
                        definition_filter: None,
                    }],
                    fact_filters: vec![],
                },
                FactAggregate {
                    fact: ResolvedEntity {
                        name: "returns".into(),
                        physical_table: "fact_returns".into(),
                        physical_schema: None,
                        materialized: true,
                    },
                    cte_alias: "returns_agg".into(),
                    join_keys: vec![FactJoinKey {
                        fact_column: "date_id".into(),
                        dimension: "date".into(),
                        dimension_column: "date_id".into(),
                    }],
                    measures: vec![ResolvedMeasure {
                        entity_alias: "returns".into(),
                        name: "return_amount".into(),
                        aggregation: AggregationType::Sum,
                        source_column: "amount".into(),
                        filter: None,
                        definition_filter: None,
                    }],
                    fact_filters: vec![],
                },
            ],
            shared_dimensions: vec![SharedDimension {
                dimension: ResolvedEntity {
                    name: "date".into(),
                    physical_table: "dim_date".into(),
                    physical_schema: None,
                    materialized: true,
                },
                columns: vec![ResolvedColumn {
                    entity_alias: "date".into(),
                    logical_name: "month".into(),
                    physical_name: "month".into(),
                }],
                paths: vec![
                    (
                        "orders".into(),
                        FactJoinKey {
                            fact_column: "date_id".into(),
                            dimension: "date".into(),
                            dimension_column: "date_id".into(),
                        },
                    ),
                    (
                        "returns".into(),
                        FactJoinKey {
                            fact_column: "date_id".into(),
                            dimension: "date".into(),
                            dimension_column: "date_id".into(),
                        },
                    ),
                ],
            }],
            global_filters: vec![],
            order_by: vec![],
            limit: Some(100),
        }
    }

    #[test]
    fn test_multi_fact_emitter_generates_ctes() {
        let mfq = sample_multi_fact_query();
        let emitter = MultiFactEmitter::new(&mfq);
        let query = emitter.emit();
        let sql = query.to_sql(Dialect::Postgres);

        // Should have CTEs
        assert!(sql.contains("WITH"), "Should have WITH clause. SQL: {}", sql);
        assert!(sql.contains("orders_agg"), "Should have orders_agg CTE. SQL: {}", sql);
        assert!(sql.contains("returns_agg"), "Should have returns_agg CTE. SQL: {}", sql);
    }

    #[test]
    fn test_multi_fact_emitter_generates_full_outer_join() {
        let mfq = sample_multi_fact_query();
        let emitter = MultiFactEmitter::new(&mfq);
        let query = emitter.emit();
        let sql = query.to_sql(Dialect::Postgres);

        // Should have FULL OUTER JOIN
        assert!(
            sql.contains("FULL OUTER JOIN"),
            "Should have FULL OUTER JOIN. SQL: {}",
            sql
        );
    }

    #[test]
    fn test_multi_fact_emitter_generates_coalesce() {
        let mfq = sample_multi_fact_query();
        let emitter = MultiFactEmitter::new(&mfq);
        let query = emitter.emit();
        let sql = query.to_sql(Dialect::Postgres);

        // Should have COALESCE for measures
        assert!(sql.contains("COALESCE"), "Should have COALESCE. SQL: {}", sql);
    }

    #[test]
    fn test_multi_fact_emitter_generates_limit() {
        let mfq = sample_multi_fact_query();
        let emitter = MultiFactEmitter::new(&mfq);
        let query = emitter.emit();
        let sql = query.to_sql(Dialect::Postgres);

        // Should have LIMIT
        assert!(sql.contains("LIMIT 100"), "Should have LIMIT. SQL: {}", sql);
    }
}
