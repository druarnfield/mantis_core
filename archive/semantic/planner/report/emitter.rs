//! Report SQL Emitter.
//!
//! Converts a ReportPlan into SQL with CTEs and FULL OUTER JOIN.

use crate::expr::{coalesce, col, table_col, Expr, ExprExt};
use crate::query::{Cte, Query, SelectExpr, TableRef};
use crate::semantic::error::{PlanError, PlanResult};

use super::planner::{FactCte, ReportPlan};

/// Emitter for Report SQL generation.
///
/// Converts a ReportPlan into a SQL Query with CTEs.
pub struct ReportEmitter {
    default_schema: String,
}

impl Default for ReportEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl ReportEmitter {
    pub fn new() -> Self {
        Self {
            default_schema: "dbo".to_string(),
        }
    }

    pub fn with_default_schema(mut self, schema: &str) -> Self {
        self.default_schema = schema.to_string();
        self
    }

    /// Emit a SQL Query from a ReportPlan.
    ///
    /// Generates:
    /// 1. A CTE for each fact with measures
    /// 2. A main query joining CTEs with FULL OUTER JOIN
    /// 3. COALESCE for group_by columns from all CTEs
    pub fn emit(&self, plan: &ReportPlan) -> PlanResult<Query> {
        if plan.fact_ctes.is_empty() {
            return Err(PlanError::InvalidModel(
                "Report plan has no fact CTEs".to_string(),
            ));
        }

        let mut query = Query::new();

        // Build CTEs for each fact
        for cte in &plan.fact_ctes {
            let cte_query = self.build_cte_query(cte, &plan.group_by)?;
            query = query.with_cte(Cte::new(&cte.cte_name, cte_query));
        }

        // Build main query
        // FROM first_cte
        // FULL OUTER JOIN remaining CTEs on group_by columns
        let first_cte = &plan.fact_ctes[0];
        query = query.from(TableRef::new(&first_cte.cte_name));

        // Add FULL OUTER JOINs for remaining CTEs
        for i in 1..plan.fact_ctes.len() {
            let cte = &plan.fact_ctes[i];
            let prev_cte = &plan.fact_ctes[i - 1];

            // Build ON clause: cte1.group_col = cte2.group_col AND ...
            let on_expr = self.build_join_condition(&plan.group_by, &prev_cte.cte_name, &cte.cte_name)?;
            query = query.full_join(TableRef::new(&cte.cte_name), on_expr);
        }

        // Build SELECT clause
        let select_exprs = self.build_select_clause(plan)?;
        query = query.select(select_exprs);

        Ok(query)
    }

    /// Build the CTE subquery for a single fact.
    fn build_cte_query(&self, cte: &FactCte, group_by: &[String]) -> PlanResult<Query> {
        let mut query = Query::new();

        // FROM fact table
        query = query.from(self.parse_table_ref(&cte.fact_table));

        // TODO: Add JOINs for required_joins
        // This requires access to the ModelGraph which we don't have here
        // For now, we'll add this as a placeholder

        // WHERE clause with applicable filters
        for filter_expr in &cte.applicable_filters {
            // Parse simple filter expressions
            // TODO: Use proper expression parser
            let expr = self.parse_simple_filter(filter_expr)?;
            query = query.filter(expr);
        }

        // SELECT measures and group_by columns
        let mut select_exprs = Vec::new();

        // Add group_by columns
        for group_col in group_by {
            let col_name = group_col.split('.').next_back().unwrap_or(group_col);
            // For now, use the column directly (joins not yet implemented)
            select_exprs.push(SelectExpr::new(col(col_name)).with_alias(col_name));
        }

        // Add measures with aggregation
        for measure in &cte.measures {
            let agg_expr = self.build_aggregate_expr(&measure.aggregation, &measure.source_expr);
            select_exprs.push(SelectExpr::new(agg_expr).with_alias(&measure.alias));
        }

        query = query.select(select_exprs);

        // GROUP BY
        if !group_by.is_empty() {
            let group_exprs: Vec<Expr> = group_by
                .iter()
                .map(|c| {
                    let col_name = c.split('.').next_back().unwrap_or(c);
                    col(col_name)
                })
                .collect();
            query = query.group_by(group_exprs);
        }

        Ok(query)
    }

    /// Build the join condition for FULL OUTER JOIN between CTEs.
    fn build_join_condition(
        &self,
        group_by: &[String],
        left_cte: &str,
        right_cte: &str,
    ) -> PlanResult<Expr> {
        if group_by.is_empty() {
            return Err(PlanError::InvalidModel(
                "Report with multiple facts requires group_by for joining CTEs".to_string(),
            ));
        }

        let mut conditions = Vec::new();
        for group_col in group_by {
            let col_name = group_col.split('.').next_back().unwrap_or(group_col);
            let left = table_col(left_cte, col_name);
            let right = table_col(right_cte, col_name);
            conditions.push(ExprExt::eq(left, right));
        }

        // AND all conditions together
        let mut result = conditions.remove(0);
        for cond in conditions {
            result = ExprExt::and(result, cond);
        }

        Ok(result)
    }

    /// Build the SELECT clause for the main query.
    fn build_select_clause(&self, plan: &ReportPlan) -> PlanResult<Vec<SelectExpr>> {
        let mut select_exprs = Vec::new();

        // For group_by columns, use COALESCE across all CTEs
        for col in &plan.group_by {
            let col_name = col.split('.').next_back().unwrap_or(col);
            let cte_refs: Vec<Expr> = plan
                .fact_ctes
                .iter()
                .map(|cte| table_col(&cte.cte_name, col_name))
                .collect();

            let coalesce_expr = coalesce(cte_refs);
            select_exprs.push(SelectExpr::new(coalesce_expr).with_alias(col_name));
        }

        // Add measure columns from each CTE
        for cte in &plan.fact_ctes {
            for measure in &cte.measures {
                let col_expr = table_col(&cte.cte_name, &measure.alias);
                let alias = format!("{}_{}", cte.fact_name, measure.alias);
                select_exprs.push(SelectExpr::new(col_expr).with_alias(&alias));
            }
        }

        Ok(select_exprs)
    }

    /// Build an aggregate expression.
    pub(crate) fn build_aggregate_expr(&self, aggregation: &str, source: &str) -> Expr {
        let source_expr = if source == "*" {
            Expr::Star { table: None }
        } else {
            col(source)
        };

        let func = |name: &str, args: Vec<Expr>| Expr::Function {
            name: name.to_string(),
            args,
            distinct: false,
        };

        match aggregation {
            "SUM" => func("SUM", vec![source_expr]),
            "COUNT" => func("COUNT", vec![source_expr]),
            "COUNT DISTINCT" => Expr::Function {
                name: "COUNT".to_string(),
                args: vec![source_expr],
                distinct: true,
            },
            "AVG" => func("AVG", vec![source_expr]),
            "MIN" => func("MIN", vec![source_expr]),
            "MAX" => func("MAX", vec![source_expr]),
            _ => func(aggregation, vec![source_expr]),
        }
    }

    /// Parse a simple table reference like "schema.table".
    fn parse_table_ref(&self, table: &str) -> TableRef {
        if let Some((schema, name)) = table.split_once('.') {
            TableRef::new(name).with_schema(schema)
        } else {
            TableRef::new(table).with_schema(&self.default_schema)
        }
    }

    /// Parse a simple filter expression.
    ///
    /// This is a basic parser for expressions like "entity.column = 'value'".
    /// TODO: Use a proper expression parser.
    fn parse_simple_filter(&self, filter: &str) -> PlanResult<Expr> {
        // For now, just create a raw SQL expression
        // A proper implementation would parse and validate the filter
        Ok(Expr::Raw(filter.to_string()))
    }
}
