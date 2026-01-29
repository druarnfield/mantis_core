//! Physical execution plan nodes.

use crate::model::expr::Expr as ModelExpr;
use crate::planner::expr_converter::{ExprConverter, QueryContext};
use crate::planner::logical::{ExpandedMeasure, ProjectionItem};
use crate::sql::expr::{col, table_col, Expr as SqlExpr};
use crate::sql::query::{OrderByExpr, Query, SelectExpr, SortDir, TableRef};

/// Parse a column reference string that may contain a table prefix (e.g., "sales.revenue")
/// and return the appropriate SQL expression.
fn parse_column_ref(column_ref: &str) -> SqlExpr {
    if let Some(dot_pos) = column_ref.find('.') {
        let table = &column_ref[..dot_pos];
        let column = &column_ref[dot_pos + 1..];
        table_col(table, column)
    } else {
        col(column_ref)
    }
}

/// Physical execution plan node
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    /// Table scan with strategy
    TableScan {
        table: String,
        strategy: TableScanStrategy,
        estimated_rows: Option<usize>,
    },

    /// Hash join
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Vec<(String, String)>,
        estimated_rows: Option<usize>,
    },

    /// Nested loop join
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Vec<(String, String)>,
        estimated_rows: Option<usize>,
    },

    /// Filter operation
    Filter {
        input: Box<PhysicalPlan>,
        predicate: ModelExpr,
    },

    /// Hash aggregate
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<ExpandedMeasure>,
    },

    /// Sort operation
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<SortKey>,
    },

    /// Projection
    Project {
        input: Box<PhysicalPlan>,
        projections: Vec<ProjectionItem>,
    },

    /// Limit
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
    },
}

/// Strategy for scanning a table
#[derive(Debug, Clone, PartialEq)]
pub enum TableScanStrategy {
    /// Full table scan
    FullScan,

    /// Index scan (if available)
    IndexScan { index: String },
}

/// Sort key with direction
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

impl PhysicalPlan {
    /// Extract table information from the physical plan to build a QueryContext.
    /// This walks the plan tree to find all table scans and adds them to the context.
    fn extract_query_context(&self) -> QueryContext {
        let mut context = QueryContext::new();
        self.populate_query_context(&mut context);
        context
    }

    /// Recursively populate the query context with table information.
    fn populate_query_context(&self, context: &mut QueryContext) {
        match self {
            PhysicalPlan::TableScan { table, .. } => {
                // Use the table name as both entity and alias
                context.add_table(table.clone(), table.clone());
            }
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => {
                input.populate_query_context(context);
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                left.populate_query_context(context);
                right.populate_query_context(context);
            }
        }
    }

    /// Convert physical plan to SQL query
    /// Extract table name from a physical plan (works for scans and joins)
    fn extract_table_name(plan: &PhysicalPlan) -> String {
        match plan {
            PhysicalPlan::TableScan { table, .. } => table.clone(),
            PhysicalPlan::HashJoin { right, .. } | PhysicalPlan::NestedLoopJoin { right, .. } => {
                Self::extract_table_name(right)
            }
            _ => "unknown".to_string(),
        }
    }

    /// Build JOIN ON condition from column pairs
    fn build_join_condition(on: &[(String, String)]) -> crate::sql::expr::Expr {
        use crate::sql::expr::{BinaryOperator, Expr, Literal};

        if on.is_empty() {
            // No join condition - this shouldn't happen but handle gracefully
            return Expr::Literal(Literal::Bool(true));
        }

        // Build condition for each pair: left_col = right_col
        let conditions: Vec<Expr> = on
            .iter()
            .map(|(left_col, right_col)| Expr::BinaryOp {
                left: Box::new(parse_column_ref(left_col)),
                op: BinaryOperator::Eq,
                right: Box::new(parse_column_ref(right_col)),
            })
            .collect();

        // Combine multiple conditions with AND
        if conditions.len() == 1 {
            conditions[0].clone()
        } else {
            let mut result = conditions[conditions.len() - 1].clone();
            for condition in conditions[..conditions.len() - 1].iter().rev() {
                result = Expr::BinaryOp {
                    left: Box::new(condition.clone()),
                    op: BinaryOperator::And,
                    right: Box::new(result),
                };
            }
            result
        }
    }

    pub fn to_query(&self) -> Query {
        match self {
            PhysicalPlan::TableScan { table, .. } => Query::new().from(TableRef::new(table)),
            PhysicalPlan::Filter { input, predicate } => {
                let mut query = input.to_query();

                // Extract query context for expression conversion
                let context = self.extract_query_context();

                // Convert predicate from ModelExpr to SqlExpr
                // Note: If conversion fails, we panic. This is acceptable for now since
                // it indicates a programming error in the plan generation.
                let sql_predicate = ExprConverter::convert(predicate, &context)
                    .expect("Failed to convert filter predicate to SQL");

                // Add WHERE clause
                query = query.filter(sql_predicate);
                query
            }
            PhysicalPlan::Project { input, projections } => {
                let query = input.to_query();
                let context = self.extract_query_context();

                let select_exprs: Vec<SelectExpr> = projections
                    .iter()
                    .map(|item| match item {
                        ProjectionItem::Column(col) => {
                            let expr = table_col(&col.entity, &col.column);
                            SelectExpr::new(expr)
                        }
                        ProjectionItem::Measure(m) => {
                            let sql_expr = ExprConverter::convert(&m.expr, &context)
                                .expect("Failed to convert measure expression");
                            SelectExpr::new(sql_expr).with_alias(&m.name)
                        }
                        ProjectionItem::Expr { expr, alias } => {
                            let sql_expr = ExprConverter::convert(expr, &context)
                                .expect("Failed to convert expression");
                            let mut se = SelectExpr::new(sql_expr);
                            if let Some(a) = alias {
                                se = se.with_alias(a);
                            }
                            se
                        }
                    })
                    .collect();
                query.select(select_exprs)
            }
            PhysicalPlan::Sort { input, keys } => {
                let query = input.to_query();
                // Convert sort keys to OrderByExpr
                let order_exprs: Vec<OrderByExpr> = keys
                    .iter()
                    .map(|key| {
                        let expr = parse_column_ref(&key.column);
                        OrderByExpr {
                            expr,
                            dir: Some(if key.ascending {
                                SortDir::Asc
                            } else {
                                SortDir::Desc
                            }),
                            nulls: None,
                        }
                    })
                    .collect();
                query.order_by(order_exprs)
            }
            PhysicalPlan::Limit { input, limit } => input.to_query().limit(*limit as u64),
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let mut query = input.to_query();

                // GROUP BY
                let group_exprs: Vec<_> = group_by
                    .iter()
                    .map(|column| parse_column_ref(column))
                    .collect();
                if !group_exprs.is_empty() {
                    query = query.group_by(group_exprs);
                }

                // SELECT: convert ExpandedMeasure.expr to SQL with alias
                let context = self.extract_query_context();
                let agg_exprs: Vec<SelectExpr> = aggregates
                    .iter()
                    .map(|m| {
                        let sql_expr = ExprConverter::convert(&m.expr, &context)
                            .expect("Failed to convert measure expression");
                        SelectExpr::new(sql_expr).with_alias(&m.name)
                    })
                    .collect();

                if !agg_exprs.is_empty() {
                    query = query.select(agg_exprs);
                }
                query
            }
            PhysicalPlan::HashJoin {
                left, right, on, ..
            }
            | PhysicalPlan::NestedLoopJoin {
                left, right, on, ..
            } => {
                use crate::sql::expr::BinaryOperator;

                // Start with left query
                let mut query = left.to_query();

                // Extract right table name
                let right_table = Self::extract_table_name(right);

                // Build join condition from column pairs
                let join_condition = Self::build_join_condition(on);

                // Add INNER JOIN with ON clause
                query = query.inner_join(TableRef::new(&right_table), join_condition);

                query
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::expr::{BinaryOp, Expr as ModelExpr, Literal};
    use crate::sql::Dialect;

    #[test]
    fn test_filter_to_query_with_predicate() {
        // Create a simple filter: WHERE sales.amount > 100
        let predicate = ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(ModelExpr::Literal(Literal::Int(100))),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
                strategy: TableScanStrategy::FullScan,
                estimated_rows: None,
            }),
            predicate,
        };

        let query = plan.to_query();
        let sql = query.to_sql(Dialect::Postgres);

        // Should have WHERE clause with the predicate
        assert!(
            sql.contains("WHERE"),
            "Query should contain WHERE clause, got: {}",
            sql
        );
        assert!(
            sql.contains("amount") && sql.contains("100"),
            "Query should contain predicate, got: {}",
            sql
        );
    }

    #[test]
    fn test_filter_to_query_with_entity_qualified_column() {
        // Test that entity names are properly converted to table aliases in WHERE clause
        let predicate = ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "region".to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
                strategy: TableScanStrategy::FullScan,
                estimated_rows: None,
            }),
            predicate,
        };

        let query = plan.to_query();
        let sql = query.to_sql(Dialect::Postgres);

        // Should have WHERE clause with qualified column
        assert!(
            sql.contains("WHERE"),
            "Query should contain WHERE clause, got: {}",
            sql
        );
    }
}
