//! Physical execution plan nodes.

use crate::model::expr::Expr as ModelExpr;
use crate::sql::expr::col;
use crate::sql::query::{OrderByExpr, Query, SelectExpr, SortDir, TableRef};

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
        aggregates: Vec<String>,
    },

    /// Sort operation
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<SortKey>,
    },

    /// Projection
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<String>,
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
    /// Convert physical plan to SQL query
    pub fn to_query(&self) -> Query {
        match self {
            PhysicalPlan::TableScan { table, .. } => Query::new().from(TableRef::new(table)),
            PhysicalPlan::Filter { input, .. } => {
                // For now, just pass through the input's query
                // TODO: Add WHERE clause when we have ModelExpr -> SqlExpr conversion
                input.to_query()
            }
            PhysicalPlan::Project { input, columns } => {
                let query = input.to_query();
                // Convert column names to SelectExpr
                let select_exprs: Vec<SelectExpr> = columns
                    .iter()
                    .map(|column| SelectExpr::new(col(column)))
                    .collect();
                query.select(select_exprs)
            }
            PhysicalPlan::Sort { input, keys } => {
                let query = input.to_query();
                // Convert sort keys to OrderByExpr
                let order_exprs: Vec<OrderByExpr> = keys
                    .iter()
                    .map(|key| {
                        let expr = col(&key.column);
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
                // Set GROUP BY
                let group_exprs: Vec<_> = group_by.iter().map(|column| col(column)).collect();
                if !group_exprs.is_empty() {
                    query = query.group_by(group_exprs);
                }
                // Add aggregates to SELECT list
                let agg_exprs: Vec<SelectExpr> = aggregates
                    .iter()
                    .map(|column| SelectExpr::new(col(column)))
                    .collect();
                if !agg_exprs.is_empty() {
                    query = query.select(agg_exprs);
                }
                query
            }
            PhysicalPlan::HashJoin { left, .. } | PhysicalPlan::NestedLoopJoin { left, .. } => {
                // Simple join: start with left, add right as join
                // TODO: Add proper join conditions when Query builder supports it
                let left_query = left.to_query();
                // For now, just use left query
                // This is a stub - real implementation needs join support in Query
                left_query
            }
        }
    }
}
