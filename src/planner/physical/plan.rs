//! Physical execution plan nodes.

use crate::model::expr::Expr;
use crate::sql::query::Query;

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
        predicate: Expr,
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
    /// Convert physical plan to SQL query (stub for now)
    pub fn to_query(&self) -> Query {
        Query::new()
    }
}
