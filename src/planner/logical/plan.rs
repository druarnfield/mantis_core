//! Logical plan node types.

use crate::model::expr::Expr;
use crate::model::TimeSuffix;
use crate::semantic::graph::{query::ColumnRef as GraphColumnRef, Cardinality};

/// Logical plan - abstract operation tree.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // Core relational operations
    Scan(ScanNode),
    Join(JoinNode),
    Filter(FilterNode),
    Aggregate(AggregateNode),

    // Report-specific operations
    TimeMeasure(TimeMeasureNode),
    DrillPath(DrillPathNode),
    InlineMeasure(InlineMeasureNode),

    // Output formatting
    Project(ProjectNode),
    Sort(SortNode),
    Limit(LimitNode),
}

/// Scan a table.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanNode {
    pub entity: String,
}

/// Join type.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Join two plans.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: JoinCondition,
    pub join_type: JoinType,
    pub cardinality: Option<Cardinality>,
}

/// Join condition (columns to join on).
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    /// Equi-join on column pairs (most common)
    Equi(Vec<(GraphColumnRef, GraphColumnRef)>),
    /// Complex expression (for theta joins)
    Expr(Expr),
}

/// Filter rows.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicates: Vec<Expr>,
}

/// Aggregate (GROUP BY).
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<ColumnRef>,
    pub measures: Vec<MeasureRef>,
}

/// Time measure (YTD, prior period, etc).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeMeasureNode {
    pub input: Box<LogicalPlan>,
    pub base_measure: String,
    pub time_suffix: TimeSuffix,
    pub calendar: String,
}

/// Drill path navigation.
#[derive(Debug, Clone, PartialEq)]
pub struct DrillPathNode {
    pub input: Box<LogicalPlan>,
    pub source: String,
    pub path: String,
    pub level: String,
}

/// Inline measure (user-defined calculation).
#[derive(Debug, Clone, PartialEq)]
pub struct InlineMeasureNode {
    pub input: Box<LogicalPlan>,
    pub name: String,
    pub expr: Expr,
}

/// Project columns (SELECT).
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub projections: Vec<ProjectionItem>,
}

/// Projection item in SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionItem {
    Column(ColumnRef),
    Measure(MeasureRef),
    Expr { expr: Expr, alias: Option<String> },
}

/// Sort rows (ORDER BY).
#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    pub input: Box<LogicalPlan>,
    pub order_by: Vec<OrderRef>,
}

/// ORDER BY reference.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRef {
    pub column: String,
    pub descending: bool,
}

/// Limit rows.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub limit: u64,
}

/// Reference to a column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub entity: String,
    pub column: String,
}

/// Reference to a measure.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureRef {
    pub entity: String,
    pub measure: String,
}
