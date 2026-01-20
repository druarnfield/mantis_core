// src/model/report.rs
use crate::model::table::SqlExpr;

/// A report definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub name: String,
    /// Source tables (maps to SemanticQuery.from)
    pub from: Vec<String>,
    /// Time columns for period filtering (one per table)
    pub use_date: Vec<String>,
    /// Time period (compile-time evaluated to date range filter)
    pub period: Option<PeriodExpr>,
    /// Grouping (drill path references) - resolved to FieldRef
    pub group: Vec<GroupItem>,
    /// Measures to show (simple, with time suffix, or inline)
    pub show: Vec<ShowItem>,
    /// Filter conditions
    pub filters: Vec<SqlExpr>,
    /// Sort order
    pub sort: Vec<SortItem>,
    /// Row limit
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupItem {
    /// Drill path ref: dates.standard.month
    DrillPathRef {
        source: String,
        path: String,
        level: String,
        label: Option<String>,
    },
    /// Inline slicer: region
    InlineSlicer { name: String, label: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowItem {
    /// Simple measure: revenue
    Measure { name: String, label: Option<String> },
    /// Measure with time suffix: revenue.ytd
    MeasureWithSuffix {
        name: String,
        suffix: TimeSuffix,
        label: Option<String>,
    },
    /// Inline measure: net = { revenue - cost }
    InlineMeasure {
        name: String,
        expr: SqlExpr,
        label: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSuffix {
    // Accumulations
    Ytd,
    Qtd,
    Mtd,
    Wtd,
    FiscalYtd,
    FiscalQtd,
    // Prior periods
    PriorYear,
    PriorQuarter,
    PriorMonth,
    PriorWeek,
    // Growth
    YoyGrowth,
    QoqGrowth,
    MomGrowth,
    WowGrowth,
    // Deltas
    YoyDelta,
    QoqDelta,
    MomDelta,
    WowDelta,
    // Rolling
    Rolling3m,
    Rolling6m,
    Rolling12m,
    Rolling3mAvg,
    Rolling6mAvg,
    Rolling12mAvg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeriodExpr {
    // Placeholder - will implement later
    LastNMonths(u32),
}
