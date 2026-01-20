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
    /// Relative period (e.g., today, this_month, last_12_months).
    Relative(RelativePeriod),
    /// Explicit date range: `range(2024-01-01, 2024-12-31)`.
    Range { start: String, end: String },
    /// Specific month: `month(2024-03)`.
    Month { year: u16, month: u8 },
    /// Specific quarter: `quarter(2024-Q2)`.
    Quarter { year: u16, quarter: u8 },
    /// Specific year: `year(2024)`.
    Year { year: u16 },
}

/// Relative period expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum RelativePeriod {
    // Single day
    Today,
    Yesterday,

    // Current periods
    ThisWeek,
    ThisMonth,
    ThisQuarter,
    ThisYear,

    // Previous complete periods
    LastWeek,
    LastMonth,
    LastQuarter,
    LastYear,

    // Period-to-date
    Ytd,
    Qtd,
    Mtd,

    // Fiscal periods
    ThisFiscalYear,
    LastFiscalYear,
    ThisFiscalQuarter,
    LastFiscalQuarter,
    FiscalYtd,

    // Trailing periods: last_N_<unit>
    Trailing { count: u32, unit: PeriodUnit },
}

/// Unit for trailing periods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodUnit {
    Days,
    Weeks,
    Months,
    Quarters,
    Years,
}
