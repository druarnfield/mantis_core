//! AST node types for the Mantis semantic model DSL.
//!
//! This module defines the abstract syntax tree for the DSL, covering:
//! - Model (root container)
//! - Defaults block
//! - Calendar definitions (physical and generated)
//! - Dimension definitions
//! - Table definitions with atoms, times, and slicers
//! - Measure blocks
//! - Report definitions

use crate::dsl::span::{Span, Spanned};

// ============================================================================
// Model (Root)
// ============================================================================

/// The root AST node representing a complete semantic model.
#[derive(Debug, Clone, PartialEq)]
pub struct Model {
    /// Optional model-wide defaults.
    pub defaults: Option<Spanned<Defaults>>,
    /// All items defined in the model.
    pub items: Vec<Spanned<Item>>,
}

/// A top-level item in the model.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    /// A calendar definition.
    Calendar(Calendar),
    /// A dimension definition.
    Dimension(Dimension),
    /// A table definition.
    Table(Table),
    /// A measures block for a table.
    MeasureBlock(MeasureBlock),
    /// A report definition.
    Report(Report),
}

// ============================================================================
// Defaults
// ============================================================================

/// Model-wide default settings.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Defaults {
    pub settings: Vec<Spanned<DefaultSetting>>,
}

/// A single default setting.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultSetting {
    /// Default calendar for time bindings: `calendar <name>;`
    Calendar(Spanned<String>),
    /// Fiscal year start month: `fiscal_year_start <month>;`
    FiscalYearStart(Spanned<Month>),
    /// First day of week: `week_start <day>;`
    WeekStart(Spanned<Weekday>),
    /// Division NULL handling: `null_handling <mode>;`
    NullHandling(Spanned<NullHandling>),
    /// Default decimal precision: `decimal_places <n>;`
    DecimalPlaces(Spanned<u8>),
}

/// Month of the year (for fiscal_year_start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Month {
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
}

impl Month {
    /// Parse a month from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "january" | "jan" => Some(Month::January),
            "february" | "feb" => Some(Month::February),
            "march" | "mar" => Some(Month::March),
            "april" | "apr" => Some(Month::April),
            "may" => Some(Month::May),
            "june" | "jun" => Some(Month::June),
            "july" | "jul" => Some(Month::July),
            "august" | "aug" => Some(Month::August),
            "september" | "sep" => Some(Month::September),
            "october" | "oct" => Some(Month::October),
            "november" | "nov" => Some(Month::November),
            "december" | "dec" => Some(Month::December),
            _ => None,
        }
    }
}

/// Day of the week (for week_start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Weekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl Weekday {
    /// Parse a weekday from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "monday" | "mon" => Some(Weekday::Monday),
            "tuesday" | "tue" => Some(Weekday::Tuesday),
            "wednesday" | "wed" => Some(Weekday::Wednesday),
            "thursday" | "thu" => Some(Weekday::Thursday),
            "friday" | "fri" => Some(Weekday::Friday),
            "saturday" | "sat" => Some(Weekday::Saturday),
            "sunday" | "sun" => Some(Weekday::Sunday),
            _ => None,
        }
    }
}

/// NULL handling mode for division.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullHandling {
    /// Division by zero/null returns 0.
    CoalesceZero,
    /// Division by zero/null returns NULL (default).
    #[default]
    NullOnZero,
    /// Division by zero raises a compile error.
    ErrorOnZero,
}

impl NullHandling {
    /// Parse a NULL handling mode from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "coalesce_zero" => Some(NullHandling::CoalesceZero),
            "null_on_zero" => Some(NullHandling::NullOnZero),
            "error_on_zero" => Some(NullHandling::ErrorOnZero),
            _ => None,
        }
    }
}

// ============================================================================
// Calendar
// ============================================================================

/// A calendar definition (physical or generated).
#[derive(Debug, Clone, PartialEq)]
pub struct Calendar {
    /// Calendar name.
    pub name: Spanned<String>,
    /// Calendar body (physical or generated).
    pub body: Spanned<CalendarBody>,
}

/// The body of a calendar definition.
#[derive(Debug, Clone, PartialEq)]
pub enum CalendarBody {
    /// Physical calendar referencing an existing date dimension table.
    Physical(PhysicalCalendar),
    /// Generated ephemeral calendar (CTE date spine).
    Generated(GeneratedCalendar),
}

/// A physical calendar referencing an existing date dimension table.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCalendar {
    /// Source table name (e.g., "dbo.dim_date").
    pub source: Spanned<String>,
    /// Grain level mappings (column must contain period-start dates).
    pub grain_mappings: Vec<Spanned<GrainMapping>>,
    /// Named drill paths defining valid aggregation hierarchies.
    pub drill_paths: Vec<Spanned<DrillPath>>,
    /// Fiscal year start month.
    pub fiscal_year_start: Option<Spanned<Month>>,
    /// First day of week.
    pub week_start: Option<Spanned<Weekday>>,
}

/// A generated ephemeral calendar (CTE date spine).
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedCalendar {
    /// Base grain level with + suffix (e.g., day+, month+).
    pub base_grain: Spanned<GrainLevel>,
    /// Include fiscal grains with specified fiscal year start.
    pub fiscal: Option<Spanned<Month>>,
    /// Date range specification.
    pub range: Option<Spanned<CalendarRange>>,
    /// Named drill paths defining valid aggregation hierarchies.
    pub drill_paths: Vec<Spanned<DrillPath>>,
    /// First day of week.
    pub week_start: Option<Spanned<Weekday>>,
}

/// A mapping from a grain level to a column name in a physical calendar.
#[derive(Debug, Clone, PartialEq)]
pub struct GrainMapping {
    /// The grain level being mapped.
    pub level: Spanned<GrainLevel>,
    /// The column name containing period-start dates.
    pub column: Spanned<String>,
}

/// Calendar grain levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrainLevel {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    FiscalMonth,
    FiscalQuarter,
    FiscalYear,
}

impl GrainLevel {
    /// Parse a grain level from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "minute" => Some(GrainLevel::Minute),
            "hour" => Some(GrainLevel::Hour),
            "day" => Some(GrainLevel::Day),
            "week" => Some(GrainLevel::Week),
            "month" => Some(GrainLevel::Month),
            "quarter" => Some(GrainLevel::Quarter),
            "year" => Some(GrainLevel::Year),
            "fiscal_month" => Some(GrainLevel::FiscalMonth),
            "fiscal_quarter" => Some(GrainLevel::FiscalQuarter),
            "fiscal_year" => Some(GrainLevel::FiscalYear),
            _ => None,
        }
    }

    /// Returns all grains at this level and coarser (for the + suffix).
    pub fn and_coarser(&self) -> Vec<GrainLevel> {
        match self {
            GrainLevel::Minute => vec![
                GrainLevel::Minute,
                GrainLevel::Hour,
                GrainLevel::Day,
                GrainLevel::Week,
                GrainLevel::Month,
                GrainLevel::Quarter,
                GrainLevel::Year,
            ],
            GrainLevel::Hour => vec![
                GrainLevel::Hour,
                GrainLevel::Day,
                GrainLevel::Week,
                GrainLevel::Month,
                GrainLevel::Quarter,
                GrainLevel::Year,
            ],
            GrainLevel::Day => vec![
                GrainLevel::Day,
                GrainLevel::Week,
                GrainLevel::Month,
                GrainLevel::Quarter,
                GrainLevel::Year,
            ],
            GrainLevel::Week => vec![
                GrainLevel::Week,
                GrainLevel::Month,
                GrainLevel::Quarter,
                GrainLevel::Year,
            ],
            GrainLevel::Month => vec![GrainLevel::Month, GrainLevel::Quarter, GrainLevel::Year],
            GrainLevel::Quarter => vec![GrainLevel::Quarter, GrainLevel::Year],
            GrainLevel::Year => vec![GrainLevel::Year],
            // Fiscal grains are not used as base grain
            GrainLevel::FiscalMonth => vec![
                GrainLevel::FiscalMonth,
                GrainLevel::FiscalQuarter,
                GrainLevel::FiscalYear,
            ],
            GrainLevel::FiscalQuarter => {
                vec![GrainLevel::FiscalQuarter, GrainLevel::FiscalYear]
            }
            GrainLevel::FiscalYear => vec![GrainLevel::FiscalYear],
        }
    }
}

/// Date range specification for generated calendars.
#[derive(Debug, Clone, PartialEq)]
pub enum CalendarRange {
    /// Explicit date range: `range <start> to <end>;`
    Explicit {
        start: Spanned<DateLiteral>,
        end: Spanned<DateLiteral>,
    },
    /// Inferred from data: `range infer;`
    Infer {
        /// Optional floor for inferred start date.
        min: Option<Spanned<DateLiteral>>,
        /// Optional ceiling for inferred end date.
        max: Option<Spanned<DateLiteral>>,
    },
}

/// A date literal (YYYY-MM-DD format).
#[derive(Debug, Clone, PartialEq)]
pub struct DateLiteral {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl DateLiteral {
    pub fn new(year: u16, month: u8, day: u8) -> Self {
        Self { year, month, day }
    }
}

impl std::fmt::Display for DateLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// A named drill path defining a valid aggregation hierarchy.
#[derive(Debug, Clone, PartialEq)]
pub struct DrillPath {
    /// Drill path name.
    pub name: Spanned<String>,
    /// Ordered levels from fine to coarse grain.
    pub levels: Vec<Spanned<String>>,
}

// ============================================================================
// Dimension
// ============================================================================

/// A dimension definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Dimension {
    /// Dimension name.
    pub name: Spanned<String>,
    /// Source table (e.g., "dbo.dim_customers").
    pub source: Spanned<String>,
    /// Primary key column.
    pub key: Spanned<String>,
    /// Dimension attributes.
    pub attributes: Vec<Spanned<Attribute>>,
    /// Named drill paths defining valid aggregation hierarchies.
    pub drill_paths: Vec<Spanned<DrillPath>>,
}

/// A dimension attribute.
#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    /// Attribute name.
    pub name: Spanned<String>,
    /// Attribute data type.
    pub data_type: Spanned<DataType>,
}

/// Data types for attributes and slicers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    Int,
    Decimal,
    Float,
    Bool,
    Date,
    Timestamp,
}

impl DataType {
    /// Parse a data type from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "string" => Some(DataType::String),
            "int" => Some(DataType::Int),
            "decimal" => Some(DataType::Decimal),
            "float" => Some(DataType::Float),
            "bool" => Some(DataType::Bool),
            "date" => Some(DataType::Date),
            "timestamp" => Some(DataType::Timestamp),
            _ => None,
        }
    }
}

// ============================================================================
// Table
// ============================================================================

/// A table definition (fact, wide table, or CSV).
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// Table name.
    pub name: Spanned<String>,
    /// Source table/file (e.g., "dbo.fact_sales" or "exports/q4_sales.csv").
    pub source: Spanned<String>,
    /// Numeric columns for aggregation.
    pub atoms: Vec<Spanned<Atom>>,
    /// Date/time columns for time intelligence.
    pub times: Vec<Spanned<TimeBinding>>,
    /// Columns for slicing/grouping.
    pub slicers: Vec<Spanned<Slicer>>,
}

/// An atom (numeric column for aggregation).
#[derive(Debug, Clone, PartialEq)]
pub struct Atom {
    /// Atom name.
    pub name: Spanned<String>,
    /// Atom type (int, decimal, float).
    pub atom_type: Spanned<AtomType>,
}

/// Atom types (numeric only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomType {
    Int,
    Decimal,
    Float,
}

impl AtomType {
    /// Parse an atom type from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "int" => Some(AtomType::Int),
            "decimal" => Some(AtomType::Decimal),
            "float" => Some(AtomType::Float),
            _ => None,
        }
    }
}

/// A time binding to a calendar grain level.
#[derive(Debug, Clone, PartialEq)]
pub struct TimeBinding {
    /// Time column name.
    pub name: Spanned<String>,
    /// Calendar name.
    pub calendar: Spanned<String>,
    /// Grain level (e.g., day, month).
    pub grain: Spanned<GrainLevel>,
}

/// A slicer (column for slicing/grouping).
#[derive(Debug, Clone, PartialEq)]
pub struct Slicer {
    /// Slicer name.
    pub name: Spanned<String>,
    /// Slicer kind.
    pub kind: Spanned<SlicerKind>,
}

/// The kind of slicer.
#[derive(Debug, Clone, PartialEq)]
pub enum SlicerKind {
    /// Inline slicer with a type: `<name> <type>;`
    Inline { data_type: DataType },
    /// Foreign key to dimension: `<name> -> <dimension>.<key>;`
    ForeignKey {
        dimension: String,
        key_column: String,
    },
    /// Derived slicer via FK: `<name> via <fk_slicer>;`
    Via { fk_slicer: String },
    /// Calculated slicer: `<name> <type> = { <sql_expression> };`
    Calculated { data_type: DataType, expr: SqlExpr },
}

/// A raw SQL expression wrapped in `{ }`.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlExpr {
    /// The raw SQL expression text (without the braces).
    pub sql: String,
    /// The span of the expression (including braces).
    pub span: Span,
}

impl SqlExpr {
    pub fn new(sql: String, span: Span) -> Self {
        Self { sql, span }
    }
}

// ============================================================================
// Measures
// ============================================================================

/// A measures block for a table.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureBlock {
    /// Table name this block applies to.
    pub table: Spanned<String>,
    /// Measure definitions.
    pub measures: Vec<Spanned<Measure>>,
}

/// A measure definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    /// Measure name.
    pub name: Spanned<String>,
    /// Measure expression.
    pub expr: Spanned<SqlExpr>,
    /// Optional filter condition (WHERE clause).
    pub filter: Option<Spanned<SqlExpr>>,
    /// Optional per-measure NULL handling override.
    pub null_handling: Option<Spanned<NullHandling>>,
}

// ============================================================================
// Report
// ============================================================================

/// A report definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    /// Report name.
    pub name: Spanned<String>,
    /// Source table(s).
    pub from: Vec<Spanned<String>>,
    /// Time column(s) for period filtering (one per table in `from`).
    pub use_date: Vec<Spanned<String>>,
    /// Time period for the report.
    pub period: Option<Spanned<PeriodExpr>>,
    /// Grouping columns (drill path references or inline slicers).
    pub group: Vec<Spanned<GroupItem>>,
    /// Measures to display.
    pub show: Vec<Spanned<ShowItem>>,
    /// Filter condition.
    pub filter: Option<Spanned<SqlExpr>>,
    /// Sort order.
    pub sort: Vec<Spanned<SortItem>>,
    /// Maximum rows returned.
    pub limit: Option<Spanned<u64>>,
}

/// A period expression (compile-time date range).
#[derive(Debug, Clone, PartialEq)]
pub enum PeriodExpr {
    /// Relative period (e.g., today, this_month, last_12_months).
    Relative(RelativePeriod),
    /// Explicit date range: `range(2024-01-01, 2024-12-31)`.
    Range {
        start: DateLiteral,
        end: DateLiteral,
    },
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

impl PeriodUnit {
    /// Parse a period unit from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "days" | "day" => Some(PeriodUnit::Days),
            "weeks" | "week" => Some(PeriodUnit::Weeks),
            "months" | "month" => Some(PeriodUnit::Months),
            "quarters" | "quarter" => Some(PeriodUnit::Quarters),
            "years" | "year" => Some(PeriodUnit::Years),
            _ => None,
        }
    }
}

/// A grouping item in a report.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupItem {
    /// Drill path reference: `<source>.<drill_path>.<level>` (e.g., dates.standard.month).
    DrillPathRef(DrillPathRef),
    /// Inline slicer reference: `<slicer_name>`.
    InlineSlicer { name: String },
}

/// A drill path reference with source, path, and level.
#[derive(Debug, Clone, PartialEq)]
pub struct DrillPathRef {
    /// Source (calendar or dimension name).
    pub source: String,
    /// Drill path name.
    pub path: String,
    /// Level within the drill path.
    pub level: String,
    /// Optional label.
    pub label: Option<String>,
}

/// An item to show in a report.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowItem {
    /// Simple measure reference: `<measure>`.
    Measure {
        name: String,
        label: Option<String>,
    },
    /// Measure with time intelligence suffix: `<measure>.<suffix>`.
    MeasureWithSuffix {
        name: String,
        suffix: TimeSuffix,
        label: Option<String>,
    },
    /// Inline measure: `<name> = { <expression> }`.
    InlineMeasure {
        name: String,
        expr: SqlExpr,
        label: Option<String>,
    },
}

/// Time intelligence suffixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSuffix {
    // Accumulations
    Ytd,
    Qtd,
    Mtd,
    Wtd,
    FiscalYtd,
    FiscalQtd,

    // Prior period
    PriorYear,
    PriorQuarter,
    PriorMonth,
    PriorWeek,

    // Growth (percentage)
    YoyGrowth,
    QoqGrowth,
    MomGrowth,
    WowGrowth,

    // Delta (absolute)
    YoyDelta,
    QoqDelta,
    MomDelta,
    WowDelta,

    // Rolling sums
    Rolling3m,
    Rolling6m,
    Rolling12m,

    // Rolling averages
    Rolling3mAvg,
    Rolling6mAvg,
    Rolling12mAvg,
}

impl TimeSuffix {
    /// Parse a time suffix from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            // Accumulations
            "ytd" => Some(TimeSuffix::Ytd),
            "qtd" => Some(TimeSuffix::Qtd),
            "mtd" => Some(TimeSuffix::Mtd),
            "wtd" => Some(TimeSuffix::Wtd),
            "fiscal_ytd" => Some(TimeSuffix::FiscalYtd),
            "fiscal_qtd" => Some(TimeSuffix::FiscalQtd),

            // Prior period
            "prior_year" => Some(TimeSuffix::PriorYear),
            "prior_quarter" => Some(TimeSuffix::PriorQuarter),
            "prior_month" => Some(TimeSuffix::PriorMonth),
            "prior_week" => Some(TimeSuffix::PriorWeek),

            // Growth (percentage)
            "yoy_growth" => Some(TimeSuffix::YoyGrowth),
            "qoq_growth" => Some(TimeSuffix::QoqGrowth),
            "mom_growth" => Some(TimeSuffix::MomGrowth),
            "wow_growth" => Some(TimeSuffix::WowGrowth),

            // Delta (absolute)
            "yoy_delta" => Some(TimeSuffix::YoyDelta),
            "qoq_delta" => Some(TimeSuffix::QoqDelta),
            "mom_delta" => Some(TimeSuffix::MomDelta),
            "wow_delta" => Some(TimeSuffix::WowDelta),

            // Rolling sums
            "rolling_3m" => Some(TimeSuffix::Rolling3m),
            "rolling_6m" => Some(TimeSuffix::Rolling6m),
            "rolling_12m" => Some(TimeSuffix::Rolling12m),

            // Rolling averages
            "rolling_3m_avg" => Some(TimeSuffix::Rolling3mAvg),
            "rolling_6m_avg" => Some(TimeSuffix::Rolling6mAvg),
            "rolling_12m_avg" => Some(TimeSuffix::Rolling12mAvg),

            _ => None,
        }
    }
}

/// A sort item.
#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    /// Column or measure to sort by (can be a drill path ref or measure name).
    pub column: String,
    /// Sort direction.
    pub direction: SortDirection,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

impl SortDirection {
    /// Parse a sort direction from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "asc" => Some(SortDirection::Asc),
            "desc" => Some(SortDirection::Desc),
            _ => None,
        }
    }
}

// ============================================================================
// Display implementations for debugging
// ============================================================================

impl std::fmt::Display for GrainLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrainLevel::Minute => write!(f, "minute"),
            GrainLevel::Hour => write!(f, "hour"),
            GrainLevel::Day => write!(f, "day"),
            GrainLevel::Week => write!(f, "week"),
            GrainLevel::Month => write!(f, "month"),
            GrainLevel::Quarter => write!(f, "quarter"),
            GrainLevel::Year => write!(f, "year"),
            GrainLevel::FiscalMonth => write!(f, "fiscal_month"),
            GrainLevel::FiscalQuarter => write!(f, "fiscal_quarter"),
            GrainLevel::FiscalYear => write!(f, "fiscal_year"),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Int => write!(f, "int"),
            DataType::Decimal => write!(f, "decimal"),
            DataType::Float => write!(f, "float"),
            DataType::Bool => write!(f, "bool"),
            DataType::Date => write!(f, "date"),
            DataType::Timestamp => write!(f, "timestamp"),
        }
    }
}

impl std::fmt::Display for AtomType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomType::Int => write!(f, "int"),
            AtomType::Decimal => write!(f, "decimal"),
            AtomType::Float => write!(f, "float"),
        }
    }
}

impl std::fmt::Display for Month {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Month::January => write!(f, "January"),
            Month::February => write!(f, "February"),
            Month::March => write!(f, "March"),
            Month::April => write!(f, "April"),
            Month::May => write!(f, "May"),
            Month::June => write!(f, "June"),
            Month::July => write!(f, "July"),
            Month::August => write!(f, "August"),
            Month::September => write!(f, "September"),
            Month::October => write!(f, "October"),
            Month::November => write!(f, "November"),
            Month::December => write!(f, "December"),
        }
    }
}

impl std::fmt::Display for Weekday {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Weekday::Monday => write!(f, "Monday"),
            Weekday::Tuesday => write!(f, "Tuesday"),
            Weekday::Wednesday => write!(f, "Wednesday"),
            Weekday::Thursday => write!(f, "Thursday"),
            Weekday::Friday => write!(f, "Friday"),
            Weekday::Saturday => write!(f, "Saturday"),
            Weekday::Sunday => write!(f, "Sunday"),
        }
    }
}

impl std::fmt::Display for NullHandling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NullHandling::CoalesceZero => write!(f, "coalesce_zero"),
            NullHandling::NullOnZero => write!(f, "null_on_zero"),
            NullHandling::ErrorOnZero => write!(f, "error_on_zero"),
        }
    }
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "asc"),
            SortDirection::Desc => write!(f, "desc"),
        }
    }
}

impl std::fmt::Display for TimeSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeSuffix::Ytd => write!(f, "ytd"),
            TimeSuffix::Qtd => write!(f, "qtd"),
            TimeSuffix::Mtd => write!(f, "mtd"),
            TimeSuffix::Wtd => write!(f, "wtd"),
            TimeSuffix::FiscalYtd => write!(f, "fiscal_ytd"),
            TimeSuffix::FiscalQtd => write!(f, "fiscal_qtd"),
            TimeSuffix::PriorYear => write!(f, "prior_year"),
            TimeSuffix::PriorQuarter => write!(f, "prior_quarter"),
            TimeSuffix::PriorMonth => write!(f, "prior_month"),
            TimeSuffix::PriorWeek => write!(f, "prior_week"),
            TimeSuffix::YoyGrowth => write!(f, "yoy_growth"),
            TimeSuffix::QoqGrowth => write!(f, "qoq_growth"),
            TimeSuffix::MomGrowth => write!(f, "mom_growth"),
            TimeSuffix::WowGrowth => write!(f, "wow_growth"),
            TimeSuffix::YoyDelta => write!(f, "yoy_delta"),
            TimeSuffix::QoqDelta => write!(f, "qoq_delta"),
            TimeSuffix::MomDelta => write!(f, "mom_delta"),
            TimeSuffix::WowDelta => write!(f, "wow_delta"),
            TimeSuffix::Rolling3m => write!(f, "rolling_3m"),
            TimeSuffix::Rolling6m => write!(f, "rolling_6m"),
            TimeSuffix::Rolling12m => write!(f, "rolling_12m"),
            TimeSuffix::Rolling3mAvg => write!(f, "rolling_3m_avg"),
            TimeSuffix::Rolling6mAvg => write!(f, "rolling_6m_avg"),
            TimeSuffix::Rolling12mAvg => write!(f, "rolling_12m_avg"),
        }
    }
}
