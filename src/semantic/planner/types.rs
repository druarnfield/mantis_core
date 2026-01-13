//! Semantic query types - the user-facing query representation.

/// A semantic query - what the user writes.
///
/// The `from` field is optional - if not specified, anchor facts are
/// inferred from the measures in the query. For multi-fact queries
/// (measures from different facts), a symmetric aggregate pattern is used.
#[derive(Debug, Clone, Default)]
pub struct SemanticQuery {
    /// Explicit anchor fact. If None, inferred from measures.
    pub from: Option<String>,
    pub filters: Vec<FieldFilter>,
    pub group_by: Vec<FieldRef>,
    pub select: Vec<SelectField>,
    /// Derived measures - calculations from other measures.
    ///
    /// These are computed after aggregation and can reference
    /// measures in the select list.
    pub derived: Vec<DerivedField>,
    pub order_by: Vec<OrderField>,
    pub limit: Option<u64>,
}

/// A reference to a field: entity.field
#[derive(Debug, Clone)]
pub struct FieldRef {
    pub entity: String,
    pub field: String,
}

impl FieldRef {
    pub fn new(entity: &str, field: &str) -> Self {
        Self {
            entity: entity.into(),
            field: field.into(),
        }
    }
}

/// A filter condition.
#[derive(Debug, Clone)]
pub struct FieldFilter {
    pub field: FieldRef,
    pub op: FilterOp,
    pub value: FilterValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    In,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
    List(Vec<FilterValue>),
}

/// A field in the select list, optionally with aggregation.
#[derive(Debug, Clone)]
pub struct SelectField {
    pub field: FieldRef,
    pub alias: Option<String>,
    /// Optional aggregation function (SUM, COUNT, AVG, etc.)
    pub aggregation: Option<String>,
    /// Optional filter for filtered measures (generates CASE WHEN).
    ///
    /// When set, the aggregation becomes conditional:
    /// `SUM(CASE WHEN condition THEN column END)`
    pub measure_filter: Option<Vec<FieldFilter>>,
}

impl SelectField {
    pub fn new(entity: &str, field: &str) -> Self {
        Self {
            field: FieldRef::new(entity, field),
            alias: None,
            aggregation: None,
            measure_filter: None,
        }
    }

    /// Create an aggregated field (e.g., SUM(orders.amount))
    pub fn aggregate(entity: &str, field: &str, agg: &str) -> Self {
        Self {
            field: FieldRef::new(entity, field),
            alias: None,
            aggregation: Some(agg.to_uppercase()),
            measure_filter: None,
        }
    }

    pub fn with_alias(mut self, alias: &str) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Add a filter for conditional aggregation (filtered measure).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Creates: SUM(CASE WHEN segment = 'Enterprise' THEN amount END)
    /// SelectField::aggregate("sales", "revenue", "SUM")
    ///     .with_filter(vec![FieldFilter { ... }])
    /// ```
    pub fn with_filter(mut self, filter: Vec<FieldFilter>) -> Self {
        self.measure_filter = Some(filter);
        self
    }

    /// Check if this is an aggregate field.
    pub fn is_aggregate(&self) -> bool {
        self.aggregation.is_some()
    }

    /// Check if this is a filtered measure.
    pub fn is_filtered(&self) -> bool {
        self.measure_filter.is_some()
    }
}

/// A derived field - a calculation from other measures.
///
/// These are computed after aggregation and reference
/// measures by their output alias.
#[derive(Debug, Clone)]
pub struct DerivedField {
    /// Output alias for this derived measure
    pub alias: String,
    /// The expression to compute
    pub expression: DerivedExpr,
}

/// An expression for derived fields.
#[derive(Debug, Clone)]
pub enum DerivedExpr {
    /// Reference to a measure by name (output alias)
    MeasureRef(String),
    /// A literal numeric value
    Literal(f64),
    /// Binary operation
    BinaryOp {
        left: Box<DerivedExpr>,
        op: DerivedBinaryOp,
        right: Box<DerivedExpr>,
    },
    /// Unary negation
    Negate(Box<DerivedExpr>),

    // =========================================================================
    // Time Intelligence Extensions
    // =========================================================================

    /// A time intelligence function (YTD, prior year, rolling, etc.)
    TimeFunction(TimeFunction),

    /// Delta: difference between current and previous value.
    ///
    /// `delta(revenue, prior_year(revenue))` → `revenue - prior_year_revenue`
    Delta {
        current: Box<DerivedExpr>,
        previous: Box<DerivedExpr>,
    },

    /// Growth: percentage change from previous to current.
    ///
    /// `growth(revenue, prior_year(revenue))` → `(revenue - py) / NULLIF(py, 0) * 100`
    Growth {
        current: Box<DerivedExpr>,
        previous: Box<DerivedExpr>,
    },
}

/// Binary operators for derived expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl DerivedBinaryOp {
    pub fn to_sql(&self) -> &'static str {
        match self {
            DerivedBinaryOp::Add => "+",
            DerivedBinaryOp::Sub => "-",
            DerivedBinaryOp::Mul => "*",
            DerivedBinaryOp::Div => "/",
        }
    }
}

/// Time intelligence functions for period-over-period analysis.
///
/// These functions generate window functions and lag calculations
/// for temporal analytics like YTD, prior period, and rolling windows.
#[derive(Debug, Clone)]
pub enum TimeFunction {
    /// Year-to-date: cumulative sum from start of year.
    ///
    /// SQL: `SUM(measure) OVER (PARTITION BY year ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
    YearToDate {
        /// The measure to accumulate
        measure: String,
        /// Column to partition by (year column from date dimension)
        year_column: Option<String>,
        /// Column to order by (month/week/day column from date dimension)
        period_column: Option<String>,
        /// Optional role override (e.g., "ship_date" instead of default "order_date")
        via: Option<String>,
    },

    /// Quarter-to-date: cumulative sum from start of quarter.
    ///
    /// SQL: `SUM(measure) OVER (PARTITION BY year, quarter ORDER BY period ROWS UNBOUNDED PRECEDING)`
    QuarterToDate {
        measure: String,
        year_column: Option<String>,
        quarter_column: Option<String>,
        period_column: Option<String>,
        via: Option<String>,
    },

    /// Month-to-date: cumulative sum from start of month.
    ///
    /// SQL: `SUM(measure) OVER (PARTITION BY year, month ORDER BY day ROWS UNBOUNDED PRECEDING)`
    MonthToDate {
        measure: String,
        year_column: Option<String>,
        month_column: Option<String>,
        day_column: Option<String>,
        via: Option<String>,
    },

    /// Prior period: value from N periods ago.
    ///
    /// SQL: `LAG(measure, N) OVER (PARTITION BY dims ORDER BY period)`
    PriorPeriod {
        measure: String,
        /// Number of periods to look back (default: 1)
        periods_back: u32,
        via: Option<String>,
    },

    /// Prior year: same period in the prior year.
    ///
    /// For monthly grain: `LAG(measure, 12)`
    /// For quarterly grain: `LAG(measure, 4)`
    /// For daily grain: uses date arithmetic or LAG(measure, 365)
    PriorYear {
        measure: String,
        via: Option<String>,
    },

    /// Prior quarter: same period in the prior quarter.
    ///
    /// For monthly grain: `LAG(measure, 3)`
    PriorQuarter {
        measure: String,
        via: Option<String>,
    },

    /// Rolling sum: sum over the last N periods.
    ///
    /// SQL: `SUM(measure) OVER (ORDER BY period ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)`
    RollingSum {
        measure: String,
        /// Number of periods to include (including current)
        periods: u32,
        via: Option<String>,
    },

    /// Rolling average: average over the last N periods.
    ///
    /// SQL: `AVG(measure) OVER (ORDER BY period ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)`
    RollingAvg {
        measure: String,
        periods: u32,
        via: Option<String>,
    },
}

impl TimeFunction {
    /// Create a year-to-date function with defaults.
    pub fn ytd(measure: impl Into<String>) -> Self {
        Self::YearToDate {
            measure: measure.into(),
            year_column: None,
            period_column: None,
            via: None,
        }
    }

    /// Create a prior year function.
    pub fn prior_year(measure: impl Into<String>) -> Self {
        Self::PriorYear {
            measure: measure.into(),
            via: None,
        }
    }

    /// Create a prior period function.
    pub fn prior_period(measure: impl Into<String>, periods_back: u32) -> Self {
        Self::PriorPeriod {
            measure: measure.into(),
            periods_back,
            via: None,
        }
    }

    /// Create a rolling sum function.
    pub fn rolling_sum(measure: impl Into<String>, periods: u32) -> Self {
        Self::RollingSum {
            measure: measure.into(),
            periods,
            via: None,
        }
    }

    /// Create a rolling average function.
    pub fn rolling_avg(measure: impl Into<String>, periods: u32) -> Self {
        Self::RollingAvg {
            measure: measure.into(),
            periods,
            via: None,
        }
    }

    /// Get the measure this function operates on.
    pub fn measure(&self) -> &str {
        match self {
            Self::YearToDate { measure, .. } => measure,
            Self::QuarterToDate { measure, .. } => measure,
            Self::MonthToDate { measure, .. } => measure,
            Self::PriorPeriod { measure, .. } => measure,
            Self::PriorYear { measure, .. } => measure,
            Self::PriorQuarter { measure, .. } => measure,
            Self::RollingSum { measure, .. } => measure,
            Self::RollingAvg { measure, .. } => measure,
        }
    }

    /// Get the via (role override) if specified.
    pub fn via(&self) -> Option<&str> {
        match self {
            Self::YearToDate { via, .. } => via.as_deref(),
            Self::QuarterToDate { via, .. } => via.as_deref(),
            Self::MonthToDate { via, .. } => via.as_deref(),
            Self::PriorPeriod { via, .. } => via.as_deref(),
            Self::PriorYear { via, .. } => via.as_deref(),
            Self::PriorQuarter { via, .. } => via.as_deref(),
            Self::RollingSum { via, .. } => via.as_deref(),
            Self::RollingAvg { via, .. } => via.as_deref(),
        }
    }
}

impl DerivedField {
    pub fn new(alias: impl Into<String>, expression: DerivedExpr) -> Self {
        Self {
            alias: alias.into(),
            expression,
        }
    }
}

/// An ordering field.
#[derive(Debug, Clone)]
pub struct OrderField {
    pub field: FieldRef,
    pub descending: bool,
}

impl OrderField {
    pub fn asc(entity: &str, field: &str) -> Self {
        Self {
            field: FieldRef::new(entity, field),
            descending: false,
        }
    }

    pub fn desc(entity: &str, field: &str) -> Self {
        Self {
            field: FieldRef::new(entity, field),
            descending: true,
        }
    }
}
