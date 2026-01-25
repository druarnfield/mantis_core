//! Role-playing dimension types.
//!
//! Role-playing dimensions allow the same dimension to be used with different meanings
//! in a single fact table. For example, a date dimension can represent order_date,
//! ship_date, and delivery_date in an orders fact.
//!
//! # Example
//!
//! ```lua
//! -- Define roles in link statements
//! link(orders.order_date_id, date.date_id) as order_date
//! link(orders.ship_date_id, date.date_id) as ship_date
//!
//! -- Usage in queries
//! query {
//!     order_date.month,    -- Uses order_date_id join
//!     ship_date.month,     -- Uses ship_date_id join
//!     revenue,
//! }
//! ```

use serde::{Deserialize, Serialize};

/// A role-playing dimension - same dimension used with different meanings.
///
/// Role-playing dimensions solve the ambiguity when a fact has multiple foreign keys
/// to the same dimension table. For example, an orders fact might have:
/// - `order_date_id` → date dimension (when was it ordered?)
/// - `ship_date_id` → date dimension (when did it ship?)
/// - `delivery_date_id` → date dimension (when was it delivered?)
///
/// Each role creates an alias that can be used in queries to specify which
/// relationship to traverse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DimensionRole {
    /// Role name (e.g., "order_date", "ship_date").
    ///
    /// This becomes an alias that can be used in queries like `order_date.month`.
    pub name: String,

    /// The FK column on the fact (e.g., "order_date_id").
    pub fk_column: String,

    /// The target dimension entity (e.g., "date").
    pub dimension: String,

    /// The PK column on the dimension (e.g., "date_id").
    pub pk_column: String,
}

impl DimensionRole {
    /// Create a new dimension role.
    pub fn new(
        name: impl Into<String>,
        fk_column: impl Into<String>,
        dimension: impl Into<String>,
        pk_column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            fk_column: fk_column.into(),
            dimension: dimension.into(),
            pk_column: pk_column.into(),
        }
    }
}

/// Date dimension configuration for a fact.
///
/// Configures how date dimensions are used in a fact, including:
/// - Which roles are available (order_date, ship_date, etc.)
/// - Which role is the primary date (used by default for time intelligence)
/// - How to detect the grain (year, quarter, month, week, day columns)
///
/// # Example
///
/// ```rust,ignore
/// let date_config = DateConfig::new()
///     .with_role(DimensionRole::new("order_date", "order_date_id", "date", "date_id"))
///     .with_role(DimensionRole::new("ship_date", "ship_date_id", "date", "date_id"))
///     .with_primary_role("order_date")
///     .with_grain_columns(GrainColumns::new("year")
///         .with_quarter("quarter")
///         .with_month("month")
///         .with_day("day"));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateConfig {
    /// All date roles for this fact.
    pub roles: Vec<DimensionRole>,

    /// Primary role for time intelligence (defaults to first role).
    ///
    /// This role is used when time intelligence functions like `ytd()` or
    /// `prior_year()` are called without an explicit `via` clause.
    pub primary_role: Option<String>,

    /// Grain detection columns (year, quarter, month, week, day).
    ///
    /// These columns are used by time intelligence functions to understand
    /// the temporal structure of the date dimension.
    pub grain_columns: Option<GrainColumns>,
}

impl DateConfig {
    /// Create a new empty date configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a date role.
    pub fn with_role(mut self, role: DimensionRole) -> Self {
        self.roles.push(role);
        self
    }

    /// Add a date role (mutable).
    pub fn add_role(&mut self, role: DimensionRole) {
        self.roles.push(role);
    }

    /// Set the primary date role.
    pub fn with_primary_role(mut self, name: impl Into<String>) -> Self {
        self.primary_role = Some(name.into());
        self
    }

    /// Set the grain columns configuration.
    pub fn with_grain_columns(mut self, grain: GrainColumns) -> Self {
        self.grain_columns = Some(grain);
        self
    }

    /// Get the primary role, or the first role if not explicitly set.
    pub fn get_primary_role(&self) -> Option<&DimensionRole> {
        if let Some(ref primary_name) = self.primary_role {
            self.roles.iter().find(|r| &r.name == primary_name)
        } else {
            self.roles.first()
        }
    }

    /// Find a role by name.
    pub fn get_role(&self, name: &str) -> Option<&DimensionRole> {
        self.roles.iter().find(|r| r.name == name)
    }

    /// Check if there are multiple roles to the same dimension.
    ///
    /// This helps detect when disambiguation is required.
    pub fn has_ambiguous_dimension(&self, dimension: &str) -> bool {
        self.roles.iter().filter(|r| r.dimension == dimension).count() > 1
    }

    /// Get all role names.
    pub fn role_names(&self) -> Vec<&str> {
        self.roles.iter().map(|r| r.name.as_str()).collect()
    }
}

/// Grain detection columns for date dimensions.
///
/// These columns tell time intelligence functions how to partition
/// and order data for period-to-date and prior period calculations.
///
/// # SQL Generation Examples
///
/// - `ytd(revenue)` uses `year` for partitioning and `month` for ordering
/// - `prior_year(revenue)` uses `year` to compute LAG offset
/// - `rolling_sum(revenue, 3)` uses the finest available grain for ordering
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrainColumns {
    /// Year column name (required for time intelligence).
    pub year: String,

    /// Quarter column name (optional, for QTD calculations).
    pub quarter: Option<String>,

    /// Month column name (optional, for MTD and monthly calculations).
    pub month: Option<String>,

    /// Week column name (optional, for weekly calculations).
    pub week: Option<String>,

    /// Day column name (optional, for daily calculations).
    pub day: Option<String>,
}

impl GrainColumns {
    /// Create grain columns with just the year column.
    pub fn new(year: impl Into<String>) -> Self {
        Self {
            year: year.into(),
            quarter: None,
            month: None,
            week: None,
            day: None,
        }
    }

    /// Set the quarter column.
    pub fn with_quarter(mut self, column: impl Into<String>) -> Self {
        self.quarter = Some(column.into());
        self
    }

    /// Set the month column.
    pub fn with_month(mut self, column: impl Into<String>) -> Self {
        self.month = Some(column.into());
        self
    }

    /// Set the week column.
    pub fn with_week(mut self, column: impl Into<String>) -> Self {
        self.week = Some(column.into());
        self
    }

    /// Set the day column.
    pub fn with_day(mut self, column: impl Into<String>) -> Self {
        self.day = Some(column.into());
        self
    }

    /// Get the finest grain available.
    ///
    /// Returns the most granular column that's defined, in order:
    /// day > week > month > quarter > year
    pub fn finest_grain(&self) -> &str {
        if self.day.is_some() {
            self.day.as_ref().unwrap()
        } else if self.week.is_some() {
            self.week.as_ref().unwrap()
        } else if self.month.is_some() {
            self.month.as_ref().unwrap()
        } else if self.quarter.is_some() {
            self.quarter.as_ref().unwrap()
        } else {
            &self.year
        }
    }

    /// Determine the time grain based on available columns.
    pub fn detected_grain(&self) -> TimeGrain {
        if self.day.is_some() {
            TimeGrain::Daily
        } else if self.week.is_some() {
            TimeGrain::Weekly
        } else if self.month.is_some() {
            TimeGrain::Monthly
        } else if self.quarter.is_some() {
            TimeGrain::Quarterly
        } else {
            TimeGrain::Yearly
        }
    }
}

/// Time grain for date dimension.
///
/// Used by time intelligence functions to determine:
/// - How many periods to lag for prior_year (12 for monthly, 4 for quarterly, etc.)
/// - What partitioning to use for YTD calculations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeGrain {
    /// Daily grain (365 periods per year).
    Daily,
    /// Weekly grain (52 periods per year).
    Weekly,
    /// Monthly grain (12 periods per year).
    Monthly,
    /// Quarterly grain (4 periods per year).
    Quarterly,
    /// Yearly grain (1 period per year).
    Yearly,
}

impl TimeGrain {
    /// Get the number of periods per year for this grain.
    ///
    /// Used by `prior_year()` to determine LAG offset.
    pub fn periods_per_year(&self) -> u32 {
        match self {
            TimeGrain::Daily => 365,
            TimeGrain::Weekly => 52,
            TimeGrain::Monthly => 12,
            TimeGrain::Quarterly => 4,
            TimeGrain::Yearly => 1,
        }
    }

    /// Get the number of periods per quarter for this grain.
    ///
    /// Used by `prior_quarter()` to determine LAG offset.
    pub fn periods_per_quarter(&self) -> u32 {
        match self {
            TimeGrain::Daily => 91,   // ~365/4
            TimeGrain::Weekly => 13,  // ~52/4
            TimeGrain::Monthly => 3,
            TimeGrain::Quarterly => 1,
            TimeGrain::Yearly => 0,   // N/A
        }
    }
}

impl std::fmt::Display for TimeGrain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeGrain::Daily => write!(f, "daily"),
            TimeGrain::Weekly => write!(f, "weekly"),
            TimeGrain::Monthly => write!(f, "monthly"),
            TimeGrain::Quarterly => write!(f, "quarterly"),
            TimeGrain::Yearly => write!(f, "yearly"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dimension_role_new() {
        let role = DimensionRole::new("order_date", "order_date_id", "date", "date_id");

        assert_eq!(role.name, "order_date");
        assert_eq!(role.fk_column, "order_date_id");
        assert_eq!(role.dimension, "date");
        assert_eq!(role.pk_column, "date_id");
    }

    #[test]
    fn test_date_config_builder() {
        let config = DateConfig::new()
            .with_role(DimensionRole::new("order_date", "order_date_id", "date", "date_id"))
            .with_role(DimensionRole::new("ship_date", "ship_date_id", "date", "date_id"))
            .with_primary_role("order_date")
            .with_grain_columns(
                GrainColumns::new("year")
                    .with_quarter("quarter")
                    .with_month("month")
                    .with_day("day"),
            );

        assert_eq!(config.roles.len(), 2);
        assert_eq!(config.primary_role, Some("order_date".into()));
        assert!(config.grain_columns.is_some());
    }

    #[test]
    fn test_date_config_get_primary_role() {
        let config = DateConfig::new()
            .with_role(DimensionRole::new("order_date", "order_date_id", "date", "date_id"))
            .with_role(DimensionRole::new("ship_date", "ship_date_id", "date", "date_id"))
            .with_primary_role("ship_date");

        let primary = config.get_primary_role().unwrap();
        assert_eq!(primary.name, "ship_date");
    }

    #[test]
    fn test_date_config_get_primary_role_defaults_to_first() {
        let config = DateConfig::new()
            .with_role(DimensionRole::new("order_date", "order_date_id", "date", "date_id"))
            .with_role(DimensionRole::new("ship_date", "ship_date_id", "date", "date_id"));

        let primary = config.get_primary_role().unwrap();
        assert_eq!(primary.name, "order_date");
    }

    #[test]
    fn test_date_config_has_ambiguous_dimension() {
        let config = DateConfig::new()
            .with_role(DimensionRole::new("order_date", "order_date_id", "date", "date_id"))
            .with_role(DimensionRole::new("ship_date", "ship_date_id", "date", "date_id"))
            .with_role(DimensionRole::new("region", "region_id", "region", "region_id"));

        assert!(config.has_ambiguous_dimension("date"));
        assert!(!config.has_ambiguous_dimension("region"));
    }

    #[test]
    fn test_grain_columns_builder() {
        let grain = GrainColumns::new("fiscal_year")
            .with_quarter("fiscal_quarter")
            .with_month("fiscal_month")
            .with_week("fiscal_week")
            .with_day("calendar_day");

        assert_eq!(grain.year, "fiscal_year");
        assert_eq!(grain.quarter, Some("fiscal_quarter".into()));
        assert_eq!(grain.month, Some("fiscal_month".into()));
        assert_eq!(grain.week, Some("fiscal_week".into()));
        assert_eq!(grain.day, Some("calendar_day".into()));
    }

    #[test]
    fn test_grain_columns_finest_grain() {
        // Daily is finest
        let daily = GrainColumns::new("year")
            .with_month("month")
            .with_day("day");
        assert_eq!(daily.finest_grain(), "day");

        // Weekly is finest
        let weekly = GrainColumns::new("year")
            .with_month("month")
            .with_week("week");
        assert_eq!(weekly.finest_grain(), "week");

        // Monthly is finest
        let monthly = GrainColumns::new("year").with_month("month");
        assert_eq!(monthly.finest_grain(), "month");

        // Quarterly is finest
        let quarterly = GrainColumns::new("year").with_quarter("quarter");
        assert_eq!(quarterly.finest_grain(), "quarter");

        // Only year
        let yearly = GrainColumns::new("year");
        assert_eq!(yearly.finest_grain(), "year");
    }

    #[test]
    fn test_grain_columns_detected_grain() {
        let daily = GrainColumns::new("year").with_day("day");
        assert_eq!(daily.detected_grain(), TimeGrain::Daily);

        let monthly = GrainColumns::new("year").with_month("month");
        assert_eq!(monthly.detected_grain(), TimeGrain::Monthly);

        let yearly = GrainColumns::new("year");
        assert_eq!(yearly.detected_grain(), TimeGrain::Yearly);
    }

    #[test]
    fn test_time_grain_periods_per_year() {
        assert_eq!(TimeGrain::Daily.periods_per_year(), 365);
        assert_eq!(TimeGrain::Weekly.periods_per_year(), 52);
        assert_eq!(TimeGrain::Monthly.periods_per_year(), 12);
        assert_eq!(TimeGrain::Quarterly.periods_per_year(), 4);
        assert_eq!(TimeGrain::Yearly.periods_per_year(), 1);
    }

    #[test]
    fn test_time_grain_display() {
        assert_eq!(format!("{}", TimeGrain::Daily), "daily");
        assert_eq!(format!("{}", TimeGrain::Monthly), "monthly");
        assert_eq!(format!("{}", TimeGrain::Yearly), "yearly");
    }

    #[test]
    fn test_dimension_role_serialization() {
        let role = DimensionRole::new("order_date", "order_date_id", "date", "date_id");

        let json = serde_json::to_string(&role).unwrap();
        let deserialized: DimensionRole = serde_json::from_str(&json).unwrap();

        assert_eq!(role, deserialized);
    }

    #[test]
    fn test_date_config_serialization() {
        let config = DateConfig::new()
            .with_role(DimensionRole::new("order_date", "order_date_id", "date", "date_id"))
            .with_primary_role("order_date")
            .with_grain_columns(GrainColumns::new("year").with_month("month"));

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DateConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config, deserialized);
    }
}
