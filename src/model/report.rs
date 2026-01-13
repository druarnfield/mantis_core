//! Report entity definitions.
//!
//! Reports are flat collections of measures from potentially unrelated facts.
//! The system automatically routes filters to applicable measures via the
//! relationship graph.
//!
//! # Example
//!
//! ```lua
//! report "executive_dashboard" {
//!     measures = {
//!         "orders_fact.revenue",
//!         "orders_fact.order_count",
//!         "inventory_fact.stock_value",
//!         "support_fact.open_tickets",
//!     },
//!     filters = {
//!         "customers.region = 'EMEA'",
//!         "order_date >= '2024-01-01'",
//!     },
//!     group_by = { "order_date" },
//!
//!     -- Materialization (optional)
//!     materialized = true,
//!     table_type = TABLE,
//!     target_table = "analytics.rpt_executive_dashboard",
//!     refresh_delta = "4 hours",
//! }
//! ```

use serde::{Deserialize, Serialize};

/// Table type for report materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ReportTableType {
    /// Physical table - requires refresh_delta.
    #[default]
    Table,
    /// Database view - always current, no refresh needed.
    View,
}

/// Refresh interval for materialized report tables.
///
/// Specifies how often the report should be rebuilt.
/// The CLI checks if `now - last_refresh > delta` and triggers a full rebuild.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshDelta {
    /// Number of seconds in the refresh interval.
    pub seconds: u64,
    /// Original string representation (e.g., "4 hours", "30 minutes").
    pub original: String,
}

impl RefreshDelta {
    /// Create a new refresh delta from seconds.
    pub fn from_seconds(seconds: u64) -> Self {
        Self {
            seconds,
            original: format!("{} seconds", seconds),
        }
    }

    /// Create a refresh delta from minutes.
    pub fn from_minutes(minutes: u64) -> Self {
        Self {
            seconds: minutes * 60,
            original: format!("{} minutes", minutes),
        }
    }

    /// Create a refresh delta from hours.
    pub fn from_hours(hours: u64) -> Self {
        Self {
            seconds: hours * 3600,
            original: format!("{} hours", hours),
        }
    }

    /// Create a refresh delta from days.
    pub fn from_days(days: u64) -> Self {
        Self {
            seconds: days * 86400,
            original: format!("{} days", days),
        }
    }

    /// Parse a refresh delta from a string like "4 hours", "30 minutes", "1 day".
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().to_lowercase();
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.len() != 2 {
            return None;
        }

        let value: u64 = parts[0].parse().ok()?;
        let unit = parts[1].trim_end_matches('s'); // Handle plural

        let seconds = match unit {
            "second" => value,
            "minute" => value * 60,
            "hour" => value * 3600,
            "day" => value * 86400,
            "week" => value * 604800,
            _ => return None,
        };

        Some(Self {
            seconds,
            original: s,
        })
    }

    /// Get the duration as std::time::Duration.
    pub fn as_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.seconds)
    }
}

impl std::fmt::Display for RefreshDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.original)
    }
}

/// Materialization settings for reports.
///
/// Reports can optionally be materialized as physical tables or views.
/// Unlike facts, reports use a simple refresh model - the CLI checks
/// if the refresh delta has passed and rebuilds entirely (no incremental).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportMaterialization {
    /// Whether to materialize as a physical object.
    pub materialized: bool,

    /// What kind of physical object (TABLE or VIEW).
    pub table_type: ReportTableType,

    /// Target table/view name.
    pub target_table: String,

    /// Target schema (optional, uses default if not specified).
    pub target_schema: Option<String>,

    /// Refresh interval for TABLE materialization.
    ///
    /// The CLI will rematerialize when this delta has passed since last refresh.
    /// Only relevant for `table_type = Table`.
    pub refresh_delta: Option<RefreshDelta>,
}

impl ReportMaterialization {
    /// Create a new materialization config for a table.
    pub fn table(target_table: impl Into<String>, refresh_delta: RefreshDelta) -> Self {
        Self {
            materialized: true,
            table_type: ReportTableType::Table,
            target_table: target_table.into(),
            target_schema: None,
            refresh_delta: Some(refresh_delta),
        }
    }

    /// Create a new materialization config for a view.
    pub fn view(target_table: impl Into<String>) -> Self {
        Self {
            materialized: true,
            table_type: ReportTableType::View,
            target_table: target_table.into(),
            target_schema: None,
            refresh_delta: None,
        }
    }

    /// Set the target schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.target_schema = Some(schema.into());
        self
    }
}

/// A reference to a measure in a fact table.
///
/// Format: "fact_name.measure_name"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasureRef {
    /// The fact entity containing the measure.
    pub fact: String,
    /// The measure name within the fact.
    pub measure: String,
}

impl MeasureRef {
    /// Create a new measure reference.
    pub fn new(fact: impl Into<String>, measure: impl Into<String>) -> Self {
        Self {
            fact: fact.into(),
            measure: measure.into(),
        }
    }

    /// Parse a measure reference from "fact.measure" format.
    pub fn parse(s: &str) -> Option<Self> {
        let (fact, measure) = s.split_once('.')?;
        Some(Self {
            fact: fact.to_string(),
            measure: measure.to_string(),
        })
    }
}

impl std::fmt::Display for MeasureRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.fact, self.measure)
    }
}

/// Default settings for report consumers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReportDefaults {
    /// Default time range filter (e.g., "last_30_days", "last_12_months").
    pub time_range: Option<String>,
    /// Default row limit.
    pub limit: Option<u32>,
}

/// A report definition.
///
/// Reports collect measures from multiple facts into a single query result.
/// Filters are automatically routed to applicable facts based on the
/// relationship graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// Report name (unique identifier).
    pub name: String,

    /// Measures to include from various facts.
    ///
    /// Each measure is a reference in "fact.measure" format.
    pub measures: Vec<MeasureRef>,

    /// Filter expressions.
    ///
    /// Filters are SQL expressions that reference dimension columns.
    /// The planner routes each filter to facts that have a path to
    /// the dimension being filtered.
    pub filters: Vec<String>,

    /// Grouping dimensions.
    ///
    /// Results are grouped and aligned on these dimensions.
    /// All facts contributing to the report must have a path to
    /// the grouping dimensions (or NULL padding is used).
    pub group_by: Vec<String>,

    /// Default settings for consumers.
    pub defaults: Option<ReportDefaults>,

    /// Optional description.
    pub description: Option<String>,

    /// Materialization settings (optional).
    ///
    /// If set, the report can be materialized as a TABLE or VIEW.
    /// For TABLE, specify `refresh_delta` to control rebuild frequency.
    pub materialization: Option<ReportMaterialization>,
}

impl Report {
    /// Create a new report.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            measures: Vec::new(),
            filters: Vec::new(),
            group_by: Vec::new(),
            defaults: None,
            description: None,
            materialization: None,
        }
    }

    /// Add a measure reference.
    pub fn with_measure(mut self, fact: &str, measure: &str) -> Self {
        self.measures.push(MeasureRef::new(fact, measure));
        self
    }

    /// Add a measure reference from string format.
    pub fn with_measure_ref(mut self, measure_ref: &str) -> Self {
        if let Some(m) = MeasureRef::parse(measure_ref) {
            self.measures.push(m);
        }
        self
    }

    /// Add a filter expression.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Add a group by dimension.
    pub fn with_group_by(mut self, column: impl Into<String>) -> Self {
        self.group_by.push(column.into());
        self
    }

    /// Set default settings.
    pub fn with_defaults(mut self, defaults: ReportDefaults) -> Self {
        self.defaults = Some(defaults);
        self
    }

    /// Set description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set materialization settings.
    pub fn with_materialization(mut self, materialization: ReportMaterialization) -> Self {
        self.materialization = Some(materialization);
        self
    }

    /// Check if this report is materialized.
    pub fn is_materialized(&self) -> bool {
        self.materialization.as_ref().map(|m| m.materialized).unwrap_or(false)
    }

    /// Get the unique facts referenced by this report.
    pub fn referenced_facts(&self) -> Vec<&str> {
        let mut facts: Vec<&str> = self.measures.iter().map(|m| m.fact.as_str()).collect();
        facts.sort();
        facts.dedup();
        facts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_measure_ref_parse() {
        let m = MeasureRef::parse("orders_fact.revenue").unwrap();
        assert_eq!(m.fact, "orders_fact");
        assert_eq!(m.measure, "revenue");
    }

    #[test]
    fn test_measure_ref_display() {
        let m = MeasureRef::new("orders_fact", "revenue");
        assert_eq!(m.to_string(), "orders_fact.revenue");
    }

    #[test]
    fn test_report_builder() {
        let report = Report::new("executive_dashboard")
            .with_measure("orders_fact", "revenue")
            .with_measure("orders_fact", "order_count")
            .with_measure("inventory_fact", "stock_value")
            .with_filter("customers.region = 'EMEA'")
            .with_group_by("order_date");

        assert_eq!(report.measures.len(), 3);
        assert_eq!(report.filters.len(), 1);
        assert_eq!(report.group_by.len(), 1);
    }

    #[test]
    fn test_referenced_facts() {
        let report = Report::new("test")
            .with_measure("orders_fact", "revenue")
            .with_measure("orders_fact", "order_count")
            .with_measure("inventory_fact", "stock_value");

        let facts = report.referenced_facts();
        assert_eq!(facts, vec!["inventory_fact", "orders_fact"]);
    }

    #[test]
    fn test_report_serialization() {
        let report = Report::new("test")
            .with_measure("orders_fact", "revenue")
            .with_filter("year = 2024");

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: Report = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, report.name);
        assert_eq!(deserialized.measures.len(), 1);
    }
}
