//! Pivot report entity definitions.
//!
//! Pivot reports are cross-tab/matrix reports with rows, columns, and values -
//! like Excel pivot tables. They generate dialect-specific pivot SQL.
//!
//! # Example
//!
//! ```lua
//! pivot_report "quarterly_sales" {
//!     rows = {
//!         "customers.region",
//!         "customers.segment",
//!     },
//!     columns = "time.quarter",
//!     values = {
//!         revenue = { measure = "orders_fact.revenue" },
//!         quantity = { measure = "orders_fact.order_count" },
//!     },
//!     filters = {
//!         "time.year = 2024",
//!     },
//!     totals = {
//!         rows = true,
//!         columns = true,
//!         grand = true,
//!     },
//! }
//! ```

use serde::{Deserialize, Serialize};

use super::report::{MeasureRef, ReportMaterialization};

/// Column specification for a pivot report.
///
/// Can be a simple dimension reference or explicit column values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PivotColumns {
    /// Dynamic: query distinct values from the dimension.
    ///
    /// The planner will execute `SELECT DISTINCT` to discover values.
    Dynamic(String),

    /// Explicit: user-specified list of column values.
    ///
    /// More predictable output - the columns are known ahead of time.
    Explicit {
        /// The dimension to pivot on.
        dimension: String,
        /// The specific values that become column headers.
        values: Vec<String>,
    },
}

impl PivotColumns {
    /// Get the dimension being pivoted.
    pub fn dimension(&self) -> &str {
        match self {
            PivotColumns::Dynamic(d) => d,
            PivotColumns::Explicit { dimension, .. } => dimension,
        }
    }
}

/// A value (measure) in a pivot report cell.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PivotValue {
    /// Name for this value (used in column headers).
    pub name: String,
    /// The measure to aggregate.
    pub measure: MeasureRef,
    /// Optional format string (e.g., "currency", "percent").
    pub format: Option<String>,
}

impl PivotValue {
    /// Create a new pivot value.
    pub fn new(name: impl Into<String>, fact: &str, measure: &str) -> Self {
        Self {
            name: name.into(),
            measure: MeasureRef::new(fact, measure),
            format: None,
        }
    }

    /// Set format string.
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }
}

/// Totals configuration for pivot reports.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TotalsConfig {
    /// Add row totals (rightmost column).
    pub rows: bool,
    /// Add column totals (bottom row).
    pub columns: bool,
    /// Add grand total (bottom-right cell).
    pub grand: bool,
}

impl TotalsConfig {
    /// Create config with all totals enabled.
    pub fn all() -> Self {
        Self {
            rows: true,
            columns: true,
            grand: true,
        }
    }

    /// Check if any totals are enabled.
    pub fn any(&self) -> bool {
        self.rows || self.columns || self.grand
    }
}

/// Sort direction for pivot results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Sort configuration for pivot reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PivotSort {
    /// Column to sort by (usually a value name like "revenue").
    pub by: String,
    /// Sort direction.
    pub direction: SortDirection,
}

impl PivotSort {
    /// Create ascending sort.
    pub fn asc(by: impl Into<String>) -> Self {
        Self {
            by: by.into(),
            direction: SortDirection::Asc,
        }
    }

    /// Create descending sort.
    pub fn desc(by: impl Into<String>) -> Self {
        Self {
            by: by.into(),
            direction: SortDirection::Desc,
        }
    }
}

/// A pivot report definition.
///
/// Pivot reports create cross-tab/matrix output with:
/// - Row dimensions on the left
/// - Column dimension values as headers
/// - Aggregated measures in cells
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PivotReport {
    /// Report name (unique identifier).
    pub name: String,

    /// Row dimensions (left side of pivot).
    ///
    /// These become the row headers. Multiple dimensions create
    /// a hierarchy (e.g., region > segment).
    pub rows: Vec<String>,

    /// Column dimension (values become column headers).
    ///
    /// Can be dynamic (query distinct values) or explicit (fixed list).
    pub columns: PivotColumns,

    /// Values to aggregate in each cell.
    ///
    /// Each value becomes a column for each pivot column value.
    /// E.g., with columns Q1-Q4 and values [revenue, count]:
    /// Q1_revenue, Q1_count, Q2_revenue, Q2_count, ...
    pub values: Vec<PivotValue>,

    /// Filter expressions.
    pub filters: Vec<String>,

    /// Totals configuration.
    pub totals: Option<TotalsConfig>,

    /// Sort configuration.
    pub sort: Option<PivotSort>,

    /// Optional description.
    pub description: Option<String>,

    /// Materialization settings (optional).
    ///
    /// If set, the pivot report can be materialized as a TABLE or VIEW.
    /// For TABLE, specify `refresh_delta` to control rebuild frequency.
    pub materialization: Option<ReportMaterialization>,
}

impl PivotReport {
    /// Create a new pivot report.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            rows: Vec::new(),
            columns: PivotColumns::Dynamic(String::new()),
            values: Vec::new(),
            filters: Vec::new(),
            totals: None,
            sort: None,
            description: None,
            materialization: None,
        }
    }

    /// Add a row dimension.
    pub fn with_row(mut self, dimension: impl Into<String>) -> Self {
        self.rows.push(dimension.into());
        self
    }

    /// Set the pivot column dimension (dynamic).
    pub fn with_columns(mut self, dimension: impl Into<String>) -> Self {
        self.columns = PivotColumns::Dynamic(dimension.into());
        self
    }

    /// Set the pivot column dimension with explicit values.
    pub fn with_columns_explicit(
        mut self,
        dimension: impl Into<String>,
        values: Vec<String>,
    ) -> Self {
        self.columns = PivotColumns::Explicit {
            dimension: dimension.into(),
            values,
        };
        self
    }

    /// Add a pivot value.
    pub fn with_value(mut self, name: &str, fact: &str, measure: &str) -> Self {
        self.values.push(PivotValue::new(name, fact, measure));
        self
    }

    /// Add a filter expression.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Set totals configuration.
    pub fn with_totals(mut self, totals: TotalsConfig) -> Self {
        self.totals = Some(totals);
        self
    }

    /// Set sort configuration.
    pub fn with_sort(mut self, sort: PivotSort) -> Self {
        self.sort = Some(sort);
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

    /// Check if this pivot report is materialized.
    pub fn is_materialized(&self) -> bool {
        self.materialization.as_ref().map(|m| m.materialized).unwrap_or(false)
    }

    /// Get the unique facts referenced by this pivot report.
    pub fn referenced_facts(&self) -> Vec<&str> {
        let mut facts: Vec<&str> = self.values.iter().map(|v| v.measure.fact.as_str()).collect();
        facts.sort();
        facts.dedup();
        facts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pivot_columns_dimension() {
        let dynamic = PivotColumns::Dynamic("time.quarter".to_string());
        assert_eq!(dynamic.dimension(), "time.quarter");

        let explicit = PivotColumns::Explicit {
            dimension: "time.quarter".to_string(),
            values: vec!["Q1".to_string(), "Q2".to_string()],
        };
        assert_eq!(explicit.dimension(), "time.quarter");
    }

    #[test]
    fn test_totals_config() {
        let all = TotalsConfig::all();
        assert!(all.rows);
        assert!(all.columns);
        assert!(all.grand);
        assert!(all.any());

        let none = TotalsConfig::default();
        assert!(!none.any());
    }

    #[test]
    fn test_pivot_report_builder() {
        let report = PivotReport::new("quarterly_sales")
            .with_row("customers.region")
            .with_row("customers.segment")
            .with_columns("time.quarter")
            .with_value("revenue", "orders_fact", "revenue")
            .with_value("orders", "orders_fact", "order_count")
            .with_filter("time.year = 2024")
            .with_totals(TotalsConfig::all())
            .with_sort(PivotSort::desc("revenue"));

        assert_eq!(report.rows.len(), 2);
        assert_eq!(report.values.len(), 2);
        assert_eq!(report.filters.len(), 1);
        assert!(report.totals.is_some());
        assert!(report.sort.is_some());
    }

    #[test]
    fn test_explicit_columns() {
        let report = PivotReport::new("by_quarter")
            .with_columns_explicit(
                "time.quarter",
                vec!["2024-Q1".to_string(), "2024-Q2".to_string()],
            );

        match &report.columns {
            PivotColumns::Explicit { dimension, values } => {
                assert_eq!(dimension, "time.quarter");
                assert_eq!(values.len(), 2);
            }
            _ => panic!("Expected explicit columns"),
        }
    }

    #[test]
    fn test_referenced_facts() {
        let report = PivotReport::new("test")
            .with_value("revenue", "orders_fact", "revenue")
            .with_value("stock", "inventory_fact", "stock_value");

        let facts = report.referenced_facts();
        assert_eq!(facts, vec!["inventory_fact", "orders_fact"]);
    }

    #[test]
    fn test_pivot_report_serialization() {
        let report = PivotReport::new("test")
            .with_row("region")
            .with_columns("quarter")
            .with_value("revenue", "orders_fact", "revenue");

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: PivotReport = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, report.name);
        assert_eq!(deserialized.rows.len(), 1);
        assert_eq!(deserialized.values.len(), 1);
    }
}
