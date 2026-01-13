//! Report Planner - Multi-fact query generation.
//!
//! Reports combine measures from multiple (potentially unrelated) facts.
//! The planner:
//! 1. Groups measures by their source fact
//! 2. Routes filters to applicable facts via `ModelGraph::validate_safe_path()`
//! 3. Generates a CTE for each fact with its measures and applicable filters
//! 4. Joins all CTEs with FULL OUTER JOIN on the group_by columns
//!
//! # Example
//!
//! ```text
//! report "executive_dashboard" {
//!     measures = {
//!         "orders_fact.revenue",       -- from orders_fact
//!         "inventory_fact.stock_value", -- from inventory_fact
//!     },
//!     filters = {
//!         "customers.region = 'EMEA'",  -- only applies to orders_fact
//!     },
//!     group_by = { "date.month" },
//! }
//! ```
//!
//! Generates:
//!
//! ```sql
//! WITH orders_metrics AS (
//!     SELECT d.month, SUM(f.revenue) as revenue
//!     FROM orders_fact f
//!     JOIN customers c ON ...
//!     JOIN date d ON ...
//!     WHERE c.region = 'EMEA'
//!     GROUP BY d.month
//! ),
//! inventory_metrics AS (
//!     SELECT d.month, SUM(f.stock_value) as stock_value
//!     FROM inventory_fact f
//!     JOIN date d ON ...
//!     -- No region filter (no path to customers)
//!     GROUP BY d.month
//! )
//! SELECT
//!     COALESCE(o.month, i.month) as month,
//!     o.revenue,
//!     i.stock_value
//! FROM orders_metrics o
//! FULL OUTER JOIN inventory_metrics i ON o.month = i.month
//! ```

mod planner;
mod emitter;
mod pivot_planner;
mod pivot_emitter;

#[cfg(test)]
mod tests;

// Re-export planner types
pub use planner::{
    FactCte,
    OutputColumn,
    PlannedMeasure,
    ReportPlan,
    ReportPlanner,
};


// Re-export emitter types
pub use emitter::ReportEmitter;

// Re-export pivot planner types
pub use pivot_planner::{
    PivotColumnValues,
    PivotDimension,
    PivotMeasure,
    PivotPlan,
    PivotPlanner,
    PivotSortPlan,
    PivotTotals,
};

// Re-export pivot emitter types
pub use pivot_emitter::PivotEmitter;
