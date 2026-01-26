//! SQL query planner - converts model::Report to optimized SQL.
//!
//! Three-phase architecture:
//! 1. Logical Planning: Report → LogicalPlan (abstract operations)
//! 2. Physical Candidates: LogicalPlan → Vec<PhysicalPlan> (alternative strategies)
//! 3. Cost Estimation: Vec<PhysicalPlan> → Query (pick best, emit SQL)

pub mod cost;
pub mod expr_converter;
pub mod join_builder;
pub mod join_optimizer;
pub mod logical;
pub mod physical;

pub use expr_converter::{ExprConverter, QueryContext};
pub use join_builder::JoinBuilder;

use crate::model::Report;
use crate::semantic::graph::UnifiedGraph;
use crate::sql::query::Query;
use thiserror::Error;

/// Errors that can occur during planning.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Logical planning failed: {0}")]
    LogicalPlanError(String),

    #[error("Physical planning failed: {0}")]
    PhysicalPlanError(String),

    #[error("Cost estimation failed: {0}")]
    CostEstimationError(String),

    #[error("No valid physical plans generated")]
    NoValidPlans,
}

pub type PlanResult<T> = Result<T, PlanError>;

/// Main entry point for SQL planning.
pub struct SqlPlanner<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> SqlPlanner<'a> {
    /// Create a new SQL planner with access to the unified graph.
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    /// Plan a report into an optimized SQL query.
    ///
    /// This orchestrates all three phases:
    /// 1. Build logical plan from report
    /// 2. Generate physical plan candidates
    /// 3. Estimate costs and select best plan
    pub fn plan(&self, report: &Report) -> PlanResult<Query> {
        // Phase 1: Logical planning
        let logical_plan = logical::LogicalPlanner::new(self.graph).plan(report)?;

        // Phase 2: Generate physical candidates
        let candidates =
            physical::PhysicalPlanner::new(self.graph).generate_candidates(&logical_plan)?;

        // Phase 3: Cost estimation and selection
        let best_plan = cost::CostEstimator::new(self.graph).select_best(candidates)?;

        // Convert to Query
        Ok(best_plan.to_query())
    }
}
