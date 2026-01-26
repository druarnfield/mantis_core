//! Physical plan generation and optimization.

mod plan;
pub use plan::*;

use crate::planner::logical::LogicalPlan;
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

/// Physical planner that generates execution strategies
pub struct PhysicalPlanner<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn generate_candidates(
        &self,
        _logical_plan: &LogicalPlan,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        Err(PlanError::PhysicalPlanError(
            "Not yet implemented".to_string(),
        ))
    }
}
