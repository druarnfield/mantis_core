//! Cost estimation - selects best physical plan.

use crate::planner::physical::PhysicalPlan;
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

/// Cost estimator.
pub struct CostEstimator<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> CostEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn select_best(&self, mut candidates: Vec<PhysicalPlan>) -> PlanResult<PhysicalPlan> {
        candidates.pop().ok_or(PlanError::NoValidPlans)
    }
}
