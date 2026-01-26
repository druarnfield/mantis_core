//! Physical plan generation and optimization.

mod converter;
pub mod join_optimizer;
mod plan;

use converter::PhysicalConverter;
pub use plan::*;

use crate::planner::logical::LogicalPlan;
use crate::planner::PlanResult;
use crate::semantic::graph::UnifiedGraph;

/// Physical planner that generates execution strategies
pub struct PhysicalPlanner<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn generate_candidates(&self, logical_plan: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        PhysicalConverter::new(self.graph).convert(logical_plan)
    }
}
