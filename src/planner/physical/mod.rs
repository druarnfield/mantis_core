//! Physical plan generation and optimization.

mod converter;
pub mod join_optimizer;
mod plan;

use converter::PhysicalConverter;
pub use plan::*;

use crate::planner::logical::LogicalPlan;
use crate::planner::PlanResult;
use crate::semantic::graph::UnifiedGraph;

/// Configuration for physical planner.
pub struct PhysicalPlannerConfig {
    /// Strategy for join order optimization.
    pub optimizer_strategy: join_optimizer::OptimizerStrategy,
}

impl Default for PhysicalPlannerConfig {
    fn default() -> Self {
        Self {
            optimizer_strategy: join_optimizer::OptimizerStrategy::Adaptive,
        }
    }
}

/// Physical planner that generates execution strategies
pub struct PhysicalPlanner<'a> {
    graph: &'a UnifiedGraph,
    config: PhysicalPlannerConfig,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self {
            graph,
            config: PhysicalPlannerConfig::default(),
        }
    }

    pub fn with_config(graph: &'a UnifiedGraph, config: PhysicalPlannerConfig) -> Self {
        Self { graph, config }
    }

    pub fn generate_candidates(&self, logical_plan: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        PhysicalConverter::new(self.graph, &self.config).convert(logical_plan)
    }
}
