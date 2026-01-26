//! Cost estimation for physical plans.

use crate::planner::physical::PhysicalPlan;
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

pub struct CostEstimator<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> CostEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn select_best(&self, candidates: Vec<PhysicalPlan>) -> PlanResult<PhysicalPlan> {
        if candidates.is_empty() {
            return Err(PlanError::NoValidPlans);
        }

        // For now, use simple heuristic: prefer plans with lower estimated_rows
        let best = candidates
            .into_iter()
            .min_by_key(|plan| self.estimate_cost(plan))
            .unwrap();

        Ok(best)
    }

    fn estimate_cost(&self, plan: &PhysicalPlan) -> u64 {
        match plan {
            PhysicalPlan::TableScan { estimated_rows, .. } => {
                estimated_rows.unwrap_or(u64::MAX as usize) as u64
            }
            PhysicalPlan::Filter { input, .. } => {
                // Assume filter reduces rows by 10%
                self.estimate_cost(input) / 10
            }
            PhysicalPlan::HashJoin { estimated_rows, .. } => {
                estimated_rows.unwrap_or(u64::MAX as usize) as u64
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // NLJ cost is roughly O(left * right)
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                left_cost.saturating_mul(right_cost)
            }
            PhysicalPlan::HashAggregate { input, .. } => {
                // Hash aggregate roughly same cost as input
                self.estimate_cost(input)
            }
            PhysicalPlan::Sort { input, .. } => {
                // Sort is O(n log n)
                let input_cost = self.estimate_cost(input);
                if input_cost > 0 {
                    let log_factor = (input_cost as f64).log2() as u64;
                    input_cost.saturating_mul(log_factor)
                } else {
                    0
                }
            }
            PhysicalPlan::Project { input, .. } => {
                // Project has same cost as input
                self.estimate_cost(input)
            }
            PhysicalPlan::Limit { input, limit } => {
                // Limit reduces cost
                std::cmp::min(self.estimate_cost(input), *limit as u64)
            }
        }
    }
}
