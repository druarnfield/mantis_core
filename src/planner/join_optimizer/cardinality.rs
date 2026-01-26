// src/planner/join_optimizer/cardinality.rs
use crate::planner::join_optimizer::join_graph::JoinEdge;
use crate::semantic::graph::{Cardinality, UnifiedGraph};

pub struct CardinalityEstimator<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> CardinalityEstimator<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    /// Estimate output rows for a join.
    pub fn estimate_join_output(
        &self,
        left_rows: usize,
        right_rows: usize,
        join_info: &JoinEdge,
    ) -> usize {
        match join_info.cardinality {
            Cardinality::OneToOne => left_rows.min(right_rows),
            Cardinality::OneToMany => right_rows,
            Cardinality::ManyToOne => left_rows,
            Cardinality::ManyToMany => {
                // For now, use simple heuristic
                // TODO: Use join column cardinality
                (left_rows as f64 * (right_rows as f64).sqrt()) as usize
            }
            Cardinality::Unknown => {
                // Conservative estimate for unknown cardinality
                left_rows.max(right_rows)
            }
        }
    }
}
