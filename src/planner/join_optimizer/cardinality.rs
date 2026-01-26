// src/planner/join_optimizer/cardinality.rs
use crate::model::expr::{BinaryOp, Expr};
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

    /// Estimate selectivity of a filter predicate (0.0 to 1.0).
    pub fn estimate_filter_selectivity(&self, filter: &Expr) -> f64 {
        match filter {
            Expr::BinaryOp { op, left, right } => match op {
                BinaryOp::Eq => {
                    // Equality: default 10% selectivity
                    // TODO: Use column cardinality from graph
                    0.1
                }
                BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte => {
                    // Range predicates: 33% selectivity
                    0.33
                }
                BinaryOp::And => {
                    // AND combines multiplicatively
                    let s1 = self.estimate_filter_selectivity(left);
                    let s2 = self.estimate_filter_selectivity(right);
                    s1 * s2
                }
                BinaryOp::Or => {
                    // OR combines with probability union
                    let s1 = self.estimate_filter_selectivity(left);
                    let s2 = self.estimate_filter_selectivity(right);
                    s1 + s2 - (s1 * s2)
                }
                _ => 0.5, // Default for other operators
            },
            _ => 0.5, // Default for other expressions
        }
    }
}
