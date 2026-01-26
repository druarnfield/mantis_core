//! Join path resolution using UnifiedGraph.

use crate::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::{query::ColumnRef, Cardinality, GraphEdge, GraphNode, UnifiedGraph};

pub struct JoinBuilder<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> JoinBuilder<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    /// Build join tree for multiple tables.
    ///
    /// Supports both direct joins and multi-hop joins through intermediate tables.
    pub fn build_join_tree(&self, tables: &[String]) -> PlanResult<LogicalPlan> {
        if tables.is_empty() {
            return Err(PlanError::LogicalPlanError("No tables specified".into()));
        }

        if tables.len() == 1 {
            return Ok(LogicalPlan::Scan(ScanNode {
                entity: tables[0].clone(),
            }));
        }

        // Start with first table
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: tables[0].clone(),
        });

        // Join remaining tables in order
        for right_table in &tables[1..] {
            let left_table = self.get_rightmost_table(&plan);

            // Find path from left to right (may be multi-hop)
            let path = self
                .graph
                .find_path(&left_table, right_table)
                .map_err(|e| PlanError::LogicalPlanError(format!("No join path: {}", e)))?;

            // For each step in the path, create a join
            // This automatically handles multi-hop by creating intermediate joins
            for step in &path.steps {
                let (join_columns, cardinality) = self.get_join_info(&step.from, &step.to)?;

                plan = LogicalPlan::Join(JoinNode {
                    left: Box::new(plan),
                    right: Box::new(LogicalPlan::Scan(ScanNode {
                        entity: step.to.clone(),
                    })),
                    on: JoinCondition::Equi(join_columns),
                    join_type: JoinType::Inner,
                    cardinality: Some(cardinality),
                });
            }
        }

        Ok(plan)
    }

    /// Get join columns and cardinality from graph edge.
    fn get_join_info(
        &self,
        from: &str,
        to: &str,
    ) -> PlanResult<(Vec<(ColumnRef, ColumnRef)>, Cardinality)> {
        // Look up entity indices
        let from_idx = self
            .graph
            .entity_index(from)
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", from)))?;
        let to_idx = self
            .graph
            .entity_index(to)
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", to)))?;

        // Find edge between entities
        if let Some(edge_idx) = self.graph.graph().find_edge(from_idx, to_idx) {
            if let Some(GraphEdge::JoinsTo(edge)) = self.graph.graph().edge_weight(edge_idx) {
                // Convert (String, String) to (ColumnRef, ColumnRef)
                let columns = edge
                    .join_columns
                    .iter()
                    .map(|(from_col, to_col)| {
                        (
                            ColumnRef::new(from.to_string(), from_col.clone()),
                            ColumnRef::new(to.to_string(), to_col.clone()),
                        )
                    })
                    .collect();
                return Ok((columns, edge.cardinality));
            }
        }

        Err(PlanError::LogicalPlanError(format!(
            "No join relationship between {} and {}",
            from, to
        )))
    }

    /// Get the rightmost table in the join tree.
    fn get_rightmost_table(&self, plan: &LogicalPlan) -> String {
        match plan {
            LogicalPlan::Scan(scan) => scan.entity.clone(),
            LogicalPlan::Join(join) => self.get_rightmost_table(&join.right),
            _ => panic!("Unexpected plan node in join tree"),
        }
    }
}
