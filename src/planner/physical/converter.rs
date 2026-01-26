//! Convert logical plans to physical execution plans.

use crate::planner::logical::LogicalPlan;
use crate::planner::physical::{PhysicalPlan, TableScanStrategy};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

/// Format a projection item as a column reference
fn format_projection_item(item: &crate::planner::logical::ProjectionItem) -> String {
    use crate::planner::logical::ProjectionItem;
    match item {
        ProjectionItem::Column(col) => format!("{}.{}", col.entity, col.column),
        ProjectionItem::Measure(m) => format!("{}.{}", m.entity, m.measure),
        ProjectionItem::Expr { alias, .. } => alias.clone().unwrap_or_else(|| "expr".to_string()),
    }
}

pub struct PhysicalConverter<'a> {
    #[allow(dead_code)]
    graph: &'a UnifiedGraph,
}

impl<'a> PhysicalConverter<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn convert(&self, logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        match logical {
            LogicalPlan::Scan(scan) => self.convert_scan(scan),
            LogicalPlan::Filter(filter) => self.convert_filter(filter),
            LogicalPlan::Aggregate(agg) => self.convert_aggregate(agg),
            LogicalPlan::Project(proj) => self.convert_project(proj),
            LogicalPlan::Sort(sort) => self.convert_sort(sort),
            LogicalPlan::Limit(limit) => self.convert_limit(limit),
            _ => Err(PlanError::PhysicalPlanError(
                "Logical plan node not yet supported".to_string(),
            )),
        }
    }

    fn convert_scan(
        &self,
        scan: &crate::planner::logical::ScanNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        Ok(vec![PhysicalPlan::TableScan {
            table: scan.entity.clone(),
            strategy: TableScanStrategy::FullScan,
            estimated_rows: None,
        }])
    }

    fn convert_filter(
        &self,
        filter: &crate::planner::logical::FilterNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        if filter.predicates.is_empty() {
            return Err(PlanError::PhysicalPlanError(
                "Filter node has no predicates".to_string(),
            ));
        }

        // For now, only support single predicates to avoid silently dropping data
        // Multiple predicates should be combined with AND in the logical plan
        if filter.predicates.len() > 1 {
            return Err(PlanError::PhysicalPlanError(
                format!(
                    "Multiple predicates not yet supported (found {}). Combine predicates with AND in logical plan.",
                    filter.predicates.len()
                )
            ));
        }

        let input_candidates = self.convert(&filter.input)?;
        Ok(input_candidates
            .into_iter()
            .map(|input| PhysicalPlan::Filter {
                input: Box::new(input),
                predicate: filter.predicates[0].clone(),
            })
            .collect())
    }

    fn convert_aggregate(
        &self,
        agg: &crate::planner::logical::AggregateNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        let input_candidates = self.convert(&agg.input)?;
        Ok(input_candidates
            .into_iter()
            .map(|input| PhysicalPlan::HashAggregate {
                input: Box::new(input),
                group_by: agg
                    .group_by
                    .iter()
                    .map(|col| format!("{}.{}", col.entity, col.column))
                    .collect(),
                aggregates: agg
                    .measures
                    .iter()
                    .map(|m| format!("{}.{}", m.entity, m.measure))
                    .collect(),
            })
            .collect())
    }

    fn convert_project(
        &self,
        proj: &crate::planner::logical::ProjectNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        let input_candidates = self.convert(&proj.input)?;
        Ok(input_candidates
            .into_iter()
            .map(|input| PhysicalPlan::Project {
                input: Box::new(input),
                columns: proj
                    .projections
                    .iter()
                    .map(format_projection_item)
                    .collect(),
            })
            .collect())
    }

    fn convert_sort(
        &self,
        sort: &crate::planner::logical::SortNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        let input_candidates = self.convert(&sort.input)?;
        Ok(input_candidates
            .into_iter()
            .map(|input| PhysicalPlan::Sort {
                input: Box::new(input),
                keys: sort
                    .order_by
                    .iter()
                    .map(|k| crate::planner::physical::SortKey {
                        column: k.column.clone(),
                        ascending: !k.descending,
                    })
                    .collect(),
            })
            .collect())
    }

    fn convert_limit(
        &self,
        limit: &crate::planner::logical::LimitNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        let input_candidates = self.convert(&limit.input)?;
        Ok(input_candidates
            .into_iter()
            .map(|input| PhysicalPlan::Limit {
                input: Box::new(input),
                limit: usize::try_from(limit.limit).unwrap_or(usize::MAX),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::expr::{BinaryOp, Expr as ModelExpr, Literal};
    use crate::planner::logical::{FilterNode, LogicalPlan, ScanNode};

    fn create_test_graph() -> UnifiedGraph {
        UnifiedGraph::new()
    }

    #[test]
    fn test_convert_filter_with_multiple_predicates_should_error() {
        let graph = create_test_graph();
        let converter = PhysicalConverter::new(&graph);

        // Create a filter with multiple predicates
        let predicate1 = ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(ModelExpr::Literal(Literal::Int(100))),
        };

        let predicate2 = ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "region".to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(ModelExpr::Literal(Literal::String("WEST".to_string()))),
        };

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                entity: "sales".to_string(),
            })),
            predicates: vec![predicate1, predicate2],
        });

        // Should return an error instead of silently dropping predicates
        let result = converter.convert(&filter);
        assert!(
            result.is_err(),
            "Converting filter with multiple predicates should return an error"
        );

        if let Err(e) = result {
            let error_msg = format!("{:?}", e);
            assert!(
                error_msg.contains("multiple predicates")
                    || error_msg.contains("Multiple predicates"),
                "Error message should mention multiple predicates, got: {}",
                error_msg
            );
        }
    }

    #[test]
    fn test_convert_filter_with_single_predicate_succeeds() {
        let graph = create_test_graph();
        let converter = PhysicalConverter::new(&graph);

        let predicate = ModelExpr::BinaryOp {
            left: Box::new(ModelExpr::Column {
                entity: Some("sales".to_string()),
                column: "amount".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(ModelExpr::Literal(Literal::Int(100))),
        };

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                entity: "sales".to_string(),
            })),
            predicates: vec![predicate.clone()],
        });

        let result = converter.convert(&filter);
        assert!(result.is_ok(), "Single predicate filter should succeed");

        let plans = result.unwrap();
        assert_eq!(plans.len(), 1);

        match &plans[0] {
            PhysicalPlan::Filter { predicate: p, .. } => {
                assert_eq!(p, &predicate);
            }
            _ => panic!("Expected Filter plan"),
        }
    }
}
