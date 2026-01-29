//! Convert logical plans to physical execution plans.

use crate::planner::logical::LogicalPlan;
use crate::planner::physical::join_optimizer::JoinOrderOptimizer;
use crate::planner::physical::{PhysicalPlan, TableScanStrategy};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

pub struct PhysicalConverter<'a> {
    graph: &'a UnifiedGraph,
    config: &'a crate::planner::physical::PhysicalPlannerConfig,
}

impl<'a> PhysicalConverter<'a> {
    pub fn new(
        graph: &'a UnifiedGraph,
        config: &'a crate::planner::physical::PhysicalPlannerConfig,
    ) -> Self {
        Self { graph, config }
    }

    pub fn convert(&self, logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        match logical {
            LogicalPlan::Scan(scan) => self.convert_scan(scan),
            LogicalPlan::Join(join) => self.convert_join(join),
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

    fn convert_join(
        &self,
        join: &crate::planner::logical::JoinNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        // Detect multi-table queries and use optimizer for join order
        let join_plan = LogicalPlan::Join(join.clone());
        let optimizer = JoinOrderOptimizer::new(self.graph);
        let tables = optimizer.extract_tables(&join_plan);
        let table_count = tables.len();

        // For multi-table queries (2+ tables), use join order optimizer
        if table_count >= 2 {
            return self.convert_multi_table_join(&join_plan, table_count);
        }

        // Fallback for single table (shouldn't happen, but be defensive)
        self.convert_simple_join(join)
    }

    /// Convert a simple join without optimization.
    fn convert_simple_join(
        &self,
        join: &crate::planner::logical::JoinNode,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        use crate::planner::logical::JoinCondition;

        // Convert join condition to column pairs
        let on_columns: Vec<(String, String)> = match &join.on {
            JoinCondition::Equi(pairs) => pairs
                .iter()
                .map(|(left, right)| {
                    (
                        format!("{}.{}", left.entity, left.column),
                        format!("{}.{}", right.entity, right.column),
                    )
                })
                .collect(),
            JoinCondition::Expr(_) => {
                return Err(PlanError::PhysicalPlanError(
                    "Complex join expressions not yet supported".to_string(),
                ));
            }
        };

        // Convert left and right inputs
        let left_candidates = self.convert(&join.left)?;
        let right_candidates = self.convert(&join.right)?;

        // Generate physical join plans
        let mut plans = Vec::new();

        for left_plan in &left_candidates {
            for right_plan in &right_candidates {
                // Generate HashJoin (most common strategy)
                plans.push(PhysicalPlan::HashJoin {
                    left: Box::new(left_plan.clone()),
                    right: Box::new(right_plan.clone()),
                    on: on_columns.clone(),
                    estimated_rows: None,
                });

                // Also generate NestedLoopJoin as alternative (for small tables)
                plans.push(PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_plan.clone()),
                    right: Box::new(right_plan.clone()),
                    on: on_columns.clone(),
                    estimated_rows: None,
                });
            }
        }

        Ok(plans)
    }

    /// Convert a multi-table join using the join order optimizer.
    ///
    /// Strategy selection:
    /// - DP: Use dynamic programming optimizer (for ≤10 tables)
    /// - Legacy: Use enumeration (≤3 tables) or greedy (>3 tables)
    /// - Adaptive: Choose based on table count
    fn convert_multi_table_join(
        &self,
        join_plan: &LogicalPlan,
        table_count: usize,
    ) -> PlanResult<Vec<PhysicalPlan>> {
        use crate::planner::physical::join_optimizer::OptimizerStrategy;

        let optimizer =
            JoinOrderOptimizer::with_strategy(self.graph, self.config.optimizer_strategy.clone());

        // Extract tables for strategy selection
        let tables = optimizer.extract_tables(join_plan);
        let table_vec: Vec<String> = tables.into_iter().collect();

        // Select strategy based on config
        let strategy = optimizer.select_strategy(&table_vec);

        println!(
            "[CONVERTER] Using strategy {:?} for {} tables",
            strategy, table_count
        );

        // Get optimized join orders based on selected strategy
        let logical_candidates = match strategy {
            OptimizerStrategy::DP => {
                // Use DP optimizer
                use crate::planner::join_optimizer::dp_optimizer::DPOptimizer;

                let mut dp = DPOptimizer::new(self.graph);

                // Extract filters from the plan (if any)
                let filters = self.extract_filters_from_plan(join_plan);

                // Run DP optimization
                if let Some(optimized) = dp.optimize(table_vec, filters) {
                    vec![Some(optimized)]
                } else {
                    vec![]
                }
            }
            OptimizerStrategy::Legacy | OptimizerStrategy::Adaptive => {
                // Use legacy approach
                if table_count <= 3 {
                    // Small joins: enumerate all permutations
                    optimizer.enumerate_join_orders(join_plan)
                } else {
                    // Large joins: use greedy algorithm
                    let table_refs: Vec<&str> = table_vec.iter().map(|s| s.as_str()).collect();

                    if let Some(greedy_plan) = optimizer.greedy_join_order(&table_refs) {
                        vec![Some(greedy_plan)]
                    } else {
                        vec![]
                    }
                }
            }
        };

        // Convert each logical candidate to physical plans
        let mut all_physical_plans = Vec::new();

        for logical_candidate in logical_candidates {
            if let Some(logical) = logical_candidate {
                // Convert this logical plan to physical plans
                let physical_candidates = self.convert_logical_to_physical(&logical)?;
                all_physical_plans.extend(physical_candidates);
            }
        }

        if all_physical_plans.is_empty() {
            return Err(PlanError::PhysicalPlanError(
                "No valid physical plans generated from optimizer".to_string(),
            ));
        }

        Ok(all_physical_plans)
    }

    /// Extract filters from a logical plan for DP optimizer.
    fn extract_filters_from_plan(&self, plan: &LogicalPlan) -> Vec<crate::model::expr::Expr> {
        let mut filters = Vec::new();
        self.collect_filters(plan, &mut filters);
        filters
    }

    /// Recursively collect filters from a logical plan.
    fn collect_filters(&self, plan: &LogicalPlan, filters: &mut Vec<crate::model::expr::Expr>) {
        match plan {
            LogicalPlan::Filter(filter_node) => {
                filters.extend(filter_node.predicates.clone());
                self.collect_filters(&filter_node.input, filters);
            }
            LogicalPlan::Join(join) => {
                self.collect_filters(&join.left, filters);
                self.collect_filters(&join.right, filters);
            }
            _ => {}
        }
    }

    /// Convert an optimized logical plan to physical plans.
    ///
    /// This recursively converts the logical plan, generating both HashJoin and
    /// NestedLoopJoin strategies for each join in the plan.
    fn convert_logical_to_physical(&self, logical: &LogicalPlan) -> PlanResult<Vec<PhysicalPlan>> {
        match logical {
            LogicalPlan::Scan(scan) => Ok(vec![PhysicalPlan::TableScan {
                table: scan.entity.clone(),
                strategy: TableScanStrategy::FullScan,
                estimated_rows: None,
            }]),
            LogicalPlan::Join(join) => {
                use crate::planner::logical::JoinCondition;

                // Convert join condition to column pairs
                let on_columns: Vec<(String, String)> = match &join.on {
                    JoinCondition::Equi(pairs) => pairs
                        .iter()
                        .map(|(left, right)| {
                            (
                                format!("{}.{}", left.entity, left.column),
                                format!("{}.{}", right.entity, right.column),
                            )
                        })
                        .collect(),
                    JoinCondition::Expr(_) => {
                        return Err(PlanError::PhysicalPlanError(
                            "Complex join expressions not yet supported".to_string(),
                        ));
                    }
                };

                // Convert left and right inputs (no recursion into optimizer - already optimized)
                let left_plans = self.convert_logical_to_physical(&join.left)?;
                let right_plans = self.convert_logical_to_physical(&join.right)?;

                let mut plans = Vec::new();

                // Generate both HashJoin and NestedLoopJoin for each combination
                for left_plan in &left_plans {
                    for right_plan in &right_plans {
                        plans.push(PhysicalPlan::HashJoin {
                            left: Box::new(left_plan.clone()),
                            right: Box::new(right_plan.clone()),
                            on: on_columns.clone(),
                            estimated_rows: None,
                        });

                        plans.push(PhysicalPlan::NestedLoopJoin {
                            left: Box::new(left_plan.clone()),
                            right: Box::new(right_plan.clone()),
                            on: on_columns.clone(),
                            estimated_rows: None,
                        });
                    }
                }

                Ok(plans)
            }
            _ => {
                // For other node types, use regular convert
                self.convert(logical)
            }
        }
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

        // Combine multiple predicates with AND
        let combined_predicate = if filter.predicates.len() == 1 {
            filter.predicates[0].clone()
        } else {
            Self::combine_predicates_with_and(&filter.predicates)
        };

        let input_candidates = self.convert(&filter.input)?;
        Ok(input_candidates
            .into_iter()
            .map(|input| PhysicalPlan::Filter {
                input: Box::new(input),
                predicate: combined_predicate.clone(),
            })
            .collect())
    }

    /// Combine multiple predicates with AND operator.
    fn combine_predicates_with_and(
        predicates: &[crate::model::expr::Expr],
    ) -> crate::model::expr::Expr {
        use crate::model::expr::{BinaryOp, Expr};

        assert!(!predicates.is_empty(), "Cannot combine empty predicates");

        if predicates.len() == 1 {
            return predicates[0].clone();
        }

        // Build nested AND tree: (p1 AND (p2 AND (p3 AND ...)))
        let mut result = predicates[predicates.len() - 1].clone();
        for predicate in predicates[..predicates.len() - 1].iter().rev() {
            result = Expr::BinaryOp {
                left: Box::new(predicate.clone()),
                op: BinaryOp::And,
                right: Box::new(result),
            };
        }

        result
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
                aggregates: agg.measures.clone(),
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
                projections: proj.projections.clone(),
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
    fn test_convert_filter_with_multiple_predicates_combines_with_and() {
        let graph = create_test_graph();
        let config = crate::planner::physical::PhysicalPlannerConfig::default();
        let converter = PhysicalConverter::new(&graph, &config);

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
            predicates: vec![predicate1.clone(), predicate2.clone()],
        });

        // Should succeed and combine predicates with AND
        let result = converter.convert(&filter);
        assert!(
            result.is_ok(),
            "Converting filter with multiple predicates should succeed"
        );

        let plans = result.unwrap();
        assert_eq!(plans.len(), 1);

        // Verify the combined predicate is an AND operation
        match &plans[0] {
            PhysicalPlan::Filter { predicate, .. } => {
                match predicate {
                    ModelExpr::BinaryOp {
                        op: BinaryOp::And,
                        left,
                        right,
                    } => {
                        // Left should be predicate1, right should be predicate2
                        assert_eq!(**left, predicate1);
                        assert_eq!(**right, predicate2);
                    }
                    _ => panic!("Expected combined predicate to be AND operation"),
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_convert_filter_with_single_predicate_succeeds() {
        let graph = create_test_graph();
        let config = crate::planner::physical::PhysicalPlannerConfig::default();
        let converter = PhysicalConverter::new(&graph, &config);

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
