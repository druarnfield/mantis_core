//! Build logical plans from reports.

use crate::model::{Report, ShowItem};
use crate::planner::join_builder::JoinBuilder;
use crate::planner::logical::{
    AggregateNode, ColumnRef, ExpandedMeasure, FilterNode, LogicalPlan, OrderRef, ProjectNode,
    ProjectionItem, ScanNode, SortNode,
};
use crate::planner::{PlanError, PlanResult};
use crate::semantic::graph::UnifiedGraph;

pub struct PlanBuilder<'a> {
    graph: &'a UnifiedGraph,
}

impl<'a> PlanBuilder<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        Self { graph }
    }

    pub fn build(&self, report: &Report) -> PlanResult<LogicalPlan> {
        // Start with base scan (or joins for multi-table)
        let mut plan = self.build_scan(report)?;

        // Add filters (WHERE clause)
        if !report.filters.is_empty() {
            plan = self.build_filter(plan, report)?;
        }

        // Add aggregation if needed
        plan = self.build_aggregate(plan, report)?;

        // Add projection
        plan = self.build_project(plan, report)?;

        // Add sort if needed
        if !report.sort.is_empty() {
            plan = self.build_sort(plan, report)?;
        }

        // Add limit if needed
        if let Some(limit) = report.limit {
            plan = LogicalPlan::Limit(crate::planner::logical::LimitNode {
                input: Box::new(plan),
                limit,
            });
        }

        Ok(plan)
    }

    fn build_scan(&self, report: &Report) -> PlanResult<LogicalPlan> {
        if report.from.is_empty() {
            return Err(PlanError::LogicalPlanError(
                "Report has no FROM table".to_string(),
            ));
        }

        // Use JoinBuilder for multi-table queries
        if report.from.len() > 1 {
            let join_builder = JoinBuilder::new(self.graph);
            join_builder.build_join_tree(&report.from)
        } else {
            // Single table - simple scan
            Ok(LogicalPlan::Scan(ScanNode {
                entity: report.from[0].clone(),
            }))
        }
    }

    fn build_filter(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicates: report.filters.clone(),
        }))
    }

    fn build_aggregate(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        let mut measures = Vec::new();

        for item in &report.show {
            match item {
                ShowItem::Measure { name, .. } => {
                    if let Some(entity) = report.from.first() {
                        // Expand measure using graph helper
                        let expr = self
                            .graph
                            .expand_measure(entity, name)
                            .map_err(|e| PlanError::LogicalPlanError(e.to_string()))?;

                        measures.push(ExpandedMeasure {
                            name: name.clone(),
                            entity: entity.clone(),
                            expr,
                        });
                    }
                }
                _ => {}
            }
        }

        if measures.is_empty() {
            return Ok(input);
        }

        let group_by = self.extract_group_by(report)?;

        Ok(LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by,
            measures,
        }))
    }

    fn extract_group_by(&self, report: &Report) -> PlanResult<Vec<ColumnRef>> {
        use crate::model::GroupItem;

        let mut group_by = Vec::new();

        // Extract columns from report.group (explicit grouping)
        for group_item in &report.group {
            match group_item {
                GroupItem::InlineSlicer { name, .. } => {
                    // Assume column belongs to first table for now
                    if let Some(entity) = report.from.first() {
                        group_by.push(ColumnRef {
                            entity: entity.clone(),
                            column: name.clone(),
                        });
                    }
                }
                GroupItem::DrillPathRef { level, .. } => {
                    // For drill paths, use the level name as the column
                    // Assume it belongs to first table for now
                    if let Some(entity) = report.from.first() {
                        group_by.push(ColumnRef {
                            entity: entity.clone(),
                            column: level.clone(),
                        });
                    }
                }
            }
        }

        Ok(group_by)
    }

    fn build_project(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        let mut projections = Vec::new();

        for item in &report.show {
            match item {
                ShowItem::Measure { name, .. } => {
                    if let Some(entity) = report.from.first() {
                        // Expand measure using graph helper
                        let expr = self
                            .graph
                            .expand_measure(entity, name)
                            .map_err(|e| PlanError::LogicalPlanError(e.to_string()))?;

                        projections.push(ProjectionItem::Measure(ExpandedMeasure {
                            name: name.clone(),
                            entity: entity.clone(),
                            expr,
                        }));
                    }
                }
                _ => {}
            }
        }

        Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            projections,
        }))
    }

    fn build_sort(&self, input: LogicalPlan, report: &Report) -> PlanResult<LogicalPlan> {
        let order_by = report
            .sort
            .iter()
            .map(|sort_item| OrderRef {
                column: sort_item.column.clone(),
                descending: matches!(sort_item.direction, crate::model::SortDirection::Desc),
            })
            .collect();

        Ok(LogicalPlan::Sort(SortNode {
            input: Box::new(input),
            order_by,
        }))
    }
}
