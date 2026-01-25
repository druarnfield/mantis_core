//! Phase 1: Resolution
//!
//! This phase resolves all field references in the semantic query to their
//! physical representations. It validates that all referenced entities and
//! fields exist, and collects the set of entities that need to be joined.
//!
//! For multi-fact queries (measures from different facts), this phase also
//! detects anchor facts and validates shared dimensions.

use std::collections::HashSet;

use crate::semantic::error::{PlanError, PlanResult, SemanticError};
use crate::semantic::model_graph::{EntityType, ModelGraph, ModelResolvedField};

use super::resolved::{
    FactAggregate, FactJoinKey, MultiFactQuery, ResolvedColumn, ResolvedDerivedExpr,
    ResolvedEntity, ResolvedFilter, ResolvedMeasure, ResolvedOrder, ResolvedOrderExpr,
    ResolvedQuery, ResolvedQueryPlan, ResolvedSelect, SharedDimension,
};
use super::types::{FieldRef, SemanticQuery};

/// Resolver - handles Phase 1 of query planning.
pub struct Resolver<'a> {
    graph: &'a ModelGraph,
}

impl<'a> Resolver<'a> {
    pub fn new(graph: &'a ModelGraph) -> Self {
        Self { graph }
    }

    /// Resolve a semantic query into a fully resolved query.
    ///
    /// For single-fact queries, returns a standard ResolvedQuery.
    /// For multi-fact queries (detected via `detect_anchors`), additional
    /// processing is needed at the QueryPlanner level.
    pub fn resolve(&self, query: &SemanticQuery) -> PlanResult<ResolvedQuery> {
        // Detect anchor facts from measures (or use explicit from)
        let anchors = self.detect_anchors(query)?;

        // For now, we only handle single-fact queries in this method.
        // Multi-fact queries are handled by resolve_multi_fact.
        if anchors.len() > 1 {
            // Multi-fact query - caller should use resolve_multi_fact instead
            // For backward compatibility, use the first anchor
            // TODO: This should return an error or a different type
        }

        // Use explicit from, or first detected anchor
        let from_entity = query
            .from
            .clone()
            .or_else(|| anchors.first().cloned())
            .ok_or(SemanticError::NoAnchor)?;

        let from = self.resolve_entity(&from_entity)?;

        // Collect all referenced entities
        let referenced_entities = self.collect_entities(query, &from_entity)?;

        // Resolve filters
        let filters = self.resolve_filters(&query.filters)?;

        // Resolve group by
        let group_by = self.resolve_group_by(&query.group_by)?;

        // Resolve select
        let mut select = self.resolve_select(&query.select)?;

        // Resolve derived measures and append to select
        let derived = self.resolve_derived(&query.derived)?;
        select.extend(derived);

        // Resolve order by
        let order_by = self.resolve_order_by(&query.order_by)?;

        Ok(ResolvedQuery {
            from,
            referenced_entities,
            filters,
            group_by,
            select,
            order_by,
            limit: query.limit,
        })
    }

    /// Detect anchor facts from measures in the query.
    ///
    /// Returns the set of fact entities that contain the measures referenced
    /// in the query. If an explicit `from` is specified, it's included.
    pub fn detect_anchors(&self, query: &SemanticQuery) -> PlanResult<Vec<String>> {
        let mut anchors = HashSet::new();

        // Collect facts from measures in select
        for field in &query.select {
            if let Ok(ResolvedFieldKind::Measure(m)) = self.resolve_field(&field.field) {
                anchors.insert(m.entity_alias);
            }
        }

        // If explicit from, add it (only if it's a fact)
        if let Some(ref from) = query.from {
            if let Ok(info) = self.graph.get_entity_info(from) {
                if info.entity_type == EntityType::Fact {
                    anchors.insert(from.clone());
                }
            }
        }

        // If no anchors found, check if there's a dimension-only query
        // with an explicit from
        if anchors.is_empty() {
            if let Some(ref from) = query.from {
                anchors.insert(from.clone());
            }
        }

        if anchors.is_empty() {
            return Err(SemanticError::NoAnchor);
        }

        // Sort for deterministic ordering
        let mut result: Vec<_> = anchors.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Check if this is a multi-fact query.
    pub fn is_multi_fact(&self, query: &SemanticQuery) -> PlanResult<bool> {
        let anchors = self.detect_anchors(query)?;
        Ok(anchors.len() > 1)
    }

    /// Resolve a query, returning either single-fact or multi-fact plan.
    pub fn resolve_plan(&self, query: &SemanticQuery) -> PlanResult<ResolvedQueryPlan> {
        let anchors = self.detect_anchors(query)?;

        if anchors.len() == 1 {
            Ok(ResolvedQueryPlan::Single(self.resolve(query)?))
        } else {
            Ok(ResolvedQueryPlan::Multi(self.resolve_multi_fact(query, &anchors)?))
        }
    }

    /// Resolve a multi-fact query.
    ///
    /// This finds shared dimensions between all anchor facts and builds
    /// the MultiFactQuery structure for CTE generation.
    pub fn resolve_multi_fact(
        &self,
        query: &SemanticQuery,
        anchors: &[String],
    ) -> PlanResult<MultiFactQuery> {
        // 1. Collect dimensions from query (non-measures in select + group_by)
        let dimension_refs = self.collect_dimension_refs(query)?;

        // 2. Find shared dimensions - validate paths from all anchors
        let shared_dimensions = self.find_shared_dimensions(anchors, &dimension_refs)?;

        // 3. Build fact aggregates - measures grouped by anchor fact
        let fact_aggregates = self.build_fact_aggregates(query, anchors, &shared_dimensions)?;

        // 4. Resolve filters
        let global_filters = self.resolve_filters(&query.filters)?;

        // 5. Resolve order by
        let order_by = self.resolve_order_by(&query.order_by)?;

        Ok(MultiFactQuery {
            fact_aggregates,
            shared_dimensions,
            global_filters,
            order_by,
            limit: query.limit,
        })
    }

    /// Collect dimension references from query (non-measures).
    fn collect_dimension_refs(&self, query: &SemanticQuery) -> PlanResult<Vec<FieldRef>> {
        let mut dims = Vec::new();
        let mut seen = HashSet::new();

        // From select list (non-measures)
        for field in &query.select {
            if let Ok(ResolvedFieldKind::Column(_)) = self.resolve_field(&field.field) {
                let key = format!("{}.{}", field.field.entity, field.field.field);
                if seen.insert(key) {
                    dims.push(field.field.clone());
                }
            }
        }

        // From group_by
        for field in &query.group_by {
            let key = format!("{}.{}", field.entity, field.field);
            if seen.insert(key) {
                dims.push(field.clone());
            }
        }

        Ok(dims)
    }

    /// Find shared dimensions - dimensions reachable from ALL anchor facts.
    fn find_shared_dimensions(
        &self,
        anchors: &[String],
        dimension_refs: &[FieldRef],
    ) -> PlanResult<Vec<SharedDimension>> {
        // Group dimension refs by entity
        let mut dims_by_entity: std::collections::HashMap<String, Vec<ResolvedColumn>> =
            std::collections::HashMap::new();

        for dim_ref in dimension_refs {
            let column = self.resolve_column(dim_ref)?;
            dims_by_entity
                .entry(dim_ref.entity.clone())
                .or_default()
                .push(column);
        }

        let mut shared = Vec::new();

        for (dim_entity, columns) in dims_by_entity {
            // Check that all anchors can reach this dimension
            let mut paths = Vec::new();

            for anchor in anchors {
                // Find path from anchor to dimension
                let join_path = self.graph.find_path(anchor, &dim_entity)?;

                // The join key is the last edge's columns
                if let Some(last_edge) = join_path.edges.last() {
                    paths.push((
                        anchor.clone(),
                        FactJoinKey {
                            fact_column: last_edge.from_column.clone(),
                            dimension: dim_entity.clone(),
                            dimension_column: last_edge.to_column.clone(),
                        },
                    ));
                } else if anchor == &dim_entity {
                    // Direct - dimension is the anchor itself
                    // This is unusual but possible
                    paths.push((
                        anchor.clone(),
                        FactJoinKey {
                            fact_column: String::new(),
                            dimension: dim_entity.clone(),
                            dimension_column: String::new(),
                        },
                    ));
                } else {
                    // No path found
                    return Err(SemanticError::DimensionNotShared {
                        dimension: dim_entity.clone(),
                        unreachable_from: anchor.clone(),
                    });
                }
            }

            let dim_info = self.resolve_entity(&dim_entity)?;
            shared.push(SharedDimension {
                dimension: dim_info,
                columns,
                paths,
            });
        }

        Ok(shared)
    }

    /// Build fact aggregates - measures grouped by anchor fact.
    fn build_fact_aggregates(
        &self,
        query: &SemanticQuery,
        anchors: &[String],
        shared_dimensions: &[SharedDimension],
    ) -> PlanResult<Vec<FactAggregate>> {
        let mut aggregates = Vec::new();

        for anchor in anchors {
            let fact = self.resolve_entity(anchor)?;
            let cte_alias = format!("{}_agg", anchor);

            // Collect measures from this fact
            let mut measures = Vec::new();
            for field in &query.select {
                if let Ok(ResolvedFieldKind::Measure(m)) = self.resolve_field(&field.field) {
                    if m.entity_alias == *anchor {
                        measures.push(m);
                    }
                }
            }

            // Collect join keys for this fact
            let join_keys: Vec<FactJoinKey> = shared_dimensions
                .iter()
                .filter_map(|sd| {
                    sd.paths
                        .iter()
                        .find(|(fact_name, _)| fact_name == anchor)
                        .map(|(_, key)| key.clone())
                })
                .collect();

            aggregates.push(FactAggregate {
                fact,
                cte_alias,
                join_keys,
                measures,
                fact_filters: vec![], // TODO: Separate fact-specific filters
            });
        }

        Ok(aggregates)
    }

    /// Resolve an entity reference.
    fn resolve_entity(&self, name: &str) -> PlanResult<ResolvedEntity> {
        let info = self.graph.get_entity_info(name)?;

        Ok(ResolvedEntity {
            name: info.name,
            physical_table: info.physical_table,
            physical_schema: info.physical_schema,
            materialized: info.materialized,
        })
    }

    /// Collect all entities referenced in the query.
    fn collect_entities(
        &self,
        query: &SemanticQuery,
        from_entity: &str,
    ) -> PlanResult<HashSet<String>> {
        let mut entities = HashSet::new();
        entities.insert(from_entity.to_string());

        // Validate that each referenced entity exists
        for filter in &query.filters {
            self.validate_entity(&filter.field.entity)?;
            entities.insert(filter.field.entity.clone());
        }

        for field in &query.group_by {
            self.validate_entity(&field.entity)?;
            entities.insert(field.entity.clone());
        }

        for field in &query.select {
            self.validate_entity(&field.field.entity)?;
            entities.insert(field.field.entity.clone());
        }

        for field in &query.order_by {
            self.validate_entity(&field.field.entity)?;
            entities.insert(field.field.entity.clone());
        }

        Ok(entities)
    }

    /// Validate that an entity exists.
    fn validate_entity(&self, name: &str) -> PlanResult<()> {
        if !self.graph.has_entity(name) {
            return Err(PlanError::UnknownEntity(name.into()));
        }
        Ok(())
    }

    /// Resolve a field reference to a column.
    fn resolve_column(&self, field: &FieldRef) -> PlanResult<ResolvedColumn> {
        let resolved = self.graph.resolve_field(&field.entity, &field.field)?;

        match resolved {
            ModelResolvedField::Column { entity, column } => Ok(ResolvedColumn {
                entity_alias: entity,
                logical_name: column.clone(),
                physical_name: column,
            }),
            ModelResolvedField::Measure { .. } => Err(PlanError::InvalidReference(format!(
                "Expected column but found measure: {}.{}",
                field.entity, field.field
            ))),
        }
    }

    /// Resolve a field reference (could be column or measure).
    fn resolve_field(&self, field: &FieldRef) -> PlanResult<ResolvedFieldKind> {
        let resolved = self.graph.resolve_field(&field.entity, &field.field)?;

        match resolved {
            ModelResolvedField::Column { entity, column } => {
                Ok(ResolvedFieldKind::Column(ResolvedColumn {
                    entity_alias: entity,
                    logical_name: column.clone(),
                    physical_name: column,
                }))
            }
            ModelResolvedField::Measure {
                entity,
                measure,
                aggregation,
                source_column,
                filter,
            } => Ok(ResolvedFieldKind::Measure(ResolvedMeasure {
                entity_alias: entity,
                name: measure,
                aggregation,
                source_column,
                filter: None,
                definition_filter: filter,
            })),
        }
    }

    /// Resolve filter conditions.
    fn resolve_filters(
        &self,
        filters: &[super::types::FieldFilter],
    ) -> PlanResult<Vec<ResolvedFilter>> {
        let mut resolved = Vec::with_capacity(filters.len());

        for filter in filters {
            let field = self.resolve_field(&filter.field)?;

            match field {
                ResolvedFieldKind::Column(column) => {
                    resolved.push(ResolvedFilter {
                        column,
                        op: filter.op,
                        value: filter.value.clone(),
                    });
                }
                ResolvedFieldKind::Measure(_) => {
                    return Err(PlanError::InvalidReference(
                        "Cannot filter on measures directly. Use HAVING clause.".into(),
                    ));
                }
            }
        }

        Ok(resolved)
    }

    /// Resolve GROUP BY columns.
    fn resolve_group_by(&self, fields: &[FieldRef]) -> PlanResult<Vec<ResolvedColumn>> {
        let mut resolved = Vec::with_capacity(fields.len());

        for field in fields {
            resolved.push(self.resolve_column(field)?);
        }

        Ok(resolved)
    }

    /// Resolve SELECT expressions.
    fn resolve_select(
        &self,
        fields: &[super::types::SelectField],
    ) -> PlanResult<Vec<ResolvedSelect>> {
        let mut resolved = Vec::with_capacity(fields.len());

        for select_field in fields {
            let field = self.resolve_field(&select_field.field)?;

            match field {
                ResolvedFieldKind::Column(column) => {
                    // Check if this is an inline aggregate (e.g., SUM(orders.amount))
                    if let Some(ref agg) = select_field.aggregation {
                        resolved.push(ResolvedSelect::Aggregate {
                            column,
                            aggregation: agg.clone(),
                            alias: select_field.alias.clone(),
                        });
                    } else {
                        resolved.push(ResolvedSelect::Column {
                            column,
                            alias: select_field.alias.clone(),
                        });
                    }
                }
                ResolvedFieldKind::Measure(mut measure) => {
                    // Handle filtered measures - resolve the filter conditions
                    if let Some(ref filters) = select_field.measure_filter {
                        let resolved_filters = self.resolve_filters(filters)?;
                        measure.filter = Some(resolved_filters);
                    }

                    resolved.push(ResolvedSelect::Measure {
                        measure,
                        alias: select_field.alias.clone(),
                    });
                }
            }
        }

        Ok(resolved)
    }

    /// Resolve derived expressions.
    fn resolve_derived(
        &self,
        derived_fields: &[super::types::DerivedField],
    ) -> PlanResult<Vec<ResolvedSelect>> {
        let mut resolved = Vec::new();

        for derived in derived_fields {
            let expr = self.resolve_derived_expr(&derived.expression)?;
            resolved.push(ResolvedSelect::Derived {
                alias: derived.alias.clone(),
                expression: expr,
            });
        }

        Ok(resolved)
    }

    /// Resolve a derived expression.
    fn resolve_derived_expr(
        &self,
        expr: &super::types::DerivedExpr,
    ) -> PlanResult<ResolvedDerivedExpr> {
        match expr {
            super::types::DerivedExpr::MeasureRef(name) => {
                // Resolve the measure to its full definition so we can emit
                // the aggregate expression (not just an alias reference)
                let (fact_name, measure_def) = self
                    .graph
                    .find_measure_entity(name)
                    .ok_or_else(|| SemanticError::UnknownMeasure { name: name.clone() })?;

                Ok(ResolvedDerivedExpr::MeasureRef(ResolvedMeasure {
                    entity_alias: fact_name.to_string(),
                    name: name.clone(),
                    aggregation: measure_def.aggregation,
                    source_column: measure_def.source_column.clone(),
                    filter: None,
                    // Include the definition filter so derived expressions
                    // generate proper CASE WHEN for filtered measures
                    definition_filter: measure_def.filter.clone(),
                }))
            }
            super::types::DerivedExpr::Literal(value) => Ok(ResolvedDerivedExpr::Literal(*value)),
            super::types::DerivedExpr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_derived_expr(left)?;
                let resolved_right = self.resolve_derived_expr(right)?;
                Ok(ResolvedDerivedExpr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: *op,
                    right: Box::new(resolved_right),
                })
            }
            super::types::DerivedExpr::Negate(inner) => {
                let resolved_inner = self.resolve_derived_expr(inner)?;
                Ok(ResolvedDerivedExpr::Negate(Box::new(resolved_inner)))
            }
            // Time intelligence functions - passed through as-is for now
            // The emitter will handle generating the appropriate window functions
            super::types::DerivedExpr::TimeFunction(time_fn) => {
                Ok(ResolvedDerivedExpr::TimeFunction(time_fn.clone()))
            }
            super::types::DerivedExpr::Delta { current, previous } => {
                let resolved_current = self.resolve_derived_expr(current)?;
                let resolved_previous = self.resolve_derived_expr(previous)?;
                Ok(ResolvedDerivedExpr::Delta {
                    current: Box::new(resolved_current),
                    previous: Box::new(resolved_previous),
                })
            }
            super::types::DerivedExpr::Growth { current, previous } => {
                let resolved_current = self.resolve_derived_expr(current)?;
                let resolved_previous = self.resolve_derived_expr(previous)?;
                Ok(ResolvedDerivedExpr::Growth {
                    current: Box::new(resolved_current),
                    previous: Box::new(resolved_previous),
                })
            }
        }
    }

    /// Resolve ORDER BY expressions.
    fn resolve_order_by(&self, fields: &[super::types::OrderField]) -> PlanResult<Vec<ResolvedOrder>> {
        let mut resolved = Vec::with_capacity(fields.len());

        for order_field in fields {
            let field = self.resolve_field(&order_field.field)?;

            let expr = match field {
                ResolvedFieldKind::Column(column) => ResolvedOrderExpr::Column(column),
                ResolvedFieldKind::Measure(measure) => ResolvedOrderExpr::Measure(measure),
            };

            resolved.push(ResolvedOrder {
                expr,
                descending: order_field.descending,
            });
        }

        Ok(resolved)
    }
}

/// Internal helper for field resolution.
enum ResolvedFieldKind {
    Column(ResolvedColumn),
    Measure(ResolvedMeasure),
}
