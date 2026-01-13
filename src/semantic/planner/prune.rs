//! Phase 2.5: Column Pruning
//!
//! This phase determines the minimal set of columns needed to execute a query.
//! By computing required columns upfront, we can optimize the SQL generation
//! to only SELECT columns that are actually needed.
//!
//! # How it works
//!
//! 1. Collect all columns referenced in the query (SELECT, WHERE, GROUP BY, ORDER BY)
//! 2. Add join key columns (needed for JOINs even if not in SELECT)
//! 3. Expand computed columns to their source dependencies using lineage graph
//! 4. Return the minimal set of source columns needed

use std::collections::HashSet;

use crate::semantic::column_lineage::{ColumnLineageGraph, ColumnRef};

use super::resolved::{
    ResolvedColumn, ResolvedJoinTree, ResolvedOrderExpr, ResolvedQuery, ResolvedSelect,
};
use super::validate::ValidatedQuery;

/// Column pruner - determines minimal columns needed for a query.
pub struct ColumnPruner<'a> {
    lineage: &'a ColumnLineageGraph,
}

impl<'a> ColumnPruner<'a> {
    /// Create a new column pruner with the given lineage graph.
    pub fn new(lineage: &'a ColumnLineageGraph) -> Self {
        Self { lineage }
    }

    /// Compute the minimal set of source columns needed for a query.
    ///
    /// This analyzes the query to find all column references and then
    /// expands them to their source columns using the lineage graph.
    pub fn required_columns(&self, query: &ValidatedQuery) -> HashSet<ColumnRef> {
        let mut target_columns = HashSet::new();

        // 1. Columns from SELECT
        self.collect_select_columns(&query.query, &mut target_columns);

        // 2. Columns from WHERE filters
        self.collect_filter_columns(&query.query, &mut target_columns);

        // 3. Columns from GROUP BY
        self.collect_group_by_columns(&query.query, &mut target_columns);

        // 4. Columns from ORDER BY
        self.collect_order_by_columns(&query.query, &mut target_columns);

        // 5. Columns from JOIN keys
        self.collect_join_columns(&query.join_tree, &mut target_columns);

        // 6. Expand to source columns using lineage
        self.expand_to_source_columns(target_columns)
    }

    /// Collect columns referenced in SELECT clause.
    fn collect_select_columns(&self, query: &ResolvedQuery, columns: &mut HashSet<ColumnRef>) {
        for select in &query.select {
            match select {
                ResolvedSelect::Column { column, .. } => {
                    columns.insert(self.resolved_to_ref(column));
                }
                ResolvedSelect::Measure { measure, .. } => {
                    // For measures, use the measure NAME for lineage lookup
                    // (lineage tracks: source.column -> fact.measure_name)
                    columns.insert(ColumnRef::new(&measure.entity_alias, &measure.name));

                    // Also collect columns from measure's filter if present
                    if let Some(filters) = &measure.filter {
                        for filter in filters {
                            columns.insert(self.resolved_to_ref(&filter.column));
                        }
                    }
                }
                ResolvedSelect::Aggregate { column, .. } => {
                    columns.insert(self.resolved_to_ref(column));
                }
                ResolvedSelect::Derived { expression, .. } => {
                    // Derived expressions reference other measures by alias,
                    // so they don't add new column requirements here
                    // (the referenced measures are already in select)
                    let _ = expression; // Suppress unused warning
                }
            }
        }
    }

    /// Collect columns referenced in WHERE filters.
    fn collect_filter_columns(&self, query: &ResolvedQuery, columns: &mut HashSet<ColumnRef>) {
        for filter in &query.filters {
            columns.insert(self.resolved_to_ref(&filter.column));
        }
    }

    /// Collect columns referenced in GROUP BY.
    fn collect_group_by_columns(&self, query: &ResolvedQuery, columns: &mut HashSet<ColumnRef>) {
        for col in &query.group_by {
            columns.insert(self.resolved_to_ref(col));
        }
    }

    /// Collect columns referenced in ORDER BY.
    fn collect_order_by_columns(&self, query: &ResolvedQuery, columns: &mut HashSet<ColumnRef>) {
        for order in &query.order_by {
            match &order.expr {
                ResolvedOrderExpr::Column(col) => {
                    columns.insert(self.resolved_to_ref(col));
                }
                ResolvedOrderExpr::Measure(measure) => {
                    // Use measure name for lineage lookup (same as SELECT)
                    columns.insert(ColumnRef::new(&measure.entity_alias, &measure.name));
                }
            }
        }
    }

    /// Collect columns used as join keys.
    fn collect_join_columns(&self, join_tree: &ResolvedJoinTree, columns: &mut HashSet<ColumnRef>) {
        for edge in &join_tree.edges {
            // From side join key
            columns.insert(ColumnRef::new(&edge.from_entity, &edge.from_column));
            // To side join key
            columns.insert(ColumnRef::new(&edge.to_entity, &edge.to_column));
        }
    }

    /// Expand target columns to their required source columns.
    ///
    /// For each target column, finds all source columns it depends on.
    /// If a column has no lineage (is a source column itself), it's included as-is.
    fn expand_to_source_columns(&self, target_columns: HashSet<ColumnRef>) -> HashSet<ColumnRef> {
        let mut source_columns = HashSet::new();

        for col in target_columns {
            // Get the required source columns for this column
            let required = self.lineage.required_source_columns(&col);

            if required.is_empty() {
                // Column has no upstream dependencies - it IS a source column
                source_columns.insert(col);
            } else {
                // Add all required source columns
                source_columns.extend(required);
            }
        }

        source_columns
    }

    /// Convert a ResolvedColumn to a ColumnRef.
    fn resolved_to_ref(&self, col: &ResolvedColumn) -> ColumnRef {
        // Use logical name for lineage lookup (lineage uses logical names)
        ColumnRef::new(&col.entity_alias, &col.logical_name)
    }
}

/// Result of column pruning - columns needed per entity.
#[derive(Debug, Clone)]
pub struct PrunedColumns {
    /// All required source columns.
    pub columns: HashSet<ColumnRef>,
}

impl PrunedColumns {
    /// Create from a set of column references.
    pub fn new(columns: HashSet<ColumnRef>) -> Self {
        Self { columns }
    }

    /// Check if a specific column is needed.
    pub fn is_needed(&self, entity: &str, column: &str) -> bool {
        self.columns.contains(&ColumnRef::new(entity, column))
    }

    /// Get all columns needed for a specific entity.
    pub fn columns_for_entity(&self, entity: &str) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.entity == entity)
            .map(|c| c.column.as_str())
            .collect()
    }

    /// Get all entities that have required columns.
    pub fn entities(&self) -> HashSet<&str> {
        self.columns.iter().map(|c| c.entity.as_str()).collect()
    }

    /// Get the total number of required columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if no columns are required.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AggregationType;
    use crate::semantic::planner::resolved::{ResolvedEntity, ResolvedFilter, ResolvedMeasure};
    use crate::semantic::planner::types::FilterOp;

    fn create_test_lineage() -> ColumnLineageGraph {
        use crate::semantic::column_lineage::LineageEdge;

        let mut lineage = ColumnLineageGraph::new();

        // source.order_id -> fact.order_id (passthrough)
        lineage.add_edge(
            ColumnRef::new("orders_source", "order_id"),
            ColumnRef::new("orders_fact", "order_id"),
            LineageEdge::passthrough(),
        );

        // source.amount -> fact.revenue (aggregate)
        lineage.add_edge(
            ColumnRef::new("orders_source", "amount"),
            ColumnRef::new("orders_fact", "revenue"),
            LineageEdge::aggregate(),
        );

        // source.quantity, source.price -> fact.total (computed)
        lineage.add_edge(
            ColumnRef::new("orders_source", "quantity"),
            ColumnRef::new("orders_fact", "total"),
            LineageEdge::transform("quantity * price"),
        );
        lineage.add_edge(
            ColumnRef::new("orders_source", "price"),
            ColumnRef::new("orders_fact", "total"),
            LineageEdge::transform("quantity * price"),
        );

        // customers source columns (self-referential, no upstream)
        // These are source columns - they have no upstream dependencies

        lineage
    }

    fn create_simple_query() -> ValidatedQuery {
        use std::collections::HashMap;

        ValidatedQuery {
            query: ResolvedQuery {
                from: ResolvedEntity {
                    name: "orders_fact".into(),
                    physical_table: "fact_orders".into(),
                    physical_schema: Some("dbo".into()),
                    materialized: true,
                },
                referenced_entities: HashSet::new(),
                filters: vec![],
                group_by: vec![],
                select: vec![ResolvedSelect::Measure {
                    measure: ResolvedMeasure {
                        entity_alias: "orders_fact".into(),
                        name: "revenue".into(),
                        aggregation: AggregationType::Sum,
                        source_column: "amount".into(),
                        filter: None,
                        definition_filter: None,
                    },
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            },
            join_tree: ResolvedJoinTree::empty("orders_fact"),
            entity_info: HashMap::new(),
        }
    }

    #[test]
    fn test_prune_simple_measure() {
        let lineage = create_test_lineage();
        let query = create_simple_query();
        let pruner = ColumnPruner::new(&lineage);

        let required = pruner.required_columns(&query);

        // revenue depends on orders_source.amount
        assert!(required.contains(&ColumnRef::new("orders_source", "amount")));
    }

    #[test]
    fn test_prune_computed_column() {
        let lineage = create_test_lineage();
        let pruner = ColumnPruner::new(&lineage);

        // Query that selects 'total' (computed from quantity * price)
        let mut query = create_simple_query();
        query.query.select = vec![ResolvedSelect::Column {
            column: ResolvedColumn {
                entity_alias: "orders_fact".into(),
                logical_name: "total".into(),
                physical_name: "total".into(),
            },
            alias: None,
        }];

        let required = pruner.required_columns(&query);

        // total depends on both quantity and price
        assert!(required.contains(&ColumnRef::new("orders_source", "quantity")));
        assert!(required.contains(&ColumnRef::new("orders_source", "price")));
    }

    #[test]
    fn test_prune_preserves_filter_columns() {
        let lineage = create_test_lineage();
        let pruner = ColumnPruner::new(&lineage);

        let mut query = create_simple_query();
        query.query.filters = vec![ResolvedFilter {
            column: ResolvedColumn {
                entity_alias: "customers".into(),
                logical_name: "region".into(),
                physical_name: "region".into(),
            },
            op: FilterOp::Eq,
            value: crate::semantic::planner::types::FilterValue::String("APAC".into()),
        }];

        let required = pruner.required_columns(&query);

        // Filter column should be included (as source column since no lineage)
        assert!(required.contains(&ColumnRef::new("customers", "region")));
    }

    #[test]
    fn test_prune_preserves_join_keys() {
        use crate::model::Cardinality;
        use crate::semantic::model_graph::JoinEdge;

        let lineage = create_test_lineage();
        let pruner = ColumnPruner::new(&lineage);

        let mut query = create_simple_query();
        query.join_tree.edges = vec![JoinEdge {
            from_entity: "orders_fact".into(),
            to_entity: "customers".into(),
            from_column: "customer_id".into(),
            to_column: "customer_id".into(),
            cardinality: Cardinality::ManyToOne,
        }];

        let required = pruner.required_columns(&query);

        // Join keys must be included even if not in SELECT
        assert!(required.contains(&ColumnRef::new("orders_fact", "customer_id")));
        assert!(required.contains(&ColumnRef::new("customers", "customer_id")));
    }

    #[test]
    fn test_pruned_columns_helpers() {
        let mut columns = HashSet::new();
        columns.insert(ColumnRef::new("orders", "order_id"));
        columns.insert(ColumnRef::new("orders", "amount"));
        columns.insert(ColumnRef::new("customers", "name"));

        let pruned = PrunedColumns::new(columns);

        assert!(pruned.is_needed("orders", "order_id"));
        assert!(pruned.is_needed("orders", "amount"));
        assert!(pruned.is_needed("customers", "name"));
        assert!(!pruned.is_needed("orders", "status"));

        assert_eq!(pruned.columns_for_entity("orders").len(), 2);
        assert_eq!(pruned.columns_for_entity("customers").len(), 1);

        let entities = pruned.entities();
        assert!(entities.contains("orders"));
        assert!(entities.contains("customers"));

        assert_eq!(pruned.len(), 3);
        assert!(!pruned.is_empty());
    }
}
