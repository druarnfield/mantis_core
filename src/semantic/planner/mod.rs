//! Query Planner - Multi-phase semantic query compilation.
//!
//! The planner converts semantic queries to SQL through four phases:
//!
//! ```text
//! SemanticQuery (user input)
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │  Phase 1: RESOLVE   │  Resolve field references to physical names
//! │  (resolve.rs)       │  Collect referenced entities
//! └─────────────────────┘
//!        │
//!        ▼
//!   ResolvedQuery
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │  Phase 2: VALIDATE  │  Check join safety (no fan-out)
//! │  (validate.rs)      │  Validate GROUP BY completeness
//! └─────────────────────┘
//!        │
//!        ▼
//!   ValidatedQuery
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │  Phase 3: PLAN      │  Build logical operation tree
//! │  (logical.rs)       │  Structure query operations
//! └─────────────────────┘
//!        │
//!        ▼
//!   LogicalPlan
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │  Phase 4: EMIT      │  Convert to SQL Query builder
//! │  (emit.rs)          │  Apply physical names
//! └─────────────────────┘
//!        │
//!        ▼
//!   Query (ready for dialect serialization)
//! ```

pub mod emit;
pub mod emit_multi;
pub mod emit_time;
pub mod logical;
pub mod prune;
pub mod report;
pub mod resolve;
pub mod resolved;
pub mod types;
pub mod validate;

// Re-export main types
pub use emit::Emitter;
pub use emit_multi::MultiFactEmitter;
pub use emit_time::TimeEmitter;
pub use prune::{ColumnPruner, PrunedColumns};
pub use logical::{LogicalPlan, LogicalPlanner};
pub use resolve::Resolver;
pub use resolved::{
    FactAggregate, FactJoinKey, MultiFactQuery, ResolvedColumn, ResolvedMeasure,
    ResolvedQuery, ResolvedQueryPlan, ResolvedSelect, SharedDimension,
};
pub use types::{
    DerivedBinaryOp, DerivedExpr, DerivedField, FieldFilter, FieldRef, FilterOp, FilterValue,
    OrderField, SelectField, SemanticQuery, TimeFunction,
};
pub use validate::{ValidatedQuery, Validator};

use crate::query::Query;
use crate::semantic::column_lineage::ColumnLineageGraph;
use crate::semantic::error::{PlanResult, SemanticError};
use crate::semantic::model_graph::ModelGraph;

/// Query planner - converts semantic queries to SQL.
///
/// This is the main entry point for query planning. It orchestrates
/// the four phases: resolve → validate → plan → emit.
///
/// Optionally uses column lineage for:
/// - Cycle detection (validates no circular column dependencies)
/// - Column pruning (determines minimal columns needed)
pub struct QueryPlanner<'a> {
    graph: &'a ModelGraph,
    lineage: Option<&'a ColumnLineageGraph>,
    default_schema: String,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(graph: &'a ModelGraph) -> Self {
        Self {
            graph,
            lineage: None,
            default_schema: "dbo".to_string(),
        }
    }

    /// Enable column lineage analysis for cycle detection and column pruning.
    pub fn with_lineage(mut self, lineage: &'a ColumnLineageGraph) -> Self {
        self.lineage = Some(lineage);
        self
    }

    /// Set the default schema for entities without explicit schema.
    pub fn with_default_schema(mut self, schema: &str) -> Self {
        self.default_schema = schema.to_string();
        self
    }

    /// Plan a semantic query into a SQL query.
    ///
    /// This is the main entry point that runs all four phases.
    /// If lineage is enabled, also validates for cycles and prunes columns.
    ///
    /// For multi-fact queries (measures from multiple facts), this automatically
    /// routes to the symmetric aggregate pattern with CTEs.
    pub fn plan(&self, query: &SemanticQuery) -> PlanResult<Query> {
        // Pre-validation: Check for lineage cycles
        if let Some(lineage) = &self.lineage {
            lineage.validate_no_cycles().map_err(SemanticError::from)?;
        }

        // Phase 1: Resolve - detect single vs multi-fact
        let resolver = Resolver::new(self.graph);

        // Check if this is a multi-fact query
        if resolver.is_multi_fact(query)? {
            return self.plan_multi_fact(query, &resolver);
        }

        // Single-fact query - use standard path
        let resolved = resolver.resolve(query)?;

        // Phase 2: Validate
        let validator = Validator::new(self.graph);
        let validated = validator.validate(resolved)?;

        // Phase 2.5: Column Pruning (if lineage enabled)
        let pruned_columns = self.lineage.map(|lineage| {
            let pruner = ColumnPruner::new(lineage);
            PrunedColumns::new(pruner.required_columns(&validated))
        });

        // Phase 3: Logical Plan (with graph for virtual fact support)
        let logical_planner = LogicalPlanner::with_graph(self.graph);
        let logical_plan = logical_planner.plan(&validated)?;

        // Phase 4: Emit
        let mut emitter = Emitter::new().with_default_schema(&self.default_schema);
        if let Some(pruned) = pruned_columns {
            emitter = emitter.with_pruned_columns(pruned);
        }
        emitter.emit(&logical_plan)
    }

    /// Plan a multi-fact query using the symmetric aggregate pattern.
    fn plan_multi_fact(&self, query: &SemanticQuery, resolver: &Resolver) -> PlanResult<Query> {
        let anchors = resolver.detect_anchors(query)?;
        let multi_fact = resolver.resolve_multi_fact(query, &anchors)?;

        // Use the multi-fact emitter
        let emitter = MultiFactEmitter::new(&multi_fact);
        Ok(emitter.emit())
    }

    /// Plan with access to intermediate representations.
    ///
    /// Use this when you need to inspect or modify the plan at each phase.
    pub fn plan_phases(&self, query: &SemanticQuery) -> PlanResult<PlanPhases> {
        // Pre-validation: Check for lineage cycles
        if let Some(lineage) = &self.lineage {
            lineage.validate_no_cycles().map_err(SemanticError::from)?;
        }

        // Phase 1: Resolve
        let resolver = Resolver::new(self.graph);
        let resolved = resolver.resolve(query)?;

        // Phase 2: Validate
        let validator = Validator::new(self.graph);
        let validated = validator.validate(resolved)?;

        // Phase 2.5: Column Pruning (if lineage enabled)
        let pruned_columns = self.lineage.map(|lineage| {
            let pruner = ColumnPruner::new(lineage);
            PrunedColumns::new(pruner.required_columns(&validated))
        });

        // Phase 3: Logical Plan (with graph for virtual fact support)
        let logical_planner = LogicalPlanner::with_graph(self.graph);
        let logical_plan = logical_planner.plan(&validated)?;

        // Phase 4: Emit
        let mut emitter = Emitter::new().with_default_schema(&self.default_schema);
        if let Some(ref pruned) = pruned_columns {
            emitter = emitter.with_pruned_columns(pruned.clone());
        }
        let sql_query = emitter.emit(&logical_plan)?;

        Ok(PlanPhases {
            validated,
            logical_plan,
            sql_query,
            pruned_columns,
        })
    }
}

/// Result of planning with all intermediate representations.
#[derive(Debug)]
pub struct PlanPhases {
    /// The validated query (after phases 1 & 2).
    pub validated: ValidatedQuery,

    /// The logical plan (after phase 3).
    pub logical_plan: LogicalPlan,

    /// The final SQL query (after phase 4).
    pub sql_query: Query,

    /// Pruned columns (if lineage was enabled).
    pub pruned_columns: Option<PrunedColumns>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::Dialect;
    use crate::model::{
        Cardinality, DataType, FactDefinition, Model, Relationship, SourceEntity,
    };

    fn sample_graph() -> ModelGraph {
        let model = Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.fact_orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("order_date", DataType::Date)
                    .with_required_column("amount", DataType::Decimal(10, 2))
                    .with_primary_key(vec!["order_id"]),
            )
            .with_source(
                SourceEntity::new("customers", "dbo.dim_customers")
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("customer_name", DataType::String)
                    .with_required_column("region", DataType::String)
                    .with_primary_key(vec!["customer_id"]),
            )
            // Source relationship: orders -> customers (Many-to-One)
            .with_relationship(Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            // Fact relationship: orders_fact -> customers (Many-to-One, via customer_id)
            // This allows the fact to join to dimension tables
            .with_relationship(Relationship::new(
                "orders_fact",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            .with_fact(
                FactDefinition::new("orders_fact", "dbo.orders_fact")
                    .with_grain("orders", "order_id")
                    .with_sum("revenue", "amount")
                    .with_count("order_count", "*"),
            );

        ModelGraph::from_model(model).unwrap()
    }

    #[test]
    fn test_simple_plan() {
        let graph = sample_graph();
        let planner = QueryPlanner::new(&graph);

        // Query the fact entity which has the measures
        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let query = planner.plan(&sq).unwrap();
        let sql = query.to_sql(Dialect::DuckDb);

        println!("Generated SQL:\n{}", sql);

        assert!(sql.contains("SUM"));
        assert!(sql.contains("orders_fact"));
    }

    #[test]
    fn test_plan_with_join() {
        let graph = sample_graph();
        let planner = QueryPlanner::new(&graph);

        // Query from fact, join to customers for filter/group by
        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![FieldFilter {
                field: FieldRef::new("customers", "region"),
                op: FilterOp::Eq,
                value: FilterValue::String("APAC".into()),
            }],
            group_by: vec![FieldRef::new("customers", "region")],
            select: vec![
                SelectField::new("orders_fact", "revenue"),
                SelectField::new("orders_fact", "order_count"),
            ],
            derived: vec![],
            order_by: vec![OrderField::desc("orders_fact", "revenue")],
            limit: Some(10),
        };

        let query = planner.plan(&sq).unwrap();
        let sql = query.to_sql(Dialect::DuckDb);

        println!("Generated SQL:\n{}", sql);

        assert!(sql.contains("JOIN"));
        assert!(sql.contains("dim_customers"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_plan_phases() {
        let graph = sample_graph();
        let planner = QueryPlanner::new(&graph);

        // Query from fact, join to customers for group by
        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![FieldRef::new("customers", "region")],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let phases = planner.plan_phases(&sq).unwrap();

        // Check the validated query
        assert_eq!(phases.validated.query.from.name, "orders_fact");
        assert!(phases.validated.join_tree.is_safe);

        // Check the logical plan
        assert!(matches!(phases.logical_plan, LogicalPlan::Project(_)));

        // Check the SQL
        let sql = phases.sql_query.to_sql(Dialect::DuckDb);
        assert!(sql.contains("GROUP BY"));
    }

    #[test]
    fn test_multiple_dialects() {
        let graph = sample_graph();
        let planner = QueryPlanner::new(&graph);

        // Query from fact, join to customers for group by
        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![FieldRef::new("customers", "region")],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![OrderField::desc("orders_fact", "revenue")],
            limit: Some(10),
        };

        let query = planner.plan(&sq).unwrap();

        // Test different dialects
        let duckdb = query.to_sql(Dialect::DuckDb);
        let tsql = query.to_sql(Dialect::TSql);
        let postgres = query.to_sql(Dialect::Postgres);

        println!("DuckDB:\n{}\n", duckdb);
        println!("T-SQL:\n{}\n", tsql);
        println!("Postgres:\n{}\n", postgres);

        // DuckDB/Postgres use LIMIT
        assert!(duckdb.contains("LIMIT 10"));
        assert!(postgres.contains("LIMIT 10"));

        // T-SQL uses FETCH
        assert!(tsql.contains("FETCH"));
    }

    #[test]
    fn test_planner_with_lineage() {
        use crate::semantic::column_lineage::{ColumnLineageGraph, ColumnRef, LineageEdge};

        let graph = sample_graph();

        // Build lineage graph for orders_fact
        let mut lineage = ColumnLineageGraph::new();
        // orders.order_id -> orders_fact.order_id
        lineage.add_edge(
            ColumnRef::new("orders", "order_id"),
            ColumnRef::new("orders_fact", "order_id"),
            LineageEdge::passthrough(),
        );
        // orders.amount -> orders_fact.revenue
        lineage.add_edge(
            ColumnRef::new("orders", "amount"),
            ColumnRef::new("orders_fact", "revenue"),
            LineageEdge::aggregate(),
        );

        let planner = QueryPlanner::new(&graph).with_lineage(&lineage);

        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        // Should succeed (no cycles)
        let result = planner.plan(&sq);
        assert!(result.is_ok());
    }

    #[test]
    fn test_planner_with_cycle_detection() {
        use crate::semantic::column_lineage::{ColumnLineageGraph, ColumnRef, LineageEdge};
        use crate::semantic::error::SemanticError;

        let graph = sample_graph();

        // Build lineage graph with a cycle
        let mut lineage = ColumnLineageGraph::new();
        lineage.add_edge(
            ColumnRef::new("fact", "a"),
            ColumnRef::new("fact", "b"),
            LineageEdge::transform("..."),
        );
        lineage.add_edge(
            ColumnRef::new("fact", "b"),
            ColumnRef::new("fact", "c"),
            LineageEdge::transform("..."),
        );
        lineage.add_edge(
            ColumnRef::new("fact", "c"),
            ColumnRef::new("fact", "a"),
            LineageEdge::transform("cycle!"),
        );

        let planner = QueryPlanner::new(&graph).with_lineage(&lineage);

        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        // Should fail due to cycle
        let result = planner.plan(&sq);
        assert!(result.is_err());

        match result.unwrap_err() {
            SemanticError::ColumnLineageCycle { cycles } => {
                assert_eq!(cycles.len(), 1);
                assert_eq!(cycles[0].len(), 3);
            }
            e => panic!("Expected ColumnLineageCycle, got {:?}", e),
        }
    }

    #[test]
    fn test_plan_phases_with_pruning() {
        use crate::semantic::column_lineage::{ColumnLineageGraph, ColumnRef, LineageEdge};

        let graph = sample_graph();

        // Build lineage graph
        let mut lineage = ColumnLineageGraph::new();
        lineage.add_edge(
            ColumnRef::new("orders", "amount"),
            ColumnRef::new("orders_fact", "revenue"),
            LineageEdge::aggregate(),
        );
        lineage.add_edge(
            ColumnRef::new("orders", "order_id"),
            ColumnRef::new("orders_fact", "order_count"),
            LineageEdge::aggregate(),
        );

        let planner = QueryPlanner::new(&graph).with_lineage(&lineage);

        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let phases = planner.plan_phases(&sq).unwrap();

        // Should have pruned columns
        assert!(phases.pruned_columns.is_some());
        let pruned = phases.pruned_columns.unwrap();

        // revenue depends on orders.amount
        assert!(
            pruned.is_needed("orders", "amount"),
            "Should need orders.amount for revenue"
        );

        // order_count's source (order_id) should NOT be needed since we didn't select it
        assert!(
            !pruned.is_needed("orders", "order_id"),
            "Should not need orders.order_id since order_count is not selected"
        );
    }

    #[test]
    fn test_virtual_fact_reconstruction() {
        // Create a model where the fact is virtual (materialized = false)
        let model = Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("amount", DataType::Decimal(10, 2))
                    .with_primary_key(vec!["order_id"]),
            )
            .with_source(
                SourceEntity::new("customers", "dbo.customers")
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("customer_name", DataType::String)
                    .with_primary_key(vec!["customer_id"]),
            )
            .with_relationship(Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            // Virtual fact - does not have a physical table
            .with_fact(
                FactDefinition::new("orders_fact", "dbo.orders_fact")
                    .with_grain("orders", "order_id")
                    .with_sum("revenue", "amount")
                    .with_materialized(false), // Virtual fact!
            );

        let graph = ModelGraph::from_model(model).unwrap();
        let planner = QueryPlanner::new(&graph);

        // Query the virtual fact
        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let query = planner.plan(&sq).unwrap();
        let sql = query.to_sql(Dialect::DuckDb);

        println!("Virtual fact SQL:\n{}", sql);

        // Should query from the source table (orders), not the target (orders_fact)
        // The SQL uses quoted identifiers like "dbo"."orders"
        assert!(
            sql.contains(r#""dbo"."orders""#) || sql.contains("dbo.orders"),
            "Should query source table 'orders', not virtual fact table. Got:\n{}",
            sql
        );
        // Should NOT contain a direct reference to the non-existent orders_fact table
        // (it may appear in column aliases, but not in FROM clause)
        assert!(
            !sql.contains(r#"FROM "dbo"."orders_fact""#) && !sql.contains("FROM dbo.orders_fact"),
            "Should not directly query virtual fact table in FROM clause. Got:\n{}",
            sql
        );
    }

    #[test]
    fn test_materialized_fact_uses_target_table() {
        // Create a model where the fact is materialized (default)
        let model = Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("amount", DataType::Decimal(10, 2))
                    .with_primary_key(vec!["order_id"]),
            )
            // Materialized fact (default behavior)
            .with_fact(
                FactDefinition::new("orders_fact", "dbo.orders_fact")
                    .with_grain("orders", "order_id")
                    .with_sum("revenue", "amount"),
                // materialized defaults to true
            );

        let graph = ModelGraph::from_model(model).unwrap();
        let planner = QueryPlanner::new(&graph);

        let sq = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let query = planner.plan(&sq).unwrap();
        let sql = query.to_sql(Dialect::DuckDb);

        println!("Materialized fact SQL:\n{}", sql);

        // Should query the fact table directly (with quoted identifiers)
        assert!(
            sql.contains(r#""dbo"."orders_fact""#) || sql.contains("dbo.orders_fact"),
            "Should query materialized fact table. Got:\n{}",
            sql
        );
    }
}
