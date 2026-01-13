//! Query Executor - High-level API for executing semantic queries.
//!
//! The `QueryExecutor` provides a convenient facade for running queries
//! defined in the model or constructed programmatically.
//!
//! # Example
//!
//! ```rust,ignore
//! use mantis::model::loader::ModelLoader;
//! use mantis::semantic::QueryExecutor;
//! use mantis::dialect::Dialect;
//!
//! // Load model from Lua
//! let model = ModelLoader::new().load_from_file("models/ecommerce.lua")?;
//!
//! // Create executor with lineage enabled (cycle detection + column pruning)
//! let executor = QueryExecutor::new(model)?
//!     .with_default_schema("public")
//!     .with_lineage();
//!
//! // Execute a named query
//! let sql = executor.query_to_sql("sales_by_region", Dialect::Postgres)?;
//! println!("{}", sql);
//! ```

use crate::cache::MetadataCache;
use crate::dialect::Dialect;
use crate::model::Model;
use crate::query::Query;
use crate::semantic::error::{SemanticError, SemanticResult};
use crate::semantic::model_graph::ModelGraph;
use crate::semantic::planner::types::SemanticQuery;
use crate::semantic::semantic_model::SemanticModel;

/// Query executor - high-level API for semantic query execution.
///
/// The executor owns a `SemanticModel` and provides convenient methods
/// for executing named queries or ad-hoc semantic queries.
///
/// # Lineage Support
///
/// By default, lineage analysis is disabled for maximum performance.
/// Enable it with `.with_lineage()` to get:
/// - Cycle detection (fails fast if circular column dependencies exist)
/// - Column pruning (only select columns actually needed)
pub struct QueryExecutor {
    semantic: SemanticModel,
    default_schema: String,
    use_lineage: bool,
}

impl QueryExecutor {
    /// Create a new executor from a model.
    ///
    /// This builds the internal semantic model needed for query planning.
    /// Lineage is disabled by default for performance; use `.with_lineage()`
    /// to enable cycle detection and column pruning.
    ///
    /// # Errors
    ///
    /// Returns an error if the model graph cannot be built (e.g., due to
    /// invalid relationships or missing entities).
    pub fn new(model: Model) -> SemanticResult<Self> {
        let semantic = SemanticModel::new(model)?;
        Ok(Self {
            semantic,
            default_schema: "dbo".into(),
            use_lineage: false,
        })
    }

    /// Set the default schema for entities without explicit schema.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let executor = QueryExecutor::new(model)?
    ///     .with_default_schema("analytics");
    /// ```
    pub fn with_default_schema(mut self, schema: &str) -> Self {
        self.default_schema = schema.into();
        self
    }

    /// Enable lineage analysis for cycle detection and column pruning.
    ///
    /// When enabled, the executor will:
    /// - Validate for cycles before planning (fails fast if circular deps exist)
    /// - Compute pruned columns (minimal set needed for the query)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let executor = QueryExecutor::new(model)?
    ///     .with_lineage();
    /// ```
    pub fn with_lineage(mut self) -> Self {
        self.use_lineage = true;
        self
    }

    /// Enable lineage analysis with cache support.
    ///
    /// Same as `with_lineage()` but uses the provided cache to store/load
    /// the column lineage graph, avoiding recomputation on subsequent runs.
    pub fn with_cached_lineage(mut self, cache: &MetadataCache) -> Self {
        // Trigger lineage computation with cache
        let _ = self.semantic.column_lineage_cached(cache);
        self.use_lineage = true;
        self
    }

    /// Execute a named query from the model.
    ///
    /// Looks up the query by name in the model's queries collection,
    /// converts it to a semantic query with model-aware measure resolution,
    /// and plans it to SQL.
    ///
    /// # Errors
    ///
    /// Returns `SemanticError::UnknownQuery` if the query name is not found.
    /// Returns `SemanticError::UnknownMeasure` if a measure reference is invalid.
    /// Returns `SemanticError::UnknownEntity` if an entity reference is invalid.
    /// Other errors may occur during planning (invalid joins, etc).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let query = executor.execute_named("top_customers")?;
    /// let sql = query.to_sql(Dialect::Postgres);
    /// ```
    pub fn execute_named(&self, name: &str) -> SemanticResult<Query> {
        let model = self.semantic.model();

        let query_def = model
            .queries
            .get(name)
            .ok_or_else(|| SemanticError::UnknownQuery { name: name.into() })?;

        // Use model-aware conversion for proper measure resolution
        let semantic_query = query_def.to_semantic_query_with_model(model)?;
        self.execute(&semantic_query)
    }

    /// Execute a semantic query directly.
    ///
    /// Use this for programmatically constructed queries that aren't
    /// defined in the model.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mantis::semantic::planner::types::*;
    ///
    /// let query = SemanticQuery {
    ///     from: Some("sales".into()),
    ///     select: vec![SelectField::new("customers", "region")],
    ///     filters: vec![],
    ///     group_by: vec![FieldRef::new("customers", "region")],
    ///     derived: vec![],
    ///     order_by: vec![],
    ///     limit: Some(10),
    /// };
    ///
    /// let result = executor.execute(&query)?;
    /// ```
    pub fn execute(&self, query: &SemanticQuery) -> SemanticResult<Query> {
        let planner = if self.use_lineage {
            self.semantic.planner()
        } else {
            self.semantic.planner_fast()
        };
        planner.with_default_schema(&self.default_schema).plan(query)
    }

    /// Generate SQL for a named query in a specific dialect.
    ///
    /// This is a convenience method that combines `execute_named` and
    /// `Query::to_sql`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let sql = executor.query_to_sql("monthly_revenue", Dialect::Snowflake)?;
    /// println!("{}", sql);
    /// ```
    pub fn query_to_sql(&self, name: &str, dialect: Dialect) -> SemanticResult<String> {
        let query = self.execute_named(name)?;
        Ok(query.to_sql(dialect))
    }

    /// Generate SQL for a semantic query in a specific dialect.
    pub fn to_sql(&self, query: &SemanticQuery, dialect: Dialect) -> SemanticResult<String> {
        let result = self.execute(query)?;
        Ok(result.to_sql(dialect))
    }

    /// List all query names defined in the model.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// for name in executor.list_queries() {
    ///     println!("  - {}", name);
    /// }
    /// ```
    pub fn list_queries(&self) -> Vec<&str> {
        self.semantic
            .model()
            .queries
            .keys()
            .map(|s| s.as_str())
            .collect()
    }

    /// Get a reference to the underlying model.
    pub fn model(&self) -> &Model {
        self.semantic.model()
    }

    /// Get a reference to the semantic model.
    pub fn semantic_model(&self) -> &SemanticModel {
        &self.semantic
    }

    /// Get a reference to the entity graph.
    pub fn entity_graph(&self) -> &ModelGraph {
        self.semantic.entity_graph()
    }

    /// Get the default schema.
    pub fn default_schema(&self) -> &str {
        &self.default_schema
    }

    /// Check if lineage analysis is enabled.
    pub fn is_lineage_enabled(&self) -> bool {
        self.use_lineage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Cardinality, DataType, FactDefinition, QueryDefinition, QueryOrderBy, QuerySelect,
        Relationship, SourceEntity,
    };

    fn sample_model() -> Model {
        Model::new()
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
                    .with_required_column("name", DataType::String)
                    .with_required_column("region", DataType::String)
                    .with_primary_key(vec!["customer_id"]),
            )
            .with_relationship(Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            .with_relationship(Relationship::new(
                "sales",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            .with_fact(
                FactDefinition::new("sales", "dbo.fact_sales")
                    .with_grain("orders", "order_id")
                    .with_sum("revenue", "amount")
                    .with_count("order_count", "*"),
            )
            .with_query({
                let mut q = QueryDefinition::new("top_regions", "sales");
                q.select = vec![
                    QuerySelect::Dimension {
                        entity: "customers".into(),
                        column: "region".into(),
                    },
                    QuerySelect::Measure {
                        entity: None,
                        name: "revenue".into(),
                        alias: None,
                    },
                ];
                q.order_by = vec![QueryOrderBy::desc("revenue")];
                q.limit = Some(10);
                q
            })
    }

    #[test]
    fn test_executor_creation() {
        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        assert_eq!(executor.default_schema(), "dbo");
        assert!(!executor.list_queries().is_empty());
    }

    #[test]
    fn test_executor_with_schema() {
        let model = sample_model();
        let executor = QueryExecutor::new(model)
            .unwrap()
            .with_default_schema("analytics");

        assert_eq!(executor.default_schema(), "analytics");
    }

    #[test]
    fn test_list_queries() {
        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        let queries = executor.list_queries();
        assert!(queries.contains(&"top_regions"));
    }

    #[test]
    fn test_execute_named_query() {
        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        let query = executor.execute_named("top_regions").unwrap();
        let sql = query.to_sql(Dialect::Postgres);

        println!("Generated SQL:\n{}", sql);

        assert!(sql.contains("SUM"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_unknown_query_error() {
        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        let result = executor.execute_named("nonexistent");
        assert!(matches!(
            result,
            Err(SemanticError::UnknownQuery { name }) if name == "nonexistent"
        ));
    }

    #[test]
    fn test_query_to_sql() {
        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        let sql = executor
            .query_to_sql("top_regions", Dialect::DuckDb)
            .unwrap();

        assert!(sql.contains("SUM"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_model_access() {
        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        // Can access underlying model
        assert_eq!(executor.model().sources.len(), 2);
        assert_eq!(executor.model().facts.len(), 1);
    }

    #[test]
    fn test_unknown_measure_error() {
        // Create model with query referencing unknown measure
        let model = Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_primary_key(vec!["order_id"]),
            )
            .with_fact(
                FactDefinition::new("sales", "dbo.fact_sales")
                    .with_grain("orders", "order_id")
                    .with_count("order_count", "*"),
            )
            .with_query({
                let mut q = QueryDefinition::new("bad_query", "sales");
                q.select = vec![QuerySelect::Measure {
                    entity: None,
                    name: "unknown_measure".into(),
                    alias: None,
                }];
                q
            });

        let executor = QueryExecutor::new(model).unwrap();
        let result = executor.execute_named("bad_query");

        assert!(matches!(
            result,
            Err(SemanticError::UnknownMeasure { name }) if name == "unknown_measure"
        ));
    }

    #[test]
    fn test_programmatic_query() {
        use crate::semantic::planner::types::*;

        let model = sample_model();
        let executor = QueryExecutor::new(model).unwrap();

        // Build a query programmatically
        let query = SemanticQuery {
            from: Some("sales".into()),
            select: vec![SelectField::new("sales", "revenue")],
            filters: vec![],
            group_by: vec![],
            derived: vec![],
            order_by: vec![],
            limit: Some(5),
        };

        let result = executor.execute(&query).unwrap();
        let sql = result.to_sql(Dialect::Postgres);

        assert!(sql.contains("SUM"));
        assert!(sql.contains("LIMIT 5"));
    }
}
