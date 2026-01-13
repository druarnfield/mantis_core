//! Unified semantic model with lazy-loaded analysis graphs.
//!
//! `SemanticModel` provides a single entry point for semantic layer operations,
//! unifying the `Model`, `ModelGraph`, and `ColumnLineageGraph` under one type
//! with consistent lifecycle management.
//!
//! # Example
//!
//! ```ignore
//! use mantis::semantic::SemanticModel;
//!
//! // Create from a Model
//! let semantic = SemanticModel::new(model)?;
//!
//! // Get a planner with lineage enabled (cycle detection + column pruning)
//! let result = semantic.planner().plan(&query)?;
//!
//! // Or get a fast planner without lineage overhead
//! let result = semantic.planner_fast().plan(&query)?;
//!
//! // Access the underlying graphs directly if needed
//! let entity_graph = semantic.entity_graph();
//! let lineage = semantic.column_lineage();  // Computed lazily on first access
//! ```

use std::cell::OnceCell;

use crate::cache::{CacheKey, MetadataCache};
use crate::model::Model;

use super::column_lineage::{ColumnLineageGraph, SerializedLineage};
use super::error::SemanticError;
use super::model_graph::ModelGraph;
use super::planner::QueryPlanner;

/// Unified semantic model with lazy-loaded analysis graphs.
///
/// Owns the Model and provides:
/// - Entity-level graph (ModelGraph) - always available, used for join path finding
/// - Column-level lineage (ColumnLineageGraph) - lazy loaded on first access
/// - Consistent cache integration with content-based invalidation
///
/// # Lazy Loading
///
/// The column lineage graph is expensive to compute for large models. It's only
/// built when first accessed via `column_lineage()` or `column_lineage_cached()`.
/// If you only need entity-level operations, use `planner_fast()` to skip lineage.
pub struct SemanticModel {
    /// The underlying model definition
    model: Model,
    /// Entity-level graph (relationships, join paths)
    entity_graph: ModelGraph,
    /// Column-level lineage (lazy, computed on demand)
    column_lineage: OnceCell<ColumnLineageGraph>,
    /// Content hash for cache invalidation
    content_hash: String,
}

impl SemanticModel {
    /// Create a SemanticModel from a Model.
    ///
    /// This builds the entity graph immediately but defers column lineage
    /// computation until first access.
    pub fn new(model: Model) -> Result<Self, SemanticError> {
        let content_hash = model.content_hash();
        let entity_graph = ModelGraph::from_model(model.clone())?;

        Ok(Self {
            model,
            entity_graph,
            column_lineage: OnceCell::new(),
            content_hash,
        })
    }

    /// Get the underlying model.
    pub fn model(&self) -> &Model {
        &self.model
    }

    /// Get the entity-level graph (for join path finding, etc.).
    pub fn entity_graph(&self) -> &ModelGraph {
        &self.entity_graph
    }

    /// Get the column lineage graph (lazily computed on first access).
    ///
    /// The lineage graph enables:
    /// - Cycle detection (circular column dependencies)
    /// - Column pruning (minimal columns needed for a query)
    /// - Impact analysis (what's affected by a column change)
    pub fn column_lineage(&self) -> &ColumnLineageGraph {
        self.column_lineage
            .get_or_init(|| ColumnLineageGraph::from_model(&self.model))
    }

    /// Get the column lineage graph, using cache if available.
    ///
    /// This tries to load from the cache first (using content hash as key),
    /// falling back to computation if not cached. The result is stored in
    /// the cache for future use.
    pub fn column_lineage_cached(&self, cache: &MetadataCache) -> &ColumnLineageGraph {
        self.column_lineage.get_or_init(|| {
            let key = CacheKey::lineage(&self.content_hash);

            // Try cache first
            if let Ok(Some(data)) = cache.get::<SerializedLineage>(&key) {
                return ColumnLineageGraph::from_serializable(data);
            }

            // Compute and cache
            let lineage = ColumnLineageGraph::from_model(&self.model);
            let _ = cache.set(&key, &lineage.to_serializable());
            lineage
        })
    }

    /// Check if column lineage has been computed.
    ///
    /// Returns `true` if `column_lineage()` or `column_lineage_cached()` has
    /// been called at least once.
    pub fn is_lineage_computed(&self) -> bool {
        self.column_lineage.get().is_some()
    }

    /// Get the content hash for cache keys.
    ///
    /// This hash changes when the model definition changes, ensuring
    /// cached lineage data is invalidated appropriately.
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Create a QueryPlanner with lineage enabled.
    ///
    /// The planner will:
    /// - Validate for cycles before planning (fails fast if circular deps exist)
    /// - Compute pruned columns (minimal set needed for the query)
    ///
    /// Use this for production queries where optimization matters.
    pub fn planner(&self) -> QueryPlanner<'_> {
        QueryPlanner::new(&self.entity_graph).with_lineage(self.column_lineage())
    }

    /// Create a QueryPlanner with lineage enabled, using cached lineage.
    ///
    /// Same as `planner()` but loads lineage from cache if available.
    pub fn planner_cached(&self, cache: &MetadataCache) -> QueryPlanner<'_> {
        QueryPlanner::new(&self.entity_graph).with_lineage(self.column_lineage_cached(cache))
    }

    /// Create a QueryPlanner without lineage (faster, no pruning).
    ///
    /// Use this when:
    /// - You don't need column pruning optimization
    /// - You're doing quick exploratory queries
    /// - You've already validated the model has no cycles
    pub fn planner_fast(&self) -> QueryPlanner<'_> {
        QueryPlanner::new(&self.entity_graph)
    }
}

impl std::fmt::Debug for SemanticModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entity_count = self.model.sources.len()
            + self.model.facts.len()
            + self.model.dimensions.len();

        f.debug_struct("SemanticModel")
            .field("content_hash", &self.content_hash)
            .field("lineage_computed", &self.is_lineage_computed())
            .field("entities", &entity_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model::{Cardinality, DataType, FactDefinition, Relationship, SourceEntity};
    use crate::semantic::planner::{SelectField, SemanticQuery};

    fn sample_model() -> Model {
        Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.fact_orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("amount", DataType::Decimal(10, 2))
                    .with_primary_key(vec!["order_id"]),
            )
            .with_source(
                SourceEntity::new("customers", "dbo.dim_customers")
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("name", DataType::String)
                    .with_primary_key(vec!["customer_id"]),
            )
            .with_relationship(Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            .with_fact(
                FactDefinition::new("orders_fact", "dbo.orders_fact")
                    .with_grain("orders", "order_id")
                    .with_sum("revenue", "amount"),
            )
    }

    #[test]
    fn test_semantic_model_creation() {
        let model = sample_model();
        let semantic = SemanticModel::new(model).unwrap();

        // Entity graph is available immediately
        assert!(semantic.entity_graph().has_entity("orders"));
        assert!(semantic.entity_graph().has_entity("customers"));

        // Lineage not computed yet
        assert!(!semantic.is_lineage_computed());

        // Content hash is set
        assert!(!semantic.content_hash().is_empty());
    }

    #[test]
    fn test_semantic_model_lazy_lineage() {
        let model = sample_model();
        let semantic = SemanticModel::new(model).unwrap();

        // Lineage not computed yet
        assert!(!semantic.is_lineage_computed());

        // Access lineage (triggers computation)
        let lineage = semantic.column_lineage();
        assert!(lineage.column_count() > 0);

        // Now it's computed
        assert!(semantic.is_lineage_computed());

        // Second access returns same instance (no recomputation)
        let lineage2 = semantic.column_lineage();
        assert_eq!(lineage.column_count(), lineage2.column_count());
    }

    #[test]
    fn test_semantic_model_cache_integration() {
        let model = sample_model();
        let semantic = SemanticModel::new(model).unwrap();
        let cache = MetadataCache::open_in_memory().unwrap();

        // First access computes and caches
        let lineage = semantic.column_lineage_cached(&cache);
        assert!(lineage.column_count() > 0);

        // Verify it's in cache
        let key = CacheKey::lineage(semantic.content_hash());
        let cached: Option<SerializedLineage> = cache.get(&key).unwrap();
        assert!(cached.is_some());
    }

    #[test]
    fn test_semantic_model_planner() {
        let model = sample_model();
        let semantic = SemanticModel::new(model).unwrap();

        let query = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        // planner() includes lineage
        let result = semantic.planner().plan(&query);
        assert!(result.is_ok());

        // Lineage was computed
        assert!(semantic.is_lineage_computed());
    }

    #[test]
    fn test_semantic_model_planner_fast() {
        let model = sample_model();
        let semantic = SemanticModel::new(model).unwrap();

        let query = SemanticQuery {
            from: Some("orders_fact".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders_fact", "revenue")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        // planner_fast() skips lineage
        let result = semantic.planner_fast().plan(&query);
        assert!(result.is_ok());

        // Lineage was NOT computed
        assert!(!semantic.is_lineage_computed());
    }

    #[test]
    fn test_semantic_model_content_hash_changes() {
        let model1 = sample_model();
        let model2 = sample_model().with_source(
            SourceEntity::new("products", "dbo.dim_products")
                .with_required_column("product_id", DataType::Int64),
        );

        let semantic1 = SemanticModel::new(model1).unwrap();
        let semantic2 = SemanticModel::new(model2).unwrap();

        // Different models have different hashes
        assert_ne!(semantic1.content_hash(), semantic2.content_hash());
    }

    #[test]
    fn test_semantic_model_debug() {
        let model = sample_model();
        let semantic = SemanticModel::new(model).unwrap();

        let debug = format!("{:?}", semantic);
        assert!(debug.contains("SemanticModel"));
        assert!(debug.contains("lineage_computed: false"));
    }
}
