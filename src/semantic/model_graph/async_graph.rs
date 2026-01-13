//! Async wrapper for ModelGraph with metadata introspection.
//!
//! This module provides `AsyncModelGraph`, which wraps a `ModelGraph` and adds
//! async methods for on-demand metadata introspection via a `MetadataProvider`.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    AsyncModelGraph                          │
//! │  ┌───────────────────────────────────────────────────────┐  │
//! │  │  ModelGraph (sync)     │  MetadataProvider (async)    │  │
//! │  │  - Entity graph        │  - Worker RPC calls          │  │
//! │  │  - Path finding        │  - Introspection             │  │
//! │  │  - Relationships       │  - Column stats              │  │
//! │  └───────────────────────────────────────────────────────┘  │
//! │                           │                                  │
//! │                           ▼                                  │
//! │  ┌───────────────────────────────────────────────────────┐  │
//! │  │              MetadataCache (SQLite)                   │  │
//! │  │  - Persistent storage                                 │  │
//! │  │  - Connection-scoped                                  │  │
//! │  └───────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::cache::{CacheKey, CacheResult, MetadataCache};
use crate::metadata::{
    ForeignKeyInfo, MetadataProvider, SchemaInfo, TableInfo, TableMetadata,
};
use crate::model::{Cardinality, Relationship, SourceEntity};
use crate::semantic::inference::{InferenceConfig, InferenceEngine, TableInfo as InferenceTableInfo};
use crate::worker::WorkerError;

use super::{GraphResult, ModelGraph};

/// Error type for async graph operations.
#[derive(Debug, thiserror::Error)]
pub enum AsyncGraphError {
    #[error("Graph error: {0}")]
    Graph(#[from] super::GraphError),

    #[error("Worker error: {0}")]
    Worker(#[from] WorkerError),

    #[error("Cache error: {0}")]
    Cache(#[from] crate::cache::CacheError),

    #[error("Entity not found: {0}")]
    EntityNotFound(String),
}

pub type AsyncGraphResult<T> = Result<T, AsyncGraphError>;

/// Async wrapper for ModelGraph with metadata introspection.
///
/// This wrapper adds async methods for on-demand introspection:
/// - `get_or_fetch_entity`: Get an entity, fetching from DB if not in graph
/// - `discover_relationships`: Find FK and inferred relationships
/// - `introspect_schema`: Batch introspect all tables in a schema
pub struct AsyncModelGraph<P: MetadataProvider> {
    /// The underlying sync graph (protected by RwLock for async access)
    inner: RwLock<ModelGraph>,

    /// Metadata provider for async introspection
    provider: Arc<P>,

    /// Persistent cache
    cache: MetadataCache,

    /// Connection hash for cache keys
    conn_hash: String,

    /// Default schema for introspection
    default_schema: String,

    /// Track which entities were introspected (vs defined in Lua)
    introspected_entities: RwLock<HashSet<String>>,

    /// Inference configuration
    inference_config: InferenceConfig,
}

impl<P: MetadataProvider> AsyncModelGraph<P> {
    /// Create a new AsyncModelGraph.
    ///
    /// # Arguments
    ///
    /// * `graph` - The base ModelGraph (from Lua model)
    /// * `provider` - Metadata provider for introspection
    /// * `driver` - Database driver name (for cache key)
    /// * `conn_string` - Connection string (for cache key)
    /// * `default_schema` - Default schema for unqualified table names
    pub fn new(
        graph: ModelGraph,
        provider: Arc<P>,
        driver: &str,
        conn_string: &str,
        default_schema: &str,
    ) -> CacheResult<Self> {
        let cache = MetadataCache::open()?;
        let conn_hash = CacheKey::hash_connection(driver, conn_string);

        Ok(Self {
            inner: RwLock::new(graph),
            provider,
            cache,
            conn_hash,
            default_schema: default_schema.to_string(),
            introspected_entities: RwLock::new(HashSet::new()),
            inference_config: InferenceConfig::default(),
        })
    }

    /// Create with a custom cache (for testing).
    pub fn with_cache(
        graph: ModelGraph,
        provider: Arc<P>,
        cache: MetadataCache,
        conn_hash: String,
        default_schema: String,
    ) -> Self {
        Self {
            inner: RwLock::new(graph),
            provider,
            cache,
            conn_hash,
            default_schema,
            introspected_entities: RwLock::new(HashSet::new()),
            inference_config: InferenceConfig::default(),
        }
    }

    /// Set inference configuration.
    pub fn with_inference_config(mut self, config: InferenceConfig) -> Self {
        self.inference_config = config;
        self
    }

    /// Get read access to the underlying graph.
    pub async fn graph(&self) -> tokio::sync::RwLockReadGuard<'_, ModelGraph> {
        self.inner.read().await
    }

    /// Check if an entity exists in the graph.
    pub async fn has_entity(&self, name: &str) -> bool {
        self.inner.read().await.has_entity(name)
    }

    /// Check if an entity was introspected (vs defined in Lua).
    pub async fn is_introspected(&self, name: &str) -> bool {
        self.introspected_entities.read().await.contains(name)
    }

    // =========================================================================
    // Cache Operations
    // =========================================================================

    /// Get table metadata from cache.
    fn get_cached_metadata(&self, schema: &str, table: &str) -> Option<TableMetadata> {
        let key = CacheKey::table_metadata(&self.conn_hash, schema, table);
        self.cache.get(&key).ok().flatten()
    }

    /// Store table metadata in cache.
    fn cache_metadata(&self, metadata: &TableMetadata) -> CacheResult<()> {
        let key = CacheKey::table_metadata(&self.conn_hash, &metadata.schema, &metadata.name);
        self.cache.set(&key, metadata)
    }

    /// Get foreign keys from cache.
    fn get_cached_fks(&self, schema: &str, table: &str) -> Option<Vec<ForeignKeyInfo>> {
        let key = CacheKey::foreign_keys(&self.conn_hash, schema, table);
        self.cache.get(&key).ok().flatten()
    }

    /// Store foreign keys in cache.
    fn cache_fks(&self, schema: &str, table: &str, fks: &[ForeignKeyInfo]) -> CacheResult<()> {
        let key = CacheKey::foreign_keys(&self.conn_hash, schema, table);
        self.cache.set(&key, &fks.to_vec())
    }

    /// Get schema tables from cache.
    fn get_cached_tables(&self, schema: &str) -> Option<Vec<TableInfo>> {
        let key = CacheKey::tables(&self.conn_hash, schema);
        self.cache.get(&key).ok().flatten()
    }

    /// Store schema tables in cache.
    fn cache_tables(&self, schema: &str, tables: &[TableInfo]) -> CacheResult<()> {
        let key = CacheKey::tables(&self.conn_hash, schema);
        self.cache.set(&key, &tables.to_vec())
    }

    // =========================================================================
    // Introspection Operations
    // =========================================================================

    /// Get an entity, fetching from database if not in graph.
    ///
    /// This is the main entry point for lazy introspection:
    /// 1. Check if entity exists in graph → return immediately
    /// 2. Check cache → add to graph if found
    /// 3. Fetch from database → cache and add to graph
    pub async fn get_or_fetch_entity(&self, name: &str) -> AsyncGraphResult<()> {
        // Fast path: already in graph
        if self.has_entity(name).await {
            return Ok(());
        }

        // Parse schema.table or use default schema
        let (schema, table) = self.parse_entity_name(name);

        // Check cache first
        if let Some(metadata) = self.get_cached_metadata(&schema, &table) {
            self.add_introspected_entity(metadata).await?;
            return Ok(());
        }

        // Fetch from database
        let metadata = self.provider.get_table(&schema, &table).await?;

        // Cache it
        let _ = self.cache_metadata(&metadata);

        // Add to graph
        self.add_introspected_entity(metadata).await?;

        Ok(())
    }

    /// Add an introspected entity to the graph.
    async fn add_introspected_entity(&self, metadata: TableMetadata) -> GraphResult<()> {
        let name = metadata.name.clone();
        let source: SourceEntity = metadata.into();

        let mut graph = self.inner.write().await;
        graph.add_source(source);

        self.introspected_entities.write().await.insert(name);

        Ok(())
    }

    /// Parse an entity name into (schema, table).
    fn parse_entity_name(&self, name: &str) -> (String, String) {
        if let Some((schema, table)) = name.split_once('.') {
            (schema.to_string(), table.to_string())
        } else {
            (self.default_schema.clone(), name.to_string())
        }
    }

    /// Discover relationships for an entity.
    ///
    /// This combines:
    /// 1. Foreign key constraints from the database
    /// 2. Inferred relationships from naming conventions
    ///
    /// Discovered relationships are added to the graph.
    pub async fn discover_relationships(&self, entity_name: &str) -> AsyncGraphResult<usize> {
        // Ensure entity exists
        self.get_or_fetch_entity(entity_name).await?;

        let (schema, table) = self.parse_entity_name(entity_name);
        let mut added = 0;

        // Get foreign keys (from cache or fetch)
        let fks = match self.get_cached_fks(&schema, &table) {
            Some(fks) => fks,
            None => {
                let fks = self.provider.get_foreign_keys(&schema, &table).await?;
                let _ = self.cache_fks(&schema, &table, &fks);
                fks
            }
        };

        // Add FK relationships
        for fk in &fks {
            // Ensure referenced entity exists
            let ref_name = if fk.referenced_schema.is_empty() || fk.referenced_schema == self.default_schema {
                fk.referenced_table.clone()
            } else {
                format!("{}.{}", fk.referenced_schema, fk.referenced_table)
            };

            if self.get_or_fetch_entity(&ref_name).await.is_ok() {
                // Add relationship (FK column to referenced column)
                if fk.columns.len() == 1 && fk.referenced_columns.len() == 1 {
                    let rel = Relationship::from_foreign_key(
                        entity_name,
                        &ref_name,
                        &fk.columns[0],
                        &fk.referenced_columns[0],
                        Cardinality::ManyToOne,
                    );

                    let mut graph = self.inner.write().await;
                    if graph.add_relationship(rel).unwrap_or(false) {
                        added += 1;
                    }
                }
            }
        }

        // Infer relationships from naming conventions
        added += self.infer_relationships_for_entity(entity_name).await?;

        Ok(added)
    }

    /// Infer relationships for an entity using naming conventions.
    async fn infer_relationships_for_entity(&self, entity_name: &str) -> AsyncGraphResult<usize> {
        let graph = self.inner.read().await;

        // Get the source entity
        let source = match graph.model().sources.get(entity_name) {
            Some(s) => s.clone(),
            None => return Ok(0),
        };

        // Build table info for inference
        let table_info = InferenceTableInfo {
            schema: source.schema.clone().unwrap_or_default(),
            name: source.name.clone(),
            columns: source
                .columns
                .values()
                .map(|c| crate::semantic::inference::ColumnInfo {
                    name: c.name.clone(),
                    data_type: format!("{:?}", c.data_type),
                    is_nullable: c.nullable,
                    is_unique: None,
                })
                .collect(),
            primary_key: source.primary_key.clone(),
        };

        // Collect all tables for inference context
        let all_tables: Vec<InferenceTableInfo> = graph
            .model()
            .sources
            .values()
            .map(|s| InferenceTableInfo {
                schema: s.schema.clone().unwrap_or_default(),
                name: s.name.clone(),
                columns: s
                    .columns
                    .values()
                    .map(|c| crate::semantic::inference::ColumnInfo {
                        name: c.name.clone(),
                        data_type: format!("{:?}", c.data_type),
                        is_nullable: c.nullable,
                        is_unique: None,
                    })
                    .collect(),
                primary_key: s.primary_key.clone(),
            })
            .collect();

        drop(graph); // Release read lock

        // Run inference
        let engine = InferenceEngine::with_config(self.inference_config.clone());
        let inferred = engine.infer_relationships(&table_info, &all_tables);

        let mut added = 0;

        for inf in inferred {
            // Only add high-confidence relationships
            if inf.confidence >= self.inference_config.min_confidence {
                let rel = Relationship::inferred(
                    entity_name,
                    &inf.to_table,
                    &inf.from_column,
                    &inf.to_column,
                    Cardinality::ManyToOne, // Inferred are typically many-to-one
                    &inf.rule,
                    inf.confidence,
                );

                let mut graph = self.inner.write().await;
                if graph.add_relationship(rel).unwrap_or(false) {
                    added += 1;
                }
            }
        }

        Ok(added)
    }

    /// Batch introspect all tables in a schema.
    ///
    /// This is more efficient than individual fetches for compilation.
    pub async fn introspect_schema(&self, schema: &str) -> AsyncGraphResult<usize> {
        // Get table list (from cache or fetch)
        let tables = match self.get_cached_tables(schema) {
            Some(t) => t,
            None => {
                let t = self.provider.list_tables(schema).await?;
                let _ = self.cache_tables(schema, &t);
                t
            }
        };

        // Batch fetch metadata for all tables
        let table_refs: Vec<(String, String)> = tables
            .iter()
            .map(|t| (t.schema.clone(), t.name.clone()))
            .collect();

        let metadatas = self.provider.get_tables_batch(&table_refs).await?;

        // Add all to graph
        let mut added = 0;
        for metadata in metadatas {
            let _ = self.cache_metadata(&metadata);

            if !self.has_entity(&metadata.name).await {
                self.add_introspected_entity(metadata).await?;
                added += 1;
            }
        }

        Ok(added)
    }

    /// List available schemas.
    pub async fn list_schemas(&self) -> AsyncGraphResult<Vec<SchemaInfo>> {
        // Check cache
        let key = CacheKey::schemas(&self.conn_hash);
        if let Some(schemas) = self.cache.get::<Vec<SchemaInfo>>(&key).ok().flatten() {
            return Ok(schemas);
        }

        // Fetch from provider
        let schemas = self.provider.list_schemas().await?;
        let _ = self.cache.set(&key, &schemas);

        Ok(schemas)
    }

    /// Clear the cache for this connection.
    pub fn clear_cache(&self) -> CacheResult<usize> {
        self.cache.clear_connection(&self.conn_hash)
    }

    // =========================================================================
    // Statistics Operations (Phase 2)
    // =========================================================================

    /// Get column statistics from cache.
    fn get_cached_column_stats(
        &self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Option<crate::metadata::ColumnStats> {
        let key = CacheKey::column_stats(&self.conn_hash, schema, table, column);
        self.cache.get(&key).ok().flatten()
    }

    /// Store column statistics in cache.
    fn cache_column_stats(
        &self,
        schema: &str,
        table: &str,
        column: &str,
        stats: &crate::metadata::ColumnStats,
    ) -> CacheResult<()> {
        let key = CacheKey::column_stats(&self.conn_hash, schema, table, column);
        self.cache.set(&key, stats)
    }

    /// Get value overlap from cache.
    fn get_cached_value_overlap(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> Option<crate::metadata::ValueOverlap> {
        let key = CacheKey::value_overlap(
            &self.conn_hash,
            from_schema,
            from_table,
            from_column,
            to_schema,
            to_table,
            to_column,
        );
        self.cache.get(&key).ok().flatten()
    }

    /// Store value overlap in cache.
    #[allow(clippy::too_many_arguments)]
    fn cache_value_overlap(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
        overlap: &crate::metadata::ValueOverlap,
    ) -> CacheResult<()> {
        let key = CacheKey::value_overlap(
            &self.conn_hash,
            from_schema,
            from_table,
            from_column,
            to_schema,
            to_table,
            to_column,
        );
        self.cache.set(&key, overlap)
    }

    /// Get or fetch column statistics.
    ///
    /// Checks cache first, then fetches from provider if not found.
    pub async fn get_or_fetch_column_stats(
        &self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> AsyncGraphResult<crate::metadata::ColumnStats> {
        // Check cache first
        if let Some(stats) = self.get_cached_column_stats(schema, table, column) {
            return Ok(stats);
        }

        // Fetch from provider
        let stats = self.provider.get_column_stats(schema, table, column).await?;
        let _ = self.cache_column_stats(schema, table, column, &stats);
        Ok(stats)
    }

    /// Get or fetch value overlap between two columns.
    ///
    /// Checks cache first, then fetches from provider if not found.
    pub async fn get_or_fetch_value_overlap(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> AsyncGraphResult<crate::metadata::ValueOverlap> {
        // Check cache first
        if let Some(overlap) = self.get_cached_value_overlap(
            from_schema,
            from_table,
            from_column,
            to_schema,
            to_table,
            to_column,
        ) {
            return Ok(overlap);
        }

        // Fetch from provider
        let overlap = self
            .provider
            .check_value_overlap(
                from_schema,
                from_table,
                from_column,
                to_schema,
                to_table,
                to_column,
            )
            .await?;
        let _ = self.cache_value_overlap(
            from_schema,
            from_table,
            from_column,
            to_schema,
            to_table,
            to_column,
            &overlap,
        );
        Ok(overlap)
    }

    /// Validate an inferred relationship using database statistics.
    ///
    /// This fetches column stats and value overlap to refine confidence
    /// and determine cardinality.
    pub async fn validate_relationship_with_stats(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> AsyncGraphResult<crate::semantic::inference::signals::statistics::StatisticsSignals> {
        use crate::semantic::inference::signals::statistics::StatisticsSignals;

        // Fetch stats in parallel
        let (from_stats, to_stats, overlap) = tokio::try_join!(
            self.get_or_fetch_column_stats(from_schema, from_table, from_column),
            self.get_or_fetch_column_stats(to_schema, to_table, to_column),
            self.get_or_fetch_value_overlap(
                from_schema,
                from_table,
                from_column,
                to_schema,
                to_table,
                to_column
            )
        )?;

        Ok(StatisticsSignals::from_stats(&from_stats, &to_stats, &overlap))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{ColumnStats, DatabaseInfo, ValueOverlap};
    use crate::model::Model;
    use async_trait::async_trait;

    /// Mock provider for testing.
    struct MockProvider {
        tables: std::collections::HashMap<String, TableMetadata>,
    }

    impl MockProvider {
        fn new() -> Self {
            Self {
                tables: std::collections::HashMap::new(),
            }
        }

        fn with_table(mut self, metadata: TableMetadata) -> Self {
            let key = format!("{}.{}", metadata.schema, metadata.name);
            self.tables.insert(key, metadata);
            self
        }
    }

    #[async_trait]
    impl MetadataProvider for MockProvider {
        async fn list_schemas(&self) -> Result<Vec<SchemaInfo>, WorkerError> {
            Ok(vec![SchemaInfo {
                name: "main".to_string(),
                is_default: true,
            }])
        }

        async fn list_tables(&self, schema: &str) -> Result<Vec<TableInfo>, WorkerError> {
            Ok(self
                .tables
                .values()
                .filter(|t| t.schema == schema)
                .map(|t| TableInfo {
                    schema: t.schema.clone(),
                    name: t.name.clone(),
                    table_type: crate::metadata::TableType::Table,
                })
                .collect())
        }

        async fn get_table(&self, schema: &str, table: &str) -> Result<TableMetadata, WorkerError> {
            let key = format!("{}.{}", schema, table);
            self.tables
                .get(&key)
                .cloned()
                .ok_or_else(|| WorkerError::remote("NOT_FOUND", format!("Table not found: {}", key)))
        }

        async fn get_foreign_keys(
            &self,
            _schema: &str,
            _table: &str,
        ) -> Result<Vec<ForeignKeyInfo>, WorkerError> {
            Ok(vec![])
        }

        async fn get_column_stats(
            &self,
            _schema: &str,
            _table: &str,
            _column: &str,
        ) -> Result<ColumnStats, WorkerError> {
            Ok(ColumnStats {
                total_count: 100,
                distinct_count: 100,
                null_count: 0,
                is_unique: true,
                sample_values: vec![],
            })
        }

        async fn check_value_overlap(
            &self,
            _left_schema: &str,
            _left_table: &str,
            _left_column: &str,
            _right_schema: &str,
            _right_table: &str,
            _right_column: &str,
        ) -> Result<ValueOverlap, WorkerError> {
            Ok(ValueOverlap {
                left_sample_size: 100,
                left_total_distinct: 100,
                right_total_distinct: 50,
                overlap_count: 100,
                overlap_percentage: 100.0,
                right_is_superset: true,
                left_is_unique: false,
                right_is_unique: true,
            })
        }

        async fn get_database_info(&self) -> Result<DatabaseInfo, WorkerError> {
            Ok(DatabaseInfo {
                product_name: "MockDB".to_string(),
                product_version: "1.0".to_string(),
                database_name: "test".to_string(),
                default_schema: Some("main".to_string()),
                collation: None,
            })
        }
    }

    fn sample_table_metadata(name: &str) -> TableMetadata {
        use crate::metadata::{ColumnInfo, TableType};

        TableMetadata {
            schema: "main".to_string(),
            name: name.to_string(),
            table_type: TableType::Table,
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    position: 1,
                    data_type: "INTEGER".to_string(),
                    is_nullable: false,
                    max_length: None,
                    numeric_precision: None,
                    numeric_scale: None,
                    default_value: None,
                    is_identity: true,
                    is_computed: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    position: 2,
                    data_type: "VARCHAR".to_string(),
                    is_nullable: true,
                    max_length: Some(255),
                    numeric_precision: None,
                    numeric_scale: None,
                    default_value: None,
                    is_identity: false,
                    is_computed: false,
                },
            ],
            primary_key: Some(crate::metadata::PrimaryKeyInfo {
                name: "pk".to_string(),
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![],
            unique_constraints: vec![],
        }
    }

    #[tokio::test]
    async fn test_get_or_fetch_entity() {
        let provider = MockProvider::new().with_table(sample_table_metadata("orders"));
        let graph = ModelGraph::from_model(Model::new()).unwrap();
        let cache = MetadataCache::open_in_memory().unwrap();

        let async_graph = AsyncModelGraph::with_cache(
            graph,
            Arc::new(provider),
            cache,
            "test_conn".to_string(),
            "main".to_string(),
        );

        // Entity doesn't exist initially
        assert!(!async_graph.has_entity("orders").await);

        // Fetch it
        async_graph.get_or_fetch_entity("orders").await.unwrap();

        // Now it exists
        assert!(async_graph.has_entity("orders").await);
        assert!(async_graph.is_introspected("orders").await);
    }

    #[tokio::test]
    async fn test_introspect_schema() {
        let provider = MockProvider::new()
            .with_table(sample_table_metadata("orders"))
            .with_table(sample_table_metadata("customers"));

        let graph = ModelGraph::from_model(Model::new()).unwrap();
        let cache = MetadataCache::open_in_memory().unwrap();

        let async_graph = AsyncModelGraph::with_cache(
            graph,
            Arc::new(provider),
            cache,
            "test_conn".to_string(),
            "main".to_string(),
        );

        // Batch introspect
        let added = async_graph.introspect_schema("main").await.unwrap();
        assert_eq!(added, 2);

        // Both entities exist
        assert!(async_graph.has_entity("orders").await);
        assert!(async_graph.has_entity("customers").await);
    }
}
