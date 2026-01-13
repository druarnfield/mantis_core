//! MetadataProvider trait definition.
//!
//! The MetadataProvider trait abstracts over different ways of fetching
//! database metadata. The primary implementation uses the WorkerClient
//! for async RPC calls to the Go worker.

use async_trait::async_trait;

use super::types::*;
use crate::semantic::inference::{
    InferenceConfig, InferenceEngine, InferredRelationship, TableInfo as InferenceTableInfo,
};
use crate::worker::WorkerError;

/// Result type for metadata operations.
pub type MetadataResult<T> = Result<T, WorkerError>;

/// Trait for fetching database metadata.
///
/// This trait provides async methods for fetching metadata from a database
/// via the worker, and a sync method for local relationship inference.
///
/// # Architecture Note
///
/// Relationship inference happens locally in Rust using the `InferenceEngine`,
/// not via worker calls. The worker provides raw metadata and cardinality
/// statistics. This trait combines both capabilities.
///
/// # Example
///
/// ```ignore
/// use mantis::metadata::MetadataProvider;
///
/// async fn example(provider: &impl MetadataProvider) -> MetadataResult<()> {
///     // Fetch metadata via worker RPC
///     let schemas = provider.list_schemas().await?;
///     let tables = provider.list_tables("main").await?;
///     let table = provider.get_table("main", "orders").await?;
///
///     // Infer relationships locally (no RPC)
///     let inferred = provider.infer_relationships(&[table], Default::default());
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait MetadataProvider: Send + Sync {
    // =========================================================================
    // Worker RPC calls (async, network I/O)
    // =========================================================================

    /// List all schemas in the database.
    async fn list_schemas(&self) -> MetadataResult<Vec<SchemaInfo>>;

    /// List all tables in a schema.
    ///
    /// If `schema` is empty, lists tables from the default schema.
    async fn list_tables(&self, schema: &str) -> MetadataResult<Vec<TableInfo>>;

    /// Get complete metadata for a table.
    async fn get_table(&self, schema: &str, table: &str) -> MetadataResult<TableMetadata>;

    /// Get foreign keys for a table.
    async fn get_foreign_keys(&self, schema: &str, table: &str) -> MetadataResult<Vec<ForeignKeyInfo>>;

    /// Get column statistics for cardinality analysis.
    async fn get_column_stats(
        &self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> MetadataResult<ColumnStats>;

    /// Check value overlap between two columns.
    ///
    /// This is useful for validating potential join relationships.
    async fn check_value_overlap(
        &self,
        left_schema: &str,
        left_table: &str,
        left_column: &str,
        right_schema: &str,
        right_table: &str,
        right_column: &str,
    ) -> MetadataResult<ValueOverlap>;

    /// Get database information.
    async fn get_database_info(&self) -> MetadataResult<DatabaseInfo>;

    // =========================================================================
    // Batch operations (default implementations using parallel fetches)
    // =========================================================================

    /// Batch fetch multiple tables.
    ///
    /// Default implementation fetches tables in parallel using `join_all`.
    async fn get_tables_batch(
        &self,
        tables: &[(String, String)],
    ) -> MetadataResult<Vec<TableMetadata>> {
        let futures: Vec<_> = tables
            .iter()
            .map(|(schema, table)| self.get_table(schema, table))
            .collect();

        let results = futures::future::join_all(futures).await;

        // Collect results, failing if any failed
        results.into_iter().collect()
    }

    /// List all tables across all schemas.
    ///
    /// Default implementation fetches schemas first, then tables for each.
    async fn list_all_tables(&self) -> MetadataResult<Vec<TableInfo>> {
        let schemas = self.list_schemas().await?;

        let futures: Vec<_> = schemas
            .iter()
            .map(|s| self.list_tables(&s.name))
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut all_tables = Vec::new();
        for result in results {
            all_tables.extend(result?);
        }

        Ok(all_tables)
    }

    // =========================================================================
    // Local inference (no worker call)
    // =========================================================================

    /// Infer relationships using the Rust InferenceEngine.
    ///
    /// This does NOT call the worker - it uses local heuristics based on
    /// naming conventions and column types.
    ///
    /// # Arguments
    ///
    /// * `tables` - Tables to analyze for relationships.
    /// * `config` - Configuration for the inference engine.
    ///
    /// # Returns
    ///
    /// A list of inferred relationships with confidence scores.
    fn infer_relationships(
        &self,
        tables: &[TableMetadata],
        config: InferenceConfig,
    ) -> Vec<InferredRelationship> {
        let table_infos: Vec<InferenceTableInfo> = tables
            .iter()
            .map(|t| t.into())
            .collect();

        let mut engine = InferenceEngine::with_config(config);

        // Prepare: analyze schema conventions
        engine.prepare(&table_infos);

        // Load database constraints for high-confidence detection
        engine.load_constraints(tables);

        engine.infer_all_relationships(&table_infos)
    }

    /// Infer relationships with default configuration.
    fn infer_relationships_default(&self, tables: &[TableMetadata]) -> Vec<InferredRelationship> {
        self.infer_relationships(tables, InferenceConfig::default())
    }
}

/// Extension trait for MetadataProvider with additional convenience methods.
#[async_trait]
pub trait MetadataProviderExt: MetadataProvider {
    /// Fetch table metadata and infer relationships in one call.
    ///
    /// This is a convenience method that:
    /// 1. Fetches metadata for all tables in a schema
    /// 2. Infers relationships between them
    async fn introspect_schema(
        &self,
        schema: &str,
        config: InferenceConfig,
    ) -> MetadataResult<IntrospectionResult> {
        let table_list = self.list_tables(schema).await?;

        let table_refs: Vec<(String, String)> = table_list
            .iter()
            .map(|t| (t.schema.clone(), t.name.clone()))
            .collect();

        let tables = self.get_tables_batch(&table_refs).await?;
        let inferred_relationships = self.infer_relationships(&tables, config);

        // Collect explicit foreign keys from all tables
        let explicit_relationships: Vec<ForeignKeyInfo> = tables
            .iter()
            .flat_map(|t| t.foreign_keys.clone())
            .collect();

        Ok(IntrospectionResult {
            tables,
            explicit_relationships,
            inferred_relationships,
        })
    }

    /// Validate a potential relationship by checking value overlap.
    ///
    /// Returns a validation result indicating whether the relationship
    /// appears valid and the confidence level.
    async fn validate_relationship(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> MetadataResult<RelationshipValidation> {
        let overlap = self
            .check_value_overlap(
                from_schema,
                from_table,
                from_column,
                to_schema,
                to_table,
                to_column,
            )
            .await?;

        let is_valid = overlap.suggests_foreign_key();
        let confidence = if is_valid {
            // High overlap + superset + unique target = high confidence
            0.9
        } else if overlap.overlap_percentage >= 80.0 {
            0.7
        } else if overlap.overlap_percentage >= 50.0 {
            0.5
        } else {
            0.3
        };

        Ok(RelationshipValidation {
            is_valid,
            confidence,
            overlap,
        })
    }

    /// Validate inferred relationships by checking cardinality in parallel.
    ///
    /// Filters out relationships where the target column is not unique
    /// (i.e., many-to-many relationships that aren't valid FK targets).
    ///
    /// Returns the validated relationships and count of rejected ones.
    async fn validate_inferred_relationships(
        &self,
        relationships: Vec<InferredRelationship>,
        default_schema: &str,
    ) -> (Vec<InferredRelationship>, usize) {
        use futures::future::join_all;
        use std::collections::HashMap;

        // Separate DB constraints (keep as-is) from inferred (need validation)
        let (db_constraints, inferred): (Vec<_>, Vec<_>) = relationships
            .into_iter()
            .partition(|r| r.source == crate::semantic::inference::RelationshipSource::DatabaseConstraint);

        if inferred.is_empty() {
            return (db_constraints, 0);
        }

        // Collect unique target columns to query (avoid duplicate queries)
        let mut unique_targets: HashMap<(String, String, String), usize> = HashMap::new();
        for (idx, rel) in inferred.iter().enumerate() {
            let schema = if rel.to_schema.is_empty() {
                default_schema.to_string()
            } else {
                rel.to_schema.clone()
            };
            let key = (schema, rel.to_table.clone(), rel.to_column.clone());
            unique_targets.entry(key).or_insert(idx);
        }

        // Fetch stats for all unique targets in parallel
        let queries: Vec<_> = unique_targets
            .keys()
            .map(|(schema, table, column)| {
                let schema = schema.clone();
                let table = table.clone();
                let column = column.clone();
                async move {
                    let result = self.get_column_stats(&schema, &table, &column).await;
                    ((schema, table, column), result)
                }
            })
            .collect();

        let results: Vec<_> = join_all(queries).await;

        // Build lookup map of stats results
        let stats_map: HashMap<(String, String, String), bool> = results
            .into_iter()
            .map(|(key, result)| {
                let is_valid = match result {
                    Ok(stats) => {
                        // Valid if unique or near-unique (>= 95% distinct)
                        stats.is_unique
                            || (stats.total_count > 0
                                && stats.distinct_count as f64 / stats.total_count as f64 >= 0.95)
                    }
                    Err(_) => true, // If query fails, assume valid
                };
                (key, is_valid)
            })
            .collect();

        // Filter inferred relationships based on validation
        let mut validated = db_constraints;
        let mut rejected_count = 0;

        for rel in inferred {
            let schema = if rel.to_schema.is_empty() {
                default_schema.to_string()
            } else {
                rel.to_schema.clone()
            };
            let key = (schema, rel.to_table.clone(), rel.to_column.clone());

            if *stats_map.get(&key).unwrap_or(&true) {
                validated.push(rel);
            } else {
                rejected_count += 1;
            }
        }

        (validated, rejected_count)
    }
}

// Blanket implementation for all MetadataProvider implementations
impl<T: MetadataProvider> MetadataProviderExt for T {}

/// Result of schema introspection.
#[derive(Debug, Clone)]
pub struct IntrospectionResult {
    /// All table metadata.
    pub tables: Vec<TableMetadata>,
    /// Explicit foreign key relationships from database constraints.
    pub explicit_relationships: Vec<ForeignKeyInfo>,
    /// Inferred relationships from naming conventions.
    pub inferred_relationships: Vec<InferredRelationship>,
}

/// Result of relationship validation.
#[derive(Debug, Clone)]
pub struct RelationshipValidation {
    /// Whether the relationship appears valid.
    pub is_valid: bool,
    /// Confidence score (0.0 - 1.0).
    pub confidence: f64,
    /// The value overlap analysis.
    pub overlap: ValueOverlap,
}
