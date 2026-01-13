//! Model integration for relationship inference.
//!
//! This module provides methods to infer relationships from a Model's source entities
//! and add them to the model's relationship list.

use std::collections::HashSet;

use crate::model::{Cardinality, Model, Relationship, SourceEntity};

use super::engine::{ColumnInfo, InferenceConfig, InferenceEngine, TableInfo};
use super::InferredRelationship;

/// Configuration for model-level relationship inference.
#[derive(Debug, Clone)]
pub struct ModelInferenceConfig {
    /// Minimum confidence threshold (0.0 to 1.0)
    pub min_confidence: f64,
    /// Maximum candidates per column
    pub max_candidates_per_column: usize,
    /// Whether to skip columns that are already part of explicit relationships
    pub skip_existing_relationships: bool,
}

impl Default for ModelInferenceConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.70,
            max_candidates_per_column: 2,
            skip_existing_relationships: true,
        }
    }
}

/// Result of running inference on a model.
#[derive(Debug, Clone)]
pub struct InferenceResult {
    /// Relationships that were inferred
    pub inferred: Vec<InferredRelationship>,
    /// Number of relationships added to the model
    pub added_count: usize,
    /// Number of relationships skipped (already existed)
    pub skipped_count: usize,
}

impl Model {
    /// Infer relationships between source entities and add them to the model.
    ///
    /// This uses heuristic rules based on column naming conventions to discover
    /// potential foreign key relationships. Inferred relationships have a confidence
    /// score - only those above the threshold are added.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut model = load_model(path)?;
    /// let result = model.infer_relationships(ModelInferenceConfig::default());
    /// println!("Added {} inferred relationships", result.added_count);
    /// ```
    pub fn infer_relationships(&mut self, config: ModelInferenceConfig) -> InferenceResult {
        // Build set of existing relationship pairs to avoid duplicates
        let existing_pairs: HashSet<(String, String, String, String)> = self
            .relationships
            .iter()
            .map(|r| {
                (
                    r.from_entity.to_lowercase(),
                    r.from_column.to_lowercase(),
                    r.to_entity.to_lowercase(),
                    r.to_column.to_lowercase(),
                )
            })
            .collect();

        // Convert sources to TableInfo for the inference engine
        let table_infos: Vec<TableInfo> = self
            .sources
            .values()
            .map(source_to_table_info)
            .collect();

        // Run inference
        let engine_config = InferenceConfig {
            min_confidence: config.min_confidence,
            validate_cardinality: false, // We don't have runtime stats
            max_candidates_per_column: config.max_candidates_per_column,
            ..Default::default()
        };
        let engine = InferenceEngine::with_config(engine_config);
        let inferred = engine.infer_all_relationships(&table_infos);

        let mut added_count = 0;
        let mut skipped_count = 0;

        for inf_rel in &inferred {
            let pair = (
                inf_rel.from_table.to_lowercase(),
                inf_rel.from_column.to_lowercase(),
                inf_rel.to_table.to_lowercase(),
                inf_rel.to_column.to_lowercase(),
            );

            // Skip if relationship already exists
            if config.skip_existing_relationships && existing_pairs.contains(&pair) {
                skipped_count += 1;
                continue;
            }

            // Convert to model Relationship
            let cardinality: Cardinality = inf_rel.cardinality;
            let relationship = Relationship::new(
                &inf_rel.from_table,
                &inf_rel.to_table,
                &inf_rel.from_column,
                &inf_rel.to_column,
                cardinality,
            );

            self.relationships.push(relationship);
            added_count += 1;
        }

        InferenceResult {
            inferred,
            added_count,
            skipped_count,
        }
    }

    /// Infer relationships with default configuration.
    pub fn infer_relationships_default(&mut self) -> InferenceResult {
        self.infer_relationships(ModelInferenceConfig::default())
    }

    /// Get candidate relationships without adding them to the model.
    ///
    /// Useful for previewing what would be inferred before committing.
    pub fn get_inferred_relationships(
        &self,
        config: ModelInferenceConfig,
    ) -> Vec<InferredRelationship> {
        let table_infos: Vec<TableInfo> = self
            .sources
            .values()
            .map(source_to_table_info)
            .collect();

        let engine_config = InferenceConfig {
            min_confidence: config.min_confidence,
            validate_cardinality: false,
            max_candidates_per_column: config.max_candidates_per_column,
            ..Default::default()
        };
        let engine = InferenceEngine::with_config(engine_config);

        let mut inferred = engine.infer_all_relationships(&table_infos);

        // Filter out existing relationships if configured
        if config.skip_existing_relationships {
            let existing_pairs: HashSet<(String, String, String, String)> = self
                .relationships
                .iter()
                .map(|r| {
                    (
                        r.from_entity.to_lowercase(),
                        r.from_column.to_lowercase(),
                        r.to_entity.to_lowercase(),
                        r.to_column.to_lowercase(),
                    )
                })
                .collect();

            inferred.retain(|inf_rel| {
                let pair = (
                    inf_rel.from_table.to_lowercase(),
                    inf_rel.from_column.to_lowercase(),
                    inf_rel.to_table.to_lowercase(),
                    inf_rel.to_column.to_lowercase(),
                );
                !existing_pairs.contains(&pair)
            });
        }

        inferred
    }
}

/// Convert a SourceEntity to TableInfo for the inference engine.
fn source_to_table_info(source: &SourceEntity) -> TableInfo {
    // Parse schema from table name if not explicitly set
    let schema = if let Some(ref schema) = source.schema {
        schema.clone()
    } else if source.table.contains('.') {
        let parts: Vec<&str> = source.table.splitn(2, '.').collect();
        parts[0].to_string()
    } else {
        "public".to_string()
    };

    let columns: Vec<ColumnInfo> = source
        .columns
        .values()
        .map(|col| ColumnInfo {
            name: col.name.clone(),
            data_type: format!("{:?}", col.data_type).to_lowercase(),
            is_nullable: col.nullable,
            // We don't have uniqueness info at definition time
            is_unique: if source.primary_key.contains(&col.name) {
                Some(true)
            } else {
                None
            },
        })
        .collect();

    TableInfo {
        schema,
        name: source.name.clone(), // Use logical name for matching
        columns,
        primary_key: source.primary_key.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DataType, SourceColumn, SourceEntity};

    fn make_test_model() -> Model {
        let mut model = Model::new();

        // Orders source
        let mut orders = SourceEntity::new("orders", "raw.orders");
        orders.columns.insert(
            "id".to_string(),
            SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                description: None,
            },
        );
        orders.columns.insert(
            "customer_id".to_string(),
            SourceColumn {
                name: "customer_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                description: None,
            },
        );
        orders.columns.insert(
            "product_id".to_string(),
            SourceColumn {
                name: "product_id".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                description: None,
            },
        );
        orders.primary_key = vec!["id".to_string()];
        model.add_source(orders);

        // Customers source
        let mut customers = SourceEntity::new("customers", "raw.customers");
        customers.columns.insert(
            "id".to_string(),
            SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                description: None,
            },
        );
        customers.columns.insert(
            "name".to_string(),
            SourceColumn {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                description: None,
            },
        );
        customers.primary_key = vec!["id".to_string()];
        model.add_source(customers);

        // Products source
        let mut products = SourceEntity::new("products", "raw.products");
        products.columns.insert(
            "id".to_string(),
            SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                description: None,
            },
        );
        products.columns.insert(
            "name".to_string(),
            SourceColumn {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                description: None,
            },
        );
        products.primary_key = vec!["id".to_string()];
        model.add_source(products);

        model
    }

    #[test]
    fn test_infer_relationships() {
        let mut model = make_test_model();

        let result = model.infer_relationships(ModelInferenceConfig {
            min_confidence: 0.60,
            max_candidates_per_column: 3,
            skip_existing_relationships: true,
        });

        // Should find customer_id -> customers.id and product_id -> products.id
        assert!(result.added_count >= 2, "Expected at least 2 relationships, got {}", result.added_count);

        // Model should now have relationships
        assert!(!model.relationships.is_empty());

        // Check that we found the customer relationship
        let customer_rel = model
            .relationships
            .iter()
            .find(|r| r.to_entity == "customers");
        assert!(customer_rel.is_some(), "Should find customer relationship");

        if let Some(rel) = customer_rel {
            assert_eq!(rel.from_column, "customer_id");
            assert_eq!(rel.to_column, "id");
        }
    }

    #[test]
    fn test_skip_existing_relationships() {
        let mut model = make_test_model();

        // Add an explicit relationship
        model.add_relationship(Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "id",
            Cardinality::ManyToOne,
        ));

        let result = model.infer_relationships(ModelInferenceConfig {
            min_confidence: 0.60,
            skip_existing_relationships: true,
            ..Default::default()
        });

        // The customer relationship should be skipped
        assert!(result.skipped_count >= 1);

        // Should only have 2 relationships (1 explicit + 1 inferred for products)
        // (might have more depending on inference rules)
        let customer_rels: Vec<_> = model
            .relationships
            .iter()
            .filter(|r| r.to_entity == "customers")
            .collect();
        assert_eq!(customer_rels.len(), 1, "Should not duplicate customer relationship");
    }

    #[test]
    fn test_get_inferred_relationships_preview() {
        let model = make_test_model();

        let candidates = model.get_inferred_relationships(ModelInferenceConfig {
            min_confidence: 0.60,
            ..Default::default()
        });

        // Should find candidates without modifying the model
        assert!(!candidates.is_empty());
        assert!(model.relationships.is_empty(), "Model should not be modified");
    }
}
