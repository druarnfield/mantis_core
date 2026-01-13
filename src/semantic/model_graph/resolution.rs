//! Entity and field resolution for ModelGraph.
//!
//! This module contains methods for resolving entity references,
//! columns, measures, and relationships.

use super::{EntityInfo, EntityType, GraphResult, ModelGraph, ModelResolvedField, SemanticError};

/// Parse a potentially qualified table name into (schema, table).
///
/// If the table name contains a `.`, split it into schema and table parts.
/// If an explicit schema is provided, it takes precedence.
fn parse_qualified_table(table_name: &str, explicit_schema: Option<&str>) -> (Option<String>, String) {
    // If explicit schema is provided, use it
    if let Some(schema) = explicit_schema {
        // Table name might still be qualified, extract just the table part
        let table = if let Some((_s, t)) = table_name.split_once('.') {
            t.to_string()
        } else {
            table_name.to_string()
        };
        return (Some(schema.to_string()), table);
    }

    // Parse qualified name from table_name
    if let Some((schema, table)) = table_name.split_once('.') {
        (Some(schema.to_string()), table.to_string())
    } else {
        (None, table_name.to_string())
    }
}

impl ModelGraph {
    /// Get a source entity by name.
    pub fn get_source(&self, name: &str) -> Option<&crate::model::SourceEntity> {
        self.model.sources.get(name)
    }

    /// Get a fact definition by name.
    pub fn get_fact(&self, name: &str) -> Option<&crate::model::FactDefinition> {
        self.model.facts.get(name)
    }

    /// Get a dimension definition by name.
    pub fn get_dimension(&self, name: &str) -> Option<&crate::model::DimensionDefinition> {
        self.model.dimensions.get(name)
    }

    /// Get entity information for query planning.
    ///
    /// Returns the physical table name, schema, and entity type for any entity
    /// (source, fact, or dimension). This is the primary method for query
    /// planning to resolve entity references.
    ///
    /// Supports role-playing dimensions: if `name` is a role alias (e.g., "order_date"),
    /// it resolves to the underlying dimension's physical table.
    pub fn get_entity_info(&self, name: &str) -> GraphResult<EntityInfo> {
        // First, check if this is a role alias
        let actual_entity = if let Some(alias) = self.role_aliases.get(name) {
            &alias.to_entity
        } else {
            name
        };

        // Check sources (always materialized as physical tables)
        if let Some(source) = self.model.sources.get(actual_entity) {
            let (schema, table) = parse_qualified_table(&source.table, source.schema.as_deref());
            return Ok(EntityInfo {
                name: name.to_string(), // Use the requested name (could be role)
                physical_table: table,
                physical_schema: schema,
                entity_type: EntityType::Source,
                materialized: true, // Sources are always physical tables
            });
        }

        // Check facts (may be virtual/unmaterialized)
        if let Some(fact) = self.model.facts.get(actual_entity) {
            let (schema, table) = parse_qualified_table(&fact.target_table, fact.target_schema.as_deref());
            return Ok(EntityInfo {
                name: name.to_string(),
                physical_table: table,
                physical_schema: schema,
                entity_type: EntityType::Fact,
                materialized: fact.materialized,
            });
        }

        // Check dimensions (may be virtual/unmaterialized)
        if let Some(dim) = self.model.dimensions.get(actual_entity) {
            let (schema, table) = parse_qualified_table(&dim.target_table, dim.target_schema.as_deref());
            return Ok(EntityInfo {
                name: name.to_string(),
                physical_table: table,
                physical_schema: schema,
                entity_type: EntityType::Dimension,
                materialized: dim.materialized,
            });
        }

        Err(SemanticError::UnknownEntity(name.into()))
    }

    /// Check if an entity has a specific column.
    ///
    /// Supports role aliases: if `entity` is a role name, checks the underlying dimension.
    pub fn has_column(&self, entity: &str, column: &str) -> bool {
        // Resolve role alias if present
        let actual_entity = if let Some(alias) = self.role_aliases.get(entity) {
            &alias.to_entity
        } else {
            entity
        };

        // Check sources
        if let Some(source) = self.model.sources.get(actual_entity) {
            return source.columns.contains_key(column);
        }

        // Check dimensions
        if let Some(dim) = self.model.dimensions.get(actual_entity) {
            return dim.columns.iter().any(|col| {
                let target_name = col.target_column.as_ref().unwrap_or(&col.source_column);
                target_name == column || col.source_column == column
            });
        }

        false
    }

    /// Check if an entity has a specific measure.
    pub fn has_measure(&self, entity: &str, measure: &str) -> bool {
        if let Some(fact) = self.model.facts.get(entity) {
            return fact.measures.contains_key(measure);
        }
        false
    }

    /// Resolve a column reference (entity.column) to its definition.
    ///
    /// Returns the SourceColumn if found, or an error if the entity or column doesn't exist.
    pub fn resolve_column(
        &self,
        entity: &str,
        column: &str,
    ) -> GraphResult<&crate::model::SourceColumn> {
        let source = self
            .model
            .sources
            .get(entity)
            .ok_or_else(|| SemanticError::UnknownEntity(entity.into()))?;

        source.columns.get(column).ok_or_else(|| {
            SemanticError::UnknownField {
                entity: entity.into(),
                field: column.into(),
            }
        })
    }

    /// Find the relationship between two entities.
    ///
    /// Returns the relationship from `from` to `to` if one exists.
    /// This checks both direct relationships and their reverses.
    pub fn find_relationship(
        &self,
        from: &str,
        to: &str,
    ) -> Option<&crate::model::Relationship> {
        // Check direct relationships
        self.model.relationships.iter().find(|&rel| rel.from_entity == from && rel.to_entity == to).map(|v| v as _)
    }

    /// Find the relationship between two entities, in either direction.
    ///
    /// Returns (relationship, is_reversed) where is_reversed is true if
    /// the relationship is from `to` to `from` rather than `from` to `to`.
    pub fn find_relationship_either_direction(
        &self,
        from: &str,
        to: &str,
    ) -> Option<(&crate::model::Relationship, bool)> {
        for rel in &self.model.relationships {
            if rel.from_entity == from && rel.to_entity == to {
                return Some((rel, false));
            }
            if rel.from_entity == to && rel.to_entity == from {
                return Some((rel, true));
            }
        }
        None
    }

    /// Find which fact defines a given measure.
    ///
    /// Returns (fact_name, measure_definition) if found.
    pub fn find_measure_entity(
        &self,
        measure_name: &str,
    ) -> Option<(&str, &crate::model::fact::MeasureDefinition)> {
        for (fact_name, fact) in &self.model.facts {
            if let Some(measure) = fact.measures.get(measure_name) {
                return Some((fact_name.as_str(), measure));
            }
        }
        None
    }

    /// List all measures across all facts.
    ///
    /// Returns a list of (fact_name, measure_name, measure_definition).
    pub fn list_measures(&self) -> Vec<(&str, &str, &crate::model::fact::MeasureDefinition)> {
        let mut measures = Vec::new();
        for (fact_name, fact) in &self.model.facts {
            for (measure_name, measure) in &fact.measures {
                measures.push((fact_name.as_str(), measure_name.as_str(), measure));
            }
        }
        measures
    }

    /// Resolve a field reference that could be either a column or a measure.
    ///
    /// This is useful for the query planner which needs to resolve user-provided
    /// field names that could refer to either type.
    ///
    /// Supports role-playing dimensions: if `entity` is a role alias (e.g., "order_date"),
    /// it resolves to the underlying dimension (e.g., "date") while preserving the
    /// role information for JOIN generation.
    ///
    /// Returns `AmbiguousDimensionRole` error if the entity is a dimension that has
    /// multiple role aliases and the user didn't use a role name to disambiguate.
    pub fn resolve_field(&self, entity: &str, field: &str) -> GraphResult<ModelResolvedField> {
        // First, check if entity is a role alias
        let (resolved_entity, using_role) = if let Some(alias) = self.role_aliases.get(entity) {
            // Role alias - use the target dimension
            (&alias.to_entity[..], true)
        } else {
            (entity, false)
        };

        // Check if it's a column on a source entity
        if let Some(source) = self.model.sources.get(resolved_entity) {
            if source.columns.contains_key(field) {
                // Check for ambiguous dimension reference (not using a role when one is required)
                if !using_role && self.is_dimension_ambiguous(resolved_entity) {
                    let available_roles: Vec<String> = self
                        .roles_for_dimension(resolved_entity)
                        .iter()
                        .map(|r| r.role_name.clone())
                        .collect();
                    return Err(SemanticError::AmbiguousDimensionRole {
                        dimension: resolved_entity.to_string(),
                        available_roles,
                    });
                }

                return Ok(ModelResolvedField::Column {
                    // Use original entity name (role name) so JOIN generation knows which FK to use
                    entity: entity.to_string(),
                    column: field.to_string(),
                });
            }
        }

        // Check if it's a measure on a fact
        if let Some(fact) = self.model.facts.get(resolved_entity) {
            if let Some(measure) = fact.measures.get(field) {
                return Ok(ModelResolvedField::Measure {
                    entity: entity.to_string(),
                    measure: field.to_string(),
                    aggregation: measure.aggregation,
                    source_column: measure.source_column.clone(),
                    filter: measure.filter.clone(),
                });
            }
        }

        // Check if it's a column on a dimension entity
        if let Some(dim) = self.model.dimensions.get(resolved_entity) {
            // Look for the field in dimension's columns
            // The field could match either the target_column (if renamed) or source_column
            let dim_col = dim.columns.iter().find(|col| {
                let target_name = col.target_column.as_ref().unwrap_or(&col.source_column);
                target_name == field || col.source_column == field
            });

            if dim_col.is_some() {
                return Ok(ModelResolvedField::Column {
                    entity: entity.to_string(),
                    column: field.to_string(),
                });
            }
        }

        // Check if entity exists at all (either as regular entity or role alias)
        if !self.has_entity(resolved_entity) && resolved_entity == entity {
            return Err(SemanticError::UnknownEntity(entity.into()));
        }

        // Entity exists but field wasn't found
        Err(SemanticError::UnknownField {
            entity: entity.into(),
            field: field.into(),
        })
    }

    /// Get the data type of a column in an entity.
    ///
    /// This is used for type-safe join validation and filter type checking.
    pub fn get_column_type(
        &self,
        entity: &str,
        column: &str,
    ) -> GraphResult<crate::model::DataType> {
        // Check sources
        if let Some(source) = self.model.sources.get(entity) {
            if let Some(col) = source.columns.get(column) {
                return Ok(col.data_type.clone());
            }
        }

        // Check dimensions - get type from the source entity
        if let Some(dim) = self.model.dimensions.get(entity) {
            // Find the column in dimension's column list
            let dim_col = dim.columns.iter().find(|col| {
                let target_name = col.target_column.as_ref().unwrap_or(&col.source_column);
                target_name == column || col.source_column == column
            });

            if let Some(dim_col) = dim_col {
                // Get the type from the source entity
                if let Some(source) = self.model.sources.get(&dim.source_entity) {
                    if let Some(src_col) = source.columns.get(&dim_col.source_column) {
                        return Ok(src_col.data_type.clone());
                    }
                }
            }
        }

        // Check if entity exists
        if !self.has_entity(entity) {
            return Err(SemanticError::UnknownEntity(entity.into()));
        }

        Err(SemanticError::UnknownField {
            entity: entity.into(),
            field: column.into(),
        })
    }

    /// Check if a column is nullable.
    ///
    /// Returns true if the column allows NULL values.
    pub fn is_column_nullable(&self, entity: &str, column: &str) -> GraphResult<bool> {
        // Check sources
        if let Some(source) = self.model.sources.get(entity) {
            if let Some(col) = source.columns.get(column) {
                return Ok(col.nullable);
            }
        }

        // Check dimensions - get nullability from the source entity
        if let Some(dim) = self.model.dimensions.get(entity) {
            // Find the column in dimension's column list
            let dim_col = dim.columns.iter().find(|col| {
                let target_name = col.target_column.as_ref().unwrap_or(&col.source_column);
                target_name == column || col.source_column == column
            });

            if let Some(dim_col) = dim_col {
                // Get nullability from the source entity
                if let Some(source) = self.model.sources.get(&dim.source_entity) {
                    if let Some(src_col) = source.columns.get(&dim_col.source_column) {
                        return Ok(src_col.nullable);
                    }
                }
            }
        }

        // Check if entity exists
        if !self.has_entity(entity) {
            return Err(SemanticError::UnknownEntity(entity.into()));
        }

        Err(SemanticError::UnknownField {
            entity: entity.into(),
            field: column.into(),
        })
    }

    /// Get the physical table name for an entity.
    ///
    /// Returns the qualified table name (schema.table or just table).
    pub fn get_physical_table(&self, entity: &str) -> GraphResult<String> {
        if let Some(source) = self.model.sources.get(entity) {
            return Ok(source.qualified_table_name());
        }

        if let Some(fact) = self.model.facts.get(entity) {
            let table = match &fact.target_schema {
                Some(schema) => format!("{}.{}", schema, fact.target_table),
                None => fact.target_table.clone(),
            };
            return Ok(table);
        }

        if let Some(dim) = self.model.dimensions.get(entity) {
            let table = match &dim.target_schema {
                Some(schema) => format!("{}.{}", schema, dim.target_table),
                None => dim.target_table.clone(),
            };
            return Ok(table);
        }

        Err(SemanticError::UnknownEntity(entity.into()))
    }
}
