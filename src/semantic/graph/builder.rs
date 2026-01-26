//! Graph construction logic for the unified semantic graph.
//!
//! This module implements the builder pattern for constructing UnifiedGraph
//! from a DSL Model, inferred relationships, and column statistics.

use std::collections::HashMap;

use crate::dsl::ast::{CalendarBody, Item, Model};
use crate::metadata::ColumnStats;
use crate::semantic::inference::InferredRelationship;

use super::{
    BelongsToEdge, CalendarNode, ColumnNode, DataType, DependsOnEdge, EntityNode, EntityType,
    GraphEdge, GraphNode, JoinsToEdge, MeasureNode, ReferencesEdge, RelationshipSource,
    SizeCategory, UnifiedGraph,
};

/// Convert inference RelationshipSource to graph RelationshipSource.
fn convert_relationship_source(
    source: crate::semantic::inference::RelationshipSource,
) -> RelationshipSource {
    match source {
        crate::semantic::inference::RelationshipSource::DatabaseConstraint => {
            RelationshipSource::ForeignKey
        }
        crate::semantic::inference::RelationshipSource::Inferred => RelationshipSource::Statistical,
        crate::semantic::inference::RelationshipSource::UserDefined => RelationshipSource::Explicit,
    }
}

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during graph construction.
#[derive(Debug, thiserror::Error)]
pub enum GraphBuildError {
    #[error("Duplicate entity name: {0}")]
    DuplicateEntity(String),

    #[error("Duplicate column: {0}")]
    DuplicateColumn(String),

    #[error("Duplicate measure: {0}")]
    DuplicateMeasure(String),

    #[error("Duplicate calendar: {0}")]
    DuplicateCalendar(String),

    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Invalid reference: {0}")]
    InvalidReference(String),
}

pub type GraphBuildResult<T> = Result<T, GraphBuildError>;

// ============================================================================
// Construction Entry Point
// ============================================================================

impl UnifiedGraph {
    /// Construct a UnifiedGraph from a DSL model with inference data.
    ///
    /// This is the primary entry point for building the unified graph.
    /// It takes:
    /// - `model`: The parsed DSL model containing tables, dimensions, measures, calendars
    /// - `relationships`: Inferred FK relationships from the inference engine
    /// - `stats`: Column statistics keyed by (table_name, column_name)
    ///
    /// Construction happens in two phases:
    /// - Phase 1: Create all nodes (entities, columns, measures, calendars)
    /// - Phase 2: Create all edges (references, joins, dependencies)
    pub fn from_model_with_inference(
        model: &Model,
        relationships: &[InferredRelationship],
        stats: &HashMap<(String, String), ColumnStats>,
    ) -> GraphBuildResult<Self> {
        let mut graph = UnifiedGraph::new();

        // Phase 1: Create all nodes
        graph.create_entity_nodes(model)?;
        graph.create_column_nodes(model, stats)?;
        graph.create_measure_nodes(model)?;
        graph.create_calendar_nodes(model)?;

        // Phase 2: Create edges (implemented in Task 3)
        graph.create_references_edges(relationships)?;
        graph.create_joins_to_edges(relationships)?;
        graph.create_depends_on_edges(model)?;

        Ok(graph)
    }
}

// ============================================================================
// Phase 1: Node Creation
// ============================================================================

impl UnifiedGraph {
    /// Create entity nodes from tables and dimensions.
    ///
    /// Iterates through all tables and dimensions in the model and creates
    /// an EntityNode for each. The entity type is determined by the item kind:
    /// - Tables become Fact entities (assumption: tables with atoms are facts)
    /// - Dimensions become Dimension entities
    pub(crate) fn create_entity_nodes(&mut self, model: &Model) -> GraphBuildResult<()> {
        for item in &model.items {
            match &item.value {
                Item::Table(table) => {
                    let entity_name = table.name.value.clone();

                    // Check for duplicates
                    if self.entity_index.contains_key(&entity_name) {
                        return Err(GraphBuildError::DuplicateEntity(entity_name));
                    }

                    let entity_node = EntityNode {
                        name: entity_name.clone(),
                        entity_type: EntityType::Fact, // Tables with atoms are facts
                        physical_name: Some(table.source.value.clone()),
                        schema: None,    // Will be enriched later if needed
                        row_count: None, // Will be enriched from stats
                        size_category: SizeCategory::Unknown,
                        metadata: HashMap::new(),
                    };

                    let node_idx = self.graph.add_node(GraphNode::Entity(entity_node));
                    self.entity_index.insert(entity_name.clone(), node_idx);
                    self.node_index.insert(entity_name, node_idx);
                }
                Item::Dimension(dimension) => {
                    let entity_name = dimension.name.value.clone();

                    // Check for duplicates
                    if self.entity_index.contains_key(&entity_name) {
                        return Err(GraphBuildError::DuplicateEntity(entity_name));
                    }

                    let entity_node = EntityNode {
                        name: entity_name.clone(),
                        entity_type: EntityType::Dimension,
                        physical_name: Some(dimension.source.value.clone()),
                        schema: None,
                        row_count: None,
                        size_category: SizeCategory::Unknown,
                        metadata: HashMap::new(),
                    };

                    let node_idx = self.graph.add_node(GraphNode::Entity(entity_node));
                    self.entity_index.insert(entity_name.clone(), node_idx);
                    self.node_index.insert(entity_name, node_idx);
                }
                _ => {} // Skip calendars, measures, reports
            }
        }

        Ok(())
    }

    /// Create column nodes from table atoms and dimension attributes.
    ///
    /// For each table, creates ColumnNode instances for all atoms (numeric columns).
    /// For each dimension, creates ColumnNode instances for the key and attributes.
    /// Also creates BELONGS_TO edges linking columns to their entities.
    /// Enriches entity nodes with row counts from statistics.
    pub(crate) fn create_column_nodes(
        &mut self,
        model: &Model,
        stats: &HashMap<(String, String), ColumnStats>,
    ) -> GraphBuildResult<()> {
        for item in &model.items {
            match &item.value {
                Item::Table(table) => {
                    let entity_name = &table.name.value;
                    let entity_idx = self
                        .entity_index
                        .get(entity_name)
                        .ok_or_else(|| GraphBuildError::EntityNotFound(entity_name.clone()))?;

                    // Create column nodes for atoms
                    for atom in &table.atoms {
                        let column_name = &atom.value.name.value;
                        let qualified_name = format!("{}.{}", entity_name, column_name);

                        // Check for duplicates
                        if self.column_index.contains_key(&qualified_name) {
                            return Err(GraphBuildError::DuplicateColumn(qualified_name));
                        }

                        // Map atom type to DataType
                        let data_type = match atom.value.atom_type.value {
                            crate::dsl::ast::AtomType::Int => DataType::Integer,
                            crate::dsl::ast::AtomType::Decimal => DataType::Float,
                            crate::dsl::ast::AtomType::Float => DataType::Float,
                        };

                        // Get column stats if available
                        let col_stats = stats.get(&(entity_name.clone(), column_name.clone()));
                        let unique = col_stats.map(|s| s.is_unique).unwrap_or(false);

                        // Enrich entity with row count from first column's stats
                        if let Some(stats) = col_stats {
                            if let Some(GraphNode::Entity(entity)) =
                                self.graph.node_weight_mut(*entity_idx)
                            {
                                if entity.row_count.is_none() {
                                    entity.row_count = Some(stats.total_count as usize);

                                    // Determine size category
                                    entity.size_category = match stats.total_count {
                                        0..=100_000 => SizeCategory::Small,
                                        100_001..=10_000_000 => SizeCategory::Medium,
                                        _ => SizeCategory::Large,
                                    };
                                }
                            }
                        }

                        let column_node = ColumnNode {
                            entity: entity_name.clone(),
                            name: column_name.clone(),
                            data_type,
                            nullable: true, // Conservative default
                            unique,
                            primary_key: false, // Atoms are not PKs
                            metadata: HashMap::new(),
                        };

                        let col_idx = self.graph.add_node(GraphNode::Column(column_node));
                        self.column_index.insert(qualified_name.clone(), col_idx);
                        self.node_index.insert(qualified_name.clone(), col_idx);

                        // Create BELONGS_TO edge: column -> entity
                        let edge = GraphEdge::BelongsTo(BelongsToEdge {
                            column: qualified_name,
                            entity: entity_name.clone(),
                        });
                        self.graph.add_edge(col_idx, *entity_idx, edge);
                    }
                }
                Item::Dimension(dimension) => {
                    let entity_name = &dimension.name.value;
                    let entity_idx = self
                        .entity_index
                        .get(entity_name)
                        .ok_or_else(|| GraphBuildError::EntityNotFound(entity_name.clone()))?;

                    // Create column node for primary key
                    let key_name = &dimension.key.value;
                    let key_qualified_name = format!("{}.{}", entity_name, key_name);

                    if !self.column_index.contains_key(&key_qualified_name) {
                        let key_node = ColumnNode {
                            entity: entity_name.clone(),
                            name: key_name.clone(),
                            data_type: DataType::Integer, // Assume integer keys
                            nullable: false,
                            unique: true,
                            primary_key: true,
                            metadata: HashMap::new(),
                        };

                        let key_idx = self.graph.add_node(GraphNode::Column(key_node));
                        self.column_index
                            .insert(key_qualified_name.clone(), key_idx);
                        self.node_index.insert(key_qualified_name.clone(), key_idx);

                        // Create BELONGS_TO edge
                        let edge = GraphEdge::BelongsTo(BelongsToEdge {
                            column: key_qualified_name,
                            entity: entity_name.clone(),
                        });
                        self.graph.add_edge(key_idx, *entity_idx, edge);
                    }

                    // Create column nodes for attributes
                    for attribute in &dimension.attributes {
                        let attr_name = &attribute.value.name.value;
                        let attr_qualified_name = format!("{}.{}", entity_name, attr_name);

                        if self.column_index.contains_key(&attr_qualified_name) {
                            return Err(GraphBuildError::DuplicateColumn(attr_qualified_name));
                        }

                        // Map DSL DataType to graph DataType
                        let data_type = match attribute.value.data_type.value {
                            crate::dsl::ast::DataType::String => DataType::String,
                            crate::dsl::ast::DataType::Int => DataType::Integer,
                            crate::dsl::ast::DataType::Decimal => DataType::Float,
                            crate::dsl::ast::DataType::Float => DataType::Float,
                            crate::dsl::ast::DataType::Bool => DataType::Boolean,
                            crate::dsl::ast::DataType::Date => DataType::Date,
                            crate::dsl::ast::DataType::Timestamp => DataType::Timestamp,
                        };

                        let attr_node = ColumnNode {
                            entity: entity_name.clone(),
                            name: attr_name.clone(),
                            data_type,
                            nullable: true,
                            unique: false,
                            primary_key: false,
                            metadata: HashMap::new(),
                        };

                        let attr_idx = self.graph.add_node(GraphNode::Column(attr_node));
                        self.column_index
                            .insert(attr_qualified_name.clone(), attr_idx);
                        self.node_index
                            .insert(attr_qualified_name.clone(), attr_idx);

                        // Create BELONGS_TO edge
                        let edge = GraphEdge::BelongsTo(BelongsToEdge {
                            column: attr_qualified_name,
                            entity: entity_name.clone(),
                        });
                        self.graph.add_edge(attr_idx, *entity_idx, edge);
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Create measure nodes from measure blocks.
    ///
    /// Iterates through all measure blocks in the model and creates a MeasureNode
    /// for each measure definition.
    pub(crate) fn create_measure_nodes(&mut self, model: &Model) -> GraphBuildResult<()> {
        for item in &model.items {
            if let Item::MeasureBlock(measure_block) = &item.value {
                let entity_name = &measure_block.table.value;

                // Verify the entity exists
                if !self.entity_index.contains_key(entity_name) {
                    return Err(GraphBuildError::EntityNotFound(entity_name.clone()));
                }

                for measure in &measure_block.measures {
                    let measure_name = &measure.value.name.value;
                    let qualified_name = format!("{}.{}", entity_name, measure_name);

                    // Check for duplicates
                    if self.measure_index.contains_key(&qualified_name) {
                        return Err(GraphBuildError::DuplicateMeasure(qualified_name));
                    }

                    let measure_node = MeasureNode {
                        name: measure_name.clone(),
                        entity: entity_name.clone(),
                        aggregation: "CUSTOM".to_string(), // Measures use SQL expressions
                        source_column: None, // Complex measures don't have single source
                        expression: Some(format!("{:?}", measure.value.expr.value)),
                        metadata: HashMap::new(),
                    };

                    let measure_idx = self.graph.add_node(GraphNode::Measure(measure_node));
                    self.measure_index
                        .insert(qualified_name.clone(), measure_idx);
                    self.node_index.insert(qualified_name, measure_idx);
                }
            }
        }

        Ok(())
    }

    /// Create calendar nodes from calendar definitions.
    ///
    /// Iterates through all calendar definitions in the model and creates
    /// a CalendarNode for each.
    pub(crate) fn create_calendar_nodes(&mut self, model: &Model) -> GraphBuildResult<()> {
        for item in &model.items {
            if let Item::Calendar(calendar) = &item.value {
                let calendar_name = &calendar.name.value;

                // Check for duplicates
                if self.calendar_index.contains_key(calendar_name) {
                    return Err(GraphBuildError::DuplicateCalendar(calendar_name.clone()));
                }

                let (physical_name, date_column, grain_levels) = match &calendar.body.value {
                    CalendarBody::Physical(physical) => {
                        let grain_levels = physical
                            .grain_mappings
                            .iter()
                            .map(|gm| gm.value.level.value.to_string())
                            .collect();

                        (
                            physical.source.value.clone(),
                            // Use first grain mapping column as date column
                            physical
                                .grain_mappings
                                .first()
                                .map(|gm| gm.value.column.value.clone())
                                .unwrap_or_else(|| "date".to_string()),
                            grain_levels,
                        )
                    }
                    CalendarBody::Generated(generated) => {
                        let grain_levels = generated
                            .base_grain
                            .value
                            .and_coarser()
                            .iter()
                            .map(|g| g.to_string())
                            .collect();

                        (
                            format!("generated_{}", calendar_name),
                            "date".to_string(),
                            grain_levels,
                        )
                    }
                };

                let calendar_node = CalendarNode {
                    name: calendar_name.clone(),
                    physical_name,
                    schema: None,
                    date_column,
                    grain_levels,
                    metadata: HashMap::new(),
                };

                let cal_idx = self.graph.add_node(GraphNode::Calendar(calendar_node));
                self.calendar_index.insert(calendar_name.clone(), cal_idx);
                self.node_index.insert(calendar_name.clone(), cal_idx);
            }
        }

        Ok(())
    }
}

// ============================================================================
// Phase 2: Edge Creation (Stubs for Task 3)
// ============================================================================

impl UnifiedGraph {
    /// Create REFERENCES edges from inferred relationships.
    ///
    /// Iterates through all inferred relationships and creates a REFERENCES edge
    /// for each column-to-column FK relationship.
    pub(crate) fn create_references_edges(
        &mut self,
        relationships: &[InferredRelationship],
    ) -> GraphBuildResult<()> {
        for rel in relationships {
            // Build qualified column names
            let from_col = format!("{}.{}", rel.from_table, rel.from_column);
            let to_col = format!("{}.{}", rel.to_table, rel.to_column);

            // Look up column nodes
            let from_idx = self
                .column_index
                .get(&from_col)
                .ok_or_else(|| GraphBuildError::ColumnNotFound(from_col.clone()))?;
            let to_idx = self
                .column_index
                .get(&to_col)
                .ok_or_else(|| GraphBuildError::ColumnNotFound(to_col.clone()))?;

            // Create REFERENCES edge with metadata
            let edge = GraphEdge::References(ReferencesEdge {
                from_column: from_col,
                to_column: to_col,
                source: convert_relationship_source(rel.source),
            });

            self.graph.add_edge(*from_idx, *to_idx, edge);
        }

        Ok(())
    }

    /// Create JOINS_TO edges from inferred relationships.
    ///
    /// Groups REFERENCES edges by entity pair and creates a JOINS_TO edge
    /// for each unique entity-to-entity relationship.
    pub(crate) fn create_joins_to_edges(
        &mut self,
        relationships: &[InferredRelationship],
    ) -> GraphBuildResult<()> {
        // Group relationships by entity pair
        let mut entity_joins: HashMap<(String, String), Vec<&InferredRelationship>> =
            HashMap::new();

        for rel in relationships {
            let key = (rel.from_table.clone(), rel.to_table.clone());
            entity_joins.entry(key).or_default().push(rel);
        }

        // Create JOINS_TO edges for each entity pair
        for ((from_entity, to_entity), rels) in entity_joins {
            // Look up entity nodes
            let from_idx = self
                .entity_index
                .get(&from_entity)
                .ok_or_else(|| GraphBuildError::EntityNotFound(from_entity.clone()))?;
            let to_idx = self
                .entity_index
                .get(&to_entity)
                .ok_or_else(|| GraphBuildError::EntityNotFound(to_entity.clone()))?;

            // Collect join columns
            let join_columns: Vec<(String, String)> = rels
                .iter()
                .map(|rel| (rel.from_column.clone(), rel.to_column.clone()))
                .collect();

            // Use the best (highest confidence) relationship's properties
            let best_rel = rels
                .iter()
                .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap())
                .unwrap();

            let edge = GraphEdge::JoinsTo(JoinsToEdge {
                from_entity,
                to_entity,
                join_columns,
                cardinality: best_rel.cardinality,
                source: convert_relationship_source(best_rel.source),
            });

            self.graph.add_edge(*from_idx, *to_idx, edge);
        }

        Ok(())
    }

    /// Create DEPENDS_ON edges from measure definitions.
    ///
    /// Extracts atom references from measure expressions and creates DEPENDS_ON edges
    /// from measures to the columns they reference.
    pub(crate) fn create_depends_on_edges(&mut self, model: &Model) -> GraphBuildResult<()> {
        for item in &model.items {
            if let Item::MeasureBlock(measure_block) = &item.value {
                let entity_name = &measure_block.table.value;

                for measure in &measure_block.measures {
                    let measure_name = &measure.value.name.value;
                    let qualified_measure = format!("{}.{}", entity_name, measure_name);

                    // Get measure node
                    let measure_idx =
                        self.measure_index.get(&qualified_measure).ok_or_else(|| {
                            GraphBuildError::InvalidReference(qualified_measure.clone())
                        })?;

                    // Extract atom references from the expression AST
                    let atom_refs = measure.value.expr.value.atom_refs();
                    let mut referenced_columns = Vec::new();

                    for atom_name in atom_refs {
                        let qualified_col = format!("{}.{}", entity_name, atom_name);

                        // Verify the column exists
                        if let Some(col_idx) = self.column_index.get(&qualified_col) {
                            referenced_columns.push(qualified_col.clone());

                            // Create DEPENDS_ON edge from measure to column
                            let edge = GraphEdge::DependsOn(DependsOnEdge {
                                measure: qualified_measure.clone(),
                                columns: vec![qualified_col],
                            });

                            self.graph.add_edge(*measure_idx, *col_idx, edge);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
