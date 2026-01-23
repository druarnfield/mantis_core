//! ModelGraph - Graph representation of a Model for path finding and dependency analysis.
//!
//! This is the core of the semantic layer, bridging the user-facing Model types
//! with graph algorithms needed by both Transform and Query planners.
//!
//! The module is organized into submodules:
//! - `path`: Path finding between entities (for JOIN generation)
//! - `dependencies`: Dependency analysis (for build ordering)
//! - `resolution`: Entity and field resolution
//! - `validation`: Model validation helpers
//! - `async_graph`: Async wrapper for metadata introspection

pub mod async_graph;
mod dependencies;
mod path;
mod resolution;
mod validation;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use petgraph::graph::{DiGraph, NodeIndex};

use crate::model::Model;
use crate::semantic::graph::Cardinality;

// Re-export error types from the unified error module
pub use super::error::{SemanticError, SemanticResult};

// Type aliases for backward compatibility within this module
pub type GraphError = SemanticError;
pub type GraphResult<T> = SemanticResult<T>;

/// Type of entity in the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityType {
    /// A source entity (raw table from upstream)
    Source,
    /// A fact table (materialized target)
    Fact,
    /// A dimension table (materialized target)
    Dimension,
    /// Custom entity type for extensions
    Custom(String),
}

impl EntityType {
    /// Create a custom entity type.
    pub fn custom(name: impl Into<String>) -> Self {
        Self::Custom(name.into())
    }

    /// Is this a materialized entity type (fact or dimension)?
    pub fn is_materialized(&self) -> bool {
        matches!(self, EntityType::Fact | EntityType::Dimension)
    }

    /// Is this a source entity?
    pub fn is_source(&self) -> bool {
        matches!(self, EntityType::Source)
    }

    /// Get the type name as a string.
    pub fn as_str(&self) -> &str {
        match self {
            EntityType::Source => "source",
            EntityType::Fact => "fact",
            EntityType::Dimension => "dimension",
            EntityType::Custom(name) => name,
        }
    }
}

impl std::fmt::Display for EntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A node in the entity graph.
#[derive(Debug, Clone)]
pub struct EntityNode {
    /// Name of the entity
    pub name: String,
    /// Type of entity
    pub entity_type: EntityType,
}

impl EntityNode {
    pub fn source(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            entity_type: EntityType::Source,
        }
    }

    pub fn fact(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            entity_type: EntityType::Fact,
        }
    }

    pub fn dimension(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            entity_type: EntityType::Dimension,
        }
    }
}

/// Edge data for relationships in the graph.
#[derive(Debug, Clone)]
pub struct EdgeData {
    /// Column in the source entity
    pub from_column: String,
    /// Column in the target entity
    pub to_column: String,
    /// Cardinality of the relationship
    pub cardinality: Cardinality,
}

/// An edge in a join path.
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub from_entity: String,
    pub to_entity: String,
    pub from_column: String,
    pub to_column: String,
    pub cardinality: Cardinality,
}

impl JoinEdge {
    /// Does this edge cause row multiplication (fan-out)?
    pub fn causes_fanout(&self) -> bool {
        self.cardinality.causes_fanout()
    }
}

/// A path through the graph (sequence of joins).
#[derive(Debug, Clone, Default)]
pub struct JoinPath {
    pub edges: Vec<JoinEdge>,
}

impl JoinPath {
    /// Create an empty path.
    pub fn new() -> Self {
        Self { edges: vec![] }
    }

    /// Does any edge in this path cause fan-out?
    pub fn causes_fanout(&self) -> bool {
        self.edges.iter().any(|e| e.causes_fanout())
    }

    /// Is this path safe (no fan-out)?
    pub fn is_safe(&self) -> bool {
        !self.causes_fanout()
    }

    /// Get all entities in this path (including start and end).
    pub fn entities(&self) -> Vec<&str> {
        let mut result: Vec<&str> = Vec::new();
        for edge in &self.edges {
            if result.last().copied() != Some(edge.from_entity.as_str()) {
                result.push(&edge.from_entity);
            }
            result.push(&edge.to_entity);
        }
        result
    }

    /// Number of hops in this path.
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Is this path empty (same source and destination)?
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

/// Entity information for query planning.
///
/// Contains the resolved physical table information for an entity,
/// regardless of whether it's a source, fact, or dimension.
#[derive(Debug, Clone)]
pub struct EntityInfo {
    /// Logical entity name
    pub name: String,
    /// Physical table name
    pub physical_table: String,
    /// Physical schema (if specified)
    pub physical_schema: Option<String>,
    /// Type of entity
    pub entity_type: EntityType,
    /// Whether this entity is materialized as a physical table.
    ///
    /// When `false`, the entity is "virtual" and must be reconstructed
    /// from source entities at query time.
    pub materialized: bool,
}

/// A resolved field reference (column or measure).
#[derive(Debug, Clone)]
pub enum ModelResolvedField {
    /// A column on an entity
    Column { entity: String, column: String },
    /// A measure on a fact
    Measure {
        entity: String,
        measure: String,
        aggregation: crate::model::AggregationType,
        source_column: String,
        /// Optional filter expression (for conditional aggregates)
        filter: Option<crate::model::expr::Expr>,
    },
}

/// Information about a role alias for role-playing dimensions.
///
/// When a relationship has a role (e.g., `link_as(orders.ship_date_id, date.date_id, "ship_date")`),
/// this stores the mapping so queries can use `ship_date.month` instead of `date.month`.
#[derive(Debug, Clone)]
pub struct RoleAlias {
    /// The role name (e.g., "ship_date")
    pub role_name: String,
    /// The entity that has the FK (e.g., "orders")
    pub from_entity: String,
    /// The FK column (e.g., "ship_date_id")
    pub from_column: String,
    /// The target entity (dimension) (e.g., "date")
    pub to_entity: String,
    /// The PK column on the target (e.g., "date_id")
    pub to_column: String,
}

impl RoleAlias {
    /// Create a new role alias from a relationship.
    pub fn from_relationship(rel: &crate::model::Relationship) -> Option<Self> {
        rel.role.as_ref().map(|role_name| Self {
            role_name: role_name.clone(),
            from_entity: rel.from_entity.clone(),
            from_column: rel.from_column.clone(),
            to_entity: rel.to_entity.clone(),
            to_column: rel.to_column.clone(),
        })
    }
}

/// Result of resolving an entity name that may be a role alias.
#[derive(Debug, Clone)]
pub struct ResolvedEntity<'a> {
    /// The resolved entity name (either the original or the target of a role alias)
    pub entity: &'a str,
    /// If this was a role alias, contains the alias information
    pub role_alias: Option<&'a RoleAlias>,
}

impl<'a> ResolvedEntity<'a> {
    /// Check if this resolution came from a role alias.
    pub fn is_role(&self) -> bool {
        self.role_alias.is_some()
    }

    /// Get the role name if this was a role alias.
    pub fn role_name(&self) -> Option<&str> {
        self.role_alias.map(|a| a.role_name.as_str())
    }

    /// Get the FK column if this was a role alias (for JOIN generation).
    pub fn fk_column(&self) -> Option<&str> {
        self.role_alias.map(|a| a.from_column.as_str())
    }
}

/// Graph representation of a Model.
///
/// Provides graph algorithms for:
/// - Path finding between entities (for JOIN generation)
/// - Dependency analysis (for build ordering)
/// - Impact analysis (for incremental builds)
#[derive(Debug, Clone)]
pub struct ModelGraph {
    /// The underlying model (source of truth)
    pub(crate) model: Model,

    /// Entity relationship graph for path finding
    /// Nodes are sources, edges are relationships (bidirectional)
    pub(crate) entity_graph: DiGraph<EntityNode, EdgeData>,

    /// Mapping from entity name to node index
    pub(crate) node_indices: HashMap<String, NodeIndex>,

    /// Mapping from role names to their alias information.
    ///
    /// Used for role-playing dimensions where a single dimension is referenced
    /// multiple times with different roles (e.g., order_date, ship_date).
    pub(crate) role_aliases: HashMap<String, RoleAlias>,
}

impl ModelGraph {
    /// Create a ModelGraph from a Model.
    ///
    /// This builds the internal graph structure from the model's
    /// sources, relationships, facts, and dimensions.
    pub fn from_model(model: Model) -> GraphResult<Self> {
        let mut entity_graph = DiGraph::new();
        let mut node_indices = HashMap::new();

        // Add all sources as nodes
        for name in model.sources.keys() {
            let idx = entity_graph.add_node(EntityNode::source(name));
            node_indices.insert(name.clone(), idx);
        }

        // Add all facts as nodes
        for name in model.facts.keys() {
            let idx = entity_graph.add_node(EntityNode::fact(name));
            node_indices.insert(name.clone(), idx);
        }

        // Add all dimensions as nodes
        for name in model.dimensions.keys() {
            let idx = entity_graph.add_node(EntityNode::dimension(name));
            node_indices.insert(name.clone(), idx);
        }

        // Add relationships as bidirectional edges
        // Track seen pairs to prevent duplicate edges
        let mut seen_pairs: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        // Also collect role aliases from relationships
        let mut role_aliases = HashMap::new();

        for rel in &model.relationships {
            // Extract role alias if present
            if let Some(alias) = RoleAlias::from_relationship(rel) {
                role_aliases.insert(alias.role_name.clone(), alias);
            }

            // Skip duplicate relationships
            let pair = (rel.from_entity.clone(), rel.to_entity.clone());
            if !seen_pairs.insert(pair) {
                continue;
            }

            let from_idx = node_indices.get(&rel.from_entity).ok_or_else(|| {
                GraphError::InvalidModel(format!(
                    "Relationship references unknown entity: '{}'",
                    rel.from_entity
                ))
            })?;

            let to_idx = node_indices.get(&rel.to_entity).ok_or_else(|| {
                GraphError::InvalidModel(format!(
                    "Relationship references unknown entity: '{}'",
                    rel.to_entity
                ))
            })?;

            // Forward edge
            entity_graph.add_edge(
                *from_idx,
                *to_idx,
                EdgeData {
                    from_column: rel.from_column.clone(),
                    to_column: rel.to_column.clone(),
                    cardinality: rel.cardinality,
                },
            );

            // Reverse edge (with reversed cardinality)
            entity_graph.add_edge(
                *to_idx,
                *from_idx,
                EdgeData {
                    from_column: rel.to_column.clone(),
                    to_column: rel.from_column.clone(),
                    cardinality: rel.cardinality.reverse(),
                },
            );
        }

        // Add implicit edges from facts to their related entities.
        // Facts need edges to:
        // 1. Their grain sources (the base entities they're built from)
        // 2. Their included entities (dimensions/sources they denormalize from)
        //
        // For included entities, we find the join columns by looking up relationships
        // from the grain sources to the included entities. The fact table is expected
        // to have the FK columns from its grain sources (for materialized facts).
        for (fact_name, fact) in &model.facts {
            let fact_idx = node_indices.get(fact_name).unwrap();

            // Collect unique source entities from grain with their PK columns
            let grain_entities: std::collections::HashSet<_> =
                fact.grain.iter().map(|g| g.source_entity.clone()).collect();

            // Get the primary grain column (for simple single-entity grain)
            let primary_grain_column = fact
                .grain
                .first()
                .map(|g| g.source_column.clone())
                .unwrap_or_default();

            // Add edges from fact to grain sources
            for grain_entity in &grain_entities {
                if let Some(grain_idx) = node_indices.get(grain_entity) {
                    let pair = (fact_name.clone(), grain_entity.clone());
                    if !seen_pairs.insert(pair) {
                        continue;
                    }

                    // The join column is the grain PK column (same on both sides)
                    let grain_col = fact
                        .grain
                        .iter()
                        .find(|g| &g.source_entity == grain_entity)
                        .map(|g| g.source_column.clone())
                        .unwrap_or_else(|| primary_grain_column.clone());

                    // Forward edge: fact -> grain source (one-to-one, same grain)
                    entity_graph.add_edge(
                        *fact_idx,
                        *grain_idx,
                        EdgeData {
                            from_column: grain_col.clone(),
                            to_column: grain_col.clone(),
                            cardinality: Cardinality::OneToOne,
                        },
                    );

                    // Reverse edge: grain source -> fact
                    entity_graph.add_edge(
                        *grain_idx,
                        *fact_idx,
                        EdgeData {
                            from_column: grain_col.clone(),
                            to_column: grain_col,
                            cardinality: Cardinality::OneToOne,
                        },
                    );
                }
            }

            // Add edges from fact to included entities
            // Find the join columns by looking up relationships from grain sources
            // or from other included entities (for multi-hop relationships)
            let include_entities: Vec<_> = fact.includes.keys().cloned().collect();

            for include_entity in fact.includes.keys() {
                if let Some(include_idx) = node_indices.get(include_entity) {
                    let pair = (fact_name.clone(), include_entity.clone());
                    if !seen_pairs.insert(pair) {
                        continue;
                    }

                    // Find the relationship to this included entity
                    // First, check relationships from grain sources
                    let mut join_columns = (String::new(), String::new());
                    'outer: for grain_entity in &grain_entities {
                        // Check if there's a relationship from grain to included entity
                        if let Some(rel) = model.relationships.iter().find(|r| {
                            r.from_entity == *grain_entity && &r.to_entity == include_entity
                        }) {
                            join_columns = (rel.from_column.clone(), rel.to_column.clone());
                            break 'outer;
                        }
                        // Also check reverse direction
                        if let Some(rel) = model.relationships.iter().find(|r| {
                            r.to_entity == *grain_entity && &r.from_entity == include_entity
                        }) {
                            join_columns = (rel.to_column.clone(), rel.from_column.clone());
                            break 'outer;
                        }
                    }

                    // If no direct relationship from grain, check relationships from other included entities
                    // This handles multi-hop relationships like: order_items -> orders -> dates
                    if join_columns.0.is_empty() {
                        'outer2: for other_include in &include_entities {
                            if other_include == include_entity {
                                continue;
                            }
                            // Check relationship from other include to this include
                            if let Some(rel) = model.relationships.iter().find(|r| {
                                &r.from_entity == other_include && &r.to_entity == include_entity
                            }) {
                                join_columns = (rel.from_column.clone(), rel.to_column.clone());
                                break 'outer2;
                            }
                            // Also check reverse direction
                            if let Some(rel) = model.relationships.iter().find(|r| {
                                &r.to_entity == other_include && &r.from_entity == include_entity
                            }) {
                                join_columns = (rel.to_column.clone(), rel.from_column.clone());
                                break 'outer2;
                            }
                        }
                    }

                    // Forward edge: fact -> included entity (many-to-one)
                    entity_graph.add_edge(
                        *fact_idx,
                        *include_idx,
                        EdgeData {
                            from_column: join_columns.0.clone(),
                            to_column: join_columns.1.clone(),
                            cardinality: Cardinality::ManyToOne,
                        },
                    );

                    // Reverse edge: included entity -> fact (one-to-many)
                    entity_graph.add_edge(
                        *include_idx,
                        *fact_idx,
                        EdgeData {
                            from_column: join_columns.1,
                            to_column: join_columns.0,
                            cardinality: Cardinality::OneToMany,
                        },
                    );
                }
            }

            // Also create edges from fact to dimensions whose source entity is included
            // This allows queries to reference dimension columns when the source is included
            for (dim_name, dim) in &model.dimensions {
                // Check if this dimension's source entity is in the fact's includes
                if fact.includes.contains_key(&dim.source_entity) {
                    if let Some(dim_idx) = node_indices.get(dim_name) {
                        let pair = (fact_name.clone(), dim_name.clone());
                        if !seen_pairs.insert(pair) {
                            continue;
                        }

                        // Find join columns from grain to the dimension's source entity
                        let mut join_columns = (String::new(), String::new());
                        'dim_outer: for grain_entity in &grain_entities {
                            if let Some(rel) = model.relationships.iter().find(|r| {
                                r.from_entity == *grain_entity && r.to_entity == dim.source_entity
                            }) {
                                join_columns = (rel.from_column.clone(), rel.to_column.clone());
                                break 'dim_outer;
                            }
                            if let Some(rel) = model.relationships.iter().find(|r| {
                                r.to_entity == *grain_entity && r.from_entity == dim.source_entity
                            }) {
                                join_columns = (rel.to_column.clone(), rel.from_column.clone());
                                break 'dim_outer;
                            }
                        }

                        // Forward edge: fact -> dimension (many-to-one)
                        entity_graph.add_edge(
                            *fact_idx,
                            *dim_idx,
                            EdgeData {
                                from_column: join_columns.0.clone(),
                                to_column: join_columns.1.clone(),
                                cardinality: Cardinality::ManyToOne,
                            },
                        );

                        // Reverse edge: dimension -> fact (one-to-many)
                        entity_graph.add_edge(
                            *dim_idx,
                            *fact_idx,
                            EdgeData {
                                from_column: join_columns.1,
                                to_column: join_columns.0,
                                cardinality: Cardinality::OneToMany,
                            },
                        );
                    }
                }
            }
        }

        Ok(Self {
            model,
            entity_graph,
            node_indices,
            role_aliases,
        })
    }

    /// Get a reference to the underlying model.
    pub fn model(&self) -> &Model {
        &self.model
    }

    /// Get the number of entities (sources + facts + dimensions).
    pub fn entity_count(&self) -> usize {
        self.node_indices.len()
    }

    /// Get the number of relationships.
    pub fn relationship_count(&self) -> usize {
        // Divide by 2 because we store bidirectional edges
        self.entity_graph.edge_count() / 2
    }

    /// Check if an entity exists in the graph.
    pub fn has_entity(&self, name: &str) -> bool {
        self.node_indices.contains_key(name)
    }

    /// Get the type of an entity.
    pub fn entity_type(&self, name: &str) -> Option<&EntityType> {
        self.node_indices
            .get(name)
            .map(|idx| &self.entity_graph[*idx].entity_type)
    }

    /// Get all entity names.
    pub fn entity_names(&self) -> Vec<&str> {
        self.node_indices.keys().map(|s| s.as_str()).collect()
    }

    /// Get all source names.
    pub fn source_names(&self) -> Vec<&str> {
        self.model.sources.keys().map(|s| s.as_str()).collect()
    }

    /// Get all fact names.
    pub fn fact_names(&self) -> Vec<&str> {
        self.model.facts.keys().map(|s| s.as_str()).collect()
    }

    /// Get all dimension names.
    pub fn dimension_names(&self) -> Vec<&str> {
        self.model.dimensions.keys().map(|s| s.as_str()).collect()
    }

    /// Get all target names (facts + dimensions).
    pub fn target_names(&self) -> Vec<&str> {
        self.model.target_names()
    }

    // =========================================================================
    // Role-Playing Dimension Support
    // =========================================================================

    /// Check if a name is a role alias.
    ///
    /// Role aliases are created when relationships have a `role` attribute,
    /// allowing disambiguation when a fact has multiple FKs to the same dimension.
    pub fn is_role_alias(&self, name: &str) -> bool {
        self.role_aliases.contains_key(name)
    }

    /// Get a role alias by name.
    pub fn get_role_alias(&self, name: &str) -> Option<&RoleAlias> {
        self.role_aliases.get(name)
    }

    /// Resolve a potentially role-aliased entity name to the actual entity.
    ///
    /// - If `name` is a role alias (e.g., "order_date"), returns the target entity (e.g., "date")
    /// - If `name` is a regular entity, returns `name` unchanged
    /// - If neither, returns None
    pub fn resolve_entity_name<'a>(&'a self, name: &str) -> Option<ResolvedEntity<'a>> {
        // First check if it's a role alias
        if let Some(alias) = self.role_aliases.get(name) {
            return Some(ResolvedEntity {
                entity: &alias.to_entity,
                role_alias: Some(alias),
            });
        }
        // Otherwise check if it's a regular entity
        if let Some((entity_name, _)) = self.node_indices.get_key_value(name) {
            return Some(ResolvedEntity {
                entity: entity_name,
                role_alias: None,
            });
        }
        None
    }

    /// Get all role alias names.
    pub fn role_alias_names(&self) -> Vec<&str> {
        self.role_aliases.keys().map(|s| s.as_str()).collect()
    }

    /// Get all roles that point to a specific dimension entity.
    ///
    /// Used to detect ambiguous dimension references - if there are multiple roles
    /// pointing to the same dimension, queries must use role names to disambiguate.
    pub fn roles_for_dimension(&self, dimension: &str) -> Vec<&RoleAlias> {
        self.role_aliases
            .values()
            .filter(|alias| alias.to_entity == dimension)
            .collect()
    }

    /// Check if a dimension is ambiguous (has multiple roles pointing to it).
    ///
    /// When true, queries referencing this dimension must use role names
    /// (e.g., `order_date.month` instead of `date.month`).
    pub fn is_dimension_ambiguous(&self, dimension: &str) -> bool {
        self.roles_for_dimension(dimension).len() > 1
    }

    // =========================================================================
    // Incremental Updates (for async introspection)
    // =========================================================================

    /// Add a source entity to the graph.
    ///
    /// Returns true if the source was added, false if it already existed.
    /// This is used during async introspection to add discovered tables.
    pub fn add_source(&mut self, source: crate::model::SourceEntity) -> bool {
        let name = source.name.clone();

        // Skip if already exists
        if self.node_indices.contains_key(&name) {
            return false;
        }

        // Add to model
        self.model.sources.insert(name.clone(), source);

        // Add node to graph
        let idx = self.entity_graph.add_node(EntityNode::source(&name));
        self.node_indices.insert(name, idx);

        true
    }

    /// Add a relationship to the graph.
    ///
    /// Returns Ok(true) if added, Ok(false) if it already exists,
    /// or Err if referenced entities don't exist.
    pub fn add_relationship(
        &mut self,
        relationship: crate::model::Relationship,
    ) -> GraphResult<bool> {
        // Check if entities exist
        let from_idx = self
            .node_indices
            .get(&relationship.from_entity)
            .ok_or_else(|| GraphError::UnknownEntity(relationship.from_entity.clone()))?;

        let to_idx = self
            .node_indices
            .get(&relationship.to_entity)
            .ok_or_else(|| GraphError::UnknownEntity(relationship.to_entity.clone()))?;

        // Check for duplicate relationship
        let exists = self.model.relationships.iter().any(|r| {
            r.from_entity == relationship.from_entity
                && r.to_entity == relationship.to_entity
                && r.from_column == relationship.from_column
                && r.to_column == relationship.to_column
        });

        if exists {
            return Ok(false);
        }

        // Add forward edge
        self.entity_graph.add_edge(
            *from_idx,
            *to_idx,
            EdgeData {
                from_column: relationship.from_column.clone(),
                to_column: relationship.to_column.clone(),
                cardinality: relationship.cardinality,
            },
        );

        // Add reverse edge
        self.entity_graph.add_edge(
            *to_idx,
            *from_idx,
            EdgeData {
                from_column: relationship.to_column.clone(),
                to_column: relationship.from_column.clone(),
                cardinality: relationship.cardinality.reverse(),
            },
        );

        // Add to model
        self.model.relationships.push(relationship);

        Ok(true)
    }

    /// Check if a relationship exists between two entities.
    pub fn has_relationship(&self, from: &str, to: &str) -> bool {
        self.model
            .relationships
            .iter()
            .any(|r| r.from_entity == from && r.to_entity == to)
    }

    /// Get a mutable reference to the underlying model.
    ///
    /// Use with care - modifications won't automatically update the graph.
    /// Prefer using `add_source` and `add_relationship` for incremental updates.
    pub fn model_mut(&mut self) -> &mut Model {
        &mut self.model
    }
}
