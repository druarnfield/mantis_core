//! Type definitions for the unified semantic graph.
//!
//! This module defines all node types, edge types, and supporting enums
//! for the unified graph architecture that replaces ModelGraph + ColumnLineageGraph.

use std::collections::HashMap;

// ============================================================================
// Supporting Enums
// ============================================================================

/// Size category for entities (small dimension vs large fact).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SizeCategory {
    /// Small table (typically dimensions, < 100K rows)
    Small,
    /// Medium table (100K-10M rows)
    Medium,
    /// Large table (> 10M rows, typically facts)
    Large,
    /// Size unknown (default)
    Unknown,
}

/// Type of entity in the semantic model.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityType {
    /// Source table (raw upstream data)
    Source,
    /// Fact table (materialized, grain table with measures)
    Fact,
    /// Dimension table (materialized, reference data)
    Dimension,
    /// Calendar table (time dimension)
    Calendar,
}

/// Cardinality of a relationship between entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cardinality {
    /// One-to-one relationship
    OneToOne,
    /// One-to-many relationship
    OneToMany,
    /// Many-to-one relationship
    ManyToOne,
    /// Many-to-many relationship
    ManyToMany,
    /// Unknown cardinality (inference needed)
    Unknown,
}

impl Cardinality {
    /// Reverse the cardinality (swap left/right sides).
    pub fn reverse(self) -> Self {
        match self {
            Cardinality::OneToMany => Cardinality::ManyToOne,
            Cardinality::ManyToOne => Cardinality::OneToMany,
            Cardinality::OneToOne => Cardinality::OneToOne,
            Cardinality::ManyToMany => Cardinality::ManyToMany,
            Cardinality::Unknown => Cardinality::Unknown,
        }
    }

    /// Determine cardinality from uniqueness constraints on both sides.
    pub fn from_uniqueness(left_unique: bool, right_unique: bool) -> Self {
        match (left_unique, right_unique) {
            (true, true) => Cardinality::OneToOne,
            (true, false) => Cardinality::OneToMany,
            (false, true) => Cardinality::ManyToOne,
            (false, false) => Cardinality::ManyToMany,
        }
    }

    /// Returns true if this cardinality can cause row multiplication.
    /// One-to-many and many-to-many can fan out.
    pub fn causes_fanout(&self) -> bool {
        matches!(self, Cardinality::OneToMany | Cardinality::ManyToMany)
    }

    /// Returns true if the cardinality is known (not Unknown).
    pub fn is_known(&self) -> bool {
        !matches!(self, Cardinality::Unknown)
    }
}

impl std::fmt::Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cardinality::OneToOne => write!(f, "1:1"),
            Cardinality::OneToMany => write!(f, "1:N"),
            Cardinality::ManyToOne => write!(f, "N:1"),
            Cardinality::ManyToMany => write!(f, "N:N"),
            Cardinality::Unknown => write!(f, "?:?"),
        }
    }
}

/// Data type of a column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// String/text type
    String,
    /// Integer type
    Integer,
    /// Floating point type
    Float,
    /// Boolean type
    Boolean,
    /// Date type
    Date,
    /// Timestamp type
    Timestamp,
    /// JSON type
    Json,
    /// Unknown type
    Unknown,
}

// ============================================================================
// Node Types
// ============================================================================

/// Entity node (table-level).
#[derive(Debug, Clone)]
pub struct EntityNode {
    /// Entity name (table name)
    pub name: String,
    /// Type of entity
    pub entity_type: EntityType,
    /// Physical table name (if different from logical name)
    pub physical_name: Option<String>,
    /// Schema name
    pub schema: Option<String>,
    /// Estimated row count
    pub row_count: Option<usize>,
    /// Size category
    pub size_category: SizeCategory,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

/// Column node (field-level).
#[derive(Debug, Clone)]
pub struct ColumnNode {
    /// Owning entity name
    pub entity: String,
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Is this column nullable?
    pub nullable: bool,
    /// Is this column unique?
    pub unique: bool,
    /// Is this column part of the primary key?
    pub primary_key: bool,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl ColumnNode {
    /// Get fully qualified name (entity.column).
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.entity, self.name)
    }
}

/// Measure node (pre-defined aggregation).
#[derive(Debug, Clone)]
pub struct MeasureNode {
    /// Measure name
    pub name: String,
    /// Source entity
    pub entity: String,
    /// Aggregation function (SUM, AVG, COUNT, etc.)
    pub aggregation: String,
    /// Source column (if applicable)
    pub source_column: Option<String>,
    /// SQL expression (for complex measures)
    pub expression: Option<String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

/// Calendar node (time dimension).
#[derive(Debug, Clone)]
pub struct CalendarNode {
    /// Calendar name
    pub name: String,
    /// Physical table name
    pub physical_name: String,
    /// Schema name
    pub schema: Option<String>,
    /// Date column name
    pub date_column: String,
    /// Grain levels available (day, week, month, etc.)
    pub grain_levels: Vec<String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

// ============================================================================
// Edge Types
// ============================================================================

/// BELONGS_TO edge: column → entity.
#[derive(Debug, Clone)]
pub struct BelongsToEdge {
    /// Column qualified name
    pub column: String,
    /// Entity name
    pub entity: String,
}

/// REFERENCES edge: column → column (FK relationship).
#[derive(Debug, Clone)]
pub struct ReferencesEdge {
    /// Source column (FK)
    pub from_column: String,
    /// Target column (PK)
    pub to_column: String,
    /// Source of this relationship
    pub source: RelationshipSource,
}

/// DERIVED_FROM edge: column → column(s) (lineage/transformation).
#[derive(Debug, Clone)]
pub struct DerivedFromEdge {
    /// Derived column
    pub target: String,
    /// Source columns
    pub sources: Vec<String>,
    /// Transformation expression
    pub expression: Option<String>,
}

/// DEPENDS_ON edge: measure → column(s).
#[derive(Debug, Clone)]
pub struct DependsOnEdge {
    /// Measure name
    pub measure: String,
    /// Columns this measure depends on
    pub columns: Vec<String>,
}

/// JOINS_TO edge: entity → entity (table-level join).
#[derive(Debug, Clone)]
pub struct JoinsToEdge {
    /// Source entity
    pub from_entity: String,
    /// Target entity
    pub to_entity: String,
    /// Join columns (pairs of from_col, to_col)
    pub join_columns: Vec<(String, String)>,
    /// Cardinality
    pub cardinality: Cardinality,
    /// Source of this relationship
    pub source: RelationshipSource,
}

/// Source of a relationship (for provenance tracking).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RelationshipSource {
    /// Explicitly declared in the model
    Explicit,
    /// Inferred from foreign key constraints
    ForeignKey,
    /// Inferred from naming conventions
    Convention,
    /// Inferred from data statistics
    Statistical,
}

// ============================================================================
// Unified Enums (Graph Storage)
// ============================================================================

/// Unified node type for graph storage.
#[derive(Debug, Clone)]
pub enum GraphNode {
    /// Entity node
    Entity(EntityNode),
    /// Column node
    Column(ColumnNode),
    /// Measure node
    Measure(MeasureNode),
    /// Calendar node
    Calendar(CalendarNode),
}

impl GraphNode {
    /// Get the node's name (identifier).
    pub fn name(&self) -> &str {
        match self {
            GraphNode::Entity(n) => &n.name,
            GraphNode::Column(n) => &n.name,
            GraphNode::Measure(n) => &n.name,
            GraphNode::Calendar(n) => &n.name,
        }
    }

    /// Get the node's qualified name (for columns, entity.column).
    pub fn qualified_name(&self) -> String {
        match self {
            GraphNode::Entity(n) => n.name.clone(),
            GraphNode::Column(n) => n.qualified_name(),
            GraphNode::Measure(n) => format!("{}.{}", n.entity, n.name),
            GraphNode::Calendar(n) => n.name.clone(),
        }
    }
}

/// Unified edge type for graph storage.
#[derive(Debug, Clone)]
pub enum GraphEdge {
    /// Column belongs to entity
    BelongsTo(BelongsToEdge),
    /// Column references column (FK)
    References(ReferencesEdge),
    /// Column derived from column(s)
    DerivedFrom(DerivedFromEdge),
    /// Measure depends on column(s)
    DependsOn(DependsOnEdge),
    /// Entity joins to entity
    JoinsTo(JoinsToEdge),
}

impl GraphEdge {
    /// Get the edge type as a string.
    pub fn edge_type(&self) -> &'static str {
        match self {
            GraphEdge::BelongsTo(_) => "BELONGS_TO",
            GraphEdge::References(_) => "REFERENCES",
            GraphEdge::DerivedFrom(_) => "DERIVED_FROM",
            GraphEdge::DependsOn(_) => "DEPENDS_ON",
            GraphEdge::JoinsTo(_) => "JOINS_TO",
        }
    }
}
