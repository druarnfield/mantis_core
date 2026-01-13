//! Unified model definitions for Mantis.
//!
//! The `Model` struct is the central definition that drives both:
//! - **Transform Planner**: Builds the warehouse (sources → facts/dimensions)
//! - **Query Planner**: Queries the warehouse (semantic queries → SQL)
//!
//! # Loading from Lua
//!
//! ```rust,ignore
//! use mantis::model::loader::load_model;
//! use std::path::Path;
//!
//! let model = load_model(Path::new("model.lua"))?;
//! ```
//!
//! # Programmatic Construction
//!
//! ```rust
//! use mantis::model::{Model, SourceEntity, FactDefinition, Relationship, Cardinality, DataType};
//!
//! let model = Model::new()
//!     .with_source(
//!         SourceEntity::new("orders", "raw.orders")
//!             .with_required_column("order_id", DataType::Int64)
//!             .with_required_column("customer_id", DataType::Int64)
//!             .with_primary_key(vec!["order_id"])
//!     )
//!     .with_source(
//!         SourceEntity::new("customers", "raw.customers")
//!             .with_required_column("customer_id", DataType::Int64)
//!             .with_required_column("name", DataType::String)
//!     )
//!     .with_relationship(Relationship::new(
//!         "orders", "customers",
//!         "customer_id", "customer_id",
//!         Cardinality::ManyToOne,
//!     ))
//!     .with_fact(
//!         FactDefinition::new("fact_orders", "analytics.fact_orders")
//!             .with_grain("orders", "order_id")
//!             .include("customers", vec!["name"])
//!     );
//! ```

pub mod dimension;
pub mod dimension_role;
pub mod emitter;
pub mod expr;
pub mod fact;
pub mod loader;
pub mod pivot_report;
pub mod query;
pub mod report;
pub mod source;
pub mod table;
pub mod types;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub use dimension::{DimensionColumn, DimensionDefinition, SCDType};
pub use dimension_role::{DateConfig, DimensionRole, GrainColumns, TimeGrain};
pub use expr::{
    BinaryOp, ColumnDef, Expr, FrameBound, FrameKind, Func, IntervalUnit, Literal, NullsOrder,
    OrderByExpr, SortDir, UnaryOp, WhenClause, WindowFrame, WindowFunc,
};
pub use fact::{
    ColumnSelection, DimensionInclude, FactDefinition, GrainColumn, MeasureDefinition,
    WindowColumnDef,
};
pub use pivot_report::{PivotColumns, PivotReport, PivotSort, PivotValue, SortDirection, TotalsConfig};
pub use query::{
    DerivedExpression, DerivedOp, QueryDefinition, QueryFilter, QueryFilterOp, QueryFilterValue,
    QueryOrderBy, QuerySelect, QueryTimeFunction,
};
pub use report::{MeasureRef, RefreshDelta, Report, ReportDefaults, ReportMaterialization, ReportTableType};
pub use source::{ChangeTracking, DedupConfig, DedupKeep, SourceColumn, SourceEntity};
pub use table::{FromClause, JoinDef, JoinType, TableDefinition, TableTypeLabel, UnionType};
pub use types::{AggregationType, DataType, MaterializationStrategy, TableType};

/// Cardinality of a relationship between entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cardinality {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
    /// Could not determine cardinality (used during inference).
    /// Excluded from serialization - should be resolved before persisting.
    #[serde(skip)]
    Unknown,
}

impl Cardinality {
    /// Does traversing this relationship cause row multiplication?
    pub fn causes_fanout(&self) -> bool {
        matches!(self, Cardinality::OneToMany | Cardinality::ManyToMany)
    }

    /// Get the reverse cardinality.
    pub fn reverse(&self) -> Self {
        match self {
            Cardinality::OneToOne => Cardinality::OneToOne,
            Cardinality::OneToMany => Cardinality::ManyToOne,
            Cardinality::ManyToOne => Cardinality::OneToMany,
            Cardinality::ManyToMany => Cardinality::ManyToMany,
            Cardinality::Unknown => Cardinality::Unknown,
        }
    }

    /// Is this cardinality known (not Unknown)?
    pub fn is_known(&self) -> bool {
        !matches!(self, Cardinality::Unknown)
    }

    /// Infer cardinality from uniqueness flags.
    ///
    /// # Arguments
    /// * `from_is_unique` - Whether the source (FK) column has unique values
    /// * `to_is_unique` - Whether the target (PK) column has unique values
    #[must_use]
    pub fn from_uniqueness(from_is_unique: bool, to_is_unique: bool) -> Self {
        match (from_is_unique, to_is_unique) {
            (true, true) => Cardinality::OneToOne,
            (false, true) => Cardinality::ManyToOne,
            (true, false) => Cardinality::OneToMany,
            (false, false) => Cardinality::Unknown,
        }
    }
}

impl std::fmt::Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cardinality::OneToOne => write!(f, "1:1"),
            Cardinality::OneToMany => write!(f, "1:N"),
            Cardinality::ManyToOne => write!(f, "N:1"),
            Cardinality::ManyToMany => write!(f, "M:N"),
            Cardinality::Unknown => write!(f, "?:?"),
        }
    }
}

/// Source of a relationship definition.
///
/// Tracks how a relationship was discovered/defined for debugging
/// and to allow different handling (e.g., inferred relationships
/// might have lower priority in path finding).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RelationshipSource {
    /// Explicitly defined in the Lua model file.
    Explicit,
    /// Discovered from a database foreign key constraint.
    ForeignKey,
    /// Inferred from naming conventions.
    Inferred {
        /// Which inference rule matched.
        rule: String,
        /// Confidence score (0.0 - 1.0).
        confidence: f64,
    },
}

impl Default for RelationshipSource {
    fn default() -> Self {
        Self::Explicit
    }
}

impl RelationshipSource {
    /// Create an inferred relationship source.
    pub fn inferred(rule: impl Into<String>, confidence: f64) -> Self {
        Self::Inferred {
            rule: rule.into(),
            confidence,
        }
    }

    /// Is this an explicitly defined relationship?
    pub fn is_explicit(&self) -> bool {
        matches!(self, Self::Explicit)
    }

    /// Is this from a foreign key constraint?
    pub fn is_foreign_key(&self) -> bool {
        matches!(self, Self::ForeignKey)
    }

    /// Is this an inferred relationship?
    pub fn is_inferred(&self) -> bool {
        matches!(self, Self::Inferred { .. })
    }

    /// Get the confidence score (1.0 for explicit/FK, actual score for inferred).
    pub fn confidence(&self) -> f64 {
        match self {
            Self::Explicit => 1.0,
            Self::ForeignKey => 1.0,
            Self::Inferred { confidence, .. } => *confidence,
        }
    }
}

/// A relationship between two entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// Source entity name
    pub from_entity: String,
    /// Target entity name
    pub to_entity: String,
    /// Join column in source entity
    pub from_column: String,
    /// Join column in target entity
    pub to_column: String,
    /// Cardinality of the relationship
    pub cardinality: Cardinality,
    /// How this relationship was discovered/defined.
    #[serde(default)]
    pub source: RelationshipSource,
    /// Optional role name for role-playing dimensions.
    ///
    /// When a fact has multiple foreign keys to the same dimension (e.g.,
    /// order_date, ship_date, delivery_date all pointing to a date dimension),
    /// the role name provides an alias for disambiguation in queries.
    ///
    /// Example: `link(orders.order_date_id, date.date_id):as("order_date")`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

impl Relationship {
    /// Create a new relationship (explicitly defined).
    pub fn new(
        from_entity: impl Into<String>,
        to_entity: impl Into<String>,
        from_column: impl Into<String>,
        to_column: impl Into<String>,
        cardinality: Cardinality,
    ) -> Self {
        Self {
            from_entity: from_entity.into(),
            to_entity: to_entity.into(),
            from_column: from_column.into(),
            to_column: to_column.into(),
            cardinality,
            source: RelationshipSource::Explicit,
            role: None,
        }
    }

    /// Create a relationship with a specific source.
    pub fn with_source(
        from_entity: impl Into<String>,
        to_entity: impl Into<String>,
        from_column: impl Into<String>,
        to_column: impl Into<String>,
        cardinality: Cardinality,
        source: RelationshipSource,
    ) -> Self {
        Self {
            from_entity: from_entity.into(),
            to_entity: to_entity.into(),
            from_column: from_column.into(),
            to_column: to_column.into(),
            cardinality,
            source,
            role: None,
        }
    }

    /// Set a role name for this relationship.
    ///
    /// Used for role-playing dimensions where the same dimension is
    /// referenced multiple times with different meanings.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Create a relationship from a foreign key constraint.
    pub fn from_foreign_key(
        from_entity: impl Into<String>,
        to_entity: impl Into<String>,
        from_column: impl Into<String>,
        to_column: impl Into<String>,
        cardinality: Cardinality,
    ) -> Self {
        Self::with_source(
            from_entity,
            to_entity,
            from_column,
            to_column,
            cardinality,
            RelationshipSource::ForeignKey,
        )
    }

    /// Create an inferred relationship.
    pub fn inferred(
        from_entity: impl Into<String>,
        to_entity: impl Into<String>,
        from_column: impl Into<String>,
        to_column: impl Into<String>,
        cardinality: Cardinality,
        rule: impl Into<String>,
        confidence: f64,
    ) -> Self {
        Self::with_source(
            from_entity,
            to_entity,
            from_column,
            to_column,
            cardinality,
            RelationshipSource::inferred(rule, confidence),
        )
    }

    /// Create the reverse of this relationship.
    pub fn reverse(&self) -> Self {
        Self {
            from_entity: self.to_entity.clone(),
            to_entity: self.from_entity.clone(),
            from_column: self.to_column.clone(),
            to_column: self.from_column.clone(),
            cardinality: self.cardinality.reverse(),
            source: self.source.clone(),
            role: self.role.clone(),
        }
    }

    /// Check if this relationship has a role assigned.
    pub fn has_role(&self) -> bool {
        self.role.is_some()
    }

    /// Get the role name if assigned.
    pub fn role_name(&self) -> Option<&str> {
        self.role.as_deref()
    }
}

/// The unified semantic model.
///
/// Contains all definitions needed for both transform and query planning:
/// - **Sources**: Raw data from upstream systems
/// - **Relationships**: How entities connect (used for JOINs)
/// - **Facts**: Denormalized fact tables to materialize
/// - **Dimensions**: Conformed dimension tables to materialize
/// - **Intermediates**: Ephemeral staging tables for transformations
/// - **Reports**: Multi-fact measure collections for dashboards
/// - **Pivot Reports**: Cross-tab/matrix reports
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Model {
    /// Source entities (raw tables)
    pub sources: HashMap<String, SourceEntity>,

    /// Relationships between entities
    pub relationships: Vec<Relationship>,

    /// Fact definitions (targets for transform planner)
    pub facts: HashMap<String, FactDefinition>,

    /// Dimension definitions (targets for transform planner)
    pub dimensions: HashMap<String, DimensionDefinition>,

    /// Table definitions (ETL layer)
    pub tables: HashMap<String, TableDefinition>,

    /// Report definitions (multi-fact measure collections)
    pub reports: HashMap<String, Report>,

    /// Pivot report definitions (cross-tab/matrix reports)
    pub pivot_reports: HashMap<String, PivotReport>,

    /// Query definitions (semantic queries)
    pub queries: HashMap<String, QueryDefinition>,
}

impl Model {
    /// Create a new empty model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a source entity.
    pub fn with_source(mut self, source: SourceEntity) -> Self {
        self.sources.insert(source.name.clone(), source);
        self
    }

    /// Add a source entity (mutable).
    pub fn add_source(&mut self, source: SourceEntity) {
        self.sources.insert(source.name.clone(), source);
    }

    /// Add a relationship.
    pub fn with_relationship(mut self, relationship: Relationship) -> Self {
        self.relationships.push(relationship);
        self
    }

    /// Add a relationship (mutable).
    pub fn add_relationship(&mut self, relationship: Relationship) {
        self.relationships.push(relationship);
    }

    /// Add a fact definition.
    pub fn with_fact(mut self, fact: FactDefinition) -> Self {
        self.facts.insert(fact.name.clone(), fact);
        self
    }

    /// Add a fact definition (mutable).
    pub fn add_fact(&mut self, fact: FactDefinition) {
        self.facts.insert(fact.name.clone(), fact);
    }

    /// Add a dimension definition.
    pub fn with_dimension(mut self, dimension: DimensionDefinition) -> Self {
        self.dimensions.insert(dimension.name.clone(), dimension);
        self
    }

    /// Add a dimension definition (mutable).
    pub fn add_dimension(&mut self, dimension: DimensionDefinition) {
        self.dimensions.insert(dimension.name.clone(), dimension);
    }

    /// Add a table definition.
    pub fn with_table(mut self, table: TableDefinition) -> Self {
        self.tables.insert(table.name.clone(), table);
        self
    }

    /// Add a table definition (mutable).
    pub fn add_table(&mut self, table: TableDefinition) {
        self.tables.insert(table.name.clone(), table);
    }

    /// Get a table by name.
    pub fn get_table(&self, name: &str) -> Option<&TableDefinition> {
        self.tables.get(name)
    }

    /// Get all tables with a specific tag.
    pub fn tables_with_tag(&self, tag: &str) -> Vec<&TableDefinition> {
        self.tables.values().filter(|t| t.tags.contains(&tag.to_string())).collect()
    }

    /// Add a report definition.
    pub fn with_report(mut self, report: Report) -> Self {
        self.reports.insert(report.name.clone(), report);
        self
    }

    /// Add a report definition (mutable).
    pub fn add_report(&mut self, report: Report) {
        self.reports.insert(report.name.clone(), report);
    }

    /// Add a pivot report definition.
    pub fn with_pivot_report(mut self, pivot_report: PivotReport) -> Self {
        self.pivot_reports.insert(pivot_report.name.clone(), pivot_report);
        self
    }

    /// Add a pivot report definition (mutable).
    pub fn add_pivot_report(&mut self, pivot_report: PivotReport) {
        self.pivot_reports.insert(pivot_report.name.clone(), pivot_report);
    }

    /// Add a query definition.
    pub fn with_query(mut self, query: QueryDefinition) -> Self {
        self.queries.insert(query.name.clone(), query);
        self
    }

    /// Add a query definition (mutable).
    pub fn add_query(&mut self, query: QueryDefinition) {
        self.queries.insert(query.name.clone(), query);
    }

    /// Get a source by name.
    pub fn get_source(&self, name: &str) -> Option<&SourceEntity> {
        self.sources.get(name)
    }

    /// Get a fact by name.
    pub fn get_fact(&self, name: &str) -> Option<&FactDefinition> {
        self.facts.get(name)
    }

    /// Get a dimension by name.
    pub fn get_dimension(&self, name: &str) -> Option<&DimensionDefinition> {
        self.dimensions.get(name)
    }

    /// Get a report by name.
    pub fn get_report(&self, name: &str) -> Option<&Report> {
        self.reports.get(name)
    }

    /// Get a pivot report by name.
    pub fn get_pivot_report(&self, name: &str) -> Option<&PivotReport> {
        self.pivot_reports.get(name)
    }

    /// Get a query by name.
    pub fn get_query(&self, name: &str) -> Option<&QueryDefinition> {
        self.queries.get(name)
    }

    /// Find a measure by name across all facts.
    ///
    /// Returns the fact name and measure definition if found.
    pub fn find_measure(&self, name: &str) -> Option<(&str, &MeasureDefinition)> {
        for (fact_name, fact) in &self.facts {
            if let Some(measure) = fact.measures.get(name) {
                return Some((fact_name.as_str(), measure));
            }
        }
        None
    }

    /// Find a measure in a specific fact.
    pub fn find_measure_in_fact(&self, fact_name: &str, measure_name: &str) -> Option<&MeasureDefinition> {
        self.facts.get(fact_name)?.measures.get(measure_name)
    }

    /// Check if the model has any targets (facts or dimensions).
    pub fn has_targets(&self) -> bool {
        !self.facts.is_empty() || !self.dimensions.is_empty()
    }

    /// Get all target names (facts and dimensions).
    pub fn target_names(&self) -> Vec<&str> {
        self.facts
            .keys()
            .chain(self.dimensions.keys())
            .map(|s| s.as_str())
            .collect()
    }

    /// Find relationships from a given entity.
    pub fn relationships_from(&self, entity: &str) -> Vec<&Relationship> {
        self.relationships
            .iter()
            .filter(|r| r.from_entity == entity)
            .collect()
    }

    /// Find relationships to a given entity.
    pub fn relationships_to(&self, entity: &str) -> Vec<&Relationship> {
        self.relationships
            .iter()
            .filter(|r| r.to_entity == entity)
            .collect()
    }

    /// Check if an entity exists (source, fact, dimension, or table).
    pub fn entity_exists(&self, name: &str) -> bool {
        self.sources.contains_key(name)
            || self.facts.contains_key(name)
            || self.tables.contains_key(name)
    }

    /// Check if an entity exists (source, fact, dimension, or table).
    ///
    /// This is used for query validation where dimensions are valid references.
    pub fn has_entity(&self, name: &str) -> bool {
        self.sources.contains_key(name)
            || self.facts.contains_key(name)
            || self.dimensions.contains_key(name)
            || self.tables.contains_key(name)
    }

    /// Check if an entity is a source (not a table or fact).
    pub fn is_source(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    /// Check if an entity is a fact.
    pub fn is_fact(&self, name: &str) -> bool {
        self.facts.contains_key(name)
    }

    /// Check if an entity is a table.
    pub fn is_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Validate the model for internal consistency.
    pub fn validate(&self) -> Result<(), ModelError> {
        // Check that all relationship entities exist (can be sources or intermediates)
        for rel in &self.relationships {
            if !self.entity_exists(&rel.from_entity) {
                return Err(ModelError::UnknownEntity {
                    name: rel.from_entity.clone(),
                    context: "relationship.from_entity".into(),
                });
            }
            if !self.entity_exists(&rel.to_entity) {
                return Err(ModelError::UnknownEntity {
                    name: rel.to_entity.clone(),
                    context: "relationship.to_entity".into(),
                });
            }
        }

        // Check that fact grain references exist (can be sources OR intermediates)
        for fact in self.facts.values() {
            for grain in &fact.grain {
                if !self.entity_exists(&grain.source_entity) {
                    return Err(ModelError::UnknownEntity {
                        name: grain.source_entity.clone(),
                        context: format!("fact '{}' grain", fact.name),
                    });
                }
            }
            // Check 'from' field if present (must be a valid entity)
            if let Some(ref from_entity) = fact.from {
                if !self.entity_exists(from_entity) {
                    return Err(ModelError::UnknownEntity {
                        name: from_entity.clone(),
                        context: format!("fact '{}' from", fact.name),
                    });
                }
            }
            // Check includes (can be sources OR intermediates for joining)
            for include in fact.includes.values() {
                if !self.entity_exists(&include.entity) {
                    return Err(ModelError::UnknownEntity {
                        name: include.entity.clone(),
                        context: format!("fact '{}' include", fact.name),
                    });
                }
            }
        }

        // Check that dimension sources exist (can be sources OR intermediates)
        for dim in self.dimensions.values() {
            if !self.entity_exists(&dim.source_entity) {
                return Err(ModelError::UnknownEntity {
                    name: dim.source_entity.clone(),
                    context: format!("dimension '{}' source_entity", dim.name),
                });
            }
        }

        // Check that table 'from' references exist
        for table in self.tables.values() {
            for source in table.from.sources() {
                if !self.entity_exists(source) {
                    return Err(ModelError::UnknownEntity {
                        name: source.to_string(),
                        context: format!("table '{}' from", table.name),
                    });
                }
            }
        }

        Ok(())
    }

    /// Compute a content hash of the model for cache invalidation.
    ///
    /// The hash is computed by serializing the model to a stable JSON format
    /// and hashing the result. This ensures that any change to the model
    /// definition will produce a different hash.
    ///
    /// # Example
    /// ```ignore
    /// let model = Model::new();
    /// let hash = model.content_hash();
    /// let cache_key = CacheKey::lineage(&hash);
    /// ```
    pub fn content_hash(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Serialize to JSON (canonical form)
        let json = serde_json::to_string(self).unwrap_or_default();

        // Hash the JSON
        let mut hasher = DefaultHasher::new();
        json.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

/// Errors that can occur during model validation.
#[derive(Debug, Clone, PartialEq)]
pub enum ModelError {
    /// Referenced entity does not exist in sources
    UnknownEntity { name: String, context: String },
    /// Referenced column does not exist
    UnknownColumn {
        entity: String,
        column: String,
        context: String,
    },
    /// Duplicate name
    DuplicateName { name: String, kind: String },
}

impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelError::UnknownEntity { name, context } => {
                write!(f, "Unknown entity '{}' in {}", name, context)
            }
            ModelError::UnknownColumn {
                entity,
                column,
                context,
            } => {
                write!(
                    f,
                    "Unknown column '{}.{}' in {}",
                    entity, column, context
                )
            }
            ModelError::DuplicateName { name, kind } => {
                write!(f, "Duplicate {} name: '{}'", kind, name)
            }
        }
    }
}

impl std::error::Error for ModelError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_model() -> Model {
        Model::new()
            .with_source(
                SourceEntity::new("orders", "raw.orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("total", DataType::Decimal(10, 2))
                    .with_primary_key(vec!["order_id"]),
            )
            .with_source(
                SourceEntity::new("customers", "raw.customers")
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
            .with_fact(
                FactDefinition::new("fact_orders", "analytics.fact_orders")
                    .with_grain("orders", "order_id")
                    .include("customers", vec!["name", "region"])
                    .with_sum("revenue", "total"),
            )
            .with_dimension(
                DimensionDefinition::new("dim_customers", "analytics.dim_customers", "customers")
                    .with_columns(vec!["customer_id", "name", "region"]),
            )
    }

    #[test]
    fn test_model_builder() {
        let model = sample_model();

        assert_eq!(model.sources.len(), 2);
        assert_eq!(model.relationships.len(), 1);
        assert_eq!(model.facts.len(), 1);
        assert_eq!(model.dimensions.len(), 1);
    }

    #[test]
    fn test_model_validate_success() {
        let model = sample_model();
        assert!(model.validate().is_ok());
    }

    #[test]
    fn test_model_validate_unknown_entity_in_relationship() {
        let model = Model::new()
            .with_source(SourceEntity::new("orders", "raw.orders"))
            .with_relationship(Relationship::new(
                "orders",
                "nonexistent",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ));

        let result = model.validate();
        assert!(matches!(result, Err(ModelError::UnknownEntity { .. })));
    }

    #[test]
    fn test_model_validate_unknown_entity_in_fact() {
        let model = Model::new()
            .with_source(SourceEntity::new("orders", "raw.orders"))
            .with_fact(
                FactDefinition::new("fact_orders", "analytics.fact_orders")
                    .with_grain("nonexistent", "order_id"),
            );

        let result = model.validate();
        assert!(matches!(result, Err(ModelError::UnknownEntity { .. })));
    }

    #[test]
    fn test_relationships_from() {
        let model = sample_model();

        let rels = model.relationships_from("orders");
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].to_entity, "customers");

        let rels = model.relationships_from("customers");
        assert_eq!(rels.len(), 0);
    }

    #[test]
    fn test_target_names() {
        let model = sample_model();

        let targets = model.target_names();
        assert!(targets.contains(&"fact_orders"));
        assert!(targets.contains(&"dim_customers"));
    }

    #[test]
    fn test_cardinality_fanout() {
        assert!(!Cardinality::OneToOne.causes_fanout());
        assert!(!Cardinality::ManyToOne.causes_fanout());
        assert!(Cardinality::OneToMany.causes_fanout());
        assert!(Cardinality::ManyToMany.causes_fanout());
    }

    #[test]
    fn test_cardinality_reverse() {
        assert_eq!(Cardinality::OneToMany.reverse(), Cardinality::ManyToOne);
        assert_eq!(Cardinality::ManyToOne.reverse(), Cardinality::OneToMany);
        assert_eq!(Cardinality::OneToOne.reverse(), Cardinality::OneToOne);
    }

    #[test]
    fn test_model_serialization() {
        let model = sample_model();
        let json = serde_json::to_string(&model).unwrap();
        let deserialized: Model = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.sources.len(), model.sources.len());
        assert_eq!(deserialized.facts.len(), model.facts.len());
    }
}
