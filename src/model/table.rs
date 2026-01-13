//! Table definitions for ETL transformations.
//!
//! Tables are the ETL layer - they transform data from sources or other tables.
//! They don't contain BI concepts (measures, includes) - those belong in Facts/Dimensions.

use serde::{Deserialize, Serialize};

use super::expr::{ColumnDef, Expr};
use super::source::DedupConfig;
use super::types::MaterializationStrategy;
use super::fact::WindowColumnDef;

/// A table definition for ETL transformations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Logical name for this table
    pub name: String,

    /// Table type (Staging, Mart, Table)
    pub table_type: TableTypeLabel,

    /// Physical target table (e.g., "analytics.stg_orders")
    pub target_table: Option<String>,

    /// Optional schema override
    pub target_schema: Option<String>,

    /// Whether this table is materialized as a physical table
    #[serde(default = "default_materialized")]
    pub materialized: bool,

    /// Source(s) - single entity or multiple for UNION
    pub from: FromClause,

    /// Union type when from has multiple sources
    #[serde(default)]
    pub union_type: UnionType,

    /// Joins to other entities
    #[serde(default)]
    pub joins: Vec<JoinDef>,

    /// Filter to apply to the source
    pub filter: Option<Expr>,

    /// Deduplication configuration
    pub dedup: Option<DedupConfig>,

    /// Column definitions (pass-through, renamed, computed)
    #[serde(default)]
    pub columns: Vec<ColumnDef>,

    /// Window column definitions
    #[serde(default)]
    pub window_columns: Vec<WindowColumnDef>,

    /// GROUP BY columns for pre-aggregation
    #[serde(default)]
    pub group_by: Vec<String>,

    /// Primary key columns
    #[serde(default)]
    pub primary_key: Vec<String>,

    /// Materialization strategy
    #[serde(default)]
    pub strategy: MaterializationStrategy,

    /// Tags for filtering builds
    #[serde(default)]
    pub tags: Vec<String>,

    /// Optional description
    pub description: Option<String>,
}

fn default_materialized() -> bool {
    true
}

/// Source clause - single source or multiple for UNION.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FromClause {
    Single(String),
    Multiple(Vec<String>),
}

impl FromClause {
    /// Get the primary source (first one if multiple).
    pub fn primary(&self) -> &str {
        match self {
            FromClause::Single(s) => s,
            FromClause::Multiple(v) => v.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }

    /// Check if this is a union of multiple sources.
    pub fn is_union(&self) -> bool {
        matches!(self, FromClause::Multiple(v) if v.len() > 1)
    }

    /// Get all sources.
    pub fn sources(&self) -> Vec<&str> {
        match self {
            FromClause::Single(s) => vec![s.as_str()],
            FromClause::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

/// Union type for multiple sources.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnionType {
    /// UNION (remove duplicates)
    #[default]
    Distinct,
    /// UNION ALL (keep all rows)
    All,
}

/// Table type label (for organization, not behavior).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TableTypeLabel {
    #[default]
    Staging,
    Mart,
    Table,
}

/// A join definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinDef {
    /// Entity to join to
    pub entity: String,
    /// Join type
    #[serde(default)]
    pub join_type: JoinType,
    /// Join condition
    pub on: Expr,
}

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JoinType {
    #[default]
    Left,
    Inner,
    Right,
    Full,
}

impl TableDefinition {
    /// Create a new table definition.
    pub fn new(name: impl Into<String>, from: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_type: TableTypeLabel::default(),
            target_table: None,
            target_schema: None,
            materialized: true,
            from: FromClause::Single(from.into()),
            union_type: UnionType::default(),
            joins: vec![],
            filter: None,
            dedup: None,
            columns: vec![],
            window_columns: vec![],
            group_by: vec![],
            primary_key: vec![],
            strategy: MaterializationStrategy::default(),
            tags: vec![],
            description: None,
        }
    }

    /// Set the table type.
    pub fn with_table_type(mut self, table_type: TableTypeLabel) -> Self {
        self.table_type = table_type;
        self
    }

    /// Set the target table.
    pub fn with_target_table(mut self, target: impl Into<String>) -> Self {
        self.target_table = Some(target.into());
        self
    }

    /// Add a filter.
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Add columns.
    pub fn with_columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.columns = columns;
        self
    }

    /// Add a tag.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add multiple tags.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Set primary key.
    pub fn with_primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = columns;
        self
    }

    /// Set the strategy.
    pub fn with_strategy(mut self, strategy: MaterializationStrategy) -> Self {
        self.strategy = strategy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_definition_new() {
        let table = TableDefinition::new("stg_orders", "raw_orders");
        assert_eq!(table.name, "stg_orders");
        assert_eq!(table.from.primary(), "raw_orders");
        assert!(!table.from.is_union());
    }

    #[test]
    fn test_from_clause_single() {
        let from = FromClause::Single("orders".into());
        assert_eq!(from.primary(), "orders");
        assert!(!from.is_union());
        assert_eq!(from.sources(), vec!["orders"]);
    }

    #[test]
    fn test_from_clause_multiple() {
        let from = FromClause::Multiple(vec!["orders_2023".into(), "orders_2024".into()]);
        assert_eq!(from.primary(), "orders_2023");
        assert!(from.is_union());
        assert_eq!(from.sources(), vec!["orders_2023", "orders_2024"]);
    }

    #[test]
    fn test_union_type_default() {
        assert_eq!(UnionType::default(), UnionType::Distinct);
    }
}
