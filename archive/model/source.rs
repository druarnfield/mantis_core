//! Source entity definitions - raw data from upstream systems.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::expr::{Expr, OrderByExpr};
use super::types::DataType;

/// A source entity representing raw data from an upstream system.
///
/// Sources are the input to the transform planner - they describe what
/// data exists in the source database and how to track changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceEntity {
    /// Logical name used in the model (e.g., "orders")
    pub name: String,

    /// Physical table name in the database (e.g., "raw.orders")
    pub table: String,

    /// Optional schema override (if not embedded in table name)
    pub schema: Option<String>,

    /// Column definitions with types
    pub columns: HashMap<String, SourceColumn>,

    /// Primary key columns
    pub primary_key: Vec<String>,

    /// How to track changes for incremental loads
    pub change_tracking: Option<ChangeTracking>,

    /// Filter to apply when reading from this source.
    ///
    /// Use for filtering out soft-deleted rows, test data, etc.
    /// Example: `deleted_at IS NULL AND environment = 'production'`
    pub filter: Option<Expr>,

    /// Deduplication configuration.
    ///
    /// When set, generates ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1
    /// to pick a single row per key.
    pub dedup: Option<DedupConfig>,
}

/// A column in a source entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceColumn {
    /// Column name in the source table
    pub name: String,

    /// Data type of the column
    pub data_type: DataType,

    /// Whether the column allows NULL values
    pub nullable: bool,

    /// Optional description for documentation
    pub description: Option<String>,
}

/// How changes are tracked in the source for incremental processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeTracking {
    /// Append-only source - new rows have increasing timestamp
    AppendOnly {
        /// Column containing the append timestamp
        timestamp_column: String,
    },

    /// CDC (Change Data Capture) with operation tracking
    CDC {
        /// Column indicating operation type (I/U/D)
        operation_column: String,
        /// Column containing the change timestamp
        timestamp_column: String,
    },

    /// Full snapshot - no change tracking, reload everything
    FullSnapshot,
}

/// Deduplication configuration for sources.
///
/// When applied, generates:
/// ```sql
/// SELECT * FROM (
///   SELECT *, ROW_NUMBER() OVER (
///     PARTITION BY <partition_by>
///     ORDER BY <order_by>
///   ) AS _rn
///   FROM source
/// ) WHERE _rn = 1
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupConfig {
    /// Columns to partition by (the key to deduplicate on)
    pub partition_by: Vec<String>,

    /// Columns to order by (determines which row to keep)
    pub order_by: Vec<OrderByExpr>,

    /// Which row to keep after ordering
    pub keep: DedupKeep,
}

/// Which row to keep when deduplicating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DedupKeep {
    /// Keep the first row (ROW_NUMBER() = 1)
    First,
    /// Keep the last row (reverse order, ROW_NUMBER() = 1)
    Last,
}

impl DedupConfig {
    /// Create a new dedup configuration.
    ///
    /// # Arguments
    /// * `partition_by` - Columns to deduplicate on (the key)
    /// * `order_by` - How to order rows to determine which to keep
    pub fn new(
        partition_by: Vec<impl Into<String>>,
        order_by: Vec<OrderByExpr>,
    ) -> Self {
        Self {
            partition_by: partition_by.into_iter().map(Into::into).collect(),
            order_by,
            keep: DedupKeep::First,
        }
    }

    /// Set which row to keep.
    pub fn with_keep(mut self, keep: DedupKeep) -> Self {
        self.keep = keep;
        self
    }

    /// Keep the first row (default).
    pub fn keep_first(mut self) -> Self {
        self.keep = DedupKeep::First;
        self
    }

    /// Keep the last row.
    pub fn keep_last(mut self) -> Self {
        self.keep = DedupKeep::Last;
        self
    }
}

impl SourceEntity {
    /// Create a new source entity.
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            schema: None,
            columns: HashMap::new(),
            primary_key: vec![],
            change_tracking: None,
            filter: None,
            dedup: None,
        }
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add a column.
    pub fn with_column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        let name = name.into();
        self.columns.insert(
            name.clone(),
            SourceColumn {
                name,
                data_type,
                nullable,
                description: None,
            },
        );
        self
    }

    /// Add a non-nullable column.
    pub fn with_required_column(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.with_column(name, data_type, false)
    }

    /// Add a nullable column.
    pub fn with_nullable_column(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.with_column(name, data_type, true)
    }

    /// Set the primary key.
    pub fn with_primary_key(mut self, columns: Vec<impl Into<String>>) -> Self {
        self.primary_key = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Set change tracking strategy.
    pub fn with_change_tracking(mut self, tracking: ChangeTracking) -> Self {
        self.change_tracking = Some(tracking);
        self
    }

    /// Set a filter expression to apply when reading from this source.
    ///
    /// Use for filtering out soft-deleted rows, test data, etc.
    /// Example: `deleted_at IS NULL AND environment = 'production'`
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set deduplication configuration.
    ///
    /// When set, generates ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1
    /// to pick a single row per key.
    pub fn with_dedup(mut self, dedup: DedupConfig) -> Self {
        self.dedup = Some(dedup);
        self
    }

    /// Get the fully qualified table name (schema.table or just table).
    pub fn qualified_table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.table),
            None => self.table.clone(),
        }
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&SourceColumn> {
        self.columns.get(name)
    }
}

impl SourceColumn {
    /// Create a new source column.
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            description: None,
        }
    }

    /// Add a description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_entity_builder() {
        let orders = SourceEntity::new("orders", "fact_orders")
            .with_schema("raw")
            .with_required_column("order_id", DataType::Int64)
            .with_required_column("customer_id", DataType::Int64)
            .with_required_column("order_date", DataType::Date)
            .with_nullable_column("total", DataType::Decimal(10, 2))
            .with_primary_key(vec!["order_id"])
            .with_change_tracking(ChangeTracking::AppendOnly {
                timestamp_column: "created_at".into(),
            });

        assert_eq!(orders.name, "orders");
        assert_eq!(orders.qualified_table_name(), "raw.fact_orders");
        assert_eq!(orders.columns.len(), 4);
        assert!(orders.has_column("order_id"));
        assert!(!orders.has_column("nonexistent"));

        let order_id = orders.get_column("order_id").unwrap();
        assert_eq!(order_id.data_type, DataType::Int64);
        assert!(!order_id.nullable);

        let total = orders.get_column("total").unwrap();
        assert!(total.nullable);
    }

    #[test]
    fn test_qualified_table_name_without_schema() {
        let source = SourceEntity::new("test", "my_table");
        assert_eq!(source.qualified_table_name(), "my_table");
    }

    #[test]
    fn test_change_tracking_variants() {
        let append = ChangeTracking::AppendOnly {
            timestamp_column: "ts".into(),
        };
        let cdc = ChangeTracking::CDC {
            operation_column: "op".into(),
            timestamp_column: "ts".into(),
        };
        let snapshot = ChangeTracking::FullSnapshot;

        // Just ensure they serialize correctly
        let _ = serde_json::to_string(&append).unwrap();
        let _ = serde_json::to_string(&cdc).unwrap();
        let _ = serde_json::to_string(&snapshot).unwrap();
    }

    #[test]
    fn test_source_with_filter() {
        use super::super::expr::{BinaryOp, Literal};

        // Filter: deleted_at IS NULL (represented as deleted_at = NULL check)
        let filter = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                entity: None,
                column: "is_deleted".into(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Bool(false))),
        };

        let source = SourceEntity::new("orders", "raw.orders").with_filter(filter.clone());

        assert!(source.filter.is_some());
        assert_eq!(source.filter.unwrap(), filter);
    }

    #[test]
    fn test_source_with_dedup() {
        use super::super::expr::SortDir;

        let dedup = DedupConfig::new(
            vec!["order_id"],
            vec![OrderByExpr {
                expr: Expr::Column {
                    entity: None,
                    column: "updated_at".into(),
                },
                dir: SortDir::Desc,
                nulls: None,
            }],
        )
        .keep_first();

        let source = SourceEntity::new("orders", "raw.orders").with_dedup(dedup);

        assert!(source.dedup.is_some());
        let dedup = source.dedup.unwrap();
        assert_eq!(dedup.partition_by, vec!["order_id"]);
        assert_eq!(dedup.keep, DedupKeep::First);
    }

    #[test]
    fn test_dedup_keep_last() {
        use super::super::expr::SortDir;

        let dedup = DedupConfig::new(
            vec!["customer_id", "product_id"],
            vec![OrderByExpr {
                expr: Expr::Column {
                    entity: None,
                    column: "event_time".into(),
                },
                dir: SortDir::Asc,
                nulls: None,
            }],
        )
        .keep_last();

        assert_eq!(dedup.partition_by, vec!["customer_id", "product_id"]);
        assert_eq!(dedup.keep, DedupKeep::Last);
    }

    #[test]
    fn test_dedup_serialization() {
        use super::super::expr::SortDir;

        let dedup = DedupConfig::new(
            vec!["id"],
            vec![OrderByExpr {
                expr: Expr::Column {
                    entity: None,
                    column: "ts".into(),
                },
                dir: SortDir::Desc,
                nulls: None,
            }],
        );

        let json = serde_json::to_string(&dedup).unwrap();
        let deserialized: DedupConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.partition_by, dedup.partition_by);
        assert_eq!(deserialized.keep, dedup.keep);
    }
}
