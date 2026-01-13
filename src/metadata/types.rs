//! Metadata types for the provider abstraction.
//!
//! These types are Rust-native representations of database metadata,
//! convertible to/from worker protocol types.

use serde::{Deserialize, Serialize};

use crate::model::{DataType, SourceColumn, SourceEntity};
use crate::semantic::inference::{ColumnInfo as InferenceColumnInfo, TableInfo as InferenceTableInfo};
use crate::worker::protocol;

/// Information about a database schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Schema name.
    pub name: String,
    /// Whether this is the default schema.
    pub is_default: bool,
}

impl From<protocol::SchemaInfo> for SchemaInfo {
    fn from(p: protocol::SchemaInfo) -> Self {
        Self {
            name: p.name,
            is_default: p.is_default,
        }
    }
}

/// Basic information about a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table type (TABLE, VIEW, MATERIALIZED_VIEW).
    pub table_type: TableType,
}

impl From<protocol::TableInfo> for TableInfo {
    fn from(p: protocol::TableInfo) -> Self {
        Self {
            schema: p.schema,
            name: p.name,
            table_type: TableType::from_str(&p.table_type),
        }
    }
}

/// Type of database table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableType {
    Table,
    View,
    MaterializedView,
    Unknown,
}

impl TableType {
    fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "TABLE" | "BASE TABLE" => Self::Table,
            "VIEW" => Self::View,
            "MATERIALIZED VIEW" | "MATERIALIZED_VIEW" => Self::MaterializedView,
            _ => Self::Unknown,
        }
    }
}

/// Information about a table column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Ordinal position (1-based).
    pub position: i32,
    /// Database-specific type name.
    pub data_type: String,
    /// Whether NULL values are allowed.
    pub is_nullable: bool,
    /// Maximum length for string types.
    pub max_length: Option<i32>,
    /// Numeric precision.
    pub numeric_precision: Option<i32>,
    /// Numeric scale.
    pub numeric_scale: Option<i32>,
    /// Default value expression.
    pub default_value: Option<String>,
    /// Whether this is an identity/auto-increment column.
    pub is_identity: bool,
    /// Whether this is a computed column.
    pub is_computed: bool,
}

impl From<protocol::ColumnInfo> for ColumnInfo {
    fn from(p: protocol::ColumnInfo) -> Self {
        Self {
            name: p.name,
            position: p.position,
            data_type: p.data_type,
            is_nullable: p.is_nullable,
            max_length: p.max_length,
            numeric_precision: p.numeric_precision,
            numeric_scale: p.numeric_scale,
            default_value: p.default_value,
            is_identity: p.is_identity,
            is_computed: p.is_computed,
        }
    }
}

/// Primary key constraint information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyInfo {
    /// Constraint name.
    pub name: String,
    /// Columns in the primary key (ordered).
    pub columns: Vec<String>,
}

impl From<protocol::PrimaryKeyInfo> for PrimaryKeyInfo {
    fn from(p: protocol::PrimaryKeyInfo) -> Self {
        Self {
            name: p.name,
            columns: p.columns,
        }
    }
}

/// Foreign key constraint information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    /// Constraint name.
    pub name: String,
    /// Columns in the foreign key (ordered).
    pub columns: Vec<String>,
    /// Schema of the referenced table.
    pub referenced_schema: String,
    /// Name of the referenced table.
    pub referenced_table: String,
    /// Columns in the referenced table (ordered).
    pub referenced_columns: Vec<String>,
    /// ON DELETE action.
    pub on_delete: Option<String>,
    /// ON UPDATE action.
    pub on_update: Option<String>,
}

impl From<protocol::ForeignKeyInfo> for ForeignKeyInfo {
    fn from(p: protocol::ForeignKeyInfo) -> Self {
        Self {
            name: p.name,
            columns: p.columns,
            referenced_schema: p.referenced_schema,
            referenced_table: p.referenced_table,
            referenced_columns: p.referenced_columns,
            on_delete: p.on_delete,
            on_update: p.on_update,
        }
    }
}

/// Unique constraint information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraintInfo {
    /// Constraint name.
    pub name: String,
    /// Columns in the unique constraint (ordered).
    pub columns: Vec<String>,
    /// Whether this is also the primary key.
    pub is_primary_key: bool,
}

impl From<protocol::UniqueConstraintInfo> for UniqueConstraintInfo {
    fn from(p: protocol::UniqueConstraintInfo) -> Self {
        Self {
            name: p.name,
            columns: p.columns,
            is_primary_key: p.is_primary_key,
        }
    }
}

/// Complete metadata for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table type.
    pub table_type: TableType,
    /// Columns in the table.
    pub columns: Vec<ColumnInfo>,
    /// Primary key (if any).
    pub primary_key: Option<PrimaryKeyInfo>,
    /// Foreign keys.
    pub foreign_keys: Vec<ForeignKeyInfo>,
    /// Unique constraints.
    pub unique_constraints: Vec<UniqueConstraintInfo>,
}

impl From<protocol::TableDetailInfo> for TableMetadata {
    fn from(p: protocol::TableDetailInfo) -> Self {
        Self {
            schema: p.schema,
            name: p.name,
            table_type: TableType::from_str(&p.table_type),
            columns: p.columns.into_iter().map(Into::into).collect(),
            primary_key: p.primary_key.map(Into::into),
            foreign_keys: p.foreign_keys.into_iter().map(Into::into).collect(),
            unique_constraints: p.unique_constraints.into_iter().map(Into::into).collect(),
        }
    }
}

impl TableMetadata {
    /// Get primary key column names.
    pub fn primary_key_columns(&self) -> Vec<&str> {
        self.primary_key
            .as_ref()
            .map(|pk| pk.columns.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Check if a column is part of the primary key.
    pub fn is_primary_key_column(&self, column: &str) -> bool {
        self.primary_key
            .as_ref()
            .map(|pk| pk.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
            .unwrap_or(false)
    }

    /// Check if a column is unique (PK or unique constraint).
    pub fn is_unique_column(&self, column: &str) -> bool {
        if self.is_primary_key_column(column) {
            return true;
        }
        self.unique_constraints.iter().any(|uc| {
            uc.columns.len() == 1 && uc.columns[0].eq_ignore_ascii_case(column)
        })
    }

    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
}

/// Convert TableMetadata to InferenceTableInfo for the inference engine.
impl From<&TableMetadata> for InferenceTableInfo {
    fn from(tm: &TableMetadata) -> Self {
        InferenceTableInfo {
            schema: tm.schema.clone(),
            name: tm.name.clone(),
            columns: tm
                .columns
                .iter()
                .map(|c| InferenceColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    is_nullable: c.is_nullable,
                    is_unique: if tm.is_unique_column(&c.name) {
                        Some(true)
                    } else {
                        None
                    },
                })
                .collect(),
            primary_key: tm.primary_key_columns().iter().map(|s| s.to_string()).collect(),
        }
    }
}

/// Convert TableMetadata to SourceEntity for the semantic model.
///
/// This allows introspected tables to be added to the ModelGraph.
impl From<TableMetadata> for SourceEntity {
    fn from(tm: TableMetadata) -> Self {
        Self::from(&tm)
    }
}

impl From<&TableMetadata> for SourceEntity {
    fn from(tm: &TableMetadata) -> Self {
        let columns = tm
            .columns
            .iter()
            .map(|c| {
                let data_type = parse_db_type(&c.data_type, c.numeric_precision, c.numeric_scale)
                    .unwrap_or(DataType::String);

                (
                    c.name.clone(),
                    SourceColumn {
                        name: c.name.clone(),
                        data_type,
                        nullable: c.is_nullable,
                        description: None,
                    },
                )
            })
            .collect();

        SourceEntity {
            name: tm.name.clone(),
            table: if tm.schema.is_empty() {
                tm.name.clone()
            } else {
                format!("{}.{}", tm.schema, tm.name)
            },
            schema: if tm.schema.is_empty() {
                None
            } else {
                Some(tm.schema.clone())
            },
            columns,
            primary_key: tm.primary_key_columns().iter().map(|s| s.to_string()).collect(),
            change_tracking: None,
            filter: None,
            dedup: None,
        }
    }
}

/// Parse a database type string to a DataType.
///
/// This handles various database-specific type names and maps them
/// to our internal DataType enum.
fn parse_db_type(
    type_name: &str,
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<DataType> {
    let type_lower = type_name.to_lowercase();

    // Handle parameterized types first
    if type_lower.contains("decimal") || type_lower.contains("numeric") {
        let p = precision.unwrap_or(18) as u8;
        let s = scale.unwrap_or(0) as u8;
        return Some(DataType::Decimal(p, s));
    }

    if type_lower.contains("varchar") || type_lower.contains("nvarchar") {
        let len = precision.map(|p| p as u16).unwrap_or(255);
        return Some(DataType::Varchar(len));
    }

    if type_lower.contains("char") && !type_lower.contains("var") {
        let len = precision.map(|p| p as u16).unwrap_or(1);
        return Some(DataType::Char(len));
    }

    // Simple type mappings
    match type_lower.as_str() {
        // Boolean
        "bool" | "boolean" | "bit" => Some(DataType::Bool),

        // Integers
        "tinyint" => Some(DataType::Int8),
        "smallint" | "int2" => Some(DataType::Int16),
        "int" | "integer" | "int4" => Some(DataType::Int32),
        "bigint" | "int8" => Some(DataType::Int64),

        // Floats
        "real" | "float4" | "float" => Some(DataType::Float32),
        "double" | "double precision" | "float8" => Some(DataType::Float64),

        // Strings
        "text" | "string" | "clob" | "ntext" => Some(DataType::String),

        // Date/Time
        "date" => Some(DataType::Date),
        "time" => Some(DataType::Time),
        "timestamp" | "datetime" | "datetime2" | "smalldatetime" => Some(DataType::Timestamp),
        "timestamptz" | "timestamp with time zone" | "datetimeoffset" => Some(DataType::TimestampTz),

        // Binary
        "binary" | "varbinary" | "blob" | "bytea" | "image" => Some(DataType::Binary),

        // JSON
        "json" | "jsonb" => Some(DataType::Json),

        // UUID
        "uuid" | "uniqueidentifier" => Some(DataType::Uuid),

        // Unknown - try the DataType parser as fallback
        _ => DataType::parse(&type_lower),
    }
}

/// Column statistics for cardinality analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Total number of rows in the table.
    pub total_count: i64,
    /// Number of distinct values in the column.
    pub distinct_count: i64,
    /// Number of NULL values.
    pub null_count: i64,
    /// Whether all non-null values are unique.
    pub is_unique: bool,
    /// Sample distinct values from the column.
    pub sample_values: Vec<serde_json::Value>,
}

impl From<protocol::ColumnStatsResponse> for ColumnStats {
    fn from(p: protocol::ColumnStatsResponse) -> Self {
        Self {
            total_count: p.total_count,
            distinct_count: p.distinct_count,
            null_count: p.null_count,
            is_unique: p.is_unique,
            sample_values: p.sample_values,
        }
    }
}

impl ColumnStats {
    /// Calculate the null percentage.
    pub fn null_percentage(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            (self.null_count as f64 / self.total_count as f64) * 100.0
        }
    }

    /// Calculate the distinct percentage (cardinality ratio).
    pub fn distinct_percentage(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            (self.distinct_count as f64 / self.total_count as f64) * 100.0
        }
    }
}

/// Value overlap analysis between two columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueOverlap {
    /// Number of distinct values sampled from left.
    pub left_sample_size: i64,
    /// Total distinct values in left column.
    pub left_total_distinct: i64,
    /// Total distinct values in right column.
    pub right_total_distinct: i64,
    /// How many sampled left values exist in right.
    pub overlap_count: i64,
    /// Overlap percentage (overlap_count / left_sample_size * 100).
    pub overlap_percentage: f64,
    /// Whether all sampled left values exist in right.
    pub right_is_superset: bool,
    /// Whether left column has unique values.
    pub left_is_unique: bool,
    /// Whether right column has unique values.
    pub right_is_unique: bool,
}

impl From<protocol::ValueOverlapResponse> for ValueOverlap {
    fn from(p: protocol::ValueOverlapResponse) -> Self {
        Self {
            left_sample_size: p.left_sample_size,
            left_total_distinct: p.left_total_distinct,
            right_total_distinct: p.right_total_distinct,
            overlap_count: p.overlap_count,
            overlap_percentage: p.overlap_percentage,
            right_is_superset: p.right_is_superset,
            left_is_unique: p.left_is_unique,
            right_is_unique: p.right_is_unique,
        }
    }
}

impl ValueOverlap {
    /// Check if this looks like a valid foreign key relationship.
    ///
    /// Returns true if:
    /// - High overlap (>= 90%)
    /// - Right side is a superset of sampled left values
    /// - Right side appears to be a lookup table (unique values)
    pub fn suggests_foreign_key(&self) -> bool {
        self.overlap_percentage >= 90.0 && self.right_is_superset && self.right_is_unique
    }

    /// Estimate the cardinality of a join between these columns.
    ///
    /// Returns a factor to multiply the left row count by.
    /// A factor < 1.0 means rows will be filtered, > 1.0 means row expansion.
    pub fn estimated_join_factor(&self) -> f64 {
        // Edge case: no data on right side means join produces no results
        if self.right_total_distinct == 0 {
            return 0.0;
        }

        if self.right_is_unique {
            // Many-to-one: each left row matches at most one right row
            self.overlap_percentage / 100.0
        } else if self.left_is_unique {
            // One-to-many: each left row could match multiple right rows
            // Estimate based on right side density
            let right_density = 1.0 / (self.right_total_distinct as f64);
            self.overlap_percentage / 100.0 * (1.0 / right_density).min(10.0)
        } else {
            // Many-to-many: could explode
            // Conservative estimate
            (self.overlap_percentage / 100.0) * 2.0
        }
    }
}

/// Database information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    /// Database product name.
    pub product_name: String,
    /// Database version.
    pub product_version: String,
    /// Current database name.
    pub database_name: String,
    /// Default schema.
    pub default_schema: Option<String>,
    /// Database collation.
    pub collation: Option<String>,
}

impl From<protocol::DatabaseInfo> for DatabaseInfo {
    fn from(p: protocol::DatabaseInfo) -> Self {
        Self {
            product_name: p.product_name,
            product_version: p.product_version,
            database_name: p.database_name,
            default_schema: p.default_schema,
            collation: p.collation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_type_from_str() {
        assert_eq!(TableType::from_str("TABLE"), TableType::Table);
        assert_eq!(TableType::from_str("BASE TABLE"), TableType::Table);
        assert_eq!(TableType::from_str("VIEW"), TableType::View);
        assert_eq!(TableType::from_str("MATERIALIZED VIEW"), TableType::MaterializedView);
        assert_eq!(TableType::from_str("unknown"), TableType::Unknown);
    }

    #[test]
    fn test_column_stats_percentages() {
        let stats = ColumnStats {
            total_count: 1000,
            distinct_count: 100,
            null_count: 50,
            is_unique: false,
            sample_values: vec![],
        };

        assert!((stats.null_percentage() - 5.0).abs() < 0.01);
        assert!((stats.distinct_percentage() - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_value_overlap_suggests_fk() {
        let overlap = ValueOverlap {
            left_sample_size: 100,
            left_total_distinct: 100,
            right_total_distinct: 50,
            overlap_count: 95,
            overlap_percentage: 95.0,
            right_is_superset: true,
            left_is_unique: false,
            right_is_unique: true,
        };

        assert!(overlap.suggests_foreign_key());
    }

    #[test]
    fn test_table_metadata_primary_key() {
        let metadata = TableMetadata {
            schema: "main".to_string(),
            name: "orders".to_string(),
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
                    name: "customer_id".to_string(),
                    position: 2,
                    data_type: "INTEGER".to_string(),
                    is_nullable: false,
                    max_length: None,
                    numeric_precision: None,
                    numeric_scale: None,
                    default_value: None,
                    is_identity: false,
                    is_computed: false,
                },
            ],
            primary_key: Some(PrimaryKeyInfo {
                name: "pk_orders".to_string(),
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![],
            unique_constraints: vec![],
        };

        assert!(metadata.is_primary_key_column("id"));
        assert!(metadata.is_primary_key_column("ID")); // Case insensitive
        assert!(!metadata.is_primary_key_column("customer_id"));
        assert!(metadata.is_unique_column("id"));
        assert!(!metadata.is_unique_column("customer_id"));
    }
}
