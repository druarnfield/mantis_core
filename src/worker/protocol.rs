//! Protocol types for worker communication.
//!
//! These types mirror the Go worker protocol defined in:
//! - `worker/internal/protocol/request.go`
//! - `worker/internal/protocol/response.go`

use serde::{Deserialize, Serialize};

// ============================================================================
// Request/Response Envelope
// ============================================================================

/// Request envelope sent to the worker.
#[derive(Debug, Clone, Serialize)]
pub struct RequestEnvelope {
    /// Unique request ID for correlation.
    pub id: String,
    /// Method name (e.g., "metadata.list_schemas").
    pub method: String,
    /// Method-specific parameters.
    pub params: serde_json::Value,
}

/// Response envelope received from the worker.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseEnvelope {
    /// Request ID this response corresponds to.
    pub id: String,
    /// Whether the request succeeded.
    pub success: bool,
    /// Result data (present if success = true).
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    /// Error information (present if success = false).
    #[serde(default)]
    pub error: Option<ErrorInfo>,
}

/// Error information in a failed response.
#[derive(Debug, Clone, Deserialize)]
pub struct ErrorInfo {
    /// Error code.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

// ============================================================================
// Connection Parameters (included in all requests)
// ============================================================================

/// Database connection parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionParams {
    /// Database driver name (e.g., "duckdb", "mssql").
    pub driver: String,
    /// Driver-specific connection string.
    pub connection_string: String,
}

// ============================================================================
// Metadata Request Parameters
// ============================================================================

/// Parameters for `metadata.list_schemas`.
#[derive(Debug, Clone, Serialize)]
pub struct ListSchemasParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
}

/// Parameters for `metadata.list_tables`.
#[derive(Debug, Clone, Serialize)]
pub struct ListTablesParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema to list tables from (optional, uses default if empty).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// Parameters for `metadata.get_table`.
#[derive(Debug, Clone, Serialize)]
pub struct GetTableParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Parameters for `metadata.get_columns`.
#[derive(Debug, Clone, Serialize)]
pub struct GetColumnsParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Parameters for `metadata.get_primary_key`.
#[derive(Debug, Clone, Serialize)]
pub struct GetPrimaryKeyParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Parameters for `metadata.get_foreign_keys`.
#[derive(Debug, Clone, Serialize)]
pub struct GetForeignKeysParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Parameters for `metadata.get_unique_constraints`.
#[derive(Debug, Clone, Serialize)]
pub struct GetUniqueConstraintsParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Parameters for `metadata.get_indexes`.
#[derive(Debug, Clone, Serialize)]
pub struct GetIndexesParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Parameters for `metadata.get_row_count`.
#[derive(Debug, Clone, Serialize)]
pub struct GetRowCountParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Whether to use exact count (slower) or estimate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exact: Option<bool>,
}

/// Parameters for `metadata.sample_rows`.
#[derive(Debug, Clone, Serialize)]
pub struct SampleRowsParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Maximum number of rows to return (default: 10).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
}

/// Parameters for `metadata.get_database_info`.
#[derive(Debug, Clone, Serialize)]
pub struct GetDatabaseInfoParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
}

// ============================================================================
// Cardinality Discovery Parameters
// ============================================================================

/// Parameters for `metadata.get_column_stats`.
#[derive(Debug, Clone, Serialize)]
pub struct GetColumnStatsParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Column name.
    pub column: String,
    /// Number of sample values to return (default: 5).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_size: Option<i32>,
}

/// Parameters for `metadata.check_value_overlap`.
#[derive(Debug, Clone, Serialize)]
pub struct CheckValueOverlapParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// Schema of the left table.
    pub left_schema: String,
    /// Name of the left table.
    pub left_table: String,
    /// Column in the left table.
    pub left_column: String,
    /// Schema of the right table.
    pub right_schema: String,
    /// Name of the right table.
    pub right_table: String,
    /// Column in the right table.
    pub right_column: String,
    /// Number of values to check (default: 1000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_size: Option<i32>,
}

// ============================================================================
// Query Execution Parameters
// ============================================================================

/// Parameters for `query.execute`.
#[derive(Debug, Clone, Serialize)]
pub struct ExecuteQueryParams {
    #[serde(flatten)]
    pub connection: ConnectionParams,
    /// SQL query to execute.
    pub sql: String,
    /// Query parameters (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<serde_json::Value>>,
}

// ============================================================================
// Metadata Response Types
// ============================================================================

/// Schema information.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaInfo {
    /// Schema name.
    pub name: String,
    /// Whether this is the default schema.
    #[serde(default)]
    pub is_default: bool,
}

/// Response from `metadata.list_schemas`.
#[derive(Debug, Clone, Deserialize)]
pub struct ListSchemasResponse {
    pub schemas: Vec<SchemaInfo>,
}

/// Basic table information.
#[derive(Debug, Clone, Deserialize)]
pub struct TableInfo {
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table type ("TABLE", "VIEW", "MATERIALIZED_VIEW").
    #[serde(rename = "type")]
    pub table_type: String,
}

/// Response from `metadata.list_tables`.
#[derive(Debug, Clone, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableInfo>,
}

/// Column information.
#[derive(Debug, Clone, Deserialize)]
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
    #[serde(default)]
    pub max_length: Option<i32>,
    /// Numeric precision.
    #[serde(default)]
    pub numeric_precision: Option<i32>,
    /// Numeric scale.
    #[serde(default)]
    pub numeric_scale: Option<i32>,
    /// Default value expression.
    #[serde(default)]
    pub default_value: Option<String>,
    /// Whether this is an identity/auto-increment column.
    #[serde(default)]
    pub is_identity: bool,
    /// Whether this is a computed column.
    #[serde(default)]
    pub is_computed: bool,
}

/// Response from `metadata.get_columns`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetColumnsResponse {
    pub columns: Vec<ColumnInfo>,
}

/// Primary key information.
#[derive(Debug, Clone, Deserialize)]
pub struct PrimaryKeyInfo {
    /// Constraint name.
    pub name: String,
    /// Columns in the primary key (ordered).
    pub columns: Vec<String>,
}

/// Response from `metadata.get_primary_key`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetPrimaryKeyResponse {
    pub primary_key: Option<PrimaryKeyInfo>,
}

/// Foreign key information.
#[derive(Debug, Clone, Deserialize)]
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
    #[serde(default)]
    pub on_delete: Option<String>,
    /// ON UPDATE action.
    #[serde(default)]
    pub on_update: Option<String>,
}

/// Response from `metadata.get_foreign_keys`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetForeignKeysResponse {
    pub foreign_keys: Vec<ForeignKeyInfo>,
}

/// Unique constraint information.
#[derive(Debug, Clone, Deserialize)]
pub struct UniqueConstraintInfo {
    /// Constraint name.
    pub name: String,
    /// Columns in the unique constraint (ordered).
    pub columns: Vec<String>,
    /// Whether this is also the primary key.
    #[serde(default)]
    pub is_primary_key: bool,
}

/// Response from `metadata.get_unique_constraints`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetUniqueConstraintsResponse {
    pub unique_constraints: Vec<UniqueConstraintInfo>,
}

/// Index column information.
#[derive(Debug, Clone, Deserialize)]
pub struct IndexColumnInfo {
    /// Column name.
    pub name: String,
    /// Position in the index (1-based).
    pub position: i32,
    /// Whether sort order is descending.
    #[serde(default)]
    pub is_descending: bool,
    /// Whether this is an included (non-key) column.
    #[serde(default)]
    pub is_included: bool,
}

/// Index information.
#[derive(Debug, Clone, Deserialize)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Columns in the index (ordered).
    pub columns: Vec<IndexColumnInfo>,
    /// Whether the index enforces uniqueness.
    pub is_unique: bool,
    /// Whether this backs the primary key.
    #[serde(default)]
    pub is_primary_key: bool,
    /// Whether this is a clustered index.
    #[serde(default)]
    pub is_clustered: bool,
    /// Index type (BTREE, HASH, etc.).
    #[serde(default, rename = "type")]
    pub index_type: Option<String>,
}

/// Response from `metadata.get_indexes`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetIndexesResponse {
    pub indexes: Vec<IndexInfo>,
}

/// Detailed table information.
#[derive(Debug, Clone, Deserialize)]
pub struct TableDetailInfo {
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table type.
    #[serde(rename = "type")]
    pub table_type: String,
    /// Columns in the table.
    pub columns: Vec<ColumnInfo>,
    /// Primary key (if any).
    #[serde(default)]
    pub primary_key: Option<PrimaryKeyInfo>,
    /// Foreign keys.
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyInfo>,
    /// Unique constraints.
    #[serde(default)]
    pub unique_constraints: Vec<UniqueConstraintInfo>,
}

/// Response from `metadata.get_table`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetTableResponse {
    pub table: TableDetailInfo,
}

/// Response from `metadata.get_row_count`.
#[derive(Debug, Clone, Deserialize)]
pub struct RowCountResponse {
    /// Number of rows.
    pub row_count: i64,
    /// Whether this is an exact count or estimate.
    pub is_exact: bool,
}

/// Response from `metadata.sample_rows`.
#[derive(Debug, Clone, Deserialize)]
pub struct SampleRowsResponse {
    /// Column names in order.
    pub columns: Vec<String>,
    /// Sampled data rows.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned.
    pub row_count: i32,
}

/// Database information.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseInfo {
    /// Database product name.
    pub product_name: String,
    /// Database version.
    pub product_version: String,
    /// Current database name.
    pub database_name: String,
    /// Default schema.
    #[serde(default)]
    pub default_schema: Option<String>,
    /// Database collation.
    #[serde(default)]
    pub collation: Option<String>,
}

/// Response from `metadata.get_database_info`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetDatabaseInfoResponse {
    pub database: DatabaseInfo,
}

// ============================================================================
// Cardinality Discovery Response Types
// ============================================================================

/// Response from `metadata.get_column_stats`.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnStatsResponse {
    /// Total number of rows in the table.
    pub total_count: i64,
    /// Number of distinct values in the column.
    pub distinct_count: i64,
    /// Number of NULL values.
    pub null_count: i64,
    /// Whether all non-null values are unique.
    pub is_unique: bool,
    /// Sample distinct values from the column.
    #[serde(default)]
    pub sample_values: Vec<serde_json::Value>,
}

/// Response from `metadata.check_value_overlap`.
#[derive(Debug, Clone, Deserialize)]
pub struct ValueOverlapResponse {
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

impl ValueOverlapResponse {
    /// Determine if the overlap suggests a valid foreign key relationship.
    ///
    /// A valid FK suggestion requires:
    /// - High overlap (>= 90%)
    /// - Right is a superset (all left values exist in right)
    /// - Right is unique (can be referenced)
    pub fn suggests_foreign_key(&self) -> bool {
        self.overlap_percentage >= 90.0 && self.right_is_superset && self.right_is_unique
    }
}

// ============================================================================
// Query Execution Response Types
// ============================================================================

/// Column information in query results.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryResultColumn {
    /// Column name or alias.
    pub name: String,
    /// Database-specific type.
    pub data_type: String,
}

/// Response from `query.execute`.
#[derive(Debug, Clone, Deserialize)]
pub struct ExecuteQueryResponse {
    /// Result column descriptions.
    pub columns: Vec<QueryResultColumn>,
    /// Result data rows.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned.
    pub row_count: i32,
    /// Rows affected (for INSERT/UPDATE/DELETE).
    #[serde(default)]
    pub rows_affected: Option<i64>,
}

// ============================================================================
// Method Names
// ============================================================================

/// Worker method names.
pub mod methods {
    pub const LIST_SCHEMAS: &str = "metadata.list_schemas";
    pub const LIST_TABLES: &str = "metadata.list_tables";
    pub const GET_TABLE: &str = "metadata.get_table";
    pub const GET_COLUMNS: &str = "metadata.get_columns";
    pub const GET_PRIMARY_KEY: &str = "metadata.get_primary_key";
    pub const GET_FOREIGN_KEYS: &str = "metadata.get_foreign_keys";
    pub const GET_UNIQUE_CONSTRAINTS: &str = "metadata.get_unique_constraints";
    pub const GET_INDEXES: &str = "metadata.get_indexes";
    pub const GET_ROW_COUNT: &str = "metadata.get_row_count";
    pub const SAMPLE_ROWS: &str = "metadata.sample_rows";
    pub const GET_DATABASE_INFO: &str = "metadata.get_database_info";
    pub const GET_COLUMN_STATS: &str = "metadata.get_column_stats";
    pub const CHECK_VALUE_OVERLAP: &str = "metadata.check_value_overlap";
    pub const EXECUTE_QUERY: &str = "query.execute";
}
