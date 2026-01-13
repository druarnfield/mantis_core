//! Database introspection API handlers.
//!
//! Provides endpoints for database connection management and schema introspection.
//! Limited to MSSQL and DuckDB drivers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cache::{MetadataCache, SavedCredential};
use crate::config::Settings;
use crate::crypto;
use crate::metadata::{
    MetadataProvider, SchemaInfo, TableInfo, TableMetadata, WorkerMetadataProvider,
};
use crate::worker::WorkerClient;

/// Supported database drivers for introspection.
const SUPPORTED_DRIVERS: &[&str] = &["mssql", "duckdb"];

// ============================================================================
// Shared State
// ============================================================================

/// Active database connection state.
#[derive(Default)]
pub struct DatabaseConnection {
    /// The active connection configuration.
    pub config: Option<ActiveConnection>,
}

/// Configuration for an active database connection.
pub struct ActiveConnection {
    /// Database driver name.
    pub driver: String,
    /// Connection string.
    pub connection_string: String,
    /// Worker client for database operations.
    pub client: Arc<WorkerClient>,
    /// Default schema for this connection.
    pub default_schema: Option<String>,
}

/// Shared database connection state wrapped in RwLock for thread-safe access.
pub type SharedConnection = Arc<RwLock<DatabaseConnection>>;

/// Create a new shared connection state.
pub fn new_shared_connection() -> SharedConnection {
    Arc::new(RwLock::new(DatabaseConnection::default()))
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to test a database connection.
#[derive(Debug, Deserialize)]
pub struct TestConnectionRequest {
    /// Database driver: "mssql" or "duckdb".
    pub driver: String,
    /// Connection string.
    pub connection_string: String,
}

/// Response from connection test.
#[derive(Debug, Serialize)]
pub struct TestConnectionResponse {
    /// Whether the connection was successful.
    pub success: bool,
    /// Error message if connection failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Database information if connection succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_info: Option<DatabaseInfoResponse>,
    /// Available schemas if connection succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<SchemaInfo>>,
}

/// Database information returned from connection.
#[derive(Debug, Serialize)]
pub struct DatabaseInfoResponse {
    /// Database product name.
    pub product_name: String,
    /// Database version.
    pub product_version: String,
    /// Current database name.
    pub database_name: String,
    /// Default schema.
    pub default_schema: Option<String>,
}

/// Request to set/update the active connection.
#[derive(Debug, Deserialize)]
pub struct SetConnectionRequest {
    /// Database driver: "mssql" or "duckdb".
    pub driver: String,
    /// Connection string.
    pub connection_string: String,
    /// Default schema (optional).
    pub default_schema: Option<String>,
}

/// Response for connection status.
#[derive(Debug, Serialize)]
pub struct ConnectionStatusResponse {
    /// Whether a connection is active.
    pub connected: bool,
    /// Current driver if connected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>,
    /// Supported drivers for introspection.
    pub supported_drivers: Vec<String>,
    /// Default schema if set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_schema: Option<String>,
}

/// Request to list tables in a schema.
#[derive(Debug, Deserialize)]
pub struct ListTablesRequest {
    /// Schema name (required).
    pub schema: String,
}

/// Request to get table details.
#[derive(Debug, Deserialize)]
pub struct GetTableRequest {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// Response for table details.
#[derive(Debug, Serialize)]
pub struct TableDetailResponse {
    /// Schema the table belongs to.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table type.
    pub table_type: String,
    /// Columns in the table.
    pub columns: Vec<ColumnDetailResponse>,
    /// Primary key columns.
    pub primary_key: Option<Vec<String>>,
    /// Foreign keys.
    pub foreign_keys: Vec<ForeignKeyResponse>,
}

/// Column detail for table response.
#[derive(Debug, Serialize)]
pub struct ColumnDetailResponse {
    /// Column name.
    pub name: String,
    /// Data type.
    pub data_type: String,
    /// Whether nullable.
    pub is_nullable: bool,
    /// Whether part of primary key.
    pub is_primary_key: bool,
    /// Is this an identity column.
    pub is_identity: bool,
}

/// Foreign key detail for table response.
#[derive(Debug, Serialize)]
pub struct ForeignKeyResponse {
    /// Constraint name.
    pub name: String,
    /// Source columns.
    pub columns: Vec<String>,
    /// Referenced table (schema.table).
    pub referenced_table: String,
    /// Referenced columns.
    pub referenced_columns: Vec<String>,
}

/// Request to generate Lua source definitions.
#[derive(Debug, Deserialize)]
pub struct GenerateSourcesRequest {
    /// Tables to generate sources for.
    pub tables: Vec<TableSelection>,
}

/// Table selection for source generation.
#[derive(Debug, Deserialize)]
pub struct TableSelection {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Optional custom entity name (defaults to table name).
    pub entity_name: Option<String>,
}

/// Response from source generation.
#[derive(Debug, Serialize)]
pub struct GenerateSourcesResponse {
    /// Whether generation was successful.
    pub success: bool,
    /// Generated Lua code (single file, for backwards compatibility).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lua_code: Option<String>,
    /// Generated files (filename -> content).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<std::collections::HashMap<String, String>>,
    /// Error message if generation failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to detect relationships between tables.
#[derive(Debug, Deserialize)]
pub struct DetectRelationshipsRequest {
    /// Tables to detect relationships between.
    pub tables: Vec<TableSelection>,
}

/// A detected relationship between tables.
#[derive(Debug, Clone, Serialize)]
pub struct DetectedRelationship {
    /// Source table name.
    pub from_table: String,
    /// Source column name.
    pub from_column: String,
    /// Target table name.
    pub to_table: String,
    /// Target column name.
    pub to_column: String,
    /// Confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Source of the relationship.
    pub source: RelationshipSourceType,
    /// Cardinality of the relationship.
    pub cardinality: String,
    /// Inference rule that matched (for inferred relationships).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
}

/// Source type for a detected relationship.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipSourceType {
    /// From database foreign key constraint.
    DatabaseConstraint,
    /// Inferred from naming patterns.
    Inferred,
}

/// Response from relationship detection.
#[derive(Debug, Serialize)]
pub struct DetectRelationshipsResponse {
    /// Whether detection was successful.
    pub success: bool,
    /// Detected relationships.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relationships: Option<Vec<DetectedRelationship>>,
    /// Error message if detection failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to generate sources with relationships.
#[derive(Debug, Deserialize)]
pub struct GenerateWithRelationshipsRequest {
    /// Tables to generate sources for.
    pub tables: Vec<TableSelection>,
    /// Relationships to include in output.
    pub relationships: Vec<RelationshipSelection>,
}

/// Relationship selection for code generation.
#[derive(Debug, Deserialize)]
pub struct RelationshipSelection {
    /// Source table name.
    pub from_table: String,
    /// Source column name.
    pub from_column: String,
    /// Target table name.
    pub to_table: String,
    /// Target column name.
    pub to_column: String,
}

// ============================================================================
// Table Classification Types
// ============================================================================

/// Suggested table type based on analysis.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SuggestedTableType {
    /// Raw data source, no special handling.
    Source,
    /// Conformed dimension table.
    Dimension,
    /// Fact table with measures.
    Fact,
    /// Bridge/junction table for many-to-many relationships.
    Bridge,
    /// Could not determine type.
    Unknown,
}

/// Classification result for a table.
#[derive(Debug, Clone, Serialize)]
pub struct TableClassification {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Suggested table type.
    pub suggested_type: SuggestedTableType,
    /// Confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Reasons for the classification.
    pub reasons: Vec<String>,
    /// Suggestion details (dimension or fact config).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<TableSuggestion>,
}

/// Suggestion for how to configure a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableSuggestion {
    /// Dimension configuration suggestion.
    Dimension(DimensionSuggestion),
    /// Fact configuration suggestion.
    Fact(FactSuggestion),
}

/// Suggested dimension configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionSuggestion {
    /// Dimension name (e.g., "dim_customers").
    pub name: String,
    /// Source table name.
    pub source: String,
    /// Source table schema.
    #[serde(default)]
    pub source_schema: String,
    /// Target table for materialized dimension.
    pub target_table: String,
    /// Columns to include.
    pub columns: Vec<String>,
    /// Primary key columns.
    pub primary_key: Vec<String>,
    /// SCD type (0, 1, or 2).
    pub scd_type: i32,
    /// Whether to materialize.
    pub materialized: bool,
}

/// Suggested fact configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactSuggestion {
    /// Fact name (e.g., "fact_orders").
    pub name: String,
    /// Source table name.
    pub source: String,
    /// Source table schema.
    #[serde(default)]
    pub source_schema: String,
    /// Target table for materialized fact.
    pub target_table: String,
    /// Grain columns (what defines one row).
    pub grain: Vec<GrainColumnSuggestion>,
    /// Suggested measures.
    pub measures: Vec<MeasureSuggestion>,
    /// Dimension references for includes.
    pub dimension_refs: Vec<DimensionRef>,
    /// Whether to materialize.
    pub materialized: bool,
}

/// Grain column in a fact suggestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainColumnSuggestion {
    /// Source entity name.
    pub source_entity: String,
    /// Column name.
    pub column: String,
}

/// Suggested measure configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureSuggestion {
    /// Measure name (e.g., "total_revenue").
    pub name: String,
    /// Source column.
    pub column: String,
    /// Aggregation type: sum, count, avg, min, max.
    pub aggregation: String,
}

/// Reference to a dimension from a fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionRef {
    /// FK column in the fact table.
    pub column: String,
    /// Referenced dimension name.
    pub dimension: String,
    /// PK column in the dimension.
    pub dimension_key: String,
    /// Columns to include from the dimension.
    pub include_columns: Vec<String>,
}

/// Request to classify tables.
#[derive(Debug, Deserialize)]
pub struct ClassifyTablesRequest {
    /// Tables to classify.
    pub tables: Vec<TableSelection>,
    /// Detected relationships (used for FK detection).
    #[serde(default)]
    pub relationships: Vec<RelationshipSelection>,
}

/// Response from table classification.
#[derive(Debug, Serialize)]
pub struct ClassifyTablesResponse {
    /// Whether classification was successful.
    pub success: bool,
    /// Classification results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classifications: Option<Vec<TableClassification>>,
    /// Error message if classification failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to generate code with suggestions.
#[derive(Debug, Deserialize)]
pub struct GenerateWithSuggestionsRequest {
    /// Tables to generate as plain sources.
    pub sources: Vec<TableSelection>,
    /// Dimension configurations.
    pub dimensions: Vec<DimensionSuggestion>,
    /// Fact configurations.
    pub facts: Vec<FactSuggestion>,
    /// Relationships to include.
    pub relationships: Vec<RelationshipSelection>,
}

/// Application state including database connection.
pub struct AppStateWithDb {
    /// Directory containing model files.
    pub model_dir: std::path::PathBuf,
    /// Shared database connection state.
    pub db_connection: SharedConnection,
    /// Application settings.
    pub settings: Settings,
}

// ============================================================================
// Credential API Types
// ============================================================================

/// Response from listing credentials.
#[derive(Debug, Serialize)]
pub struct ListCredentialsResponse {
    /// List of saved credentials (metadata only, no connection strings).
    pub credentials: Vec<SavedCredential>,
    /// Whether credentials are persistent across restarts.
    pub persistent: bool,
}

/// Request to save a new credential.
#[derive(Debug, Deserialize)]
pub struct SaveCredentialRequest {
    /// Database driver: "mssql" or "duckdb".
    pub driver: String,
    /// Connection string to encrypt and store.
    pub connection_string: String,
    /// Optional display name for the credential.
    pub display_name: Option<String>,
}

/// Response from saving a credential.
#[derive(Debug, Serialize)]
pub struct SaveCredentialResponse {
    /// Whether the save was successful.
    pub success: bool,
    /// The ID of the saved credential (if successful).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Error message if save failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Generic response for simple success/error operations.
#[derive(Debug, Serialize)]
pub struct GenericResponse {
    /// Whether the operation was successful.
    pub success: bool,
    /// Error message if operation failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// API Handlers
// ============================================================================

/// GET /api/connection - Get connection status.
pub async fn get_connection_status(
    State(state): State<Arc<AppStateWithDb>>,
) -> Json<ConnectionStatusResponse> {
    let conn = state.db_connection.read().await;

    let (connected, driver, default_schema) = match &conn.config {
        Some(config) => (true, Some(config.driver.clone()), config.default_schema.clone()),
        None => (false, None, None),
    };

    Json(ConnectionStatusResponse {
        connected,
        driver,
        supported_drivers: SUPPORTED_DRIVERS.iter().map(|s| s.to_string()).collect(),
        default_schema,
    })
}

/// POST /api/connection/test - Test a database connection without storing it.
pub async fn test_connection(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<TestConnectionRequest>,
) -> Json<TestConnectionResponse> {
    // Validate driver
    if !SUPPORTED_DRIVERS.contains(&req.driver.as_str()) {
        return Json(TestConnectionResponse {
            success: false,
            error: Some(format!(
                "Unsupported driver '{}'. Supported: {:?}",
                req.driver, SUPPORTED_DRIVERS
            )),
            database_info: None,
            schemas: None,
        });
    }

    // Spawn worker and test connection
    match WorkerClient::spawn_with_settings(&state.settings).await {
        Ok(client) => {
            let provider = WorkerMetadataProvider::with_client(
                client,
                &req.driver,
                &req.connection_string,
            );

            // Try to get database info as connection test
            match provider.get_database_info().await {
                Ok(info) => {
                    // Also fetch schemas
                    let schemas = provider.list_schemas().await.ok();

                    Json(TestConnectionResponse {
                        success: true,
                        error: None,
                        database_info: Some(DatabaseInfoResponse {
                            product_name: info.product_name,
                            product_version: info.product_version,
                            database_name: info.database_name,
                            default_schema: info.default_schema,
                        }),
                        schemas,
                    })
                }
                Err(e) => Json(TestConnectionResponse {
                    success: false,
                    error: Some(format!("Connection failed: {}", e)),
                    database_info: None,
                    schemas: None,
                }),
            }
        }
        Err(e) => Json(TestConnectionResponse {
            success: false,
            error: Some(format!("Failed to start worker: {}", e)),
            database_info: None,
            schemas: None,
        }),
    }
}

/// POST /api/connection - Set/update the active connection.
pub async fn set_connection(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<SetConnectionRequest>,
) -> Result<Json<TestConnectionResponse>, StatusCode> {
    // Validate driver
    if !SUPPORTED_DRIVERS.contains(&req.driver.as_str()) {
        return Ok(Json(TestConnectionResponse {
            success: false,
            error: Some(format!(
                "Unsupported driver '{}'. Supported: {:?}",
                req.driver, SUPPORTED_DRIVERS
            )),
            database_info: None,
            schemas: None,
        }));
    }

    // Spawn worker
    let client = match WorkerClient::spawn_with_settings(&state.settings).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return Ok(Json(TestConnectionResponse {
                success: false,
                error: Some(format!("Failed to start worker: {}", e)),
                database_info: None,
                schemas: None,
            }));
        }
    };

    // Test the connection
    let provider = WorkerMetadataProvider::new(
        client.clone(),
        &req.driver,
        &req.connection_string,
    );

    match provider.get_database_info().await {
        Ok(info) => {
            let schemas = provider.list_schemas().await.ok();
            let default_schema = req.default_schema.or(info.default_schema.clone());

            // Store the connection
            let mut conn = state.db_connection.write().await;
            conn.config = Some(ActiveConnection {
                driver: req.driver.clone(),
                connection_string: req.connection_string.clone(),
                client,
                default_schema: default_schema.clone(),
            });

            Ok(Json(TestConnectionResponse {
                success: true,
                error: None,
                database_info: Some(DatabaseInfoResponse {
                    product_name: info.product_name,
                    product_version: info.product_version,
                    database_name: info.database_name,
                    default_schema,
                }),
                schemas,
            }))
        }
        Err(e) => Ok(Json(TestConnectionResponse {
            success: false,
            error: Some(format!("Connection failed: {}", e)),
            database_info: None,
            schemas: None,
        })),
    }
}

/// DELETE /api/connection - Disconnect the active connection.
pub async fn disconnect(
    State(state): State<Arc<AppStateWithDb>>,
) -> StatusCode {
    let mut conn = state.db_connection.write().await;
    conn.config = None;
    StatusCode::OK
}

/// GET /api/database/schemas - List all schemas.
pub async fn list_schemas(
    State(state): State<Arc<AppStateWithDb>>,
) -> Result<Json<Vec<SchemaInfo>>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    provider
        .list_schemas()
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// GET /api/database/tables/:schema - List tables in a schema.
pub async fn list_tables(
    State(state): State<Arc<AppStateWithDb>>,
    axum::extract::Path(schema): axum::extract::Path<String>,
) -> Result<Json<Vec<TableInfo>>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    provider
        .list_tables(&schema)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// GET /api/database/table/:schema/:table - Get table details.
pub async fn get_table(
    State(state): State<Arc<AppStateWithDb>>,
    axum::extract::Path((schema, table)): axum::extract::Path<(String, String)>,
) -> Result<Json<TableDetailResponse>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    let metadata = provider
        .get_table(&schema, &table)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(table_metadata_to_response(&metadata)))
}

/// POST /api/generate/sources - Generate Lua source definitions from selected tables.
pub async fn generate_sources(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<GenerateSourcesRequest>,
) -> Result<Json<GenerateSourcesResponse>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    // Fetch metadata for all selected tables
    let mut tables: Vec<TableMetadata> = Vec::new();
    for selection in &req.tables {
        match provider.get_table(&selection.schema, &selection.table).await {
            Ok(metadata) => tables.push(metadata),
            Err(e) => {
                return Ok(Json(GenerateSourcesResponse {
                    success: false,
                    lua_code: None,
                    files: None,
                    error: Some(format!(
                        "Failed to fetch table {}.{}: {}",
                        selection.schema, selection.table, e
                    )),
                }));
            }
        }
    }

    // Generate Lua code
    let lua_code = generate_lua_sources(&tables, &req.tables, &[]);

    Ok(Json(GenerateSourcesResponse {
        success: true,
        lua_code: Some(lua_code),
        files: None,
        error: None,
    }))
}

/// POST /api/generate/relationships - Detect relationships between tables.
pub async fn detect_relationships(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<DetectRelationshipsRequest>,
) -> Result<Json<DetectRelationshipsResponse>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    // Fetch metadata for all selected tables
    let mut table_metadata: Vec<TableMetadata> = Vec::new();
    for selection in &req.tables {
        match provider.get_table(&selection.schema, &selection.table).await {
            Ok(metadata) => table_metadata.push(metadata),
            Err(e) => {
                return Ok(Json(DetectRelationshipsResponse {
                    success: false,
                    relationships: None,
                    error: Some(format!(
                        "Failed to fetch table {}.{}: {}",
                        selection.schema, selection.table, e
                    )),
                }));
            }
        }
    }

    // Detect relationships using the inference engine
    let relationships = detect_relationships_from_tables(&table_metadata);

    Ok(Json(DetectRelationshipsResponse {
        success: true,
        relationships: Some(relationships),
        error: None,
    }))
}

/// POST /api/generate/with-relationships - Generate sources with relationships.
pub async fn generate_with_relationships(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<GenerateWithRelationshipsRequest>,
) -> Result<Json<GenerateSourcesResponse>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    // Fetch metadata for all selected tables
    let mut tables: Vec<TableMetadata> = Vec::new();
    for selection in &req.tables {
        match provider.get_table(&selection.schema, &selection.table).await {
            Ok(metadata) => tables.push(metadata),
            Err(e) => {
                return Ok(Json(GenerateSourcesResponse {
                    success: false,
                    lua_code: None,
                    files: None,
                    error: Some(format!(
                        "Failed to fetch table {}.{}: {}",
                        selection.schema, selection.table, e
                    )),
                }));
            }
        }
    }

    // Generate Lua code with relationships
    let lua_code = generate_lua_sources(&tables, &req.tables, &req.relationships);

    Ok(Json(GenerateSourcesResponse {
        success: true,
        lua_code: Some(lua_code),
        files: None,
        error: None,
    }))
}

/// POST /api/generate/classify - Classify tables as dimension/fact/source.
pub async fn classify_tables(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<ClassifyTablesRequest>,
) -> Result<Json<ClassifyTablesResponse>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    // Fetch metadata for all selected tables
    let mut table_metadata: Vec<TableMetadata> = Vec::new();
    for selection in &req.tables {
        match provider.get_table(&selection.schema, &selection.table).await {
            Ok(metadata) => table_metadata.push(metadata),
            Err(e) => {
                return Ok(Json(ClassifyTablesResponse {
                    success: false,
                    classifications: None,
                    error: Some(format!(
                        "Failed to fetch table {}.{}: {}",
                        selection.schema, selection.table, e
                    )),
                }));
            }
        }
    }

    // Classify each table
    let classifications = classify_tables_internal(&table_metadata, &req.relationships);

    Ok(Json(ClassifyTablesResponse {
        success: true,
        classifications: Some(classifications),
        error: None,
    }))
}

/// POST /api/generate/with-suggestions - Generate code from suggestions.
pub async fn generate_with_suggestions(
    State(state): State<Arc<AppStateWithDb>>,
    Json(req): Json<GenerateWithSuggestionsRequest>,
) -> Result<Json<GenerateSourcesResponse>, (StatusCode, String)> {
    let conn = state.db_connection.read().await;

    let config = conn.config.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let provider = WorkerMetadataProvider::new(
        config.client.clone(),
        &config.driver,
        &config.connection_string,
    );

    // Build a map to fetch metadata for all tables we need
    // We need metadata for: sources, dimensions, and facts
    let mut all_table_metadata: std::collections::HashMap<String, TableMetadata> = std::collections::HashMap::new();

    // Collect all unique table names we need
    let mut tables_to_fetch: Vec<(String, String)> = Vec::new();

    // Add source tables
    for selection in &req.sources {
        let key = format!("{}.{}", selection.schema, selection.table).to_lowercase();
        if !all_table_metadata.contains_key(&key) {
            tables_to_fetch.push((selection.schema.clone(), selection.table.clone()));
        }
    }

    // Add dimension source tables
    for dim in &req.dimensions {
        // Use source_schema directly from the suggestion
        let schema = dim.source_schema.clone();
        let key = format!("{}.{}", schema, dim.source).to_lowercase();
        if !all_table_metadata.contains_key(&key) && !tables_to_fetch.iter().any(|(s, t)| s == &schema && t.eq_ignore_ascii_case(&dim.source)) {
            tables_to_fetch.push((schema, dim.source.clone()));
        }
    }

    // Add fact source tables
    for fact in &req.facts {
        // Use source_schema directly from the suggestion
        let schema = fact.source_schema.clone();
        let key = format!("{}.{}", schema, fact.source).to_lowercase();
        if !all_table_metadata.contains_key(&key) && !tables_to_fetch.iter().any(|(s, t)| s == &schema && t.eq_ignore_ascii_case(&fact.source)) {
            tables_to_fetch.push((schema, fact.source.clone()));
        }
    }

    // Fetch all table metadata
    for (schema, table) in &tables_to_fetch {
        match provider.get_table(schema, table).await {
            Ok(metadata) => {
                let key = format!("{}.{}", schema, table).to_lowercase();
                all_table_metadata.insert(key, metadata);
            }
            Err(e) => {
                return Ok(Json(GenerateSourcesResponse {
                    success: false,
                    lua_code: None,
                    files: None,
                    error: Some(format!(
                        "Failed to fetch table {}.{}: {}",
                        schema, table, e
                    )),
                }));
            }
        }
    }

    // Generate separate files
    let files = generate_lua_files(
        &all_table_metadata,
        &req.sources,
        &req.dimensions,
        &req.facts,
        &req.relationships,
    );

    // Also generate combined lua_code for backwards compatibility
    let mut lua_code = String::new();
    if let Some(sources) = files.get("sources.lua") {
        lua_code.push_str(sources);
        lua_code.push_str("\n");
    }
    if let Some(dimensions) = files.get("dimensions.lua") {
        lua_code.push_str(dimensions);
        lua_code.push_str("\n");
    }
    if let Some(facts) = files.get("facts.lua") {
        lua_code.push_str(facts);
        lua_code.push_str("\n");
    }
    if let Some(relationships) = files.get("relationships.lua") {
        lua_code.push_str(relationships);
    }

    Ok(Json(GenerateSourcesResponse {
        success: true,
        lua_code: Some(lua_code),
        files: Some(files),
        error: None,
    }))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert TableMetadata to API response.
fn table_metadata_to_response(metadata: &TableMetadata) -> TableDetailResponse {
    let pk_columns: Vec<String> = metadata
        .primary_key
        .as_ref()
        .map(|pk| pk.columns.clone())
        .unwrap_or_default();

    let columns: Vec<ColumnDetailResponse> = metadata
        .columns
        .iter()
        .map(|col| ColumnDetailResponse {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            is_nullable: col.is_nullable,
            is_primary_key: pk_columns.iter().any(|pk| pk.eq_ignore_ascii_case(&col.name)),
            is_identity: col.is_identity,
        })
        .collect();

    let foreign_keys: Vec<ForeignKeyResponse> = metadata
        .foreign_keys
        .iter()
        .map(|fk| ForeignKeyResponse {
            name: fk.name.clone(),
            columns: fk.columns.clone(),
            referenced_table: if fk.referenced_schema.is_empty() {
                fk.referenced_table.clone()
            } else {
                format!("{}.{}", fk.referenced_schema, fk.referenced_table)
            },
            referenced_columns: fk.referenced_columns.clone(),
        })
        .collect();

    TableDetailResponse {
        schema: metadata.schema.clone(),
        name: metadata.name.clone(),
        table_type: format!("{:?}", metadata.table_type),
        columns,
        primary_key: if pk_columns.is_empty() {
            None
        } else {
            Some(pk_columns)
        },
        foreign_keys,
    }
}

/// Generate Lua source definitions from table metadata.
fn generate_lua_sources(
    tables: &[TableMetadata],
    selections: &[TableSelection],
    relationships: &[RelationshipSelection],
) -> String {
    let mut lua = String::new();
    lua.push_str("-- Auto-generated source definitions\n");
    lua.push_str("-- Review and customize as needed\n\n");

    // Build a map of table name -> entity name for relationship generation
    let entity_names: std::collections::HashMap<String, String> = selections
        .iter()
        .zip(tables.iter())
        .map(|(sel, meta)| {
            let entity_name = sel.entity_name.clone().unwrap_or_else(|| meta.name.clone());
            (meta.name.clone().to_lowercase(), entity_name)
        })
        .collect();

    for (metadata, selection) in tables.iter().zip(selections.iter()) {
        let entity_name = selection
            .entity_name
            .as_ref()
            .unwrap_or(&metadata.name);

        lua.push_str(&format!("source \"{}\" {{\n", entity_name));

        // Table reference
        if metadata.schema.is_empty() {
            lua.push_str(&format!("  table = \"{}\",\n", metadata.name));
        } else {
            lua.push_str(&format!("  table = \"{}.{}\",\n", metadata.schema, metadata.name));
        }

        // Primary key
        let pk_columns = metadata.primary_key_columns();
        if !pk_columns.is_empty() {
            if pk_columns.len() == 1 {
                lua.push_str(&format!("  primary_key = \"{}\",\n", pk_columns[0]));
            } else {
                let pk_list: Vec<String> = pk_columns.iter().map(|s| format!("\"{}\"", s)).collect();
                lua.push_str(&format!("  primary_key = {{ {} }},\n", pk_list.join(", ")));
            }
        }

        // Columns
        lua.push_str("  columns = {\n");
        for col in &metadata.columns {
            let mantis_type = db_type_to_mantis_type(&col.data_type);
            let nullable = if col.is_nullable { ", nullable = true" } else { "" };
            lua.push_str(&format!(
                "    {} = {{ type = \"{}\"{} }},\n",
                col.name, mantis_type, nullable
            ));
        }
        lua.push_str("  },\n");

        lua.push_str("}\n\n");
    }

    // Generate link() statements for relationships
    if !relationships.is_empty() {
        lua.push_str("-- Relationships\n");
        for rel in relationships {
            // Look up entity names for from/to tables
            let from_entity = entity_names
                .get(&rel.from_table.to_lowercase())
                .cloned()
                .unwrap_or_else(|| rel.from_table.clone());
            let to_entity = entity_names
                .get(&rel.to_table.to_lowercase())
                .cloned()
                .unwrap_or_else(|| rel.to_table.clone());

            lua.push_str(&format!(
                "link({}.{}, {}.{})\n",
                from_entity, rel.from_column, to_entity, rel.to_column
            ));
        }
        lua.push('\n');
    }

    lua
}

/// Detect relationships between tables using FK constraints and inference engine.
fn detect_relationships_from_tables(tables: &[TableMetadata]) -> Vec<DetectedRelationship> {
    use crate::semantic::inference::{
        InferenceEngine, RelationshipSource, TableInfo as InferenceTableInfo,
    };

    let mut relationships = Vec::new();

    // Build set of table names for filtering
    let table_names: std::collections::HashSet<String> = tables
        .iter()
        .map(|t| t.name.to_lowercase())
        .collect();

    // First, extract relationships from FK constraints
    for table in tables {
        for fk in &table.foreign_keys {
            // Only include if the referenced table is in our selection
            if table_names.contains(&fk.referenced_table.to_lowercase()) {
                // Handle multi-column FKs by creating one relationship per column pair
                for (i, from_col) in fk.columns.iter().enumerate() {
                    let to_col = fk.referenced_columns.get(i).cloned()
                        .unwrap_or_else(|| fk.referenced_columns.first().cloned().unwrap_or_default());

                    relationships.push(DetectedRelationship {
                        from_table: table.name.clone(),
                        from_column: from_col.clone(),
                        to_table: fk.referenced_table.clone(),
                        to_column: to_col,
                        confidence: 0.98, // High confidence for DB constraints
                        source: RelationshipSourceType::DatabaseConstraint,
                        cardinality: "ManyToOne".to_string(),
                        rule: Some(format!("FK: {}", fk.name)),
                    });
                }
            }
        }
    }

    // Then, run the inference engine to find additional relationships
    let inference_tables: Vec<InferenceTableInfo> = tables
        .iter()
        .map(|t| InferenceTableInfo::from(t))
        .collect();

    let mut engine = InferenceEngine::default();
    engine.prepare(&inference_tables);
    engine.load_constraints(tables);

    let inferred = engine.infer_all_relationships(&inference_tables);

    // Add inferred relationships that aren't already from FK constraints
    let existing_keys: std::collections::HashSet<(String, String, String, String)> = relationships
        .iter()
        .map(|r| (
            r.from_table.to_lowercase(),
            r.from_column.to_lowercase(),
            r.to_table.to_lowercase(),
            r.to_column.to_lowercase(),
        ))
        .collect();

    for rel in inferred {
        // Only include if both tables are in our selection
        if !table_names.contains(&rel.from_table.to_lowercase())
            || !table_names.contains(&rel.to_table.to_lowercase())
        {
            continue;
        }

        let key = (
            rel.from_table.to_lowercase(),
            rel.from_column.to_lowercase(),
            rel.to_table.to_lowercase(),
            rel.to_column.to_lowercase(),
        );

        if !existing_keys.contains(&key) {
            relationships.push(DetectedRelationship {
                from_table: rel.from_table.clone(),
                from_column: rel.from_column.clone(),
                to_table: rel.to_table.clone(),
                to_column: rel.to_column.clone(),
                confidence: rel.confidence,
                source: match rel.source {
                    RelationshipSource::DatabaseConstraint => RelationshipSourceType::DatabaseConstraint,
                    RelationshipSource::Inferred | RelationshipSource::UserDefined => RelationshipSourceType::Inferred,
                },
                cardinality: format!("{:?}", rel.cardinality),
                rule: Some(rel.rule.clone()),
            });
        }
    }

    // Sort by confidence descending
    relationships.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    relationships
}

/// Classify tables into dimension/fact/source categories.
fn classify_tables_internal(
    tables: &[TableMetadata],
    relationships: &[RelationshipSelection],
) -> Vec<TableClassification> {
    // Build lookup maps
    let table_map: std::collections::HashMap<String, &TableMetadata> = tables
        .iter()
        .map(|t| (t.name.to_lowercase(), t))
        .collect();

    // Build FK relationship lookup (from_table -> list of to_tables)
    let fk_targets: std::collections::HashMap<String, Vec<String>> = relationships
        .iter()
        .fold(std::collections::HashMap::new(), |mut acc, rel| {
            acc.entry(rel.from_table.to_lowercase())
                .or_default()
                .push(rel.to_table.to_lowercase());
            acc
        });

    // Build reverse FK lookup (to_table -> list of from_tables that reference it)
    let fk_sources: std::collections::HashMap<String, Vec<String>> = relationships
        .iter()
        .fold(std::collections::HashMap::new(), |mut acc, rel| {
            acc.entry(rel.to_table.to_lowercase())
                .or_default()
                .push(rel.from_table.to_lowercase());
            acc
        });

    // First pass: classify all tables
    let mut classifications: Vec<TableClassification> = tables
        .iter()
        .map(|metadata| classify_single_table(metadata, &fk_targets, &fk_sources))
        .collect();

    // Second pass: generate suggestions based on classification
    // Build dimension name lookup for fact suggestions
    let dimension_names: std::collections::HashMap<String, String> = classifications
        .iter()
        .filter(|c| c.suggested_type == SuggestedTableType::Dimension)
        .map(|c| (c.table.to_lowercase(), c.table.clone()))
        .collect();

    for classification in &mut classifications {
        if let Some(metadata) = table_map.get(&classification.table.to_lowercase()) {
            match classification.suggested_type {
                SuggestedTableType::Dimension => {
                    classification.suggestion = Some(TableSuggestion::Dimension(
                        generate_dimension_suggestion(metadata),
                    ));
                }
                SuggestedTableType::Fact => {
                    classification.suggestion = Some(TableSuggestion::Fact(
                        generate_fact_suggestion(metadata, relationships, &dimension_names),
                    ));
                }
                _ => {}
            }
        }
    }

    classifications
}

/// Classify a single table based on naming patterns and structure.
fn classify_single_table(
    metadata: &TableMetadata,
    fk_targets: &std::collections::HashMap<String, Vec<String>>,
    fk_sources: &std::collections::HashMap<String, Vec<String>>,
) -> TableClassification {
    let name_lower = metadata.name.to_lowercase();
    let mut reasons = Vec::new();
    let mut suggested_type = SuggestedTableType::Unknown;
    let mut confidence = 0.0;

    // Rule 1: Naming pattern detection (highest priority)
    if let Some((name_type, name_conf, reason)) = detect_naming_pattern(&name_lower) {
        suggested_type = name_type;
        confidence = name_conf;
        reasons.push(reason);
    }

    // If not determined by naming, analyze structure
    if suggested_type == SuggestedTableType::Unknown {
        let (struct_type, struct_conf, struct_reasons) =
            analyze_table_structure(metadata, fk_targets, fk_sources);
        suggested_type = struct_type;
        confidence = struct_conf;
        reasons.extend(struct_reasons);
    }

    // Default to Source if still unknown
    if suggested_type == SuggestedTableType::Unknown {
        suggested_type = SuggestedTableType::Source;
        confidence = 0.5;
        reasons.push("No clear pattern detected, defaulting to source".to_string());
    }

    TableClassification {
        schema: metadata.schema.clone(),
        table: metadata.name.clone(),
        suggested_type,
        confidence,
        reasons,
        suggestion: None, // Filled in second pass
    }
}

/// Detect table type from naming patterns.
fn detect_naming_pattern(name: &str) -> Option<(SuggestedTableType, f64, String)> {
    // Dimension patterns
    if name.starts_with("dim_") || name.starts_with("dimension_") {
        return Some((
            SuggestedTableType::Dimension,
            0.95,
            format!("Name prefix 'dim_' indicates dimension table"),
        ));
    }
    if name.starts_with("d_") && name.len() > 2 {
        return Some((
            SuggestedTableType::Dimension,
            0.85,
            format!("Name prefix 'd_' suggests dimension table"),
        ));
    }

    // Fact patterns
    if name.starts_with("fct_") || name.starts_with("fact_") {
        return Some((
            SuggestedTableType::Fact,
            0.95,
            format!("Name prefix 'fct_' or 'fact_' indicates fact table"),
        ));
    }
    if name.starts_with("f_") && name.len() > 2 {
        return Some((
            SuggestedTableType::Fact,
            0.85,
            format!("Name prefix 'f_' suggests fact table"),
        ));
    }

    // Bridge/junction patterns
    if name.starts_with("bridge_") || name.starts_with("xref_") || name.starts_with("link_") {
        return Some((
            SuggestedTableType::Bridge,
            0.90,
            format!("Name prefix indicates bridge/junction table"),
        ));
    }

    // Staging/source patterns
    if name.starts_with("stg_") || name.starts_with("staging_") || name.starts_with("raw_") {
        return Some((
            SuggestedTableType::Source,
            0.85,
            format!("Name prefix indicates staging/source table"),
        ));
    }

    None
}

/// Analyze table structure to determine type.
fn analyze_table_structure(
    metadata: &TableMetadata,
    fk_targets: &std::collections::HashMap<String, Vec<String>>,
    fk_sources: &std::collections::HashMap<String, Vec<String>>,
) -> (SuggestedTableType, f64, Vec<String>) {
    let name_lower = metadata.name.to_lowercase();
    let mut reasons = Vec::new();

    // Count column types
    let pk_columns = metadata.primary_key_columns();
    let fk_columns: Vec<&str> = metadata
        .foreign_keys
        .iter()
        .flat_map(|fk| fk.columns.iter().map(|s| s.as_str()))
        .collect();

    let numeric_non_fk: Vec<&str> = metadata
        .columns
        .iter()
        .filter(|c| {
            let is_fk = fk_columns.iter().any(|fk| fk.eq_ignore_ascii_case(&c.name));
            let is_pk = pk_columns.iter().any(|pk| pk.eq_ignore_ascii_case(&c.name));
            !is_fk && !is_pk && is_numeric_type(&c.data_type) && !is_date_type(&c.data_type)
        })
        .map(|c| c.name.as_str())
        .collect();

    let date_columns: Vec<&str> = metadata
        .columns
        .iter()
        .filter(|c| is_date_type(&c.data_type))
        .map(|c| c.name.as_str())
        .collect();

    // Get FK relationships
    let outgoing_fks = fk_targets.get(&name_lower).map(|v| v.len()).unwrap_or(0);
    let incoming_fks = fk_sources.get(&name_lower).map(|v| v.len()).unwrap_or(0);

    // Bridge table detection: exactly 2 FK columns, minimal other columns
    if fk_columns.len() == 2 && metadata.columns.len() <= 4 {
        reasons.push(format!(
            "Has exactly 2 FK columns with {} total columns (bridge pattern)",
            metadata.columns.len()
        ));
        return (SuggestedTableType::Bridge, 0.80, reasons);
    }

    // Fact table detection: multiple FKs + numeric columns
    if outgoing_fks >= 2 && !numeric_non_fk.is_empty() {
        reasons.push(format!(
            "Has {} FK relationships and {} numeric measure columns",
            outgoing_fks,
            numeric_non_fk.len()
        ));
        if !date_columns.is_empty() {
            reasons.push(format!("Has date columns: {:?}", date_columns));
        }
        return (SuggestedTableType::Fact, 0.75, reasons);
    }

    // Dimension detection: referenced by other tables, few/no outgoing FKs
    if incoming_fks >= 1 && outgoing_fks == 0 && numeric_non_fk.is_empty() {
        reasons.push(format!(
            "Referenced by {} tables with no outgoing FKs (dimension pattern)",
            incoming_fks
        ));
        return (SuggestedTableType::Dimension, 0.70, reasons);
    }

    // Dimension detection: has _id suffix in name and primarily descriptive columns
    if (name_lower.ends_with("s") || name_lower.ends_with("es"))
        && !pk_columns.is_empty()
        && numeric_non_fk.len() <= 1
    {
        reasons.push("Plural name with primary key and mostly descriptive columns".to_string());
        return (SuggestedTableType::Dimension, 0.60, reasons);
    }

    reasons.push("Structure analysis inconclusive".to_string());
    (SuggestedTableType::Unknown, 0.0, reasons)
}

/// Generate dimension suggestion from metadata.
fn generate_dimension_suggestion(metadata: &TableMetadata) -> DimensionSuggestion {
    let pk_columns: Vec<String> = metadata.primary_key_columns().into_iter().map(|s| s.to_string()).collect();

    // System columns to exclude
    let system_columns: std::collections::HashSet<&str> = [
        "created_at",
        "updated_at",
        "modified_at",
        "deleted_at",
        "created_by",
        "updated_by",
        "modified_by",
        "deleted_by",
        "row_version",
        "etl_batch_id",
        "dw_insert_date",
        "dw_update_date",
    ]
    .into_iter()
    .collect();

    // Columns to include (exclude system columns)
    let columns: Vec<String> = metadata
        .columns
        .iter()
        .filter(|c| !system_columns.contains(c.name.to_lowercase().as_str()))
        .map(|c| c.name.clone())
        .collect();

    // Detect SCD type from column names
    let has_valid_from = metadata
        .columns
        .iter()
        .any(|c| c.name.to_lowercase().contains("valid_from") || c.name.to_lowercase().contains("effective_from"));
    let has_valid_to = metadata
        .columns
        .iter()
        .any(|c| c.name.to_lowercase().contains("valid_to") || c.name.to_lowercase().contains("effective_to"));

    let scd_type = if has_valid_from && has_valid_to { 2 } else { 1 };

    // Generate target table name
    let target_table = format!("analytics.{}", metadata.name);

    DimensionSuggestion {
        name: metadata.name.clone(),
        source: metadata.name.clone(),
        source_schema: metadata.schema.clone(),
        target_table,
        columns,
        primary_key: pk_columns,
        scd_type,
        materialized: true,
    }
}

/// Generate fact suggestion from metadata.
fn generate_fact_suggestion(
    metadata: &TableMetadata,
    relationships: &[RelationshipSelection],
    dimension_names: &std::collections::HashMap<String, String>,
) -> FactSuggestion {
    let pk_columns: Vec<String> = metadata.primary_key_columns().into_iter().map(|s| s.to_string()).collect();
    let name_lower = metadata.name.to_lowercase();

    // Get FK columns from relationships where this table is the "from" side
    let fk_columns: Vec<String> = relationships
        .iter()
        .filter(|rel| rel.from_table.to_lowercase() == name_lower)
        .map(|rel| rel.from_column.clone())
        .collect();

    // Build grain: use PK if available, otherwise use all FK columns
    let grain: Vec<GrainColumnSuggestion> = if !pk_columns.is_empty() {
        pk_columns
            .iter()
            .map(|col| GrainColumnSuggestion {
                source_entity: metadata.name.clone(),
                column: col.clone(),
            })
            .collect()
    } else {
        // No PK - use FK columns as grain (dimension keys define the grain)
        fk_columns
            .iter()
            .map(|col| GrainColumnSuggestion {
                source_entity: metadata.name.clone(),
                column: col.clone(),
            })
            .collect()
    };

    // Get FK columns as a set for filtering measures
    let fk_column_set: std::collections::HashSet<String> = fk_columns
        .iter()
        .map(|s| s.to_lowercase())
        .collect();

    // Identify measure columns (numeric, non-FK, non-PK, non-date)
    let measures: Vec<MeasureSuggestion> = metadata
        .columns
        .iter()
        .filter(|c| {
            let col_lower = c.name.to_lowercase();
            let is_fk = fk_column_set.contains(&col_lower);
            let is_pk = pk_columns.iter().any(|pk| pk.eq_ignore_ascii_case(&c.name));
            !is_fk && !is_pk && is_numeric_type(&c.data_type) && !is_date_type(&c.data_type)
        })
        .map(|c| {
            let col_lower = c.name.to_lowercase();
            let aggregation = infer_aggregation_type(&col_lower);
            let name = generate_measure_name(&col_lower, &aggregation);
            MeasureSuggestion {
                name,
                column: c.name.clone(),
                aggregation,
            }
        })
        .collect();

    // Build dimension references from FK relationships
    let dimension_refs: Vec<DimensionRef> = relationships
        .iter()
        .filter(|rel| rel.from_table.to_lowercase() == name_lower)
        .filter_map(|rel| {
            let to_lower = rel.to_table.to_lowercase();
            dimension_names.get(&to_lower).map(|dim_name| DimensionRef {
                column: rel.from_column.clone(),
                dimension: dim_name.clone(),
                dimension_key: rel.to_column.clone(),
                include_columns: Vec::new(), // User can add columns later
            })
        })
        .collect();

    // Generate target table name
    let target_table = format!("analytics.{}", metadata.name);

    FactSuggestion {
        name: metadata.name.clone(),
        source: metadata.name.clone(),
        source_schema: metadata.schema.clone(),
        target_table,
        grain,
        measures,
        dimension_refs,
        materialized: true,
    }
}

/// Check if a database type is numeric.
fn is_numeric_type(db_type: &str) -> bool {
    let lower = db_type.to_lowercase();
    lower.contains("int")
        || lower.contains("decimal")
        || lower.contains("numeric")
        || lower.contains("float")
        || lower.contains("double")
        || lower.contains("real")
        || lower == "money"
        || lower == "smallmoney"
}

/// Check if a database type is a date/time type.
fn is_date_type(db_type: &str) -> bool {
    let lower = db_type.to_lowercase();
    lower.contains("date") || lower.contains("time") || lower == "timestamp"
}

/// Infer aggregation type from column name.
fn infer_aggregation_type(col_name: &str) -> String {
    if col_name.ends_with("_amount")
        || col_name.ends_with("_total")
        || col_name.ends_with("_qty")
        || col_name.ends_with("_quantity")
        || col_name.ends_with("_count")
        || col_name.ends_with("_sum")
        || col_name.contains("revenue")
        || col_name.contains("sales")
        || col_name.contains("cost")
        || col_name.contains("price")
    {
        return "sum".to_string();
    }
    if col_name.ends_with("_rate")
        || col_name.ends_with("_pct")
        || col_name.ends_with("_percent")
        || col_name.ends_with("_avg")
        || col_name.ends_with("_average")
    {
        return "avg".to_string();
    }
    // Default to sum for other numeric columns
    "sum".to_string()
}

/// Generate a descriptive measure name.
fn generate_measure_name(col_name: &str, aggregation: &str) -> String {
    // If column already has a good name, use it with aggregation prefix
    let clean_name = col_name
        .trim_start_matches("_")
        .trim_end_matches("_amount")
        .trim_end_matches("_total")
        .trim_end_matches("_qty")
        .trim_end_matches("_sum");

    match aggregation {
        "sum" => {
            if col_name.contains("revenue") || col_name.contains("sales") {
                format!("total_{}", clean_name)
            } else if col_name.contains("qty") || col_name.contains("quantity") {
                format!("total_{}", clean_name)
            } else {
                format!("{}_total", clean_name)
            }
        }
        "avg" => format!("avg_{}", clean_name),
        "count" => format!("{}_count", clean_name),
        "min" => format!("min_{}", clean_name),
        "max" => format!("max_{}", clean_name),
        _ => col_name.to_string(),
    }
}

/// Generate separate Lua files for sources, dimensions, facts, and relationships.
fn generate_lua_files(
    all_metadata: &std::collections::HashMap<String, TableMetadata>,
    source_selections: &[TableSelection],
    dimensions: &[DimensionSuggestion],
    facts: &[FactSuggestion],
    relationships: &[RelationshipSelection],
) -> std::collections::HashMap<String, String> {
    let mut files = std::collections::HashMap::new();

    // =========================================================================
    // sources.lua - Source definitions for ALL tables (sources, dims, facts)
    // =========================================================================
    let mut sources_lua = String::new();
    sources_lua.push_str("-- Auto-generated source definitions\n");
    sources_lua.push_str("-- These define the raw tables from your database\n\n");

    // Generate sources for explicitly marked source tables
    for selection in source_selections {
        let key = format!("{}.{}", selection.schema, selection.table).to_lowercase();
        if let Some(metadata) = all_metadata.get(&key) {
            sources_lua.push_str(&generate_source_block(metadata, selection.entity_name.as_deref()));
        }
    }

    // Generate sources for dimension tables
    for dim in dimensions {
        // Find metadata by source name
        let metadata = all_metadata.values().find(|m| m.name.eq_ignore_ascii_case(&dim.source));
        if let Some(metadata) = metadata {
            sources_lua.push_str(&generate_source_block(metadata, None));
        }
    }

    // Generate sources for fact tables
    for fact in facts {
        // Find metadata by source name
        let metadata = all_metadata.values().find(|m| m.name.eq_ignore_ascii_case(&fact.source));
        if let Some(metadata) = metadata {
            sources_lua.push_str(&generate_source_block(metadata, None));
        }
    }

    if !sources_lua.trim().is_empty() && sources_lua.contains("source ") {
        files.insert("sources.lua".to_string(), sources_lua);
    }

    // =========================================================================
    // dimensions.lua - Dimension definitions
    // =========================================================================
    if !dimensions.is_empty() {
        let mut dims_lua = String::new();
        dims_lua.push_str("-- Auto-generated dimension definitions\n");
        dims_lua.push_str("-- Dimensions represent descriptive attributes for analysis\n\n");

        for dim in dimensions {
            dims_lua.push_str(&format!("dimension \"{}\" {{\n", dim.name));
            dims_lua.push_str(&format!("  target_table = \"{}\",\n", dim.target_table));
            dims_lua.push_str(&format!("  source = \"{}\",\n", dim.source));

            // Columns
            if !dim.columns.is_empty() {
                let col_list: Vec<String> = dim.columns.iter().map(|s| format!("\"{}\"", s)).collect();
                dims_lua.push_str(&format!("  columns = {{ {} }},\n", col_list.join(", ")));
            }

            // Primary key
            if !dim.primary_key.is_empty() {
                if dim.primary_key.len() == 1 {
                    dims_lua.push_str(&format!("  primary_key = {{ \"{}\" }},\n", dim.primary_key[0]));
                } else {
                    let pk_list: Vec<String> = dim.primary_key.iter().map(|s| format!("\"{}\"", s)).collect();
                    dims_lua.push_str(&format!("  primary_key = {{ {} }},\n", pk_list.join(", ")));
                }
            }

            // SCD type
            dims_lua.push_str(&format!("  scd_type = SCD{},\n", dim.scd_type));

            // Materialized
            if dim.materialized {
                dims_lua.push_str("  materialized = true,\n");
            }

            dims_lua.push_str("}\n\n");
        }

        files.insert("dimensions.lua".to_string(), dims_lua);
    }

    // =========================================================================
    // facts.lua - Fact definitions
    // =========================================================================
    if !facts.is_empty() {
        let mut facts_lua = String::new();
        facts_lua.push_str("-- Auto-generated fact definitions\n");
        facts_lua.push_str("-- Facts represent measurable events and transactions\n\n");

        for fact in facts {
            facts_lua.push_str(&format!("fact \"{}\" {{\n", fact.name));
            facts_lua.push_str(&format!("  target_table = \"{}\",\n", fact.target_table));

            // Grain
            if !fact.grain.is_empty() {
                let grain_list: Vec<String> = fact
                    .grain
                    .iter()
                    .map(|g| format!("{}.{}", g.source_entity, g.column))
                    .collect();
                facts_lua.push_str(&format!("  grain = {{ {} }},\n", grain_list.join(", ")));
            }

            // Includes (dimension references)
            if !fact.dimension_refs.is_empty() {
                facts_lua.push_str("  include = {\n");
                for dim_ref in &fact.dimension_refs {
                    if dim_ref.include_columns.is_empty() {
                        facts_lua.push_str(&format!("    {} = ALL,\n", dim_ref.dimension));
                    } else {
                        let cols: Vec<String> = dim_ref
                            .include_columns
                            .iter()
                            .map(|s| format!("\"{}\"", s))
                            .collect();
                        facts_lua.push_str(&format!(
                            "    {} = {{ {} }},\n",
                            dim_ref.dimension,
                            cols.join(", ")
                        ));
                    }
                }
                facts_lua.push_str("  },\n");
            }

            // Measures
            if !fact.measures.is_empty() {
                facts_lua.push_str("  measures = {\n");
                for measure in &fact.measures {
                    let agg_fn = match measure.aggregation.as_str() {
                        "sum" => format!("sum \"{}\"", measure.column),
                        "count" => "count()".to_string(),
                        "count_distinct" => format!("count_distinct \"{}\"", measure.column),
                        "avg" => format!("avg \"{}\"", measure.column),
                        "min" => format!("min \"{}\"", measure.column),
                        "max" => format!("max \"{}\"", measure.column),
                        _ => format!("sum \"{}\"", measure.column),
                    };
                    facts_lua.push_str(&format!("    {} = {},\n", measure.name, agg_fn));
                }
                facts_lua.push_str("  },\n");
            }

            // Materialized
            if fact.materialized {
                facts_lua.push_str("  materialized = true,\n");
            }

            facts_lua.push_str("}\n\n");
        }

        files.insert("facts.lua".to_string(), facts_lua);
    }

    // =========================================================================
    // relationships.lua - Link statements
    // =========================================================================
    if !relationships.is_empty() {
        let mut rels_lua = String::new();
        rels_lua.push_str("-- Auto-generated relationship definitions\n");
        rels_lua.push_str("-- These define how tables are connected\n\n");

        for rel in relationships {
            rels_lua.push_str(&format!(
                "link({}.{}, {}.{})\n",
                rel.from_table, rel.from_column, rel.to_table, rel.to_column
            ));
        }

        files.insert("relationships.lua".to_string(), rels_lua);
    }

    files
}

/// Generate a source {} block for a table.
fn generate_source_block(metadata: &TableMetadata, entity_name: Option<&str>) -> String {
    let mut lua = String::new();
    let name = entity_name.unwrap_or(&metadata.name);

    lua.push_str(&format!("source \"{}\" {{\n", name));

    if metadata.schema.is_empty() {
        lua.push_str(&format!("  table = \"{}\",\n", metadata.name));
    } else {
        lua.push_str(&format!("  table = \"{}.{}\",\n", metadata.schema, metadata.name));
    }

    let pk_columns = metadata.primary_key_columns();
    if !pk_columns.is_empty() {
        if pk_columns.len() == 1 {
            lua.push_str(&format!("  primary_key = \"{}\",\n", pk_columns[0]));
        } else {
            let pk_list: Vec<String> = pk_columns.iter().map(|s| format!("\"{}\"", s)).collect();
            lua.push_str(&format!("  primary_key = {{ {} }},\n", pk_list.join(", ")));
        }
    }

    lua.push_str("  columns = {\n");
    for col in &metadata.columns {
        let mantis_type = db_type_to_mantis_type(&col.data_type);
        let nullable = if col.is_nullable { ", nullable = true" } else { "" };
        lua.push_str(&format!(
            "    {} = {{ type = \"{}\"{} }},\n",
            col.name, mantis_type, nullable
        ));
    }
    lua.push_str("  },\n");
    lua.push_str("}\n\n");

    lua
}

/// Generate Lua code from suggestions (sources, dimensions, facts, relationships).
/// DEPRECATED: Use generate_lua_files instead for separate file generation.
fn generate_lua_with_suggestions(
    source_tables: &[TableMetadata],
    source_selections: &[TableSelection],
    dimensions: &[DimensionSuggestion],
    facts: &[FactSuggestion],
    relationships: &[RelationshipSelection],
) -> String {
    let mut lua = String::new();
    lua.push_str("-- Auto-generated Mantis definitions\n");
    lua.push_str("-- Review and customize as needed\n\n");

    // Generate source definitions
    if !source_tables.is_empty() {
        lua.push_str("-- =============================================================================\n");
        lua.push_str("-- Sources\n");
        lua.push_str("-- =============================================================================\n\n");

        for (metadata, selection) in source_tables.iter().zip(source_selections.iter()) {
            let entity_name = selection.entity_name.as_ref().unwrap_or(&metadata.name);
            lua.push_str(&format!("source \"{}\" {{\n", entity_name));

            if metadata.schema.is_empty() {
                lua.push_str(&format!("  table = \"{}\",\n", metadata.name));
            } else {
                lua.push_str(&format!("  table = \"{}.{}\",\n", metadata.schema, metadata.name));
            }

            let pk_columns = metadata.primary_key_columns();
            if !pk_columns.is_empty() {
                if pk_columns.len() == 1 {
                    lua.push_str(&format!("  primary_key = \"{}\",\n", pk_columns[0]));
                } else {
                    let pk_list: Vec<String> = pk_columns.iter().map(|s| format!("\"{}\"", s)).collect();
                    lua.push_str(&format!("  primary_key = {{ {} }},\n", pk_list.join(", ")));
                }
            }

            lua.push_str("  columns = {\n");
            for col in &metadata.columns {
                let mantis_type = db_type_to_mantis_type(&col.data_type);
                let nullable = if col.is_nullable { ", nullable = true" } else { "" };
                lua.push_str(&format!(
                    "    {} = {{ type = \"{}\"{} }},\n",
                    col.name, mantis_type, nullable
                ));
            }
            lua.push_str("  },\n");
            lua.push_str("}\n\n");
        }
    }

    // Generate dimension definitions
    if !dimensions.is_empty() {
        lua.push_str("-- =============================================================================\n");
        lua.push_str("-- Dimensions\n");
        lua.push_str("-- =============================================================================\n\n");

        for dim in dimensions {
            lua.push_str(&format!("dimension \"{}\" {{\n", dim.name));
            lua.push_str(&format!("  target_table = \"{}\",\n", dim.target_table));
            lua.push_str(&format!("  source = \"{}\",\n", dim.source));

            // Columns
            if !dim.columns.is_empty() {
                let col_list: Vec<String> = dim.columns.iter().map(|s| format!("\"{}\"", s)).collect();
                lua.push_str(&format!("  columns = {{ {} }},\n", col_list.join(", ")));
            }

            // Primary key
            if !dim.primary_key.is_empty() {
                if dim.primary_key.len() == 1 {
                    lua.push_str(&format!("  primary_key = {{ \"{}\" }},\n", dim.primary_key[0]));
                } else {
                    let pk_list: Vec<String> = dim.primary_key.iter().map(|s| format!("\"{}\"", s)).collect();
                    lua.push_str(&format!("  primary_key = {{ {} }},\n", pk_list.join(", ")));
                }
            }

            // SCD type
            lua.push_str(&format!("  scd_type = SCD{},\n", dim.scd_type));

            // Materialized
            if dim.materialized {
                lua.push_str("  materialized = true,\n");
            }

            lua.push_str("}\n\n");
        }
    }

    // Generate fact definitions
    if !facts.is_empty() {
        lua.push_str("-- =============================================================================\n");
        lua.push_str("-- Facts\n");
        lua.push_str("-- =============================================================================\n\n");

        for fact in facts {
            lua.push_str(&format!("fact \"{}\" {{\n", fact.name));
            lua.push_str(&format!("  target_table = \"{}\",\n", fact.target_table));

            // Grain
            if !fact.grain.is_empty() {
                let grain_list: Vec<String> = fact
                    .grain
                    .iter()
                    .map(|g| format!("{}.{}", g.source_entity, g.column))
                    .collect();
                lua.push_str(&format!("  grain = {{ {} }},\n", grain_list.join(", ")));
            }

            // Includes (dimension references)
            if !fact.dimension_refs.is_empty() {
                lua.push_str("  include = {\n");
                for dim_ref in &fact.dimension_refs {
                    if dim_ref.include_columns.is_empty() {
                        lua.push_str(&format!("    {} = ALL,\n", dim_ref.dimension));
                    } else {
                        let cols: Vec<String> = dim_ref
                            .include_columns
                            .iter()
                            .map(|s| format!("\"{}\"", s))
                            .collect();
                        lua.push_str(&format!(
                            "    {} = {{ {} }},\n",
                            dim_ref.dimension,
                            cols.join(", ")
                        ));
                    }
                }
                lua.push_str("  },\n");
            }

            // Measures
            if !fact.measures.is_empty() {
                lua.push_str("  measures = {\n");
                for measure in &fact.measures {
                    let agg_fn = match measure.aggregation.as_str() {
                        "sum" => format!("sum \"{}\"", measure.column),
                        "count" => "count()".to_string(),
                        "count_distinct" => format!("count_distinct \"{}\"", measure.column),
                        "avg" => format!("avg \"{}\"", measure.column),
                        "min" => format!("min \"{}\"", measure.column),
                        "max" => format!("max \"{}\"", measure.column),
                        _ => format!("sum \"{}\"", measure.column),
                    };
                    lua.push_str(&format!("    {} = {},\n", measure.name, agg_fn));
                }
                lua.push_str("  },\n");
            }

            // Materialized
            if fact.materialized {
                lua.push_str("  materialized = true,\n");
            }

            lua.push_str("}\n\n");
        }
    }

    // Generate link statements for relationships
    if !relationships.is_empty() {
        lua.push_str("-- =============================================================================\n");
        lua.push_str("-- Relationships\n");
        lua.push_str("-- =============================================================================\n\n");

        for rel in relationships {
            lua.push_str(&format!(
                "link({}.{}, {}.{})\n",
                rel.from_table, rel.from_column, rel.to_table, rel.to_column
            ));
        }
        lua.push('\n');
    }

    lua
}

/// Map database type string to Mantis type.
fn db_type_to_mantis_type(db_type: &str) -> String {
    let lower = db_type.to_lowercase();

    // Boolean
    if lower == "bit" || lower == "bool" || lower == "boolean" {
        return "bool".to_string();
    }

    // Integers
    if lower == "tinyint" {
        return "int8".to_string();
    }
    if lower == "smallint" || lower == "int2" {
        return "int16".to_string();
    }
    if lower == "int" || lower == "integer" || lower == "int4" {
        return "int32".to_string();
    }
    if lower == "bigint" || lower == "int8" {
        return "int64".to_string();
    }

    // Floats
    if lower == "real" || lower == "float4" || lower.starts_with("float") {
        return "float32".to_string();
    }
    if lower == "double" || lower == "float8" || lower.starts_with("double") {
        return "float64".to_string();
    }

    // Decimal/Numeric - parse precision and scale if available
    if lower.starts_with("decimal") || lower.starts_with("numeric") {
        // Try to parse decimal(p,s) or numeric(p,s)
        if let Some(params) = extract_type_params(&lower) {
            let parts: Vec<&str> = params.split(',').collect();
            if parts.len() == 2 {
                let precision = parts[0].trim();
                let scale = parts[1].trim();
                return format!("decimal({}, {})", precision, scale);
            } else if parts.len() == 1 {
                let precision = parts[0].trim();
                return format!("decimal({}, 0)", precision);
            }
        }
        // Default decimal with reasonable precision
        return "decimal(18, 2)".to_string();
    }
    if lower == "money" {
        return "decimal(19, 4)".to_string();
    }
    if lower == "smallmoney" {
        return "decimal(10, 4)".to_string();
    }

    // Strings
    if lower.contains("char") || lower == "text" || lower == "ntext" || lower == "string" {
        return "string".to_string();
    }

    // Date/Time
    if lower == "date" {
        return "date".to_string();
    }
    if lower == "time" {
        return "time".to_string();
    }
    if lower.starts_with("datetime") || lower == "timestamp" || lower == "smalldatetime" {
        return "timestamp".to_string();
    }
    if lower == "datetimeoffset" || lower == "timestamptz" {
        return "timestamptz".to_string();
    }

    // Binary
    if lower.contains("binary") || lower == "image" || lower == "bytea" || lower == "blob" {
        return "binary".to_string();
    }

    // UUID
    if lower == "uniqueidentifier" || lower == "uuid" {
        return "uuid".to_string();
    }

    // JSON
    if lower == "json" || lower == "jsonb" {
        return "json".to_string();
    }

    // Default to string for unknown types
    "string".to_string()
}

/// Extract parameters from a type like "decimal(18, 2)" -> "18, 2"
fn extract_type_params(type_str: &str) -> Option<&str> {
    let start = type_str.find('(')?;
    let end = type_str.find(')')?;
    if end > start + 1 {
        Some(&type_str[start + 1..end])
    } else {
        None
    }
}

// ============================================================================
// Credential API Handlers
// ============================================================================

/// GET /api/credentials - List saved credentials.
///
/// Returns metadata for all saved credentials (no connection strings).
/// Also indicates whether credentials are persistent (using MANTIS_MASTER_KEY).
pub async fn list_credentials(
    State(_state): State<Arc<AppStateWithDb>>,
) -> Json<ListCredentialsResponse> {
    let key_state = crypto::get_master_key();

    let cache = match MetadataCache::open() {
        Ok(c) => c,
        Err(_) => {
            return Json(ListCredentialsResponse {
                credentials: vec![],
                persistent: key_state.is_persistent(),
            });
        }
    };

    let credentials = cache.list_credentials().unwrap_or_default();

    Json(ListCredentialsResponse {
        credentials,
        persistent: key_state.is_persistent(),
    })
}

/// POST /api/credentials - Save a new credential.
///
/// Encrypts the connection string with the master key and stores it.
pub async fn save_credential(
    State(_state): State<Arc<AppStateWithDb>>,
    Json(req): Json<SaveCredentialRequest>,
) -> Json<SaveCredentialResponse> {
    // Validate driver
    if !SUPPORTED_DRIVERS.contains(&req.driver.as_str()) {
        return Json(SaveCredentialResponse {
            success: false,
            id: None,
            error: Some(format!(
                "Unsupported driver '{}'. Supported: {:?}",
                req.driver, SUPPORTED_DRIVERS
            )),
        });
    }

    let key_state = crypto::get_master_key();

    let cache = match MetadataCache::open() {
        Ok(c) => c,
        Err(e) => {
            return Json(SaveCredentialResponse {
                success: false,
                id: None,
                error: Some(format!("Failed to open cache: {}", e)),
            });
        }
    };

    match cache.save_credential(
        key_state.key(),
        &req.driver,
        &req.connection_string,
        req.display_name.as_deref(),
    ) {
        Ok(id) => Json(SaveCredentialResponse {
            success: true,
            id: Some(id),
            error: None,
        }),
        Err(e) => Json(SaveCredentialResponse {
            success: false,
            id: None,
            error: Some(format!("Failed to save credential: {}", e)),
        }),
    }
}

/// DELETE /api/credentials/:id - Delete a saved credential.
pub async fn delete_credential(
    State(_state): State<Arc<AppStateWithDb>>,
    Path(id): Path<String>,
) -> Json<GenericResponse> {
    let cache = match MetadataCache::open() {
        Ok(c) => c,
        Err(e) => {
            return Json(GenericResponse {
                success: false,
                error: Some(format!("Failed to open cache: {}", e)),
            });
        }
    };

    match cache.delete_credential(&id) {
        Ok(deleted) => {
            if deleted {
                Json(GenericResponse {
                    success: true,
                    error: None,
                })
            } else {
                Json(GenericResponse {
                    success: false,
                    error: Some("Credential not found".to_string()),
                })
            }
        }
        Err(e) => Json(GenericResponse {
            success: false,
            error: Some(format!("Failed to delete credential: {}", e)),
        }),
    }
}

/// POST /api/credentials/:id/connect - Connect using a saved credential.
///
/// Decrypts the connection string and establishes a connection.
pub async fn connect_with_credential(
    State(state): State<Arc<AppStateWithDb>>,
    Path(id): Path<String>,
) -> Json<TestConnectionResponse> {
    let key_state = crypto::get_master_key();

    let cache = match MetadataCache::open() {
        Ok(c) => c,
        Err(e) => {
            return Json(TestConnectionResponse {
                success: false,
                error: Some(format!("Failed to open cache: {}", e)),
                database_info: None,
                schemas: None,
            });
        }
    };

    // Get credential metadata to find the driver
    let credentials = match cache.list_credentials() {
        Ok(c) => c,
        Err(e) => {
            return Json(TestConnectionResponse {
                success: false,
                error: Some(format!("Failed to list credentials: {}", e)),
                database_info: None,
                schemas: None,
            });
        }
    };

    let credential = match credentials.iter().find(|c| c.id == id) {
        Some(c) => c,
        None => {
            return Json(TestConnectionResponse {
                success: false,
                error: Some("Credential not found".to_string()),
                database_info: None,
                schemas: None,
            });
        }
    };

    // Decrypt connection string
    let connection_string = match cache.get_credential_connection_string(key_state.key(), &id) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Json(TestConnectionResponse {
                success: false,
                error: Some("Failed to decrypt credential (key may have changed)".to_string()),
                database_info: None,
                schemas: None,
            });
        }
        Err(e) => {
            return Json(TestConnectionResponse {
                success: false,
                error: Some(format!("Failed to decrypt credential: {}", e)),
                database_info: None,
                schemas: None,
            });
        }
    };

    // Validate driver
    if !SUPPORTED_DRIVERS.contains(&credential.driver.as_str()) {
        return Json(TestConnectionResponse {
            success: false,
            error: Some(format!(
                "Unsupported driver '{}'. Supported: {:?}",
                credential.driver, SUPPORTED_DRIVERS
            )),
            database_info: None,
            schemas: None,
        });
    }

    // Connect using existing logic
    do_set_connection(state, &credential.driver, &connection_string, None).await
}

/// Internal helper to set a connection and return the response.
///
/// This is shared between `set_connection` and `connect_with_credential`.
async fn do_set_connection(
    state: Arc<AppStateWithDb>,
    driver: &str,
    connection_string: &str,
    default_schema: Option<String>,
) -> Json<TestConnectionResponse> {
    // Spawn worker
    let client = match WorkerClient::spawn_with_settings(&state.settings).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return Json(TestConnectionResponse {
                success: false,
                error: Some(format!("Failed to start worker: {}", e)),
                database_info: None,
                schemas: None,
            });
        }
    };

    // Test the connection
    let provider = WorkerMetadataProvider::new(client.clone(), driver, connection_string);

    match provider.get_database_info().await {
        Ok(info) => {
            let schemas = provider.list_schemas().await.ok();
            let resolved_default_schema = default_schema.or(info.default_schema.clone());

            // Store the connection
            let mut conn = state.db_connection.write().await;
            conn.config = Some(ActiveConnection {
                driver: driver.to_string(),
                connection_string: connection_string.to_string(),
                client,
                default_schema: resolved_default_schema.clone(),
            });

            Json(TestConnectionResponse {
                success: true,
                error: None,
                database_info: Some(DatabaseInfoResponse {
                    product_name: info.product_name,
                    product_version: info.product_version,
                    database_name: info.database_name,
                    default_schema: resolved_default_schema,
                }),
                schemas,
            })
        }
        Err(e) => Json(TestConnectionResponse {
            success: false,
            error: Some(format!("Connection failed: {}", e)),
            database_info: None,
            schemas: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_type_mapping() {
        assert_eq!(db_type_to_mantis_type("int"), "int32");
        assert_eq!(db_type_to_mantis_type("bigint"), "int64");
        assert_eq!(db_type_to_mantis_type("varchar(255)"), "string");
        assert_eq!(db_type_to_mantis_type("decimal(18,2)"), "decimal(18, 2)");
        assert_eq!(db_type_to_mantis_type("numeric(10,4)"), "decimal(10, 4)");
        assert_eq!(db_type_to_mantis_type("decimal"), "decimal(18, 2)");
        assert_eq!(db_type_to_mantis_type("money"), "decimal(19, 4)");
        assert_eq!(db_type_to_mantis_type("datetime2"), "timestamp");
        assert_eq!(db_type_to_mantis_type("bit"), "bool");
        assert_eq!(db_type_to_mantis_type("uniqueidentifier"), "uuid");
    }

    #[test]
    fn test_extract_type_params() {
        assert_eq!(extract_type_params("decimal(18, 2)"), Some("18, 2"));
        assert_eq!(extract_type_params("varchar(255)"), Some("255"));
        assert_eq!(extract_type_params("int"), None);
        assert_eq!(extract_type_params("decimal()"), None);
    }

    #[test]
    fn test_supported_drivers() {
        assert!(SUPPORTED_DRIVERS.contains(&"mssql"));
        assert!(SUPPORTED_DRIVERS.contains(&"duckdb"));
        assert!(!SUPPORTED_DRIVERS.contains(&"postgres"));
    }
}
