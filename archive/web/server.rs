//! Axum web server for Mantis Playground
//!
//! Serves the embedded UI and provides API endpoints for model validation
//! and database introspection.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode, Uri},
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use rust_embed::{Embed, RustEmbed};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use crate::config::Settings;
use crate::dialect::Dialect;
use crate::model::loader::{
    extract_symbols_regex, load_model_from_str, load_model_from_str_lenient, ParseError,
};
use crate::model::Model;
use crate::semantic::column_lineage::{ColumnLineageGraph, ColumnRef, LineageType};
use crate::semantic::QueryExecutor;

use super::database::{self, new_shared_connection, AppStateWithDb};

/// Embedded static files from the UI build
#[derive(RustEmbed)]
#[folder = "ui/.output/public"]
struct Assets;

/// Application state shared across handlers
pub struct AppState {
    /// Directory containing .lua model files
    pub model_dir: PathBuf,
}

/// Build the axum router with all routes
pub fn router(state: Arc<AppStateWithDb>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // API routes
        .route("/api/validate", post(validate_model))
        .route("/api/symbols", post(extract_symbols))
        .route("/api/lineage", post(get_lineage))
        .route("/api/query", post(execute_query))
        .route("/api/models", get(list_models))
        .route("/api/models/{name}", get(get_model))
        .route("/api/models/{name}", put(save_model))
        .route("/api/models/{name}", delete(delete_model))
        // Database introspection routes
        .route("/api/connection", get(database::get_connection_status))
        .route("/api/connection", post(database::set_connection))
        .route("/api/connection", delete(database::disconnect))
        .route("/api/connection/test", post(database::test_connection))
        .route("/api/database/schemas", get(database::list_schemas))
        .route("/api/database/tables/{schema}", get(database::list_tables))
        .route(
            "/api/database/table/{schema}/{table}",
            get(database::get_table),
        )
        .route("/api/generate/sources", post(database::generate_sources))
        .route(
            "/api/generate/relationships",
            post(database::detect_relationships),
        )
        .route(
            "/api/generate/with-relationships",
            post(database::generate_with_relationships),
        )
        .route("/api/generate/classify", post(database::classify_tables))
        .route(
            "/api/generate/with-suggestions",
            post(database::generate_with_suggestions),
        )
        // Credential storage routes
        .route("/api/credentials", get(database::list_credentials))
        .route("/api/credentials", post(database::save_credential))
        .route("/api/credentials/{id}", delete(database::delete_credential))
        .route(
            "/api/credentials/{id}/connect",
            post(database::connect_with_credential),
        )
        // Static files (SPA fallback)
        .fallback(static_handler)
        .layer(cors)
        .with_state(state)
}

/// Start the web server
pub async fn serve(
    model_dir: PathBuf,
    port: u16,
    open_browser: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load settings for worker configuration
    let settings = Settings::load().unwrap_or_default();

    let state = Arc::new(AppStateWithDb {
        model_dir: model_dir.clone(),
        db_connection: new_shared_connection(),
        settings,
    });
    let app = router(state);

    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    println!("🦎 Mantis Playground");
    println!("   URL: http://localhost:{}", port);
    println!("   Models: {}", model_dir.display());
    println!();
    println!("   Press Ctrl+C to stop");

    if open_browser {
        let _ = open::that(format!("http://localhost:{}", port));
    }

    axum::serve(listener, app).await?;
    Ok(())
}

// ============================================================================
// API Handlers
// ============================================================================

#[derive(Deserialize)]
struct ValidateRequest {
    content: String,
}

#[derive(Serialize)]
struct ValidationResult {
    valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<Model>,
}

/// POST /api/validate - Validate Lua model content
async fn validate_model(Json(req): Json<ValidateRequest>) -> Json<ValidationResult> {
    match load_model_from_str(&req.content, "editor.lua") {
        Ok(model) => Json(ValidationResult {
            valid: true,
            error: None,
            model: Some(model),
        }),
        Err(e) => Json(ValidationResult {
            valid: false,
            error: Some(e.to_string()),
            model: None,
        }),
    }
}

// ============================================================================
// Symbol Extraction API
// ============================================================================

#[derive(Deserialize)]
struct SymbolsRequest {
    content: String,
}

/// A symbol extracted from the model.
#[derive(Serialize)]
struct SymbolInfo {
    name: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<usize>,
    status: String, // "complete", "error", "syntax_fallback"
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct SymbolsResponse {
    /// True if Lua executed without syntax errors
    parse_successful: bool,
    /// Lua syntax/runtime error if any
    #[serde(skip_serializing_if = "Option::is_none")]
    lua_error: Option<String>,
    /// Extracted symbols (may be partial)
    symbols: Vec<SymbolInfo>,
    /// Per-entity parse errors
    errors: Vec<ParseError>,
    /// The partial model (if any parsing succeeded)
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<Model>,
}

/// POST /api/symbols - Extract symbols from Lua model content (lenient mode)
///
/// Unlike /api/validate, this endpoint:
/// - Continues after entity-level parse errors
/// - Returns partial results even when some entities fail
/// - Falls back to regex extraction on Lua syntax errors
async fn extract_symbols(Json(req): Json<SymbolsRequest>) -> Json<SymbolsResponse> {
    // Try lenient load first
    let result = load_model_from_str_lenient(&req.content, "editor.lua");

    // Check if it was a Lua syntax error (prevents any execution)
    let is_syntax_error = result
        .lua_error
        .as_ref()
        .map(|e| e.contains("syntax error") || e.contains("unexpected") || e.contains("expected"))
        .unwrap_or(false);

    // If syntax error and no symbols extracted, fall back to regex
    if is_syntax_error
        && result.model.sources.is_empty()
        && result.model.facts.is_empty()
        && result.model.dimensions.is_empty()
    {
        let basic_symbols = extract_symbols_regex(&req.content);
        let symbols: Vec<SymbolInfo> = basic_symbols
            .into_iter()
            .map(|s| SymbolInfo {
                name: s.name,
                kind: s.kind,
                line: Some(s.line),
                status: "syntax_fallback".to_string(),
                error: None,
            })
            .collect();

        return Json(SymbolsResponse {
            parse_successful: false,
            lua_error: result.lua_error,
            symbols,
            errors: vec![],
            model: None,
        });
    }

    // Build symbols from the partial model
    let mut symbols = Vec::new();

    // Sources
    for (name, _source) in &result.model.sources {
        symbols.push(SymbolInfo {
            name: name.clone(),
            kind: "source".to_string(),
            line: None,
            status: "complete".to_string(),
            error: None,
        });
    }

    // Facts
    for (name, _fact) in &result.model.facts {
        symbols.push(SymbolInfo {
            name: name.clone(),
            kind: "fact".to_string(),
            line: None,
            status: "complete".to_string(),
            error: None,
        });
    }

    // Dimensions
    for (name, _dim) in &result.model.dimensions {
        symbols.push(SymbolInfo {
            name: name.clone(),
            kind: "dimension".to_string(),
            line: None,
            status: "complete".to_string(),
            error: None,
        });
    }

    // Tables
    for (name, _table) in &result.model.tables {
        symbols.push(SymbolInfo {
            name: name.clone(),
            kind: "table".to_string(),
            line: None,
            status: "complete".to_string(),
            error: None,
        });
    }

    // Queries
    for (name, _query) in &result.model.queries {
        symbols.push(SymbolInfo {
            name: name.clone(),
            kind: "query".to_string(),
            line: None,
            status: "complete".to_string(),
            error: None,
        });
    }

    // Add error symbols for entities that failed
    for err in &result.parse_errors {
        symbols.push(SymbolInfo {
            name: err.entity_name.clone(),
            kind: err.entity_type.clone(),
            line: None,
            status: "error".to_string(),
            error: Some(err.message.clone()),
        });
    }

    let has_model = !result.model.sources.is_empty()
        || !result.model.facts.is_empty()
        || !result.model.dimensions.is_empty();

    Json(SymbolsResponse {
        parse_successful: result.lua_error.is_none(),
        lua_error: result.lua_error,
        symbols,
        errors: result.parse_errors,
        model: if has_model { Some(result.model) } else { None },
    })
}

// ============================================================================
// Query Execution API
// ============================================================================

#[derive(Deserialize)]
struct QueryRequest {
    /// The Lua model content to execute against
    content: String,
    /// Name of a query defined in the model
    query_name: String,
    /// SQL dialect for output: "duckdb", "postgres", "tsql", "mysql", "snowflake", "bigquery"
    #[serde(default = "default_dialect")]
    dialect: String,
}

fn default_dialect() -> String {
    "duckdb".to_string()
}

#[derive(Serialize)]
struct QueryResult {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// List of available query names in the model
    available_queries: Vec<String>,
    /// The dialect used
    dialect: String,
}

/// Parse dialect string to Dialect enum
fn parse_dialect(s: &str) -> Dialect {
    match s.to_lowercase().as_str() {
        "postgres" | "postgresql" => Dialect::Postgres,
        "tsql" | "mssql" | "sqlserver" => Dialect::TSql,
        "mysql" => Dialect::MySql,
        _ => Dialect::DuckDb, // Default to DuckDB
    }
}

/// POST /api/query - Execute a named query and return SQL
async fn execute_query(Json(req): Json<QueryRequest>) -> Json<QueryResult> {
    // Step 1: Load and parse the model
    let model = match load_model_from_str(&req.content, "editor.lua") {
        Ok(m) => m,
        Err(e) => {
            return Json(QueryResult {
                success: false,
                sql: None,
                error: Some(format!("Model error: {}", e)),
                available_queries: vec![],
                dialect: req.dialect,
            });
        }
    };

    // Collect available query names
    let available_queries: Vec<String> = model.queries.keys().cloned().collect();

    // Step 2: Create QueryExecutor
    let executor = match QueryExecutor::new(model) {
        Ok(e) => e,
        Err(e) => {
            return Json(QueryResult {
                success: false,
                sql: None,
                error: Some(format!("Executor error: {}", e)),
                available_queries,
                dialect: req.dialect,
            });
        }
    };

    // Step 3: Parse dialect
    let dialect = parse_dialect(&req.dialect);

    // Step 4: Execute the named query
    match executor.query_to_sql(&req.query_name, dialect) {
        Ok(sql) => Json(QueryResult {
            success: true,
            sql: Some(sql),
            error: None,
            available_queries,
            dialect: req.dialect,
        }),
        Err(e) => Json(QueryResult {
            success: false,
            sql: None,
            error: Some(e.to_string()),
            available_queries,
            dialect: req.dialect,
        }),
    }
}

// ============================================================================
// Column Lineage API
// ============================================================================

#[derive(Serialize)]
struct LineageNode {
    id: String,
    entity: String,
    column: String,
    entity_type: String,
}

#[derive(Serialize)]
struct LineageEdge {
    from: String,
    to: String,
    lineage_type: String,
}

#[derive(Serialize)]
struct LineageResult {
    valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    nodes: Vec<LineageNode>,
    edges: Vec<LineageEdge>,
}

fn lineage_type_to_string(lt: &LineageType) -> &'static str {
    match lt {
        LineageType::Passthrough => "passthrough",
        LineageType::Transform => "transform",
        LineageType::Aggregate => "aggregate",
        LineageType::GroupBy => "group_by",
        LineageType::JoinKey => "join_key",
        LineageType::WindowPartition => "window_partition",
        LineageType::WindowOrder => "window_order",
        LineageType::Filter => "filter",
    }
}

/// Determine the entity type based on which collection it belongs to in the model
fn get_entity_type(entity_name: &str, model: &Model) -> &'static str {
    if model.sources.contains_key(entity_name) {
        "source"
    } else if model.tables.contains_key(entity_name) {
        "table"
    } else if model.facts.contains_key(entity_name) {
        "fact"
    } else if model.dimensions.contains_key(entity_name) {
        "dimension"
    } else if model.reports.contains_key(entity_name) {
        "report"
    } else if model.pivot_reports.contains_key(entity_name) {
        "report"
    } else {
        "unknown"
    }
}

/// POST /api/lineage - Get column-level lineage graph
async fn get_lineage(Json(req): Json<ValidateRequest>) -> Json<LineageResult> {
    match load_model_from_str(&req.content, "editor.lua") {
        Ok(model) => {
            let graph = ColumnLineageGraph::from_model(&model);

            // Collect all unique nodes
            let mut nodes = Vec::new();
            let mut seen_nodes = std::collections::HashSet::new();

            // Get all source columns (nodes with no upstream)
            for col in graph.source_columns() {
                let id = format!("{}.{}", col.entity, col.column);
                if seen_nodes.insert(id.clone()) {
                    nodes.push(LineageNode {
                        id,
                        entity: col.entity.clone(),
                        column: col.column.clone(),
                        entity_type: get_entity_type(&col.entity, &model).to_string(),
                    });
                }
            }

            // Get all terminal columns
            for col in graph.terminal_columns() {
                let id = format!("{}.{}", col.entity, col.column);
                if seen_nodes.insert(id.clone()) {
                    nodes.push(LineageNode {
                        id,
                        entity: col.entity.clone(),
                        column: col.column.clone(),
                        entity_type: get_entity_type(&col.entity, &model).to_string(),
                    });
                }
            }

            // Build edges by traversing the graph
            let mut edges = Vec::new();
            let mut seen_edges = std::collections::HashSet::new();
            let mut visited_cols = std::collections::HashSet::new();

            for col in graph.terminal_columns() {
                collect_edges_recursive(
                    &graph,
                    &model,
                    &col,
                    &mut edges,
                    &mut seen_edges,
                    &mut seen_nodes,
                    &mut nodes,
                    &mut visited_cols,
                );
            }

            Json(LineageResult {
                valid: true,
                error: None,
                nodes,
                edges,
            })
        }
        Err(e) => Json(LineageResult {
            valid: false,
            error: Some(e.to_string()),
            nodes: vec![],
            edges: vec![],
        }),
    }
}

fn collect_edges_recursive(
    graph: &ColumnLineageGraph,
    model: &Model,
    col: &ColumnRef,
    edges: &mut Vec<LineageEdge>,
    seen_edges: &mut std::collections::HashSet<String>,
    seen_nodes: &mut std::collections::HashSet<String>,
    nodes: &mut Vec<LineageNode>,
    visited_cols: &mut std::collections::HashSet<String>,
) {
    let col_id = format!("{}.{}", col.entity, col.column);

    // Prevent infinite recursion
    if !visited_cols.insert(col_id.clone()) {
        return;
    }

    let deps = graph.direct_dependencies(col);

    for (dep, lineage_type) in deps {
        let from_id = format!("{}.{}", dep.entity, dep.column);
        let edge_key = format!(
            "{}->{}:{}",
            from_id,
            col_id,
            lineage_type_to_string(&lineage_type)
        );

        // Add node if not seen
        if seen_nodes.insert(from_id.clone()) {
            nodes.push(LineageNode {
                id: from_id.clone(),
                entity: dep.entity.clone(),
                column: dep.column.clone(),
                entity_type: get_entity_type(&dep.entity, model).to_string(),
            });
        }

        // Add edge if not seen
        if seen_edges.insert(edge_key) {
            edges.push(LineageEdge {
                from: from_id,
                to: col_id.clone(),
                lineage_type: lineage_type_to_string(&lineage_type).to_string(),
            });
        }

        // Recurse upstream
        collect_edges_recursive(
            graph,
            model,
            &dep,
            edges,
            seen_edges,
            seen_nodes,
            nodes,
            visited_cols,
        );
    }
}

/// GET /api/models - List available .lua files
async fn list_models(
    State(state): State<Arc<AppStateWithDb>>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let models = std::fs::read_dir(&state.model_dir)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|ext| ext == "lua")
                .unwrap_or(false)
        })
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();

    Ok(Json(models))
}

/// GET /api/models/:name - Get model file content
async fn get_model(
    State(state): State<Arc<AppStateWithDb>>,
    Path(name): Path<String>,
) -> Result<String, StatusCode> {
    eprintln!("get_model called with {}", name);
    // Sanitize path to prevent directory traversal
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err(StatusCode::BAD_REQUEST);
    }

    let path = state.model_dir.join(name);
    std::fs::read_to_string(path).map_err(|_| StatusCode::NOT_FOUND)
}

/// PUT /api/models/:name - Save model file content
async fn save_model(
    State(state): State<Arc<AppStateWithDb>>,
    Path(name): Path<String>,
    body: String,
) -> StatusCode {
    // Sanitize path
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return StatusCode::BAD_REQUEST;
    }

    let path = state.model_dir.join(&name);
    match std::fs::write(path, body) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// DELETE /api/models/:name - Delete a model file
async fn delete_model(
    State(state): State<Arc<AppStateWithDb>>,
    Path(name): Path<String>,
) -> StatusCode {
    // Sanitize path
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return StatusCode::BAD_REQUEST;
    }

    let path = state.model_dir.join(&name);
    match std::fs::remove_file(path) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::NOT_FOUND,
    }
}

// ============================================================================
// Static File Handler
// ============================================================================

/// Serve static files with SPA fallback
async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try exact path first
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(header::CONTENT_TYPE, mime.as_ref())],
                content.data.into_owned(),
            )
                .into_response()
        }
        None => {
            // SPA fallback - serve index.html for client-side routing
            match Assets::get("index.html") {
                Some(content) => (
                    [(header::CONTENT_TYPE, "text/html")],
                    content.data.into_owned(),
                )
                    .into_response(),
                None => (StatusCode::NOT_FOUND, "Not found").into_response(),
            }
        }
    }
}
