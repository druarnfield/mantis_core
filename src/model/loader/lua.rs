//! Lua model loader.
//!
//! Parses Lua model files with the following DSL:
//!
//! ```lua
//! source "orders" {
//!     table = "raw.orders",
//!     columns = {
//!         order_id = { type = "int64", primary_key = true },
//!     },
//! }
//!
//! relationship {
//!     from = "orders.customer_id",
//!     to = "customers.customer_id",
//!     cardinality = "many_to_one",
//! }
//!
//! fact "fact_orders" { ... }
//! dimension "dim_customers" { ... }
//!
//! import "other_file.lua"
//! ```

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use mlua::{Lua, Result as LuaResult, Table, Value};

use super::{sql_expr, LoadError, LoadResult};
use crate::model::table::{FromClause, TableDefinition, TableTypeLabel, UnionType};
use crate::model::{
    AggregationType,
    BinaryOp,
    Cardinality,
    ChangeTracking,
    ColumnDef,
    ColumnSelection,
    DataType,
    // Role-playing dimension types
    DateConfig,
    DedupConfig,
    DedupKeep,
    DimensionDefinition,
    DimensionInclude,
    DimensionRole,
    // New types for expression parsing
    Expr,
    FactDefinition,
    FrameBound,
    FrameKind,
    Func,
    GrainColumn,
    GrainColumns,
    Literal,
    MaterializationStrategy,
    MeasureDefinition,
    MeasureRef,
    Model,
    NullsOrder,
    OrderByExpr,
    PivotColumns,
    PivotReport,
    PivotSort,
    PivotValue,
    // Query definition types
    QueryDefinition,
    QueryFilter,
    QueryFilterOp,
    QueryFilterValue,
    QueryOrderBy,
    QuerySelect,
    RefreshDelta,
    Relationship,
    // Report layer types
    Report,
    ReportDefaults,
    ReportMaterialization,
    ReportTableType,
    SCDType,
    SortDir,
    SortDirection,
    SourceColumn,
    SourceEntity,
    TotalsConfig,
    UnaryOp,
    WindowColumnDef,
    WindowFrame,
    WindowFunc,
};

/// Lua model loader.
pub struct LuaLoader;

impl LuaLoader {
    /// The prelude is loaded before user code to provide helper functions and constants.
    const PRELUDE: &'static str = include_str!("prelude.lua");
}

/// Shared state during model loading.
struct LoaderState {
    model: Model,
    imported_files: HashSet<PathBuf>,
    base_path: PathBuf,
    /// Errors collected during lenient parsing (entity-level errors that don't stop execution)
    parse_errors: Vec<ParseError>,
}

/// An error that occurred while parsing a specific entity.
/// In lenient mode, these are collected rather than stopping execution.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParseError {
    pub entity_type: String,
    pub entity_name: String,
    pub message: String,
}

/// Result of lenient model loading - returns partial model even with errors.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LenientLoadResult {
    /// The (possibly partial) model
    pub model: Model,
    /// Entity-level parse errors that were encountered
    pub parse_errors: Vec<ParseError>,
    /// Lua syntax/runtime error if execution failed
    pub lua_error: Option<String>,
}

impl LuaLoader {
    /// Load a model from a Lua file.
    pub fn load(path: &Path) -> LoadResult<Model> {
        let content = std::fs::read_to_string(path)?;
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let base_path = canonical.parent().unwrap_or(Path::new(".")).to_path_buf();

        Self::load_with_state(&content, &canonical, base_path)
    }

    /// Load a model from a Lua string.
    pub fn load_from_str(content: &str, filename: &str) -> LoadResult<Model> {
        let path = PathBuf::from(filename);
        let base_path = path.parent().unwrap_or(Path::new(".")).to_path_buf();

        Self::load_with_state(content, &path, base_path)
    }

    /// Load a model from a Lua string in lenient mode.
    ///
    /// Unlike `load_from_str`, this function:
    /// - Continues execution after entity-level parse errors
    /// - Returns a partial model with whatever entities parsed successfully
    /// - Collects all parse errors for reporting
    /// - Still fails on Lua syntax errors, but returns what was parsed before the error
    pub fn load_from_str_lenient(content: &str, filename: &str) -> LenientLoadResult {
        let path = PathBuf::from(filename);
        let base_path = path.parent().unwrap_or(Path::new(".")).to_path_buf();

        Self::load_with_state_lenient(content, &path, base_path)
    }

    fn load_with_state_lenient(
        content: &str,
        path: &Path,
        base_path: PathBuf,
    ) -> LenientLoadResult {
        let lua = Lua::new();

        let state = Rc::new(RefCell::new(LoaderState {
            model: Model::new(),
            imported_files: HashSet::new(),
            base_path,
            parse_errors: Vec::new(),
        }));

        // Mark current file as imported
        state.borrow_mut().imported_files.insert(path.to_path_buf());

        // Register global functions (lenient mode)
        if let Err(e) = Self::register_globals_lenient(&lua, Rc::clone(&state), path) {
            return LenientLoadResult {
                model: state.borrow().model.clone(),
                parse_errors: state.borrow().parse_errors.clone(),
                lua_error: Some(format!("Failed to register globals: {}", e)),
            };
        }

        // Load the prelude
        if let Err(e) = lua
            .load(Self::PRELUDE)
            .set_name("mantis://prelude.lua")
            .exec()
        {
            return LenientLoadResult {
                model: state.borrow().model.clone(),
                parse_errors: state.borrow().parse_errors.clone(),
                lua_error: Some(format!("Error in prelude: {}", e)),
            };
        }

        // Execute the user's Lua code - capture error but don't fail
        let lua_error = match lua
            .load(content)
            .set_name(path.display().to_string())
            .exec()
        {
            Ok(()) => None,
            Err(e) => Some(e.to_string()),
        };

        // Extract the model and errors
        let borrowed = state.borrow();
        LenientLoadResult {
            model: borrowed.model.clone(),
            parse_errors: borrowed.parse_errors.clone(),
            lua_error,
        }
    }

    fn load_with_state(content: &str, path: &Path, base_path: PathBuf) -> LoadResult<Model> {
        let lua = Lua::new();

        let state = Rc::new(RefCell::new(LoaderState {
            model: Model::new(),
            imported_files: HashSet::new(),
            base_path,
            parse_errors: Vec::new(),
        }));

        // Mark current file as imported
        state.borrow_mut().imported_files.insert(path.to_path_buf());

        // Register global functions
        Self::register_globals(&lua, Rc::clone(&state), path).map_err(|e| LoadError::Lua {
            file: path.display().to_string(),
            message: e.to_string(),
        })?;

        // Load the prelude (helper functions, type constants, etc.)
        lua.load(Self::PRELUDE)
            .set_name("mantis://prelude.lua")
            .exec()
            .map_err(|e| LoadError::Lua {
                file: "prelude.lua".to_string(),
                message: format!("Error in prelude: {}", e),
            })?;

        // Execute the user's Lua code
        lua.load(content)
            .set_name(path.display().to_string())
            .exec()
            .map_err(|e| LoadError::Lua {
                file: path.display().to_string(),
                message: e.to_string(),
            })?;

        // Extract the model (clone since closures still hold Rc references)
        let model = state.borrow().model.clone();

        // Validate the model
        model.validate()?;

        Ok(model)
    }

    fn register_globals(
        lua: &Lua,
        state: Rc<RefCell<LoaderState>>,
        current_file: &Path,
    ) -> LuaResult<()> {
        let globals = lua.globals();

        // source("name"):from("schema.table"):columns({...}) - chained syntax
        let state_clone = Rc::clone(&state);
        lua.set_app_data(Rc::clone(&state_clone));
        let source_fn =
            lua.create_function(move |lua, name: String| Self::create_source_builder(lua, name))?;
        globals.set("source", source_fn)?;

        // relationship { ... }
        let state_clone = Rc::clone(&state);
        let relationship_fn = lua.create_function(move |_, table: Table| {
            let rel = parse_relationship(&table)?;
            state_clone.borrow_mut().model.add_relationship(rel);
            Ok(())
        })?;
        globals.set("relationship", relationship_fn)?;

        // fact("name"):target("table"):grain({...}):measure(...) - chained syntax
        let fact_fn =
            lua.create_function(move |lua, name: String| Self::create_fact_builder(lua, name))?;
        globals.set("fact", fact_fn)?;

        // dimension("name"):target("table"):from("entity") - chained syntax
        let dimension_fn = lua
            .create_function(move |lua, name: String| Self::create_dimension_builder(lua, name))?;
        globals.set("dimension", dimension_fn)?;

        // report "name" { ... }
        let state_clone = Rc::clone(&state);
        let report_fn = lua.create_function(move |lua, name: String| {
            let state = Rc::clone(&state_clone);
            let inner = lua.create_function(move |_, table: Table| {
                let report = parse_report(&name, &table)?;
                state.borrow_mut().model.add_report(report);
                Ok(())
            })?;
            Ok(inner)
        })?;
        globals.set("report", report_fn)?;

        // pivot_report "name" { ... }
        let state_clone = Rc::clone(&state);
        let pivot_report_fn = lua.create_function(move |lua, name: String| {
            let state = Rc::clone(&state_clone);
            let inner = lua.create_function(move |_, table: Table| {
                let pivot_report = parse_pivot_report(&name, &table)?;
                state.borrow_mut().model.add_pivot_report(pivot_report);
                Ok(())
            })?;
            Ok(inner)
        })?;
        globals.set("pivot_report", pivot_report_fn)?;

        // query "name" { ... }
        let state_clone = Rc::clone(&state);
        let query_fn = lua.create_function(move |lua, name: String| {
            let state = Rc::clone(&state_clone);
            let inner = lua.create_function(move |_, table: Table| {
                let query = parse_query(&name, &table)?;
                state.borrow_mut().model.add_query(query);
                Ok(())
            })?;
            Ok(inner)
        })?;
        globals.set("query", query_fn)?;

        // table("name", { ... }) - unified table syntax
        // Save Lua's built-in table library before overwriting
        let lua_table_lib: Value = globals.get("table")?;
        globals.set("_lua_table", lua_table_lib)?;

        let state_clone = Rc::clone(&state);
        let table_fn = lua.create_function(move |lua, args: (String, Table)| {
            let name = args.0;
            let config = args.1;

            let state = lua
                .app_data_ref::<Rc<RefCell<LoaderState>>>()
                .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                .clone();

            match Self::parse_table(&name, &config, lua) {
                Ok(table) => {
                    state.borrow_mut().model.add_table(table);
                }
                Err(e) => {
                    return Err(e);
                }
            }

            // Create global ref for entity.column syntax (e.g., orders.customer_id)
            let globals = lua.globals();
            let ref_fn: mlua::Function = globals.get("ref")?;
            let entity_ref: mlua::Value = ref_fn.call(name.clone())?;
            globals.set(name.clone(), entity_ref)?;

            // Return a table builder for chaining
            Self::create_table_builder(lua, name)
        })?;
        // Store state in app_data for access in closures
        lua.set_app_data(Rc::clone(&state_clone));
        globals.set("table", table_fn)?;

        // import "path"
        let state_clone = Rc::clone(&state);
        let _current_file = current_file.to_path_buf();
        let import_fn = lua.create_function(move |lua, import_path: String| {
            let base = state_clone.borrow().base_path.clone();
            let resolved = resolve_import_path(&base, &import_path);

            // Check for circular import
            if state_clone.borrow().imported_files.contains(&resolved) {
                // Already imported, skip silently (not an error)
                return Ok(());
            }

            // Mark as imported
            state_clone
                .borrow_mut()
                .imported_files
                .insert(resolved.clone());

            // Read and execute the file
            let content = std::fs::read_to_string(&resolved).map_err(|e| {
                mlua::Error::external(format!("Failed to import '{}': {}", import_path, e))
            })?;

            lua.load(&content)
                .set_name(resolved.display().to_string())
                .exec()
                .map_err(|e| {
                    mlua::Error::external(format!(
                        "Error in imported file '{}': {}",
                        import_path, e
                    ))
                })?;

            Ok(())
        })?;
        globals.set("import", import_fn)?;

        Ok(())
    }

    /// Register global DSL functions in lenient mode.
    /// Entity parse errors are collected rather than propagating.
    fn register_globals_lenient(
        lua: &Lua,
        state: Rc<RefCell<LoaderState>>,
        _current_file: &Path,
    ) -> LuaResult<()> {
        let globals = lua.globals();

        // source("name"):from("schema.table"):columns({...}) - lenient mode with chained syntax
        // Store state in app_data for builder closures
        lua.set_app_data(Rc::clone(&state));
        let source_fn =
            lua.create_function(move |lua, name: String| Self::create_source_builder(lua, name))?;
        globals.set("source", source_fn)?;

        // relationship { ... } - lenient mode
        let state_clone = Rc::clone(&state);
        let relationship_fn = lua.create_function(move |_, table: Table| {
            match parse_relationship(&table) {
                Ok(rel) => {
                    state_clone.borrow_mut().model.add_relationship(rel);
                }
                Err(e) => {
                    state_clone.borrow_mut().parse_errors.push(ParseError {
                        entity_type: "relationship".to_string(),
                        entity_name: "(anonymous)".to_string(),
                        message: e.to_string(),
                    });
                }
            }
            Ok(())
        })?;
        globals.set("relationship", relationship_fn)?;

        // fact("name"):target("table"):grain({...}) - lenient mode with chained syntax
        let fact_fn =
            lua.create_function(move |lua, name: String| Self::create_fact_builder(lua, name))?;
        globals.set("fact", fact_fn)?;

        // dimension("name"):target("table"):from("entity") - lenient mode with chained syntax
        let dimension_fn = lua
            .create_function(move |lua, name: String| Self::create_dimension_builder(lua, name))?;
        globals.set("dimension", dimension_fn)?;

        // report "name" { ... } - lenient mode
        let state_clone = Rc::clone(&state);
        let report_fn = lua.create_function(move |lua, name: String| {
            let state = Rc::clone(&state_clone);
            let inner = lua.create_function(move |_, table: Table| {
                match parse_report(&name, &table) {
                    Ok(report) => {
                        state.borrow_mut().model.add_report(report);
                    }
                    Err(e) => {
                        state.borrow_mut().parse_errors.push(ParseError {
                            entity_type: "report".to_string(),
                            entity_name: name.clone(),
                            message: e.to_string(),
                        });
                    }
                }
                Ok(())
            })?;
            Ok(inner)
        })?;
        globals.set("report", report_fn)?;

        // pivot_report "name" { ... } - lenient mode
        let state_clone = Rc::clone(&state);
        let pivot_report_fn = lua.create_function(move |lua, name: String| {
            let state = Rc::clone(&state_clone);
            let inner = lua.create_function(move |_, table: Table| {
                match parse_pivot_report(&name, &table) {
                    Ok(pivot_report) => {
                        state.borrow_mut().model.add_pivot_report(pivot_report);
                    }
                    Err(e) => {
                        state.borrow_mut().parse_errors.push(ParseError {
                            entity_type: "pivot_report".to_string(),
                            entity_name: name.clone(),
                            message: e.to_string(),
                        });
                    }
                }
                Ok(())
            })?;
            Ok(inner)
        })?;
        globals.set("pivot_report", pivot_report_fn)?;

        // query "name" { ... } - lenient mode
        let state_clone = Rc::clone(&state);
        let query_fn = lua.create_function(move |lua, name: String| {
            let state = Rc::clone(&state_clone);
            let inner = lua.create_function(move |_, table: Table| {
                match parse_query(&name, &table) {
                    Ok(query) => {
                        state.borrow_mut().model.add_query(query);
                    }
                    Err(e) => {
                        state.borrow_mut().parse_errors.push(ParseError {
                            entity_type: "query".to_string(),
                            entity_name: name.clone(),
                            message: e.to_string(),
                        });
                    }
                }
                Ok(())
            })?;
            Ok(inner)
        })?;
        globals.set("query", query_fn)?;

        // table("name", { ... }) - unified table syntax (lenient mode)
        // Save Lua's built-in table library before overwriting
        let lua_table_lib: Value = globals.get("table")?;
        globals.set("_lua_table", lua_table_lib)?;

        let state_clone = Rc::clone(&state);
        let table_fn = lua.create_function(move |lua, args: (String, Table)| {
            let name = args.0;
            let config = args.1;

            let state = lua
                .app_data_ref::<Rc<RefCell<LoaderState>>>()
                .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                .clone();

            match Self::parse_table(&name, &config, lua) {
                Ok(table) => {
                    state.borrow_mut().model.add_table(table);
                }
                Err(e) => {
                    state.borrow_mut().parse_errors.push(ParseError {
                        entity_type: "table".to_string(),
                        entity_name: name.clone(),
                        message: e.to_string(),
                    });
                }
            }

            // Return a table builder for chaining
            Self::create_table_builder(lua, name)
        })?;
        // Store state in app_data for access in closures
        lua.set_app_data(Rc::clone(&state_clone));
        globals.set("table", table_fn)?;

        // import "path" - lenient mode (errors still collected but don't stop execution)
        let state_clone = Rc::clone(&state);
        let import_fn = lua.create_function(move |lua, import_path: String| {
            let base = state_clone.borrow().base_path.clone();
            let resolved = resolve_import_path(&base, &import_path);

            // Check for circular import
            if state_clone.borrow().imported_files.contains(&resolved) {
                return Ok(());
            }

            // Mark as imported
            state_clone
                .borrow_mut()
                .imported_files
                .insert(resolved.clone());

            // Read and execute the file
            match std::fs::read_to_string(&resolved) {
                Ok(content) => {
                    if let Err(e) = lua
                        .load(&content)
                        .set_name(resolved.display().to_string())
                        .exec()
                    {
                        state_clone.borrow_mut().parse_errors.push(ParseError {
                            entity_type: "import".to_string(),
                            entity_name: import_path.clone(),
                            message: e.to_string(),
                        });
                    }
                }
                Err(e) => {
                    state_clone.borrow_mut().parse_errors.push(ParseError {
                        entity_type: "import".to_string(),
                        entity_name: import_path.clone(),
                        message: format!("Failed to read file: {}", e),
                    });
                }
            }
            Ok(())
        })?;
        globals.set("import", import_fn)?;

        Ok(())
    }

    fn parse_table(name: &str, config: &Table, _lua: &Lua) -> LuaResult<TableDefinition> {
        // Parse 'from' - can be string or array
        let from = if let Ok(single) = config.get::<String>("from") {
            FromClause::Single(single)
        } else if let Ok(table) = config.get::<Table>("from") {
            let sources: Vec<String> = table
                .sequence_values::<String>()
                .filter_map(|r| r.ok())
                .collect();
            if sources.is_empty() {
                return Err(mlua::Error::external(format!(
                    "table '{}' requires 'from' field",
                    name
                )));
            }
            FromClause::Multiple(sources)
        } else {
            return Err(mlua::Error::external(format!(
                "table '{}' requires 'from' field",
                name
            )));
        };

        // Parse table_type
        let table_type = match config.get::<String>("table_type") {
            Ok(t) => match t.to_lowercase().as_str() {
                "staging" => TableTypeLabel::Staging,
                "mart" => TableTypeLabel::Mart,
                "table" => TableTypeLabel::Table,
                _ => TableTypeLabel::Staging,
            },
            Err(_) => TableTypeLabel::Staging,
        };

        // Parse union_type
        let union_type = match config.get::<String>("union_type") {
            Ok(t) => match t.to_lowercase().as_str() {
                "all" => UnionType::All,
                _ => UnionType::Distinct,
            },
            Err(_) => UnionType::Distinct,
        };

        let mut table = TableDefinition::new(name, "").with_table_type(table_type);
        table.from = from;
        table.union_type = union_type;

        // Parse optional target_table
        if let Ok(target) = config.get::<String>("target_table") {
            table.target_table = Some(target);
        }

        // Parse optional tags
        if let Ok(tags_table) = config.get::<Table>("tags") {
            table.tags = tags_table
                .sequence_values::<String>()
                .filter_map(|r| r.ok())
                .collect();
        }

        // Parse optional primary_key
        if let Ok(pk_table) = config.get::<Table>("primary_key") {
            table.primary_key = pk_table
                .sequence_values::<String>()
                .filter_map(|r| r.ok())
                .collect();
        }

        // Parse optional description
        if let Ok(desc) = config.get::<String>("description") {
            table.description = Some(desc);
        }

        // Parse optional filter
        if let Ok(filter_str) = config.get::<String>("filter") {
            let filter_expr = sql_expr::parse_sql_expr(&filter_str).map_err(|e| {
                mlua::Error::external(format!(
                    "SQL expression error in table '{}' filter: {}",
                    name, e
                ))
            })?;
            table.filter = Some(filter_expr);
        }

        // Parse columns - can be array of strings or named computed columns
        if let Ok(columns_table) = config.get::<Table>("columns") {
            // First, process array elements (simple columns)
            for col_name in columns_table.clone().sequence_values::<String>().flatten() {
                table.columns.push(ColumnDef::Simple(col_name));
            }

            // Then process named elements (computed columns)
            for pair in columns_table.pairs::<Value, Value>() {
                let (key, value) = pair?;

                // Skip integer keys (already processed as array elements)
                if let Value::Integer(_) = key {
                    continue;
                }

                let col_name = match key {
                    Value::String(s) => s.to_str()?.to_string(),
                    _ => continue, // Skip non-string keys
                };

                if let Value::String(s) = value {
                    // SQL expression string
                    let sql_str = s.to_str()?;
                    let expr = sql_expr::parse_sql_expr(&sql_str).map_err(|e| {
                        mlua::Error::external(format!(
                            "SQL expression error in table '{}' column '{}': {}",
                            name, col_name, e
                        ))
                    })?;
                    table.columns.push(ColumnDef::Computed {
                        name: col_name,
                        expr,
                        data_type: None,
                    });
                }
            }
        }

        Ok(table)
    }

    /// Create a source builder for chained syntax: source("name"):from("schema.table"):columns({...})
    fn create_source_builder(lua: &Lua, name: String) -> LuaResult<Table> {
        let builder = lua.create_table()?;
        let name_clone = name.clone();

        // :from() method - required, sets the physical table and registers the source
        let from_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, table_name): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                // Create a source with the physical table
                let source = SourceEntity::new(&name, &table_name);
                state.borrow_mut().model.add_source(source);

                // Create global ref for entity.column syntax
                let globals = lua.globals();
                let ref_fn: mlua::Function = globals.get("ref")?;
                let entity_ref: mlua::Value = ref_fn.call(name.clone())?;
                globals.set(name.clone(), entity_ref)?;

                Ok(builder)
            })?
        };
        builder.set("from", from_fn)?;

        // :columns() method - optional, sets column definitions
        let columns_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, cols): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(source) = state.borrow_mut().model.sources.get_mut(&name) {
                    // Parse column definitions from the table
                    for pair in cols.pairs::<String, Value>() {
                        let (col_name, col_value) = pair?;

                        // Check if this column is marked as primary key (from pk() wrapper)
                        let is_primary_key = if let Value::Table(ref t) = col_value {
                            get_optional::<bool>(t, "primary_key")?.unwrap_or(false)
                        } else {
                            false
                        };

                        let col = parse_source_column(
                            &col_name,
                            col_value,
                            &format!("source '{}' column '{}'", name, col_name),
                        )?;

                        // Track primary keys
                        if is_primary_key {
                            source.primary_key.push(col_name.clone());
                        }

                        source.columns.insert(col_name, col);
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("columns", columns_fn)?;

        // :metadata() method - optional, sets change_tracking and timestamp_column
        let metadata_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, metadata): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(source) = state.borrow_mut().model.sources.get_mut(&name) {
                    // Parse change_tracking
                    if let Ok(ct_value) = metadata.get::<Value>("change_tracking") {
                        if let Some(ct) = parse_change_tracking_value(ct_value, &metadata)? {
                            source.change_tracking = Some(ct);
                        }
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("metadata", metadata_fn)?;

        // :filter() method - optional, sets source filter
        let filter_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, filter_str): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(source) = state.borrow_mut().model.sources.get_mut(&name) {
                    let filter_expr = sql_expr::parse_sql_expr(&filter_str).map_err(|e| {
                        mlua::Error::external(format!(
                            "SQL expression error in source '{}' filter: {}",
                            name, e
                        ))
                    })?;
                    source.filter = Some(filter_expr);
                }

                Ok(builder)
            })?
        };
        builder.set("filter", filter_fn)?;

        // :dedup() method - optional, sets deduplication config
        let dedup_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, dedup_config): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(source) = state.borrow_mut().model.sources.get_mut(&name) {
                    let context = format!("source '{}' dedup", name);
                    source.dedup = Some(parse_dedup_config(&dedup_config, &context)?);
                }

                Ok(builder)
            })?
        };
        builder.set("dedup", dedup_fn)?;

        // :change_tracking() method - optional, convenience method for change tracking
        let change_tracking_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, ct_value): (Table, Value)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(source) = state.borrow_mut().model.sources.get_mut(&name) {
                    // Create empty table for timestamp parsing (will get from :timestamp_column if needed)
                    let empty_table = lua.create_table()?;
                    if let Some(ct) = parse_change_tracking_value(ct_value, &empty_table)? {
                        source.change_tracking = Some(ct);
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("change_tracking", change_tracking_fn)?;

        // :timestamp_column() method - optional, convenience method for timestamp column
        let timestamp_column_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, ts_col): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(source) = state.borrow_mut().model.sources.get_mut(&name) {
                    // Update timestamp column in existing change_tracking, or create new one
                    match &mut source.change_tracking {
                        Some(ChangeTracking::AppendOnly { timestamp_column }) => {
                            *timestamp_column = ts_col;
                        }
                        Some(ChangeTracking::CDC {
                            timestamp_column, ..
                        }) => {
                            *timestamp_column = ts_col;
                        }
                        Some(ChangeTracking::FullSnapshot) => {
                            // FullSnapshot doesn't use timestamp, ignore
                        }
                        None => {
                            // Default to AppendOnly with this timestamp
                            source.change_tracking = Some(ChangeTracking::AppendOnly {
                                timestamp_column: ts_col,
                            });
                        }
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("timestamp_column", timestamp_column_fn)?;

        Ok(builder)
    }

    /// Create a fact builder for chained syntax:
    /// fact("name"):target("table"):grain({...}):measure("name", sum("col"))
    fn create_fact_builder(lua: &Lua, name: String) -> LuaResult<Table> {
        use crate::model::fact::{DimensionInclude, FactDefinition, GrainColumn};
        use crate::model::MaterializationStrategy;
        use std::collections::HashMap as StdHashMap;

        let builder = lua.create_table()?;
        let name_clone = name.clone();

        // Create the fact with defaults and register immediately
        {
            let state = lua
                .app_data_ref::<Rc<RefCell<LoaderState>>>()
                .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                .clone();

            let fact = FactDefinition {
                name: name.clone(),
                target_table: String::new(), // Will be set by :target()
                target_schema: None,
                materialized: true, // Default to materialized
                from: None,
                grain: Vec::new(),
                includes: StdHashMap::new(),
                measures: StdHashMap::new(),
                columns: Vec::new(),
                window_columns: Vec::new(),
                materialization: MaterializationStrategy::Table,
                date_config: None,
            };
            state.borrow_mut().model.add_fact(fact);

            // Create global ref for entity.column syntax
            let globals = lua.globals();
            let ref_fn: mlua::Function = globals.get("ref")?;
            let entity_ref: mlua::Value = ref_fn.call(name.clone())?;
            globals.set(name.clone(), entity_ref)?;
        }

        // :target() method - required, sets the target table
        let target_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, target): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    fact.target_table = target;
                }

                Ok(builder)
            })?
        };
        builder.set("target", target_fn.clone())?;
        // :target_table() is an alias for :target()
        builder.set("target_table", target_fn)?;

        // :grain() method - required, sets the grain columns
        let grain_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, grain_cols): (Table, Vec<String>)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    fact.grain = grain_cols
                        .into_iter()
                        .map(|s| {
                            // Parse "entity.column" format
                            let parts: Vec<&str> = s.split('.').collect();
                            if parts.len() == 2 {
                                GrainColumn {
                                    source_entity: parts[0].to_string(),
                                    source_column: parts[1].to_string(),
                                    target_name: None,
                                }
                            } else {
                                // Just a column name, entity will be inferred
                                GrainColumn {
                                    source_entity: String::new(),
                                    source_column: s,
                                    target_name: None,
                                }
                            }
                        })
                        .collect();
                }

                Ok(builder)
            })?
        };
        builder.set("grain", grain_fn)?;

        // :from() method - optional, sets the source entity
        let from_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, from_entity): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    fact.from = Some(from_entity);
                }

                Ok(builder)
            })?
        };
        builder.set("from", from_fn.clone())?;
        builder.set("from_entity", from_fn)?; // Alias for :from()

        // :include() method - optional, includes columns from a dimension
        let include_fn = {
            use crate::model::fact::ColumnSelection;

            let name = name_clone.clone();
            lua.create_function(
                move |lua, (builder, entity, columns): (Table, String, Value)| {
                    let state = lua
                        .app_data_ref::<Rc<RefCell<LoaderState>>>()
                        .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                        .clone();

                    if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                        let selection = match columns {
                            Value::Table(col_table) => {
                                let cols: Vec<String> = col_table
                                    .sequence_values::<String>()
                                    .filter_map(|r| r.ok())
                                    .collect();
                                ColumnSelection::Columns(cols)
                            }
                            Value::String(s) if s.to_str().map(|s| s == "*").unwrap_or(false) => {
                                ColumnSelection::All
                            }
                            _ => ColumnSelection::Columns(vec![]),
                        };
                        let include = DimensionInclude {
                            entity: entity.clone(),
                            selection,
                            prefix: None,
                        };
                        fact.includes.insert(entity, include);
                    }

                    Ok(builder)
                },
            )?
        };
        builder.set("include", include_fn)?;

        // :source() method - alias for :from(), sets the source entity
        let source_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, from_entity): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    fact.from = Some(from_entity);
                }

                Ok(builder)
            })?
        };
        builder.set("source", source_fn)?;

        // :column() method - adds a single computed column
        let column_fn = {
            use crate::model::expr::ColumnDef;

            let name = name_clone.clone();
            lua.create_function(
                move |lua, (builder, col_name, expr_str): (Table, String, String)| {
                    let state = lua
                        .app_data_ref::<Rc<RefCell<LoaderState>>>()
                        .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                        .clone();

                    if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                        let expr = sql_expr::parse_sql_expr(&expr_str).map_err(|e| {
                            mlua::Error::external(format!(
                                "SQL expression error in fact '{}' column '{}': {}",
                                name, col_name, e
                            ))
                        })?;
                        let col_def = ColumnDef::Computed {
                            name: col_name,
                            expr,
                            data_type: None,
                        };
                        fact.columns.push(col_def);
                    }

                    Ok(builder)
                },
            )?
        };
        builder.set("column", column_fn)?;

        // :columns() method - adds multiple computed columns from a table
        let columns_fn = {
            use crate::model::expr::ColumnDef;

            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, cols): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    for pair in cols.pairs::<String, String>() {
                        let (col_name, expr_str) = pair?;
                        let expr = sql_expr::parse_sql_expr(&expr_str).map_err(|e| {
                            mlua::Error::external(format!(
                                "SQL expression error in fact '{}' column '{}': {}",
                                name, col_name, e
                            ))
                        })?;
                        let col_def = ColumnDef::Computed {
                            name: col_name,
                            expr,
                            data_type: None,
                        };
                        fact.columns.push(col_def);
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("columns", columns_fn)?;

        // :measure() method - required at least once, adds a measure
        let measure_fn = {
            let name = name_clone.clone();
            lua.create_function(
                move |lua, (builder, measure_name, measure_def): (Table, String, Table)| {
                    let state = lua
                        .app_data_ref::<Rc<RefCell<LoaderState>>>()
                        .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                        .clone();

                    if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                        let context = format!("fact '{}' measure '{}'", name, measure_name);
                        let measure = parse_measure(&measure_name, &measure_def, &context)?;
                        fact.measures.insert(measure_name, measure);
                    }

                    Ok(builder)
                },
            )?
        };
        builder.set("measure", measure_fn)?;

        // :materialized() method - optional, sets whether the fact is materialized
        let materialized_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, materialized): (Table, bool)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    fact.materialized = materialized;
                }

                Ok(builder)
            })?
        };
        builder.set("materialized", materialized_fn)?;

        // :incremental() method - optional, sets incremental materialization strategy
        let incremental_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, config): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    let key: String =
                        get_required(&config, "key", &format!("fact '{}' incremental", name))?;
                    let unique_key: Option<Vec<String>> = get_optional(&config, "unique_key")?;

                    fact.materialization = MaterializationStrategy::Incremental {
                        incremental_key: key,
                        unique_key: unique_key.unwrap_or_default(),
                        lookback: None, // TODO: parse lookback duration if needed
                    };
                }

                Ok(builder)
            })?
        };
        builder.set("incremental", incremental_fn)?;

        // :table_type() method - optional, sets the materialization type (TABLE, VIEW, INCREMENTAL)
        let table_type_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, table_type): (Table, String)| {
                let state = lua.app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    match table_type.to_lowercase().as_str() {
                        "view" => fact.materialization = MaterializationStrategy::View,
                        "table" => fact.materialization = MaterializationStrategy::Table,
                        "incremental" => {
                            // If already incremental, keep settings; otherwise set defaults
                            if !matches!(fact.materialization, MaterializationStrategy::Incremental { .. }) {
                                fact.materialization = MaterializationStrategy::Incremental {
                                    incremental_key: String::new(),
                                    unique_key: Vec::new(),
                                    lookback: None,
                                };
                            }
                        }
                        other => {
                            return Err(mlua::Error::external(format!(
                                "Invalid table_type '{}' in fact '{}'. Expected TABLE, VIEW, or INCREMENTAL.",
                                other, name
                            )));
                        }
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("table_type", table_type_fn)?;

        // :unique_key() method - optional, sets unique key for incremental processing
        let unique_key_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, keys): (Table, Vec<String>)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    // If already incremental, update unique_key; otherwise create new incremental config
                    match &mut fact.materialization {
                        MaterializationStrategy::Incremental { unique_key, .. } => {
                            *unique_key = keys;
                        }
                        _ => {
                            fact.materialization = MaterializationStrategy::Incremental {
                                incremental_key: String::new(),
                                unique_key: keys,
                                lookback: None,
                            };
                        }
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("unique_key", unique_key_fn)?;

        // :incremental_key() method - optional, sets the incremental key column
        let incremental_key_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, key): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    // If already incremental, update incremental_key; otherwise create new incremental config
                    match &mut fact.materialization {
                        MaterializationStrategy::Incremental {
                            incremental_key, ..
                        } => {
                            *incremental_key = key;
                        }
                        _ => {
                            fact.materialization = MaterializationStrategy::Incremental {
                                incremental_key: key,
                                unique_key: Vec::new(),
                                lookback: None,
                            };
                        }
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("incremental_key", incremental_key_fn)?;

        // :date_config() method - optional, sets the date dimension configuration
        let date_config_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, config): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(fact) = state.borrow_mut().model.facts.get_mut(&name) {
                    fact.date_config =
                        Some(parse_date_config(&config, &format!("fact '{}'", name))?);
                }

                Ok(builder)
            })?
        };
        builder.set("date_config", date_config_fn)?;

        Ok(builder)
    }

    /// Create a dimension builder for chained syntax:
    /// dimension("name"):target("table"):from("entity"):columns({...}):primary_key("id")
    fn create_dimension_builder(lua: &Lua, name: String) -> LuaResult<Table> {
        use crate::model::dimension::{DimensionColumn, DimensionDefinition, SCDType};
        use crate::model::MaterializationStrategy;

        let builder = lua.create_table()?;
        let name_clone = name.clone();

        // Create the dimension with defaults and register immediately
        {
            let state = lua
                .app_data_ref::<Rc<RefCell<LoaderState>>>()
                .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                .clone();

            let dim = DimensionDefinition {
                name: name.clone(),
                target_table: String::new(), // Will be set by :target()
                target_schema: None,
                materialized: true,           // Default to materialized
                source_entity: String::new(), // Will be set by :from()
                columns: Vec::new(),
                primary_key: Vec::new(),
                scd_type: SCDType::Type1, // Default to Type1
                materialization: MaterializationStrategy::Table,
            };
            state.borrow_mut().model.add_dimension(dim);

            // Create global ref for entity.column syntax
            let globals = lua.globals();
            let ref_fn: mlua::Function = globals.get("ref")?;
            let entity_ref: mlua::Value = ref_fn.call(name.clone())?;
            globals.set(name.clone(), entity_ref)?;
        }

        // :target() method - required, sets the target table
        let target_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, target): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    dim.target_table = target;
                }

                Ok(builder)
            })?
        };
        builder.set("target", target_fn)?;

        // :from() method - required, sets the source entity
        let from_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, source): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    dim.source_entity = source;
                }

                Ok(builder)
            })?
        };
        builder.set("from", from_fn)?;

        // :columns() method - required, sets the columns
        let columns_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, cols): (Table, Vec<String>)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    dim.columns = cols
                        .into_iter()
                        .map(|col| DimensionColumn {
                            source_column: col,
                            target_column: None,
                            description: None,
                        })
                        .collect();
                }

                Ok(builder)
            })?
        };
        builder.set("columns", columns_fn)?;

        // :primary_key() method - required, sets the primary key
        let primary_key_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, pk): (Table, Value)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    dim.primary_key = match pk {
                        Value::String(s) => vec![s.to_str()?.to_string()],
                        Value::Table(t) => t
                            .sequence_values::<String>()
                            .filter_map(|r| r.ok())
                            .collect(),
                        _ => vec![],
                    };
                }

                Ok(builder)
            })?
        };
        builder.set("primary_key", primary_key_fn)?;

        // :scd() method - optional, sets the SCD type
        let scd_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, config): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    // Parse SCD type from config - can be numeric (0, 1, 2) or string ("SCD0", "SCD1", "SCD2")
                    let scd_type_value: Value = config.get("type")?;
                    let scd_type_num: i64 = match scd_type_value {
                        Value::Integer(n) => n,
                        Value::String(s) => {
                            let s_str = s.to_str()?.to_string();
                            match s_str.as_str() {
                                "SCD0" | "Type0" => 0,
                                "SCD1" | "Type1" => 1,
                                "SCD2" | "Type2" => 2,
                                _ => 1, // Default to SCD1
                            }
                        }
                        _ => 1, // Default to SCD1
                    };

                    dim.scd_type = match scd_type_num {
                        0 => SCDType::Type0,
                        2 => {
                            let effective_from: String = config
                                .get("effective_from")
                                .unwrap_or_else(|_| "effective_from".to_string());
                            let effective_to: String = config
                                .get("effective_to")
                                .unwrap_or_else(|_| "effective_to".to_string());
                            let is_current: Option<String> = config.get("is_current").ok();
                            SCDType::Type2 {
                                effective_from,
                                effective_to,
                                is_current,
                            }
                        }
                        _ => SCDType::Type1, // 1 or any other value defaults to Type1
                    };
                }

                Ok(builder)
            })?
        };
        builder.set("scd", scd_fn)?;

        // :materialized() method - optional, sets whether the dimension is materialized
        let materialized_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, materialized): (Table, bool)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    dim.materialized = materialized;
                }

                Ok(builder)
            })?
        };
        builder.set("materialized", materialized_fn)?;

        // :table_type() method - optional, sets the materialization type (TABLE, VIEW, INCREMENTAL)
        let table_type_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, table_type): (Table, String)| {
                let state = lua.app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(dim) = state.borrow_mut().model.dimensions.get_mut(&name) {
                    match table_type.to_lowercase().as_str() {
                        "view" => dim.materialization = MaterializationStrategy::View,
                        "table" => dim.materialization = MaterializationStrategy::Table,
                        "incremental" => {
                            if !matches!(dim.materialization, MaterializationStrategy::Incremental { .. }) {
                                dim.materialization = MaterializationStrategy::Incremental {
                                    incremental_key: String::new(),
                                    unique_key: Vec::new(),
                                    lookback: None,
                                };
                            }
                        }
                        other => {
                            return Err(mlua::Error::external(format!(
                                "Invalid table_type '{}' in dimension '{}'. Expected TABLE, VIEW, or INCREMENTAL.",
                                other, name
                            )));
                        }
                    }
                }

                Ok(builder)
            })?
        };
        builder.set("table_type", table_type_fn)?;

        Ok(builder)
    }

    fn create_table_builder(lua: &Lua, name: String) -> LuaResult<Table> {
        let builder = lua.create_table()?;
        let name_clone = name.clone();

        // :columns() method
        let columns_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, cols): (Table, Value)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    if let Value::Table(col_table) = cols {
                        // Parse array elements (simple columns)
                        for col_name in col_table.clone().sequence_values::<String>().flatten() {
                            table.columns.push(ColumnDef::Simple(col_name));
                        }

                        // Parse named elements (computed columns)
                        for pair in col_table.pairs::<Value, Value>() {
                            let (key, value) = pair?;

                            // Skip integer keys (already processed as array elements)
                            if let Value::Integer(_) = key {
                                continue;
                            }

                            let col_name = match key {
                                Value::String(s) => s.to_str()?.to_string(),
                                _ => continue,
                            };

                            if let Value::String(s) = value {
                                let sql_str = s.to_str()?;
                                match sql_expr::parse_sql_expr(&sql_str) {
                                    Ok(expr) => {
                                        table.columns.push(ColumnDef::Computed {
                                            name: col_name,
                                            expr,
                                            data_type: None,
                                        });
                                    }
                                    Err(e) => {
                                        return Err(mlua::Error::external(format!(
                                            "SQL expression error in column '{}': {}",
                                            col_name, e
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(builder)
            })?
        };
        builder.set("columns", columns_fn)?;

        // :filter() method
        let filter_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, expr): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    match sql_expr::parse_sql_expr(&expr) {
                        Ok(parsed) => table.filter = Some(parsed),
                        Err(e) => {
                            return Err(mlua::Error::external(format!(
                                "filter parse error: {}",
                                e
                            )));
                        }
                    }
                }
                Ok(builder)
            })?
        };
        builder.set("filter", filter_fn)?;

        // :tags() method
        let tags_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, tags): (Table, Vec<String>)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    table.tags = tags;
                }
                Ok(builder)
            })?
        };
        builder.set("tags", tags_fn)?;

        // :primary_key() method
        let pk_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, keys): (Table, Value)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    table.primary_key = match keys {
                        Value::String(s) => vec![s.to_str()?.to_string()],
                        Value::Table(t) => t
                            .sequence_values::<String>()
                            .filter_map(|r| r.ok())
                            .collect(),
                        _ => vec![],
                    };
                }
                Ok(builder)
            })?
        };
        builder.set("primary_key", pk_fn)?;

        // :from() method - sets the source entity (for chained syntax)
        let from_fn = {
            use crate::model::table::FromClause;
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, source): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    table.from = FromClause::Single(source);
                }
                Ok(builder)
            })?
        };
        builder.set("from", from_fn)?;

        // :description() method - sets the description
        let description_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, desc): (Table, String)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    table.description = Some(desc);
                }
                Ok(builder)
            })?
        };
        builder.set("description", description_fn)?;

        // :group_by() method - for aggregating intermediates
        let group_by_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, cols): (Table, Vec<String>)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table) = state.borrow_mut().model.tables.get_mut(&name) {
                    table.group_by = cols;
                }
                Ok(builder)
            })?
        };
        builder.set("group_by", group_by_fn)?;

        // :window_columns() method - for window function definitions
        let window_columns_fn = {
            let name = name_clone.clone();
            lua.create_function(move |lua, (builder, cols): (Table, Table)| {
                let state = lua
                    .app_data_ref::<Rc<RefCell<LoaderState>>>()
                    .ok_or_else(|| mlua::Error::external("LoaderState not found"))?
                    .clone();

                if let Some(table_def) = state.borrow_mut().model.tables.get_mut(&name) {
                    // Parse window columns like in parse_intermediate
                    for pair in cols.clone().pairs::<Value, Value>() {
                        let (key, value) = pair?;

                        if let Value::Integer(idx) = key {
                            // Array element: { name = "foo", func = ..., partition_by = ..., ... }
                            if let Value::Table(col_def) = value {
                                let col_name: String = get_required(
                                    &col_def,
                                    "name",
                                    &format!("table '{}' window_column '{}'", name, idx),
                                )?;
                                let window_col = parse_window_column_def_from_table(
                                    &col_name,
                                    &col_def,
                                    &format!("table '{}' window_column '{}'", name, col_name),
                                )?;
                                table_def.window_columns.push(window_col);
                            }
                        } else if let Value::String(col_name_lua) = key {
                            // Named element: foo = row_number():partition_by(...):order_by(...)
                            let col_name = col_name_lua.to_str()?.to_string();
                            if let Value::Table(col_def) = value {
                                let window_col = parse_window_column_def_from_chained(
                                    &col_name,
                                    &col_def,
                                    &format!("table '{}' window_column '{}'", name, col_name),
                                )?;
                                table_def.window_columns.push(window_col);
                            }
                        }
                    }
                }
                Ok(builder)
            })?
        };
        builder.set("window_columns", window_columns_fn)?;

        Ok(builder)
    }
}

fn resolve_import_path(base: &Path, import_path: &str) -> PathBuf {
    let path = Path::new(import_path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

// =============================================================================
// Parsing functions for block syntax (deprecated - kept for reference)
// =============================================================================
// NOTE: These functions were used with the old block syntax (e.g., source "name" { ... })
// They are now unused as the chained syntax (source("name"):from(...):columns(...)) is preferred.
// Kept for reference and potential future use.

#[allow(dead_code)]
fn parse_source(name: &str, table: &Table) -> LuaResult<SourceEntity> {
    let source_table: String = get_required(table, "table", &format!("source '{}'", name))?;

    let mut source = SourceEntity::new(name, &source_table);

    // Optional schema
    if let Some(schema) = get_optional::<String>(table, "schema")? {
        source.schema = Some(schema);
    }

    // Columns
    if let Some(columns_table) = get_optional::<Table>(table, "columns")? {
        for pair in columns_table.pairs::<String, Value>() {
            let (col_name, col_value) = pair?;
            let col = parse_source_column(
                &col_name,
                col_value,
                &format!("source '{}' column '{}'", name, col_name),
            )?;
            source.columns.insert(col_name, col);
        }
    }

    // Primary key (can be in columns or top-level)
    if let Some(pk) = get_optional::<Table>(table, "primary_key")? {
        source.primary_key = table_to_string_vec(&pk)?;
    } else {
        // Re-parse to find primary keys from column definitions (primary_key = true)
        if let Some(columns_table) = get_optional::<Table>(table, "columns")? {
            let mut pks = vec![];
            for pair in columns_table.pairs::<String, Value>() {
                let (col_name, col_value) = pair?;
                // Only check for primary_key if it's a table definition
                if let Value::Table(col_table) = col_value {
                    if get_optional::<bool>(&col_table, "primary_key")?.unwrap_or(false) {
                        pks.push(col_name);
                    }
                }
            }
            if !pks.is_empty() {
                source.primary_key = pks;
            }
        }
    }

    // Change tracking
    if let Some(ct) = get_optional::<String>(table, "change_tracking")? {
        let tracking = match ct.as_str() {
            "append_only" => {
                let ts_col = get_optional::<String>(table, "timestamp_column")?
                    .unwrap_or_else(|| "created_at".to_string());
                ChangeTracking::AppendOnly {
                    timestamp_column: ts_col,
                }
            }
            "cdc" => {
                let op_col = get_required::<String>(
                    table,
                    "operation_column",
                    &format!("source '{}' cdc", name),
                )?;
                let ts_col = get_required::<String>(
                    table,
                    "timestamp_column",
                    &format!("source '{}' cdc", name),
                )?;
                ChangeTracking::CDC {
                    operation_column: op_col,
                    timestamp_column: ts_col,
                }
            }
            "full_snapshot" => ChangeTracking::FullSnapshot,
            other => {
                return Err(mlua::Error::external(format!(
                    "Invalid change_tracking value '{}' in source '{}'. Expected: append_only, cdc, full_snapshot",
                    other, name
                )));
            }
        };
        source.change_tracking = Some(tracking);
    }

    Ok(source)
}

/// Parse a change tracking value from Lua.
/// Accepts either a string constant (APPEND_ONLY, CDC, FULL_SNAPSHOT) or a table with details.
fn parse_change_tracking_value(
    value: Value,
    metadata: &Table,
) -> LuaResult<Option<ChangeTracking>> {
    match value {
        Value::String(s) => {
            let ct_str = s.to_str()?.to_string();
            match ct_str.as_str() {
                "append_only" => {
                    // Get timestamp_column from metadata table or use default
                    let ts_col = get_optional::<String>(metadata, "timestamp_column")?
                        .unwrap_or_else(|| "created_at".to_string());
                    Ok(Some(ChangeTracking::AppendOnly {
                        timestamp_column: ts_col,
                    }))
                }
                "cdc" => {
                    let op_col = get_required::<String>(
                        metadata,
                        "operation_column",
                        "cdc change_tracking",
                    )?;
                    let ts_col = get_required::<String>(
                        metadata,
                        "timestamp_column",
                        "cdc change_tracking",
                    )?;
                    Ok(Some(ChangeTracking::CDC {
                        operation_column: op_col,
                        timestamp_column: ts_col,
                    }))
                }
                "full_snapshot" => Ok(Some(ChangeTracking::FullSnapshot)),
                _ => Err(mlua::Error::external(format!(
                    "Invalid change_tracking value '{}'. Expected: append_only, cdc, full_snapshot",
                    ct_str
                ))),
            }
        }
        Value::Nil => Ok(None),
        _ => Err(mlua::Error::external(
            "change_tracking must be a string (APPEND_ONLY, CDC, or FULL_SNAPSHOT)",
        )),
    }
}

fn parse_source_column(name: &str, value: Value, context: &str) -> LuaResult<SourceColumn> {
    match value {
        Value::Table(table) => {
            let type_str: String = get_required(&table, "type", context)?;
            let data_type = DataType::parse(&type_str).ok_or_else(|| {
                mlua::Error::external(format!("Invalid data type '{}' in {}", type_str, context))
            })?;
            let nullable = get_optional::<bool>(&table, "nullable")?.unwrap_or(true);
            let description = get_optional::<String>(&table, "description")?;

            Ok(SourceColumn {
                name: name.to_string(),
                data_type,
                nullable,
                description,
            })
        }
        Value::String(type_str) => {
            // Shorthand: column = "int64"
            let type_s = type_str.to_str()?;
            let data_type = DataType::parse(&type_s).ok_or_else(|| {
                mlua::Error::external(format!("Invalid data type '{}' in {}", type_s, context))
            })?;
            Ok(SourceColumn {
                name: name.to_string(),
                data_type,
                nullable: true,
                description: None,
            })
        }
        _ => Err(mlua::Error::external(format!(
            "Invalid column definition in {}. Expected table or string.",
            context
        ))),
    }
}

fn parse_relationship(table: &Table) -> LuaResult<Relationship> {
    let from: String = get_required(table, "from", "relationship")?;
    let to: String = get_required(table, "to", "relationship")?;
    let cardinality_str: String = get_required(table, "cardinality", "relationship")?;

    // Parse "entity.column" format
    let (from_entity, from_column) = parse_entity_column(&from, "relationship.from")?;
    let (to_entity, to_column) = parse_entity_column(&to, "relationship.to")?;

    let cardinality = match cardinality_str.as_str() {
        "one_to_one" | "1:1" => Cardinality::OneToOne,
        "one_to_many" | "1:n" | "1:N" => Cardinality::OneToMany,
        "many_to_one" | "n:1" | "N:1" => Cardinality::ManyToOne,
        "many_to_many" | "n:n" | "N:N" | "m:n" | "M:N" => Cardinality::ManyToMany,
        other => {
            return Err(mlua::Error::external(format!(
                "Invalid cardinality '{}'. Expected: one_to_one, one_to_many, many_to_one, many_to_many",
                other
            )));
        }
    };

    // Parse optional role name (for role-playing dimensions)
    let role: Option<String> = get_optional(table, "role")?;

    let mut rel = Relationship::new(from_entity, to_entity, from_column, to_column, cardinality);
    if let Some(role_name) = role {
        rel = rel.with_role(role_name);
    }

    Ok(rel)
}

#[allow(dead_code)]
fn parse_fact(name: &str, table: &Table) -> LuaResult<FactDefinition> {
    // target_table is optional for non-materialized (virtual) facts
    let materialized = get_optional::<bool>(table, "materialized")?.unwrap_or(true);
    let target_table: String = if materialized {
        get_required(table, "target_table", &format!("fact '{}'", name))?
    } else {
        get_optional::<String>(table, "target_table")?
            .unwrap_or_else(|| format!("__virtual__.{}", name))
    };

    let mut fact = FactDefinition::new(name, &target_table);

    // Set materialized flag (determines if we query target table or reconstruct from sources)
    fact.materialized = materialized;

    // Target schema
    if let Some(schema) = get_optional::<String>(table, "target_schema")? {
        fact.target_schema = Some(schema);
    }

    // From (source entity or intermediate to build from)
    if let Some(from) = get_optional::<String>(table, "from")? {
        fact.from = Some(from);
    }

    // Grain
    if let Some(grain_table) = get_optional::<Table>(table, "grain")? {
        for value in grain_table.sequence_values::<String>() {
            let grain_str = value?;
            let (entity, column) =
                parse_entity_column(&grain_str, &format!("fact '{}' grain", name))?;
            fact.grain.push(GrainColumn {
                source_entity: entity,
                source_column: column,
                target_name: None,
            });
        }
    }

    // Include (dimension attributes to denormalize)
    if let Some(include_table) = get_optional::<Table>(table, "include")? {
        for pair in include_table.pairs::<String, Value>() {
            let (entity, value) = pair?;
            let selection =
                parse_column_selection(value, &format!("fact '{}' include '{}'", name, entity))?;
            fact.includes.insert(
                entity.clone(),
                DimensionInclude {
                    entity: entity.clone(),
                    selection,
                    prefix: None,
                },
            );
        }
    }

    // Measures
    if let Some(measures_table) = get_optional::<Table>(table, "measures")? {
        for pair in measures_table.pairs::<String, Table>() {
            let (measure_name, measure_table) = pair?;
            let measure = parse_measure(
                &measure_name,
                &measure_table,
                &format!("fact '{}' measure '{}'", name, measure_name),
            )?;
            fact.measures.insert(measure_name, measure);
        }
    }

    // Materialization
    if let Some(mat_str) = get_optional::<String>(table, "materialization")? {
        fact.materialization = parse_materialization(&mat_str, table, &format!("fact '{}'", name))?;
    }

    // Date configuration (role-playing dimensions)
    if let Some(date_config_table) = get_optional::<Table>(table, "date_config")? {
        fact.date_config = Some(parse_date_config(
            &date_config_table,
            &format!("fact '{}'", name),
        )?);
    }

    Ok(fact)
}

fn parse_measure(name: &str, table: &Table, context: &str) -> LuaResult<MeasureDefinition> {
    let agg_str: String = get_required(table, "agg", context)?;
    let column: String = get_required(table, "column", context)?;

    let aggregation = match agg_str.as_str() {
        "sum" => AggregationType::Sum,
        "count" => AggregationType::Count,
        "count_distinct" => AggregationType::CountDistinct,
        "avg" => AggregationType::Avg,
        "min" => AggregationType::Min,
        "max" => AggregationType::Max,
        other => {
            return Err(mlua::Error::external(format!(
                "Invalid aggregation '{}' in {}. Expected: sum, count, count_distinct, avg, min, max",
                other, context
            )));
        }
    };

    // Parse filter string into Expr if provided
    let filter = if let Some(filter_str) = get_optional::<String>(table, "filter")? {
        let filter_expr = sql_expr::parse_sql_expr(&filter_str).map_err(|e| {
            mlua::Error::external(format!(
                "Failed to parse filter '{}' in {}: {}",
                filter_str, context, e
            ))
        })?;
        Some(filter_expr)
    } else {
        None
    };

    let description = get_optional::<String>(table, "description")?;

    Ok(MeasureDefinition {
        name: name.to_string(),
        aggregation,
        source_column: column,
        filter,
        description,
    })
}

/// Parse date configuration for role-playing dimensions.
///
/// Lua syntax:
/// ```lua
/// date_config = {
///     roles = {
///         order_date = "order_date_id",    -- role_name = fk_column
///         ship_date = "ship_date_id",
///     },
///     dimension = "date",                   -- target dimension
///     pk_column = "date_id",                -- PK column on dimension
///     primary_role = "order_date",          -- default for time intelligence
///     grain_columns = {
///         year = "year",
///         quarter = "quarter",
///         month = "month",
///         day = "day",
///     },
/// }
/// ```
fn parse_date_config(table: &Table, context: &str) -> LuaResult<DateConfig> {
    let mut config = DateConfig::new();

    // Parse dimension name (target dimension entity)
    let dimension: String = get_optional(table, "dimension")?.unwrap_or_else(|| "date".to_string());

    // Parse PK column on dimension
    let pk_column: String =
        get_optional(table, "pk_column")?.unwrap_or_else(|| "date_id".to_string());

    // Parse roles (role_name -> fk_column mappings)
    if let Some(roles_table) = get_optional::<Table>(table, "roles")? {
        for pair in roles_table.pairs::<String, String>() {
            let (role_name, fk_column) = pair?;
            config.add_role(DimensionRole::new(
                role_name,
                fk_column,
                dimension.clone(),
                pk_column.clone(),
            ));
        }
    }

    // Parse primary role
    if let Some(primary) = get_optional::<String>(table, "primary_role")? {
        config.primary_role = Some(primary);
    }

    // Parse grain columns
    if let Some(grain_table) = get_optional::<Table>(table, "grain_columns")? {
        let year: String =
            get_required(&grain_table, "year", &format!("{} grain_columns", context))?;
        let mut grain = GrainColumns::new(year);

        if let Some(quarter) = get_optional::<String>(&grain_table, "quarter")? {
            grain = grain.with_quarter(quarter);
        }
        if let Some(month) = get_optional::<String>(&grain_table, "month")? {
            grain = grain.with_month(month);
        }
        if let Some(week) = get_optional::<String>(&grain_table, "week")? {
            grain = grain.with_week(week);
        }
        if let Some(day) = get_optional::<String>(&grain_table, "day")? {
            grain = grain.with_day(day);
        }

        config.grain_columns = Some(grain);
    }

    Ok(config)
}

#[allow(dead_code)]
fn parse_dimension(name: &str, table: &Table) -> LuaResult<DimensionDefinition> {
    // target_table is optional for non-materialized (virtual) dimensions
    let materialized = get_optional::<bool>(table, "materialized")?.unwrap_or(true);
    let target_table: String = if materialized {
        get_required(table, "target_table", &format!("dimension '{}'", name))?
    } else {
        get_optional::<String>(table, "target_table")?
            .unwrap_or_else(|| format!("__virtual__.{}", name))
    };
    let source: String = get_required(table, "source", &format!("dimension '{}'", name))?;

    let mut dim = DimensionDefinition::new(name, &target_table, &source);

    // Set materialized flag (determines if we query target table or reconstruct from source)
    dim.materialized = materialized;

    // Target schema
    if let Some(schema) = get_optional::<String>(table, "target_schema")? {
        dim.target_schema = Some(schema);
    }

    // Columns
    if let Some(columns_table) = get_optional::<Table>(table, "columns")? {
        let columns = table_to_string_vec(&columns_table)?;
        for col in columns {
            dim = dim.with_column(col);
        }
    }

    // Primary key
    if let Some(pk_table) = get_optional::<Table>(table, "primary_key")? {
        dim.primary_key = table_to_string_vec(&pk_table)?;
    }

    // SCD type
    if let Some(scd) = get_optional::<i64>(table, "scd_type")? {
        dim.scd_type = match scd {
            0 => SCDType::Type0,
            1 => SCDType::Type1,
            2 => {
                // Type 2 needs additional config
                let effective_from = get_optional::<String>(table, "effective_from")?
                    .unwrap_or_else(|| "valid_from".to_string());
                let effective_to = get_optional::<String>(table, "effective_to")?
                    .unwrap_or_else(|| "valid_to".to_string());
                let is_current = get_optional::<String>(table, "is_current")?;
                SCDType::Type2 {
                    effective_from,
                    effective_to,
                    is_current,
                }
            }
            _ => {
                return Err(mlua::Error::external(format!(
                    "Unsupported SCD type {} in dimension '{}'. Supported: 0, 1, 2",
                    scd, name
                )));
            }
        };
    }

    // Materialization
    if let Some(mat_str) = get_optional::<String>(table, "materialization")? {
        dim.materialization =
            parse_materialization(&mat_str, table, &format!("dimension '{}'", name))?;
    }

    Ok(dim)
}

fn parse_report(name: &str, table: &Table) -> LuaResult<Report> {
    let mut report = Report::new(name);

    // Measures - array of "fact.measure" strings
    if let Some(measures_table) = get_optional::<Table>(table, "measures")? {
        for value in measures_table.sequence_values::<String>() {
            let measure_str = value?;
            if let Some(measure_ref) = MeasureRef::parse(&measure_str) {
                report.measures.push(measure_ref);
            } else {
                return Err(mlua::Error::external(format!(
                    "Invalid measure reference '{}' in report '{}'. Expected 'fact.measure' format.",
                    measure_str, name
                )));
            }
        }
    }

    // Filters - array of SQL filter expressions
    if let Some(filters_table) = get_optional::<Table>(table, "filters")? {
        report.filters = table_to_string_vec(&filters_table)?;
    }

    // Group by - array of dimension columns
    if let Some(group_by_table) = get_optional::<Table>(table, "group_by")? {
        report.group_by = table_to_string_vec(&group_by_table)?;
    }

    // Defaults
    if let Some(defaults_table) = get_optional::<Table>(table, "defaults")? {
        let time_range = get_optional::<String>(&defaults_table, "time_range")?;
        let limit = get_optional::<u32>(&defaults_table, "limit")?;
        report.defaults = Some(ReportDefaults { time_range, limit });
    }

    // Description
    if let Some(desc) = get_optional::<String>(table, "description")? {
        report.description = Some(desc);
    }

    // Materialization (optional)
    let materialized = get_optional::<bool>(table, "materialized")?.unwrap_or(false);
    if materialized {
        let target_table: String = get_required(
            table,
            "target_table",
            &format!(
                "report '{}' (materialized = true requires target_table)",
                name
            ),
        )?;
        let target_schema = get_optional::<String>(table, "target_schema")?;

        // Parse table_type (TABLE or VIEW)
        let table_type = if let Some(type_str) = get_optional::<String>(table, "table_type")? {
            match type_str.to_uppercase().as_str() {
                "VIEW" => ReportTableType::View,
                "TABLE" => ReportTableType::Table,
                other => {
                    return Err(mlua::Error::external(format!(
                        "Invalid table_type '{}' in report '{}'. Expected TABLE or VIEW.",
                        other, name
                    )))
                }
            }
        } else {
            ReportTableType::Table // Default to TABLE
        };

        // Parse refresh_delta (required for TABLE, ignored for VIEW)
        let refresh_delta = if let Some(delta_str) = get_optional::<String>(table, "refresh_delta")?
        {
            RefreshDelta::parse(&delta_str).ok_or_else(|| {
                mlua::Error::external(format!(
                    "Invalid refresh_delta '{}' in report '{}'. Expected format like '4 hours', '30 minutes', '1 day'.",
                    delta_str, name
                ))
            })?
        } else if table_type == ReportTableType::Table {
            return Err(mlua::Error::external(format!(
                "report '{}' with table_type = TABLE requires refresh_delta (e.g., '4 hours').",
                name
            )));
        } else {
            // VIEW doesn't need refresh_delta
            RefreshDelta::from_hours(0) // Placeholder, not used for VIEW
        };

        report.materialization = Some(ReportMaterialization {
            materialized: true,
            table_type,
            target_table,
            target_schema,
            refresh_delta: if table_type == ReportTableType::Table {
                Some(refresh_delta)
            } else {
                None
            },
        });
    }

    Ok(report)
}

fn parse_pivot_report(name: &str, table: &Table) -> LuaResult<PivotReport> {
    let mut pivot = PivotReport::new(name);

    // Rows - array of dimension columns
    if let Some(rows_table) = get_optional::<Table>(table, "rows")? {
        pivot.rows = table_to_string_vec(&rows_table)?;
    }

    // Columns - can be:
    // 1. Simple string: "dimension.column" (dynamic)
    // 2. Table with dimension and values: { dimension = "...", values = {...} } (explicit)
    if let Some(columns_str) = get_optional::<String>(table, "columns")? {
        pivot.columns = PivotColumns::Dynamic(columns_str);
    } else if let Some(columns_table) = get_optional::<Table>(table, "columns")? {
        // Check if it's explicit (has dimension and values) or just a string array
        if let Some(dimension) = get_optional::<String>(&columns_table, "dimension")? {
            let values =
                if let Some(values_table) = get_optional::<Table>(&columns_table, "values")? {
                    table_to_string_vec(&values_table)?
                } else {
                    vec![]
                };
            pivot.columns = PivotColumns::Explicit { dimension, values };
        } else {
            // Treat as dynamic with first string value
            if let Some(value) = columns_table.sequence_values::<String>().next() {
                pivot.columns = PivotColumns::Dynamic(value?);
            }
        }
    }

    // Values - named measure definitions: { revenue = { measure = "fact.measure" }, ... }
    if let Some(values_table) = get_optional::<Table>(table, "values")? {
        for pair in values_table.pairs::<String, Table>() {
            let (value_name, value_def) = pair?;
            let measure_str: String = get_required(
                &value_def,
                "measure",
                &format!("pivot_report '{}' value '{}'", name, value_name),
            )?;

            if let Some(measure_ref) = MeasureRef::parse(&measure_str) {
                let format = get_optional::<String>(&value_def, "format")?;
                pivot.values.push(PivotValue {
                    name: value_name,
                    measure: measure_ref,
                    format,
                });
            } else {
                return Err(mlua::Error::external(format!(
                    "Invalid measure reference '{}' in pivot_report '{}' value '{}'. Expected 'fact.measure' format.",
                    measure_str, name, value_name
                )));
            }
        }
    }

    // Filters - array of SQL filter expressions
    if let Some(filters_table) = get_optional::<Table>(table, "filters")? {
        pivot.filters = table_to_string_vec(&filters_table)?;
    }

    // Totals configuration
    if let Some(totals_table) = get_optional::<Table>(table, "totals")? {
        let rows = get_optional::<bool>(&totals_table, "rows")?.unwrap_or(false);
        let columns = get_optional::<bool>(&totals_table, "columns")?.unwrap_or(false);
        let grand = get_optional::<bool>(&totals_table, "grand")?.unwrap_or(false);
        pivot.totals = Some(TotalsConfig {
            rows,
            columns,
            grand,
        });
    }

    // Sort configuration
    if let Some(sort_table) = get_optional::<Table>(table, "sort")? {
        let by: String = get_required(&sort_table, "by", &format!("pivot_report '{}' sort", name))?;
        let direction_str =
            get_optional::<String>(&sort_table, "direction")?.unwrap_or_else(|| "asc".to_string());
        let direction = match direction_str.to_lowercase().as_str() {
            "desc" => SortDirection::Desc,
            _ => SortDirection::Asc,
        };
        pivot.sort = Some(PivotSort { by, direction });
    }

    // Description
    if let Some(desc) = get_optional::<String>(table, "description")? {
        pivot.description = Some(desc);
    }

    // Materialization (optional)
    let materialized = get_optional::<bool>(table, "materialized")?.unwrap_or(false);
    if materialized {
        let target_table: String = get_required(
            table,
            "target_table",
            &format!(
                "pivot_report '{}' (materialized = true requires target_table)",
                name
            ),
        )?;
        let target_schema = get_optional::<String>(table, "target_schema")?;

        // Parse table_type (TABLE or VIEW)
        let table_type = if let Some(type_str) = get_optional::<String>(table, "table_type")? {
            match type_str.to_uppercase().as_str() {
                "VIEW" => ReportTableType::View,
                "TABLE" => ReportTableType::Table,
                other => {
                    return Err(mlua::Error::external(format!(
                        "Invalid table_type '{}' in pivot_report '{}'. Expected TABLE or VIEW.",
                        other, name
                    )))
                }
            }
        } else {
            ReportTableType::Table // Default to TABLE
        };

        // Parse refresh_delta (required for TABLE, ignored for VIEW)
        let refresh_delta = if let Some(delta_str) = get_optional::<String>(table, "refresh_delta")?
        {
            RefreshDelta::parse(&delta_str).ok_or_else(|| {
                mlua::Error::external(format!(
                    "Invalid refresh_delta '{}' in pivot_report '{}'. Expected format like '4 hours', '30 minutes', '1 day'.",
                    delta_str, name
                ))
            })?
        } else if table_type == ReportTableType::Table {
            return Err(mlua::Error::external(format!(
                "pivot_report '{}' with table_type = TABLE requires refresh_delta (e.g., '4 hours').",
                name
            )));
        } else {
            // VIEW doesn't need refresh_delta
            RefreshDelta::from_hours(0) // Placeholder, not used for VIEW
        };

        pivot.materialization = Some(ReportMaterialization {
            materialized: true,
            table_type,
            target_table,
            target_schema,
            refresh_delta: if table_type == ReportTableType::Table {
                Some(refresh_delta)
            } else {
                None
            },
        });
    }

    Ok(pivot)
}

fn parse_query(name: &str, table: &Table) -> LuaResult<QueryDefinition> {
    // from is optional - anchor can be inferred from measures
    let from: Option<String> = get_optional(table, "from")?;

    let mut query = if let Some(ref f) = from {
        QueryDefinition::new(name, f)
    } else {
        QueryDefinition::new_inferred(name)
    };

    // Parse select list
    if let Some(select_table) = get_optional::<Table>(table, "select")? {
        for pair in select_table.pairs::<Value, Value>() {
            let (key, value) = pair?;

            match (key, value) {
                // Numeric index with string value (array item)
                (Value::Integer(_), Value::String(s)) => {
                    let s = s.to_str()?;
                    query.select.push(QuerySelect::parse(&s));
                }
                // Numeric index with table (measure ref, filtered measure, or derived)
                (Value::Integer(_), Value::Table(t)) => {
                    // Check for filtered measure: { _filtered_measure = true, name = "...", filters = {...} }
                    // Name can be "measure_name" or "entity.measure_name"
                    if get_optional::<bool>(&t, "_filtered_measure")?.unwrap_or(false) {
                        let measure_name: String = get_required(&t, "name", "filtered_measure")?;
                        let alias = get_optional::<String>(&t, "alias")?;
                        let filters = parse_filtered_measure_filters(&t)?;
                        // Parse entity.measure format
                        let (entity, name) = if let Some((e, m)) = measure_name.split_once('.') {
                            (Some(e.to_string()), m.to_string())
                        } else {
                            (None, measure_name)
                        };
                        query.select.push(QuerySelect::FilteredMeasure {
                            entity,
                            name,
                            alias,
                            filters,
                        });
                    }
                    // Check for derived measure: { _derived_measure = true, alias = "...", expression = {...} }
                    else if get_optional::<bool>(&t, "_derived_measure")?.unwrap_or(false) {
                        let alias: String = get_required(&t, "alias", "derived_measure")?;
                        let expr_value: Value = t.get("expression")?;
                        if !matches!(expr_value, Value::Nil) {
                            let expression = parse_derived_expression(&expr_value)?;
                            query
                                .select
                                .push(QuerySelect::DerivedMeasure { alias, expression });
                        }
                    }
                    // Regular measure reference: { _measure_ref = true, name = "..." }
                    // Name can be "measure_name" or "entity.measure_name"
                    else if let Some(measure_name) = get_optional::<String>(&t, "name")? {
                        let alias = get_optional::<String>(&t, "alias")?;
                        // Parse entity.measure format
                        let (entity, name) = if let Some((e, m)) = measure_name.split_once('.') {
                            (Some(e.to_string()), m.to_string())
                        } else {
                            (None, measure_name)
                        };
                        query.select.push(QuerySelect::Measure {
                            entity,
                            name,
                            alias,
                        });
                    }
                }
                // String key with value (named alias)
                (Value::String(alias), Value::String(s)) => {
                    let alias_str = alias.to_str()?;
                    let s = s.to_str()?;
                    let mut sel = QuerySelect::parse(&s);
                    if let QuerySelect::Measure { ref mut alias, .. } = sel {
                        *alias = Some(alias_str.to_string());
                    }
                    query.select.push(sel);
                }
                _ => {}
            }
        }
    }

    // Parse where clause (filters) - structured filter objects
    if let Some(where_table) = get_optional::<Table>(table, "where")? {
        for value in where_table.sequence_values::<Value>() {
            let filter_value = value?;
            if let Value::Table(filter_table) = filter_value {
                if let Some(filter) = parse_query_filter(&filter_table)? {
                    query.filters.push(filter);
                }
            }
        }
    }

    // Parse filter clause - SQL expression strings
    if let Some(filter_table) = get_optional::<Table>(table, "filter")? {
        for value in filter_table.sequence_values::<Value>() {
            let filter_value = value?;
            match filter_value {
                Value::String(s) => {
                    // SQL expression string like "year >= 2023"
                    let sql_str = s.to_str()?;
                    let expr = sql_expr::parse_sql_expr(&sql_str).map_err(|e| {
                        mlua::Error::external(format!(
                            "SQL expression error in query '{}' filter: {}",
                            name, e
                        ))
                    })?;
                    query.filter_exprs.push(expr);
                }
                Value::Table(filter_table) => {
                    // Structured filter from helper functions like gte(), ne()
                    if let Some(filter) = parse_query_filter(&filter_table)? {
                        query.filters.push(filter);
                    }
                }
                _ => {}
            }
        }
    }

    // Parse group_by (explicit grouping beyond auto-inferred from dimensions)
    if let Some(group_by_table) = get_optional::<Table>(table, "group_by")? {
        query.group_by = table_to_string_vec(&group_by_table)?;
    }

    // Parse order_by
    if let Some(order_by_table) = get_optional::<Table>(table, "order_by")? {
        for value in order_by_table.sequence_values::<Value>() {
            let order_value = value?;
            match order_value {
                Value::Table(t) => {
                    // { _order = true, field = "...", dir = "desc" }
                    if let Some(field) = get_optional::<String>(&t, "field")? {
                        let dir =
                            get_optional::<String>(&t, "dir")?.unwrap_or_else(|| "asc".to_string());
                        query.order_by.push(QueryOrderBy {
                            field,
                            descending: dir.to_lowercase() == "desc",
                        });
                    }
                }
                Value::String(s) => {
                    // Bare string - ascending
                    let field = s.to_str()?.to_string();
                    query.order_by.push(QueryOrderBy::asc(field));
                }
                _ => {}
            }
        }
    }

    // Parse limit
    if let Some(limit) = get_optional::<u64>(table, "limit")? {
        query.limit = Some(limit);
    }

    // Parse offset
    if let Some(offset) = get_optional::<u64>(table, "offset")? {
        query.offset = Some(offset);
    }

    // Parse description
    if let Some(desc) = get_optional::<String>(table, "description")? {
        query.description = Some(desc);
    }

    Ok(query)
}

/// Parse a filter from Lua table to QueryFilter.
///
/// Supports two formats:
/// 1. Expression-style: { _expr = "binary", op = ">=", left = {...}, right = {...} }
/// 2. Filter-style: { _filter = true, field = "entity.column", op = ">=", value = 2024 }
fn parse_query_filter(table: &Table) -> LuaResult<Option<QueryFilter>> {
    // Check for _filter style (from filter() helper)
    if get_optional::<bool>(table, "_filter")?.unwrap_or(false) {
        let field: String = get_required(table, "field", "filter")?;
        let op_str: String = get_required(table, "op", "filter")?;

        let op = QueryFilterOp::from_str(&op_str)
            .ok_or_else(|| mlua::Error::external(format!("Unknown filter operator: {}", op_str)))?;

        let value = parse_filter_value(table.get::<Value>("value")?)?;

        return Ok(Some(QueryFilter { field, op, value }));
    }

    // Check for _expr style (from eq(), gte(), etc.)
    if let Some(expr_type) = get_optional::<String>(table, "_expr")? {
        if expr_type == "binary" {
            let op_str: String = get_required(table, "op", "expression")?;
            let op = QueryFilterOp::from_str(&op_str)
                .ok_or_else(|| mlua::Error::external(format!("Unknown operator: {}", op_str)))?;

            // Extract field from left side
            let left_value: Value = table.get("left")?;
            let field = extract_field_from_expr(left_value)?;

            // Extract value from right side
            let right_value: Value = table.get("right")?;
            let value = parse_filter_value(right_value)?;

            return Ok(Some(QueryFilter { field, op, value }));
        }
    }

    Ok(None)
}

/// Extract field reference (entity.column) from an expression.
fn extract_field_from_expr(value: Value) -> LuaResult<String> {
    match value {
        Value::String(s) => Ok(s.to_str()?.to_string()),
        Value::Table(t) => {
            // Could be a column reference: { _expr = "column", name = "entity.column" }
            if let Some(name) = get_optional::<String>(&t, "name")? {
                return Ok(name);
            }
            // Or entity.column became "entity.column" string via ref metatable
            if let Some(expr_str) = get_optional::<String>(&t, "value")? {
                return Ok(expr_str);
            }
            Err(mlua::Error::external(
                "Cannot extract field from expression",
            ))
        }
        _ => Err(mlua::Error::external("Expected string or table for field")),
    }
}

/// Parse a filter value from Lua.
fn parse_filter_value(value: Value) -> LuaResult<QueryFilterValue> {
    match value {
        Value::Nil => Ok(QueryFilterValue::Null),
        Value::Boolean(b) => Ok(QueryFilterValue::Bool(b)),
        Value::Integer(n) => Ok(QueryFilterValue::Int(n)),
        Value::Number(f) => Ok(QueryFilterValue::Float(f)),
        Value::String(s) => Ok(QueryFilterValue::String(s.to_str()?.to_string())),
        Value::Table(t) => {
            // Could be a literal: { _expr = "literal", value = ... }
            if let Ok(lit_value) = t.get::<Value>("value") {
                if !matches!(lit_value, Value::Nil) {
                    return parse_filter_value(lit_value);
                }
            }
            // Could be a list
            let mut items = Vec::new();
            for v in t.sequence_values::<Value>() {
                items.push(parse_filter_value(v?)?);
            }
            if !items.is_empty() {
                Ok(QueryFilterValue::List(items))
            } else {
                Ok(QueryFilterValue::Null)
            }
        }
        _ => Ok(QueryFilterValue::Null),
    }
}

/// Parse filters for a filtered measure.
/// Handles both single filter and array of filters.
fn parse_filtered_measure_filters(table: &Table) -> LuaResult<Vec<QueryFilter>> {
    let filters_value: Value = table.get("filters")?;

    match filters_value {
        Value::Table(t) => {
            // Could be a single filter or an array of filters
            // Check if it's a single filter (has _filter or op field)
            if get_optional::<bool>(&t, "_filter")?.unwrap_or(false)
                || get_optional::<String>(&t, "op")?.is_some()
            {
                // Single filter
                if let Some(filter) = parse_query_filter(&t)? {
                    return Ok(vec![filter]);
                }
            }

            // Array of filters
            let mut filters = Vec::new();
            for value in t.sequence_values::<Value>() {
                if let Value::Table(filter_table) = value? {
                    if let Some(filter) = parse_query_filter(&filter_table)? {
                        filters.push(filter);
                    }
                }
            }
            Ok(filters)
        }
        _ => Ok(Vec::new()),
    }
}

/// Parse a derived expression from Lua value.
/// Handles measure references, binary operations, literals, and time functions.
fn parse_derived_expression(value: &Value) -> LuaResult<crate::model::query::DerivedExpression> {
    use crate::model::query::{DerivedExpression, DerivedOp, QueryFilterValue};

    match value {
        Value::Table(t) => {
            // Check for time function: { _time_fn = "ytd", measure = "...", ... }
            if let Some(fn_name) = get_optional::<String>(t, "_time_fn")? {
                let time_fn = parse_time_function(&fn_name, t)?;
                return Ok(DerivedExpression::TimeFunction(time_fn));
            }

            // Check for delta: { _delta = true, current = {...}, previous = {...} }
            if get_optional::<bool>(t, "_delta")?.unwrap_or(false) {
                let current_value: Value = t.get("current")?;
                let previous_value: Value = t.get("previous")?;
                let current = parse_derived_expression(&current_value)?;
                let previous = parse_derived_expression(&previous_value)?;
                return Ok(DerivedExpression::Delta {
                    current: Box::new(current),
                    previous: Box::new(previous),
                });
            }

            // Check for growth: { _growth = true, current = {...}, previous = {...} }
            if get_optional::<bool>(t, "_growth")?.unwrap_or(false) {
                let current_value: Value = t.get("current")?;
                let previous_value: Value = t.get("previous")?;
                let current = parse_derived_expression(&current_value)?;
                let previous = parse_derived_expression(&previous_value)?;
                return Ok(DerivedExpression::Growth {
                    current: Box::new(current),
                    previous: Box::new(previous),
                });
            }

            // Check for binary operation: { _derived_op = "+", left = {...}, right = {...} }
            if let Some(op_str) = get_optional::<String>(t, "_derived_op")? {
                if op_str == "negate" {
                    // Unary negation
                    let expr_value: Value = t.get("expr")?;
                    let inner = parse_derived_expression(&expr_value)?;
                    return Ok(DerivedExpression::Negate(Box::new(inner)));
                }

                // Binary operation
                let op = DerivedOp::from_str(&op_str).ok_or_else(|| {
                    mlua::Error::external(format!("Unknown derived operator: {}", op_str))
                })?;

                let left_value: Value = t.get("left")?;
                let right_value: Value = t.get("right")?;

                let left = parse_derived_expression(&left_value)?;
                let right = parse_derived_expression(&right_value)?;

                return Ok(DerivedExpression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                });
            }

            // Check for measure reference: { _measure_ref_expr = true, name = "..." }
            if get_optional::<bool>(t, "_measure_ref_expr")?.unwrap_or(false) {
                let name: String = get_required(t, "name", "measure reference")?;
                return Ok(DerivedExpression::MeasureRef(name));
            }

            // Check for regular measure reference: { _measure_ref = true, name = "..." }
            if get_optional::<bool>(t, "_measure_ref")?.unwrap_or(false) {
                let name: String = get_required(t, "name", "measure reference")?;
                return Ok(DerivedExpression::MeasureRef(name));
            }

            // Check for literal: { _expr = "literal", value = ... }
            if let Ok(lit_value) = t.get::<Value>("value") {
                if !matches!(lit_value, Value::Nil) {
                    return parse_derived_expression(&lit_value);
                }
            }

            Err(mlua::Error::external("Cannot parse derived expression"))
        }
        Value::Integer(n) => Ok(DerivedExpression::Literal(QueryFilterValue::Int(*n))),
        Value::Number(f) => Ok(DerivedExpression::Literal(QueryFilterValue::Float(*f))),
        Value::String(s) => {
            // String could be a measure name
            Ok(DerivedExpression::MeasureRef(s.to_str()?.to_string()))
        }
        _ => Err(mlua::Error::external(format!(
            "Unexpected value type in derived expression: {:?}",
            value
        ))),
    }
}

/// Parse a time function from a Lua table.
fn parse_time_function(
    fn_name: &str,
    t: &Table,
) -> LuaResult<crate::model::query::QueryTimeFunction> {
    use crate::model::query::QueryTimeFunction;

    let measure: String = get_required(t, "measure", "time function")?;
    let via: Option<String> = get_optional(t, "via")?;

    match fn_name {
        "ytd" => Ok(QueryTimeFunction::YearToDate {
            measure,
            year_column: get_optional(t, "year_column")?,
            period_column: get_optional(t, "period_column")?,
            via,
        }),
        "qtd" => Ok(QueryTimeFunction::QuarterToDate {
            measure,
            year_column: get_optional(t, "year_column")?,
            quarter_column: get_optional(t, "quarter_column")?,
            period_column: get_optional(t, "period_column")?,
            via,
        }),
        "mtd" => Ok(QueryTimeFunction::MonthToDate {
            measure,
            year_column: get_optional(t, "year_column")?,
            month_column: get_optional(t, "month_column")?,
            day_column: get_optional(t, "day_column")?,
            via,
        }),
        "prior_period" => {
            let periods_back: u32 = get_optional(t, "periods")?.unwrap_or(1);
            Ok(QueryTimeFunction::PriorPeriod {
                measure,
                periods_back,
                via,
            })
        }
        "prior_year" => Ok(QueryTimeFunction::PriorYear { measure, via }),
        "prior_quarter" => Ok(QueryTimeFunction::PriorQuarter { measure, via }),
        "rolling_sum" => {
            let periods: u32 = get_required(t, "periods", "rolling_sum")?;
            Ok(QueryTimeFunction::RollingSum {
                measure,
                periods,
                via,
            })
        }
        "rolling_avg" => {
            let periods: u32 = get_required(t, "periods", "rolling_avg")?;
            Ok(QueryTimeFunction::RollingAvg {
                measure,
                periods,
                via,
            })
        }
        _ => Err(mlua::Error::external(format!(
            "Unknown time function: {}",
            fn_name
        ))),
    }
}

#[allow(dead_code)]
fn parse_materialization(
    strategy: &str,
    table: &Table,
    context: &str,
) -> LuaResult<MaterializationStrategy> {
    match strategy {
        "view" => Ok(MaterializationStrategy::View),
        "table" => Ok(MaterializationStrategy::Table),
        "incremental" => {
            let unique_key = get_optional::<Table>(table, "unique_key")?
                .map(|t| table_to_string_vec(&t))
                .transpose()?
                .unwrap_or_default();
            let incremental_key: String = get_required(table, "incremental_key", context)?;
            Ok(MaterializationStrategy::Incremental {
                unique_key,
                incremental_key,
                lookback: None, // TODO: parse duration if needed
            })
        }
        "snapshot" => {
            let unique_key = get_optional::<Table>(table, "unique_key")?
                .map(|t| table_to_string_vec(&t))
                .transpose()?
                .unwrap_or_default();
            let updated_at: String = get_required(table, "updated_at", context)?;
            Ok(MaterializationStrategy::Snapshot {
                unique_key,
                updated_at,
            })
        }
        other => Err(mlua::Error::external(format!(
            "Invalid materialization '{}' in {}. Expected: view, table, incremental, snapshot",
            other, context
        ))),
    }
}

// =============================================================================
// Helper functions
// =============================================================================

fn get_required<T: mlua::FromLua>(table: &Table, key: &str, context: &str) -> LuaResult<T> {
    table.get::<T>(key).map_err(|_| {
        mlua::Error::external(format!("Missing required field '{}' in {}", key, context))
    })
}

fn get_optional<T: mlua::FromLua>(table: &Table, key: &str) -> LuaResult<Option<T>> {
    match table.get::<Option<T>>(key) {
        Ok(v) => Ok(v),
        Err(_) => Ok(None),
    }
}

fn table_to_string_vec(table: &Table) -> LuaResult<Vec<String>> {
    let mut result = vec![];
    for value in table.sequence_values::<String>() {
        result.push(value?);
    }
    Ok(result)
}

/// Parse a column selection value which can be:
/// - "*" (string) -> All columns
/// - { _except = { "col1", "col2" } } (table with _except) -> All except these
/// - { "col1", "col2" } (array of strings) -> These specific columns
#[allow(dead_code)]
fn parse_column_selection(value: Value, context: &str) -> LuaResult<ColumnSelection> {
    match value {
        Value::String(s) => {
            let s = s.to_str()?;
            if s == "*" {
                Ok(ColumnSelection::All)
            } else {
                Err(mlua::Error::external(format!(
                    "Invalid column selection '{}' in {}. Use '*' for all columns or an array of column names.",
                    s, context
                )))
            }
        }
        Value::Table(table) => {
            // Check if it's an "except" specification
            if let Some(except_value) = get_optional::<Table>(&table, "_except")? {
                let columns = table_to_string_vec(&except_value)?;
                Ok(ColumnSelection::Except(columns))
            } else {
                // Assume it's an array of column names
                let columns = table_to_string_vec(&table)?;
                Ok(ColumnSelection::Columns(columns))
            }
        }
        _ => Err(mlua::Error::external(format!(
            "Invalid column selection in {}. Expected '*', array of columns, or {{ _except = {{...}} }}",
            context
        ))),
    }
}

fn parse_entity_column(s: &str, context: &str) -> LuaResult<(String, String)> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        return Err(mlua::Error::external(format!(
            "Invalid format '{}' in {}. Expected 'entity.column'",
            s, context
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

// =============================================================================
// Expression Parsing
// =============================================================================

/// Parse a Lua value into an Expr.
///
/// Expressions in Lua are represented as tables with a `_type` or `_expr` field.
/// String values are first checked to see if they look like SQL expressions
/// (containing operators or keywords). If so, they're parsed as SQL.
/// Otherwise, simple strings are treated as column references.
fn parse_expr(value: Value, context: &str) -> LuaResult<Expr> {
    match value {
        // String: could be column reference or SQL expression
        Value::String(s) => {
            let s = s.to_str()?;

            // Check if this looks like a SQL expression (contains operators or SQL keywords)
            let looks_like_sql = s.contains('=')
                || s.contains('<')
                || s.contains('>')
                || s.contains('+')
                || s.contains('-')
                || s.contains('*')
                || s.contains('/')
                || s.contains('(')
                || s.contains(')')
                || s.to_uppercase().contains(" AND ")
                || s.to_uppercase().contains(" OR ")
                || s.to_uppercase().contains(" NOT ")
                || s.to_uppercase().contains(" IS ")
                || s.to_uppercase().contains(" IN ")
                || s.to_uppercase().contains(" LIKE ")
                || s.to_uppercase().contains(" BETWEEN ")
                || s.to_uppercase().contains("CASE ")
                || s.to_uppercase().contains("COALESCE")
                || s.to_uppercase().contains("NULLIF");

            if looks_like_sql {
                // Parse as SQL expression
                sql_expr::parse_sql_expr(&s).map_err(|e| {
                    mlua::Error::external(format!("SQL expression error in {}: {}", context, e))
                })
            } else if let Some((entity, column)) = s.split_once('.') {
                // Qualified column reference: entity.column
                Ok(Expr::Column {
                    entity: Some(entity.to_string()),
                    column: column.to_string(),
                })
            } else {
                // Simple column reference
                Ok(Expr::Column {
                    entity: None,
                    column: s.to_string(),
                })
            }
        }
        // Numeric literal
        Value::Integer(n) => Ok(Expr::Literal(Literal::Int(n))),
        Value::Number(n) => Ok(Expr::Literal(Literal::Float(n))),
        // Boolean literal
        Value::Boolean(b) => Ok(Expr::Literal(Literal::Bool(b))),
        // Nil as null
        Value::Nil => Ok(Expr::Literal(Literal::Null)),
        // Table - structured expression
        Value::Table(table) => parse_expr_table(&table, context),
        _ => Err(mlua::Error::external(format!(
            "Invalid expression in {}. Expected string, number, boolean, or expression table.",
            context
        ))),
    }
}

/// Parse a Lua table as a structured expression.
fn parse_expr_table(table: &Table, context: &str) -> LuaResult<Expr> {
    // Check for SQL expression type first (from prelude's as_expr)
    if let Some(expr_marker) = get_optional::<String>(table, "_expr")? {
        match expr_marker.as_str() {
            "sql" => {
                // SQL string expression - parse with sqlparser
                let sql: String = get_required(table, "sql", context)?;
                return sql_expr::parse_sql_expr(&sql).map_err(|e| {
                    mlua::Error::external(format!("SQL expression error in {}: {}", context, e))
                });
            }
            "column" => {
                // Column reference from expression builder
                let column: String = get_required(table, "column", context)?;
                let entity = get_optional::<String>(table, "entity")?;
                return Ok(Expr::Column { entity, column });
            }
            "literal" => {
                // Literal value from expression builder
                let lit_type =
                    get_optional::<String>(table, "type")?.unwrap_or_else(|| "string".to_string());
                return match lit_type.as_str() {
                    "null" => Ok(Expr::Literal(Literal::Null)),
                    "bool" => {
                        let v: bool = get_required(table, "value", context)?;
                        Ok(Expr::Literal(Literal::Bool(v)))
                    }
                    "int" => {
                        let v: i64 = get_required(table, "value", context)?;
                        Ok(Expr::Literal(Literal::Int(v)))
                    }
                    "float" => {
                        let v: f64 = get_required(table, "value", context)?;
                        Ok(Expr::Literal(Literal::Float(v)))
                    }
                    _ => {
                        let v: String = get_required(table, "value", context)?;
                        Ok(Expr::Literal(Literal::String(v)))
                    }
                };
            }
            "binary" => {
                // Binary operation from expression builder
                let op_str: String = get_required(table, "op", context)?;
                let left: Value = get_required(table, "left", context)?;
                let right: Value = get_required(table, "right", context)?;
                let op = parse_binary_op(&op_str, context)?;
                let left_expr = parse_expr(left, context)?;
                let right_expr = parse_expr(right, context)?;
                return Ok(Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op,
                    right: Box::new(right_expr),
                });
            }
            "unary" => {
                // Unary operation from expression builder
                let op_str: String = get_required(table, "op", context)?;
                let expr: Value = get_required(table, "expr", context)?;
                let op = parse_unary_op(&op_str, context)?;
                let inner_expr = parse_expr(expr, context)?;
                return Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(inner_expr),
                });
            }
            "function" => {
                // Function call from expression builder
                let func_name: String = get_required(table, "func", context)?;
                let func = parse_func(&func_name, context)?;
                let mut args = vec![];
                if let Some(args_table) = get_optional::<Table>(table, "args")? {
                    for value in args_table.sequence_values::<Value>() {
                        args.push(parse_expr(value?, context)?);
                    }
                }
                return Ok(Expr::Function { func, args });
            }
            "case" => {
                // CASE expression from expression builder
                let when_table: Table = get_required(table, "when_clauses", context)?;
                let mut when_clauses = vec![];
                for pair in when_table.sequence_values::<Table>() {
                    let when_tbl = pair?;
                    let condition: Value = get_required(&when_tbl, "when", context)?;
                    let result: Value = get_required(&when_tbl, "then_", context)?;
                    when_clauses.push(crate::model::WhenClause {
                        condition: parse_expr(condition, context)?,
                        result: parse_expr(result, context)?,
                    });
                }
                let else_clause = get_optional::<Value>(table, "else_clause")?
                    .map(|v| parse_expr(v, context))
                    .transpose()?
                    .map(Box::new);
                return Ok(Expr::Case {
                    operand: None,
                    when_clauses,
                    else_clause,
                });
            }
            "cast" => {
                // CAST from expression builder
                let expr: Value = get_required(table, "expr", context)?;
                let target_type_str: String = get_required(table, "target_type", context)?;
                let inner_expr = parse_expr(expr, context)?;
                let target_type = DataType::parse(&target_type_str).ok_or_else(|| {
                    mlua::Error::external(format!(
                        "Invalid cast target type '{}' in {}",
                        target_type_str, context
                    ))
                })?;
                return Ok(Expr::Cast {
                    expr: Box::new(inner_expr),
                    target_type,
                });
            }
            "in" => {
                // IN expression from expression builder
                let expr: Value = get_required(table, "expr", context)?;
                let inner_expr = parse_expr(expr, context)?;
                let values_table: Table = get_required(table, "values", context)?;

                // Convert to series of OR equalities (matches SQL behavior)
                let mut values = vec![];
                for v in values_table.sequence_values::<Value>() {
                    values.push(parse_expr(v?, context)?);
                }

                if values.is_empty() {
                    return Ok(Expr::Literal(Literal::Bool(false)));
                }

                let mut result = Expr::BinaryOp {
                    left: Box::new(inner_expr.clone()),
                    op: BinaryOp::Eq,
                    right: Box::new(values[0].clone()),
                };
                for val in &values[1..] {
                    let cmp = Expr::BinaryOp {
                        left: Box::new(inner_expr.clone()),
                        op: BinaryOp::Eq,
                        right: Box::new(val.clone()),
                    };
                    result = Expr::BinaryOp {
                        left: Box::new(result),
                        op: BinaryOp::Or,
                        right: Box::new(cmp),
                    };
                }
                return Ok(result);
            }
            _ => {
                // Unknown _expr type, fall through to _type handling
            }
        }
    }

    // Fall back to _type handling for backwards compatibility
    let expr_type: String = get_required(table, "_type", context)?;

    match expr_type.as_str() {
        "column" => {
            let column: String = get_required(table, "column", context)?;
            let entity = get_optional::<String>(table, "entity")?;
            Ok(Expr::Column { entity, column })
        }
        "literal" => {
            let lit_type: String =
                get_optional(table, "lit_type")?.unwrap_or_else(|| "string".to_string());
            match lit_type.as_str() {
                "null" => Ok(Expr::Literal(Literal::Null)),
                "bool" => {
                    let v: bool = get_required(table, "value", context)?;
                    Ok(Expr::Literal(Literal::Bool(v)))
                }
                "int" => {
                    let v: i64 = get_required(table, "value", context)?;
                    Ok(Expr::Literal(Literal::Int(v)))
                }
                "float" => {
                    let v: f64 = get_required(table, "value", context)?;
                    Ok(Expr::Literal(Literal::Float(v)))
                }
                _ => {
                    let v: String = get_required(table, "value", context)?;
                    Ok(Expr::Literal(Literal::String(v)))
                }
            }
        }
        "binary_op" => {
            let op_str: String = get_required(table, "op", context)?;
            let left: Value = get_required(table, "left", context)?;
            let right: Value = get_required(table, "right", context)?;

            let op = parse_binary_op(&op_str, context)?;
            let left_expr = parse_expr(left, context)?;
            let right_expr = parse_expr(right, context)?;

            Ok(Expr::BinaryOp {
                left: Box::new(left_expr),
                op,
                right: Box::new(right_expr),
            })
        }
        "unary_op" => {
            let op_str: String = get_required(table, "op", context)?;
            let expr: Value = get_required(table, "expr", context)?;

            let op = parse_unary_op(&op_str, context)?;
            let inner_expr = parse_expr(expr, context)?;

            Ok(Expr::UnaryOp {
                op,
                expr: Box::new(inner_expr),
            })
        }
        "function" => {
            let func_name: String = get_required(table, "func", context)?;
            let args_table: Table = get_optional(table, "args")?.unwrap_or_else(|| {
                table.raw_get::<Table>("args").unwrap_or_else(|_| {
                    // Create empty table - this is a bit awkward but handles edge cases
                    table.clone()
                })
            });

            let func = parse_func(&func_name, context)?;
            let mut args = vec![];
            for value in args_table.sequence_values::<Value>() {
                args.push(parse_expr(value?, context)?);
            }

            Ok(Expr::Function { func, args })
        }
        "cast" => {
            let expr: Value = get_required(table, "expr", context)?;
            let target_type_str: String = get_required(table, "target_type", context)?;

            let inner_expr = parse_expr(expr, context)?;
            let target_type = DataType::parse(&target_type_str).ok_or_else(|| {
                mlua::Error::external(format!(
                    "Invalid cast target type '{}' in {}",
                    target_type_str, context
                ))
            })?;

            Ok(Expr::Cast {
                expr: Box::new(inner_expr),
                target_type,
            })
        }
        "case" => {
            let operand = get_optional::<Value>(table, "operand")?
                .map(|v| parse_expr(v, context))
                .transpose()?
                .map(Box::new);

            let when_table: Table = get_required(table, "when_clauses", context)?;
            let mut when_clauses = vec![];
            for pair in when_table.sequence_values::<Table>() {
                let when_tbl = pair?;
                let condition: Value = get_required(&when_tbl, "when", context)?;
                let result: Value = get_required(&when_tbl, "then", context)?;
                when_clauses.push(crate::model::WhenClause {
                    condition: parse_expr(condition, context)?,
                    result: parse_expr(result, context)?,
                });
            }

            let else_clause = get_optional::<Value>(table, "else")?
                .map(|v| parse_expr(v, context))
                .transpose()?
                .map(Box::new);

            Ok(Expr::Case {
                operand,
                when_clauses,
                else_clause,
            })
        }
        "window" => {
            let func_name: String = get_required(table, "func", context)?;
            let args_table: Option<Table> = get_optional(table, "args")?;
            let partition_table: Option<Table> = get_optional(table, "partition_by")?;
            let order_table: Option<Table> = get_optional(table, "order_by")?;
            let frame_table: Option<Table> = get_optional(table, "frame")?;

            let func = parse_window_func(&func_name, context)?;

            let mut args = vec![];
            if let Some(at) = args_table {
                for value in at.sequence_values::<Value>() {
                    args.push(parse_expr(value?, context)?);
                }
            }

            let mut partition_by = vec![];
            if let Some(pt) = partition_table {
                for value in pt.sequence_values::<Value>() {
                    partition_by.push(parse_expr(value?, context)?);
                }
            }

            let mut order_by = vec![];
            if let Some(ot) = order_table {
                for value in ot.sequence_values::<Value>() {
                    order_by.push(parse_order_by_expr(value?, context)?);
                }
            }

            let frame = frame_table
                .map(|ft| parse_window_frame(&ft, context))
                .transpose()?;

            Ok(Expr::Window {
                func,
                args,
                partition_by,
                order_by,
                frame,
            })
        }
        other => Err(mlua::Error::external(format!(
            "Unknown expression type '{}' in {}",
            other, context
        ))),
    }
}

fn parse_binary_op(op: &str, context: &str) -> LuaResult<BinaryOp> {
    match op {
        "eq" | "=" | "==" => Ok(BinaryOp::Eq),
        "ne" | "!=" | "<>" => Ok(BinaryOp::Ne),
        "lt" | "<" => Ok(BinaryOp::Lt),
        "le" | "lte" | "<=" => Ok(BinaryOp::Lte),
        "gt" | ">" => Ok(BinaryOp::Gt),
        "ge" | "gte" | ">=" => Ok(BinaryOp::Gte),
        "add" | "+" => Ok(BinaryOp::Add),
        "sub" | "-" => Ok(BinaryOp::Sub),
        "mul" | "*" => Ok(BinaryOp::Mul),
        "div" | "/" => Ok(BinaryOp::Div),
        "mod" | "%" => Ok(BinaryOp::Mod),
        "and" => Ok(BinaryOp::And),
        "or" => Ok(BinaryOp::Or),
        "like" => Ok(BinaryOp::Like),
        "ilike" => Ok(BinaryOp::ILike),
        "in" => Ok(BinaryOp::In),
        "not_in" => Ok(BinaryOp::NotIn),
        "between" => Ok(BinaryOp::Between),
        "not_between" => Ok(BinaryOp::NotBetween),
        "concat" | "||" => Ok(BinaryOp::Concat),
        other => Err(mlua::Error::external(format!(
            "Unknown binary operator '{}' in {}",
            other, context
        ))),
    }
}

fn parse_unary_op(op: &str, context: &str) -> LuaResult<UnaryOp> {
    match op {
        "not" | "!" => Ok(UnaryOp::Not),
        "neg" | "-" => Ok(UnaryOp::Neg),
        "is_null" => Ok(UnaryOp::IsNull),
        "is_not_null" => Ok(UnaryOp::IsNotNull),
        other => Err(mlua::Error::external(format!(
            "Unknown unary operator '{}' in {}",
            other, context
        ))),
    }
}

fn parse_func(name: &str, context: &str) -> LuaResult<Func> {
    match name.to_lowercase().as_str() {
        // Aggregates
        "count" => Ok(Func::Count),
        "sum" => Ok(Func::Sum),
        "avg" => Ok(Func::Avg),
        "min" => Ok(Func::Min),
        "max" => Ok(Func::Max),
        "count_distinct" => Ok(Func::CountDistinct),

        // String
        "upper" => Ok(Func::Upper),
        "lower" => Ok(Func::Lower),
        "trim" => Ok(Func::Trim),
        "ltrim" => Ok(Func::LTrim),
        "rtrim" => Ok(Func::RTrim),
        "length" | "len" => Ok(Func::Length),
        "concat" => Ok(Func::Concat),
        "substring" | "substr" => Ok(Func::Substring),
        "replace" => Ok(Func::Replace),
        "left" => Ok(Func::Left),
        "right" => Ok(Func::Right),
        "split_part" => Ok(Func::SplitPart),
        "regexp_replace" => Ok(Func::RegexpReplace),
        "regexp_extract" => Ok(Func::RegexpExtract),

        // Date/Time
        "date_trunc" | "datetrunc" => Ok(Func::DateTrunc),
        "date_part" | "datepart" | "extract" => Ok(Func::Extract),
        "date_add" | "dateadd" => Ok(Func::DateAdd),
        "date_sub" | "datesub" => Ok(Func::DateSub),
        "date_diff" | "datediff" => Ok(Func::DateDiff),
        "current_date" => Ok(Func::CurrentDate),
        "current_timestamp" | "now" => Ok(Func::CurrentTimestamp),
        "year" => Ok(Func::Year),
        "month" => Ok(Func::Month),
        "day" => Ok(Func::Day),
        "hour" => Ok(Func::Hour),
        "minute" => Ok(Func::Minute),
        "second" => Ok(Func::Second),
        "day_of_week" | "dayofweek" => Ok(Func::DayOfWeek),
        "day_of_year" | "dayofyear" => Ok(Func::DayOfYear),
        "week_of_year" | "weekofyear" => Ok(Func::WeekOfYear),
        "quarter" => Ok(Func::Quarter),
        "last_day" => Ok(Func::LastDay),
        "to_date" => Ok(Func::ToDate),
        "make_date" => Ok(Func::MakeDate),
        "make_timestamp" => Ok(Func::MakeTimestamp),

        // Numeric
        "round" => Ok(Func::Round),
        "floor" => Ok(Func::Floor),
        "ceil" | "ceiling" => Ok(Func::Ceil),
        "abs" => Ok(Func::Abs),
        "power" | "pow" => Ok(Func::Power),
        "sqrt" => Ok(Func::Sqrt),
        "log" => Ok(Func::Log),
        "ln" => Ok(Func::Ln),
        "exp" => Ok(Func::Exp),
        "sign" => Ok(Func::Sign),
        "mod" => Ok(Func::Mod),
        "truncate" | "trunc" => Ok(Func::Truncate),
        "random" | "rand" => Ok(Func::Random),

        // Conditional
        "if" | "iff" => Ok(Func::If),
        "coalesce" => Ok(Func::Coalesce),
        "nullif" => Ok(Func::NullIf),
        "ifnull" | "isnull" => Ok(Func::IfNull),
        "greatest" => Ok(Func::Greatest),
        "least" => Ok(Func::Least),

        // Cast/Convert
        "cast" => Ok(Func::Cast),
        "try_cast" => Ok(Func::TryCast),
        "to_char" => Ok(Func::ToChar),
        "to_number" => Ok(Func::ToNumber),

        // Array/JSON
        "array_agg" => Ok(Func::ArrayAgg),
        "string_agg" => Ok(Func::StringAgg),
        "array_length" => Ok(Func::ArrayLength),
        "json_extract" => Ok(Func::JsonExtract),
        "json_extract_text" => Ok(Func::JsonExtractText),
        "json_array_length" => Ok(Func::JsonArrayLength),

        other => Err(mlua::Error::external(format!(
            "Unknown function '{}' in {}",
            other, context
        ))),
    }
}

fn parse_window_func(name: &str, context: &str) -> LuaResult<WindowFunc> {
    match name.to_lowercase().as_str() {
        "row_number" => Ok(WindowFunc::RowNumber),
        "rank" => Ok(WindowFunc::Rank),
        "dense_rank" => Ok(WindowFunc::DenseRank),
        "ntile" => Ok(WindowFunc::NTile),
        "percent_rank" => Ok(WindowFunc::PercentRank),
        "cume_dist" => Ok(WindowFunc::CumeDist),
        "lag" => Ok(WindowFunc::Lag),
        "lead" => Ok(WindowFunc::Lead),
        "first_value" => Ok(WindowFunc::FirstValue),
        "last_value" => Ok(WindowFunc::LastValue),
        "nth_value" => Ok(WindowFunc::NthValue),
        "sum" => Ok(WindowFunc::Sum),
        "count" => Ok(WindowFunc::Count),
        "avg" => Ok(WindowFunc::Avg),
        "min" => Ok(WindowFunc::Min),
        "max" => Ok(WindowFunc::Max),
        other => Err(mlua::Error::external(format!(
            "Unknown window function '{}' in {}",
            other, context
        ))),
    }
}

fn parse_order_by_expr(value: Value, context: &str) -> LuaResult<OrderByExpr> {
    match value {
        // Simple string: column name with default ASC
        Value::String(s) => {
            let s_owned = s.to_str()?.to_string();
            let s_str = s_owned.as_str();
            let (col_str, dir) = if s_str.ends_with(" desc") || s_str.ends_with(" DESC") {
                (&s_str[..s_str.len() - 5], SortDir::Desc)
            } else if s_str.ends_with(" asc") || s_str.ends_with(" ASC") {
                (&s_str[..s_str.len() - 4], SortDir::Asc)
            } else {
                (s_str, SortDir::Asc)
            };

            let expr = if let Some((entity, column)) = col_str.trim().split_once('.') {
                Expr::Column {
                    entity: Some(entity.to_string()),
                    column: column.to_string(),
                }
            } else {
                Expr::Column {
                    entity: None,
                    column: col_str.trim().to_string(),
                }
            };

            Ok(OrderByExpr {
                expr,
                dir,
                nulls: None,
            })
        }
        // Table: { expr = ..., dir = "asc"|"desc", nulls = "first"|"last" }
        Value::Table(table) => {
            let expr_val: Value = get_required(&table, "expr", context)?;
            let expr = parse_expr(expr_val, context)?;

            let dir = get_optional::<String>(&table, "dir")?
                .map(|d| match d.as_str() {
                    "desc" | "DESC" => SortDir::Desc,
                    _ => SortDir::Asc,
                })
                .unwrap_or(SortDir::Asc);

            let nulls = get_optional::<String>(&table, "nulls")?.map(|n| match n.as_str() {
                "first" | "FIRST" => NullsOrder::First,
                "last" | "LAST" => NullsOrder::Last,
                _ => NullsOrder::Last,
            });

            Ok(OrderByExpr { expr, dir, nulls })
        }
        _ => Err(mlua::Error::external(format!(
            "Invalid order_by in {}. Expected string or table.",
            context
        ))),
    }
}

fn parse_window_frame(table: &Table, context: &str) -> LuaResult<WindowFrame> {
    let kind_str = get_optional::<String>(table, "kind")?.unwrap_or_else(|| "rows".to_string());
    let kind = match kind_str.as_str() {
        "range" | "RANGE" => FrameKind::Range,
        "groups" | "GROUPS" => FrameKind::Groups,
        _ => FrameKind::Rows,
    };

    let start_str: String = get_required(table, "start", context)?;
    let start = parse_frame_bound(&start_str, context)?;

    let end = get_optional::<String>(table, "end")?
        .map(|s| parse_frame_bound(&s, context))
        .transpose()?;

    Ok(WindowFrame { kind, start, end })
}

fn parse_frame_bound(s: &str, context: &str) -> LuaResult<FrameBound> {
    match s.to_lowercase().as_str() {
        "unbounded_preceding" | "unbounded preceding" => Ok(FrameBound::UnboundedPreceding),
        "current_row" | "current row" => Ok(FrameBound::CurrentRow),
        "unbounded_following" | "unbounded following" => Ok(FrameBound::UnboundedFollowing),
        _ => {
            // Try to parse as "N preceding" or "N following"
            if let Some(n_str) = s
                .strip_suffix(" preceding")
                .or_else(|| s.strip_suffix(" PRECEDING"))
            {
                if let Ok(n) = n_str.trim().parse::<u32>() {
                    return Ok(FrameBound::Preceding(n));
                }
            }
            if let Some(n_str) = s
                .strip_suffix(" following")
                .or_else(|| s.strip_suffix(" FOLLOWING"))
            {
                if let Ok(n) = n_str.trim().parse::<u32>() {
                    return Ok(FrameBound::Following(n));
                }
            }
            Err(mlua::Error::external(format!(
                "Invalid frame bound '{}' in {}. Expected: unbounded_preceding, current_row, unbounded_following, N preceding, N following",
                s, context
            )))
        }
    }
}

// =============================================================================
// Dedup and Filter Parsing
// =============================================================================

fn parse_dedup_config(table: &Table, context: &str) -> LuaResult<DedupConfig> {
    let partition_by_table: Table = get_required(table, "partition_by", context)?;
    let partition_by = table_to_string_vec(&partition_by_table)?;

    let order_by_table: Table = get_required(table, "order_by", context)?;
    let mut order_by = vec![];
    for value in order_by_table.sequence_values::<Value>() {
        order_by.push(parse_order_by_expr(value?, context)?);
    }

    let keep = get_optional::<String>(table, "keep")?
        .map(|k| match k.as_str() {
            "last" | "LAST" => DedupKeep::Last,
            _ => DedupKeep::First,
        })
        .unwrap_or(DedupKeep::First);

    Ok(DedupConfig {
        partition_by,
        order_by,
        keep,
    })
}

// =============================================================================
// Column Definition Parsing
// =============================================================================

#[allow(dead_code)]
fn parse_column_def(value: Value, context: &str) -> LuaResult<ColumnDef> {
    match value {
        // Simple string: pass-through column
        Value::String(s) => {
            let s = s.to_str()?;
            Ok(ColumnDef::Simple(s.to_string()))
        }
        // Table with _type
        Value::Table(table) => {
            let col_type =
                get_optional::<String>(&table, "_type")?.unwrap_or_else(|| "simple".to_string());

            match col_type.as_str() {
                "simple" => {
                    let name: String = get_required(&table, "name", context)?;
                    Ok(ColumnDef::Simple(name))
                }
                "renamed" => {
                    let source: String = get_required(&table, "source", context)?;
                    let target: String = get_required(&table, "target", context)?;
                    Ok(ColumnDef::Renamed { source, target })
                }
                "computed" => {
                    let name: String = get_required(&table, "name", context)?;
                    let expr_val: Value = get_required(&table, "expr", context)?;
                    let expr = parse_expr(expr_val, context)?;
                    let data_type = get_optional::<String>(&table, "data_type")?
                        .and_then(|s| DataType::parse(&s));
                    Ok(ColumnDef::Computed {
                        name,
                        expr,
                        data_type,
                    })
                }
                other => Err(mlua::Error::external(format!(
                    "Unknown column type '{}' in {}",
                    other, context
                ))),
            }
        }
        _ => Err(mlua::Error::external(format!(
            "Invalid column definition in {}. Expected string or table.",
            context
        ))),
    }
}

// =============================================================================
// Window Column Definition Parsing
// =============================================================================

/// Parse window column from array format: { name = "foo", func = row_number(), partition_by = {...}, ... }
fn parse_window_column_def_from_table(
    name: &str,
    table: &Table,
    context: &str,
) -> LuaResult<WindowColumnDef> {
    // func can be:
    // 1. A window function table: { _window = true, func = "row_number", args = {...} }
    // 2. A measure table used as window: { agg = "sum", column = "..." }
    // 3. A direct string function name: "row_number"
    let func_value: Value = get_required(table, "func", context)?;

    let (func, mut args) = match func_value {
        Value::Table(func_table) => {
            // Check if it's a window function table (_window = true, func = "...")
            if let Ok(Some(true)) = get_optional::<bool>(&func_table, "_window") {
                let func_name: String = get_required(&func_table, "func", context)?;
                let func = parse_window_func(&func_name, context)?;

                // Extract args from the nested table if present
                let mut args = vec![];
                if let Some(args_table) = get_optional::<Table>(&func_table, "args")? {
                    for value in args_table.sequence_values::<Value>() {
                        args.push(parse_expr(value?, context)?);
                    }
                }
                (func, args)
            }
            // Check if it's a measure table (agg = "sum", column = "...")
            else if let Ok(Some(agg_name)) = get_optional::<String>(&func_table, "agg") {
                let func = parse_window_func(&agg_name, context)?;

                // Get the column as argument
                let mut args = vec![];
                if let Ok(col_val) = func_table.get::<Value>("column") {
                    args.push(parse_expr(col_val, context)?);
                }
                (func, args)
            } else {
                // Try to extract func name directly (fallback)
                let func_name: String = get_required(&func_table, "func", context)?;
                let func = parse_window_func(&func_name, context)?;

                let mut args = vec![];
                if let Some(args_table) = get_optional::<Table>(&func_table, "args")? {
                    for value in args_table.sequence_values::<Value>() {
                        args.push(parse_expr(value?, context)?);
                    }
                }
                (func, args)
            }
        }
        Value::String(func_name_str) => {
            // Direct string function name
            let func_name = func_name_str.to_str()?.to_string();
            let func = parse_window_func(&func_name, context)?;
            (func, vec![])
        }
        _ => {
            return Err(mlua::Error::external(format!(
                "Invalid func value in {}. Expected table or string.",
                context
            )));
        }
    };

    // Additional args from the outer table (override inner if present)
    if let Some(args_table) = get_optional::<Table>(table, "args")? {
        args.clear();
        for value in args_table.sequence_values::<Value>() {
            args.push(parse_expr(value?, context)?);
        }
    }

    let mut partition_by = vec![];
    if let Some(part_table) = get_optional::<Table>(table, "partition_by")? {
        for value in part_table.sequence_values::<Value>() {
            partition_by.push(parse_expr(value?, context)?);
        }
    }

    let mut order_by = vec![];
    if let Some(order_table) = get_optional::<Table>(table, "order_by")? {
        for value in order_table.sequence_values::<Value>() {
            order_by.push(parse_order_by_expr(value?, context)?);
        }
    }

    let frame = get_optional::<Table>(table, "frame")?
        .map(|ft| parse_window_frame(&ft, context))
        .transpose()?;

    let data_type = get_optional::<String>(table, "data_type")?.and_then(|s| DataType::parse(&s));

    Ok(WindowColumnDef {
        name: name.to_string(),
        func,
        args,
        partition_by,
        order_by,
        frame,
        data_type,
    })
}

/// Parse window column from method-chaining format: row_number():partition_by(...):order_by(...)
/// The table has _window = true, func = "row_number", plus partition_by, order_by arrays from chaining
fn parse_window_column_def_from_chained(
    name: &str,
    table: &Table,
    context: &str,
) -> LuaResult<WindowColumnDef> {
    // The func name is directly in the table
    let func_name: String = get_required(table, "func", context)?;
    let func = parse_window_func(&func_name, context)?;

    let mut args = vec![];
    if let Some(args_table) = get_optional::<Table>(table, "args")? {
        for value in args_table.sequence_values::<Value>() {
            args.push(parse_expr(value?, context)?);
        }
    }

    // partition_by is an array of column names/expressions from :partition_by() chaining
    let mut partition_by = vec![];
    if let Some(part_table) = get_optional::<Table>(table, "partition_by")? {
        for value in part_table.sequence_values::<Value>() {
            partition_by.push(parse_expr(value?, context)?);
        }
    }

    // order_by is an array of { expr = ..., dir = ... } from :order_by() chaining
    let mut order_by = vec![];
    if let Some(order_table) = get_optional::<Table>(table, "order_by")? {
        for value in order_table.sequence_values::<Value>() {
            order_by.push(parse_order_by_expr(value?, context)?);
        }
    }

    let frame = get_optional::<Table>(table, "frame")?
        .map(|ft| parse_window_frame(&ft, context))
        .transpose()?;

    let data_type = get_optional::<String>(table, "data_type")?.and_then(|s| DataType::parse(&s));

    Ok(WindowColumnDef {
        name: name.to_string(),
        func,
        args,
        partition_by,
        order_by,
        frame,
        data_type,
    })
}

#[allow(dead_code)]
fn parse_window_column_def(name: &str, table: &Table, context: &str) -> LuaResult<WindowColumnDef> {
    let func_name: String = get_required(table, "func", context)?;
    let func = parse_window_func(&func_name, context)?;

    let mut args = vec![];
    if let Some(args_table) = get_optional::<Table>(table, "args")? {
        for value in args_table.sequence_values::<Value>() {
            args.push(parse_expr(value?, context)?);
        }
    }

    let mut partition_by = vec![];
    if let Some(part_table) = get_optional::<Table>(table, "partition_by")? {
        for value in part_table.sequence_values::<Value>() {
            partition_by.push(parse_expr(value?, context)?);
        }
    }

    let mut order_by = vec![];
    if let Some(order_table) = get_optional::<Table>(table, "order_by")? {
        for value in order_table.sequence_values::<Value>() {
            order_by.push(parse_order_by_expr(value?, context)?);
        }
    }

    let frame = get_optional::<Table>(table, "frame")?
        .map(|ft| parse_window_frame(&ft, context))
        .transpose()?;

    let data_type = get_optional::<String>(table, "data_type")?.and_then(|s| DataType::parse(&s));

    Ok(WindowColumnDef {
        name: name.to_string(),
        func,
        args,
        partition_by,
        order_by,
        frame,
        data_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_minimal_source() {
        let lua = r#"
            source("orders"):from("raw.orders")
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.sources.len(), 1);
        assert!(model.sources.contains_key("orders"));
        assert_eq!(model.sources["orders"].table, "raw.orders");
    }

    #[test]
    fn test_load_source_with_columns() {
        let lua = r#"
            source("orders")
                :from("raw.orders")
                :columns({
                    order_id = pk(int64),
                    customer_id = int64,
                    total = { type = decimal(10,2), nullable = false },
                })
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        let orders = &model.sources["orders"];
        assert_eq!(orders.columns.len(), 3);
        assert_eq!(orders.columns["order_id"].data_type, DataType::Int64);
        assert_eq!(orders.columns["total"].data_type, DataType::Decimal(10, 2));
        assert!(!orders.columns["total"].nullable);
        assert_eq!(orders.primary_key, vec!["order_id"]);
    }

    #[test]
    fn test_load_relationship() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("customers"):from("raw.customers")

            relationship {
                from = "orders.customer_id",
                to = "customers.customer_id",
                cardinality = "many_to_one",
            }
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.relationships.len(), 1);
        assert_eq!(model.relationships[0].from_entity, "orders");
        assert_eq!(model.relationships[0].to_entity, "customers");
        assert_eq!(model.relationships[0].cardinality, Cardinality::ManyToOne);
    }

    #[test]
    fn test_load_relationship_with_role() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("date"):from("raw.date_dim")

            -- Role-playing dimension: multiple FKs to same dimension
            link_as(orders.order_date_id, date.date_id, "order_date")
            link_as(orders.ship_date_id, date.date_id, "ship_date")
            link_as(orders.delivery_date_id, date.date_id, "delivery_date")
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.relationships.len(), 3);

        // Check order_date role
        let order_date_rel = model
            .relationships
            .iter()
            .find(|r| r.from_column == "order_date_id")
            .expect("order_date relationship not found");
        assert_eq!(order_date_rel.role, Some("order_date".into()));
        assert_eq!(order_date_rel.from_entity, "orders");
        assert_eq!(order_date_rel.to_entity, "date");

        // Check ship_date role
        let ship_date_rel = model
            .relationships
            .iter()
            .find(|r| r.from_column == "ship_date_id")
            .expect("ship_date relationship not found");
        assert_eq!(ship_date_rel.role, Some("ship_date".into()));

        // Check delivery_date role
        let delivery_date_rel = model
            .relationships
            .iter()
            .find(|r| r.from_column == "delivery_date_id")
            .expect("delivery_date relationship not found");
        assert_eq!(delivery_date_rel.role, Some("delivery_date".into()));
    }

    #[test]
    fn test_link_without_role() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("customers"):from("raw.customers")

            -- Simple link without role
            link(orders.customer_id, customers.customer_id)
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.relationships.len(), 1);
        assert_eq!(model.relationships[0].from_entity, "orders");
        assert_eq!(model.relationships[0].to_entity, "customers");
        assert_eq!(model.relationships[0].role, None);
    }

    #[test]
    fn test_fact_with_date_config() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("date"):from("raw.date_dim")

            link(orders.customer_id, date.date_id)

            fact("fact_orders")
                :target("analytics.fact_orders")
                :grain({ "orders.order_id" })
                :measure("revenue", sum("total"))
                :date_config({
                    dimension = "date",
                    pk_column = "date_id",
                    roles = {
                        order_date = "order_date_id",
                        ship_date = "ship_date_id",
                        delivery_date = "delivery_date_id",
                    },
                    primary_role = "order_date",
                    grain_columns = {
                        year = "cal_year",
                        quarter = "cal_quarter",
                        month = "cal_month",
                        day = "cal_day",
                    },
                })
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        let fact = model.facts.get("fact_orders").expect("fact not found");

        // Verify date_config was parsed
        let date_config = fact.date_config.as_ref().expect("date_config not parsed");

        // Check roles
        assert_eq!(date_config.roles.len(), 3);

        let order_date = date_config
            .get_role("order_date")
            .expect("order_date role not found");
        assert_eq!(order_date.fk_column, "order_date_id");
        assert_eq!(order_date.dimension, "date");
        assert_eq!(order_date.pk_column, "date_id");

        let ship_date = date_config
            .get_role("ship_date")
            .expect("ship_date role not found");
        assert_eq!(ship_date.fk_column, "ship_date_id");

        // Check primary role
        assert_eq!(date_config.primary_role, Some("order_date".into()));
        let primary = date_config
            .get_primary_role()
            .expect("primary role not found");
        assert_eq!(primary.name, "order_date");

        // Check grain columns
        let grain = date_config
            .grain_columns
            .as_ref()
            .expect("grain_columns not parsed");
        assert_eq!(grain.year, "cal_year");
        assert_eq!(grain.quarter, Some("cal_quarter".into()));
        assert_eq!(grain.month, Some("cal_month".into()));
        assert_eq!(grain.day, Some("cal_day".into()));
    }

    #[test]
    fn test_load_fact() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("customers"):from("raw.customers")

            relationship {
                from = "orders.customer_id",
                to = "customers.customer_id",
                cardinality = "many_to_one",
            }

            fact("fact_orders")
                :target("analytics.fact_orders")
                :grain({ "orders.order_id" })
                :include("customers", { "name", "region" })
                :measure("revenue", sum("total"))
                :measure("order_count", count("order_id"))
                :materialized(true)
                :incremental({ key = "order_date", unique_key = { "order_id" } })
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.facts.len(), 1);

        let fact = &model.facts["fact_orders"];
        assert_eq!(fact.target_table, "analytics.fact_orders");
        assert_eq!(fact.grain.len(), 1);
        assert_eq!(fact.grain[0].source_entity, "orders");
        assert_eq!(fact.grain[0].source_column, "order_id");
        assert_eq!(fact.measures.len(), 2);
        assert_eq!(fact.measures["revenue"].aggregation, AggregationType::Sum);
    }

    #[test]
    fn test_load_dimension() {
        let lua = r#"
            source("customers"):from("raw.customers")

            dimension("dim_customers")
                :target("analytics.dim_customers")
                :from("customers")
                :columns({ "customer_id", "name", "region" })
                :primary_key({ "customer_id" })
                :scd({ type = SCD1 })
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.dimensions.len(), 1);

        let dim = &model.dimensions["dim_customers"];
        assert_eq!(dim.source_entity, "customers");
        assert_eq!(dim.columns.len(), 3);
        assert_eq!(dim.scd_type, SCDType::Type1);
    }

    #[test]
    fn test_validation_error() {
        let lua = r#"
            -- Missing source for relationship
            relationship {
                from = "orders.customer_id",
                to = "customers.customer_id",
                cardinality = "many_to_one",
            }
        "#;

        let result = LuaLoader::load_from_str(lua, "test.lua");
        assert!(result.is_err());
    }

    #[test]
    fn test_change_tracking() {
        let lua = r#"
            source("orders")
                :from("raw.orders")
                :metadata({
                    change_tracking = APPEND_ONLY,
                    timestamp_column = "created_at",
                })
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        let orders = &model.sources["orders"];
        match &orders.change_tracking {
            Some(ChangeTracking::AppendOnly { timestamp_column }) => {
                assert_eq!(timestamp_column, "created_at");
            }
            _ => panic!("Expected AppendOnly change tracking"),
        }
    }

    #[test]
    fn test_cardinality_aliases() {
        let lua = r#"
            source("a"):from("a")
            source("b"):from("b")

            relationship { from = "a.id", to = "b.id", cardinality = "1:n" }
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.relationships[0].cardinality, Cardinality::OneToMany);
    }

    #[test]
    fn test_table_as_staging() {
        let lua = r#"
            source("orders"):from("raw.orders")

            table("orders_enriched", { from = "orders", table_type = "Staging" })
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.tables.len(), 1);
        assert!(model.tables.contains_key("orders_enriched"));
        assert_eq!(model.tables["orders_enriched"].from.primary(), "orders");
    }

    #[test]
    fn test_table_in_fact_grain() {
        let lua = r#"
            source("orders"):from("raw.orders")

            table("orders_enriched", { from = "orders", table_type = "Staging" })

            fact("fact_orders")
                :target("analytics.fact_orders")
                :grain({ "orders_enriched.order_id" })
                :from_entity("orders_enriched")
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        model.validate().unwrap();
        assert_eq!(
            model.facts["fact_orders"].from,
            Some("orders_enriched".to_string())
        );
    }

    // =============================================================================
    // Example file tests - validate all examples load and validate successfully
    // =============================================================================

    #[test]
    fn test_example_ecommerce() {
        let path = std::path::Path::new("examples/ecommerce.lua");
        if path.exists() {
            let model =
                crate::model::loader::load_model(path).expect("Failed to load ecommerce.lua");
            model.validate().expect("ecommerce.lua validation failed");
        }
    }

    #[test]
    fn test_example_ecommerce_clean() {
        let path = std::path::Path::new("examples/ecommerce_clean.lua");
        if path.exists() {
            let model =
                crate::model::loader::load_model(path).expect("Failed to load ecommerce_clean.lua");
            model
                .validate()
                .expect("ecommerce_clean.lua validation failed");
        }
    }

    #[test]
    fn test_example_ecommerce_advanced() {
        let path = std::path::Path::new("examples/ecommerce_advanced.lua");
        if path.exists() {
            let model = crate::model::loader::load_model(path)
                .expect("Failed to load ecommerce_advanced.lua");
            model
                .validate()
                .expect("ecommerce_advanced.lua validation failed");
        }
    }

    #[test]
    fn test_example_data_quality() {
        let path = std::path::Path::new("examples/data_quality.lua");
        if path.exists() {
            let model =
                crate::model::loader::load_model(path).expect("Failed to load data_quality.lua");
            model
                .validate()
                .expect("data_quality.lua validation failed");
        }
    }

    #[test]
    fn test_example_saas_metrics() {
        let path = std::path::Path::new("examples/saas_metrics.lua");
        if path.exists() {
            let model =
                crate::model::loader::load_model(path).expect("Failed to load saas_metrics.lua");
            model
                .validate()
                .expect("saas_metrics.lua validation failed");
        }
    }

    #[test]
    fn test_example_materialization_modes() {
        let path = std::path::Path::new("examples/materialization_modes.lua");
        if path.exists() {
            let model = crate::model::loader::load_model(path)
                .expect("Failed to load materialization_modes.lua");
            model
                .validate()
                .expect("materialization_modes.lua validation failed");

            // Verify reports and pivot_reports were loaded
            assert!(!model.reports.is_empty(), "Expected reports to be loaded");
            assert!(
                !model.pivot_reports.is_empty(),
                "Expected pivot_reports to be loaded"
            );

            // Check specific report
            let exec_report = model.get_report("executive_dashboard");
            assert!(exec_report.is_some(), "Expected executive_dashboard report");
            let report = exec_report.unwrap();
            assert_eq!(
                report.measures.len(),
                5,
                "Expected 5 measures in executive_dashboard"
            );
            assert!(
                !report.is_materialized(),
                "executive_dashboard should not be materialized"
            );

            // Check specific pivot report
            let pivot = model.get_pivot_report("quarterly_sales");
            assert!(pivot.is_some(), "Expected quarterly_sales pivot_report");
            let pivot = pivot.unwrap();
            assert_eq!(pivot.rows.len(), 2, "Expected 2 row dimensions");
            assert!(pivot.totals.is_some(), "Expected totals configuration");
            assert!(
                !pivot.is_materialized(),
                "quarterly_sales should not be materialized"
            );

            // Check materialized report (TABLE with refresh_delta)
            let daily_metrics = model.get_report("daily_metrics");
            assert!(daily_metrics.is_some(), "Expected daily_metrics report");
            let report = daily_metrics.unwrap();
            assert!(
                report.is_materialized(),
                "daily_metrics should be materialized"
            );
            let mat = report.materialization.as_ref().unwrap();
            assert_eq!(mat.table_type, crate::model::ReportTableType::Table);
            assert_eq!(mat.target_table, "analytics.rpt_daily_metrics");
            assert!(mat.refresh_delta.is_some());
            assert_eq!(mat.refresh_delta.as_ref().unwrap().seconds, 4 * 3600); // 4 hours

            // Check materialized report (VIEW - no refresh_delta)
            let current_summary = model.get_report("current_summary");
            assert!(current_summary.is_some(), "Expected current_summary report");
            let report = current_summary.unwrap();
            assert!(
                report.is_materialized(),
                "current_summary should be materialized"
            );
            let mat = report.materialization.as_ref().unwrap();
            assert_eq!(mat.table_type, crate::model::ReportTableType::View);
            assert!(
                mat.refresh_delta.is_none(),
                "VIEW should not have refresh_delta"
            );

            // Check materialized pivot report
            let weekly_perf = model.get_pivot_report("weekly_performance");
            assert!(
                weekly_perf.is_some(),
                "Expected weekly_performance pivot_report"
            );
            let pivot = weekly_perf.unwrap();
            assert!(
                pivot.is_materialized(),
                "weekly_performance should be materialized"
            );
            let mat = pivot.materialization.as_ref().unwrap();
            assert_eq!(mat.table_type, crate::model::ReportTableType::Table);
            assert_eq!(mat.refresh_delta.as_ref().unwrap().seconds, 86400); // 1 day
        }
    }

    #[test]
    fn test_example_ecommerce_queries() {
        let path = std::path::Path::new("examples/ecommerce_queries.lua");
        if path.exists() {
            let model = crate::model::loader::load_model(path)
                .expect("Failed to load ecommerce_queries.lua");
            model
                .validate()
                .expect("ecommerce_queries.lua validation failed");

            // Verify structure
            assert_eq!(model.sources.len(), 5, "Expected 5 sources");
            assert_eq!(model.dimensions.len(), 3, "Expected 3 dimensions");
            assert_eq!(model.facts.len(), 2, "Expected 2 facts");
            assert_eq!(model.queries.len(), 17, "Expected 17 queries");

            // Verify basic queries exist
            assert!(
                model.queries.contains_key("revenue_by_country"),
                "Expected revenue_by_country query"
            );
            assert!(
                model.queries.contains_key("top_products_by_volume"),
                "Expected top_products_by_volume query"
            );

            // Verify medium queries exist
            assert!(
                model.queries.contains_key("revenue_by_country_segment"),
                "Expected revenue_by_country_segment query"
            );
            assert!(
                model.queries.contains_key("monthly_category_performance"),
                "Expected monthly_category_performance query"
            );

            // Verify complex queries exist
            assert!(
                model.queries.contains_key("yoy_revenue_comparison"),
                "Expected yoy_revenue_comparison query"
            );
            assert!(
                model.queries.contains_key("rolling_avg_revenue"),
                "Expected rolling_avg_revenue query"
            );
            assert!(
                model.queries.contains_key("country_category_trends"),
                "Expected country_category_trends query"
            );
        }
    }

    /// Integration test that executes all queries from ecommerce_queries.lua
    /// through the semantic layer and verifies SQL generation matches expected output.
    ///
    /// This is a snapshot test - if you change the SQL generation logic, you may need
    /// to update the expected SQL strings below.
    #[test]
    fn test_ecommerce_queries_integration() {
        use crate::dialect::Dialect;
        use crate::semantic::QueryExecutor;

        let path = std::path::Path::new("examples/ecommerce_queries.lua");
        if !path.exists() {
            return; // Skip if file doesn't exist
        }

        let model =
            crate::model::loader::load_model(path).expect("Failed to load ecommerce_queries.lua");

        // Create the query executor
        let executor = QueryExecutor::new(model)
            .expect("Failed to create QueryExecutor")
            .with_default_schema("analytics");

        // Expected SQL output for all 17 queries (snapshot test)
        // This is the CORRECT SQL that the system SHOULD produce.
        // Tests will fail until the SQL generation is fixed.
        let expected_sql: &[(&str, &str)] = &[
            // =================================================================
            // BASIC QUERIES
            // =================================================================
            (
                "revenue_by_country",
                r#"SELECT
  "customers"."country" AS "country",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  COUNT(*) AS "order_count"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."customers" AS "customers" ON "fact_orders"."customer_id" = "customers"."customer_id"
GROUP BY "customers"."country"
ORDER BY SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) DESC"#,
            ),
            (
                "orders_by_status",
                r#"SELECT
  "orders"."status" AS "status",
  COUNT(*) AS "order_count",
  SUM("fact_orders"."line_total") AS "gross_revenue"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."orders" AS "orders" ON "fact_orders"."order_id" = "orders"."order_id"
GROUP BY "orders"."status"
ORDER BY COUNT(*) DESC"#,
            ),
            (
                "top_products_by_volume",
                r#"SELECT
  "products"."name" AS "name",
  "products"."category" AS "category",
  SUM("fact_order_items"."quantity") AS "items_sold"
FROM "analytics"."fact_order_items" AS "fact_order_items"
INNER JOIN "raw"."products" AS "products" ON "fact_order_items"."product_id" = "products"."product_id"
GROUP BY "products"."name", "products"."category"
ORDER BY SUM("fact_order_items"."quantity") DESC
LIMIT 10"#,
            ),
            (
                "yearly_revenue",
                r#"SELECT
  "dates"."year" AS "year",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  COUNT(*) AS "order_count"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
GROUP BY "dates"."year"
ORDER BY "dates"."year" ASC"#,
            ),
            // =================================================================
            // MEDIUM QUERIES
            // =================================================================
            (
                "revenue_by_country_segment",
                r#"SELECT
  "customers"."country" AS "country",
  "customers"."segment" AS "segment",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  COUNT(*) AS "order_count",
  COUNT(DISTINCT "fact_orders"."customer_id") AS "unique_customers",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) / COUNT(*) AS "aov"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."customers" AS "customers" ON "fact_orders"."customer_id" = "customers"."customer_id"
GROUP BY "customers"."country", "customers"."segment"
ORDER BY SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) DESC"#,
            ),
            (
                "monthly_category_performance",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."month" AS "month",
  "dates"."month_name" AS "month_name",
  "products"."category" AS "category",
  SUM("fact_order_items"."item_revenue") AS "revenue",
  SUM("fact_order_items"."quantity") AS "items_sold",
  SUM("fact_order_items"."item_profit") AS "gross_profit",
  SUM("fact_order_items"."item_profit") / SUM("fact_order_items"."item_revenue") * 100.0 AS "profit_margin"
FROM "analytics"."fact_order_items" AS "fact_order_items"
INNER JOIN "raw"."products" AS "products" ON "fact_order_items"."product_id" = "products"."product_id"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_order_items"."order_date" = "dates"."date_key"
GROUP BY "dates"."year", "dates"."month", "dates"."month_name", "products"."category"
ORDER BY "dates"."year" ASC, "dates"."month" ASC, SUM("fact_order_items"."item_revenue") DESC"#,
            ),
            (
                "brand_performance",
                r#"SELECT
  "products"."brand" AS "brand",
  "products"."category" AS "category",
  SUM("fact_order_items"."item_revenue") AS "revenue",
  SUM(CASE WHEN "fact_order_items"."status" = 'completed' THEN "fact_order_items"."item_revenue" END) AS "completed_revenue",
  COUNT(DISTINCT "fact_order_items"."order_id") AS "unique_orders",
  SUM(CASE WHEN "fact_order_items"."status" = 'completed' THEN "fact_order_items"."item_revenue" END) / SUM("fact_order_items"."item_revenue") * 100.0 AS "completion_rate"
FROM "analytics"."fact_order_items" AS "fact_order_items"
INNER JOIN "raw"."products" AS "products" ON "fact_order_items"."product_id" = "products"."product_id"
GROUP BY "products"."brand", "products"."category"
ORDER BY SUM("fact_order_items"."item_revenue") DESC
LIMIT 20"#,
            ),
            (
                "segment_analysis",
                r#"SELECT
  "customers"."segment" AS "segment",
  COUNT(DISTINCT "fact_orders"."customer_id") AS "unique_customers",
  COUNT(*) AS "order_count",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  COUNT(*) / COUNT(DISTINCT "fact_orders"."customer_id") AS "orders_per_customer",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) / COUNT(DISTINCT "fact_orders"."customer_id") AS "revenue_per_customer"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."customers" AS "customers" ON "fact_orders"."customer_id" = "customers"."customer_id"
GROUP BY "customers"."segment"
ORDER BY SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) DESC"#,
            ),
            (
                "quarterly_country_performance",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."quarter" AS "quarter",
  "customers"."country" AS "country",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  COUNT(*) AS "order_count",
  AVG("fact_orders"."line_total") AS "avg_order_value"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
INNER JOIN "raw"."customers" AS "customers" ON "fact_orders"."customer_id" = "customers"."customer_id"
GROUP BY "dates"."year", "dates"."quarter", "customers"."country"
ORDER BY "dates"."year" ASC, "dates"."quarter" ASC, SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) DESC"#,
            ),
            // =================================================================
            // COMPLEX QUERIES (time intelligence)
            // =================================================================
            (
                "yoy_revenue_comparison",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."month" AS "month",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
GROUP BY "dates"."year", "dates"."month"
ORDER BY "dates"."year" ASC, "dates"."month" ASC"#,
            ),
            (
                "ytd_revenue",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."month" AS "month",
  "dates"."month_name" AS "month_name",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
GROUP BY "dates"."year", "dates"."month", "dates"."month_name"
ORDER BY "dates"."year" ASC, "dates"."month" ASC"#,
            ),
            (
                "rolling_avg_revenue",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."month" AS "month",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
GROUP BY "dates"."year", "dates"."month"
ORDER BY "dates"."year" ASC, "dates"."month" ASC"#,
            ),
            (
                "qoq_performance",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."quarter" AS "quarter",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  COUNT(*) AS "order_count"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
GROUP BY "dates"."year", "dates"."quarter"
ORDER BY "dates"."year" ASC, "dates"."quarter" ASC"#,
            ),
            (
                "category_yoy_analysis",
                r#"SELECT
  "dates"."year" AS "year",
  "products"."category" AS "category",
  SUM("fact_order_items"."item_revenue") AS "revenue",
  SUM("fact_order_items"."item_profit") AS "gross_profit",
  SUM("fact_order_items"."item_profit") / SUM("fact_order_items"."item_revenue") * 100.0 AS "profit_margin"
FROM "analytics"."fact_order_items" AS "fact_order_items"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_order_items"."order_date" = "dates"."date_key"
INNER JOIN "raw"."products" AS "products" ON "fact_order_items"."product_id" = "products"."product_id"
GROUP BY "dates"."year", "products"."category"
ORDER BY "dates"."year" ASC, SUM("fact_order_items"."item_revenue") DESC"#,
            ),
            (
                "customer_cohort_revenue",
                r#"SELECT
  "customers"."segment" AS "segment",
  "dates"."year" AS "year",
  COUNT(DISTINCT "fact_orders"."customer_id") AS "unique_customers",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) / COUNT(DISTINCT "fact_orders"."customer_id") AS "revenue_per_customer"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_orders"."order_date" = "dates"."date_key"
INNER JOIN "raw"."customers" AS "customers" ON "fact_orders"."customer_id" = "customers"."customer_id"
GROUP BY "customers"."segment", "dates"."year"
ORDER BY "customers"."segment" ASC, "dates"."year" ASC"#,
            ),
            (
                "country_category_trends",
                r#"SELECT
  "dates"."year" AS "year",
  "dates"."quarter" AS "quarter",
  "customers"."country" AS "country",
  "products"."category" AS "category",
  SUM("fact_order_items"."item_revenue") AS "revenue",
  SUM("fact_order_items"."quantity") AS "items_sold"
FROM "analytics"."fact_order_items" AS "fact_order_items"
INNER JOIN "raw"."dim_date" AS "dates" ON "fact_order_items"."order_date" = "dates"."date_key"
INNER JOIN "raw"."customers" AS "customers" ON "fact_order_items"."customer_id" = "customers"."customer_id"
INNER JOIN "raw"."products" AS "products" ON "fact_order_items"."product_id" = "products"."product_id"
GROUP BY "dates"."year", "dates"."quarter", "customers"."country", "products"."category"
ORDER BY "dates"."year" ASC, "dates"."quarter" ASC, "customers"."country" ASC, SUM("fact_order_items"."item_revenue") DESC"#,
            ),
            (
                "top_customers_lifetime",
                r#"SELECT
  "customers"."email" AS "email",
  "customers"."first_name" AS "first_name",
  "customers"."last_name" AS "last_name",
  "customers"."country" AS "country",
  "customers"."segment" AS "segment",
  COUNT(*) AS "order_count",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) AS "net_revenue",
  SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) / COUNT(*) AS "avg_order_value"
FROM "analytics"."fact_orders" AS "fact_orders"
INNER JOIN "raw"."customers" AS "customers" ON "fact_orders"."customer_id" = "customers"."customer_id"
GROUP BY "customers"."email", "customers"."first_name", "customers"."last_name", "customers"."country", "customers"."segment"
ORDER BY SUM(CASE WHEN "fact_orders"."status" = 'completed' THEN "fact_orders"."line_total" END) DESC
LIMIT 50"#,
            ),
        ];

        // Test each query against expected SQL
        // Note: JOIN order may vary due to HashMap iteration, so we normalize
        // by sorting the lines for comparison
        for (query_name, expected) in expected_sql {
            let result = executor.query_to_sql(query_name, Dialect::DuckDb);
            match result {
                Ok(actual) => {
                    // Normalize by sorting lines (handles non-deterministic JOIN order)
                    fn normalize(s: &str) -> Vec<String> {
                        let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
                        lines.sort();
                        lines
                    }
                    let actual_normalized = normalize(&actual);
                    let expected_normalized = normalize(expected);

                    assert_eq!(
                        actual_normalized, expected_normalized,
                        "\n\nSQL mismatch for query '{}':\n\nExpected:\n{}\n\nActual:\n{}\n",
                        query_name, expected, actual
                    );
                }
                Err(e) => {
                    panic!("Query '{}' failed to generate SQL: {:?}", query_name, e);
                }
            }
        }

        // Verify we tested all 17 queries
        assert_eq!(
            expected_sql.len(),
            17,
            "Expected 17 queries in snapshot test"
        );
    }

    #[test]
    fn test_load_query_basic() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("customers"):from("raw.customers")

            query "sales_by_region" {
                from = "orders",
                select = {
                    "customers.region",
                    "customers.segment",
                    "revenue",
                    "order_count",
                },
                order_by = { desc("revenue") },
                limit = 100,
            }
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        assert_eq!(model.queries.len(), 1);
        assert!(model.queries.contains_key("sales_by_region"));

        let query = &model.queries["sales_by_region"];
        assert_eq!(query.from, Some("orders".into()));
        assert_eq!(query.select.len(), 4);
        assert_eq!(query.limit, Some(100));
        assert_eq!(query.order_by.len(), 1);
        assert!(query.order_by[0].descending);
        assert_eq!(query.order_by[0].field, "revenue");
    }

    #[test]
    fn test_load_query_with_filters() {
        let lua = r#"
            source("orders"):from("raw.orders")
            source("customers"):from("raw.customers")
            source("date"):from("raw.date_dim")

            -- Create entity refs for entity.column syntax
            orders = ref("orders")
            customers = ref("customers")
            date = ref("date")

            query "enterprise_sales" {
                from = "orders",
                select = {
                    customers.region,
                    "revenue",
                },
                where = {
                    gte(date.year, 2024),
                    eq(customers.segment, "Enterprise"),
                },
                limit = 50,
            }
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        let query = &model.queries["enterprise_sales"];

        assert_eq!(query.from, Some("orders".into()));
        assert_eq!(query.select.len(), 2);
        assert_eq!(query.filters.len(), 2);
        assert_eq!(query.limit, Some(50));

        // Check first filter (gte)
        assert_eq!(query.filters[0].field, "date.year");
        assert_eq!(query.filters[0].op, QueryFilterOp::Gte);

        // Check second filter (eq)
        assert_eq!(query.filters[1].field, "customers.segment");
        assert_eq!(query.filters[1].op, QueryFilterOp::Eq);
    }

    #[test]
    fn test_load_query_with_order_helpers() {
        let lua = r#"
            source("orders"):from("raw.orders")
            customers = ref("customers")

            query "sorted_sales" {
                from = "orders",
                select = {
                    customers.region,
                    "revenue",
                },
                order_by = {
                    desc("revenue"),
                    asc(customers.region),
                },
            }
        "#;

        let model = LuaLoader::load_from_str(lua, "test.lua").unwrap();
        let query = &model.queries["sorted_sales"];

        assert_eq!(query.order_by.len(), 2);
        assert_eq!(query.order_by[0].field, "revenue");
        assert!(query.order_by[0].descending);
        assert_eq!(query.order_by[1].field, "customers.region");
        assert!(!query.order_by[1].descending);
    }

    #[test]
    fn test_table_basic() {
        let lua_code = r#"
            source("orders")
                :from("raw.orders")
                :columns({
                    order_id = pk(int64),
                    amount = decimal(10,2),
                })

            table("stg_orders", { from = "orders", table_type = "Staging" })
                :columns({ "order_id", "amount" })
                :tags({ "daily" })
        "#;

        let model = LuaLoader::load_from_str(lua_code, "test.lua").unwrap();
        assert!(model.tables.contains_key("stg_orders"));

        let table = model.tables.get("stg_orders").unwrap();
        assert_eq!(table.from.primary(), "orders");
        assert_eq!(table.tags, vec!["daily"]);
    }

    #[test]
    fn test_table_union() {
        let lua_code = r#"
            source("orders_2023"):from("raw.orders_2023")
            source("orders_2024"):from("raw.orders_2024")

            table("all_orders", { from = { "orders_2023", "orders_2024" }, union_type = "all" })
        "#;

        let model = LuaLoader::load_from_str(lua_code, "test.lua").unwrap();
        let table = model.tables.get("all_orders").unwrap();

        assert!(table.from.is_union());
        assert_eq!(table.from.sources(), vec!["orders_2023", "orders_2024"]);
        assert_eq!(table.union_type, UnionType::All);
    }

    #[test]
    fn test_table_with_filter() {
        let lua_code = r#"
            source("orders"):from("raw.orders")

            table("active_orders", { from = "orders" })
                :filter("status = 'active'")
        "#;

        let model = LuaLoader::load_from_str(lua_code, "test.lua").unwrap();
        let table = model.tables.get("active_orders").unwrap();

        assert!(table.filter.is_some());
    }

    #[test]
    fn test_table_with_inline_config() {
        let lua_code = r#"
            source("orders"):from("raw.orders")

            table("stg_orders", {
                from = "orders",
                table_type = "Staging",
                tags = { "daily", "critical" },
                filter = "status != 'cancelled'",
                columns = { "order_id", "customer_id" },
            })
        "#;

        let model = LuaLoader::load_from_str(lua_code, "test.lua").unwrap();
        let table = model.tables.get("stg_orders").unwrap();

        assert_eq!(table.from.primary(), "orders");
        assert_eq!(table.tags, vec!["daily", "critical"]);
        assert!(table.filter.is_some());
        assert_eq!(table.columns.len(), 2);
    }
}
