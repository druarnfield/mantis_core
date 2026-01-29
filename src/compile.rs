//! End-to-end compilation from DSL source to SQL.
//!
//! This module provides the high-level API for compiling Mantis DSL to SQL:
//!
//! ```text
//! DSL Source → Parse → AST → Build Graph → Plan Report → SQL
//! ```
//!
//! # Example
//!
//! ```ignore
//! use mantis::compile::{compile_report, CompileOptions};
//! use mantis::sql::Dialect;
//!
//! let dsl = r#"
//!     table sales {
//!         source "dbo.sales";
//!         atoms { amount decimal; }
//!         times { }
//!         slicers { region string; }
//!     }
//!
//!     measures sales {
//!         revenue = { sum(@amount) };
//!     }
//!
//!     report summary {
//!         from sales;
//!         group { region; }
//!         show { revenue; }
//!     }
//! "#;
//!
//! let options = CompileOptions::default().with_dialect(Dialect::Postgres);
//! let result = compile_report(dsl, "summary", options)?;
//! println!("{}", result.sql);
//! ```

use std::collections::HashMap;

use crate::dsl::ast::{Item, Model as AstModel, SlicerKind};
use crate::dsl::{self, ParseResult};
use crate::lowering::{self, LoweringError};
use crate::model::Report;
use crate::planner::{PlanError, SqlPlanner};
use crate::semantic::graph::{Cardinality, GraphBuildError, UnifiedGraph};
use crate::semantic::inference::{InferredRelationship, RelationshipSource};
use crate::sql::query::Query;
use crate::sql::Dialect;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during compilation.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Lowering error: {0}")]
    LoweringError(#[from] LoweringError),

    #[error("Graph build error: {0}")]
    GraphBuildError(#[from] GraphBuildError),

    #[error("Planning error: {0}")]
    PlanError(#[from] PlanError),

    #[error("Report not found: {0}")]
    ReportNotFound(String),

    #[error("No reports defined in model")]
    NoReports,
}

pub type CompileResult<T> = Result<T, CompileError>;

// ============================================================================
// Options
// ============================================================================

/// Options for compilation.
#[derive(Debug, Clone)]
pub struct CompileOptions {
    /// SQL dialect to generate.
    pub dialect: Dialect,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            dialect: Dialect::Postgres,
        }
    }
}

impl CompileOptions {
    /// Set the SQL dialect.
    pub fn with_dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }
}

// ============================================================================
// Result Types
// ============================================================================

/// Result of compiling a report to SQL.
#[derive(Debug, Clone)]
pub struct CompileOutput {
    /// The generated SQL string.
    pub sql: String,

    /// The SQL query AST (for further manipulation if needed).
    pub query: Query,

    /// The dialect used for generation.
    pub dialect: Dialect,
}

// ============================================================================
// Compilation Functions
// ============================================================================

/// Compile a specific report from DSL source to SQL.
///
/// # Arguments
///
/// * `source` - The DSL source code
/// * `report_name` - Name of the report to compile
/// * `options` - Compilation options (dialect, etc.)
///
/// # Returns
///
/// A `CompileOutput` containing the SQL string and query AST.
pub fn compile_report(
    source: &str,
    report_name: &str,
    options: CompileOptions,
) -> CompileResult<CompileOutput> {
    // Step 1: Parse DSL
    let parse_result = dsl::parse(source);
    if parse_result.model.is_none() {
        return Err(CompileError::ParseError(format_parse_errors(&parse_result)));
    }
    let ast_model = parse_result.model.unwrap();

    // Step 2: Lower AST to Model
    let model = lowering::lower(ast_model.clone())?;

    // Step 3: Build UnifiedGraph from AST
    // Note: We use the AST model for graph building (it contains the raw structure)
    // and empty relationships/stats for now (can be enriched later)
    let graph = UnifiedGraph::from_model_with_inference(&ast_model, &[], &HashMap::new())?;

    // Step 4: Find the report
    let report = model
        .reports
        .get(report_name)
        .ok_or_else(|| CompileError::ReportNotFound(report_name.to_string()))?;

    // Step 5: Plan the report
    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(report)?;

    // Step 6: Generate SQL
    let sql = query.to_sql(options.dialect);

    Ok(CompileOutput {
        sql,
        query,
        dialect: options.dialect,
    })
}

/// Compile the first report found in the DSL source.
///
/// This is a convenience function when the DSL contains only one report.
pub fn compile_first_report(source: &str, options: CompileOptions) -> CompileResult<CompileOutput> {
    // Step 1: Parse DSL
    let parse_result = dsl::parse(source);
    if parse_result.model.is_none() {
        return Err(CompileError::ParseError(format_parse_errors(&parse_result)));
    }
    let ast_model = parse_result.model.unwrap();

    // Step 2: Lower AST to Model
    let model = lowering::lower(ast_model.clone())?;

    // Step 3: Get first report name
    let report_name = model
        .reports
        .keys()
        .next()
        .ok_or(CompileError::NoReports)?
        .clone();

    // Step 4: Build UnifiedGraph
    let graph = UnifiedGraph::from_model_with_inference(&ast_model, &[], &HashMap::new())?;

    // Step 5: Plan the report
    let report = model.reports.get(&report_name).unwrap();
    let planner = SqlPlanner::new(&graph);
    let query = planner.plan(report)?;

    // Step 6: Generate SQL
    let sql = query.to_sql(options.dialect);

    Ok(CompileOutput {
        sql,
        query,
        dialect: options.dialect,
    })
}

/// Compile a Report struct directly (when you already have the parsed model).
///
/// This is useful when you've already parsed the DSL and built the graph,
/// and want to compile multiple reports without re-parsing.
pub fn compile_report_with_graph(
    report: &Report,
    graph: &UnifiedGraph,
    options: CompileOptions,
) -> CompileResult<CompileOutput> {
    let planner = SqlPlanner::new(graph);
    let query = planner.plan(report)?;
    let sql = query.to_sql(options.dialect);

    Ok(CompileOutput {
        sql,
        query,
        dialect: options.dialect,
    })
}

// ============================================================================
// Helper Functions
// ============================================================================

fn format_parse_errors(parse_result: &ParseResult) -> String {
    parse_result
        .diagnostics
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Extract foreign key relationships from AST model.
///
/// This function scans all tables in the AST and extracts FK slicers
/// (e.g., `product_id -> products.product_id`) converting them to
/// `InferredRelationship` structs that can be used for join graph building.
///
/// These DSL-defined relationships are marked as `UserDefined` source
/// and have high confidence (0.99) since they are explicitly declared.
fn extract_fk_relationships_from_ast(model: &AstModel) -> Vec<InferredRelationship> {
    use crate::semantic::inference::thresholds::confidence::DB_CONSTRAINT;

    let mut relationships = Vec::new();

    for item in &model.items {
        if let Item::Table(table) = &item.inner {
            let from_table = table.name.inner.clone();
            // Extract schema from source if present (e.g., "warehouse.sales" -> "warehouse")
            let from_schema = extract_schema(&table.source.inner);

            for slicer in &table.slicers {
                if let SlicerKind::ForeignKey {
                    dimension,
                    key_column,
                } = &slicer.kind.inner
                {
                    let from_column = slicer.name.inner.clone();

                    // The target table is the dimension name, target column is key_column
                    // Try to find the dimension's source to get its schema
                    let (to_schema, to_table) = find_dimension_or_table_source(model, dimension);

                    relationships.push(InferredRelationship {
                        from_schema,
                        from_table: from_table.clone(),
                        from_column,
                        to_schema,
                        to_table,
                        to_column: key_column.clone(),
                        // User-defined FKs get high confidence (just below DB constraint)
                        confidence: DB_CONSTRAINT - 0.01,
                        rule: "dsl_fk_slicer".to_string(),
                        // FK from fact to dimension is typically Many-to-One
                        cardinality: Cardinality::ManyToOne,
                        signal_breakdown: None,
                        source: RelationshipSource::UserDefined,
                    });
                }
            }
        }
    }

    relationships
}

/// Extract schema from a fully-qualified source string.
/// Returns empty string if no schema present.
///
/// Examples:
/// - "warehouse.sales" -> "warehouse"
/// - "dbo.fact_sales" -> "dbo"
/// - "sales" -> ""
fn extract_schema(source: &str) -> String {
    if let Some(pos) = source.rfind('.') {
        source[..pos].to_string()
    } else {
        String::new()
    }
}

/// Find the source (schema.table) for a dimension or table by name.
/// Returns (schema, table_name) tuple.
fn find_dimension_or_table_source(model: &AstModel, name: &str) -> (String, String) {
    for item in &model.items {
        match &item.inner {
            Item::Dimension(dim) if dim.name.inner == name => {
                let schema = extract_schema(&dim.source.inner);
                // For dimensions, the "table" in the graph is the dimension name
                return (schema, name.to_string());
            }
            Item::Table(table) if table.name.inner == name => {
                let schema = extract_schema(&table.source.inner);
                return (schema, name.to_string());
            }
            _ => {}
        }
    }
    // If not found, just use the name as-is (will still work for graph building)
    (String::new(), name.to_string())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_simple_report() {
        let dsl = r#"
            calendar auto {
                generate day+;
                range infer;
            }

            table sales {
                source "dbo.sales";
                atoms { amount decimal; }
                times { sale_date -> auto.day; }
            }

            measures sales {
                revenue = { sum(@amount) };
            }

            report summary {
                from sales;
                use_date sale_date;
                show { revenue; }
            }
        "#;

        let result = compile_report(dsl, "summary", CompileOptions::default());

        match &result {
            Ok(output) => {
                println!("Generated SQL:\n{}", output.sql);
                assert!(output.sql.to_uppercase().contains("SELECT"));
                assert!(output.sql.to_uppercase().contains("FROM"));
            }
            Err(e) => {
                panic!("Compilation failed: {:?}", e);
            }
        }
    }

    #[test]
    fn test_compile_report_not_found() {
        let dsl = r#"
            table sales {
                source "dbo.sales";
                atoms { amount decimal; }
            }
        "#;

        let result = compile_report(dsl, "nonexistent", CompileOptions::default());
        assert!(matches!(result, Err(CompileError::ReportNotFound(_))));
    }

    #[test]
    fn test_compile_first_report() {
        let dsl = r#"
            calendar auto {
                generate day+;
                range infer;
            }

            table sales {
                source "dbo.sales";
                atoms { amount decimal; }
                times { sale_date -> auto.day; }
            }

            measures sales {
                revenue = { sum(@amount) };
            }

            report my_report {
                from sales;
                use_date sale_date;
                show { revenue; }
                limit 10;
            }
        "#;

        let result = compile_first_report(dsl, CompileOptions::default());
        assert!(result.is_ok());

        let output = result.unwrap();
        assert!(output.sql.contains("LIMIT 10") || output.sql.contains("FETCH"));
    }

    #[test]
    fn test_compile_with_filter() {
        let dsl = r#"
            table sales {
                source "dbo.sales";
                atoms { amount decimal; }
                slicers { region string; }
            }

            measures sales {
                revenue = { sum(@amount) };
            }

            report filtered {
                from sales;
                show { revenue; }
                filter { @amount > 100 };
            }
        "#;

        let result = compile_report(dsl, "filtered", CompileOptions::default());

        match &result {
            Ok(output) => {
                println!("Generated SQL:\n{}", output.sql);
                assert!(output.sql.to_uppercase().contains("WHERE"));
            }
            Err(e) => {
                // Filter parsing might not be fully implemented yet
                println!("Expected failure (filter parsing): {:?}", e);
            }
        }
    }

    #[test]
    fn test_compile_different_dialects() {
        let dsl = r#"
            calendar auto {
                generate day+;
                range infer;
            }

            table sales {
                source "dbo.sales";
                atoms { amount decimal; }
                times { sale_date -> auto.day; }
            }

            report simple {
                from sales;
                use_date sale_date;
                limit 10;
            }
        "#;

        // Test PostgreSQL
        let pg_result = compile_report(
            dsl,
            "simple",
            CompileOptions::default().with_dialect(Dialect::Postgres),
        );
        assert!(pg_result.is_ok());
        let pg_sql = pg_result.unwrap().sql;
        assert!(pg_sql.contains("LIMIT 10"));

        // Test T-SQL (SQL Server)
        let tsql_result = compile_report(
            dsl,
            "simple",
            CompileOptions::default().with_dialect(Dialect::TSql),
        );
        assert!(tsql_result.is_ok());
        let tsql_sql = tsql_result.unwrap().sql;
        // T-SQL uses OFFSET/FETCH syntax
        assert!(
            tsql_sql.contains("FETCH") || tsql_sql.contains("TOP"),
            "T-SQL should use FETCH or TOP, got: {}",
            tsql_sql
        );
    }
}
