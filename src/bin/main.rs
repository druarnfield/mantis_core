//! Mantis CLI - Compile DSL models to SQL
//!
//! Usage:
//!   mantis compile <file.mantis> [--report <name>] [--dialect <dialect>]
//!   mantis list <file.mantis>
//!
//! Examples:
//!   mantis compile examples/sales.mantis --report revenue_summary
//!   mantis compile examples/sales.mantis --report top_revenue --dialect tsql
//!   mantis list examples/sales.mantis

use clap::{Parser, Subcommand, ValueEnum};
use mantis::compile::{compile_first_report, compile_report, CompileOptions};
use mantis::dsl::{self, ast::Item};
use mantis::sql::Dialect;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "mantis")]
#[command(about = "Mantis - A universal semantic layer that compiles to multi-dialect SQL")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compile a Mantis model to SQL
    Compile {
        /// Path to the .mantis file
        file: PathBuf,

        /// Name of the report to compile (compiles first report if not specified)
        #[arg(short, long)]
        report: Option<String>,

        /// SQL dialect to generate
        #[arg(short, long, default_value = "postgres")]
        dialect: DialectArg,

        /// Output format
        #[arg(short, long, default_value = "sql")]
        output: OutputFormat,
    },

    /// List reports in a Mantis model
    List {
        /// Path to the .mantis file
        file: PathBuf,
    },

    /// Validate a Mantis model without generating SQL
    Validate {
        /// Path to the .mantis file
        file: PathBuf,
    },
}

#[derive(Clone, ValueEnum)]
enum DialectArg {
    Postgres,
    Mysql,
    Tsql,
    Duckdb,
    Bigquery,
    Snowflake,
    Databricks,
    Redshift,
}

impl From<DialectArg> for Dialect {
    fn from(arg: DialectArg) -> Self {
        match arg {
            DialectArg::Postgres => Dialect::Postgres,
            DialectArg::Mysql => Dialect::MySql,
            DialectArg::Tsql => Dialect::TSql,
            DialectArg::Duckdb => Dialect::DuckDb,
            DialectArg::Bigquery => Dialect::BigQuery,
            DialectArg::Snowflake => Dialect::Snowflake,
            DialectArg::Databricks => Dialect::Databricks,
            DialectArg::Redshift => Dialect::Redshift,
        }
    }
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    /// Output SQL only
    Sql,
    /// Output SQL with comments
    Verbose,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Compile {
            file,
            report,
            dialect,
            output,
        } => cmd_compile(file, report, dialect, output),
        Commands::List { file } => cmd_list(file),
        Commands::Validate { file } => cmd_validate(file),
    }
}

fn cmd_compile(
    file: PathBuf,
    report: Option<String>,
    dialect: DialectArg,
    output: OutputFormat,
) -> ExitCode {
    // Read the file
    let source = match fs::read_to_string(&file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{}': {}", file.display(), e);
            return ExitCode::FAILURE;
        }
    };

    let options = CompileOptions::default().with_dialect(dialect.into());

    // Compile the report
    let result = match &report {
        Some(name) => compile_report(&source, name, options),
        None => compile_first_report(&source, options),
    };

    match result {
        Ok(compiled) => {
            match output {
                OutputFormat::Sql => {
                    println!("{}", compiled.sql);
                }
                OutputFormat::Verbose => {
                    println!("-- Mantis Compiled SQL");
                    println!("-- Source: {}", file.display());
                    if let Some(name) = &report {
                        println!("-- Report: {}", name);
                    }
                    println!("-- Dialect: {:?}", compiled.dialect);
                    println!();
                    println!("{}", compiled.sql);
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Compilation error: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn cmd_list(file: PathBuf) -> ExitCode {
    // Read the file
    let source = match fs::read_to_string(&file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{}': {}", file.display(), e);
            return ExitCode::FAILURE;
        }
    };

    // Parse the DSL
    let parse_result = dsl::parse(&source);

    if parse_result.model.is_none() {
        eprintln!("Parse errors:");
        for diag in &parse_result.diagnostics {
            eprintln!("  {}", diag);
        }
        return ExitCode::FAILURE;
    }

    let model = parse_result.model.unwrap();

    println!("File: {}", file.display());
    println!();

    // Extract items by type
    let mut tables = Vec::new();
    let mut measures = Vec::new();
    let mut reports = Vec::new();
    let mut calendars = Vec::new();
    let mut dimensions = Vec::new();

    for item in &model.items {
        match &item.value {
            Item::Table(t) => tables.push(t),
            Item::MeasureBlock(m) => measures.push(m),
            Item::Report(r) => reports.push(r),
            Item::Calendar(c) => calendars.push(c),
            Item::Dimension(d) => dimensions.push(d),
        }
    }

    // List calendars
    if !calendars.is_empty() {
        println!("Calendars:");
        for cal in &calendars {
            println!("  - {}", cal.name.value);
        }
        println!();
    }

    // List tables
    if !tables.is_empty() {
        println!("Tables:");
        for table in &tables {
            println!(
                "  - {} (source: \"{}\")",
                table.name.value, table.source.value
            );
        }
        println!();
    }

    // List dimensions
    if !dimensions.is_empty() {
        println!("Dimensions:");
        for dim in &dimensions {
            println!("  - {} (source: \"{}\")", dim.name.value, dim.source.value);
        }
        println!();
    }

    // List measures
    if !measures.is_empty() {
        println!("Measures:");
        for measure_block in &measures {
            println!("  {}:", measure_block.table.value);
            for measure in &measure_block.measures {
                println!("    - {}", measure.name.value);
            }
        }
        println!();
    }

    // List reports
    if !reports.is_empty() {
        println!("Reports:");
        for report in &reports {
            let from_tables: Vec<_> = report.from.iter().map(|s| s.value.as_str()).collect();
            println!(
                "  - {} (from: {})",
                report.name.value,
                from_tables.join(", ")
            );
        }
    } else {
        println!("No reports defined.");
    }

    ExitCode::SUCCESS
}

fn cmd_validate(file: PathBuf) -> ExitCode {
    // Read the file
    let source = match fs::read_to_string(&file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{}': {}", file.display(), e);
            return ExitCode::FAILURE;
        }
    };

    // Parse the DSL
    let parse_result = dsl::parse(&source);

    if !parse_result.diagnostics.is_empty() {
        eprintln!("Validation errors:");
        for diag in &parse_result.diagnostics {
            eprintln!("  {}", diag);
        }
        return ExitCode::FAILURE;
    }

    if parse_result.model.is_none() {
        eprintln!("Failed to parse model");
        return ExitCode::FAILURE;
    }

    println!("OK: {} is valid", file.display());
    ExitCode::SUCCESS
}
