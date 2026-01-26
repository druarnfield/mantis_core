//! Schema generator for test databases.
//!
//! Generates DDL for PostgreSQL, MSSQL (T-SQL), and DuckDB from a single schema definition.
//!
//! Usage:
//!   cargo run --bin schema_gen -- postgres    # Generate PostgreSQL DDL
//!   cargo run --bin schema_gen -- mssql       # Generate MSSQL DDL
//!   cargo run --bin schema_gen -- duckdb      # Generate DuckDB DDL
//!   cargo run --bin schema_gen -- all         # Generate all (default)
//!   cargo run --bin schema_gen               # Same as "all"

use mantis::sql::ddl::{ColumnDef, CreateTable, DataType, TableConstraint};
use mantis::sql::dialect::Dialect;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// Define the retail star-schema tables.
fn define_schema() -> Vec<CreateTable> {
    vec![
        // dim_dates: Date dimension table
        CreateTable::new("dim_dates")
            .column(ColumnDef::new("date_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("date_id", DataType::Date).not_null())
            .column(ColumnDef::new("day_of_week", DataType::Int32).not_null())
            .column(ColumnDef::new("day_of_month", DataType::Int32).not_null())
            .column(ColumnDef::new("month", DataType::Int32).not_null())
            .column(ColumnDef::new("quarter", DataType::Int32).not_null())
            .column(ColumnDef::new("year", DataType::Int32).not_null()),

        // dim_customers: Customer dimension table
        CreateTable::new("dim_customers")
            .column(ColumnDef::new("customer_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("customer_id", DataType::Varchar(50)).not_null())
            .column(ColumnDef::new("first_name", DataType::Varchar(100)).not_null())
            .column(ColumnDef::new("last_name", DataType::Varchar(100)).not_null())
            .column(ColumnDef::new("email", DataType::Varchar(255)))
            .column(ColumnDef::new("residential_location", DataType::Varchar(255)))
            .column(ColumnDef::new("customer_segment", DataType::Varchar(50))),

        // dim_products: Product dimension table
        CreateTable::new("dim_products")
            .column(ColumnDef::new("product_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("product_id", DataType::Varchar(50)).not_null())
            .column(ColumnDef::new("product_name", DataType::Varchar(255)).not_null())
            .column(ColumnDef::new("category", DataType::Varchar(100)))
            .column(ColumnDef::new("brand", DataType::Varchar(100)))
            .column(ColumnDef::new("origin_location", DataType::Varchar(255))),

        // dim_stores: Store dimension table
        CreateTable::new("dim_stores")
            .column(ColumnDef::new("store_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("store_id", DataType::Varchar(50)).not_null())
            .column(ColumnDef::new("store_name", DataType::Varchar(255)).not_null())
            .column(ColumnDef::new("store_location", DataType::Varchar(255)))
            .column(ColumnDef::new("store_type", DataType::Varchar(50))),

        // dim_salespersons: Salesperson dimension table
        CreateTable::new("dim_salespersons")
            .column(ColumnDef::new("salesperson_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("salesperson_id", DataType::Varchar(50)).not_null())
            .column(ColumnDef::new("first_name", DataType::Varchar(100)).not_null())
            .column(ColumnDef::new("last_name", DataType::Varchar(100)).not_null())
            .column(ColumnDef::new("email", DataType::Varchar(255)))
            .column(ColumnDef::new("hire_date", DataType::Date))
            .column(ColumnDef::new("territory", DataType::Varchar(100))),

        // dim_campaigns: Campaign dimension table
        CreateTable::new("dim_campaigns")
            .column(ColumnDef::new("campaign_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("campaign_id", DataType::Varchar(50)).not_null())
            .column(ColumnDef::new("campaign_name", DataType::Varchar(255)).not_null())
            .column(ColumnDef::new("campaign_type", DataType::Varchar(50)))
            .column(ColumnDef::new("start_date", DataType::Date))
            .column(ColumnDef::new("end_date", DataType::Date)),

        // fact_sales_normalized: Fact table with foreign keys
        CreateTable::new("fact_sales_normalized")
            .column(ColumnDef::new("sales_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("sales_id", DataType::Varchar(50)).not_null())
            .column(ColumnDef::new("customer_sk", DataType::Int64).not_null())
            .column(ColumnDef::new("product_sk", DataType::Int64).not_null())
            .column(ColumnDef::new("store_sk", DataType::Int64).not_null())
            .column(ColumnDef::new("salesperson_sk", DataType::Int64).not_null())
            .column(ColumnDef::new("campaign_sk", DataType::Int64))
            .column(ColumnDef::new("sales_date", DataType::Date).not_null())
            .column(ColumnDef::new("total_amount", DataType::Decimal(18, 2)).not_null())
            .constraint(TableConstraint::foreign_key(["customer_sk"], "dim_customers", ["customer_sk"]))
            .constraint(TableConstraint::foreign_key(["product_sk"], "dim_products", ["product_sk"]))
            .constraint(TableConstraint::foreign_key(["store_sk"], "dim_stores", ["store_sk"]))
            .constraint(TableConstraint::foreign_key(["salesperson_sk"], "dim_salespersons", ["salesperson_sk"]))
            .constraint(TableConstraint::foreign_key(["campaign_sk"], "dim_campaigns", ["campaign_sk"])),
    ]
}

/// Generate DDL for a specific dialect.
fn generate_ddl(dialect: Dialect) -> String {
    let tables = define_schema();
    let mut ddl = String::new();

    ddl.push_str(&format!(
        "-- Schema for {} database\n-- Generated by schema_gen\n\n",
        dialect.to_string().to_uppercase()
    ));

    for table in tables {
        ddl.push_str(&table.to_sql(dialect));
        ddl.push_str(";\n\n");
    }

    ddl
}

/// Write DDL to a file in the database_test directory.
fn write_schema_file(dialect: Dialect, filename: &str) -> io::Result<()> {
    let ddl = generate_ddl(dialect);

    // Get the path to database_test directory (relative to working directory)
    let output_path = Path::new("database_test").join(filename);

    // Ensure directory exists
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = fs::File::create(&output_path)?;
    file.write_all(ddl.as_bytes())?;

    println!("Generated {}", output_path.display());
    Ok(())
}

/// Print DDL to stdout.
fn print_ddl(dialect: Dialect) {
    let ddl = generate_ddl(dialect);
    print!("{}", ddl);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let target = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    match target {
        "postgres" => print_ddl(Dialect::Postgres),
        "mssql" | "tsql" => print_ddl(Dialect::TSql),
        "duckdb" => print_ddl(Dialect::DuckDb),
        "all" => {
            // Write schema files to database_test directory
            if let Err(e) = write_schema_file(Dialect::Postgres, "schema_postgres.sql") {
                eprintln!("Error writing PostgreSQL schema: {}", e);
                std::process::exit(1);
            }
            if let Err(e) = write_schema_file(Dialect::TSql, "schema_mssql.sql") {
                eprintln!("Error writing MSSQL schema: {}", e);
                std::process::exit(1);
            }
            if let Err(e) = write_schema_file(Dialect::DuckDb, "schema_duckdb.sql") {
                eprintln!("Error writing DuckDB schema: {}", e);
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("Usage: schema_gen [postgres|mssql|duckdb|all]");
            eprintln!("  postgres  - Generate PostgreSQL DDL to stdout");
            eprintln!("  mssql     - Generate MSSQL (T-SQL) DDL to stdout");
            eprintln!("  duckdb    - Generate DuckDB DDL to stdout");
            eprintln!("  all       - Generate all schema files to database_test/");
            std::process::exit(1);
        }
    }
}
