# Database Test Environment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a Docker-based test environment with PostgreSQL, MSSQL, and DuckDB for integration testing.

**Architecture:** Extract DDL functionality into a self-contained module with its own SQL-level DataType enum, then create Docker Compose + Taskfile infrastructure for database management.

**Tech Stack:** Rust (DDL generation), Docker Compose, Taskfile, PostgreSQL 16, MSSQL 2022, DuckDB

---

## Task 1: Create SQL-level DataType enum

**Files:**
- Create: `src/sql/types.rs`
- Modify: `src/sql/mod.rs`

**Step 1: Create the SQL types module**

Create `src/sql/types.rs` with the SQL-level DataType enum:

```rust
//! SQL data types for DDL generation.
//!
//! These are SQL-level types with precision/scale, distinct from the
//! semantic model's high-level DataType enum.

use serde::{Deserialize, Serialize};

/// SQL data types with precision/scale where applicable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// Boolean type
    Bool,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// Decimal with precision and scale
    Decimal(u8, u8),
    /// Variable-length string (unbounded)
    String,
    /// Fixed-length string
    Char(u16),
    /// Variable-length string with max length
    Varchar(u16),
    /// Date (no time component)
    Date,
    /// Time (no date component)
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    TimestampTz,
    /// Binary data
    Binary,
    /// JSON data
    Json,
    /// UUID
    Uuid,
}

impl DataType {
    /// Parse a type string like "decimal(10,2)" or "varchar(255)"
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.to_lowercase();
        let s = s.trim();

        // Handle parameterized types
        if let Some(inner) = s.strip_prefix("decimal(").and_then(|s| s.strip_suffix(')')) {
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() == 2 {
                let precision = parts[0].trim().parse().ok()?;
                let scale = parts[1].trim().parse().ok()?;
                return Some(DataType::Decimal(precision, scale));
            }
        }

        if let Some(inner) = s.strip_prefix("varchar(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Varchar(len));
        }

        if let Some(inner) = s.strip_prefix("nvarchar(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Varchar(len));
        }

        if let Some(inner) = s.strip_prefix("char(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Char(len));
        }

        if let Some(inner) = s.strip_prefix("nchar(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Char(len));
        }

        // Simple types
        match s {
            "bool" | "boolean" => Some(DataType::Bool),
            "int8" | "tinyint" => Some(DataType::Int8),
            "int16" | "smallint" => Some(DataType::Int16),
            "int32" | "int" | "integer" => Some(DataType::Int32),
            "int64" | "bigint" => Some(DataType::Int64),
            "float32" | "float" | "real" => Some(DataType::Float32),
            "float64" | "double" => Some(DataType::Float64),
            "string" | "text" | "ntext" => Some(DataType::String),
            "date" => Some(DataType::Date),
            "time" => Some(DataType::Time),
            "timestamp" | "datetime" => Some(DataType::Timestamp),
            "timestamptz" | "datetimeoffset" => Some(DataType::TimestampTz),
            "binary" | "blob" | "varbinary" => Some(DataType::Binary),
            "json" | "jsonb" => Some(DataType::Json),
            "uuid" | "uniqueidentifier" => Some(DataType::Uuid),
            _ => None,
        }
    }
}
```

**Step 2: Export from sql/mod.rs**

Add to `src/sql/mod.rs`:
```rust
pub mod types;
pub use types::DataType as SqlDataType;
```

**Step 3: Verify module compiles**

Run: `cargo check --lib`
Expected: No new errors related to sql/types.rs

**Step 4: Commit**

```bash
git add src/sql/types.rs src/sql/mod.rs
git commit -m "feat(sql): add SQL-level DataType enum for DDL generation"
```

---

## Task 2: Update DDL module to use SQL DataType

**Files:**
- Modify: `src/sql/ddl.rs`

**Step 1: Update DDL imports**

In `src/sql/ddl.rs`, change line 26:
```rust
// Old:
pub use crate::model::types::DataType;

// New:
pub use super::types::DataType;
```

**Step 2: Verify DDL module compiles**

Run: `cargo check --lib`
Expected: Compilation succeeds (tests may still fail)

**Step 3: Commit**

```bash
git add src/sql/ddl.rs
git commit -m "refactor(ddl): use SQL-level DataType from sql::types"
```

---

## Task 3: Update dialect helpers to use SQL DataType

**Files:**
- Modify: `src/sql/dialect/helpers.rs`

**Step 1: Update import and emit functions**

In `src/sql/dialect/helpers.rs`, update the DataType import and all `emit_data_type_*` functions to handle the new SQL-level types:

```rust
// Change import from:
use crate::model::types::DataType;

// To:
use crate::sql::types::DataType;
```

Update `emit_data_type_ansi`:
```rust
pub fn emit_data_type_ansi(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOLEAN".into(),
        DataType::Int8 => "SMALLINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "TEXT".into(),
        DataType::Char(n) => format!("CHAR({})", n),
        DataType::Varchar(n) => format!("VARCHAR({})", n),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "TIMESTAMP".into(),
        DataType::TimestampTz => "TIMESTAMPTZ".into(),
        DataType::Binary => "BYTEA".into(),
        DataType::Json => "JSONB".into(),
        DataType::Uuid => "UUID".into(),
    }
}
```

Update `emit_data_type_tsql`:
```rust
pub fn emit_data_type_tsql(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BIT".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "FLOAT".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "NVARCHAR(MAX)".into(),
        DataType::Char(n) => format!("NCHAR({})", n),
        DataType::Varchar(n) => format!("NVARCHAR({})", n),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "DATETIME2".into(),
        DataType::TimestampTz => "DATETIMEOFFSET".into(),
        DataType::Binary => "VARBINARY(MAX)".into(),
        DataType::Json => "NVARCHAR(MAX)".into(),
        DataType::Uuid => "UNIQUEIDENTIFIER".into(),
    }
}
```

Update `emit_data_type_mysql`:
```rust
pub fn emit_data_type_mysql(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "TINYINT(1)".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        DataType::String => "TEXT".into(),
        DataType::Char(n) => format!("CHAR({})", n),
        DataType::Varchar(n) => format!("VARCHAR({})", n),
        DataType::Date => "DATE".into(),
        DataType::Time => "TIME".into(),
        DataType::Timestamp => "DATETIME".into(),
        DataType::TimestampTz => "DATETIME".into(),
        DataType::Binary => "BLOB".into(),
        DataType::Json => "JSON".into(),
        DataType::Uuid => "CHAR(36)".into(),
    }
}
```

Similarly update `emit_data_type_snowflake`, `emit_data_type_bigquery`, `emit_data_type_databricks`.

**Step 2: Verify helpers compile**

Run: `cargo check --lib`
Expected: No errors

**Step 3: Commit**

```bash
git add src/sql/dialect/helpers.rs
git commit -m "refactor(dialect): update helpers to use SQL-level DataType"
```

---

## Task 4: Fix DDL tests

**Files:**
- Modify: `src/sql/ddl.rs` (test section)

**Step 1: Update test imports**

The tests in `src/sql/ddl.rs` already use `DataType` from the module. Verify the tests use correct variants.

**Step 2: Run DDL tests**

Run: `cargo test ddl --lib`
Expected: All DDL tests pass

**Step 3: If tests fail, fix any remaining issues**

Common fixes needed:
- Update test data type references to use SQL-level types
- Fix any snapshot tests that changed output

**Step 4: Commit**

```bash
git add src/sql/ddl.rs
git commit -m "fix(ddl): update tests to use SQL-level DataType"
```

---

## Task 5: Create schema_gen binary

**Files:**
- Create: `src/bin/schema_gen.rs`
- Modify: `Cargo.toml`

**Step 1: Create the schema generator binary**

Create `src/bin/schema_gen.rs`:

```rust
//! Generate DDL for test databases from a single schema definition.

use mantis::sql::ddl::{ColumnDef, CreateTable, DataType, TableConstraint};
use mantis::sql::dialect::Dialect;
use std::fs;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dialect = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    let tables = define_schema();

    match dialect {
        "postgres" => println!("{}", generate_ddl(&tables, Dialect::Postgres)),
        "mssql" => println!("{}", generate_ddl(&tables, Dialect::TSql)),
        "duckdb" => println!("{}", generate_ddl(&tables, Dialect::DuckDb)),
        "all" | _ => {
            let base = Path::new("database_test");
            fs::create_dir_all(base).expect("Failed to create database_test directory");

            fs::write(
                base.join("schema_postgres.sql"),
                generate_ddl(&tables, Dialect::Postgres),
            ).expect("Failed to write postgres schema");

            fs::write(
                base.join("schema_mssql.sql"),
                generate_ddl(&tables, Dialect::TSql),
            ).expect("Failed to write mssql schema");

            fs::write(
                base.join("schema_duckdb.sql"),
                generate_ddl(&tables, Dialect::DuckDb),
            ).expect("Failed to write duckdb schema");

            println!("Generated schema files in database_test/");
        }
    }
}

fn generate_ddl(tables: &[CreateTable], dialect: Dialect) -> String {
    tables
        .iter()
        .map(|t| format!("{};\n", t.to_sql(dialect)))
        .collect::<Vec<_>>()
        .join("\n")
}

fn define_schema() -> Vec<CreateTable> {
    vec![
        // dim_dates
        CreateTable::new("dim_dates")
            .column(ColumnDef::new("date_sk", DataType::Int32).not_null().primary_key())
            .column(ColumnDef::new("date_id", DataType::Date).not_null())
            .column(ColumnDef::new("day_of_week", DataType::Varchar(20)))
            .column(ColumnDef::new("day_of_month", DataType::Int32))
            .column(ColumnDef::new("month", DataType::Int32))
            .column(ColumnDef::new("quarter", DataType::Int32))
            .column(ColumnDef::new("year", DataType::Int32)),

        // dim_customers
        CreateTable::new("dim_customers")
            .column(ColumnDef::new("customer_sk", DataType::Int32).not_null().primary_key())
            .column(ColumnDef::new("customer_id", DataType::Varchar(20)).not_null())
            .column(ColumnDef::new("first_name", DataType::Varchar(100)))
            .column(ColumnDef::new("last_name", DataType::Varchar(100)))
            .column(ColumnDef::new("email", DataType::Varchar(255)))
            .column(ColumnDef::new("residential_location", DataType::Varchar(100)))
            .column(ColumnDef::new("customer_segment", DataType::Varchar(50))),

        // dim_products
        CreateTable::new("dim_products")
            .column(ColumnDef::new("product_sk", DataType::Int32).not_null().primary_key())
            .column(ColumnDef::new("product_id", DataType::Varchar(20)).not_null())
            .column(ColumnDef::new("product_name", DataType::Varchar(255)))
            .column(ColumnDef::new("category", DataType::Varchar(100)))
            .column(ColumnDef::new("brand", DataType::Varchar(100)))
            .column(ColumnDef::new("origin_location", DataType::Varchar(100))),

        // dim_stores
        CreateTable::new("dim_stores")
            .column(ColumnDef::new("store_sk", DataType::Int32).not_null().primary_key())
            .column(ColumnDef::new("store_id", DataType::Varchar(20)).not_null())
            .column(ColumnDef::new("store_name", DataType::Varchar(255)))
            .column(ColumnDef::new("store_location", DataType::Varchar(100)))
            .column(ColumnDef::new("store_type", DataType::Varchar(50))),

        // dim_salespersons
        CreateTable::new("dim_salespersons")
            .column(ColumnDef::new("salesperson_sk", DataType::Int32).not_null().primary_key())
            .column(ColumnDef::new("salesperson_id", DataType::Varchar(20)).not_null())
            .column(ColumnDef::new("first_name", DataType::Varchar(100)))
            .column(ColumnDef::new("last_name", DataType::Varchar(100)))
            .column(ColumnDef::new("email", DataType::Varchar(255)))
            .column(ColumnDef::new("hire_date", DataType::Date))
            .column(ColumnDef::new("territory", DataType::Varchar(100))),

        // dim_campaigns
        CreateTable::new("dim_campaigns")
            .column(ColumnDef::new("campaign_sk", DataType::Int32).not_null().primary_key())
            .column(ColumnDef::new("campaign_id", DataType::Varchar(20)).not_null())
            .column(ColumnDef::new("campaign_name", DataType::Varchar(255)))
            .column(ColumnDef::new("campaign_type", DataType::Varchar(50)))
            .column(ColumnDef::new("start_date", DataType::Date))
            .column(ColumnDef::new("end_date", DataType::Date)),

        // fact_sales_normalized
        CreateTable::new("fact_sales_normalized")
            .column(ColumnDef::new("sales_sk", DataType::Int64).not_null().primary_key())
            .column(ColumnDef::new("sales_id", DataType::Varchar(20)).not_null())
            .column(ColumnDef::new("customer_sk", DataType::Int32).not_null())
            .column(ColumnDef::new("product_sk", DataType::Int32).not_null())
            .column(ColumnDef::new("store_sk", DataType::Int32).not_null())
            .column(ColumnDef::new("salesperson_sk", DataType::Int32).not_null())
            .column(ColumnDef::new("campaign_sk", DataType::Int32))
            .column(ColumnDef::new("sales_date", DataType::Timestamp).not_null())
            .column(ColumnDef::new("total_amount", DataType::Decimal(12, 2)).not_null())
            .constraint(TableConstraint::foreign_key(["customer_sk"], "dim_customers", ["customer_sk"]))
            .constraint(TableConstraint::foreign_key(["product_sk"], "dim_products", ["product_sk"]))
            .constraint(TableConstraint::foreign_key(["store_sk"], "dim_stores", ["store_sk"]))
            .constraint(TableConstraint::foreign_key(["salesperson_sk"], "dim_salespersons", ["salesperson_sk"]))
            .constraint(TableConstraint::foreign_key(["campaign_sk"], "dim_campaigns", ["campaign_sk"])),
    ]
}
```

**Step 2: Add binary to Cargo.toml**

Add to `Cargo.toml`:
```toml
[[bin]]
name = "schema_gen"
path = "src/bin/schema_gen.rs"
```

**Step 3: Test the binary**

Run: `cargo run --bin schema_gen -- postgres`
Expected: Prints PostgreSQL DDL to stdout

**Step 4: Commit**

```bash
git add src/bin/schema_gen.rs Cargo.toml
git commit -m "feat: add schema_gen binary for test database DDL"
```

---

## Task 6: Create Docker Compose configuration

**Files:**
- Create: `database_test/docker-compose.yml`

**Step 1: Create docker-compose.yml**

Create `database_test/docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:16
    container_name: mantis_postgres
    environment:
      POSTGRES_USER: mantis
      POSTGRES_PASSWORD: mantis
      POSTGRES_DB: mantis_test
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mantis -d mantis_test"]
      interval: 5s
      timeout: 5s
      retries: 5

  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: mantis_mssql
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "MantisTest123!"
    ports:
      - "1433:1433"
    volumes:
      - mssql_data:/var/opt/mssql
    healthcheck:
      test: /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "MantisTest123!" -C -Q "SELECT 1" || exit 1
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
  mssql_data:
```

**Step 2: Test Docker Compose**

Run: `cd database_test && docker compose config`
Expected: Valid YAML output, no errors

**Step 3: Commit**

```bash
git add database_test/docker-compose.yml
git commit -m "infra: add Docker Compose for test databases"
```

---

## Task 7: Create Taskfile

**Files:**
- Create: `database_test/Taskfile.yml`

**Step 1: Create Taskfile.yml**

Create `database_test/Taskfile.yml`:

```yaml
version: '3'

vars:
  POSTGRES_CONN: "postgresql://mantis:mantis@localhost:5432/mantis_test"
  MSSQL_HOST: "localhost,1433"
  MSSQL_USER: "sa"
  MSSQL_PASS: "MantisTest123!"

tasks:
  default:
    desc: Show available tasks
    cmds:
      - task --list

  db-up:
    desc: Start PostgreSQL and MSSQL containers
    dir: "{{.ROOT_DIR}}"
    cmds:
      - docker compose up -d
      - echo "Waiting for databases to be ready..."
      - sleep 10

  db-down:
    desc: Stop containers (keeps data)
    dir: "{{.ROOT_DIR}}"
    cmds:
      - docker compose down

  db-destroy:
    desc: Stop containers and delete all data
    dir: "{{.ROOT_DIR}}"
    cmds:
      - docker compose down -v
      - rm -f test.duckdb

  db-schema:
    desc: Generate DDL from Rust
    dir: "{{.ROOT_DIR}}/.."
    cmds:
      - cargo run --bin schema_gen

  db-load:
    desc: Generate schema and load data into all databases
    cmds:
      - task: db-schema
      - task: db-load-postgres
      - task: db-load-mssql
      - task: db-load-duckdb

  db-load-postgres:
    desc: Load data into PostgreSQL
    dir: "{{.ROOT_DIR}}"
    cmds:
      - ./scripts/load_postgres.sh

  db-load-mssql:
    desc: Load data into MSSQL
    dir: "{{.ROOT_DIR}}"
    cmds:
      - ./scripts/load_mssql.sh

  db-load-duckdb:
    desc: Load data into DuckDB
    dir: "{{.ROOT_DIR}}"
    cmds:
      - ./scripts/load_duckdb.sh

  db-reset:
    desc: Wipe and reload all data
    cmds:
      - task: db-destroy
      - task: db-up
      - task: db-load
```

**Step 2: Verify Taskfile syntax**

Run: `cd database_test && task --list`
Expected: Lists all available tasks

**Step 3: Commit**

```bash
git add database_test/Taskfile.yml
git commit -m "infra: add Taskfile for database management"
```

---

## Task 8: Create load scripts

**Files:**
- Create: `database_test/scripts/load_postgres.sh`
- Create: `database_test/scripts/load_mssql.sh`
- Create: `database_test/scripts/load_duckdb.sh`

**Step 1: Create PostgreSQL load script**

Create `database_test/scripts/load_postgres.sh`:

```bash
#!/bin/bash
set -e

CONN="postgresql://mantis:mantis@localhost:5432/mantis_test"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Loading PostgreSQL schema..."
psql "$CONN" -f "$SCRIPT_DIR/schema_postgres.sql"

echo "Loading dimension tables..."
for table in dim_dates dim_customers dim_products dim_stores dim_salespersons dim_campaigns; do
    echo "  Loading $table..."
    psql "$CONN" -c "\\copy $table FROM '$SCRIPT_DIR/data/${table}.csv' WITH CSV HEADER"
done

echo "Loading fact table..."
psql "$CONN" -c "\\copy fact_sales_normalized FROM '$SCRIPT_DIR/data/fact_sales_normalized.csv' WITH CSV HEADER"

echo "PostgreSQL load complete!"
```

**Step 2: Create MSSQL load script**

Create `database_test/scripts/load_mssql.sh`:

```bash
#!/bin/bash
set -e

HOST="localhost,1433"
USER="sa"
PASS="MantisTest123!"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Creating MSSQL database..."
sqlcmd -S "$HOST" -U "$USER" -P "$PASS" -C -Q "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'mantis_test') CREATE DATABASE mantis_test" 2>/dev/null || true

echo "Loading MSSQL schema..."
sqlcmd -S "$HOST" -U "$USER" -P "$PASS" -C -d mantis_test -i "$SCRIPT_DIR/schema_mssql.sql"

echo "Loading dimension tables..."
for table in dim_dates dim_customers dim_products dim_stores dim_salespersons dim_campaigns; do
    echo "  Loading $table..."
    # Use bcp for bulk loading
    bcp "$table" in "$SCRIPT_DIR/data/${table}.csv" -S "$HOST" -U "$USER" -P "$PASS" -d mantis_test -c -t ',' -F 2 -C 65001
done

echo "Loading fact table..."
bcp "fact_sales_normalized" in "$SCRIPT_DIR/data/fact_sales_normalized.csv" -S "$HOST" -U "$USER" -P "$PASS" -d mantis_test -c -t ',' -F 2 -C 65001

echo "MSSQL load complete!"
```

**Step 3: Create DuckDB load script**

Create `database_test/scripts/load_duckdb.sh`:

```bash
#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_FILE="$SCRIPT_DIR/test.duckdb"

echo "Creating DuckDB database..."
rm -f "$DB_FILE"

echo "Loading DuckDB schema..."
duckdb "$DB_FILE" < "$SCRIPT_DIR/schema_duckdb.sql"

echo "Loading data..."
duckdb "$DB_FILE" <<EOF
INSERT INTO dim_dates SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/dim_dates.csv', header=true);
INSERT INTO dim_customers SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/dim_customers.csv', header=true);
INSERT INTO dim_products SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/dim_products.csv', header=true);
INSERT INTO dim_stores SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/dim_stores.csv', header=true);
INSERT INTO dim_salespersons SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/dim_salespersons.csv', header=true);
INSERT INTO dim_campaigns SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/dim_campaigns.csv', header=true);
INSERT INTO fact_sales_normalized SELECT * FROM read_csv_auto('$SCRIPT_DIR/data/fact_sales_normalized.csv', header=true);
EOF

echo "DuckDB load complete!"
```

**Step 4: Make scripts executable**

Run: `chmod +x database_test/scripts/*.sh`

**Step 5: Commit**

```bash
git add database_test/scripts/
git commit -m "feat: add database load scripts"
```

---

## Task 9: End-to-end test

**Step 1: Generate schema files**

Run: `cd database_test && task db-schema`
Expected: Creates schema_postgres.sql, schema_mssql.sql, schema_duckdb.sql

**Step 2: Start containers**

Run: `cd database_test && task db-up`
Expected: PostgreSQL and MSSQL containers start

**Step 3: Load data (DuckDB only for quick test)**

Run: `cd database_test && task db-load-duckdb`
Expected: DuckDB database created and populated

**Step 4: Verify DuckDB data**

Run: `duckdb database_test/test.duckdb -c "SELECT COUNT(*) FROM fact_sales_normalized;"`
Expected: Returns row count

**Step 5: Stop containers**

Run: `cd database_test && task db-down`

**Step 6: Final commit**

```bash
git add -A
git commit -m "feat: complete database test environment setup"
```
