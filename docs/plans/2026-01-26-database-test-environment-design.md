# Database Test Environment Design

## Overview

Create a Docker-based test environment for mantis_core with PostgreSQL, MSSQL, and DuckDB databases, populated with a retail star-schema dataset for integration testing and development.

## Goals

- **Integration testing**: Verify SQL translation works correctly across dialects
- **Development**: Persistent databases for ad-hoc queries during feature development
- **Reproducibility**: Easy setup/teardown with consistent data

## Project Structure

```
database_test/
├── Taskfile.yml              # Main task runner
├── docker-compose.yml        # MSSQL + PostgreSQL containers
├── test.duckdb               # DuckDB database (generated)
├── schema_postgres.sql       # Generated DDL
├── schema_mssql.sql          # Generated DDL
├── schema_duckdb.sql         # Generated DDL
├── data/                     # Source CSV files (existing)
│   ├── dim_campaigns.csv
│   ├── dim_customers.csv
│   ├── dim_dates.csv
│   ├── dim_products.csv
│   ├── dim_salespersons.csv
│   ├── dim_stores.csv
│   ├── fact_sales_normalized.csv
│   └── fact_sales_denormalized.csv
└── scripts/
    ├── load_postgres.sh      # Load CSVs into PostgreSQL
    ├── load_mssql.sh         # Load CSVs into MSSQL
    └── load_duckdb.sh        # Load CSVs into DuckDB

src/bin/
└── schema_gen.rs             # Rust binary to generate DDL
```

## Docker Configuration

### Images
- PostgreSQL: `postgres:16` (official)
- MSSQL: `mcr.microsoft.com/mssql/server:2022-latest` (official)
- DuckDB: No container (local CLI tool)

### docker-compose.yml

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

  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: mantis_mssql
    environment:
      ACCEPT_EULA: Y
      MSSQL_SA_PASSWORD: MantisTest123!
    ports:
      - "1433:1433"
    volumes:
      - mssql_data:/var/opt/mssql

volumes:
  postgres_data:
  mssql_data:
```

## Taskfile Commands

| Command | Description |
|---------|-------------|
| `task db-up` | Start PostgreSQL and MSSQL containers |
| `task db-down` | Stop containers (keeps data) |
| `task db-destroy` | Stop containers and delete all data |
| `task db-load` | Generate schema and load data into all databases |
| `task db-schema` | Generate DDL from Rust |
| `task db-load-postgres` | Load data into PostgreSQL only |
| `task db-load-mssql` | Load data into MSSQL only |
| `task db-load-duckdb` | Load data into DuckDB only |
| `task db-reset` | Wipe and reload all data |

## Schema Generation

Schema is defined once in Rust using `src/sql/ddl.rs` and generated for each dialect:

```rust
// src/bin/schema_gen.rs
use mantis::sql::ddl::{CreateTable, ColumnDef, DataType, TableConstraint};
use mantis::sql::dialect::Dialect;

fn define_schema() -> Vec<CreateTable> {
    vec![
        CreateTable::new("dim_customers")
            .column(ColumnDef::new("customer_sk", DataType::Int32).primary_key())
            .column(ColumnDef::new("customer_id", DataType::Varchar(20)).not_null())
            // ...
    ]
}
```

Running `cargo run --bin schema_gen` outputs:
- `database_test/schema_postgres.sql`
- `database_test/schema_mssql.sql`
- `database_test/schema_duckdb.sql`

## Data Model

### Dimension Tables
| Table | Primary Key | Description |
|-------|-------------|-------------|
| `dim_customers` | `customer_sk` | Customer demographics and segments |
| `dim_products` | `product_sk` | Product catalog with categories |
| `dim_stores` | `store_sk` | Store locations |
| `dim_salespersons` | `salesperson_sk` | Sales staff |
| `dim_campaigns` | `campaign_sk` | Marketing campaigns |
| `dim_dates` | `date_sk` | Date dimension |

### Fact Tables
| Table | Foreign Keys | Description |
|-------|--------------|-------------|
| `fact_sales_normalized` | All dimension SKs | Normalized sales transactions |
| `fact_sales_denormalized` | None | Denormalized sales (for comparison) |

## Data Loading

Data is loaded via shell scripts using native CLI tools:

- **PostgreSQL**: `psql` with `\copy` command
- **MSSQL**: `sqlcmd` for schema, `bcp` or bulk insert for data
- **DuckDB**: `duckdb` CLI with `read_csv_auto()`

Load order: dimension tables first, then fact tables (to satisfy FK constraints).

## Connection Details

| Database | Host | Port | User | Password | Database |
|----------|------|------|------|----------|----------|
| PostgreSQL | localhost | 5432 | mantis | mantis | mantis_test |
| MSSQL | localhost | 1433 | sa | MantisTest123! | mantis_test |
| DuckDB | - | - | - | - | test.duckdb |

## Additional Work

- Fix/remove broken tests in `src/sql/ddl.rs` before implementation
