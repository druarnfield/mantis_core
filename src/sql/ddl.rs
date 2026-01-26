//! DDL (Data Definition Language) support.
//!
//! This module provides types and builders for generating DDL statements
//! (CREATE, ALTER, DROP) across different SQL dialects.
//!
//! # Examples
//!
//! ```ignore
//! use mantis::ddl::{CreateTable, ColumnDef, DataType};
//! use mantis::dialect::Dialect;
//!
//! let table = CreateTable::new("users")
//!     .column(ColumnDef::new("id", DataType::Int64).primary_key())
//!     .column(ColumnDef::new("name", DataType::Varchar(255)).not_null())
//!     .column(ColumnDef::new("email", DataType::Varchar(255)).unique());
//!
//! println!("{}", table.to_sql(Dialect::Postgres));
//! ```

use super::dialect::{Dialect, SqlDialect};
use super::expr::Expr;
use super::query::Query;
use super::token::{Token, TokenStream};

// Re-export DataType from sql::types for DDL generation
pub use super::types::DataType;

/// DDL statement types.
#[derive(Debug, Clone)]
pub enum DdlStatement {
    CreateTable(CreateTable),
    AlterTable(AlterTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    Truncate(Truncate),
    CreateView(CreateView),
    DropView(DropView),
}

impl DdlStatement {
    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        match self {
            DdlStatement::CreateTable(ct) => ct.to_tokens(dialect),
            DdlStatement::AlterTable(at) => at.to_tokens(dialect),
            DdlStatement::DropTable(dt) => dt.to_tokens(dialect),
            DdlStatement::CreateIndex(ci) => ci.to_tokens(dialect),
            DdlStatement::DropIndex(di) => di.to_tokens(dialect),
            DdlStatement::Truncate(t) => t.to_tokens(dialect),
            DdlStatement::CreateView(cv) => cv.to_tokens(dialect),
            DdlStatement::DropView(dv) => dv.to_tokens(dialect),
        }
    }
}

// ============================================================================
// CREATE TABLE
// ============================================================================

/// CREATE TABLE statement.
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub as_query: Option<Box<Query>>,
}

impl CreateTable {
    /// Create a new CREATE TABLE statement.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            if_not_exists: false,
            schema: None,
            name: name.into(),
            columns: Vec::new(),
            constraints: Vec::new(),
            as_query: None,
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add IF NOT EXISTS clause.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// Add a column definition.
    pub fn column(mut self, col: ColumnDef) -> Self {
        self.columns.push(col);
        self
    }

    /// Add multiple column definitions.
    pub fn columns(mut self, cols: impl IntoIterator<Item = ColumnDef>) -> Self {
        self.columns.extend(cols);
        self
    }

    /// Add a table constraint.
    pub fn constraint(mut self, constraint: TableConstraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Create table from a SELECT query (CREATE TABLE AS SELECT).
    pub fn as_select(mut self, query: Query) -> Self {
        self.as_query = Some(Box::new(query));
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // CREATE TABLE
        ts.push(Token::Create).space().push(Token::Table);

        // IF NOT EXISTS (dialect-specific)
        if self.if_not_exists && dialect.supports_if_not_exists() {
            ts.space()
                .push(Token::If)
                .space()
                .push(Token::Not)
                .space()
                .push(Token::Exists);
        }

        // Table name
        ts.space();
        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.name.clone(),
            });
        } else {
            ts.push(Token::Ident(self.name.clone()));
        }

        // AS SELECT or column definitions
        if let Some(ref query) = self.as_query {
            ts.space()
                .push(Token::As)
                .space()
                .append(&query.to_tokens_for_dialect(dialect));
        } else {
            // Column definitions and constraints
            ts.space().lparen();

            let mut first = true;
            for col in &self.columns {
                if !first {
                    ts.comma().space();
                }
                first = false;
                ts.append(&col.to_tokens(dialect));
            }

            for constraint in &self.constraints {
                if !first {
                    ts.comma().space();
                }
                first = false;
                ts.append(&constraint.to_tokens(dialect));
            }

            ts.rparen();
        }

        ts
    }
}

// ============================================================================
// Column Definition
// ============================================================================

/// Column definition for CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expr>,
    pub constraints: Vec<ColumnConstraint>,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: None,
            default: None,
            constraints: Vec::new(),
        }
    }

    /// Mark column as NOT NULL.
    pub fn not_null(mut self) -> Self {
        self.nullable = Some(false);
        self
    }

    /// Mark column as NULL (explicitly nullable).
    pub fn null(mut self) -> Self {
        self.nullable = Some(true);
        self
    }

    /// Set default value.
    pub fn default(mut self, expr: Expr) -> Self {
        self.default = Some(expr);
        self
    }

    /// Add PRIMARY KEY constraint.
    pub fn primary_key(mut self) -> Self {
        self.constraints.push(ColumnConstraint::PrimaryKey);
        self
    }

    /// Add UNIQUE constraint.
    pub fn unique(mut self) -> Self {
        self.constraints.push(ColumnConstraint::Unique);
        self
    }

    /// Add CHECK constraint.
    pub fn check(mut self, expr: Expr) -> Self {
        self.constraints.push(ColumnConstraint::Check(expr));
        self
    }

    /// Add REFERENCES constraint.
    pub fn references(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.constraints.push(ColumnConstraint::References {
            table: table.into(),
            column: column.into(),
        });
        self
    }

    /// Add IDENTITY (auto-increment) constraint.
    pub fn identity(mut self) -> Self {
        self.constraints.push(ColumnConstraint::Identity {
            start: 1,
            increment: 1,
        });
        self
    }

    /// Add IDENTITY with custom start and increment.
    pub fn identity_with(mut self, start: i64, increment: i64) -> Self {
        self.constraints
            .push(ColumnConstraint::Identity { start, increment });
        self
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // Column name
        ts.push(Token::Ident(self.name.clone()));

        // Data type
        ts.space()
            .push(Token::Raw(dialect.emit_data_type(&self.data_type)));

        // Identity/auto-increment (needs to come before NOT NULL for some dialects)
        for constraint in &self.constraints {
            if let ColumnConstraint::Identity { start, increment } = constraint {
                ts.space()
                    .append(&dialect.emit_identity(*start, *increment));
            }
        }

        // NULL/NOT NULL
        if let Some(nullable) = self.nullable {
            if nullable {
                ts.space().push(Token::Null);
            } else {
                ts.space().push(Token::Not).space().push(Token::Null);
            }
        }

        // DEFAULT
        if let Some(ref expr) = self.default {
            ts.space()
                .push(Token::Default)
                .space()
                .append(&expr.to_tokens());
        }

        // Other constraints
        for constraint in &self.constraints {
            match constraint {
                ColumnConstraint::PrimaryKey => {
                    ts.space().push(Token::Primary).space().push(Token::Key);
                }
                ColumnConstraint::Unique => {
                    ts.space().push(Token::Unique);
                }
                ColumnConstraint::Check(expr) => {
                    ts.space()
                        .push(Token::Check)
                        .space()
                        .lparen()
                        .append(&expr.to_tokens())
                        .rparen();
                }
                ColumnConstraint::References { table, column } => {
                    ts.space()
                        .push(Token::References)
                        .space()
                        .push(Token::Ident(table.clone()))
                        .lparen()
                        .push(Token::Ident(column.clone()))
                        .rparen();
                }
                ColumnConstraint::Identity { .. } => {
                    // Already handled above
                }
            }
        }

        ts
    }
}

/// Column-level constraints.
#[derive(Debug, Clone)]
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    Check(Expr),
    References { table: String, column: String },
    Identity { start: i64, increment: i64 },
}

// ============================================================================
// Table Constraints
// ============================================================================

/// Table-level constraints.
#[derive(Debug, Clone)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check {
        name: Option<String>,
        expr: Expr,
    },
}

impl TableConstraint {
    /// Create a PRIMARY KEY constraint.
    pub fn primary_key(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        TableConstraint::PrimaryKey {
            name: None,
            columns: columns.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Create a named PRIMARY KEY constraint.
    pub fn primary_key_named(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        TableConstraint::PrimaryKey {
            name: Some(name.into()),
            columns: columns.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Create a UNIQUE constraint.
    pub fn unique(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        TableConstraint::Unique {
            name: None,
            columns: columns.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Create a FOREIGN KEY constraint.
    pub fn foreign_key(
        columns: impl IntoIterator<Item = impl Into<String>>,
        references_table: impl Into<String>,
        references_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        TableConstraint::ForeignKey {
            name: None,
            columns: columns.into_iter().map(|c| c.into()).collect(),
            references_table: references_table.into(),
            references_columns: references_columns.into_iter().map(|c| c.into()).collect(),
            on_delete: None,
            on_update: None,
        }
    }

    /// Create a CHECK constraint.
    pub fn check(expr: Expr) -> Self {
        TableConstraint::Check { name: None, expr }
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        match self {
            TableConstraint::PrimaryKey { name, columns } => {
                if let Some(n) = name {
                    ts.push(Token::Constraint)
                        .space()
                        .push(Token::Ident(n.clone()))
                        .space();
                }
                ts.push(Token::Primary).space().push(Token::Key).space();
                emit_column_list(&mut ts, columns);
            }
            TableConstraint::Unique { name, columns } => {
                if let Some(n) = name {
                    ts.push(Token::Constraint)
                        .space()
                        .push(Token::Ident(n.clone()))
                        .space();
                }
                ts.push(Token::Unique).space();
                emit_column_list(&mut ts, columns);
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                references_table,
                references_columns,
                on_delete,
                on_update,
            } => {
                if let Some(n) = name {
                    ts.push(Token::Constraint)
                        .space()
                        .push(Token::Ident(n.clone()))
                        .space();
                }
                ts.push(Token::Foreign).space().push(Token::Key).space();
                emit_column_list(&mut ts, columns);
                ts.space()
                    .push(Token::References)
                    .space()
                    .push(Token::Ident(references_table.clone()))
                    .space();
                emit_column_list(&mut ts, references_columns);

                if let Some(action) = on_delete {
                    ts.space()
                        .push(Token::On)
                        .space()
                        .push(Token::Raw("DELETE".into()))
                        .space()
                        .append(&action.to_tokens(dialect));
                }
                if let Some(action) = on_update {
                    ts.space()
                        .push(Token::On)
                        .space()
                        .push(Token::Raw("UPDATE".into()))
                        .space()
                        .append(&action.to_tokens(dialect));
                }
            }
            TableConstraint::Check { name, expr } => {
                if let Some(n) = name {
                    ts.push(Token::Constraint)
                        .space()
                        .push(Token::Ident(n.clone()))
                        .space();
                }
                ts.push(Token::Check)
                    .space()
                    .lparen()
                    .append(&expr.to_tokens())
                    .rparen();
            }
        }

        ts
    }
}

/// Referential action for foreign key constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl ReferentialAction {
    /// Convert to token stream.
    pub fn to_tokens(&self, _dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();
        match self {
            ReferentialAction::NoAction => ts.push(Token::NoAction),
            ReferentialAction::Restrict => ts.push(Token::Restrict),
            ReferentialAction::Cascade => ts.push(Token::Cascade),
            ReferentialAction::SetNull => ts.push(Token::SetNull),
            ReferentialAction::SetDefault => ts.push(Token::SetDefault),
        };
        ts
    }
}

// ============================================================================
// ALTER TABLE
// ============================================================================

/// ALTER TABLE statement.
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct AlterTable {
    pub schema: Option<String>,
    pub name: String,
    pub actions: Vec<AlterAction>,
}

impl AlterTable {
    /// Create a new ALTER TABLE statement.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
            actions: Vec::new(),
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add a column.
    pub fn add_column(mut self, column: ColumnDef) -> Self {
        self.actions.push(AlterAction::AddColumn(column));
        self
    }

    /// Drop a column.
    pub fn drop_column(mut self, name: impl Into<String>) -> Self {
        self.actions
            .push(AlterAction::DropColumn { name: name.into() });
        self
    }

    /// Add a constraint.
    pub fn add_constraint(mut self, constraint: TableConstraint) -> Self {
        self.actions.push(AlterAction::AddConstraint(constraint));
        self
    }

    /// Drop a constraint.
    pub fn drop_constraint(mut self, name: impl Into<String>) -> Self {
        self.actions
            .push(AlterAction::DropConstraint { name: name.into() });
        self
    }

    /// Rename a column.
    pub fn rename_column(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.actions.push(AlterAction::RenameColumn {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        ts.push(Token::Alter).space().push(Token::Table).space();

        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.name.clone(),
            });
        } else {
            ts.push(Token::Ident(self.name.clone()));
        }

        // Actions
        let mut first = true;
        for action in &self.actions {
            if !first {
                ts.comma();
            }
            first = false;
            ts.space().append(&action.to_tokens(dialect));
        }

        ts
    }
}

/// ALTER TABLE actions.
#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn(ColumnDef),
    DropColumn { name: String },
    AddConstraint(TableConstraint),
    DropConstraint { name: String },
    RenameColumn { from: String, to: String },
}

impl AlterAction {
    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        match self {
            AlterAction::AddColumn(col) => {
                ts.push(Token::Add)
                    .space()
                    .push(Token::Column)
                    .space()
                    .append(&col.to_tokens(dialect));
            }
            AlterAction::DropColumn { name } => {
                ts.push(Token::Drop)
                    .space()
                    .push(Token::Column)
                    .space()
                    .push(Token::Ident(name.clone()));
            }
            AlterAction::AddConstraint(constraint) => {
                ts.push(Token::Add)
                    .space()
                    .push(Token::Constraint)
                    .space()
                    .append(&constraint.to_tokens(dialect));
            }
            AlterAction::DropConstraint { name } => {
                ts.push(Token::Drop)
                    .space()
                    .push(Token::Constraint)
                    .space()
                    .push(Token::Ident(name.clone()));
            }
            AlterAction::RenameColumn { from, to } => {
                // Syntax varies by dialect but we'll use standard form
                ts.push(Token::Raw("RENAME COLUMN".into()))
                    .space()
                    .push(Token::Ident(from.clone()))
                    .space()
                    .push(Token::Raw("TO".into()))
                    .space()
                    .push(Token::Ident(to.clone()));
            }
        }

        ts
    }
}

// ============================================================================
// DROP TABLE
// ============================================================================

/// DROP TABLE statement.
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct DropTable {
    pub if_exists: bool,
    pub schema: Option<String>,
    pub name: String,
    pub cascade: bool,
}

impl DropTable {
    /// Create a new DROP TABLE statement.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            if_exists: false,
            schema: None,
            name: name.into(),
            cascade: false,
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add IF EXISTS clause.
    pub fn if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    /// Add CASCADE clause.
    pub fn cascade(mut self) -> Self {
        self.cascade = true;
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        ts.push(Token::Drop).space().push(Token::Table);

        if self.if_exists && dialect.supports_if_exists() {
            ts.space().push(Token::If).space().push(Token::Exists);
        }

        ts.space();
        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.name.clone(),
            });
        } else {
            ts.push(Token::Ident(self.name.clone()));
        }

        if self.cascade && dialect.supports_drop_cascade() {
            ts.space().push(Token::Cascade);
        }

        ts
    }
}

// ============================================================================
// CREATE INDEX
// ============================================================================

/// CREATE INDEX statement.
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct CreateIndex {
    pub unique: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<IndexColumn>,
    pub include: Vec<String>,
    pub where_clause: Option<Expr>,
}

impl CreateIndex {
    /// Create a new CREATE INDEX statement.
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            unique: false,
            if_not_exists: false,
            name: name.into(),
            schema: None,
            table: table.into(),
            columns: Vec::new(),
            include: Vec::new(),
            where_clause: None,
        }
    }

    /// Make this a unique index.
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Add IF NOT EXISTS clause.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add a column to the index.
    pub fn column(mut self, col: impl Into<IndexColumn>) -> Self {
        self.columns.push(col.into());
        self
    }

    /// Add multiple columns to the index.
    pub fn columns(mut self, cols: impl IntoIterator<Item = impl Into<IndexColumn>>) -> Self {
        self.columns.extend(cols.into_iter().map(|c| c.into()));
        self
    }

    /// Add INCLUDE columns (PostgreSQL/T-SQL).
    pub fn include(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.include.extend(cols.into_iter().map(|c| c.into()));
        self
    }

    /// Add WHERE clause for partial index.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(expr);
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        ts.push(Token::Create);
        if self.unique {
            ts.space().push(Token::Unique);
        }
        ts.space().push(Token::Index);

        if self.if_not_exists && dialect.supports_if_not_exists() {
            ts.space()
                .push(Token::If)
                .space()
                .push(Token::Not)
                .space()
                .push(Token::Exists);
        }

        ts.space().push(Token::Ident(self.name.clone()));
        ts.space().push(Token::On).space();

        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.table.clone(),
            });
        } else {
            ts.push(Token::Ident(self.table.clone()));
        }

        // Columns
        ts.space().lparen();
        let mut first = true;
        for col in &self.columns {
            if !first {
                ts.comma().space();
            }
            first = false;
            ts.append(&col.to_tokens());
        }
        ts.rparen();

        // INCLUDE columns
        if !self.include.is_empty() && dialect.supports_include_columns() {
            ts.space().push(Token::Raw("INCLUDE".into())).space();
            emit_column_list(&mut ts, &self.include);
        }

        // WHERE clause for partial index
        if let Some(ref expr) = self.where_clause {
            if dialect.supports_partial_indexes() {
                ts.space()
                    .push(Token::Where)
                    .space()
                    .append(&expr.to_tokens());
            }
        }

        ts
    }
}

/// Index column specification.
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub descending: bool,
}

impl IndexColumn {
    /// Create an ascending index column.
    pub fn asc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            descending: false,
        }
    }

    /// Create a descending index column.
    pub fn desc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            descending: true,
        }
    }

    /// Convert to token stream.
    pub fn to_tokens(&self) -> TokenStream {
        let mut ts = TokenStream::new();
        ts.push(Token::Ident(self.name.clone()));
        if self.descending {
            ts.space().push(Token::Desc);
        }
        ts
    }
}

impl<S: Into<String>> From<S> for IndexColumn {
    fn from(s: S) -> Self {
        IndexColumn::asc(s)
    }
}

// ============================================================================
// DROP INDEX
// ============================================================================

/// DROP INDEX statement.
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct DropIndex {
    pub if_exists: bool,
    pub name: String,
    pub schema: Option<String>,
    pub table: Option<String>, // Required for MySQL
}

impl DropIndex {
    /// Create a new DROP INDEX statement.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            if_exists: false,
            name: name.into(),
            schema: None,
            table: None,
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the table (required for MySQL).
    pub fn on_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Add IF EXISTS clause.
    pub fn if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        ts.push(Token::Drop).space().push(Token::Index);

        if self.if_exists && dialect.supports_if_exists() {
            ts.space().push(Token::If).space().push(Token::Exists);
        }

        ts.space();
        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.name.clone(),
            });
        } else {
            ts.push(Token::Ident(self.name.clone()));
        }

        // MySQL requires ON table_name
        if let Some(ref table) = self.table {
            ts.space()
                .push(Token::On)
                .space()
                .push(Token::Ident(table.clone()));
        }

        ts
    }
}

// ============================================================================
// TRUNCATE TABLE
// ============================================================================

/// TRUNCATE TABLE statement.
///
/// Removes all rows from a table quickly without logging individual row deletions.
/// Much faster than DELETE for large tables.
///
/// # Example
///
/// ```ignore
/// let truncate = Truncate::table("staging_orders")
///     .schema("etl")
///     .cascade();
///
/// // PostgreSQL/DuckDB: TRUNCATE TABLE "etl"."staging_orders" CASCADE
/// // T-SQL/MySQL: TRUNCATE TABLE [etl].[staging_orders] (no CASCADE)
/// ```
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct Truncate {
    pub schema: Option<String>,
    pub table: String,
    pub cascade: bool,
}

impl Truncate {
    /// Create a new TRUNCATE TABLE statement.
    pub fn table(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: name.into(),
            cascade: false,
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add CASCADE clause (PostgreSQL/DuckDB only).
    ///
    /// CASCADE truncates all tables that have foreign key references
    /// to the target table.
    pub fn cascade(mut self) -> Self {
        self.cascade = true;
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        ts.push(Token::Truncate).space().push(Token::Table).space();

        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.table.clone(),
            });
        } else {
            ts.push(Token::Ident(self.table.clone()));
        }

        if self.cascade && dialect.supports_truncate_cascade() {
            ts.space().push(Token::Cascade);
        }

        ts
    }
}

// ============================================================================
// CREATE VIEW
// ============================================================================

/// CREATE VIEW statement.
///
/// Creates a view (virtual table) based on a SELECT query.
/// Supports regular views and materialized views (PostgreSQL/DuckDB).
///
/// # Example
///
/// ```ignore
/// let query = Query::from("orders").select_all();
/// let view = CreateView::new("active_orders", query)
///     .schema("analytics")
///     .or_replace();
///
/// // PostgreSQL: CREATE OR REPLACE VIEW "analytics"."active_orders" AS SELECT ...
/// ```
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct CreateView {
    pub or_replace: bool,
    pub materialized: bool,
    pub if_not_exists: bool,
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<String>,
    pub as_query: Box<Query>,
}

impl CreateView {
    /// Create a new CREATE VIEW statement.
    pub fn new(name: impl Into<String>, query: Query) -> Self {
        Self {
            or_replace: false,
            materialized: false,
            if_not_exists: false,
            schema: None,
            name: name.into(),
            columns: Vec::new(),
            as_query: Box::new(query),
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add OR REPLACE clause.
    ///
    /// Replaces the view if it already exists.
    /// Not supported by T-SQL (use DROP + CREATE instead).
    pub fn or_replace(mut self) -> Self {
        self.or_replace = true;
        self
    }

    /// Create a MATERIALIZED VIEW.
    ///
    /// Materialized views store query results physically.
    /// Supported by PostgreSQL and DuckDB, not T-SQL or MySQL.
    pub fn materialized(mut self) -> Self {
        self.materialized = true;
        self
    }

    /// Add IF NOT EXISTS clause.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// Set explicit column names for the view.
    pub fn columns(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = cols.into_iter().map(|c| c.into()).collect();
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // CREATE
        ts.push(Token::Create);

        // OR REPLACE (if supported)
        if self.or_replace && dialect.supports_create_or_replace_view() {
            ts.space().push(Token::Or).space().push(Token::Replace);
        }

        // MATERIALIZED (if supported)
        if self.materialized && dialect.supports_materialized_view() {
            ts.space().push(Token::Materialized);
        }

        // VIEW
        ts.space().push(Token::View);

        // IF NOT EXISTS
        if self.if_not_exists && dialect.supports_if_not_exists() {
            ts.space()
                .push(Token::If)
                .space()
                .push(Token::Not)
                .space()
                .push(Token::Exists);
        }

        // View name
        ts.space();
        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.name.clone(),
            });
        } else {
            ts.push(Token::Ident(self.name.clone()));
        }

        // Optional column list
        if !self.columns.is_empty() {
            ts.space();
            emit_column_list(&mut ts, &self.columns);
        }

        // AS query
        ts.space()
            .push(Token::As)
            .space()
            .append(&self.as_query.to_tokens_for_dialect(dialect));

        ts
    }
}

// ============================================================================
// DROP VIEW
// ============================================================================

/// DROP VIEW statement.
///
/// # Example
///
/// ```ignore
/// let drop = DropView::new("old_view")
///     .if_exists()
///     .cascade();
///
/// // PostgreSQL: DROP VIEW IF EXISTS "old_view" CASCADE
/// ```
#[derive(Debug, Clone)]
#[must_use = "DDL statements have no effect until converted to SQL with to_sql()"]
pub struct DropView {
    pub if_exists: bool,
    pub materialized: bool,
    pub schema: Option<String>,
    pub name: String,
    pub cascade: bool,
}

impl DropView {
    /// Create a new DROP VIEW statement.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            if_exists: false,
            materialized: false,
            schema: None,
            name: name.into(),
            cascade: false,
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add IF EXISTS clause.
    pub fn if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    /// Drop a MATERIALIZED VIEW.
    pub fn materialized(mut self) -> Self {
        self.materialized = true;
        self
    }

    /// Add CASCADE clause.
    pub fn cascade(mut self) -> Self {
        self.cascade = true;
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        ts.push(Token::Drop);

        // MATERIALIZED (if applicable and supported)
        if self.materialized && dialect.supports_materialized_view() {
            ts.space().push(Token::Materialized);
        }

        ts.space().push(Token::View);

        // IF EXISTS
        if self.if_exists && dialect.supports_if_exists() {
            ts.space().push(Token::If).space().push(Token::Exists);
        }

        // View name
        ts.space();
        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.name.clone(),
            });
        } else {
            ts.push(Token::Ident(self.name.clone()));
        }

        // CASCADE
        if self.cascade && dialect.supports_drop_cascade() {
            ts.space().push(Token::Cascade);
        }

        ts
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn emit_column_list(ts: &mut TokenStream, columns: &[String]) {
    ts.lparen();
    let mut first = true;
    for col in columns {
        if !first {
            ts.comma().space();
        }
        first = false;
        ts.push(Token::Ident(col.clone()));
    }
    ts.rparen();
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::expr::{col, lit_int, ExprExt};

    #[test]
    fn test_create_table_basic() {
        let table = CreateTable::new("users")
            .column(
                ColumnDef::new("id", DataType::Int64)
                    .not_null()
                    .primary_key(),
            )
            .column(ColumnDef::new("name", DataType::Varchar(255)).not_null())
            .column(ColumnDef::new("email", DataType::Varchar(255)).unique());

        let sql = table.to_sql(Dialect::Postgres);
        assert!(sql.contains("CREATE TABLE"));
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("BIGINT"));
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let table = CreateTable::new("users")
            .if_not_exists()
            .column(ColumnDef::new("id", DataType::Int64));

        let sql = table.to_sql(Dialect::Postgres);
        assert!(sql.contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_create_table_with_schema() {
        let table = CreateTable::new("users")
            .schema("dbo")
            .column(ColumnDef::new("id", DataType::Int64));

        let tsql = table.to_sql(Dialect::TSql);
        assert!(tsql.contains("[dbo].[users]"));
    }

    #[test]
    fn test_create_table_with_constraints() {
        let table = CreateTable::new("orders")
            .column(ColumnDef::new("id", DataType::Int64))
            .column(ColumnDef::new("user_id", DataType::Int64))
            .constraint(TableConstraint::primary_key(["id"]))
            .constraint(TableConstraint::foreign_key(["user_id"], "users", ["id"]));

        let sql = table.to_sql(Dialect::Postgres);
        assert!(sql.contains("PRIMARY KEY"));
        assert!(sql.contains("FOREIGN KEY"));
        assert!(sql.contains("REFERENCES"));
    }

    #[test]
    fn test_drop_table() {
        let drop = DropTable::new("users").if_exists().cascade();
        let sql = drop.to_sql(Dialect::Postgres);
        assert!(sql.contains("DROP TABLE"));
        assert!(sql.contains("IF EXISTS"));
        assert!(sql.contains("CASCADE"));
    }

    #[test]
    fn test_alter_table_add_column() {
        let alter = AlterTable::new("users").add_column(ColumnDef::new("age", DataType::Int32));

        let sql = alter.to_sql(Dialect::Postgres);
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("ADD COLUMN"));
        assert!(sql.contains("\"age\""));
    }

    #[test]
    fn test_alter_table_drop_column() {
        let alter = AlterTable::new("users").drop_column("age");

        let sql = alter.to_sql(Dialect::Postgres);
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("DROP COLUMN"));
    }

    #[test]
    fn test_create_index() {
        let index = CreateIndex::new("idx_users_email", "users")
            .unique()
            .column("email");

        let sql = index.to_sql(Dialect::Postgres);
        assert!(sql.contains("CREATE UNIQUE INDEX"));
        assert!(sql.contains("\"idx_users_email\""));
        assert!(sql.contains("ON \"users\""));
    }

    #[test]
    fn test_create_index_with_include() {
        let index = CreateIndex::new("idx_orders_user", "orders")
            .column("user_id")
            .include(["order_date", "total"]);

        let sql = index.to_sql(Dialect::Postgres);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("INCLUDE"));
    }

    #[test]
    fn test_create_partial_index() {
        let index = CreateIndex::new("idx_active_users", "users")
            .column("email")
            .filter(col("active").eq(lit_int(1)));

        let sql = index.to_sql(Dialect::Postgres);
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_drop_index() {
        let drop = DropIndex::new("idx_users_email").if_exists();
        let sql = drop.to_sql(Dialect::Postgres);
        assert!(sql.contains("DROP INDEX"));
        assert!(sql.contains("IF EXISTS"));
    }

    #[test]
    fn test_drop_index_mysql() {
        let drop = DropIndex::new("idx_users_email").on_table("users");
        let sql = drop.to_sql(Dialect::MySql);
        assert!(sql.contains("DROP INDEX"));
        assert!(sql.contains("ON"));
    }

    #[test]
    fn test_column_with_default() {
        let table = CreateTable::new("users").column(
            ColumnDef::new("status", DataType::Varchar(20)).default(crate::expr::lit_str("active")),
        );

        let sql = table.to_sql(Dialect::Postgres);
        assert!(sql.contains("DEFAULT"));
        assert!(sql.contains("'active'"));
    }

    #[test]
    fn test_identity_column_postgres() {
        let table = CreateTable::new("users").column(
            ColumnDef::new("id", DataType::Int64)
                .identity()
                .primary_key(),
        );

        let sql = table.to_sql(Dialect::Postgres);
        assert!(sql.contains("GENERATED ALWAYS AS IDENTITY"));
    }

    #[test]
    fn test_identity_column_tsql() {
        let table = CreateTable::new("users").column(
            ColumnDef::new("id", DataType::Int64)
                .identity()
                .primary_key(),
        );

        let sql = table.to_sql(Dialect::TSql);
        assert!(sql.contains("IDENTITY(1, 1)"));
    }

    #[test]
    fn test_identity_column_mysql() {
        let table = CreateTable::new("users").column(
            ColumnDef::new("id", DataType::Int64)
                .identity()
                .primary_key(),
        );

        let sql = table.to_sql(Dialect::MySql);
        assert!(sql.contains("AUTO_INCREMENT"));
    }

    // ========================================================================
    // Snapshot tests with roundtrip validation
    // ========================================================================

    mod snapshot_tests {
        use super::*;
        use crate::sql::expr::col;
        use crate::sql::query::{Query, TableRef};
        use crate::sql::test_utils::validate_sql;
        use insta::assert_snapshot;

        // --------------------------------------------------------------------
        // Truncate tests
        // --------------------------------------------------------------------

        #[test]
        fn truncate_basic_postgres() {
            let sql = Truncate::table("users").to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn truncate_basic_mysql() {
            let sql = Truncate::table("users").to_sql(Dialect::MySql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::MySql).unwrap();
        }

        #[test]
        fn truncate_basic_tsql() {
            let sql = Truncate::table("users").to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn truncate_cascade_postgres() {
            let sql = Truncate::table("users").cascade().to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn truncate_with_schema_postgres() {
            let sql = Truncate::table("users")
                .schema("analytics")
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn truncate_with_schema_tsql() {
            let sql = Truncate::table("users").schema("dbo").to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        // --------------------------------------------------------------------
        // CreateView tests
        // --------------------------------------------------------------------

        #[test]
        fn create_view_basic_postgres() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("active_users", query).to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn create_view_basic_mysql() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("active_users", query).to_sql(Dialect::MySql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::MySql).unwrap();
        }

        #[test]
        fn create_view_basic_tsql() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("active_users", query).to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn create_or_replace_view_postgres() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("active_users", query)
                .or_replace()
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn create_view_if_not_exists_duckdb() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("active_users", query)
                .if_not_exists()
                .to_sql(Dialect::DuckDb);
            assert_snapshot!(sql);
            // Note: sqlparser-rs doesn't support CREATE VIEW IF NOT EXISTS syntax
            // The SQL is valid DuckDB but can't be validated via roundtrip
        }

        #[test]
        fn create_materialized_view_postgres() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("user_summary", query)
                .materialized()
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            // Note: sqlparser-rs doesn't support CREATE MATERIALIZED VIEW syntax
            // The SQL is valid PostgreSQL but can't be validated via roundtrip
        }

        #[test]
        fn create_view_with_schema_postgres() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("active_users", query)
                .schema("analytics")
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn create_view_with_columns_postgres() {
            let query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("users"));
            let sql = CreateView::new("user_view", query)
                .columns(["user_id", "user_name"])
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        // --------------------------------------------------------------------
        // DropView tests
        // --------------------------------------------------------------------

        #[test]
        fn drop_view_basic_postgres() {
            let sql = DropView::new("active_users").to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn drop_view_basic_mysql() {
            let sql = DropView::new("active_users").to_sql(Dialect::MySql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::MySql).unwrap();
        }

        #[test]
        fn drop_view_if_exists_postgres() {
            let sql = DropView::new("active_users")
                .if_exists()
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn drop_view_cascade_postgres() {
            let sql = DropView::new("active_users")
                .if_exists()
                .cascade()
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn drop_materialized_view_postgres() {
            let sql = DropView::new("user_summary")
                .materialized()
                .if_exists()
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            // Note: sqlparser-rs doesn't support DROP MATERIALIZED VIEW syntax
            // The SQL is valid PostgreSQL but can't be validated via roundtrip
        }

        #[test]
        fn drop_view_with_schema_tsql() {
            let sql = DropView::new("active_users")
                .schema("dbo")
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }
    }
}
