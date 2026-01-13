//! DML (Data Manipulation Language) support.
//!
//! This module provides types and builders for generating DML statements
//! (INSERT, UPDATE, DELETE) across different SQL dialects.
//!
//! # Examples
//!
//! ```ignore
//! use mantis::dml::{Insert, Update, Delete};
//! use mantis::dialect::Dialect;
//! use mantis::expr::{col, lit_str, lit_int};
//!
//! // INSERT
//! let insert = Insert::into("users")
//!     .columns(["name", "email"])
//!     .values([lit_str("Alice"), lit_str("alice@example.com")]);
//!
//! // UPDATE
//! let update = Update::table("users")
//!     .set("status", lit_str("active"))
//!     .filter(col("id").eq(lit_int(1)));
//!
//! // DELETE
//! let delete = Delete::from("users")
//!     .filter(col("status").eq(lit_str("inactive")));
//! ```

use super::dialect::{Dialect, SqlDialect};
use super::expr::Expr;
use super::query::Query;
use super::token::{Token, TokenStream};

// ============================================================================
// INSERT
// ============================================================================

/// INSERT statement.
#[derive(Debug, Clone)]
#[must_use = "DML statements have no effect until converted to SQL with to_sql()"]
pub struct Insert {
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
    pub from_query: Option<Box<Query>>,
    pub on_conflict: Option<OnConflict>,
    pub returning: Vec<Expr>,
}

impl Insert {
    /// Create a new INSERT statement.
    pub fn into(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            columns: Vec::new(),
            values: Vec::new(),
            from_query: None,
            on_conflict: None,
            returning: Vec::new(),
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the columns to insert.
    pub fn columns(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = cols.into_iter().map(|c| c.into()).collect();
        self
    }

    /// Add a row of values.
    pub fn values(mut self, vals: impl IntoIterator<Item = impl Into<Expr>>) -> Self {
        self.values
            .push(vals.into_iter().map(|v| v.into()).collect());
        self
    }

    /// Add multiple rows of values.
    pub fn values_many(mut self, rows: impl IntoIterator<Item = Vec<Expr>>) -> Self {
        self.values.extend(rows);
        self
    }

    /// Insert from a SELECT query.
    pub fn from_select(mut self, query: Query) -> Self {
        self.from_query = Some(Box::new(query));
        self
    }

    /// Add ON CONFLICT clause (PostgreSQL/DuckDB).
    pub fn on_conflict(mut self, conflict: OnConflict) -> Self {
        self.on_conflict = Some(conflict);
        self
    }

    /// Add RETURNING clause.
    pub fn returning(mut self, exprs: impl IntoIterator<Item = impl Into<Expr>>) -> Self {
        self.returning = exprs.into_iter().map(|e| e.into()).collect();
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // INSERT INTO
        ts.push(Token::Insert).space().push(Token::Into).space();

        // Table name
        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.table.clone(),
            });
        } else {
            ts.push(Token::Ident(self.table.clone()));
        }

        // Columns
        if !self.columns.is_empty() {
            ts.space().lparen();
            for (i, col) in self.columns.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.push(Token::Ident(col.clone()));
            }
            ts.rparen();
        }

        // T-SQL: OUTPUT goes before VALUES
        if !self.returning.is_empty() && matches!(dialect, Dialect::TSql) {
            ts.space()
                .push(Token::Output)
                .space()
                .push(Token::Inserted)
                .push(Token::Dot)
                .push(Token::Star);
        }

        // VALUES or SELECT
        if let Some(ref query) = self.from_query {
            ts.space().append(&query.to_tokens_for_dialect(dialect));
        } else if !self.values.is_empty() {
            ts.space().push(Token::Values);
            for (row_idx, row) in self.values.iter().enumerate() {
                if row_idx > 0 {
                    ts.comma();
                }
                ts.space().lparen();
                for (i, val) in row.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.append(&val.to_tokens());
                }
                ts.rparen();
            }
        }

        // ON CONFLICT
        if let Some(ref conflict) = self.on_conflict {
            ts.space().append(&conflict.to_tokens(dialect));
        }

        // RETURNING (PostgreSQL/DuckDB)
        if !self.returning.is_empty() && dialect.supports_returning() {
            ts.space().push(Token::Returning).space();
            for (i, expr) in self.returning.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.append(&expr.to_tokens());
            }
        }

        ts
    }
}

/// ON CONFLICT clause for INSERT.
#[derive(Debug, Clone)]
pub enum OnConflict {
    DoNothing,
    DoUpdate {
        conflict_columns: Vec<String>,
        set: Vec<(String, Expr)>,
    },
}

impl OnConflict {
    /// Create ON CONFLICT DO NOTHING.
    pub fn do_nothing() -> Self {
        OnConflict::DoNothing
    }

    /// Create ON CONFLICT DO UPDATE.
    pub fn do_update(
        conflict_columns: impl IntoIterator<Item = impl Into<String>>,
        set: impl IntoIterator<Item = (impl Into<String>, Expr)>,
    ) -> Self {
        OnConflict::DoUpdate {
            conflict_columns: conflict_columns.into_iter().map(|c| c.into()).collect(),
            set: set.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, _dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        match self {
            OnConflict::DoNothing => {
                ts.push(Token::On)
                    .space()
                    .push(Token::Conflict)
                    .space()
                    .push(Token::Do)
                    .space()
                    .push(Token::Nothing);
            }
            OnConflict::DoUpdate {
                conflict_columns,
                set,
            } => {
                ts.push(Token::On)
                    .space()
                    .push(Token::Conflict)
                    .space()
                    .lparen();
                for (i, col) in conflict_columns.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.push(Token::Ident(col.clone()));
                }
                ts.rparen()
                    .space()
                    .push(Token::Do)
                    .space()
                    .push(Token::Update)
                    .space()
                    .push(Token::Set)
                    .space();
                for (i, (col, expr)) in set.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.push(Token::Ident(col.clone()))
                        .space()
                        .push(Token::Eq)
                        .space()
                        .append(&expr.to_tokens());
                }
            }
        }

        ts
    }
}

// ============================================================================
// UPDATE
// ============================================================================

/// UPDATE statement.
#[derive(Debug, Clone)]
#[must_use = "DML statements have no effect until converted to SQL with to_sql()"]
pub struct Update {
    pub schema: Option<String>,
    pub table: String,
    pub set: Vec<(String, Expr)>,
    pub from: Option<String>,
    pub filter: Option<Expr>,
    pub returning: Vec<Expr>,
}

impl Update {
    /// Create a new UPDATE statement.
    pub fn table(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            set: Vec::new(),
            from: None,
            filter: None,
            returning: Vec::new(),
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set a column to a value.
    pub fn set(mut self, column: impl Into<String>, value: impl Into<Expr>) -> Self {
        self.set.push((column.into(), value.into()));
        self
    }

    /// Set multiple columns.
    pub fn set_many(
        mut self,
        assignments: impl IntoIterator<Item = (impl Into<String>, impl Into<Expr>)>,
    ) -> Self {
        self.set
            .extend(assignments.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Add FROM clause (PostgreSQL).
    pub fn from(mut self, table: impl Into<String>) -> Self {
        self.from = Some(table.into());
        self
    }

    /// Add WHERE clause.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter = Some(match self.filter {
            Some(existing) => Expr::BinaryOp {
                left: Box::new(existing),
                op: crate::expr::BinaryOperator::And,
                right: Box::new(expr),
            },
            None => expr,
        });
        self
    }

    /// Add RETURNING clause.
    pub fn returning(mut self, exprs: impl IntoIterator<Item = impl Into<Expr>>) -> Self {
        self.returning = exprs.into_iter().map(|e| e.into()).collect();
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // UPDATE table
        ts.push(Token::Update).space();

        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.table.clone(),
            });
        } else {
            ts.push(Token::Ident(self.table.clone()));
        }

        // SET clause
        ts.space().push(Token::Set).space();
        for (i, (col, expr)) in self.set.iter().enumerate() {
            if i > 0 {
                ts.comma().space();
            }
            ts.push(Token::Ident(col.clone()))
                .space()
                .push(Token::Eq)
                .space()
                .append(&expr.to_tokens());
        }

        // T-SQL: OUTPUT goes after SET, before FROM/WHERE
        if !self.returning.is_empty() && matches!(dialect, Dialect::TSql) {
            ts.space()
                .push(Token::Output)
                .space()
                .push(Token::Inserted)
                .push(Token::Dot)
                .push(Token::Star);
        }

        // FROM clause
        if let Some(ref from_table) = self.from {
            ts.space()
                .push(Token::From)
                .space()
                .push(Token::Ident(from_table.clone()));
        }

        // WHERE clause
        if let Some(ref filter) = self.filter {
            ts.space()
                .push(Token::Where)
                .space()
                .append(&filter.to_tokens());
        }

        // RETURNING (PostgreSQL/DuckDB)
        if !self.returning.is_empty() && dialect.supports_returning() {
            ts.space().push(Token::Returning).space();
            for (i, expr) in self.returning.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.append(&expr.to_tokens());
            }
        }

        ts
    }
}

// ============================================================================
// DELETE
// ============================================================================

/// DELETE statement.
#[derive(Debug, Clone)]
#[must_use = "DML statements have no effect until converted to SQL with to_sql()"]
pub struct Delete {
    pub schema: Option<String>,
    pub table: String,
    pub using: Vec<String>,
    pub filter: Option<Expr>,
    pub returning: Vec<Expr>,
}

impl Delete {
    /// Create a new DELETE statement.
    pub fn from(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            using: Vec::new(),
            filter: None,
            returning: Vec::new(),
        }
    }

    /// Set the schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add USING clause (PostgreSQL).
    pub fn using(mut self, tables: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.using.extend(tables.into_iter().map(|t| t.into()));
        self
    }

    /// Add WHERE clause.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter = Some(match self.filter {
            Some(existing) => Expr::BinaryOp {
                left: Box::new(existing),
                op: crate::expr::BinaryOperator::And,
                right: Box::new(expr),
            },
            None => expr,
        });
        self
    }

    /// Add RETURNING clause.
    pub fn returning(mut self, exprs: impl IntoIterator<Item = impl Into<Expr>>) -> Self {
        self.returning = exprs.into_iter().map(|e| e.into()).collect();
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // DELETE FROM table
        ts.push(Token::Delete).space().push(Token::From).space();

        if let Some(ref schema) = self.schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.table.clone(),
            });
        } else {
            ts.push(Token::Ident(self.table.clone()));
        }

        // T-SQL: OUTPUT goes after table name, before USING/WHERE
        if !self.returning.is_empty() && matches!(dialect, Dialect::TSql) {
            ts.space()
                .push(Token::Output)
                .space()
                .push(Token::Deleted)
                .push(Token::Dot)
                .push(Token::Star);
        }

        // USING clause
        if !self.using.is_empty() {
            ts.space().push(Token::Using).space();
            for (i, table) in self.using.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.push(Token::Ident(table.clone()));
            }
        }

        // WHERE clause
        if let Some(ref filter) = self.filter {
            ts.space()
                .push(Token::Where)
                .space()
                .append(&filter.to_tokens());
        }

        // RETURNING (PostgreSQL/DuckDB)
        if !self.returning.is_empty() && dialect.supports_returning() {
            ts.space().push(Token::Returning).space();
            for (i, expr) in self.returning.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.append(&expr.to_tokens());
            }
        }

        ts
    }
}

// ============================================================================
// MERGE
// ============================================================================

/// MERGE statement for upsert operations.
///
/// MERGE is a SQL statement that performs INSERT, UPDATE, or DELETE operations
/// in a single statement based on whether rows match between source and target.
///
/// # Dialect Support
///
/// **Important:** MERGE is only supported by T-SQL and PostgreSQL 15+.
/// Check `dialect.supports_merge()` before using this statement.
///
/// For other dialects, use:
/// - PostgreSQL/DuckDB: `Insert::on_conflict()` for upserts
/// - MySQL: `Insert` with `ON DUPLICATE KEY UPDATE` (not yet implemented)
///
/// # Example
///
/// ```ignore
/// let merge = Merge::into("dim_customers")
///     .using_table("staging_customers")
///     .source_alias("src")
///     .target_alias("tgt")
///     .on(col("tgt.customer_id").eq(col("src.customer_id")))
///     .when_matched_update([
///         ("name", col("src.name")),
///         ("email", col("src.email")),
///     ])
///     .when_not_matched_insert(
///         ["customer_id", "name", "email"],
///         [col("src.customer_id"), col("src.name"), col("src.email")],
///     );
///
/// // T-SQL output:
/// // MERGE INTO [dim_customers] AS tgt
/// // USING [staging_customers] AS src
/// // ON tgt.customer_id = src.customer_id
/// // WHEN MATCHED THEN UPDATE SET [name] = src.name, [email] = src.email
/// // WHEN NOT MATCHED THEN INSERT ([customer_id], [name], [email])
/// //   VALUES (src.customer_id, src.name, src.email);
/// ```
#[derive(Debug, Clone)]
#[must_use = "DML statements have no effect until converted to SQL with to_sql()"]
pub struct Merge {
    pub target_schema: Option<String>,
    pub target_table: String,
    pub target_alias: Option<String>,
    pub source: MergeSource,
    pub source_alias: String,
    pub on_condition: Expr,
    pub when_clauses: Vec<WhenClause>,
}

/// Source for a MERGE statement - either a table or a subquery.
#[derive(Debug, Clone)]
pub enum MergeSource {
    /// A table reference.
    Table {
        schema: Option<String>,
        name: String,
    },
    /// A subquery.
    Query(Box<Query>),
}

/// A WHEN clause in a MERGE statement.
#[derive(Debug, Clone)]
pub struct WhenClause {
    /// true = WHEN MATCHED, false = WHEN NOT MATCHED
    pub matched: bool,
    /// Optional additional condition (AND ...)
    pub condition: Option<Expr>,
    /// The action to take
    pub action: MergeAction,
}

/// Action to take in a WHEN clause.
#[derive(Debug, Clone)]
pub enum MergeAction {
    /// UPDATE SET column = value, ...
    Update { assignments: Vec<(String, Expr)> },
    /// DELETE
    Delete,
    /// INSERT (columns) VALUES (values)
    Insert {
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

impl Merge {
    /// Create a new MERGE statement targeting a table.
    pub fn into(table: impl Into<String>) -> Self {
        Self {
            target_schema: None,
            target_table: table.into(),
            target_alias: None,
            source: MergeSource::Table {
                schema: None,
                name: String::new(),
            },
            source_alias: "src".into(),
            on_condition: Expr::Literal(super::expr::Literal::Bool(true)), // Placeholder
            when_clauses: Vec::new(),
        }
    }

    /// Set the target schema.
    pub fn target_schema(mut self, schema: impl Into<String>) -> Self {
        self.target_schema = Some(schema.into());
        self
    }

    /// Set the target alias.
    pub fn target_alias(mut self, alias: impl Into<String>) -> Self {
        self.target_alias = Some(alias.into());
        self
    }

    /// Set the source to a table.
    pub fn using_table(mut self, table: impl Into<String>) -> Self {
        self.source = MergeSource::Table {
            schema: None,
            name: table.into(),
        };
        self
    }

    /// Set the source to a table with schema.
    pub fn using_table_with_schema(
        mut self,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        self.source = MergeSource::Table {
            schema: Some(schema.into()),
            name: table.into(),
        };
        self
    }

    /// Set the source to a subquery.
    pub fn using_query(mut self, query: Query) -> Self {
        self.source = MergeSource::Query(Box::new(query));
        self
    }

    /// Set the source alias.
    pub fn source_alias(mut self, alias: impl Into<String>) -> Self {
        self.source_alias = alias.into();
        self
    }

    /// Set the ON condition.
    pub fn on(mut self, condition: Expr) -> Self {
        self.on_condition = condition;
        self
    }

    /// Add a WHEN MATCHED THEN UPDATE clause.
    pub fn when_matched_update(
        mut self,
        assignments: impl IntoIterator<Item = (impl Into<String>, Expr)>,
    ) -> Self {
        self.when_clauses.push(WhenClause {
            matched: true,
            condition: None,
            action: MergeAction::Update {
                assignments: assignments
                    .into_iter()
                    .map(|(c, e)| (c.into(), e))
                    .collect(),
            },
        });
        self
    }

    /// Add a WHEN MATCHED AND condition THEN UPDATE clause.
    pub fn when_matched_and_update(
        mut self,
        condition: Expr,
        assignments: impl IntoIterator<Item = (impl Into<String>, Expr)>,
    ) -> Self {
        self.when_clauses.push(WhenClause {
            matched: true,
            condition: Some(condition),
            action: MergeAction::Update {
                assignments: assignments
                    .into_iter()
                    .map(|(c, e)| (c.into(), e))
                    .collect(),
            },
        });
        self
    }

    /// Add a WHEN MATCHED THEN DELETE clause.
    pub fn when_matched_delete(mut self) -> Self {
        self.when_clauses.push(WhenClause {
            matched: true,
            condition: None,
            action: MergeAction::Delete,
        });
        self
    }

    /// Add a WHEN MATCHED AND condition THEN DELETE clause.
    pub fn when_matched_and_delete(mut self, condition: Expr) -> Self {
        self.when_clauses.push(WhenClause {
            matched: true,
            condition: Some(condition),
            action: MergeAction::Delete,
        });
        self
    }

    /// Add a WHEN NOT MATCHED THEN INSERT clause.
    pub fn when_not_matched_insert(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
        values: impl IntoIterator<Item = Expr>,
    ) -> Self {
        self.when_clauses.push(WhenClause {
            matched: false,
            condition: None,
            action: MergeAction::Insert {
                columns: columns.into_iter().map(|c| c.into()).collect(),
                values: values.into_iter().collect(),
            },
        });
        self
    }

    /// Add a WHEN NOT MATCHED AND condition THEN INSERT clause.
    pub fn when_not_matched_and_insert(
        mut self,
        condition: Expr,
        columns: impl IntoIterator<Item = impl Into<String>>,
        values: impl IntoIterator<Item = Expr>,
    ) -> Self {
        self.when_clauses.push(WhenClause {
            matched: false,
            condition: Some(condition),
            action: MergeAction::Insert {
                columns: columns.into_iter().map(|c| c.into()).collect(),
                values: values.into_iter().collect(),
            },
        });
        self
    }

    /// Add a raw WHEN clause.
    pub fn when(mut self, clause: WhenClause) -> Self {
        self.when_clauses.push(clause);
        self
    }

    /// Convert to SQL for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens(dialect).serialize(dialect)
    }

    /// Convert to token stream.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // MERGE INTO target [AS alias]
        ts.push(Token::Merge).space().push(Token::Into).space();

        if let Some(ref schema) = self.target_schema {
            ts.push(Token::QualifiedIdent {
                schema: Some(schema.clone()),
                name: self.target_table.clone(),
            });
        } else {
            ts.push(Token::Ident(self.target_table.clone()));
        }

        if let Some(ref alias) = self.target_alias {
            ts.space()
                .push(Token::As)
                .space()
                .push(Token::Ident(alias.clone()));
        }

        // USING source AS alias
        ts.space().push(Token::Using).space();

        match &self.source {
            MergeSource::Table { schema, name } => {
                if let Some(s) = schema {
                    ts.push(Token::QualifiedIdent {
                        schema: Some(s.clone()),
                        name: name.clone(),
                    });
                } else {
                    ts.push(Token::Ident(name.clone()));
                }
            }
            MergeSource::Query(query) => {
                ts.lparen()
                    .append(&query.to_tokens_for_dialect(dialect))
                    .rparen();
            }
        }

        ts.space()
            .push(Token::As)
            .space()
            .push(Token::Ident(self.source_alias.clone()));

        // ON condition
        ts.space()
            .push(Token::On)
            .space()
            .append(&self.on_condition.to_tokens());

        // WHEN clauses
        for clause in &self.when_clauses {
            ts.space().append(&clause.to_tokens(dialect));
        }

        // T-SQL requires MERGE to end with a semicolon
        if matches!(dialect, Dialect::TSql) {
            ts.push(Token::Raw(";".into()));
        }

        ts
    }
}

impl WhenClause {
    /// Convert to token stream.
    pub fn to_tokens(&self, _dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // WHEN [NOT] MATCHED
        ts.push(Token::When).space();
        if !self.matched {
            ts.push(Token::Not).space();
        }
        ts.push(Token::Matched);

        // Optional AND condition
        if let Some(ref cond) = self.condition {
            ts.space()
                .push(Token::And)
                .space()
                .append(&cond.to_tokens());
        }

        // THEN action
        ts.space().push(Token::Then).space();

        match &self.action {
            MergeAction::Update { assignments } => {
                ts.push(Token::Update).space().push(Token::Set).space();
                for (i, (col, expr)) in assignments.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.push(Token::Ident(col.clone()))
                        .space()
                        .push(Token::Eq)
                        .space()
                        .append(&expr.to_tokens());
                }
            }
            MergeAction::Delete => {
                ts.push(Token::Delete);
            }
            MergeAction::Insert { columns, values } => {
                ts.push(Token::Insert).space().lparen();
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.push(Token::Ident(col.clone()));
                }
                ts.rparen().space().push(Token::Values).space().lparen();
                for (i, val) in values.iter().enumerate() {
                    if i > 0 {
                        ts.comma().space();
                    }
                    ts.append(&val.to_tokens());
                }
                ts.rparen();
            }
        }

        ts
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::expr::{col, lit_int, lit_str, star, ExprExt};

    #[test]
    fn test_insert_values() {
        let insert = Insert::into("users")
            .columns(["name", "email"])
            .values([lit_str("Alice"), lit_str("alice@example.com")]);

        let sql = insert.to_sql(Dialect::Postgres);
        assert!(sql.contains("INSERT INTO"));
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
    }

    #[test]
    fn test_insert_multiple_rows() {
        let insert = Insert::into("users")
            .columns(["name", "age"])
            .values([lit_str("Alice"), lit_int(30)])
            .values([lit_str("Bob"), lit_int(25)]);

        let sql = insert.to_sql(Dialect::Postgres);
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("'Bob'"));
    }

    #[test]
    fn test_insert_on_conflict() {
        let insert = Insert::into("users")
            .columns(["id", "name"])
            .values([lit_int(1), lit_str("Alice")])
            .on_conflict(OnConflict::do_nothing());

        let sql = insert.to_sql(Dialect::Postgres);
        assert!(sql.contains("ON CONFLICT DO NOTHING"));
    }

    #[test]
    fn test_insert_on_conflict_update() {
        let insert = Insert::into("users")
            .columns(["id", "name"])
            .values([lit_int(1), lit_str("Alice")])
            .on_conflict(OnConflict::do_update(
                ["id"],
                [("name", lit_str("Alice Updated"))],
            ));

        let sql = insert.to_sql(Dialect::Postgres);
        assert!(sql.contains("ON CONFLICT"));
        assert!(sql.contains("DO UPDATE SET"));
    }

    #[test]
    fn test_insert_returning() {
        let insert = Insert::into("users")
            .columns(["name"])
            .values([lit_str("Alice")])
            .returning([col("id")]);

        let sql = insert.to_sql(Dialect::Postgres);
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn test_update_simple() {
        let update = Update::table("users")
            .set("status", lit_str("active"))
            .filter(col("id").eq(lit_int(1)));

        let sql = update.to_sql(Dialect::Postgres);
        assert!(sql.contains("UPDATE"));
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("SET"));
        assert!(sql.contains("\"status\""));
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_update_multiple_sets() {
        let update = Update::table("users")
            .set("name", lit_str("Alice"))
            .set("age", lit_int(30))
            .filter(col("id").eq(lit_int(1)));

        let sql = update.to_sql(Dialect::Postgres);
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("\"age\""));
    }

    #[test]
    fn test_update_returning() {
        let update = Update::table("users")
            .set("status", lit_str("active"))
            .filter(col("id").eq(lit_int(1)))
            .returning([star()]);

        let sql = update.to_sql(Dialect::Postgres);
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn test_delete_simple() {
        let delete = Delete::from("users").filter(col("status").eq(lit_str("inactive")));

        let sql = delete.to_sql(Dialect::Postgres);
        assert!(sql.contains("DELETE FROM"));
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_delete_all() {
        let delete = Delete::from("temp_data");

        let sql = delete.to_sql(Dialect::Postgres);
        assert!(sql.contains("DELETE FROM"));
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn test_delete_using() {
        let delete = Delete::from("orders")
            .using(["users"])
            .filter(col("orders.user_id").eq(col("users.id")));

        let sql = delete.to_sql(Dialect::Postgres);
        assert!(sql.contains("USING"));
    }

    #[test]
    fn test_delete_returning() {
        let delete = Delete::from("users")
            .filter(col("id").eq(lit_int(1)))
            .returning([col("id"), col("name")]);

        let sql = delete.to_sql(Dialect::Postgres);
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn test_insert_with_schema() {
        let insert = Insert::into("users")
            .schema("public")
            .columns(["name"])
            .values([lit_str("Alice")]);

        let sql = insert.to_sql(Dialect::Postgres);
        assert!(sql.contains("\"public\".\"users\""));
    }

    #[test]
    fn test_tsql_output_insert() {
        let insert = Insert::into("users")
            .columns(["name"])
            .values([lit_str("Alice")])
            .returning([col("id")]);

        let sql = insert.to_sql(Dialect::TSql);
        // T-SQL uses OUTPUT INSERTED.* instead of RETURNING
        assert!(!sql.contains("RETURNING"));
        assert!(sql.contains("OUTPUT INSERTED.*"));
    }

    #[test]
    fn test_tsql_output_update() {
        let update = Update::table("users")
            .set("status", lit_str("active"))
            .filter(col("id").eq(lit_int(1)))
            .returning([star()]);

        let sql = update.to_sql(Dialect::TSql);
        assert!(!sql.contains("RETURNING"));
        assert!(sql.contains("OUTPUT INSERTED.*"));
    }

    #[test]
    fn test_tsql_output_delete() {
        let delete = Delete::from("users")
            .filter(col("id").eq(lit_int(1)))
            .returning([col("id"), col("name")]);

        let sql = delete.to_sql(Dialect::TSql);
        assert!(!sql.contains("RETURNING"));
        assert!(sql.contains("OUTPUT DELETED.*"));
    }

    // ========================================================================
    // Snapshot tests with roundtrip validation
    // ========================================================================

    mod snapshot_tests {
        use super::*;
        use crate::sql::expr::{col, lit_int, lit_str, table_col, ExprExt};
        use crate::sql::query::{Query, TableRef};
        use crate::sql::test_utils::validate_sql;
        use insta::assert_snapshot;

        // --------------------------------------------------------------------
        // Merge tests
        // --------------------------------------------------------------------

        #[test]
        fn merge_basic_upsert_tsql() {
            let sql = Merge::into("target")
                .using_table("source")
                .source_alias("s")
                .target_alias("t")
                .on(table_col("t", "id").eq(table_col("s", "id")))
                .when_matched_update(vec![("name", table_col("s", "name"))])
                .when_not_matched_insert(
                    vec!["id", "name"],
                    vec![table_col("s", "id"), table_col("s", "name")],
                )
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn merge_basic_upsert_postgres() {
            let sql = Merge::into("target")
                .using_table("source")
                .source_alias("s")
                .target_alias("t")
                .on(table_col("t", "id").eq(table_col("s", "id")))
                .when_matched_update(vec![("name", table_col("s", "name"))])
                .when_not_matched_insert(
                    vec!["id", "name"],
                    vec![table_col("s", "id"), table_col("s", "name")],
                )
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn merge_with_delete_tsql() {
            let sql = Merge::into("target")
                .using_table("source")
                .source_alias("s")
                .target_alias("t")
                .on(table_col("t", "id").eq(table_col("s", "id")))
                .when_matched_delete()
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn merge_with_conditional_delete_tsql() {
            let sql = Merge::into("target")
                .using_table("source")
                .source_alias("s")
                .target_alias("t")
                .on(table_col("t", "id").eq(table_col("s", "id")))
                .when_matched_and_delete(table_col("s", "deleted").eq(true))
                .when_matched_update(vec![("name", table_col("s", "name"))])
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn merge_with_schema_tsql() {
            let sql = Merge::into("target")
                .target_schema("dbo")
                .using_table("source")
                .source_alias("s")
                .target_alias("t")
                .on(table_col("t", "id").eq(table_col("s", "id")))
                .when_matched_update(vec![("name", table_col("s", "name"))])
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn merge_using_query_tsql() {
            let source_query = Query::new()
                .select(vec![col("id"), col("name")])
                .from(TableRef::new("staging_users"))
                .filter(col("processed").eq(false));

            let sql = Merge::into("users")
                .using_query(source_query)
                .source_alias("s")
                .target_alias("t")
                .on(table_col("t", "id").eq(table_col("s", "id")))
                .when_matched_update(vec![("name", table_col("s", "name"))])
                .when_not_matched_insert(
                    vec!["id", "name"],
                    vec![table_col("s", "id"), table_col("s", "name")],
                )
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        #[test]
        fn merge_multiple_update_columns_tsql() {
            let sql = Merge::into("products")
                .using_table("product_updates")
                .source_alias("src")
                .target_alias("tgt")
                .on(table_col("tgt", "sku").eq(table_col("src", "sku")))
                .when_matched_update(vec![
                    ("name", table_col("src", "name")),
                    ("price", table_col("src", "price")),
                    ("stock", table_col("src", "stock")),
                ])
                .to_sql(Dialect::TSql);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::TSql).unwrap();
        }

        // --------------------------------------------------------------------
        // Insert snapshot tests
        // --------------------------------------------------------------------

        #[test]
        fn insert_basic_postgres() {
            let sql = Insert::into("users")
                .columns(["name", "email"])
                .values([lit_str("Alice"), lit_str("alice@example.com")])
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn insert_on_conflict_do_nothing_postgres() {
            let sql = Insert::into("users")
                .columns(["id", "name"])
                .values([lit_int(1), lit_str("Alice")])
                .on_conflict(OnConflict::do_nothing())
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn insert_on_conflict_do_update_postgres() {
            let sql = Insert::into("users")
                .columns(["id", "name", "email"])
                .values([lit_int(1), lit_str("Alice"), lit_str("alice@example.com")])
                .on_conflict(OnConflict::do_update(
                    ["id"],
                    [
                        ("name", lit_str("Alice Updated")),
                        ("email", lit_str("new@example.com")),
                    ],
                ))
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn insert_returning_postgres() {
            let sql = Insert::into("users")
                .columns(["name", "email"])
                .values([lit_str("Alice"), lit_str("alice@example.com")])
                .returning([col("id")])
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        // --------------------------------------------------------------------
        // Update snapshot tests
        // --------------------------------------------------------------------

        #[test]
        fn update_basic_postgres() {
            let sql = Update::table("users")
                .set("status", lit_str("active"))
                .filter(col("id").eq(lit_int(1)))
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn update_multiple_columns_postgres() {
            let sql = Update::table("users")
                .set("name", lit_str("Alice"))
                .set("age", lit_int(30))
                .set("status", lit_str("active"))
                .filter(col("id").eq(lit_int(1)))
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        // --------------------------------------------------------------------
        // Delete snapshot tests
        // --------------------------------------------------------------------

        #[test]
        fn delete_basic_postgres() {
            let sql = Delete::from("users")
                .filter(col("status").eq(lit_str("inactive")))
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }

        #[test]
        fn delete_with_returning_postgres() {
            let sql = Delete::from("users")
                .filter(col("id").eq(lit_int(1)))
                .returning([col("id"), col("name")])
                .to_sql(Dialect::Postgres);
            assert_snapshot!(sql);
            validate_sql(&sql, Dialect::Postgres).unwrap();
        }
    }
}
