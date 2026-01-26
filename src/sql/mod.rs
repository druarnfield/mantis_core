//! SQL generation module.
//!
//! This module provides a type-safe SQL builder that generates multi-dialect SQL.
//! It includes:
//!
//! - [`query`] - SELECT query builder
//! - [`expr`] - Expression AST and builder DSL
//! - [`ddl`] - Data Definition Language (CREATE, ALTER, DROP, TRUNCATE, VIEW)
//! - [`dml`] - Data Manipulation Language (INSERT, UPDATE, DELETE, MERGE)
//! - [`token`] - Token types for SQL generation
//! - [`dialect`] - SQL dialect implementations

pub mod ddl;
pub mod dialect;
pub mod dml;
pub mod expr;
pub mod query;
pub mod token;
pub mod types;

pub use types::DataType as SqlDataType;

#[cfg(test)]
pub mod test_utils;

// Re-export commonly used types at the sql module level
pub use dialect::{Dialect, SqlDialect};
pub use expr::{
    avg, coalesce, col, count, count_distinct, count_star, func, lag_offset, lit_bool, lit_float,
    lit_int, lit_null, lit_str, max, min, star, sum, table_col, table_star, BinaryOperator, Expr,
    ExprExt, Literal, UnaryOperator, WindowExt, WindowFrame, WindowOrderBy,
};
pub use query::{
    Cte, Join, JoinType, LimitOffset, NullsOrder, OrderByExpr, Query, SelectExpr, SortDir, TableRef,
};
pub use token::{Token, TokenStream};

// Re-export DDL types
pub use ddl::{
    AlterAction, AlterTable, ColumnConstraint, ColumnDef, CreateIndex, CreateTable, CreateView,
    DataType, DdlStatement, DropIndex, DropTable, DropView, IndexColumn, ReferentialAction,
    TableConstraint, Truncate,
};

// Re-export DML types
pub use dml::{Delete, Insert, Merge, MergeAction, MergeSource, OnConflict, Update, WhenClause};
