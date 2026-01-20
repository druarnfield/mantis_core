//! # Mantis
//!
//! A universal semantic layer that compiles to multi-dialect SQL.
//!
//! ## Quick Start
//!
//! ```rust
//! use mantis::prelude::*;
//!
//! let query = Query::new()
//!     .select(vec![col("id"), col("name")])
//!     .from(TableRef::new("users").with_schema("dbo"))
//!     .filter(col("active").eq(true))
//!     .filter(col("age").gte(18))
//!     .order_by(vec![OrderByExpr::desc(col("created_at"))])
//!     .limit(10);
//!
//! // Generate SQL for different dialects
//! println!("{}", query.to_sql(Dialect::DuckDb));
//! println!("{}", query.to_sql(Dialect::TSql));
//! println!("{}", query.to_sql(Dialect::MySql));
//! println!("{}", query.to_sql(Dialect::Postgres));
//! ```
//!
//! ## Architecture
//!
//! Mantis is a **unified semantic layer** serving two purposes:
//!
//! - **Build**: Transform normalized sources into a star schema warehouse
//! - **Query**: Provide a semantic interface over the warehouse for analytics
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                        Model                             │
//! │  (Sources, Relationships, Facts, Dimensions, Measures)   │
//! └─────────────────────────────────────────────────────────┘
//!                          │
//!          ┌───────────────┴───────────────┐
//!          ▼                               ▼
//!   TransformPlanner                 QueryPlanner
//!   (sources → DDL)               (query → SELECT)
//! ```

pub mod cache;
pub mod config;
pub mod crypto;
pub mod dsl;
pub mod lowering;
pub mod lsp;
pub mod metadata;
pub mod model;
pub mod semantic;
pub mod sql;
pub mod translation;
pub mod validation;
pub mod worker;

// Re-export SQL submodules at crate level for backwards compatibility
pub use sql::ddl;
pub use sql::dialect;
pub use sql::dml;
pub use sql::expr;
pub use sql::query;
pub use sql::token;

#[cfg(feature = "ui")]
pub mod web;

/// Re-exports for convenient usage.
pub mod prelude {
    pub use crate::dialect::{Dialect, SqlDialect};
    pub use crate::expr::{
        // Constructors
        avg,
        coalesce,
        col,
        count,
        count_distinct,
        count_star,
        func,
        lit_bool,
        lit_float,
        lit_int,
        lit_null,
        lit_str,
        max,
        min,
        star,
        sum,
        table_col,
        table_star,
        // Types
        BinaryOperator,
        Expr,
        ExprExt,
        Literal,
        UnaryOperator,
    };
    pub use crate::query::{
        Cte, Join, JoinType, LimitOffset, NullsOrder, OrderByExpr, Query, SelectExpr, SortDir,
        TableRef,
    };
    pub use crate::token::{Token, TokenStream};
}

// Also export at crate root for convenience
pub use dialect::Dialect;
pub use expr::{col, count_star, lit_bool, lit_int, lit_str, sum, table_col, Expr, ExprExt};
pub use query::{OrderByExpr, Query, SelectExpr, TableRef};
pub use token::{Token, TokenStream};
