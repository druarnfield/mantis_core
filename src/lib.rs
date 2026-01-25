//! # Mantis
//!
//! A universal semantic layer that compiles to multi-dialect SQL.
//!
//! ## Architecture
//!
//! Mantis provides a DSL-first semantic modeling system:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │              DSL (Semantic Model Definition)             │
//! │  (tables, dimensions, measures, calendars, reports)      │
//! └─────────────────────────────────────────────────────────┘
//!                          │
//!                          ▼ [parser]
//! ┌─────────────────────────────────────────────────────────┐
//! │                     AST                                  │
//! └─────────────────────────────────────────────────────────┘
//!                          │
//!                          ▼ [lowering]
//! ┌─────────────────────────────────────────────────────────┐
//! │                  Model (Rust Types)                      │
//! └─────────────────────────────────────────────────────────┘
//!                          │
//!                          ▼ [graph builder]
//! ┌─────────────────────────────────────────────────────────┐
//! │          UnifiedGraph (Column-Level Semantic)            │
//! │          + Inference (Relationship Discovery)            │
//! └─────────────────────────────────────────────────────────┘
//!                          │
//!                          ▼ [planner - to be rebuilt]
//! ┌─────────────────────────────────────────────────────────┐
//! │                    SQL Query                             │
//! └─────────────────────────────────────────────────────────┘
//! ```

pub mod cache;
pub mod config;
pub mod crypto;
pub mod dsl;
pub mod lowering;
// pub mod lsp;  // Temporarily disabled - needs rebuilding with new DSL loader
pub mod metadata;
pub mod model;
pub mod semantic;
pub mod sql;
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
