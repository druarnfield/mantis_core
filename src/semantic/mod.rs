//! Semantic layer - graph construction and inference.
//!
//! This module provides the new semantic layer built on:
//! - **UnifiedGraph**: Column-level graph with entities, columns, measures, calendars
//! - **Inference**: Relationship inference from database metadata
//!
//! The old planner has been archived. Query planning will be rebuilt
//! using the UnifiedGraph architecture.

pub mod error;
pub mod graph;
pub mod inference;

// Re-export Cardinality from graph (unified location)
pub use graph::Cardinality;

// Re-export error types
pub use error::{SemanticError, SemanticResult};
