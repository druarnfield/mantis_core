//! Semantic layer - entities, relationships, and query planning.
//!
//! This module provides the abstraction layer between user queries
//! and SQL generation. Users define entities (tables), their columns,
//! measures (pre-defined aggregations), and relationships (joins).
//!
//! The query planner uses a four-phase architecture:
//!
//! 1. **Resolve** - Resolve field references to physical names
//! 2. **Validate** - Check join safety and GROUP BY completeness
//! 3. **Plan** - Build logical operation tree
//! 4. **Emit** - Convert to SQL Query builder
//!
//! This separation allows for:
//! - Better error messages (phase-specific errors)
//! - Easier testing (test each phase independently)
//! - Future extensions (HAVING, window functions, subqueries)

pub mod column_lineage;
pub mod error;
pub mod executor;
pub mod graph;
pub mod inference;
pub mod model_graph;
pub mod planner;
pub mod semantic_model;

// Re-export Cardinality from graph (new unified location)
pub use graph::Cardinality;

// Re-export SemanticModel (primary entry point)
pub use semantic_model::SemanticModel;

// Re-export error types
pub use error::{SemanticError, SemanticResult};
// Legacy aliases for backward compatibility
pub use error::{GraphError, GraphResult, PlanError, PlanResult};

// Re-export column lineage types
pub use column_lineage::{
    ColumnLineageGraph, ColumnRef, LineageCycleError, LineageEdge, LineageType, SerializedEdge,
    SerializedLineage,
};

// Re-export model graph types (primary graph implementation)
pub use model_graph::{
    EdgeData, EntityInfo, EntityNode, EntityType, JoinEdge, JoinPath, ModelGraph,
    ModelResolvedField,
};

// Re-export planner types
pub use planner::{
    // Query types
    DerivedBinaryOp,
    DerivedExpr,
    DerivedField,
    // Phase types (for advanced usage)
    Emitter,
    FieldFilter,
    FieldRef,
    FilterOp,
    FilterValue,
    LogicalPlan,
    LogicalPlanner,
    OrderField,
    // Planner
    PlanPhases,
    QueryPlanner,
    ResolvedColumn,
    ResolvedMeasure,
    ResolvedQuery,
    ResolvedSelect,
    Resolver,
    SelectField,
    SemanticQuery,
    TimeEmitter,
    TimeFunction,
    ValidatedQuery,
    Validator,
};

// Re-export executor
pub use executor::QueryExecutor;
