//! Resolved types - output of the resolution phase.
//!
//! After resolution, all field references have been validated and
//! mapped to their physical representations.

use std::collections::HashSet;

use crate::model::AggregationType;
use crate::semantic::model_graph::JoinEdge;

/// A fully resolved query - all fields validated and mapped to physical names.
#[derive(Debug, Clone)]
pub struct ResolvedQuery {
    /// The root entity (logical name, used as SQL alias).
    pub from: ResolvedEntity,

    /// All entities that need to be joined.
    pub referenced_entities: HashSet<String>,

    /// Resolved filter conditions.
    pub filters: Vec<ResolvedFilter>,

    /// Resolved GROUP BY columns.
    pub group_by: Vec<ResolvedColumn>,

    /// Resolved SELECT expressions.
    pub select: Vec<ResolvedSelect>,

    /// Resolved ORDER BY expressions.
    pub order_by: Vec<ResolvedOrder>,

    /// Limit clause.
    pub limit: Option<u64>,
}

/// A resolved entity reference.
#[derive(Debug, Clone)]
pub struct ResolvedEntity {
    /// Logical entity name (used as SQL alias).
    pub name: String,

    /// Physical table name.
    pub physical_table: String,

    /// Physical schema name.
    pub physical_schema: Option<String>,

    /// Whether this entity is materialized as a physical table.
    ///
    /// When `false`, the entity is "virtual" and must be reconstructed
    /// from source entities at query time instead of querying directly.
    pub materialized: bool,
}

/// A resolved column reference.
#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    /// Logical entity name (used as SQL table alias).
    pub entity_alias: String,

    /// Logical column name.
    pub logical_name: String,

    /// Physical column name in the database.
    pub physical_name: String,
}

/// A resolved measure reference.
#[derive(Debug, Clone)]
pub struct ResolvedMeasure {
    /// Logical entity name (used as SQL table alias).
    pub entity_alias: String,

    /// Measure name.
    pub name: String,

    /// Aggregation type.
    pub aggregation: AggregationType,

    /// Source column (physical name), or "*" for COUNT(*).
    pub source_column: String,

    /// Optional query-time filter for conditional aggregation.
    ///
    /// When present, generates CASE WHEN expressions:
    /// `SUM(CASE WHEN condition THEN column END)`
    pub filter: Option<Vec<ResolvedFilter>>,

    /// Optional definition-time filter from the measure definition.
    ///
    /// This is the filter specified when the measure was defined, e.g.:
    /// `completed_revenue = sum("amount"):where("status = 'completed'")`
    pub definition_filter: Option<crate::model::expr::Expr>,
}

/// A resolved SELECT item.
#[derive(Debug, Clone)]
pub enum ResolvedSelect {
    /// A column reference.
    Column {
        column: ResolvedColumn,
        alias: Option<String>,
    },

    /// A measure (aggregate from fact definition).
    Measure {
        measure: ResolvedMeasure,
        alias: Option<String>,
    },

    /// An inline aggregate (e.g., SUM(orders.amount)).
    Aggregate {
        column: ResolvedColumn,
        aggregation: String,
        alias: Option<String>,
    },

    /// A derived measure - calculation from other measures.
    ///
    /// e.g., `aov = revenue / order_count`
    Derived {
        /// Output alias
        alias: String,
        /// The calculation expression
        expression: ResolvedDerivedExpr,
    },
}

/// A resolved derived expression - calculation from measures.
#[derive(Debug, Clone)]
pub enum ResolvedDerivedExpr {
    /// Reference to a measure - stores full measure info for SQL emission.
    MeasureRef(ResolvedMeasure),
    /// A literal numeric value
    Literal(f64),
    /// Binary operation
    BinaryOp {
        left: Box<ResolvedDerivedExpr>,
        op: super::types::DerivedBinaryOp,
        right: Box<ResolvedDerivedExpr>,
    },
    /// Unary negation
    Negate(Box<ResolvedDerivedExpr>),

    // =========================================================================
    // Time Intelligence Extensions
    // =========================================================================

    /// A time intelligence function (YTD, prior year, rolling, etc.)
    TimeFunction(super::types::TimeFunction),

    /// Delta: difference between current and previous value
    Delta {
        current: Box<ResolvedDerivedExpr>,
        previous: Box<ResolvedDerivedExpr>,
    },

    /// Growth: percentage change from previous to current
    Growth {
        current: Box<ResolvedDerivedExpr>,
        previous: Box<ResolvedDerivedExpr>,
    },
}

impl ResolvedSelect {
    /// Get the output alias for this select item.
    pub fn output_alias(&self) -> &str {
        match self {
            ResolvedSelect::Column { column, alias } => {
                alias.as_deref().unwrap_or(&column.logical_name)
            }
            ResolvedSelect::Measure { measure, alias } => {
                alias.as_deref().unwrap_or(&measure.name)
            }
            ResolvedSelect::Aggregate { column, alias, .. } => {
                alias.as_deref().unwrap_or(&column.logical_name)
            }
            ResolvedSelect::Derived { alias, .. } => alias,
        }
    }

    /// Check if this is an aggregate (Measure, Aggregate, or Derived).
    pub fn is_aggregate(&self) -> bool {
        matches!(
            self,
            ResolvedSelect::Measure { .. }
                | ResolvedSelect::Aggregate { .. }
                | ResolvedSelect::Derived { .. }
        )
    }
}

/// A resolved filter condition.
#[derive(Debug, Clone)]
pub struct ResolvedFilter {
    /// The column being filtered.
    pub column: ResolvedColumn,

    /// The filter operator.
    pub op: super::types::FilterOp,

    /// The filter value.
    pub value: super::types::FilterValue,
}

/// A resolved ORDER BY item.
#[derive(Debug, Clone)]
pub struct ResolvedOrder {
    /// What to order by.
    pub expr: ResolvedOrderExpr,

    /// Descending order?
    pub descending: bool,
}

/// A resolved ORDER BY expression.
#[derive(Debug, Clone)]
pub enum ResolvedOrderExpr {
    Column(ResolvedColumn),
    Measure(ResolvedMeasure),
}

/// The join tree computed during resolution.
#[derive(Debug, Clone)]
pub struct ResolvedJoinTree {
    /// The root entity.
    pub root: String,

    /// The edges in the join tree.
    pub edges: Vec<JoinEdge>,

    /// Is the join tree safe (no fan-out)?
    pub is_safe: bool,
}

impl ResolvedJoinTree {
    pub fn empty(root: &str) -> Self {
        Self {
            root: root.into(),
            edges: vec![],
            is_safe: true,
        }
    }
}

// =============================================================================
// Multi-Fact Query Types
// =============================================================================

/// A resolved query that may be single-fact or multi-fact.
///
/// Single-fact queries use the standard join pattern.
/// Multi-fact queries use the symmetric aggregate pattern with CTEs.
#[derive(Debug, Clone)]
pub enum ResolvedQueryPlan {
    /// Single fact query - standard join pattern.
    Single(ResolvedQuery),

    /// Multi-fact query - symmetric aggregate pattern.
    Multi(MultiFactQuery),
}

/// A multi-fact query with shared dimensions.
///
/// This generates a symmetric aggregate pattern:
/// ```sql
/// WITH fact1_agg AS (SELECT keys, measures FROM fact1 GROUP BY keys),
///      fact2_agg AS (SELECT keys, measures FROM fact2 GROUP BY keys)
/// SELECT dims, COALESCE(f1.measure, 0), COALESCE(f2.measure, 0)
/// FROM fact1_agg f1
/// FULL OUTER JOIN fact2_agg f2 ON f1.keys = f2.keys
/// JOIN dimensions ON COALESCE(f1.key, f2.key) = dim.key
/// ```
#[derive(Debug, Clone)]
pub struct MultiFactQuery {
    /// Aggregates per fact (one CTE per fact).
    pub fact_aggregates: Vec<FactAggregate>,

    /// Shared dimensions - reachable from ALL facts.
    /// These become the join keys between CTEs.
    pub shared_dimensions: Vec<SharedDimension>,

    /// Filters that apply to all facts (pushed into each CTE).
    pub global_filters: Vec<ResolvedFilter>,

    /// Order by (references output aliases from the final SELECT).
    pub order_by: Vec<ResolvedOrder>,

    /// Limit clause.
    pub limit: Option<u64>,
}

/// Aggregates from a single fact - becomes one CTE.
#[derive(Debug, Clone)]
pub struct FactAggregate {
    /// The fact entity.
    pub fact: ResolvedEntity,

    /// CTE alias (e.g., "orders_agg").
    pub cte_alias: String,

    /// Join keys - subset of shared dimensions that this fact has FKs for.
    /// These columns are selected in the CTE and used for the FULL OUTER JOIN.
    pub join_keys: Vec<FactJoinKey>,

    /// Measures from this fact.
    pub measures: Vec<ResolvedMeasure>,

    /// Filters specific to this fact (in addition to global filters).
    pub fact_filters: Vec<ResolvedFilter>,
}

/// A join key from a fact to a dimension.
#[derive(Debug, Clone)]
pub struct FactJoinKey {
    /// FK column on the fact (physical name).
    pub fact_column: String,

    /// The dimension this key joins to.
    pub dimension: String,

    /// PK column on the dimension (physical name).
    pub dimension_column: String,
}

/// A shared dimension - reachable from all anchor facts.
#[derive(Debug, Clone)]
pub struct SharedDimension {
    /// The dimension entity.
    pub dimension: ResolvedEntity,

    /// Columns selected from this dimension.
    pub columns: Vec<ResolvedColumn>,

    /// Join paths from each fact to this dimension.
    /// Key is fact name, value is the join key info.
    pub paths: Vec<(String, FactJoinKey)>,
}

impl MultiFactQuery {
    /// Get all fact names in this query.
    pub fn fact_names(&self) -> Vec<&str> {
        self.fact_aggregates
            .iter()
            .map(|fa| fa.fact.name.as_str())
            .collect()
    }

    /// Get all CTE aliases.
    pub fn cte_aliases(&self) -> Vec<&str> {
        self.fact_aggregates
            .iter()
            .map(|fa| fa.cte_alias.as_str())
            .collect()
    }

    /// Get all dimension entities.
    pub fn dimension_entities(&self) -> Vec<&ResolvedEntity> {
        self.shared_dimensions
            .iter()
            .map(|sd| &sd.dimension)
            .collect()
    }
}
