//! Column-level lineage tracking.
//!
//! This module provides fine-grained column lineage analysis, tracking
//! how each output column depends on source columns through transformations,
//! aggregations, joins, and window functions.
//!
//! # Use Cases
//!
//! - **Incremental updates**: Know exactly which columns are affected by a source change
//! - **Impact analysis**: Understand what breaks if a source column is modified
//! - **Column pruning**: Only compute columns actually needed for a query
//! - **Documentation**: Auto-generate data lineage documentation

use std::collections::{HashMap, HashSet, VecDeque};

use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::{Deserialize, Serialize};

use crate::model::dimension::SCDType;
use crate::model::expr::{Expr, OrderByExpr, WhenClause};
use crate::model::fact::ColumnSelection;
use crate::model::table::TableDefinition;
use crate::model::{ColumnDef, Model};

// =============================================================================
// Core Types
// =============================================================================

/// A reference to a specific column in a specific entity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Entity name (source, intermediate, fact, or dimension)
    pub entity: String,
    /// Column name
    pub column: String,
}

impl ColumnRef {
    pub fn new(entity: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            column: column.into(),
        }
    }

    /// Parse from "entity.column" format.
    /// Returns None for invalid formats including empty entity or column.
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.splitn(2, '.').collect();
        if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
            Some(Self::new(parts[0], parts[1]))
        } else {
            None
        }
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.entity, self.column)
    }
}

/// The type of lineage relationship between columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LineageType {
    /// Direct passthrough (column is copied as-is)
    Passthrough,

    /// Column is computed from source columns via expression
    Transform,

    /// Column is an aggregation of source column(s)
    Aggregate,

    /// Column participates in a GROUP BY clause
    GroupBy,

    /// Column is used as a join key
    JoinKey,

    /// Column is used in window function PARTITION BY
    WindowPartition,

    /// Column is used in window function ORDER BY
    WindowOrder,

    /// Column is used in a filter/WHERE condition
    Filter,
}

/// An edge in the lineage graph representing a dependency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Type of lineage relationship
    pub lineage_type: LineageType,
    /// Optional expression that creates this dependency
    pub expression: Option<String>,
}

impl LineageEdge {
    pub fn passthrough() -> Self {
        Self {
            lineage_type: LineageType::Passthrough,
            expression: None,
        }
    }

    pub fn transform(expr: impl Into<String>) -> Self {
        Self {
            lineage_type: LineageType::Transform,
            expression: Some(expr.into()),
        }
    }

    pub fn aggregate() -> Self {
        Self {
            lineage_type: LineageType::Aggregate,
            expression: None,
        }
    }

    pub fn group_by() -> Self {
        Self {
            lineage_type: LineageType::GroupBy,
            expression: None,
        }
    }

    pub fn join_key() -> Self {
        Self {
            lineage_type: LineageType::JoinKey,
            expression: None,
        }
    }

    pub fn window_partition() -> Self {
        Self {
            lineage_type: LineageType::WindowPartition,
            expression: None,
        }
    }

    pub fn window_order() -> Self {
        Self {
            lineage_type: LineageType::WindowOrder,
            expression: None,
        }
    }

    pub fn filter() -> Self {
        Self {
            lineage_type: LineageType::Filter,
            expression: None,
        }
    }
}

// =============================================================================
// Cycle Detection Error
// =============================================================================

/// Error returned when cycles are detected in the lineage graph.
#[derive(Debug, Clone)]
pub struct LineageCycleError {
    /// List of cycles found, each cycle is a list of columns in the cycle
    pub cycles: Vec<Vec<ColumnRef>>,
}

impl std::fmt::Display for LineageCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Circular dependencies detected in column lineage:")?;
        for (i, cycle) in self.cycles.iter().enumerate() {
            let path = cycle
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(" → ");
            writeln!(f, "  Cycle {}: {} → (back to start)", i + 1, path)?;
        }
        Ok(())
    }
}

impl std::error::Error for LineageCycleError {}

// =============================================================================
// Serialization
// =============================================================================

/// A single edge in serialized format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedEdge {
    /// Source column
    pub from: ColumnRef,
    /// Target column
    pub to: ColumnRef,
    /// Edge metadata
    pub edge: LineageEdge,
}

/// Serializable representation of a column lineage graph.
///
/// Uses an edge list format for efficient JSON serialization and SQLite storage.
/// This is the format used for caching lineage graphs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedLineage {
    /// All edges in the graph
    pub edges: Vec<SerializedEdge>,
}

// =============================================================================
// Column Lineage Graph
// =============================================================================

/// Column-level lineage graph.
///
/// Nodes are columns (ColumnRef), edges represent dependencies
/// with metadata about the type of dependency.
pub struct ColumnLineageGraph {
    /// The underlying directed graph
    graph: DiGraph<ColumnRef, LineageEdge>,
    /// Map from ColumnRef to NodeIndex for quick lookup
    node_index: HashMap<ColumnRef, NodeIndex>,
}

impl ColumnLineageGraph {
    /// Create an empty lineage graph.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_index: HashMap::new(),
        }
    }

    /// Build lineage graph from a Model.
    pub fn from_model(model: &Model) -> Self {
        let mut graph = Self::new();
        graph.build_from_model(model);
        graph
    }

    /// Get or create a node for a column reference.
    fn get_or_create_node(&mut self, col_ref: ColumnRef) -> NodeIndex {
        if let Some(&idx) = self.node_index.get(&col_ref) {
            idx
        } else {
            let idx = self.graph.add_node(col_ref.clone());
            self.node_index.insert(col_ref, idx);
            idx
        }
    }

    /// Add a lineage edge from source column to target column.
    /// Prevents duplicate edges with the same lineage type between the same nodes.
    pub fn add_edge(&mut self, from: ColumnRef, to: ColumnRef, edge: LineageEdge) {
        let from_idx = self.get_or_create_node(from);
        let to_idx = self.get_or_create_node(to);

        // Check if an edge with the same lineage type already exists
        let edge_exists = self
            .graph
            .edges_connecting(from_idx, to_idx)
            .any(|e| e.weight().lineage_type == edge.lineage_type);

        if !edge_exists {
            self.graph.add_edge(from_idx, to_idx, edge);
        }
    }

    /// Build the lineage graph from a model.
    fn build_from_model(&mut self, model: &Model) {
        // Process tables (ETL layer)
        for table in model.tables.values() {
            self.process_table(table, model);
        }

        // Process facts
        for fact in model.facts.values() {
            self.process_fact(fact, model);
        }

        // Process dimensions
        for dim in model.dimensions.values() {
            self.process_dimension(dim, model);
        }

        // Process reports
        for report in model.reports.values() {
            self.process_report(report, model);
        }

        // Process pivot reports
        for pivot_report in model.pivot_reports.values() {
            self.process_pivot_report(pivot_report, model);
        }
    }

    /// Process a fact definition to extract lineage.
    fn process_fact(&mut self, fact: &crate::model::FactDefinition, model: &Model) {
        let target_entity = &fact.name;

        // Determine the source entity (from field or grain entity)
        let source_entity = match fact
            .from
            .as_ref()
            .or_else(|| fact.grain.first().map(|g| &g.source_entity))
            .cloned()
        {
            Some(entity) if !entity.is_empty() => entity,
            _ => {
                // No valid source entity - skip processing this fact
                // This prevents creating invalid lineage with empty entity names
                return;
            }
        };

        // Process grain columns
        for grain in &fact.grain {
            let target_col = grain.target_name.as_ref().unwrap_or(&grain.source_column);
            self.add_edge(
                ColumnRef::new(&grain.source_entity, &grain.source_column),
                ColumnRef::new(target_entity, target_col),
                LineageEdge::passthrough(),
            );
        }

        // Process computed columns
        for col_def in &fact.columns {
            match col_def {
                ColumnDef::Simple(col_name) => {
                    self.add_edge(
                        ColumnRef::new(&source_entity, col_name),
                        ColumnRef::new(target_entity, col_name),
                        LineageEdge::passthrough(),
                    );
                }
                ColumnDef::Renamed { source, target } => {
                    self.add_edge(
                        ColumnRef::new(&source_entity, source),
                        ColumnRef::new(target_entity, target),
                        LineageEdge::passthrough(),
                    );
                }
                ColumnDef::Computed { name, expr, .. } => {
                    let deps = extract_column_refs(expr, Some(&source_entity));
                    for dep in deps {
                        self.add_edge(
                            dep,
                            ColumnRef::new(target_entity, name),
                            LineageEdge::transform(format!("{:?}", expr)),
                        );
                    }
                }
            }
        }

        // Process measures
        for (measure_name, measure) in &fact.measures {
            // Measures depend on their source column
            self.add_edge(
                ColumnRef::new(&source_entity, &measure.source_column),
                ColumnRef::new(target_entity, measure_name),
                LineageEdge::aggregate(),
            );

            // If measure has a filter, extract column references from the filter expression
            if let Some(filter_expr) = &measure.filter {
                let filter_deps = extract_column_refs(filter_expr, Some(&source_entity));
                for dep in filter_deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, measure_name),
                        LineageEdge::filter(),
                    );
                }
            }
        }

        // Process includes (dimension lookups)
        for (alias, include) in &fact.includes {
            let dim_entity = &include.entity;
            let prefix = include.prefix.as_deref().unwrap_or(alias);

            // Get all available columns - prefer dimension columns, fallback to source
            let all_columns: Vec<String> = if let Some(dim) = model.dimensions.get(dim_entity) {
                // Use the dimension's defined columns
                dim.columns.iter().map(|c| c.source_column.clone()).collect()
            } else if let Some(source) = model.sources.get(dim_entity) {
                // Fallback to source columns
                source.columns.keys().cloned().collect()
            } else {
                Vec::new()
            };

            // Get the list of columns to include
            let columns: Vec<String> = match &include.selection {
                ColumnSelection::Columns(cols) => cols.clone(),
                ColumnSelection::All => {
                    // Use all columns from the dimension/source
                    all_columns
                }
                ColumnSelection::Except(excluded) => {
                    // Use all columns except the excluded ones
                    all_columns
                        .into_iter()
                        .filter(|col| !excluded.contains(col))
                        .collect()
                }
            };

            // If we have a dimension definition, use it to map columns
            if let Some(dim) = model.dimensions.get(dim_entity) {
                for attr in &columns {
                    // The attribute comes from the dimension's source
                    self.add_edge(
                        ColumnRef::new(&dim.source_entity, attr),
                        ColumnRef::new(target_entity, format!("{}_{}", prefix, attr)),
                        LineageEdge::transform("dimension lookup"),
                    );
                }
            } else {
                // Fallback: assume attributes come directly from the included entity
                for attr in &columns {
                    self.add_edge(
                        ColumnRef::new(dim_entity, attr),
                        ColumnRef::new(target_entity, format!("{}_{}", prefix, attr)),
                        LineageEdge::transform("include"),
                    );
                }
            }
        }

        // Process window columns
        for window_col in &fact.window_columns {
            // Window functions depend on their arguments
            for arg in &window_col.args {
                let deps = extract_column_refs(arg, Some(&source_entity));
                for dep in deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, &window_col.name),
                        LineageEdge::transform("window function"),
                    );
                }
            }

            // Partition by columns
            for part in &window_col.partition_by {
                let deps = extract_column_refs(part, Some(&source_entity));
                for dep in deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, &window_col.name),
                        LineageEdge::window_partition(),
                    );
                }
            }

            // Order by columns
            for ord in &window_col.order_by {
                let deps = extract_column_refs(&ord.expr, Some(&source_entity));
                for dep in deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, &window_col.name),
                        LineageEdge::window_order(),
                    );
                }
            }
        }
    }

    /// Process a dimension definition to extract lineage.
    fn process_dimension(&mut self, dim: &crate::model::DimensionDefinition, _model: &Model) {
        let target_entity = &dim.name;
        let source_entity = &dim.source_entity;

        // Process dimension columns
        for col in &dim.columns {
            let target_col = col.target_column.as_ref().unwrap_or(&col.source_column);
            self.add_edge(
                ColumnRef::new(source_entity, &col.source_column),
                ColumnRef::new(target_entity, target_col),
                LineageEdge::passthrough(),
            );
        }

        // Process SCD columns based on SCD type
        match &dim.scd_type {
            SCDType::Type0 | SCDType::Type1 => {
                // No additional SCD columns to track
            }
            SCDType::Type2 {
                effective_from,
                effective_to,
                is_current,
            } => {
                // Type2 SCD columns are system-generated (timestamps/flags)
                // They depend on all tracked columns conceptually (any change creates new row)
                // We create a synthetic dependency from all source columns to these SCD columns
                for col in &dim.columns {
                    self.add_edge(
                        ColumnRef::new(source_entity, &col.source_column),
                        ColumnRef::new(target_entity, effective_from),
                        LineageEdge::transform("SCD2 effective_from"),
                    );
                    self.add_edge(
                        ColumnRef::new(source_entity, &col.source_column),
                        ColumnRef::new(target_entity, effective_to),
                        LineageEdge::transform("SCD2 effective_to"),
                    );
                    if let Some(is_current_col) = is_current {
                        self.add_edge(
                            ColumnRef::new(source_entity, &col.source_column),
                            ColumnRef::new(target_entity, is_current_col),
                            LineageEdge::transform("SCD2 is_current"),
                        );
                    }
                }
            }
            SCDType::Type3 { tracked_columns } => {
                // Type3: previous value columns depend on their source columns
                for (source_col, prev_col) in tracked_columns {
                    self.add_edge(
                        ColumnRef::new(source_entity, source_col),
                        ColumnRef::new(target_entity, prev_col),
                        LineageEdge::transform("SCD3 previous value"),
                    );
                }
            }
            SCDType::Type6 {
                effective_from,
                effective_to,
                is_current,
                current_columns,
            } => {
                // Type6 is a hybrid - has both Type2 and Type3 characteristics
                // SCD columns depend on all tracked columns
                for col in &dim.columns {
                    self.add_edge(
                        ColumnRef::new(source_entity, &col.source_column),
                        ColumnRef::new(target_entity, effective_from),
                        LineageEdge::transform("SCD6 effective_from"),
                    );
                    self.add_edge(
                        ColumnRef::new(source_entity, &col.source_column),
                        ColumnRef::new(target_entity, effective_to),
                        LineageEdge::transform("SCD6 effective_to"),
                    );
                    self.add_edge(
                        ColumnRef::new(source_entity, &col.source_column),
                        ColumnRef::new(target_entity, is_current),
                        LineageEdge::transform("SCD6 is_current"),
                    );
                }
                // current_columns keep current value across all historical rows
                for current_col in current_columns {
                    self.add_edge(
                        ColumnRef::new(source_entity, current_col),
                        ColumnRef::new(target_entity, format!("current_{}", current_col)),
                        LineageEdge::transform("SCD6 current value"),
                    );
                }
            }
        }
    }

    /// Process a table definition to extract lineage.
    ///
    /// Tables are ETL transformations that take data from sources or other tables
    /// and produce new columns through passthrough, renaming, or computation.
    fn process_table(&mut self, table: &TableDefinition, _model: &Model) {
        let target_entity = &table.name;

        // Get source entities from the from clause
        let source_entities = table.from.sources();
        let primary_source = table.from.primary();

        // Process columns
        for col_def in &table.columns {
            match col_def {
                ColumnDef::Simple(col_name) => {
                    // For unions, create edges from all source entities
                    for source_entity in &source_entities {
                        self.add_edge(
                            ColumnRef::new(*source_entity, col_name),
                            ColumnRef::new(target_entity, col_name),
                            LineageEdge::passthrough(),
                        );
                    }
                }
                ColumnDef::Renamed { source, target } => {
                    for source_entity in &source_entities {
                        self.add_edge(
                            ColumnRef::new(*source_entity, source),
                            ColumnRef::new(target_entity, target),
                            LineageEdge::passthrough(),
                        );
                    }
                }
                ColumnDef::Computed { name, expr, .. } => {
                    let deps = extract_column_refs(expr, Some(primary_source));
                    for dep in deps {
                        self.add_edge(
                            dep,
                            ColumnRef::new(target_entity, name),
                            LineageEdge::transform(format!("{:?}", expr)),
                        );
                    }
                }
            }
        }

        // Process joins - columns from joined entities contribute via join keys
        for join in &table.joins {
            let join_deps = extract_column_refs(&join.on, Some(primary_source));
            for dep in join_deps {
                // Create a synthetic edge for join key dependency
                self.add_edge(
                    dep.clone(),
                    ColumnRef::new(target_entity, &format!("_join_{}", join.entity)),
                    LineageEdge::join_key(),
                );
            }
        }

        // Process filter expression
        if let Some(filter) = &table.filter {
            let filter_deps = extract_column_refs(filter, Some(primary_source));
            for dep in filter_deps {
                self.add_edge(
                    dep,
                    ColumnRef::new(target_entity, "_filter"),
                    LineageEdge::filter(),
                );
            }
        }

        // Process window columns
        for window_col in &table.window_columns {
            // Window functions depend on their arguments
            for arg in &window_col.args {
                let deps = extract_column_refs(arg, Some(primary_source));
                for dep in deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, &window_col.name),
                        LineageEdge::transform("window function"),
                    );
                }
            }

            // Partition by columns
            for part in &window_col.partition_by {
                let deps = extract_column_refs(part, Some(primary_source));
                for dep in deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, &window_col.name),
                        LineageEdge::window_partition(),
                    );
                }
            }

            // Order by columns
            for ord in &window_col.order_by {
                let deps = extract_column_refs(&ord.expr, Some(primary_source));
                for dep in deps {
                    self.add_edge(
                        dep,
                        ColumnRef::new(target_entity, &window_col.name),
                        LineageEdge::window_order(),
                    );
                }
            }
        }

        // Process group by columns
        for group_col in &table.group_by {
            for source_entity in &source_entities {
                self.add_edge(
                    ColumnRef::new(*source_entity, group_col),
                    ColumnRef::new(target_entity, group_col),
                    LineageEdge::group_by(),
                );
            }
        }
    }

    /// Process a report definition to extract lineage.
    ///
    /// Reports aggregate measures from multiple facts into a single output.
    /// Each measure reference creates a lineage edge from the fact's measure
    /// to the report's output column.
    fn process_report(&mut self, report: &crate::model::Report, model: &Model) {
        let target_entity = &report.name;

        // Process measure references - each measure comes from a fact
        for measure_ref in &report.measures {
            let fact_name = &measure_ref.fact;
            let measure_name = &measure_ref.measure;

            // Create lineage from fact measure to report column
            // The report column name is just the measure name (or fact.measure for disambiguation)
            self.add_edge(
                ColumnRef::new(fact_name, measure_name),
                ColumnRef::new(target_entity, measure_name),
                LineageEdge::aggregate(),
            );

            // If the fact exists, also trace back to the fact's source measure
            if let Some(fact) = model.facts.get(fact_name) {
                if let Some(measure) = fact.measures.get(measure_name) {
                    // Get the source entity for this fact
                    let source_entity = fact
                        .from
                        .as_ref()
                        .or_else(|| fact.grain.first().map(|g| &g.source_entity));

                    if let Some(source) = source_entity {
                        // Add lineage from source column to report measure
                        self.add_edge(
                            ColumnRef::new(source, &measure.source_column),
                            ColumnRef::new(target_entity, measure_name),
                            LineageEdge::aggregate(),
                        );
                    }
                }
            }
        }

        // Process group_by columns - these are dimensions the report groups on
        for group_col in &report.group_by {
            // Group by columns create nodes in the report
            self.add_edge(
                ColumnRef::new("_dimension", group_col),
                ColumnRef::new(target_entity, group_col),
                LineageEdge::group_by(),
            );
        }
    }

    /// Process a pivot report definition to extract lineage.
    ///
    /// Pivot reports are similar to reports but with pivot/crosstab functionality.
    fn process_pivot_report(&mut self, pivot_report: &crate::model::PivotReport, model: &Model) {
        let target_entity = &pivot_report.name;

        // Process values (measures)
        for value in &pivot_report.values {
            let fact_name = &value.measure.fact;
            let measure_name = &value.measure.measure;
            let value_name = &value.name;

            // Create lineage from fact measure to pivot report column
            self.add_edge(
                ColumnRef::new(fact_name, measure_name),
                ColumnRef::new(target_entity, value_name),
                LineageEdge::aggregate(),
            );

            // If the fact exists, also trace back to the fact's source measure
            if let Some(fact) = model.facts.get(fact_name) {
                if let Some(measure) = fact.measures.get(measure_name) {
                    let source_entity = fact
                        .from
                        .as_ref()
                        .or_else(|| fact.grain.first().map(|g| &g.source_entity));

                    if let Some(source) = source_entity {
                        self.add_edge(
                            ColumnRef::new(source, &measure.source_column),
                            ColumnRef::new(target_entity, value_name),
                            LineageEdge::aggregate(),
                        );
                    }
                }
            }
        }

        // Process row columns
        for row_col in &pivot_report.rows {
            self.add_edge(
                ColumnRef::new("_dimension", row_col),
                ColumnRef::new(target_entity, row_col),
                LineageEdge::group_by(),
            );
        }

        // Process pivot columns - get dimension from the PivotColumns enum
        let pivot_col_dim = pivot_report.columns.dimension();
        if !pivot_col_dim.is_empty() {
            self.add_edge(
                ColumnRef::new("_dimension", pivot_col_dim),
                ColumnRef::new(target_entity, pivot_col_dim),
                LineageEdge::group_by(),
            );
        }
    }

    // =========================================================================
    // Query Methods
    // =========================================================================

    /// Get all columns that this column directly depends on (one level upstream).
    pub fn direct_dependencies(&self, col: &ColumnRef) -> Vec<(ColumnRef, LineageType)> {
        let Some(&idx) = self.node_index.get(col) else {
            return vec![];
        };

        self.graph
            .neighbors_directed(idx, Direction::Incoming)
            .filter_map(|neighbor_idx| {
                let neighbor = self.graph.node_weight(neighbor_idx)?;
                let edge = self.graph.edges_connecting(neighbor_idx, idx).next()?;
                Some((neighbor.clone(), edge.weight().lineage_type.clone()))
            })
            .collect()
    }

    /// Get all columns that directly depend on this column (one level downstream).
    pub fn direct_dependents(&self, col: &ColumnRef) -> Vec<(ColumnRef, LineageType)> {
        let Some(&idx) = self.node_index.get(col) else {
            return vec![];
        };

        self.graph
            .neighbors_directed(idx, Direction::Outgoing)
            .filter_map(|neighbor_idx| {
                let neighbor = self.graph.node_weight(neighbor_idx)?;
                let edge = self.graph.edges_connecting(idx, neighbor_idx).next()?;
                Some((neighbor.clone(), edge.weight().lineage_type.clone()))
            })
            .collect()
    }

    /// Get all upstream dependencies (transitive closure).
    ///
    /// Returns all columns that this column depends on, directly or indirectly.
    pub fn all_upstream(&self, col: &ColumnRef) -> HashSet<ColumnRef> {
        let Some(&start_idx) = self.node_index.get(col) else {
            return HashSet::new();
        };

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(start_idx);

        while let Some(idx) = queue.pop_front() {
            for neighbor in self.graph.neighbors_directed(idx, Direction::Incoming) {
                if let Some(col_ref) = self.graph.node_weight(neighbor) {
                    if visited.insert(col_ref.clone()) {
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        visited
    }

    /// Get all downstream dependents (transitive closure).
    ///
    /// Returns all columns that depend on this column, directly or indirectly.
    pub fn all_downstream(&self, col: &ColumnRef) -> HashSet<ColumnRef> {
        let Some(&start_idx) = self.node_index.get(col) else {
            return HashSet::new();
        };

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(start_idx);

        while let Some(idx) = queue.pop_front() {
            for neighbor in self.graph.neighbors_directed(idx, Direction::Outgoing) {
                if let Some(col_ref) = self.graph.node_weight(neighbor) {
                    if visited.insert(col_ref.clone()) {
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        visited
    }

    /// Get all source columns (columns with no upstream dependencies).
    pub fn source_columns(&self) -> Vec<ColumnRef> {
        self.graph
            .node_indices()
            .filter(|&idx| {
                self.graph
                    .neighbors_directed(idx, Direction::Incoming)
                    .next()
                    .is_none()
            })
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect()
    }

    /// Get all terminal columns (columns with no downstream dependents).
    pub fn terminal_columns(&self) -> Vec<ColumnRef> {
        self.graph
            .node_indices()
            .filter(|&idx| {
                self.graph
                    .neighbors_directed(idx, Direction::Outgoing)
                    .next()
                    .is_none()
            })
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect()
    }

    /// Get all columns in a specific entity.
    pub fn columns_in_entity(&self, entity: &str) -> Vec<ColumnRef> {
        self.node_index
            .keys()
            .filter(|col| col.entity == entity)
            .cloned()
            .collect()
    }

    /// Get the total number of columns tracked.
    pub fn column_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Get the total number of lineage edges.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Find the minimal set of source columns needed to compute a target column.
    pub fn required_source_columns(&self, col: &ColumnRef) -> HashSet<ColumnRef> {
        self.all_upstream(col)
            .into_iter()
            .filter(|c| {
                // A column is a "source" column if it has no upstream deps
                self.direct_dependencies(c).is_empty()
            })
            .collect()
    }

    /// Find all columns that would be affected if a source column changes.
    pub fn impact_analysis(&self, source_col: &ColumnRef) -> HashSet<ColumnRef> {
        self.all_downstream(source_col)
    }

    // =========================================================================
    // Cycle Detection
    // =========================================================================

    /// Detect all cycles in the lineage graph.
    ///
    /// Returns a list of cycles, where each cycle is a list of column references
    /// forming the cycle (in order). Only strongly connected components with
    /// more than one node are considered cycles.
    ///
    /// # Example
    /// ```ignore
    /// let cycles = graph.detect_cycles();
    /// for cycle in cycles {
    ///     println!("Cycle: {}", cycle.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(" → "));
    /// }
    /// ```
    pub fn detect_cycles(&self) -> Vec<Vec<ColumnRef>> {
        // Use Tarjan's algorithm to find strongly connected components
        // (single DFS pass, more efficient than Kosaraju's)
        let sccs = tarjan_scc(&self.graph);

        sccs.into_iter()
            .filter(|scc| {
                // A single node is only a cycle if it has a self-loop
                if scc.len() == 1 {
                    let idx = scc[0];
                    self.graph
                        .edges_connecting(idx, idx)
                        .next()
                        .is_some()
                } else {
                    // Multiple nodes in an SCC form a cycle
                    true
                }
            })
            .map(|scc| {
                scc.into_iter()
                    .filter_map(|idx| self.graph.node_weight(idx).cloned())
                    .collect()
            })
            .collect()
    }

    /// Validate that the lineage graph has no cycles.
    ///
    /// Returns `Ok(())` if no cycles exist, or `Err` with a detailed error
    /// message listing the columns involved in each cycle.
    ///
    /// # Example
    /// ```ignore
    /// match graph.validate_no_cycles() {
    ///     Ok(()) => println!("No cycles detected"),
    ///     Err(e) => eprintln!("Cycle detected: {}", e),
    /// }
    /// ```
    pub fn validate_no_cycles(&self) -> Result<(), LineageCycleError> {
        let cycles = self.detect_cycles();
        if cycles.is_empty() {
            Ok(())
        } else {
            Err(LineageCycleError { cycles })
        }
    }

    /// Check if the graph contains any cycles.
    pub fn has_cycles(&self) -> bool {
        !self.detect_cycles().is_empty()
    }

    // =========================================================================
    // Serialization
    // =========================================================================

    /// Convert the graph to a serializable edge list format.
    ///
    /// This format is suitable for JSON serialization and SQLite storage.
    pub fn to_serializable(&self) -> SerializedLineage {
        let edges = self
            .graph
            .edge_references()
            .filter_map(|edge_ref| {
                let from = self.graph.node_weight(edge_ref.source())?.clone();
                let to = self.graph.node_weight(edge_ref.target())?.clone();
                Some(SerializedEdge {
                    from,
                    to,
                    edge: edge_ref.weight().clone(),
                })
            })
            .collect();
        SerializedLineage { edges }
    }

    /// Reconstruct a graph from a serialized edge list.
    pub fn from_serializable(serialized: SerializedLineage) -> Self {
        let mut graph = Self::new();
        for SerializedEdge { from, to, edge } in serialized.edges {
            graph.add_edge(from, to, edge);
        }
        graph
    }
}

impl Default for ColumnLineageGraph {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Expression Walking
// =============================================================================

/// Extract all unique column references from an expression.
///
/// If `default_entity` is provided, it's used for column references
/// that don't have an explicit entity qualifier.
///
/// Returns deduplicated results to prevent duplicate edges in lineage.
pub fn extract_column_refs(expr: &Expr, default_entity: Option<&str>) -> Vec<ColumnRef> {
    let mut refs = Vec::new();
    extract_column_refs_inner(expr, default_entity, &mut refs);
    // Deduplicate - a column referenced multiple times in an expression
    // should only create one lineage edge
    let mut seen = HashSet::new();
    refs.retain(|r| seen.insert(r.clone()));
    refs
}

fn extract_column_refs_inner(expr: &Expr, default_entity: Option<&str>, refs: &mut Vec<ColumnRef>) {
    match expr {
        Expr::Column { entity, column } => {
            // Only add column ref if we can determine the entity
            // Skip if no entity qualifier and no default - better than creating invalid "_unknown" refs
            if let Some(entity_name) = entity.as_ref().map(|s| s.as_str()).or(default_entity) {
                if !entity_name.is_empty() {
                    refs.push(ColumnRef::new(entity_name, column));
                }
            }
        }

        Expr::Literal(_) => {
            // Literals don't reference columns
        }

        Expr::Function { args, .. } => {
            for arg in args {
                extract_column_refs_inner(arg, default_entity, refs);
            }
        }

        Expr::BinaryOp { left, right, .. } => {
            extract_column_refs_inner(left, default_entity, refs);
            extract_column_refs_inner(right, default_entity, refs);
        }

        Expr::UnaryOp { expr, .. } => {
            extract_column_refs_inner(expr, default_entity, refs);
        }

        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                extract_column_refs_inner(op, default_entity, refs);
            }
            for WhenClause { condition, result } in when_clauses {
                extract_column_refs_inner(condition, default_entity, refs);
                extract_column_refs_inner(result, default_entity, refs);
            }
            if let Some(else_expr) = else_clause {
                extract_column_refs_inner(else_expr, default_entity, refs);
            }
        }

        Expr::Cast { expr, .. } => {
            extract_column_refs_inner(expr, default_entity, refs);
        }

        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                extract_column_refs_inner(arg, default_entity, refs);
            }
            for part in partition_by {
                extract_column_refs_inner(part, default_entity, refs);
            }
            for OrderByExpr { expr, .. } in order_by {
                extract_column_refs_inner(expr, default_entity, refs);
            }
        }

        Expr::FilteredAgg { agg, filter } => {
            extract_column_refs_inner(agg, default_entity, refs);
            extract_column_refs_inner(filter, default_entity, refs);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::expr::{BinaryOp, Func};

    #[test]
    fn test_column_ref_parse() {
        let col = ColumnRef::parse("orders.order_id").unwrap();
        assert_eq!(col.entity, "orders");
        assert_eq!(col.column, "order_id");

        assert!(ColumnRef::parse("invalid").is_none());
    }

    #[test]
    fn test_column_ref_display() {
        let col = ColumnRef::new("orders", "order_id");
        assert_eq!(col.to_string(), "orders.order_id");
    }

    #[test]
    fn test_extract_column_refs_simple() {
        let expr = Expr::Column {
            entity: Some("orders".into()),
            column: "order_id".into(),
        };
        let refs = extract_column_refs(&expr, None);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], ColumnRef::new("orders", "order_id"));
    }

    #[test]
    fn test_extract_column_refs_with_default_entity() {
        let expr = Expr::Column {
            entity: None,
            column: "order_id".into(),
        };
        let refs = extract_column_refs(&expr, Some("orders"));
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], ColumnRef::new("orders", "order_id"));
    }

    #[test]
    fn test_extract_column_refs_binary_op() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                entity: None,
                column: "quantity".into(),
            }),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Column {
                entity: None,
                column: "price".into(),
            }),
        };
        let refs = extract_column_refs(&expr, Some("order_items"));
        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&ColumnRef::new("order_items", "quantity")));
        assert!(refs.contains(&ColumnRef::new("order_items", "price")));
    }

    #[test]
    fn test_extract_column_refs_function() {
        let expr = Expr::Function {
            func: Func::Sum,
            args: vec![Expr::Column {
                entity: None,
                column: "amount".into(),
            }],
        };
        let refs = extract_column_refs(&expr, Some("sales"));
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], ColumnRef::new("sales", "amount"));
    }

    #[test]
    fn test_lineage_graph_basic() {
        let mut graph = ColumnLineageGraph::new();

        // orders.order_id -> fact_orders.order_id (passthrough)
        graph.add_edge(
            ColumnRef::new("orders", "order_id"),
            ColumnRef::new("fact_orders", "order_id"),
            LineageEdge::passthrough(),
        );

        // orders.amount -> fact_orders.total_revenue (aggregate)
        graph.add_edge(
            ColumnRef::new("orders", "amount"),
            ColumnRef::new("fact_orders", "total_revenue"),
            LineageEdge::aggregate(),
        );

        assert_eq!(graph.column_count(), 4);
        assert_eq!(graph.edge_count(), 2);

        // Check dependencies
        let deps = graph.direct_dependencies(&ColumnRef::new("fact_orders", "total_revenue"));
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].0, ColumnRef::new("orders", "amount"));
        assert_eq!(deps[0].1, LineageType::Aggregate);
    }

    #[test]
    fn test_lineage_transitive() {
        let mut graph = ColumnLineageGraph::new();

        // source.a -> intermediate.b -> fact.c
        graph.add_edge(
            ColumnRef::new("source", "a"),
            ColumnRef::new("intermediate", "b"),
            LineageEdge::transform("step1"),
        );
        graph.add_edge(
            ColumnRef::new("intermediate", "b"),
            ColumnRef::new("fact", "c"),
            LineageEdge::transform("step2"),
        );

        // All upstream from fact.c should include both source.a and intermediate.b
        let upstream = graph.all_upstream(&ColumnRef::new("fact", "c"));
        assert_eq!(upstream.len(), 2);
        assert!(upstream.contains(&ColumnRef::new("source", "a")));
        assert!(upstream.contains(&ColumnRef::new("intermediate", "b")));

        // All downstream from source.a should include intermediate.b and fact.c
        let downstream = graph.all_downstream(&ColumnRef::new("source", "a"));
        assert_eq!(downstream.len(), 2);
        assert!(downstream.contains(&ColumnRef::new("intermediate", "b")));
        assert!(downstream.contains(&ColumnRef::new("fact", "c")));
    }

    #[test]
    fn test_required_source_columns() {
        let mut graph = ColumnLineageGraph::new();

        // source.x, source.y -> intermediate.z -> fact.result
        graph.add_edge(
            ColumnRef::new("source", "x"),
            ColumnRef::new("intermediate", "z"),
            LineageEdge::transform("x + y"),
        );
        graph.add_edge(
            ColumnRef::new("source", "y"),
            ColumnRef::new("intermediate", "z"),
            LineageEdge::transform("x + y"),
        );
        graph.add_edge(
            ColumnRef::new("intermediate", "z"),
            ColumnRef::new("fact", "result"),
            LineageEdge::passthrough(),
        );

        let required = graph.required_source_columns(&ColumnRef::new("fact", "result"));
        assert_eq!(required.len(), 2);
        assert!(required.contains(&ColumnRef::new("source", "x")));
        assert!(required.contains(&ColumnRef::new("source", "y")));
    }

    #[test]
    fn test_impact_analysis() {
        let mut graph = ColumnLineageGraph::new();

        // source.a affects multiple downstream columns
        graph.add_edge(
            ColumnRef::new("source", "a"),
            ColumnRef::new("fact1", "x"),
            LineageEdge::passthrough(),
        );
        graph.add_edge(
            ColumnRef::new("source", "a"),
            ColumnRef::new("fact2", "y"),
            LineageEdge::transform("..."),
        );

        let impact = graph.impact_analysis(&ColumnRef::new("source", "a"));
        assert_eq!(impact.len(), 2);
        assert!(impact.contains(&ColumnRef::new("fact1", "x")));
        assert!(impact.contains(&ColumnRef::new("fact2", "y")));
    }

    #[test]
    fn test_source_and_terminal_columns() {
        let mut graph = ColumnLineageGraph::new();

        graph.add_edge(
            ColumnRef::new("source", "a"),
            ColumnRef::new("fact", "x"),
            LineageEdge::passthrough(),
        );
        graph.add_edge(
            ColumnRef::new("source", "b"),
            ColumnRef::new("fact", "y"),
            LineageEdge::passthrough(),
        );

        let sources = graph.source_columns();
        assert_eq!(sources.len(), 2);

        let terminals = graph.terminal_columns();
        assert_eq!(terminals.len(), 2);
    }

    #[test]
    fn test_scd_type2_lineage() {
        use crate::model::dimension::{DimensionColumn, DimensionDefinition, SCDType};
        use crate::model::types::MaterializationStrategy;

        let dim = DimensionDefinition {
            name: "dim_customers".into(),
            target_table: "dim_customers".into(),
            target_schema: None,
            materialized: true,
            source_entity: "customers".into(),
            columns: vec![
                DimensionColumn {
                    source_column: "name".into(),
                    target_column: None,
                    description: None,
                },
                DimensionColumn {
                    source_column: "region".into(),
                    target_column: None,
                    description: None,
                },
            ],
            primary_key: vec!["customer_id".into()],
            scd_type: SCDType::Type2 {
                effective_from: "valid_from".into(),
                effective_to: "valid_to".into(),
                is_current: Some("is_current".into()),
            },
            materialization: MaterializationStrategy::Table,
        };

        let mut model = Model::new();
        model.dimensions.insert(dim.name.clone(), dim);

        let graph = ColumnLineageGraph::from_model(&model);

        // Check that SCD columns depend on source columns
        let valid_from_deps = graph.direct_dependencies(&ColumnRef::new("dim_customers", "valid_from"));
        assert!(!valid_from_deps.is_empty());
        assert!(valid_from_deps.iter().any(|(c, _)| c.entity == "customers" && c.column == "name"));
        assert!(valid_from_deps.iter().any(|(c, _)| c.entity == "customers" && c.column == "region"));

        // Check that is_current also has dependencies
        let is_current_deps = graph.direct_dependencies(&ColumnRef::new("dim_customers", "is_current"));
        assert!(!is_current_deps.is_empty());
    }

    #[test]
    fn test_scd_type3_lineage() {
        use crate::model::dimension::{DimensionColumn, DimensionDefinition, SCDType};
        use crate::model::types::MaterializationStrategy;

        let dim = DimensionDefinition {
            name: "dim_products".into(),
            target_table: "dim_products".into(),
            target_schema: None,
            materialized: true,
            source_entity: "products".into(),
            columns: vec![DimensionColumn {
                source_column: "price".into(),
                target_column: None,
                description: None,
            }],
            primary_key: vec!["product_id".into()],
            scd_type: SCDType::Type3 {
                tracked_columns: vec![("price".into(), "previous_price".into())],
            },
            materialization: MaterializationStrategy::Table,
        };

        let mut model = Model::new();
        model.dimensions.insert(dim.name.clone(), dim);

        let graph = ColumnLineageGraph::from_model(&model);

        // Check that previous_price depends on price
        let prev_price_deps = graph.direct_dependencies(&ColumnRef::new("dim_products", "previous_price"));
        assert_eq!(prev_price_deps.len(), 1);
        assert_eq!(prev_price_deps[0].0, ColumnRef::new("products", "price"));
    }

    #[test]
    fn test_empty_model() {
        let model = Model::new();
        let graph = ColumnLineageGraph::from_model(&model);
        assert_eq!(graph.column_count(), 0);
        assert_eq!(graph.edge_count(), 0);
        assert!(graph.source_columns().is_empty());
        assert!(graph.terminal_columns().is_empty());
    }

    #[test]
    fn test_duplicate_edges_prevented() {
        let mut graph = ColumnLineageGraph::new();
        let from = ColumnRef::new("source", "col");
        let to = ColumnRef::new("target", "col");

        // Add same edge multiple times
        graph.add_edge(from.clone(), to.clone(), LineageEdge::passthrough());
        graph.add_edge(from.clone(), to.clone(), LineageEdge::passthrough());
        graph.add_edge(from.clone(), to.clone(), LineageEdge::passthrough());

        // Should only have 1 edge
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_duplicate_edges_different_types_allowed() {
        let mut graph = ColumnLineageGraph::new();
        let from = ColumnRef::new("source", "col");
        let to = ColumnRef::new("target", "col");

        // Add edges with different lineage types
        graph.add_edge(from.clone(), to.clone(), LineageEdge::passthrough());
        graph.add_edge(from.clone(), to.clone(), LineageEdge::filter());

        // Should have 2 edges (different types are allowed)
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_column_ref_parse_edge_cases() {
        assert!(ColumnRef::parse("").is_none());
        assert!(ColumnRef::parse(".").is_none());
        assert!(ColumnRef::parse(".column").is_none());
        assert!(ColumnRef::parse("entity.").is_none());
        assert!(ColumnRef::parse("no_dot").is_none());
        assert!(ColumnRef::parse("entity.column").is_some());
        // Column with dot in name works (splitn(2, '.'))
        assert_eq!(
            ColumnRef::parse("entity.schema.column"),
            Some(ColumnRef::new("entity", "schema.column"))
        );
    }

    #[test]
    fn test_cyclic_lineage_no_infinite_loop() {
        let mut graph = ColumnLineageGraph::new();

        // Create cycle: a -> b -> c -> a
        graph.add_edge(
            ColumnRef::new("e", "a"),
            ColumnRef::new("e", "b"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e", "b"),
            ColumnRef::new("e", "c"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e", "c"),
            ColumnRef::new("e", "a"),
            LineageEdge::transform(""),
        );

        // Should not hang - all nodes reachable from any starting point
        let upstream = graph.all_upstream(&ColumnRef::new("e", "a"));
        assert_eq!(upstream.len(), 3);

        let downstream = graph.all_downstream(&ColumnRef::new("e", "a"));
        assert_eq!(downstream.len(), 3);
    }

    #[test]
    fn test_detect_cycles_simple() {
        let mut graph = ColumnLineageGraph::new();

        // Create cycle: a -> b -> c -> a
        graph.add_edge(
            ColumnRef::new("e", "a"),
            ColumnRef::new("e", "b"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e", "b"),
            ColumnRef::new("e", "c"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e", "c"),
            ColumnRef::new("e", "a"),
            LineageEdge::transform(""),
        );

        let cycles = graph.detect_cycles();
        assert_eq!(cycles.len(), 1, "Should detect exactly one cycle");
        assert_eq!(cycles[0].len(), 3, "Cycle should contain 3 columns");

        // Verify has_cycles works
        assert!(graph.has_cycles());

        // Verify validate_no_cycles returns an error
        let result = graph.validate_no_cycles();
        assert!(result.is_err());

        let err = result.unwrap_err();
        let error_msg = err.to_string();
        assert!(
            error_msg.contains("Circular dependencies"),
            "Error should mention circular dependencies: {}",
            error_msg
        );
    }

    #[test]
    fn test_detect_cycles_none() {
        let mut graph = ColumnLineageGraph::new();

        // Create acyclic graph: a -> b -> c
        graph.add_edge(
            ColumnRef::new("e", "a"),
            ColumnRef::new("e", "b"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e", "b"),
            ColumnRef::new("e", "c"),
            LineageEdge::transform(""),
        );

        let cycles = graph.detect_cycles();
        assert!(cycles.is_empty(), "Should detect no cycles in acyclic graph");

        assert!(!graph.has_cycles());
        assert!(graph.validate_no_cycles().is_ok());
    }

    #[test]
    fn test_detect_cycles_self_loop() {
        let mut graph = ColumnLineageGraph::new();

        // Create self-loop: a -> a
        graph.add_edge(
            ColumnRef::new("e", "a"),
            ColumnRef::new("e", "a"),
            LineageEdge::transform("recursive"),
        );

        let cycles = graph.detect_cycles();
        assert_eq!(cycles.len(), 1, "Should detect self-loop as cycle");
        assert_eq!(cycles[0].len(), 1, "Self-loop cycle contains 1 column");

        assert!(graph.has_cycles());
    }

    #[test]
    fn test_detect_cycles_multiple() {
        let mut graph = ColumnLineageGraph::new();

        // Create two separate cycles
        // Cycle 1: a -> b -> a
        graph.add_edge(
            ColumnRef::new("e1", "a"),
            ColumnRef::new("e1", "b"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e1", "b"),
            ColumnRef::new("e1", "a"),
            LineageEdge::transform(""),
        );

        // Cycle 2: x -> y -> z -> x
        graph.add_edge(
            ColumnRef::new("e2", "x"),
            ColumnRef::new("e2", "y"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e2", "y"),
            ColumnRef::new("e2", "z"),
            LineageEdge::transform(""),
        );
        graph.add_edge(
            ColumnRef::new("e2", "z"),
            ColumnRef::new("e2", "x"),
            LineageEdge::transform(""),
        );

        let cycles = graph.detect_cycles();
        assert_eq!(cycles.len(), 2, "Should detect both cycles");

        let result = graph.validate_no_cycles();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.cycles.len(), 2);
    }

    #[test]
    fn test_cycle_error_display() {
        let error = LineageCycleError {
            cycles: vec![vec![
                ColumnRef::new("fact", "amount"),
                ColumnRef::new("fact", "total"),
                ColumnRef::new("fact", "amount"),
            ]],
        };

        let display = error.to_string();
        assert!(display.contains("Circular dependencies"));
        assert!(display.contains("fact.amount"));
        assert!(display.contains("fact.total"));
        assert!(display.contains("Cycle 1:"));
    }

    #[test]
    fn test_extract_column_refs_deduplicates() {
        use crate::model::expr::{BinaryOp, Expr};

        // Expression: a + a + a (same column referenced 3 times)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    entity: Some("t".into()),
                    column: "a".into(),
                }),
                op: BinaryOp::Add,
                right: Box::new(Expr::Column {
                    entity: Some("t".into()),
                    column: "a".into(),
                }),
            }),
            op: BinaryOp::Add,
            right: Box::new(Expr::Column {
                entity: Some("t".into()),
                column: "a".into(),
            }),
        };

        let refs = extract_column_refs(&expr, None);
        // Should be deduplicated to just one reference
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], ColumnRef::new("t", "a"));
    }

    #[test]
    fn test_include_all_column_resolution() {
        use crate::model::{
            fact::{ColumnSelection, DimensionInclude, FactDefinition, GrainColumn},
            DataType, SourceColumn, SourceEntity,
        };

        let mut model = Model::new();

        // Create a source entity with multiple columns
        let mut customers_source = SourceEntity::new("customers", "raw.customers");
        customers_source.columns.insert(
            "id".into(),
            SourceColumn::new("id", DataType::Int64, false),
        );
        customers_source.columns.insert(
            "name".into(),
            SourceColumn::new("name", DataType::String, true),
        );
        customers_source.columns.insert(
            "email".into(),
            SourceColumn::new("email", DataType::String, true),
        );
        customers_source.columns.insert(
            "region".into(),
            SourceColumn::new("region", DataType::String, true),
        );
        model.sources.insert("customers".into(), customers_source);

        // Create orders source for the fact
        let mut orders_source = SourceEntity::new("orders", "raw.orders");
        orders_source.columns.insert(
            "order_id".into(),
            SourceColumn::new("order_id", DataType::Int64, false),
        );
        orders_source.columns.insert(
            "customer_id".into(),
            SourceColumn::new("customer_id", DataType::Int64, false),
        );
        model.sources.insert("orders".into(), orders_source);

        // Create a fact that includes ALL columns from customers
        let mut fact = FactDefinition::new("order_fact", "analytics.order_fact");
        fact.from = Some("orders".into());
        fact.grain = vec![GrainColumn {
            source_entity: "orders".into(),
            source_column: "order_id".into(),
            target_name: None,
        }];
        fact.includes.insert(
            "cust".into(),
            DimensionInclude {
                entity: "customers".into(),
                selection: ColumnSelection::All,
                prefix: Some("cust".into()),
            },
        );
        model.facts.insert("order_fact".into(), fact);

        // Build lineage
        let graph = ColumnLineageGraph::from_model(&model);

        // The fact should have lineage edges from customers.* to order_fact.cust_*
        let fact_columns = graph.columns_in_entity("order_fact");

        // Should include cust_id, cust_name, cust_email, cust_region
        let cust_columns: Vec<_> = fact_columns
            .iter()
            .filter(|c| c.column.starts_with("cust_"))
            .collect();

        assert_eq!(
            cust_columns.len(),
            4,
            "Should have 4 customer columns included via All: {:?}",
            cust_columns
        );

        // Verify one of the dependencies
        let cust_name_deps = graph.direct_dependencies(&ColumnRef::new("order_fact", "cust_name"));
        assert!(!cust_name_deps.is_empty(), "cust_name should have dependencies");
    }

    #[test]
    fn test_include_except_column_resolution() {
        use crate::model::{
            fact::{ColumnSelection, DimensionInclude, FactDefinition, GrainColumn},
            DataType, SourceColumn, SourceEntity,
        };

        let mut model = Model::new();

        // Create a source entity with multiple columns
        let mut customers_source = SourceEntity::new("customers", "raw.customers");
        customers_source.columns.insert(
            "id".into(),
            SourceColumn::new("id", DataType::Int64, false),
        );
        customers_source.columns.insert(
            "name".into(),
            SourceColumn::new("name", DataType::String, true),
        );
        customers_source.columns.insert(
            "email".into(),
            SourceColumn::new("email", DataType::String, true),
        );
        customers_source.columns.insert(
            "internal_notes".into(),
            SourceColumn::new("internal_notes", DataType::String, true),
        );
        model.sources.insert("customers".into(), customers_source);

        // Create orders source for the fact
        let mut orders_source = SourceEntity::new("orders", "raw.orders");
        orders_source.columns.insert(
            "order_id".into(),
            SourceColumn::new("order_id", DataType::Int64, false),
        );
        model.sources.insert("orders".into(), orders_source);

        // Create a fact that includes ALL EXCEPT internal_notes from customers
        let mut fact = FactDefinition::new("order_fact", "analytics.order_fact");
        fact.from = Some("orders".into());
        fact.grain = vec![GrainColumn {
            source_entity: "orders".into(),
            source_column: "order_id".into(),
            target_name: None,
        }];
        fact.includes.insert(
            "cust".into(),
            DimensionInclude {
                entity: "customers".into(),
                selection: ColumnSelection::Except(vec!["internal_notes".into()]),
                prefix: Some("cust".into()),
            },
        );
        model.facts.insert("order_fact".into(), fact);

        // Build lineage
        let graph = ColumnLineageGraph::from_model(&model);

        // The fact should have lineage edges for id, name, email but NOT internal_notes
        let fact_columns = graph.columns_in_entity("order_fact");

        let cust_columns: Vec<_> = fact_columns
            .iter()
            .filter(|c| c.column.starts_with("cust_"))
            .collect();

        // Should have 3 columns (id, name, email) - not internal_notes
        assert_eq!(
            cust_columns.len(),
            3,
            "Should have 3 customer columns (excluding internal_notes): {:?}",
            cust_columns
        );

        // Verify internal_notes is NOT included
        assert!(
            !cust_columns.iter().any(|c| c.column.contains("internal_notes")),
            "internal_notes should be excluded"
        );
    }

    #[test]
    fn test_serialization_round_trip() {
        let mut graph = ColumnLineageGraph::new();

        // Build a graph with various edge types
        graph.add_edge(
            ColumnRef::new("source", "a"),
            ColumnRef::new("intermediate", "b"),
            LineageEdge::passthrough(),
        );
        graph.add_edge(
            ColumnRef::new("source", "x"),
            ColumnRef::new("intermediate", "b"),
            LineageEdge::transform("a + x"),
        );
        graph.add_edge(
            ColumnRef::new("intermediate", "b"),
            ColumnRef::new("fact", "c"),
            LineageEdge::aggregate(),
        );
        graph.add_edge(
            ColumnRef::new("source", "filter_col"),
            ColumnRef::new("fact", "_filter"),
            LineageEdge::filter(),
        );

        // Serialize
        let serialized = graph.to_serializable();
        assert_eq!(serialized.edges.len(), 4);

        // Deserialize
        let restored = ColumnLineageGraph::from_serializable(serialized.clone());

        // Verify same structure
        assert_eq!(restored.column_count(), graph.column_count());
        assert_eq!(restored.edge_count(), graph.edge_count());

        // Verify dependencies preserved
        let deps = restored.direct_dependencies(&ColumnRef::new("intermediate", "b"));
        assert_eq!(deps.len(), 2);

        // Verify JSON serialization works
        let json = serde_json::to_string(&serialized).expect("JSON serialize should work");
        let from_json: SerializedLineage =
            serde_json::from_str(&json).expect("JSON deserialize should work");
        assert_eq!(from_json.edges.len(), 4);
    }
}
