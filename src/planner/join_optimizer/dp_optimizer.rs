// src/planner/join_optimizer/dp_optimizer.rs
use crate::model::expr::Expr;
use crate::planner::cost::CostEstimate;
use crate::planner::join_optimizer::cardinality::CardinalityEstimator;
use crate::planner::join_optimizer::join_graph::JoinGraph;
use crate::planner::logical::{FilterNode, LogicalPlan, ScanNode};
use crate::semantic::graph::UnifiedGraph;
use std::collections::{BTreeSet, HashMap, HashSet};

/// A set of tables represented as a BTreeSet for deterministic ordering.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct TableSet {
    tables: BTreeSet<String>,
}

impl TableSet {
    pub fn new(tables: BTreeSet<String>) -> Self {
        Self { tables }
    }

    pub fn from_vec(tables: Vec<String>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }

    pub fn single(table: &str) -> Self {
        let mut tables = BTreeSet::new();
        tables.insert(table.to_string());
        Self { tables }
    }

    pub fn size(&self) -> usize {
        self.tables.len()
    }

    pub fn contains(&self, table: &str) -> bool {
        self.tables.contains(table)
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.tables.iter()
    }

    pub fn to_vec(&self) -> Vec<String> {
        self.tables.iter().cloned().collect()
    }
}

/// Generate all subsets of given size from a list of tables.
pub fn generate_subsets(tables: &[String], size: usize) -> Vec<TableSet> {
    if size == 0 || size > tables.len() {
        return vec![];
    }

    let mut result = Vec::new();
    let mut current = Vec::new();
    generate_subsets_helper(tables, size, 0, &mut current, &mut result);
    result
}

fn generate_subsets_helper(
    tables: &[String],
    size: usize,
    start: usize,
    current: &mut Vec<String>,
    result: &mut Vec<TableSet>,
) {
    if current.len() == size {
        result.push(TableSet::from_vec(current.clone()));
        return;
    }

    for i in start..tables.len() {
        current.push(tables[i].clone());
        generate_subsets_helper(tables, size, i + 1, current, result);
        current.pop();
    }
}

/// Enumerate all ways to split a table set into two non-empty subsets.
/// Returns pairs (S1, S2) where S1 ∪ S2 = subset.
pub fn enumerate_splits(subset: &TableSet) -> Vec<(TableSet, TableSet)> {
    let tables: Vec<_> = subset.iter().cloned().collect();
    let n = tables.len();

    if n < 2 {
        return vec![];
    }

    let mut splits = Vec::new();

    // Try all non-empty, non-full subsets as S1
    // Only iterate up to (n-1) to avoid the full set
    for size in 1..n {
        for s1_subset in generate_subsets(&tables, size) {
            // S2 is the complement of S1
            let s2_tables: Vec<_> = tables
                .iter()
                .filter(|t| !s1_subset.contains(t))
                .cloned()
                .collect();
            let s2_subset = TableSet::from_vec(s2_tables);

            // Only add if s1 is smaller, or if equal size, s1 is lexicographically smaller
            // This avoids duplicates like (A,B) and (B,A)
            if s1_subset.size() < s2_subset.size() {
                splits.push((s1_subset, s2_subset));
            } else if s1_subset.size() == s2_subset.size() {
                // Compare lexicographically
                let s1_vec = s1_subset.to_vec();
                let s2_vec = s2_subset.to_vec();
                if s1_vec < s2_vec {
                    splits.push((s1_subset, s2_subset));
                }
            }
        }
    }

    splits
}

/// Dynamic Programming Join Optimizer.
pub struct DPOptimizer<'a> {
    graph: &'a UnifiedGraph,
    join_graph: Option<JoinGraph>,
    pub filters: Vec<ClassifiedFilter>,
    cardinality_estimator: CardinalityEstimator<'a>,
    memo: HashMap<TableSet, SubsetPlan>,
}

/// A filter classified by its table dependencies.
pub struct ClassifiedFilter {
    pub expr: Expr,
    pub referenced_tables: HashSet<String>,
    pub selectivity: f64,
}

pub struct SubsetPlan {
    pub plan: LogicalPlan,
    pub estimated_rows: usize,
    pub cost: CostEstimate,
}

impl<'a> DPOptimizer<'a> {
    pub fn new(graph: &'a UnifiedGraph) -> Self {
        let cardinality_estimator = CardinalityEstimator::new(graph);

        Self {
            graph,
            join_graph: None,
            filters: Vec::new(),
            cardinality_estimator,
            memo: HashMap::new(),
        }
    }

    pub fn classify_filters(&mut self, filters: Vec<Expr>) -> Vec<ClassifiedFilter> {
        filters
            .into_iter()
            .map(|expr| {
                let tables = self.extract_referenced_tables(&expr);
                let selectivity = self
                    .cardinality_estimator
                    .estimate_filter_selectivity(&expr);

                ClassifiedFilter {
                    expr,
                    referenced_tables: tables,
                    selectivity,
                }
            })
            .collect()
    }

    fn extract_referenced_tables(&self, expr: &Expr) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables_from_expr(expr, &mut tables);
        tables
    }

    fn collect_tables_from_expr(&self, expr: &Expr, tables: &mut HashSet<String>) {
        match expr {
            Expr::Column {
                entity: Some(table),
                ..
            } => {
                tables.insert(table.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_tables_from_expr(left, tables);
                self.collect_tables_from_expr(right, tables);
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_tables_from_expr(expr, tables);
            }
            Expr::Case {
                conditions,
                else_expr,
            } => {
                for (cond, val) in conditions {
                    self.collect_tables_from_expr(cond, tables);
                    self.collect_tables_from_expr(val, tables);
                }
                if let Some(e) = else_expr {
                    self.collect_tables_from_expr(e, tables);
                }
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    self.collect_tables_from_expr(arg, tables);
                }
            }
            _ => {}
        }
    }

    pub fn build_base_plan(&self, table: &str) -> SubsetPlan {
        let mut plan = LogicalPlan::Scan(ScanNode {
            entity: table.to_string(),
        });

        // Get table row count from graph
        let mut estimated_rows = self
            .graph
            .entity_index(table)
            .and_then(|idx| {
                if let Some(node) = self.graph.graph().node_weight(idx) {
                    match node {
                        crate::semantic::graph::GraphNode::Entity(entity) => entity.row_count,
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .unwrap_or(1000); // Default

        // Find filters that ONLY reference this table
        let applicable_filters: Vec<_> = self
            .filters
            .iter()
            .filter(|f| f.referenced_tables.len() == 1 && f.referenced_tables.contains(table))
            .collect();

        // Apply filters and reduce cardinality
        if !applicable_filters.is_empty() {
            let predicates: Vec<_> = applicable_filters.iter().map(|f| f.expr.clone()).collect();

            plan = LogicalPlan::Filter(FilterNode {
                input: Box::new(plan),
                predicates,
            });

            // Reduce estimated rows by filter selectivity
            for filter in &applicable_filters {
                estimated_rows = (estimated_rows as f64 * filter.selectivity) as usize;
            }
        }

        // Estimate cost (simple for now - just row count)
        let cost = CostEstimate {
            rows_out: estimated_rows,
            cpu_cost: estimated_rows as f64,
            io_cost: estimated_rows as f64,
            memory_cost: 0.0,
        };

        SubsetPlan {
            plan,
            estimated_rows,
            cost,
        }
    }
}
