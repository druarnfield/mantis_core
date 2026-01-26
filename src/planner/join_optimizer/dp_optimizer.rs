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

    /// Main DP optimization entry point.
    /// Returns the optimal join plan for the given tables and filters.
    pub fn optimize(&mut self, tables: Vec<String>, filters: Vec<Expr>) -> Option<LogicalPlan> {
        // 1. Build join graph from UnifiedGraph
        self.join_graph = Some(JoinGraph::build(self.graph, &tables));

        // 2. Classify filters by table dependencies
        self.filters = self.classify_filters(filters);

        // 3. Base case: single-table plans with applicable filters
        for table in &tables {
            let plan = self.build_base_plan(table);
            self.memo.insert(TableSet::single(table), plan);
        }

        // 4. DP: build optimal plans for increasing subset sizes
        for size in 2..=tables.len() {
            let subsets = generate_subsets(&tables, size);
            for subset in subsets {
                self.find_best_plan_for_subset(&subset);
            }
        }

        // 5. Return optimal plan for all tables
        let all_tables = TableSet::from_vec(tables);
        self.memo.get(&all_tables).map(|p| p.plan.clone())
    }

    /// Find the best join plan for a subset by trying all partitions.
    fn find_best_plan_for_subset(&mut self, subset: &TableSet) {
        let mut best_plan: Option<SubsetPlan> = None;
        let mut best_cost = f64::MAX;

        // Try all ways to partition subset into two joinable parts
        for (s1, s2) in enumerate_splits(subset) {
            // Skip if tables can't be joined
            let join_graph = self.join_graph.as_ref().unwrap();
            if !join_graph.are_sets_joinable(&s1.to_vec(), &s2.to_vec()) {
                continue;
            }

            // Get subplans from memo
            let left = match self.memo.get(&s1) {
                Some(plan) => plan,
                None => continue,
            };
            let right = match self.memo.get(&s2) {
                Some(plan) => plan,
                None => continue,
            };

            // Try both join orders: left ⋈ right and right ⋈ left
            for (l, r) in [(left, right), (right, left)] {
                let candidate = self.build_join_plan(l, r, subset);
                let cost = candidate.cost.total();

                if cost < best_cost {
                    best_cost = cost;
                    best_plan = Some(candidate);
                }
            }
        }

        if let Some(plan) = best_plan {
            self.memo.insert(subset.clone(), plan);
        }
    }

    /// Build a join plan between two subplans with cardinality estimation.
    fn build_join_plan(
        &self,
        left: &SubsetPlan,
        right: &SubsetPlan,
        _subset: &TableSet,
    ) -> SubsetPlan {
        use crate::planner::logical::{JoinCondition, JoinNode, JoinType};
        use crate::semantic::graph::query::ColumnRef;

        let join_graph = self.join_graph.as_ref().unwrap();

        // Get join edge between left and right table sets
        let left_tables = self.extract_tables_from_plan(&left.plan);
        let right_tables = self.extract_tables_from_plan(&right.plan);

        let (t1, t2, join_edge) = join_graph
            .get_join_edge_between_sets(&left_tables, &right_tables)
            .expect("Tables should be joinable");

        // Estimate output cardinality
        let estimated_rows = self.cardinality_estimator.estimate_join_output(
            left.estimated_rows,
            right.estimated_rows,
            join_edge,
        );

        // Build join condition from join_columns
        let join_pairs: Vec<(ColumnRef, ColumnRef)> = join_edge
            .join_columns
            .iter()
            .map(|(left_col, right_col)| {
                (
                    ColumnRef {
                        entity: t1.to_string(),
                        column: left_col.clone(),
                    },
                    ColumnRef {
                        entity: t2.to_string(),
                        column: right_col.clone(),
                    },
                )
            })
            .collect();

        // Build join plan
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left.plan.clone()),
            right: Box::new(right.plan.clone()),
            on: JoinCondition::Equi(join_pairs),
            join_type: JoinType::Inner,
            cardinality: Some(join_edge.cardinality),
        });

        // Estimate cost: left cost + right cost + join cost
        let join_cost = estimated_rows as f64;
        let cost = CostEstimate {
            rows_out: estimated_rows,
            cpu_cost: left.cost.cpu_cost + right.cost.cpu_cost + join_cost,
            io_cost: left.cost.io_cost + right.cost.io_cost + (join_cost * 0.1),
            memory_cost: left
                .cost
                .memory_cost
                .max(right.cost.memory_cost)
                .max(estimated_rows as f64),
        };

        SubsetPlan {
            plan,
            estimated_rows,
            cost,
        }
    }

    /// Extract table names from a logical plan.
    fn extract_tables_from_plan(&self, plan: &LogicalPlan) -> Vec<String> {
        let mut tables = Vec::new();
        self.collect_tables_from_plan(plan, &mut tables);
        tables
    }

    /// Recursively collect table names from a plan.
    fn collect_tables_from_plan(&self, plan: &LogicalPlan, tables: &mut Vec<String>) {
        match plan {
            LogicalPlan::Scan(scan) => {
                tables.push(scan.entity.clone());
            }
            LogicalPlan::Join(join) => {
                self.collect_tables_from_plan(&join.left, tables);
                self.collect_tables_from_plan(&join.right, tables);
            }
            LogicalPlan::Filter(filter) => {
                self.collect_tables_from_plan(&filter.input, tables);
            }
            _ => {}
        }
    }

    /// Helper for tests: check if memo contains a single table.
    pub fn memo_contains(&self, table: &str) -> bool {
        self.memo.contains_key(&TableSet::single(table))
    }

    /// Helper for tests: check if memo contains a table set.
    pub fn memo_contains_set(&self, table_set: &TableSet) -> bool {
        self.memo.contains_key(table_set)
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
