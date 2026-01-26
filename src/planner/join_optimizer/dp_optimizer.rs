// src/planner/join_optimizer/dp_optimizer.rs
use std::collections::BTreeSet;

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
