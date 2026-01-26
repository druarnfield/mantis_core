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
