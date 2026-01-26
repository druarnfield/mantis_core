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
