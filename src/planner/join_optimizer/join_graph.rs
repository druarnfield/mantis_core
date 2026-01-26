// src/planner/join_optimizer/join_graph.rs
use crate::semantic::graph::UnifiedGraph;
use std::collections::{HashMap, HashSet};

pub struct JoinGraph {
    tables: HashSet<String>,
    edges: HashMap<TablePair, JoinEdge>,
}

#[derive(Hash, Eq, PartialEq)]
struct TablePair(String, String);

pub struct JoinEdge {
    // Placeholder for now
}

impl JoinGraph {
    pub fn build(graph: &UnifiedGraph, tables: &[String]) -> Self {
        Self {
            tables: tables.iter().cloned().collect(),
            edges: HashMap::new(),
        }
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}
