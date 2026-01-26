// src/planner/join_optimizer/join_graph.rs
use crate::semantic::graph::{Cardinality, UnifiedGraph};
use std::collections::{HashMap, HashSet};

pub struct JoinGraph {
    tables: HashSet<String>,
    edges: HashMap<TablePair, JoinEdge>,
}

#[derive(Hash, Eq, PartialEq)]
struct TablePair(String, String);

pub struct JoinEdge {
    pub cardinality: Cardinality,
    pub join_columns: Vec<(String, String)>,
}

impl JoinGraph {
    pub fn build(graph: &UnifiedGraph, tables: &[String]) -> Self {
        let mut edges = HashMap::new();

        // For each pair of tables, check if they can be joined
        for i in 0..tables.len() {
            for j in i + 1..tables.len() {
                let t1 = &tables[i];
                let t2 = &tables[j];

                // Use UnifiedGraph.find_path() to get join info
                if let Ok(path) = graph.find_path(t1, t2) {
                    if !path.steps.is_empty() {
                        let step = &path.steps[0];

                        // Parse cardinality from string
                        let cardinality = match step.cardinality.as_str() {
                            "1:1" => Cardinality::OneToOne,
                            "1:N" => Cardinality::OneToMany,
                            "N:1" => Cardinality::ManyToOne,
                            "N:N" => Cardinality::ManyToMany,
                            _ => Cardinality::ManyToMany,
                        };

                        let edge = JoinEdge {
                            cardinality,
                            join_columns: vec![], // Will be populated from graph edge data
                        };

                        edges.insert(TablePair(t1.clone(), t2.clone()), edge);
                    }
                }
            }
        }

        Self {
            tables: tables.iter().cloned().collect(),
            edges,
        }
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn are_joinable(&self, t1: &str, t2: &str) -> bool {
        let key1 = TablePair(t1.to_string(), t2.to_string());
        let key2 = TablePair(t2.to_string(), t1.to_string());

        self.edges.contains_key(&key1) || self.edges.contains_key(&key2)
    }

    pub fn get_join_edge(&self, t1: &str, t2: &str) -> Option<&JoinEdge> {
        let key1 = TablePair(t1.to_string(), t2.to_string());
        if let Some(edge) = self.edges.get(&key1) {
            return Some(edge);
        }

        let key2 = TablePair(t2.to_string(), t1.to_string());
        self.edges.get(&key2)
    }
}
