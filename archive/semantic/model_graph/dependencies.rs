//! Dependency analysis for ModelGraph.
//!
//! This module contains methods for analyzing build dependencies,
//! determining build order, and impact analysis for incremental builds.

use std::collections::{HashMap, HashSet};

use petgraph::graph::{DiGraph, NodeIndex};

use super::{GraphResult, ModelGraph, SemanticError};

impl ModelGraph {
    /// Get the sources that a fact depends on (from grain and includes).
    fn fact_source_dependencies(&self, fact_name: &str) -> Vec<&str> {
        let mut deps = Vec::new();
        if let Some(fact) = self.model.facts.get(fact_name) {
            // Grain sources
            for grain in &fact.grain {
                deps.push(grain.source_entity.as_str());
            }
            // Included entities
            for include in fact.includes.values() {
                deps.push(include.entity.as_str());
            }
        }
        deps
    }

    /// Get the source that a dimension depends on.
    fn dimension_source_dependency(&self, dim_name: &str) -> Option<&str> {
        self.model
            .dimensions
            .get(dim_name)
            .map(|d| d.source_entity.as_str())
    }

    /// Get all sources required to build a target (fact or dimension).
    ///
    /// This follows the dependency chain transitively:
    /// - For a dimension: just its source
    /// - For a fact: grain sources + included sources (transitive if they're facts)
    pub fn get_required_sources(&self, target: &str) -> GraphResult<Vec<&str>> {
        if !self.model.facts.contains_key(target) && !self.model.dimensions.contains_key(target) {
            return Err(SemanticError::UnknownEntity(target.into()));
        }

        let mut required: HashSet<&str> = HashSet::new();
        let mut to_visit: Vec<&str> = vec![target];
        let mut visited: HashSet<&str> = HashSet::new();

        while let Some(current) = to_visit.pop() {
            if visited.contains(current) {
                continue;
            }
            visited.insert(current);

            // If it's a fact, get its dependencies
            if self.model.facts.contains_key(current) {
                for dep in self.fact_source_dependencies(current) {
                    // If the dependency is a source, add it
                    if self.model.sources.contains_key(dep) {
                        required.insert(dep);
                    }
                    // If it's another fact or dimension, traverse it
                    if self.model.facts.contains_key(dep) || self.model.dimensions.contains_key(dep)
                    {
                        to_visit.push(dep);
                    }
                }
            }

            // If it's a dimension, get its source
            if let Some(source) = self.dimension_source_dependency(current) {
                if self.model.sources.contains_key(source) {
                    required.insert(source);
                }
            }
        }

        Ok(required.into_iter().collect())
    }

    /// Get all targets (facts/dimensions) that would be affected if a source changes.
    ///
    /// This is the inverse of get_required_sources - useful for incremental builds.
    pub fn get_affected_targets(&self, source: &str) -> GraphResult<Vec<&str>> {
        if !self.model.sources.contains_key(source) {
            return Err(SemanticError::UnknownEntity(source.into()));
        }

        let mut affected: Vec<&str> = Vec::new();

        // Check all facts
        for fact_name in self.model.facts.keys() {
            if let Ok(sources) = self.get_required_sources(fact_name) {
                if sources.contains(&source) {
                    affected.push(fact_name);
                }
            }
        }

        // Check all dimensions
        for (dim_name, dim) in &self.model.dimensions {
            if dim.source_entity == source {
                affected.push(dim_name);
            }
        }

        Ok(affected)
    }

    /// Build a dependency DAG for targets (facts and dimensions).
    ///
    /// Returns a separate graph where:
    /// - Nodes are target names (facts and dimensions)
    /// - Edges represent "A depends on B" (A -> B means build B before A)
    ///
    /// This is used for determining build order.
    pub fn build_dependency_dag(&self) -> DiGraph<String, ()> {
        let mut dag: DiGraph<String, ()> = DiGraph::new();
        let mut indices: HashMap<String, NodeIndex> = HashMap::new();

        // Add all targets as nodes
        for name in self.model.facts.keys() {
            let idx = dag.add_node(name.clone());
            indices.insert(name.clone(), idx);
        }
        for name in self.model.dimensions.keys() {
            let idx = dag.add_node(name.clone());
            indices.insert(name.clone(), idx);
        }

        // Add edges for fact dependencies
        for (fact_name, fact) in &self.model.facts {
            let fact_idx = indices[fact_name];

            // Facts depend on dimensions/facts they include
            for include in fact.includes.values() {
                // Check if the included entity is a dimension
                if let Some(&dim_idx) = indices.get(&include.entity) {
                    dag.add_edge(fact_idx, dim_idx, ());
                }
            }

            // Facts depend on facts/dimensions used in grain
            for grain in &fact.grain {
                if let Some(&dep_idx) = indices.get(&grain.source_entity) {
                    dag.add_edge(fact_idx, dep_idx, ());
                }
            }
        }

        dag
    }

    /// Get targets in topological order (dependencies first).
    ///
    /// Returns targets ordered so that dependencies come before dependents.
    /// For example: [dim_customers, dim_products, fact_orders, fact_summary]
    pub fn topological_order(&self) -> GraphResult<Vec<String>> {
        let dag = self.build_dependency_dag();

        // Use petgraph's toposort
        use petgraph::algo::toposort;

        match toposort(&dag, None) {
            Ok(order) => {
                // Reverse because toposort gives dependents first
                let names: Vec<String> = order
                    .into_iter()
                    .rev()
                    .map(|idx| dag[idx].clone())
                    .collect();
                Ok(names)
            }
            Err(cycle) => {
                // There's a cycle - find the node name
                let cycle_node = dag[cycle.node_id()].clone();
                Err(SemanticError::CyclicDependency(vec![cycle_node]))
            }
        }
    }

    /// Detect cycles in target dependencies.
    ///
    /// Returns Some with the cycle path if a cycle exists, None otherwise.
    /// The returned path shows the full cycle (e.g., ["A", "B", "C", "A"]).
    pub fn detect_cycles(&self) -> Option<Vec<String>> {
        let dag = self.build_dependency_dag();

        use petgraph::algo::kosaraju_scc;

        // Find strongly connected components - any SCC with >1 node is a cycle
        let sccs = kosaraju_scc(&dag);

        for scc in sccs {
            if scc.len() > 1 {
                // Found a cycle - reconstruct the path
                let mut cycle: Vec<String> = scc.iter().map(|&idx| dag[idx].clone()).collect();
                // Add the first node again to show it's a cycle
                if let Some(first) = cycle.first().cloned() {
                    cycle.push(first);
                }
                return Some(cycle);
            }
        }

        // Also check for self-loops (single node pointing to itself)
        for node_idx in dag.node_indices() {
            if dag.neighbors(node_idx).any(|n| n == node_idx) {
                let name = dag[node_idx].clone();
                return Some(vec![name.clone(), name]);
            }
        }

        None
    }

    /// Check if target A depends on target B (directly or transitively).
    pub fn depends_on(&self, target_a: &str, target_b: &str) -> bool {
        let dag = self.build_dependency_dag();

        // Find indices
        let a_idx = dag.node_indices().find(|&i| dag[i] == target_a);
        let b_idx = dag.node_indices().find(|&i| dag[i] == target_b);

        match (a_idx, b_idx) {
            (Some(a), Some(b)) => {
                // Check if there's a path from A to B in the DAG
                use petgraph::algo::has_path_connecting;
                has_path_connecting(&dag, a, b, None)
            }
            _ => false,
        }
    }
}
