//! Path finding algorithms for ModelGraph.
//!
//! This module contains methods for finding paths between entities,
//! which is essential for JOIN generation in SQL queries.

use std::collections::{HashMap, HashSet, VecDeque};

use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::visit::EdgeRef;

use super::{GraphResult, JoinEdge, JoinPath, ModelGraph, SemanticError};

/// Parent information for path reconstruction.
/// Stores the parent node and the edge used to reach the current node.
struct ParentInfo {
    parent: NodeIndex,
    edge_idx: EdgeIndex,
}

impl ModelGraph {
    /// Find the shortest path between two entities using BFS.
    ///
    /// Returns a JoinPath containing the sequence of joins needed.
    /// The path is bidirectional - relationships can be traversed in either direction.
    ///
    /// # Performance
    ///
    /// Uses parent pointers instead of cloning paths at each step,
    /// reducing memory from O(E × P) to O(V) where E is edges explored,
    /// P is average path length, and V is vertices visited.
    pub fn find_path(&self, from: &str, to: &str) -> GraphResult<JoinPath> {
        // Same entity = empty path
        if from == to {
            return Ok(JoinPath::new());
        }

        let from_idx = self
            .node_indices
            .get(from)
            .ok_or_else(|| SemanticError::UnknownEntity(from.into()))?;

        let to_idx = self
            .node_indices
            .get(to)
            .ok_or_else(|| SemanticError::UnknownEntity(to.into()))?;

        // BFS with parent pointers instead of path cloning
        let mut visited: HashSet<NodeIndex> = HashSet::new();
        let mut parents: HashMap<NodeIndex, ParentInfo> = HashMap::new();
        let mut queue: VecDeque<NodeIndex> = VecDeque::new();

        queue.push_back(*from_idx);
        visited.insert(*from_idx);

        while let Some(current) = queue.pop_front() {
            // Check all outgoing edges
            for edge_ref in self.entity_graph.edges(current) {
                let neighbor = edge_ref.target();

                if visited.contains(&neighbor) {
                    continue;
                }

                // Store parent pointer instead of cloning path
                parents.insert(
                    neighbor,
                    ParentInfo {
                        parent: current,
                        edge_idx: edge_ref.id(),
                    },
                );

                // Found destination - reconstruct path
                if neighbor == *to_idx {
                    return Ok(self.reconstruct_path(*from_idx, neighbor, &parents));
                }

                visited.insert(neighbor);
                queue.push_back(neighbor);
            }
        }

        Err(SemanticError::NoPath {
            from: from.into(),
            to: to.into(),
        })
    }

    /// Reconstruct the path from parent pointers.
    ///
    /// Walks backward from destination to source using parent pointers,
    /// then reverses to get the correct order.
    fn reconstruct_path(
        &self,
        from_idx: NodeIndex,
        to_idx: NodeIndex,
        parents: &HashMap<NodeIndex, ParentInfo>,
    ) -> JoinPath {
        let mut edges = Vec::new();
        let mut current = to_idx;

        // Walk backward from destination to source
        while current != from_idx {
            let info = &parents[&current];
            let edge_data = &self.entity_graph[info.edge_idx];
            let from_node = &self.entity_graph[info.parent];
            let to_node = &self.entity_graph[current];

            edges.push(JoinEdge {
                from_entity: from_node.name.clone(),
                to_entity: to_node.name.clone(),
                from_column: edge_data.from_column.clone(),
                to_column: edge_data.to_column.clone(),
                cardinality: edge_data.cardinality,
            });

            current = info.parent;
        }

        // Reverse to get source-to-destination order
        edges.reverse();
        JoinPath { edges }
    }

    /// Check if a path exists between two entities.
    pub fn has_path(&self, from: &str, to: &str) -> bool {
        self.find_path(from, to).is_ok()
    }

    /// Find paths from a root entity to multiple target entities.
    ///
    /// Returns a deduplicated JoinPath containing all edges needed
    /// to reach all targets from the root. This is useful for
    /// building a join tree in SQL generation.
    pub fn find_join_tree(&self, root: &str, targets: &[&str]) -> GraphResult<JoinPath> {
        let mut all_edges: Vec<JoinEdge> = vec![];
        let mut seen_pairs: HashSet<(String, String)> = HashSet::new();

        for target in targets {
            if *target == root {
                continue;
            }

            let path = self.find_path(root, target)?;

            for edge in path.edges {
                let pair = (edge.from_entity.clone(), edge.to_entity.clone());
                if !seen_pairs.contains(&pair) {
                    seen_pairs.insert(pair);
                    all_edges.push(edge);
                }
            }
        }

        Ok(JoinPath { edges: all_edges })
    }

    /// Find all paths between two entities (up to a depth limit).
    ///
    /// This is useful for detecting ambiguous join paths where
    /// there are multiple routes between entities. The user should
    /// be warned if ambiguous paths exist.
    pub fn find_all_paths(
        &self,
        from: &str,
        to: &str,
        max_depth: usize,
    ) -> GraphResult<Vec<JoinPath>> {
        if from == to {
            return Ok(vec![JoinPath::new()]);
        }

        let from_idx = self
            .node_indices
            .get(from)
            .ok_or_else(|| SemanticError::UnknownEntity(from.into()))?;

        let to_idx = self
            .node_indices
            .get(to)
            .ok_or_else(|| SemanticError::UnknownEntity(to.into()))?;

        let mut results: Vec<JoinPath> = vec![];
        let mut stack: Vec<(NodeIndex, Vec<JoinEdge>, HashSet<NodeIndex>)> = vec![];

        let mut initial_visited = HashSet::new();
        initial_visited.insert(*from_idx);
        stack.push((*from_idx, vec![], initial_visited));

        while let Some((current, path, visited)) = stack.pop() {
            if path.len() >= max_depth {
                continue;
            }

            for edge_ref in self.entity_graph.edges(current) {
                let neighbor = edge_ref.target();

                if visited.contains(&neighbor) {
                    continue;
                }

                let edge_data = edge_ref.weight();
                let from_name = &self.entity_graph[current].name;
                let to_name = &self.entity_graph[neighbor].name;

                let join_edge = JoinEdge {
                    from_entity: from_name.clone(),
                    to_entity: to_name.clone(),
                    from_column: edge_data.from_column.clone(),
                    to_column: edge_data.to_column.clone(),
                    cardinality: edge_data.cardinality,
                };

                let mut new_path = path.clone();
                new_path.push(join_edge);

                if neighbor == *to_idx {
                    results.push(JoinPath { edges: new_path });
                } else {
                    let mut new_visited = visited.clone();
                    new_visited.insert(neighbor);
                    stack.push((neighbor, new_path, new_visited));
                }
            }
        }

        if results.is_empty() {
            Err(SemanticError::NoPath {
                from: from.into(),
                to: to.into(),
            })
        } else {
            Ok(results)
        }
    }

    /// Check if there are multiple paths between entities (ambiguous join).
    pub fn has_ambiguous_path(&self, from: &str, to: &str) -> bool {
        self.find_all_paths(from, to, 5)
            .map(|paths| paths.len() > 1)
            .unwrap_or(false)
    }

    /// Get all entities reachable from a starting entity.
    pub fn reachable_entities(&self, from: &str) -> GraphResult<Vec<&str>> {
        let from_idx = self
            .node_indices
            .get(from)
            .ok_or_else(|| SemanticError::UnknownEntity(from.into()))?;

        use petgraph::visit::Bfs;
        let mut bfs = Bfs::new(&self.entity_graph, *from_idx);

        let mut reachable = vec![];
        while let Some(node_idx) = bfs.next(&self.entity_graph) {
            let name = &self.entity_graph[node_idx].name;
            if name != from {
                reachable.push(name.as_str());
            }
        }

        Ok(reachable)
    }
}
