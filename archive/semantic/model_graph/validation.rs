//! Validation helpers for ModelGraph.
//!
//! This module contains methods for validating the model's consistency,
//! checking join path safety, and inferring grain.

use crate::model::Cardinality;

use super::{GraphResult, JoinPath, ModelGraph, SemanticError};

impl ModelGraph {
    /// Validate the entire model for consistency.
    ///
    /// This performs comprehensive validation including:
    /// - All relationship references exist
    /// - All fact/dimension sources exist
    /// - No cycles in dependencies (for build ordering)
    /// - Join columns exist in referenced entities
    pub fn validate(&self) -> GraphResult<()> {
        // First use the Model's own validation
        self.model
            .validate()
            .map_err(|e| SemanticError::InvalidModel(e.to_string()))?;

        // Check for cycles in the dependency DAG
        if let Some(cycle) = self.detect_cycles() {
            return Err(SemanticError::CyclicDependency(cycle));
        }

        // Validate that relationship columns exist
        for rel in &self.model.relationships {
            // Check from_column exists
            if let Some(source) = self.model.sources.get(&rel.from_entity) {
                if !source.columns.contains_key(&rel.from_column) {
                    return Err(SemanticError::InvalidModel(format!(
                        "Relationship column '{}' not found in entity '{}'",
                        rel.from_column, rel.from_entity
                    )));
                }
            }

            // Check to_column exists
            if let Some(source) = self.model.sources.get(&rel.to_entity) {
                if !source.columns.contains_key(&rel.to_column) {
                    return Err(SemanticError::InvalidModel(format!(
                        "Relationship column '{}' not found in entity '{}'",
                        rel.to_column, rel.to_entity
                    )));
                }
            }
        }

        // Validate grain columns exist
        for (fact_name, fact) in &self.model.facts {
            for grain in &fact.grain {
                if let Some(source) = self.model.sources.get(&grain.source_entity) {
                    if !source.columns.contains_key(&grain.source_column) {
                        return Err(SemanticError::InvalidModel(format!(
                            "Grain column '{}' not found in entity '{}' for fact '{}'",
                            grain.source_column, grain.source_entity, fact_name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate that a join path is safe (no fan-out).
    ///
    /// A join path causes fan-out when traversing a 1:N or N:M relationship
    /// in the "one to many" direction, which can cause row multiplication.
    pub fn validate_join_path(&self, path: &JoinPath) -> GraphResult<()> {
        if path.causes_fanout() {
            // Find the first edge that causes fanout for a helpful error
            for edge in &path.edges {
                if edge.causes_fanout() {
                    return Err(SemanticError::InvalidModel(format!(
                        "Join from '{}' to '{}' causes row fanout ({:?} cardinality)",
                        edge.from_entity, edge.to_entity, edge.cardinality
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check if a join path between two entities is safe.
    ///
    /// Returns Ok(JoinPath) if the path exists and is safe, or an error if
    /// the path doesn't exist or would cause fan-out.
    pub fn validate_safe_path(&self, from: &str, to: &str) -> GraphResult<JoinPath> {
        let path = self.find_path(from, to)?;
        self.validate_join_path(&path)?;
        Ok(path)
    }

    /// Infer the grain (primary entity) for a set of entities.
    ///
    /// Given a set of entities that will be joined, this determines which
    /// entity should be the "root" to minimize fan-out. Returns the entity
    /// that can reach all others without fan-out, or None if no such entity exists.
    pub fn infer_grain<'a>(&self, entities: &[&'a str]) -> Option<&'a str> {
        if entities.is_empty() {
            return None;
        }

        if entities.len() == 1 {
            return Some(entities[0]);
        }

        // Try each entity as potential grain
        for &candidate in entities {
            let mut is_valid = true;

            for &target in entities {
                if candidate == target {
                    continue;
                }

                // Check if we can reach target from candidate without fan-out
                match self.find_path(candidate, target) {
                    Ok(path) => {
                        if path.causes_fanout() {
                            is_valid = false;
                            break;
                        }
                    }
                    Err(_) => {
                        is_valid = false;
                        break;
                    }
                }
            }

            if is_valid {
                return Some(candidate);
            }
        }

        None
    }

    /// Get warnings about potential fan-out issues.
    ///
    /// Returns a list of (from_entity, to_entity, cardinality) for each
    /// relationship in the join tree that causes fan-out.
    pub fn fanout_warnings(
        &self,
        root: &str,
        entities: &[&str],
    ) -> Vec<(String, String, Cardinality)> {
        let mut warnings = Vec::new();

        for target in entities {
            if *target == root {
                continue;
            }

            if let Ok(path) = self.find_path(root, target) {
                for edge in &path.edges {
                    if edge.causes_fanout() {
                        warnings.push((
                            edge.from_entity.clone(),
                            edge.to_entity.clone(),
                            edge.cardinality,
                        ));
                    }
                }
            }
        }

        warnings
    }

    /// Validate a specific fact definition.
    ///
    /// Checks that all referenced entities and columns exist.
    pub fn validate_fact(&self, fact_name: &str) -> GraphResult<()> {
        let fact = self
            .model
            .facts
            .get(fact_name)
            .ok_or_else(|| SemanticError::UnknownEntity(fact_name.into()))?;

        // Check grain sources exist
        for grain in &fact.grain {
            if !self.model.sources.contains_key(&grain.source_entity)
                && !self.model.dimensions.contains_key(&grain.source_entity)
                && !self.model.facts.contains_key(&grain.source_entity)
            {
                return Err(SemanticError::InvalidModel(format!(
                    "Grain source '{}' not found for fact '{}'",
                    grain.source_entity, fact_name
                )));
            }
        }

        // Check included entities exist
        for (alias, include) in &fact.includes {
            if !self.model.sources.contains_key(&include.entity)
                && !self.model.dimensions.contains_key(&include.entity)
                && !self.model.facts.contains_key(&include.entity)
            {
                return Err(SemanticError::InvalidModel(format!(
                    "Included entity '{}' (alias '{}') not found for fact '{}'",
                    include.entity, alias, fact_name
                )));
            }
        }

        Ok(())
    }

    /// Validate a specific dimension definition.
    ///
    /// Checks that the source entity exists.
    pub fn validate_dimension(&self, dim_name: &str) -> GraphResult<()> {
        let dim = self
            .model
            .dimensions
            .get(dim_name)
            .ok_or_else(|| SemanticError::UnknownEntity(dim_name.into()))?;

        // Check source entity exists
        if !self.model.sources.contains_key(&dim.source_entity)
            && !self.model.facts.contains_key(&dim.source_entity)
        {
            return Err(SemanticError::InvalidModel(format!(
                "Source entity '{}' not found for dimension '{}'",
                dim.source_entity, dim_name
            )));
        }

        Ok(())
    }

    /// Validate all targets (facts and dimensions) and return all errors found.
    ///
    /// Unlike `validate()` which stops at the first error, this collects all
    /// validation errors for batch reporting.
    pub fn validate_all_targets(&self) -> Vec<(String, SemanticError)> {
        let mut errors = Vec::new();

        // Validate all facts
        for fact_name in self.model.facts.keys() {
            if let Err(e) = self.validate_fact(fact_name) {
                errors.push((fact_name.clone(), e));
            }
        }

        // Validate all dimensions
        for dim_name in self.model.dimensions.keys() {
            if let Err(e) = self.validate_dimension(dim_name) {
                errors.push((dim_name.clone(), e));
            }
        }

        // Check for cycles (applies to all targets)
        if let Some(cycle) = self.detect_cycles() {
            // Add cycle error for each target in the cycle
            for target in &cycle {
                if target != cycle.last().unwrap_or(&String::new()) {
                    errors.push((
                        target.clone(),
                        SemanticError::CyclicDependency(cycle.clone()),
                    ));
                }
            }
        }

        errors
    }
}
