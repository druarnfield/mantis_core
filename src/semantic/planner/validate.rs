//! Phase 2: Validation
//!
//! This phase validates the resolved query for semantic correctness:
//! - Checks join path safety (no fan-out)
//! - Validates GROUP BY completeness
//! - Ensures measures are only used in appropriate contexts

use std::collections::{HashMap, HashSet};

use crate::model::DataType;
use crate::semantic::error::{PlanError, PlanResult, TypeMismatchDetails};
use crate::semantic::model_graph::ModelGraph;

use super::resolved::{
    ResolvedColumn, ResolvedEntity, ResolvedJoinTree, ResolvedQuery, ResolvedSelect,
};

/// Check if two data types are compatible for joining.
///
/// Compatible types can be compared for equality in a JOIN condition.
/// This follows SQL semantics where implicit coercion is allowed.
pub fn types_compatible(left: &DataType, right: &DataType) -> bool {
    use DataType::*;

    match (left, right) {
        // Exact match is always compatible
        (a, b) if a == b => true,

        // Integer types can join with each other (implicit widening)
        (Int8 | Int16 | Int32 | Int64, Int8 | Int16 | Int32 | Int64) => true,

        // Float types can join with each other
        (Float32 | Float64, Float32 | Float64) => true,

        // Integers and floats can join (SQL allows this with coercion)
        (Int8 | Int16 | Int32 | Int64, Float32 | Float64) => true,
        (Float32 | Float64, Int8 | Int16 | Int32 | Int64) => true,

        // Decimal can join with integers and floats
        (Decimal(_, _), Int8 | Int16 | Int32 | Int64) => true,
        (Int8 | Int16 | Int32 | Int64, Decimal(_, _)) => true,
        (Decimal(_, _), Float32 | Float64) => true,
        (Float32 | Float64, Decimal(_, _)) => true,
        (Decimal(_, _), Decimal(_, _)) => true,

        // String types can join with each other
        (String | Varchar(_) | Char(_), String | Varchar(_) | Char(_)) => true,

        // UUID types can join with each other
        (Uuid, Uuid) => true,

        // Date/time types follow strict rules
        (Date, Date) => true,
        (Time, Time) => true,
        (Timestamp, Timestamp | TimestampTz) => true,
        (TimestampTz, Timestamp | TimestampTz) => true,

        // Binary types
        (Binary, Binary) => true,

        // JSON types
        (Json, Json) => true,

        // Otherwise incompatible
        _ => false,
    }
}

/// Validated query - semantically correct and ready for planning.
#[derive(Debug, Clone)]
pub struct ValidatedQuery {
    /// The original resolved query.
    pub query: ResolvedQuery,

    /// The validated join tree.
    pub join_tree: ResolvedJoinTree,

    /// Metadata for all referenced entities (name -> physical info).
    pub entity_info: HashMap<String, ResolvedEntity>,
}

/// Validator - handles Phase 2 of query planning.
pub struct Validator<'a> {
    graph: &'a ModelGraph,
}

impl<'a> Validator<'a> {
    pub fn new(graph: &'a ModelGraph) -> Self {
        Self { graph }
    }

    /// Validate a resolved query.
    pub fn validate(&self, query: ResolvedQuery) -> PlanResult<ValidatedQuery> {
        // Build entity info map for all referenced entities
        let entity_info = self.build_entity_info(&query)?;

        // Build the join tree
        let join_tree = self.build_join_tree(&query)?;

        // Check join safety (cardinality)
        self.validate_join_safety(&join_tree)?;

        // Check join type compatibility
        self.validate_join_types(&join_tree)?;

        // Validate grouping
        self.validate_grouping(&query)?;

        Ok(ValidatedQuery {
            query,
            join_tree,
            entity_info,
        })
    }

    /// Build entity info map for all referenced entities.
    fn build_entity_info(
        &self,
        query: &ResolvedQuery,
    ) -> PlanResult<HashMap<String, ResolvedEntity>> {
        let mut info = HashMap::new();

        // Add the root entity
        info.insert(query.from.name.clone(), query.from.clone());

        // Add all other referenced entities
        for entity_name in &query.referenced_entities {
            if !info.contains_key(entity_name) {
                let entity_info = self.graph.get_entity_info(entity_name)?;

                info.insert(
                    entity_name.clone(),
                    ResolvedEntity {
                        name: entity_info.name,
                        physical_table: entity_info.physical_table,
                        physical_schema: entity_info.physical_schema,
                        materialized: entity_info.materialized,
                    },
                );
            }
        }

        Ok(info)
    }

    /// Build the join tree for the query.
    fn build_join_tree(&self, query: &ResolvedQuery) -> PlanResult<ResolvedJoinTree> {
        let root = &query.from.name;

        // Get other entities that need to be joined
        let other_entities: Vec<&str> = query
            .referenced_entities
            .iter()
            .filter(|e| *e != root)
            .map(|s| s.as_str())
            .collect();

        if other_entities.is_empty() {
            return Ok(ResolvedJoinTree::empty(root));
        }

        let join_path = self.graph.find_join_tree(root, &other_entities)?;
        let is_safe = join_path.is_safe();

        Ok(ResolvedJoinTree {
            root: root.clone(),
            edges: join_path.edges,
            is_safe,
        })
    }

    /// Validate that the join tree doesn't cause fan-out.
    fn validate_join_safety(&self, join_tree: &ResolvedJoinTree) -> PlanResult<()> {
        if join_tree.is_safe {
            return Ok(());
        }

        // Find the first unsafe edge for a helpful error message
        if let Some(edge) = join_tree.edges.iter().find(|e| e.cardinality.causes_fanout()) {
            return Err(PlanError::UnsafeJoinPath {
                from: edge.from_entity.clone(),
                to: edge.to_entity.clone(),
                message: format!(
                    "Joining {} -> {} is 1:N which causes row multiplication. \
                     Start from '{}' instead.",
                    edge.from_entity, edge.to_entity, edge.to_entity
                ),
            });
        }

        Ok(())
    }

    /// Validate that join column types are compatible.
    ///
    /// This catches type mismatches before query execution, providing
    /// better error messages than database errors.
    fn validate_join_types(&self, join_tree: &ResolvedJoinTree) -> PlanResult<()> {
        for edge in &join_tree.edges {
            // Try to get column types - skip validation if types not available
            // (e.g., for fact entities that don't have explicit column definitions)
            let from_type = match self.graph.get_column_type(&edge.from_entity, &edge.from_column) {
                Ok(t) => t,
                Err(_) => continue, // Skip if column not found (e.g., fact entity)
            };

            let to_type = match self.graph.get_column_type(&edge.to_entity, &edge.to_column) {
                Ok(t) => t,
                Err(_) => continue, // Skip if column not found
            };

            if !types_compatible(&from_type, &to_type) {
                return Err(PlanError::TypeMismatch(Box::new(TypeMismatchDetails {
                    left_entity: edge.from_entity.clone(),
                    left_column: edge.from_column.clone(),
                    left_type: format!("{:?}", from_type),
                    right_entity: edge.to_entity.clone(),
                    right_column: edge.to_column.clone(),
                    right_type: format!("{:?}", to_type),
                })));
            }
        }

        Ok(())
    }

    /// Validate that all non-aggregated columns are in GROUP BY.
    fn validate_grouping(&self, query: &ResolvedQuery) -> PlanResult<()> {
        // If there's no GROUP BY, nothing to validate
        if query.group_by.is_empty() {
            return Ok(());
        }

        // Build set of grouped columns
        let grouped: HashSet<(&str, &str)> = query
            .group_by
            .iter()
            .map(|c| (c.entity_alias.as_str(), c.physical_name.as_str()))
            .collect();

        // Check that all selected columns are either grouped or aggregated
        for select in &query.select {
            if let ResolvedSelect::Column { column, .. } = select {
                if !is_column_in_group(&grouped, column) {
                    return Err(PlanError::UngroupedColumn {
                        column: format!("{}.{}", column.entity_alias, column.logical_name),
                    });
                }
            }
            // Measures and Aggregates are aggregated, so they're fine
        }

        Ok(())
    }
}

/// Check if a column is in the GROUP BY set.
fn is_column_in_group(grouped: &HashSet<(&str, &str)>, column: &ResolvedColumn) -> bool {
    grouped.contains(&(column.entity_alias.as_str(), column.physical_name.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Cardinality, DataType, FactDefinition, Model, Relationship, SourceEntity};
    use crate::semantic::model_graph::ModelGraph;
    use crate::semantic::planner::resolve::Resolver;
    use crate::semantic::planner::types::{SelectField, SemanticQuery};

    // =========================================================================
    // types_compatible tests
    // =========================================================================

    #[test]
    fn test_types_compatible_exact_match() {
        assert!(types_compatible(&DataType::Int64, &DataType::Int64));
        assert!(types_compatible(&DataType::String, &DataType::String));
        assert!(types_compatible(&DataType::Uuid, &DataType::Uuid));
    }

    #[test]
    fn test_types_compatible_integer_variants() {
        // All integer types should be compatible with each other
        assert!(types_compatible(&DataType::Int8, &DataType::Int64));
        assert!(types_compatible(&DataType::Int16, &DataType::Int32));
        assert!(types_compatible(&DataType::Int64, &DataType::Int8));
    }

    #[test]
    fn test_types_compatible_float_variants() {
        assert!(types_compatible(&DataType::Float32, &DataType::Float64));
        assert!(types_compatible(&DataType::Float64, &DataType::Float32));
    }

    #[test]
    fn test_types_compatible_numeric_cross() {
        // Integers and floats can join
        assert!(types_compatible(&DataType::Int64, &DataType::Float64));
        assert!(types_compatible(&DataType::Float32, &DataType::Int32));

        // Decimal can join with integers and floats
        assert!(types_compatible(&DataType::Decimal(10, 2), &DataType::Int64));
        assert!(types_compatible(&DataType::Int32, &DataType::Decimal(18, 4)));
        assert!(types_compatible(&DataType::Decimal(10, 2), &DataType::Float64));
    }

    #[test]
    fn test_types_compatible_string_variants() {
        assert!(types_compatible(&DataType::String, &DataType::Varchar(255)));
        assert!(types_compatible(&DataType::Varchar(100), &DataType::Char(10)));
        assert!(types_compatible(&DataType::Char(50), &DataType::String));
    }

    #[test]
    fn test_types_compatible_timestamp_variants() {
        assert!(types_compatible(&DataType::Timestamp, &DataType::TimestampTz));
        assert!(types_compatible(&DataType::TimestampTz, &DataType::Timestamp));
    }

    #[test]
    fn test_types_incompatible() {
        // String vs Integer
        assert!(!types_compatible(&DataType::String, &DataType::Int64));
        assert!(!types_compatible(&DataType::Int32, &DataType::Varchar(50)));

        // Date vs String
        assert!(!types_compatible(&DataType::Date, &DataType::String));

        // UUID vs Integer
        assert!(!types_compatible(&DataType::Uuid, &DataType::Int64));

        // Boolean vs anything else
        assert!(!types_compatible(&DataType::Bool, &DataType::Int32));
        assert!(!types_compatible(&DataType::Bool, &DataType::String));

        // Date vs Timestamp (strict - different semantics)
        assert!(!types_compatible(&DataType::Date, &DataType::Timestamp));
    }

    // =========================================================================
    // Type validation integration tests
    // =========================================================================

    fn sample_graph_with_type_mismatch() -> ModelGraph {
        // Create a model where orders.customer_id is Int64 but customers.customer_id is String
        let model = Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("customer_id", DataType::Int64) // Int64
                    .with_primary_key(vec!["order_id"]),
            )
            .with_source(
                SourceEntity::new("customers", "dbo.customers")
                    .with_required_column("customer_id", DataType::String) // String - MISMATCH!
                    .with_required_column("name", DataType::String)
                    .with_primary_key(vec!["customer_id"]),
            )
            .with_relationship(Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ));

        ModelGraph::from_model(model).unwrap()
    }

    #[test]
    fn test_validate_type_mismatch_detected() {
        let graph = sample_graph_with_type_mismatch();
        let resolver = Resolver::new(&graph);
        let validator = Validator::new(&graph);

        // Query that joins orders to customers
        let sq = SemanticQuery {
            from: Some("orders".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![
                SelectField::new("orders", "order_id"),
                SelectField::new("customers", "name"),
            ],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let resolved = resolver.resolve(&sq).expect("Resolve should succeed");
        let result = validator.validate(resolved);

        // Should fail with TypeMismatch
        assert!(
            matches!(result, Err(PlanError::TypeMismatch(_))),
            "Expected TypeMismatch error but got: {:?}",
            result
        );

        // Verify error message contains useful information
        if let Err(PlanError::TypeMismatch(details)) = result {
            assert!(
                details.left_type.contains("Int64"),
                "Expected Int64 in left_type"
            );
            assert!(
                details.right_type.contains("String"),
                "Expected String in right_type"
            );
        }
    }

    #[test]
    fn test_validate_compatible_types_pass() {
        // Use the standard sample_graph which has compatible Int64 types
        let graph = sample_graph();
        let resolver = Resolver::new(&graph);
        let validator = Validator::new(&graph);

        let sq = SemanticQuery {
            from: Some("orders".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![
                SelectField::new("orders", "amount"),
                SelectField::new("customers", "region"),
            ],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let resolved = resolver.resolve(&sq).expect("Resolve should succeed");
        let result = validator.validate(resolved);

        assert!(result.is_ok(), "Expected validation to pass but got: {:?}", result.err());
    }

    fn sample_graph() -> ModelGraph {
        let model = Model::new()
            .with_source(
                SourceEntity::new("orders", "dbo.fact_orders")
                    .with_required_column("order_id", DataType::Int64)
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("amount", DataType::Decimal(10, 2))
                    .with_primary_key(vec!["order_id"]),
            )
            .with_source(
                SourceEntity::new("customers", "dbo.dim_customers")
                    .with_required_column("customer_id", DataType::Int64)
                    .with_required_column("region", DataType::String)
                    .with_primary_key(vec!["customer_id"]),
            )
            .with_relationship(Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ))
            .with_fact(
                FactDefinition::new("orders_fact", "dbo.orders_fact")
                    .with_grain("orders", "order_id")
                    .with_sum("revenue", "amount"),
            );

        ModelGraph::from_model(model).unwrap()
    }

    #[test]
    fn test_validate_safe_join() {
        let graph = sample_graph();
        let resolver = Resolver::new(&graph);
        let validator = Validator::new(&graph);

        // Orders -> Customers is ManyToOne (safe)
        // Query source entity with column, filter by related entity
        // No GROUP BY so columns don't need aggregation
        let sq = SemanticQuery {
            from: Some("orders".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![
                SelectField::new("orders", "amount"),
                SelectField::new("customers", "region"),
            ],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let resolved = resolver.resolve(&sq).expect("Resolve failed");
        let validated = validator.validate(resolved);
        assert!(validated.is_ok(), "Expected safe join but got: {:?}", validated.err());
    }

    #[test]
    fn test_validate_unsafe_join() {
        let graph = sample_graph();
        let resolver = Resolver::new(&graph);
        let validator = Validator::new(&graph);

        // Customers -> Orders is OneToMany (unsafe)
        let sq = SemanticQuery {
            from: Some("customers".into()),
            filters: vec![],
            group_by: vec![],
            select: vec![SelectField::new("orders", "amount")],
            derived: vec![],
            order_by: vec![],
            limit: None,
        };

        let resolved = resolver.resolve(&sq).unwrap();
        let validated = validator.validate(resolved);
        assert!(matches!(validated, Err(PlanError::UnsafeJoinPath { .. })));
    }
}
