//! Tests for ModelGraph.

use super::*;
use crate::model::{DataType, DimensionDefinition, FactDefinition, Relationship, SourceEntity};

fn sample_model() -> Model {
    Model::new()
        .with_source(
            SourceEntity::new("orders", "raw.orders")
                .with_required_column("order_id", DataType::Int64)
                .with_required_column("customer_id", DataType::Int64)
                .with_required_column("product_id", DataType::Int64)
                .with_required_column("total", DataType::Decimal(10, 2))
                .with_primary_key(vec!["order_id"]),
        )
        .with_source(
            SourceEntity::new("customers", "raw.customers")
                .with_required_column("customer_id", DataType::Int64)
                .with_required_column("name", DataType::String)
                .with_required_column("region", DataType::String)
                .with_primary_key(vec!["customer_id"]),
        )
        .with_source(
            SourceEntity::new("products", "raw.products")
                .with_required_column("product_id", DataType::Int64)
                .with_required_column("name", DataType::String)
                .with_required_column("category", DataType::String)
                .with_primary_key(vec!["product_id"]),
        )
        .with_relationship(Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        ))
        .with_relationship(Relationship::new(
            "orders",
            "products",
            "product_id",
            "product_id",
            Cardinality::ManyToOne,
        ))
        .with_fact(
            FactDefinition::new("fact_orders", "analytics.fact_orders")
                .with_grain("orders", "order_id")
                .include("customers", vec!["name", "region"])
                .with_sum("revenue", "total"),
        )
        .with_dimension(
            DimensionDefinition::new("dim_customers", "analytics.dim_customers", "customers")
                .with_columns(vec!["customer_id", "name", "region"]),
        )
}

#[test]
fn test_from_model() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // 3 sources + 1 fact + 1 dimension = 5 entities
    assert_eq!(graph.entity_count(), 5);

    // 2 explicit relationships (orders->customers, orders->products)
    // + 2 implicit from fact (fact_orders->orders from grain, fact_orders->customers from include)
    // + 1 implicit from dimension (fact_orders->dim_customers since dim_customers.source is "customers" which is included)
    assert_eq!(graph.relationship_count(), 5);
}

#[test]
fn test_entity_types() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert_eq!(graph.entity_type("orders"), Some(&EntityType::Source));
    assert_eq!(graph.entity_type("customers"), Some(&EntityType::Source));
    assert_eq!(graph.entity_type("fact_orders"), Some(&EntityType::Fact));
    assert_eq!(
        graph.entity_type("dim_customers"),
        Some(&EntityType::Dimension)
    );
    assert_eq!(graph.entity_type("nonexistent"), None);
}

#[test]
fn test_has_entity() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert!(graph.has_entity("orders"));
    assert!(graph.has_entity("customers"));
    assert!(graph.has_entity("fact_orders"));
    assert!(!graph.has_entity("nonexistent"));
}

#[test]
fn test_entity_names() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let sources = graph.source_names();
    assert!(sources.contains(&"orders"));
    assert!(sources.contains(&"customers"));
    assert!(sources.contains(&"products"));

    let facts = graph.fact_names();
    assert!(facts.contains(&"fact_orders"));

    let dims = graph.dimension_names();
    assert!(dims.contains(&"dim_customers"));
}

#[test]
fn test_invalid_relationship() {
    let model = Model::new()
        .with_source(SourceEntity::new("orders", "raw.orders"))
        .with_relationship(Relationship::new(
            "orders",
            "nonexistent",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        ));

    let result = ModelGraph::from_model(model);
    assert!(matches!(result, Err(GraphError::InvalidModel(_))));
}

#[test]
fn test_join_path_fanout() {
    let path = JoinPath {
        edges: vec![JoinEdge {
            from_entity: "orders".into(),
            to_entity: "customers".into(),
            from_column: "customer_id".into(),
            to_column: "customer_id".into(),
            cardinality: Cardinality::ManyToOne,
        }],
    };

    assert!(path.is_safe());
    assert!(!path.causes_fanout());

    let unsafe_path = JoinPath {
        edges: vec![JoinEdge {
            from_entity: "customers".into(),
            to_entity: "orders".into(),
            from_column: "customer_id".into(),
            to_column: "customer_id".into(),
            cardinality: Cardinality::OneToMany,
        }],
    };

    assert!(!unsafe_path.is_safe());
    assert!(unsafe_path.causes_fanout());
}

#[test]
fn test_join_path_entities() {
    let path = JoinPath {
        edges: vec![
            JoinEdge {
                from_entity: "orders".into(),
                to_entity: "customers".into(),
                from_column: "customer_id".into(),
                to_column: "customer_id".into(),
                cardinality: Cardinality::ManyToOne,
            },
            JoinEdge {
                from_entity: "customers".into(),
                to_entity: "regions".into(),
                from_column: "region_id".into(),
                to_column: "region_id".into(),
                cardinality: Cardinality::ManyToOne,
            },
        ],
    };

    let entities = path.entities();
    assert_eq!(entities, vec!["orders", "customers", "regions"]);
}

// =========================================================================
// Path Finding Tests
// =========================================================================

#[test]
fn test_find_path_direct() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Direct path: orders -> customers (1 hop)
    let path = graph.find_path("orders", "customers").unwrap();
    assert_eq!(path.len(), 1);
    assert_eq!(path.edges[0].from_entity, "orders");
    assert_eq!(path.edges[0].to_entity, "customers");
    assert_eq!(path.edges[0].cardinality, Cardinality::ManyToOne);
    assert!(path.is_safe());
}

#[test]
fn test_find_path_reverse() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Reverse path: customers -> orders (1:N, causes fanout)
    let path = graph.find_path("customers", "orders").unwrap();
    assert_eq!(path.len(), 1);
    assert_eq!(path.edges[0].from_entity, "customers");
    assert_eq!(path.edges[0].to_entity, "orders");
    assert_eq!(path.edges[0].cardinality, Cardinality::OneToMany);
    assert!(!path.is_safe());
}

#[test]
fn test_find_path_same_entity() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Same entity = empty path
    let path = graph.find_path("orders", "orders").unwrap();
    assert!(path.is_empty());
}

#[test]
fn test_find_path_multi_hop() {
    // Create a model with multi-hop path
    let model = Model::new()
        .with_source(SourceEntity::new("orders", "raw.orders"))
        .with_source(SourceEntity::new("customers", "raw.customers"))
        .with_source(SourceEntity::new("regions", "raw.regions"))
        .with_relationship(Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        ))
        .with_relationship(Relationship::new(
            "customers",
            "regions",
            "region_id",
            "region_id",
            Cardinality::ManyToOne,
        ));

    let graph = ModelGraph::from_model(model).unwrap();

    // Multi-hop: orders -> customers -> regions
    let path = graph.find_path("orders", "regions").unwrap();
    assert_eq!(path.len(), 2);
    assert_eq!(path.edges[0].from_entity, "orders");
    assert_eq!(path.edges[0].to_entity, "customers");
    assert_eq!(path.edges[1].from_entity, "customers");
    assert_eq!(path.edges[1].to_entity, "regions");
    assert!(path.is_safe()); // All M:1, no fanout
}

#[test]
fn test_find_path_no_path() {
    let model = Model::new()
        .with_source(SourceEntity::new("orders", "raw.orders"))
        .with_source(SourceEntity::new("unrelated", "raw.unrelated"));
    // No relationship between them

    let graph = ModelGraph::from_model(model).unwrap();

    let result = graph.find_path("orders", "unrelated");
    assert!(matches!(result, Err(GraphError::NoPath { .. })));
}

#[test]
fn test_find_path_unknown_entity() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let result = graph.find_path("orders", "nonexistent");
    assert!(matches!(result, Err(GraphError::UnknownEntity(_))));
}

#[test]
fn test_has_path() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert!(graph.has_path("orders", "customers"));
    assert!(graph.has_path("customers", "orders")); // Bidirectional
    assert!(graph.has_path("orders", "products"));
    // customers -> orders -> products (bidirectional edges allow this)
    assert!(graph.has_path("customers", "products"));
}

#[test]
fn test_find_join_tree() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Find tree from orders to both customers and products
    let tree = graph
        .find_join_tree("orders", &["customers", "products"])
        .unwrap();

    // Should have 2 edges, one for each target
    assert_eq!(tree.len(), 2);
}

#[test]
fn test_find_join_tree_shared_path() {
    // Create model where path to C and D shares A->B
    let model = Model::new()
        .with_source(SourceEntity::new("a", "raw.a"))
        .with_source(SourceEntity::new("b", "raw.b"))
        .with_source(SourceEntity::new("c", "raw.c"))
        .with_source(SourceEntity::new("d", "raw.d"))
        .with_relationship(Relationship::new("a", "b", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("b", "c", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("b", "d", "id", "id", Cardinality::ManyToOne));

    let graph = ModelGraph::from_model(model).unwrap();

    // A -> C requires A->B, B->C
    // A -> D requires A->B, B->D
    // Join tree should deduplicate A->B
    let tree = graph.find_join_tree("a", &["c", "d"]).unwrap();

    // Should have 3 edges: A->B, B->C, B->D (A->B not duplicated)
    assert_eq!(tree.len(), 3);
}

#[test]
fn test_reachable_entities() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let reachable = graph.reachable_entities("orders").unwrap();

    // From orders, can reach customers and products (directly)
    // Also can reach fact_orders and dim_customers as they're in the graph
    assert!(reachable.contains(&"customers"));
    assert!(reachable.contains(&"products"));
}

#[test]
fn test_find_all_paths() {
    // Create model with multiple paths: A -> B -> D and A -> C -> D
    let model = Model::new()
        .with_source(SourceEntity::new("a", "raw.a"))
        .with_source(SourceEntity::new("b", "raw.b"))
        .with_source(SourceEntity::new("c", "raw.c"))
        .with_source(SourceEntity::new("d", "raw.d"))
        .with_relationship(Relationship::new("a", "b", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("a", "c", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("b", "d", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("c", "d", "id", "id", Cardinality::ManyToOne));

    let graph = ModelGraph::from_model(model).unwrap();

    let paths = graph.find_all_paths("a", "d", 5).unwrap();

    // Should find 2 paths: A->B->D and A->C->D
    assert_eq!(paths.len(), 2);
}

#[test]
fn test_has_ambiguous_path() {
    // Diamond pattern: A connects to D via both B and C
    let model = Model::new()
        .with_source(SourceEntity::new("a", "raw.a"))
        .with_source(SourceEntity::new("b", "raw.b"))
        .with_source(SourceEntity::new("c", "raw.c"))
        .with_source(SourceEntity::new("d", "raw.d"))
        .with_relationship(Relationship::new("a", "b", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("a", "c", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("b", "d", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("c", "d", "id", "id", Cardinality::ManyToOne));

    let graph = ModelGraph::from_model(model).unwrap();

    // A -> D has two paths: A->B->D and A->C->D
    assert!(graph.has_ambiguous_path("a", "d"));

    // Linear chain has no ambiguity
    let linear_model = Model::new()
        .with_source(SourceEntity::new("x", "raw.x"))
        .with_source(SourceEntity::new("y", "raw.y"))
        .with_source(SourceEntity::new("z", "raw.z"))
        .with_relationship(Relationship::new("x", "y", "id", "id", Cardinality::ManyToOne))
        .with_relationship(Relationship::new("y", "z", "id", "id", Cardinality::ManyToOne));

    let linear_graph = ModelGraph::from_model(linear_model).unwrap();
    assert!(!linear_graph.has_ambiguous_path("x", "z")); // Only one forward path
}

// =========================================================================
// Dependency Analysis Tests
// =========================================================================

#[test]
fn test_topological_order_simple() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let order = graph.topological_order().unwrap();

    // dim_customers should come before fact_orders (since fact includes customers)
    let dim_pos = order.iter().position(|s| s == "dim_customers");
    let fact_pos = order.iter().position(|s| s == "fact_orders");

    // Both should exist
    assert!(dim_pos.is_some());
    assert!(fact_pos.is_some());

    // For this simple model, order doesn't matter since there's no fact->dim dependency
    // in our sample_model (fact includes source "customers", not dim_customers)
}

#[test]
fn test_topological_order_with_dependencies() {
    // Create a model where fact depends on dimension
    let model = Model::new()
        .with_source(
            SourceEntity::new("raw_customers", "raw.customers")
                .with_required_column("id", DataType::Int64),
        )
        .with_source(
            SourceEntity::new("raw_orders", "raw.orders")
                .with_required_column("id", DataType::Int64),
        )
        .with_dimension(
            DimensionDefinition::new("dim_customers", "analytics.dim_customers", "raw_customers")
                .with_columns(vec!["id"]),
        )
        .with_fact(
            FactDefinition::new("fact_orders", "analytics.fact_orders")
                .with_grain("raw_orders", "id")
                // This fact includes dim_customers (another target)
                .include("dim_customers", vec!["id"]),
        );

    let graph = ModelGraph::from_model(model).unwrap();
    let order = graph.topological_order().unwrap();

    // dim_customers must come before fact_orders
    let dim_pos = order.iter().position(|s| s == "dim_customers").unwrap();
    let fact_pos = order.iter().position(|s| s == "fact_orders").unwrap();

    assert!(
        dim_pos < fact_pos,
        "dim_customers should be built before fact_orders"
    );
}

#[test]
fn test_detect_cycles_none() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert!(graph.detect_cycles().is_none());
}

#[test]
fn test_get_required_sources_dimension() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let sources = graph.get_required_sources("dim_customers").unwrap();
    assert!(sources.contains(&"customers"));
}

#[test]
fn test_get_required_sources_fact() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let sources = graph.get_required_sources("fact_orders").unwrap();

    // fact_orders has grain on "orders" and includes "customers"
    assert!(sources.contains(&"orders"));
    assert!(sources.contains(&"customers"));
}

#[test]
fn test_get_affected_targets() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // If orders changes, fact_orders is affected
    let affected = graph.get_affected_targets("orders").unwrap();
    assert!(affected.contains(&"fact_orders"));

    // If customers changes, both fact_orders and dim_customers are affected
    let affected = graph.get_affected_targets("customers").unwrap();
    assert!(affected.contains(&"fact_orders"));
    assert!(affected.contains(&"dim_customers"));
}

#[test]
fn test_depends_on() {
    // Create model with clear dependency: fact_summary -> fact_orders -> dim_customers
    let model = Model::new()
        .with_source(SourceEntity::new("raw_orders", "raw.orders"))
        .with_source(SourceEntity::new("raw_customers", "raw.customers"))
        .with_dimension(
            DimensionDefinition::new("dim_customers", "analytics.dim_customers", "raw_customers"),
        )
        .with_fact(
            FactDefinition::new("fact_orders", "analytics.fact_orders")
                .with_grain("raw_orders", "id")
                .include("dim_customers", vec!["id"]),
        )
        .with_fact(
            FactDefinition::new("fact_summary", "analytics.fact_summary")
                .with_grain("fact_orders", "id"), // Fact depending on another fact
        );

    let graph = ModelGraph::from_model(model).unwrap();

    // fact_orders depends on dim_customers
    assert!(graph.depends_on("fact_orders", "dim_customers"));

    // fact_summary depends on fact_orders
    assert!(graph.depends_on("fact_summary", "fact_orders"));

    // fact_summary transitively depends on dim_customers
    assert!(graph.depends_on("fact_summary", "dim_customers"));

    // dim_customers doesn't depend on anything
    assert!(!graph.depends_on("dim_customers", "fact_orders"));
}

#[test]
fn test_fact_depends_on_fact() {
    // Test the key use case: fact depending on another fact
    let model = Model::new()
        .with_source(SourceEntity::new("raw_events", "raw.events"))
        .with_fact(
            FactDefinition::new("fact_daily", "analytics.fact_daily")
                .with_grain("raw_events", "date"),
        )
        .with_fact(
            FactDefinition::new("fact_monthly", "analytics.fact_monthly")
                .with_grain("fact_daily", "month"), // Monthly aggregates from daily
        );

    let graph = ModelGraph::from_model(model).unwrap();

    // fact_monthly depends on fact_daily
    assert!(graph.depends_on("fact_monthly", "fact_daily"));

    // Build order should reflect this
    let order = graph.topological_order().unwrap();
    let daily_pos = order.iter().position(|s| s == "fact_daily").unwrap();
    let monthly_pos = order.iter().position(|s| s == "fact_monthly").unwrap();

    assert!(
        daily_pos < monthly_pos,
        "fact_daily should be built before fact_monthly"
    );
}

// =========================================================================
// Entity Resolution Tests (Phase 5)
// =========================================================================

#[test]
fn test_get_source() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let source = graph.get_source("orders").unwrap();
    assert_eq!(source.name, "orders");
    assert_eq!(source.table, "raw.orders");

    assert!(graph.get_source("nonexistent").is_none());
}

#[test]
fn test_get_fact() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let fact = graph.get_fact("fact_orders").unwrap();
    assert_eq!(fact.name, "fact_orders");
    assert_eq!(fact.target_table, "analytics.fact_orders");

    assert!(graph.get_fact("nonexistent").is_none());
}

#[test]
fn test_get_dimension() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let dim = graph.get_dimension("dim_customers").unwrap();
    assert_eq!(dim.name, "dim_customers");
    assert_eq!(dim.target_table, "analytics.dim_customers");

    assert!(graph.get_dimension("nonexistent").is_none());
}

#[test]
fn test_resolve_column() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Valid column
    let col = graph.resolve_column("orders", "order_id").unwrap();
    assert_eq!(col.name, "order_id");

    // Unknown entity
    let err = graph.resolve_column("nonexistent", "col").unwrap_err();
    assert!(matches!(err, GraphError::UnknownEntity(_)));

    // Unknown column
    let err = graph.resolve_column("orders", "nonexistent").unwrap_err();
    assert!(matches!(err, GraphError::UnknownField { .. }));
}

#[test]
fn test_find_relationship() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Direct relationship
    let rel = graph.find_relationship("orders", "customers").unwrap();
    assert_eq!(rel.from_entity, "orders");
    assert_eq!(rel.to_entity, "customers");

    // Reverse direction - not found (use find_relationship_either_direction)
    assert!(graph.find_relationship("customers", "orders").is_none());

    // No relationship
    assert!(graph.find_relationship("customers", "products").is_none());
}

#[test]
fn test_find_relationship_either_direction() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Forward direction
    let (rel, reversed) = graph
        .find_relationship_either_direction("orders", "customers")
        .unwrap();
    assert_eq!(rel.from_entity, "orders");
    assert!(!reversed);

    // Reverse direction
    let (rel, reversed) = graph
        .find_relationship_either_direction("customers", "orders")
        .unwrap();
    assert_eq!(rel.from_entity, "orders"); // Original relationship
    assert!(reversed);

    // No relationship
    assert!(graph
        .find_relationship_either_direction("customers", "products")
        .is_none());
}

#[test]
fn test_find_measure_entity() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let (fact_name, measure) = graph.find_measure_entity("revenue").unwrap();
    assert_eq!(fact_name, "fact_orders");
    assert_eq!(measure.name, "revenue");

    assert!(graph.find_measure_entity("nonexistent").is_none());
}

#[test]
fn test_list_measures() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let measures = graph.list_measures();
    assert_eq!(measures.len(), 1);

    let (fact, name, measure) = &measures[0];
    assert_eq!(*fact, "fact_orders");
    assert_eq!(*name, "revenue");
    assert_eq!(measure.source_column, "total");
}

#[test]
fn test_resolve_field_column() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let resolved = graph.resolve_field("orders", "order_id").unwrap();
    match resolved {
        ModelResolvedField::Column { entity, column } => {
            assert_eq!(entity, "orders");
            assert_eq!(column, "order_id");
        }
        _ => panic!("Expected column, got measure"),
    }
}

#[test]
fn test_resolve_field_measure() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let resolved = graph.resolve_field("fact_orders", "revenue").unwrap();
    match resolved {
        ModelResolvedField::Measure {
            entity,
            measure,
            source_column,
            ..
        } => {
            assert_eq!(entity, "fact_orders");
            assert_eq!(measure, "revenue");
            assert_eq!(source_column, "total");
        }
        _ => panic!("Expected measure, got column"),
    }
}

#[test]
fn test_resolve_field_unknown_entity() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let err = graph.resolve_field("nonexistent", "field").unwrap_err();
    assert!(matches!(err, GraphError::UnknownEntity(_)));
}

#[test]
fn test_resolve_field_unknown_field() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let err = graph.resolve_field("orders", "nonexistent").unwrap_err();
    assert!(matches!(err, GraphError::UnknownField { .. }));
}

#[test]
fn test_get_physical_table_source() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let table = graph.get_physical_table("orders").unwrap();
    assert_eq!(table, "raw.orders");
}

#[test]
fn test_get_physical_table_fact() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let table = graph.get_physical_table("fact_orders").unwrap();
    assert_eq!(table, "analytics.fact_orders");
}

#[test]
fn test_get_physical_table_dimension() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let table = graph.get_physical_table("dim_customers").unwrap();
    assert_eq!(table, "analytics.dim_customers");
}

#[test]
fn test_get_physical_table_unknown() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let err = graph.get_physical_table("nonexistent").unwrap_err();
    assert!(matches!(err, GraphError::UnknownEntity(_)));
}

// =========================================================================
// Validation Tests (Phase 6)
// =========================================================================

#[test]
fn test_validate_model_success() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert!(graph.validate().is_ok());
}

#[test]
fn test_validate_join_path_safe() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // orders -> customers is N:1, safe
    let path = graph.find_path("orders", "customers").unwrap();
    assert!(graph.validate_join_path(&path).is_ok());
}

#[test]
fn test_validate_join_path_fanout() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // customers -> orders is 1:N, causes fanout
    let path = graph.find_path("customers", "orders").unwrap();
    assert!(graph.validate_join_path(&path).is_err());
}

#[test]
fn test_validate_safe_path() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Safe direction
    assert!(graph.validate_safe_path("orders", "customers").is_ok());

    // Unsafe direction
    assert!(graph.validate_safe_path("customers", "orders").is_err());
}

#[test]
fn test_infer_grain_single() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // Single entity - returns that entity
    assert_eq!(graph.infer_grain(&["orders"]), Some("orders"));
}

#[test]
fn test_infer_grain_multiple() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // orders can reach customers safely (N:1)
    // customers cannot reach orders safely (1:N fanout)
    // So orders should be the grain
    assert_eq!(
        graph.infer_grain(&["orders", "customers"]),
        Some("orders")
    );
}

#[test]
fn test_infer_grain_empty() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert_eq!(graph.infer_grain(&[]), None);
}

#[test]
fn test_fanout_warnings() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    // From orders, no fanout warnings
    let warnings = graph.fanout_warnings("orders", &["customers"]);
    assert!(warnings.is_empty());

    // From customers, fanout warning
    let warnings = graph.fanout_warnings("customers", &["orders"]);
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].0, "customers");
    assert_eq!(warnings[0].1, "orders");
    assert_eq!(warnings[0].2, Cardinality::OneToMany);
}

#[test]
fn test_validate_fact_success() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    assert!(graph.validate_fact("fact_orders").is_ok());
}

#[test]
fn test_validate_fact_unknown() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let err = graph.validate_fact("nonexistent").unwrap_err();
    assert!(matches!(err, GraphError::UnknownEntity(_)));
}

#[test]
fn test_validate_all_targets_success() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model).unwrap();

    let errors = graph.validate_all_targets();
    assert!(errors.is_empty());
}

#[test]
fn test_validate_all_targets_with_errors() {
    // Create a model with an invalid dimension
    let model = Model::new()
        .with_source(SourceEntity::new("orders", "raw.orders"))
        .with_dimension(
            DimensionDefinition::new("dim_missing", "analytics.dim_missing", "nonexistent"),
        );

    let graph = ModelGraph::from_model(model).unwrap();
    let errors = graph.validate_all_targets();

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, "dim_missing");
}

// =============================================================================
// Role-Playing Dimension Tests
// =============================================================================

fn model_with_role_playing_dates() -> Model {
    Model::new()
        .with_source(
            SourceEntity::new("orders", "raw.orders")
                .with_required_column("order_id", DataType::Int64)
                .with_required_column("order_date_id", DataType::Int64)
                .with_required_column("ship_date_id", DataType::Int64)
                .with_required_column("delivery_date_id", DataType::Int64)
                .with_primary_key(vec!["order_id"]),
        )
        .with_source(
            SourceEntity::new("date", "raw.date_dim")
                .with_required_column("date_id", DataType::Int64)
                .with_required_column("year", DataType::Int32)
                .with_required_column("month", DataType::Int32)
                .with_required_column("day", DataType::Int32)
                .with_primary_key(vec!["date_id"]),
        )
        // Three different date roles
        .with_relationship(
            Relationship::new("orders", "date", "order_date_id", "date_id", Cardinality::ManyToOne)
                .with_role("order_date")
        )
        .with_relationship(
            Relationship::new("orders", "date", "ship_date_id", "date_id", Cardinality::ManyToOne)
                .with_role("ship_date")
        )
        .with_relationship(
            Relationship::new("orders", "date", "delivery_date_id", "date_id", Cardinality::ManyToOne)
                .with_role("delivery_date")
        )
}

#[test]
fn test_role_aliases_created() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Should have 3 role aliases
    assert_eq!(graph.role_alias_names().len(), 3);
    assert!(graph.is_role_alias("order_date"));
    assert!(graph.is_role_alias("ship_date"));
    assert!(graph.is_role_alias("delivery_date"));
    assert!(!graph.is_role_alias("date")); // Not a role
    assert!(!graph.is_role_alias("orders")); // Not a role
}

#[test]
fn test_get_role_alias() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    let order_date = graph.get_role_alias("order_date").unwrap();
    assert_eq!(order_date.role_name, "order_date");
    assert_eq!(order_date.from_entity, "orders");
    assert_eq!(order_date.from_column, "order_date_id");
    assert_eq!(order_date.to_entity, "date");
    assert_eq!(order_date.to_column, "date_id");

    let ship_date = graph.get_role_alias("ship_date").unwrap();
    assert_eq!(ship_date.from_column, "ship_date_id");
}

#[test]
fn test_resolve_entity_name_role() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Resolving a role should return the target entity
    let resolved = graph.resolve_entity_name("order_date").unwrap();
    assert_eq!(resolved.entity, "date");
    assert!(resolved.is_role());
    assert_eq!(resolved.role_name(), Some("order_date"));
    assert_eq!(resolved.fk_column(), Some("order_date_id"));

    // Resolving a regular entity
    let resolved = graph.resolve_entity_name("orders").unwrap();
    assert_eq!(resolved.entity, "orders");
    assert!(!resolved.is_role());
    assert_eq!(resolved.role_name(), None);
}

#[test]
fn test_roles_for_dimension() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Date dimension should have 3 roles
    let date_roles = graph.roles_for_dimension("date");
    assert_eq!(date_roles.len(), 3);

    // Orders shouldn't have any roles pointing to it
    let orders_roles = graph.roles_for_dimension("orders");
    assert!(orders_roles.is_empty());
}

#[test]
fn test_is_dimension_ambiguous() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Date dimension is ambiguous (3 roles)
    assert!(graph.is_dimension_ambiguous("date"));

    // Orders is not ambiguous
    assert!(!graph.is_dimension_ambiguous("orders"));
}

#[test]
fn test_resolve_field_with_role_alias() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Resolve field using role name should work
    let resolved = graph.resolve_field("order_date", "year").unwrap();
    match resolved {
        super::ModelResolvedField::Column { entity, column } => {
            // Entity should be the role name for JOIN generation
            assert_eq!(entity, "order_date");
            assert_eq!(column, "year");
        }
        _ => panic!("Expected Column, got Measure"),
    }

    // Resolve using the actual entity name should FAIL if dimension is ambiguous
    let result = graph.resolve_field("date", "month");
    match result {
        Err(super::SemanticError::AmbiguousDimensionRole {
            dimension,
            available_roles,
        }) => {
            assert_eq!(dimension, "date");
            assert_eq!(available_roles.len(), 3);
            // Roles should be available
            assert!(available_roles.contains(&"order_date".to_string()));
            assert!(available_roles.contains(&"ship_date".to_string()));
            assert!(available_roles.contains(&"delivery_date".to_string()));
        }
        Ok(_) => panic!("Expected AmbiguousDimensionRole error"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[test]
fn test_has_column_with_role_alias() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Check column via role name
    assert!(graph.has_column("order_date", "year"));
    assert!(graph.has_column("ship_date", "month"));
    assert!(graph.has_column("delivery_date", "day"));

    // Non-existent column
    assert!(!graph.has_column("order_date", "nonexistent"));
}

#[test]
fn test_get_entity_info_with_role_alias() {
    let model = model_with_role_playing_dates();
    let graph = ModelGraph::from_model(model).unwrap();

    // Get entity info via role name should return the dimension's physical table
    let info = graph.get_entity_info("order_date").unwrap();
    assert_eq!(info.name, "order_date"); // Keeps the role name
    assert_eq!(info.physical_table, "date_dim"); // Uses dimension's table
    assert_eq!(info.physical_schema, Some("raw".to_string())); // Schema is separate
    assert_eq!(info.entity_type, super::EntityType::Source);

    // Get entity info via regular name
    let info = graph.get_entity_info("date").unwrap();
    assert_eq!(info.name, "date");
    assert_eq!(info.physical_table, "date_dim");
    assert_eq!(info.physical_schema, Some("raw".to_string()));
}
