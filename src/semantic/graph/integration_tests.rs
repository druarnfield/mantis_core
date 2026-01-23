//! End-to-end integration tests for the unified semantic graph.
//!
//! These tests create realistic test models and verify that the entire graph
//! construction and query pipeline works correctly.

use crate::dsl::ast::{
    Atom, AtomType, Attribute, CalendarBody, DataType, Dimension, GeneratedCalendar, GrainLevel,
    GrainMapping, Item, Measure, MeasureBlock, Model, Slicer, SlicerKind, SqlExpr, Table,
    TimeBinding,
};
use crate::dsl::Span;
use crate::metadata::ColumnStats;
use crate::semantic::graph::{
    Cardinality, EntityType, GraphBuildResult, SizeCategory, UnifiedGraph,
};
use crate::semantic::inference::{InferredRelationship, RelationshipSource};
use std::collections::HashMap;

/// Helper to create a spanned value at position 0.
fn spanned<T>(value: T) -> crate::dsl::Spanned<T> {
    crate::dsl::Spanned {
        value,
        span: Span::new(0, 0),
    }
}

/// Create a realistic sales & customers test model.
///
/// Model structure:
/// - sales table (fact): sales_id, customer_id, product_id, amount, quantity, order_date
/// - customers dimension: customer_id, customer_name, customer_segment, customer_region
/// - products dimension: product_id, product_name, product_category
/// - fiscal_calendar (generated)
/// - measures: total_revenue, total_quantity, avg_order_value
fn create_sales_model() -> Model {
    // Sales table (fact)
    let sales_table = Item::Table(Table {
        name: spanned("sales".to_string()),
        source: spanned("dbo.fact_sales".to_string()),
        atoms: vec![
            spanned(Atom {
                name: spanned("amount".to_string()),
                atom_type: spanned(AtomType::Decimal),
            }),
            spanned(Atom {
                name: spanned("quantity".to_string()),
                atom_type: spanned(AtomType::Int),
            }),
        ],
        times: vec![spanned(TimeBinding {
            name: spanned("order_date".to_string()),
            calendar: spanned("fiscal_calendar".to_string()),
            grain: spanned(GrainLevel::Day),
        })],
        slicers: vec![
            spanned(Slicer {
                name: spanned("customer_id".to_string()),
                kind: spanned(SlicerKind::ForeignKey {
                    dimension: "customers".to_string(),
                    key_column: "customer_id".to_string(),
                }),
            }),
            spanned(Slicer {
                name: spanned("product_id".to_string()),
                kind: spanned(SlicerKind::ForeignKey {
                    dimension: "products".to_string(),
                    key_column: "product_id".to_string(),
                }),
            }),
        ],
    });

    // Customers dimension
    let customers_dimension = Item::Dimension(Dimension {
        name: spanned("customers".to_string()),
        source: spanned("dbo.dim_customers".to_string()),
        key: spanned("customer_id".to_string()),
        attributes: vec![
            spanned(Attribute {
                name: spanned("customer_name".to_string()),
                data_type: spanned(DataType::String),
            }),
            spanned(Attribute {
                name: spanned("customer_segment".to_string()),
                data_type: spanned(DataType::String),
            }),
            spanned(Attribute {
                name: spanned("customer_region".to_string()),
                data_type: spanned(DataType::String),
            }),
        ],
        drill_paths: vec![],
    });

    // Products dimension
    let products_dimension = Item::Dimension(Dimension {
        name: spanned("products".to_string()),
        source: spanned("dbo.dim_products".to_string()),
        key: spanned("product_id".to_string()),
        attributes: vec![
            spanned(Attribute {
                name: spanned("product_name".to_string()),
                data_type: spanned(DataType::String),
            }),
            spanned(Attribute {
                name: spanned("product_category".to_string()),
                data_type: spanned(DataType::String),
            }),
        ],
        drill_paths: vec![],
    });

    // Fiscal calendar (generated)
    let fiscal_calendar = Item::Calendar(crate::dsl::ast::Calendar {
        name: spanned("fiscal_calendar".to_string()),
        body: spanned(CalendarBody::Generated(GeneratedCalendar {
            base_grain: spanned(GrainLevel::Day),
            start_date: spanned("2020-01-01".to_string()),
            end_date: spanned("2026-12-31".to_string()),
        })),
    });

    // Measures for sales
    let sales_measures = Item::MeasureBlock(MeasureBlock {
        table: spanned("sales".to_string()),
        measures: vec![
            spanned(Measure {
                name: spanned("total_revenue".to_string()),
                expr: spanned(SqlExpr::new("SUM(@amount)".to_string(), Span::new(0, 0))),
                filter: None,
                null_handling: None,
            }),
            spanned(Measure {
                name: spanned("total_quantity".to_string()),
                expr: spanned(SqlExpr::new("SUM(@quantity)".to_string(), Span::new(0, 0))),
                filter: None,
                null_handling: None,
            }),
            spanned(Measure {
                name: spanned("avg_order_value".to_string()),
                expr: spanned(SqlExpr::new("AVG(@amount)".to_string(), Span::new(0, 0))),
                filter: None,
                null_handling: None,
            }),
        ],
    });

    Model {
        defaults: None,
        items: vec![
            spanned(sales_table),
            spanned(customers_dimension),
            spanned(products_dimension),
            spanned(fiscal_calendar),
            spanned(sales_measures),
        ],
    }
}

/// Create test relationships for the sales model.
fn create_sales_relationships() -> Vec<InferredRelationship> {
    vec![
        // sales.customer_id -> customers.customer_id
        InferredRelationship {
            from_schema: "dbo".to_string(),
            from_table: "sales".to_string(),
            from_column: "customer_id".to_string(),
            to_schema: "dbo".to_string(),
            to_table: "customers".to_string(),
            to_column: "customer_id".to_string(),
            cardinality: Cardinality::ManyToOne,
            confidence: 0.95,
            source: RelationshipSource::ForeignKey,
        },
        // sales.product_id -> products.product_id
        InferredRelationship {
            from_schema: "dbo".to_string(),
            from_table: "sales".to_string(),
            from_column: "product_id".to_string(),
            to_schema: "dbo".to_string(),
            to_table: "products".to_string(),
            to_column: "product_id".to_string(),
            cardinality: Cardinality::ManyToOne,
            confidence: 0.95,
            source: RelationshipSource::ForeignKey,
        },
    ]
}

/// Create test statistics for the sales model.
fn create_sales_statistics() -> HashMap<(String, String), ColumnStats> {
    let mut stats = HashMap::new();

    // Sales table stats (1M rows - large fact table)
    stats.insert(
        ("sales".to_string(), "amount".to_string()),
        ColumnStats {
            total_count: 1_000_000,
            distinct_count: 950_000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );
    stats.insert(
        ("sales".to_string(), "quantity".to_string()),
        ColumnStats {
            total_count: 1_000_000,
            distinct_count: 100,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );

    // Customers dimension stats (10K rows - small dimension)
    stats.insert(
        ("customers".to_string(), "customer_id".to_string()),
        ColumnStats {
            total_count: 10_000,
            distinct_count: 10_000,
            null_count: 0,
            is_unique: true,
            sample_values: vec![],
        },
    );
    stats.insert(
        ("customers".to_string(), "customer_name".to_string()),
        ColumnStats {
            total_count: 10_000,
            distinct_count: 10_000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );

    // Products dimension stats (5K rows - small dimension)
    stats.insert(
        ("products".to_string(), "product_id".to_string()),
        ColumnStats {
            total_count: 5_000,
            distinct_count: 5_000,
            null_count: 0,
            is_unique: true,
            sample_values: vec![],
        },
    );
    stats.insert(
        ("products".to_string(), "product_name".to_string()),
        ColumnStats {
            total_count: 5_000,
            distinct_count: 5_000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );

    stats
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_end_to_end_graph_construction() -> GraphBuildResult<()> {
    // Create test model
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    // Build graph
    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Verify entities were created
    assert!(
        graph.entity_index.contains_key("sales"),
        "Sales entity should exist"
    );
    assert!(
        graph.entity_index.contains_key("customers"),
        "Customers entity should exist"
    );
    assert!(
        graph.entity_index.contains_key("products"),
        "Products entity should exist"
    );

    // Verify columns were created
    assert!(
        graph.column_index.contains_key("sales.amount"),
        "sales.amount column should exist"
    );
    assert!(
        graph.column_index.contains_key("sales.quantity"),
        "sales.quantity column should exist"
    );
    assert!(
        graph.column_index.contains_key("customers.customer_id"),
        "customers.customer_id column should exist"
    );
    assert!(
        graph.column_index.contains_key("customers.customer_name"),
        "customers.customer_name column should exist"
    );

    // Verify measures were created
    assert!(
        graph.measure_index.contains_key("sales.total_revenue"),
        "sales.total_revenue measure should exist"
    );
    assert!(
        graph.measure_index.contains_key("sales.total_quantity"),
        "sales.total_quantity measure should exist"
    );
    assert!(
        graph.measure_index.contains_key("sales.avg_order_value"),
        "sales.avg_order_value measure should exist"
    );

    // Verify calendar was created
    assert!(
        graph.calendar_index.contains_key("fiscal_calendar"),
        "fiscal_calendar should exist"
    );

    Ok(())
}

#[test]
fn test_entity_size_categories() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Get entity nodes and verify size categories
    use crate::semantic::graph::GraphNode;

    let sales_idx = graph.entity_index.get("sales").unwrap();
    if let Some(GraphNode::Entity(entity)) = graph.graph.node_weight(*sales_idx) {
        assert_eq!(
            entity.size_category,
            SizeCategory::Large,
            "Sales should be categorized as Large (1M rows)"
        );
        assert_eq!(
            entity.row_count,
            Some(1_000_000),
            "Sales should have 1M rows"
        );
        assert_eq!(
            entity.entity_type,
            EntityType::Fact,
            "Sales should be a Fact entity"
        );
    } else {
        panic!("Sales entity not found in graph");
    }

    let customers_idx = graph.entity_index.get("customers").unwrap();
    if let Some(GraphNode::Entity(entity)) = graph.graph.node_weight(*customers_idx) {
        assert_eq!(
            entity.size_category,
            SizeCategory::Small,
            "Customers should be categorized as Small (10K rows)"
        );
        assert_eq!(
            entity.row_count,
            Some(10_000),
            "Customers should have 10K rows"
        );
        assert_eq!(
            entity.entity_type,
            EntityType::Dimension,
            "Customers should be a Dimension entity"
        );
    } else {
        panic!("Customers entity not found in graph");
    }

    Ok(())
}

#[test]
fn test_find_join_path() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Find path from sales to customers
    let path = graph.find_path("sales", "customers")?;
    assert_eq!(path.steps.len(), 1, "Path should have exactly 1 step");
    assert_eq!(path.steps[0].from, "sales");
    assert_eq!(path.steps[0].to, "customers");
    assert_eq!(path.steps[0].cardinality, "N:1"); // ManyToOne

    // Find path from sales to products
    let path = graph.find_path("sales", "products")?;
    assert_eq!(path.steps.len(), 1, "Path should have exactly 1 step");
    assert_eq!(path.steps[0].from, "sales");
    assert_eq!(path.steps[0].to, "products");

    Ok(())
}

#[test]
fn test_safe_path_validation() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // ManyToOne join from sales to customers should be safe
    let result = graph.validate_safe_path("sales", "customers");
    assert!(result.is_ok(), "ManyToOne join should be safe (no fan-out)");

    // OneToMany join from customers to sales would be unsafe (if reversed)
    // Note: We can't test this direction easily without adding a reverse relationship
    // In a real scenario, this would be detected

    Ok(())
}

#[test]
fn test_required_columns_for_measures() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Test total_revenue measure dependencies
    let columns = graph.required_columns("sales.total_revenue")?;
    assert_eq!(columns.len(), 1, "total_revenue should depend on 1 column");
    assert_eq!(columns[0].entity, "sales");
    assert_eq!(columns[0].column, "amount");

    // Test total_quantity measure dependencies
    let columns = graph.required_columns("sales.total_quantity")?;
    assert_eq!(columns.len(), 1, "total_quantity should depend on 1 column");
    assert_eq!(columns[0].entity, "sales");
    assert_eq!(columns[0].column, "quantity");

    // Test avg_order_value measure dependencies
    let columns = graph.required_columns("sales.avg_order_value")?;
    assert_eq!(
        columns.len(),
        1,
        "avg_order_value should depend on 1 column"
    );
    assert_eq!(columns[0].entity, "sales");
    assert_eq!(columns[0].column, "amount");

    Ok(())
}

#[test]
fn test_grain_inference() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Infer grain from sales, customers, products
    let grain = graph.infer_grain(&["sales", "customers", "products"])?;
    assert_eq!(
        grain, "sales",
        "Sales should be the grain (most rows = most detailed)"
    );

    // Infer grain from just dimensions
    let grain = graph.infer_grain(&["customers", "products"])?;
    assert_eq!(
        grain, "customers",
        "Customers should be the grain (more rows than products)"
    );

    Ok(())
}

#[test]
fn test_join_strategy_recommendation() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Get join path from sales to customers
    let path = graph.find_path("sales", "customers")?;

    // Get join strategy recommendation
    let strategy = graph.find_best_join_strategy(&path)?;

    assert_eq!(
        strategy.steps.len(),
        1,
        "Strategy should have 1 step for 1-hop path"
    );

    // Sales is Large, Customers is Small
    // Recommendation should be: Customers = build (small), Sales = probe (large)
    use crate::semantic::graph::query::JoinHint;
    let step = &strategy.steps[0];

    // Left side (sales) should be probe (large table)
    assert!(
        matches!(step.left_hint, JoinHint::HashJoinProbe),
        "Sales (large) should be probe side"
    );

    // Right side (customers) should be build (small table)
    assert!(
        matches!(step.right_hint, JoinHint::HashJoinBuild),
        "Customers (small) should be build side"
    );

    Ok(())
}

#[test]
fn test_pre_aggregation_decision() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Should we pre-aggregate sales.total_revenue before joining to customers?
    // Sales is Large (1M rows), Customers is Small (10K rows)
    // Answer: YES - pre-aggregate to reduce data before join
    let should_pre_agg = graph.should_aggregate_before_join("sales.total_revenue", "customers")?;
    assert!(
        should_pre_agg,
        "Should pre-aggregate Large fact before joining to Small dimension"
    );

    // Should we pre-aggregate sales.total_revenue before joining to products?
    // Sales is Large (1M rows), Products is Small (5K rows)
    // Answer: YES - same reasoning
    let should_pre_agg = graph.should_aggregate_before_join("sales.total_revenue", "products")?;
    assert!(
        should_pre_agg,
        "Should pre-aggregate Large fact before joining to Small dimension"
    );

    Ok(())
}

#[test]
fn test_column_uniqueness_check() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Primary keys should be unique
    let is_unique = graph.is_column_unique("customers.customer_id")?;
    assert!(
        is_unique,
        "customers.customer_id is a primary key and should be unique"
    );

    let is_unique = graph.is_column_unique("products.product_id")?;
    assert!(
        is_unique,
        "products.product_id is a primary key and should be unique"
    );

    // Regular columns should not be unique (based on our test stats)
    let is_unique = graph.is_column_unique("customers.customer_name")?;
    assert!(
        !is_unique,
        "customers.customer_name is not marked as unique"
    );

    Ok(())
}

#[test]
fn test_hybrid_query_path_with_columns() -> GraphBuildResult<()> {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

    // Hybrid query: find path AND required columns in one call
    let (path, columns) =
        graph.find_path_with_required_columns("sales", "customers", "sales.total_revenue")?;

    // Verify path
    assert_eq!(path.steps.len(), 1, "Should find direct path");
    assert_eq!(path.steps[0].from, "sales");
    assert_eq!(path.steps[0].to, "customers");

    // Verify columns
    assert_eq!(columns.len(), 1, "Measure should depend on 1 column");
    assert_eq!(columns[0].entity, "sales");
    assert_eq!(columns[0].column, "amount");

    Ok(())
}

#[test]
fn test_no_path_found_error() {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats).unwrap();

    // Try to find path between unconnected entities
    // Customers and Products are not directly connected
    let result = graph.find_path("customers", "products");

    assert!(
        result.is_err(),
        "Should fail to find path between unconnected entities"
    );

    use crate::semantic::graph::query::QueryError;
    assert!(
        matches!(result, Err(QueryError::NoPathFound { .. })),
        "Error should be NoPathFound"
    );
}

#[test]
fn test_entity_not_found_error() {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats).unwrap();

    // Try to query non-existent entity
    let result = graph.find_path("sales", "nonexistent");

    assert!(result.is_err(), "Should fail for non-existent entity");

    use crate::semantic::graph::query::QueryError;
    assert!(
        matches!(result, Err(QueryError::EntityNotFound(_))),
        "Error should be EntityNotFound"
    );
}

#[test]
fn test_measure_not_found_error() {
    let model = create_sales_model();
    let relationships = create_sales_relationships();
    let stats = create_sales_statistics();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats).unwrap();

    // Try to query non-existent measure
    let result = graph.required_columns("sales.nonexistent_measure");

    assert!(result.is_err(), "Should fail for non-existent measure");

    use crate::semantic::graph::query::QueryError;
    assert!(
        matches!(result, Err(QueryError::MeasureNotFound(_))),
        "Error should be MeasureNotFound"
    );
}
