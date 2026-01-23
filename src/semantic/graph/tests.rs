//! Unit tests for the unified semantic graph construction.

use std::collections::HashMap;

use crate::dsl::ast::{
    Atom, AtomType, Attribute, Calendar, CalendarBody, DataType, Dimension, GeneratedCalendar,
    GrainLevel, Item, Measure, MeasureBlock, Model, PhysicalCalendar, SqlExpr, Table,
};
use crate::dsl::span::{Span, Spanned};
use crate::metadata::ColumnStats;
use crate::semantic::inference::InferredRelationship;

use super::{EntityType, GraphNode, SizeCategory, UnifiedGraph};

/// Helper to create a spanned value.
fn spanned<T>(value: T) -> Spanned<T> {
    Spanned {
        value,
        span: Span::new(0, 0),
    }
}

#[test]
fn test_create_entity_nodes() {
    // Create a model with a table and a dimension
    let model = Model {
        defaults: None,
        items: vec![
            spanned(Item::Table(Table {
                name: spanned("sales".to_string()),
                source: spanned("dbo.fact_sales".to_string()),
                atoms: vec![],
                times: vec![],
                slicers: vec![],
            })),
            spanned(Item::Dimension(Dimension {
                name: spanned("customers".to_string()),
                source: spanned("dbo.dim_customers".to_string()),
                key: spanned("customer_id".to_string()),
                attributes: vec![],
                drill_paths: vec![],
            })),
        ],
    };

    let relationships: Vec<InferredRelationship> = vec![];
    let stats: HashMap<(String, String), ColumnStats> = HashMap::new();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify entities were created
    assert_eq!(graph.entity_index.len(), 2);
    assert!(graph.entity_index.contains_key("sales"));
    assert!(graph.entity_index.contains_key("customers"));

    // Verify entity types
    let sales_idx = graph.entity_index["sales"];
    if let GraphNode::Entity(entity) = &graph.graph[sales_idx] {
        assert_eq!(entity.name, "sales");
        assert_eq!(entity.entity_type, EntityType::Fact);
        assert_eq!(entity.physical_name, Some("dbo.fact_sales".to_string()));
    } else {
        panic!("Expected Entity node");
    }

    let customers_idx = graph.entity_index["customers"];
    if let GraphNode::Entity(entity) = &graph.graph[customers_idx] {
        assert_eq!(entity.name, "customers");
        assert_eq!(entity.entity_type, EntityType::Dimension);
        assert_eq!(entity.physical_name, Some("dbo.dim_customers".to_string()));
    } else {
        panic!("Expected Entity node");
    }
}

#[test]
fn test_create_column_nodes() {
    // Create a model with a table that has atoms
    let model = Model {
        defaults: None,
        items: vec![spanned(Item::Table(Table {
            name: spanned("sales".to_string()),
            source: spanned("dbo.fact_sales".to_string()),
            atoms: vec![
                spanned(Atom {
                    name: spanned("quantity".to_string()),
                    atom_type: spanned(AtomType::Int),
                }),
                spanned(Atom {
                    name: spanned("amount".to_string()),
                    atom_type: spanned(AtomType::Decimal),
                }),
            ],
            times: vec![],
            slicers: vec![],
        }))],
    };

    let relationships: Vec<InferredRelationship> = vec![];

    // Add some column stats
    let mut stats = HashMap::new();
    stats.insert(
        ("sales".to_string(), "quantity".to_string()),
        ColumnStats {
            total_count: 50000,
            distinct_count: 1000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );
    stats.insert(
        ("sales".to_string(), "amount".to_string()),
        ColumnStats {
            total_count: 50000,
            distinct_count: 25000,
            null_count: 100,
            is_unique: false,
            sample_values: vec![],
        },
    );

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify columns were created
    assert_eq!(graph.column_index.len(), 2);
    assert!(graph.column_index.contains_key("sales.quantity"));
    assert!(graph.column_index.contains_key("sales.amount"));

    // Verify column properties
    let quantity_idx = graph.column_index["sales.quantity"];
    if let GraphNode::Column(column) = &graph.graph[quantity_idx] {
        assert_eq!(column.entity, "sales");
        assert_eq!(column.name, "quantity");
        assert!(!column.unique);
    } else {
        panic!("Expected Column node");
    }

    // Verify entity was enriched with row count
    let sales_idx = graph.entity_index["sales"];
    if let GraphNode::Entity(entity) = &graph.graph[sales_idx] {
        assert_eq!(entity.row_count, Some(50000));
        assert_eq!(entity.size_category, SizeCategory::Small);
    } else {
        panic!("Expected Entity node");
    }

    // Verify BELONGS_TO edges were created (2 columns -> 1 entity = 2 edges)
    let entity_idx = graph.entity_index["sales"];
    let edges_to_entity: Vec<_> = graph
        .graph
        .edges_directed(entity_idx, petgraph::Direction::Incoming)
        .collect();
    assert_eq!(edges_to_entity.len(), 2);
}

#[test]
fn test_create_column_nodes_with_dimension_attributes() {
    // Create a model with a dimension that has attributes
    let model = Model {
        defaults: None,
        items: vec![spanned(Item::Dimension(Dimension {
            name: spanned("customers".to_string()),
            source: spanned("dbo.dim_customers".to_string()),
            key: spanned("customer_id".to_string()),
            attributes: vec![
                spanned(Attribute {
                    name: spanned("name".to_string()),
                    data_type: spanned(DataType::String),
                }),
                spanned(Attribute {
                    name: spanned("email".to_string()),
                    data_type: spanned(DataType::String),
                }),
            ],
            drill_paths: vec![],
        }))],
    };

    let relationships: Vec<InferredRelationship> = vec![];
    let stats: HashMap<(String, String), ColumnStats> = HashMap::new();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify columns were created: 1 key + 2 attributes = 3 columns
    assert_eq!(graph.column_index.len(), 3);
    assert!(graph.column_index.contains_key("customers.customer_id"));
    assert!(graph.column_index.contains_key("customers.name"));
    assert!(graph.column_index.contains_key("customers.email"));

    // Verify the key column has correct properties
    let key_idx = graph.column_index["customers.customer_id"];
    if let GraphNode::Column(column) = &graph.graph[key_idx] {
        assert_eq!(column.entity, "customers");
        assert_eq!(column.name, "customer_id");
        assert!(column.primary_key);
        assert!(column.unique);
        assert!(!column.nullable);
    } else {
        panic!("Expected Column node");
    }

    // Verify BELONGS_TO edges (3 columns -> 1 entity = 3 edges)
    let entity_idx = graph.entity_index["customers"];
    let edges_to_entity: Vec<_> = graph
        .graph
        .edges_directed(entity_idx, petgraph::Direction::Incoming)
        .collect();
    assert_eq!(edges_to_entity.len(), 3);
}

#[test]
fn test_create_measure_nodes() {
    // Create a model with a table and measures
    let model = Model {
        defaults: None,
        items: vec![
            spanned(Item::Table(Table {
                name: spanned("sales".to_string()),
                source: spanned("dbo.fact_sales".to_string()),
                atoms: vec![],
                times: vec![],
                slicers: vec![],
            })),
            spanned(Item::MeasureBlock(MeasureBlock {
                table: spanned("sales".to_string()),
                measures: vec![
                    spanned(Measure {
                        name: spanned("total_quantity".to_string()),
                        expr: spanned(SqlExpr::new("SUM(quantity)".to_string(), Span::new(0, 0))),
                        filter: None,
                        null_handling: None,
                    }),
                    spanned(Measure {
                        name: spanned("total_amount".to_string()),
                        expr: spanned(SqlExpr::new("SUM(amount)".to_string(), Span::new(0, 0))),
                        filter: None,
                        null_handling: None,
                    }),
                ],
            })),
        ],
    };

    let relationships: Vec<InferredRelationship> = vec![];
    let stats: HashMap<(String, String), ColumnStats> = HashMap::new();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify measures were created
    assert_eq!(graph.measure_index.len(), 2);
    assert!(graph.measure_index.contains_key("sales.total_quantity"));
    assert!(graph.measure_index.contains_key("sales.total_amount"));

    // Verify measure properties
    let measure_idx = graph.measure_index["sales.total_quantity"];
    if let GraphNode::Measure(measure) = &graph.graph[measure_idx] {
        assert_eq!(measure.name, "total_quantity");
        assert_eq!(measure.entity, "sales");
        assert_eq!(measure.aggregation, "CUSTOM");
        assert_eq!(measure.expression, Some("SUM(quantity)".to_string()));
    } else {
        panic!("Expected Measure node");
    }
}

#[test]
fn test_create_calendar_nodes_physical() {
    // Create a model with a physical calendar
    let model = Model {
        defaults: None,
        items: vec![spanned(Item::Calendar(Calendar {
            name: spanned("dates".to_string()),
            body: spanned(CalendarBody::Physical(PhysicalCalendar {
                source: spanned("dbo.dim_date".to_string()),
                grain_mappings: vec![],
                drill_paths: vec![],
                fiscal_year_start: None,
                week_start: None,
            })),
        }))],
    };

    let relationships: Vec<InferredRelationship> = vec![];
    let stats: HashMap<(String, String), ColumnStats> = HashMap::new();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify calendar was created
    assert_eq!(graph.calendar_index.len(), 1);
    assert!(graph.calendar_index.contains_key("dates"));

    // Verify calendar properties
    let cal_idx = graph.calendar_index["dates"];
    if let GraphNode::Calendar(calendar) = &graph.graph[cal_idx] {
        assert_eq!(calendar.name, "dates");
        assert_eq!(calendar.physical_name, "dbo.dim_date");
    } else {
        panic!("Expected Calendar node");
    }
}

#[test]
fn test_create_calendar_nodes_generated() {
    // Create a model with a generated calendar
    let model = Model {
        defaults: None,
        items: vec![spanned(Item::Calendar(Calendar {
            name: spanned("dates".to_string()),
            body: spanned(CalendarBody::Generated(GeneratedCalendar {
                base_grain: spanned(GrainLevel::Day),
                fiscal: None,
                range: None,
                drill_paths: vec![],
                week_start: None,
            })),
        }))],
    };

    let relationships: Vec<InferredRelationship> = vec![];
    let stats: HashMap<(String, String), ColumnStats> = HashMap::new();

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify calendar was created
    assert_eq!(graph.calendar_index.len(), 1);
    assert!(graph.calendar_index.contains_key("dates"));

    // Verify calendar properties
    let cal_idx = graph.calendar_index["dates"];
    if let GraphNode::Calendar(calendar) = &graph.graph[cal_idx] {
        assert_eq!(calendar.name, "dates");
        assert_eq!(calendar.physical_name, "generated_dates");
        // Grain levels should include day and all coarser grains
        assert!(calendar.grain_levels.len() >= 5); // day, week, month, quarter, year
    } else {
        panic!("Expected Calendar node");
    }
}

#[test]
fn test_size_categories() {
    // Test size category assignment based on row count
    let model = Model {
        defaults: None,
        items: vec![
            spanned(Item::Table(Table {
                name: spanned("small_table".to_string()),
                source: spanned("dbo.small".to_string()),
                atoms: vec![spanned(Atom {
                    name: spanned("value".to_string()),
                    atom_type: spanned(AtomType::Int),
                })],
                times: vec![],
                slicers: vec![],
            })),
            spanned(Item::Table(Table {
                name: spanned("medium_table".to_string()),
                source: spanned("dbo.medium".to_string()),
                atoms: vec![spanned(Atom {
                    name: spanned("value".to_string()),
                    atom_type: spanned(AtomType::Int),
                })],
                times: vec![],
                slicers: vec![],
            })),
            spanned(Item::Table(Table {
                name: spanned("large_table".to_string()),
                source: spanned("dbo.large".to_string()),
                atoms: vec![spanned(Atom {
                    name: spanned("value".to_string()),
                    atom_type: spanned(AtomType::Int),
                })],
                times: vec![],
                slicers: vec![],
            })),
        ],
    };

    let relationships: Vec<InferredRelationship> = vec![];
    let mut stats = HashMap::new();

    // Small table: 50K rows
    stats.insert(
        ("small_table".to_string(), "value".to_string()),
        ColumnStats {
            total_count: 50_000,
            distinct_count: 1000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );

    // Medium table: 5M rows
    stats.insert(
        ("medium_table".to_string(), "value".to_string()),
        ColumnStats {
            total_count: 5_000_000,
            distinct_count: 100_000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );

    // Large table: 50M rows
    stats.insert(
        ("large_table".to_string(), "value".to_string()),
        ColumnStats {
            total_count: 50_000_000,
            distinct_count: 10_000_000,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        },
    );

    let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)
        .expect("Failed to build graph");

    // Verify size categories
    if let GraphNode::Entity(entity) = &graph.graph[graph.entity_index["small_table"]] {
        assert_eq!(entity.size_category, SizeCategory::Small);
    }

    if let GraphNode::Entity(entity) = &graph.graph[graph.entity_index["medium_table"]] {
        assert_eq!(entity.size_category, SizeCategory::Medium);
    }

    if let GraphNode::Entity(entity) = &graph.graph[graph.entity_index["large_table"]] {
        assert_eq!(entity.size_category, SizeCategory::Large);
    }
}
