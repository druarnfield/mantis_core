use mantis::planner::physical::{PhysicalPlan, TableScanStrategy};

#[test]
fn test_table_scan_node_creation() {
    let scan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    assert!(matches!(scan, PhysicalPlan::TableScan { .. }));
}

#[test]
fn test_hash_join_node_creation() {
    // Test will be implemented in later tasks
}

#[test]
fn test_convert_scan_to_physical() {
    use mantis::planner::logical::{LogicalPlan, ScanNode};
    use mantis::planner::physical::PhysicalPlanner;
    use mantis::semantic::graph::UnifiedGraph;

    let graph = UnifiedGraph::new();
    let logical = LogicalPlan::Scan(ScanNode {
        entity: "sales".to_string(),
    });

    let planner = PhysicalPlanner::new(&graph);
    let candidates = planner.generate_candidates(&logical).unwrap();

    assert!(!candidates.is_empty());
    assert!(matches!(candidates[0], PhysicalPlan::TableScan { .. }));
}

// ============================================================================
// Task 15: Integration of DP Optimizer into PhysicalConverter
// ============================================================================

#[test]
fn test_physical_converter_supports_optimizer_strategy_selection() {
    use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
    use mantis::planner::physical::{PhysicalPlanner, PhysicalPlannerConfig};
    use mantis::semantic::graph::{
        query::ColumnRef, Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource,
        SizeCategory, UnifiedGraph,
    };

    let mut graph = UnifiedGraph::new();

    // Create two tables with relationship
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    let customers = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let customers_idx = graph.add_test_entity(customers);

    // Add relationship: orders -> customers (N:1)
    let join_edge = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "customers".to_string(),
        join_columns: vec![("customer_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, customers_idx, join_edge);

    // Create a simple join plan
    let join_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "customer_id".to_string()),
            ColumnRef::new("customers".to_string(), "id".to_string()),
        )]),
        join_type: JoinType::Inner,
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Test that we can create planner with DP strategy config
    use mantis::planner::physical::join_optimizer::OptimizerStrategy;
    let config = PhysicalPlannerConfig {
        optimizer_strategy: OptimizerStrategy::DP,
    };

    let planner = PhysicalPlanner::with_config(&graph, config);
    let physical_plans = planner.generate_candidates(&join_plan);

    assert!(
        physical_plans.is_ok(),
        "Should successfully generate physical plans with DP strategy: {:?}",
        physical_plans.as_ref().err()
    );
    let plans = physical_plans.unwrap();
    assert!(!plans.is_empty(), "Should generate at least one plan");
}

#[test]
fn test_physical_converter_uses_dp_optimizer_for_small_queries() {
    use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
    use mantis::planner::physical::PhysicalPlanner;
    use mantis::semantic::graph::{
        query::ColumnRef, Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource,
        SizeCategory, UnifiedGraph,
    };

    let mut graph = UnifiedGraph::new();

    // Create two tables with relationship
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    let customers = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let customers_idx = graph.add_test_entity(customers);

    // Add relationship: orders -> customers (N:1)
    let join_edge = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "customers".to_string(),
        join_columns: vec![("customer_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, customers_idx, join_edge);

    // Create a simple join plan
    let join_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "customer_id".to_string()),
            ColumnRef::new("customers".to_string(), "id".to_string()),
        )]),
        join_type: JoinType::Inner,
        cardinality: Some(Cardinality::ManyToOne),
    });

    // PhysicalConverter should generate physical plans using optimizer
    let planner = PhysicalPlanner::new(&graph);
    let physical_plans = planner.generate_candidates(&join_plan);

    assert!(
        physical_plans.is_ok(),
        "Should successfully generate physical plans"
    );
    let plans = physical_plans.unwrap();
    assert!(!plans.is_empty(), "Should generate at least one plan");

    // Verify it's a join plan (HashJoin or NestedLoopJoin)
    let has_join = plans.iter().any(|p| {
        matches!(
            p,
            PhysicalPlan::HashJoin { .. } | PhysicalPlan::NestedLoopJoin { .. }
        )
    });
    assert!(has_join, "Should generate join plans");
}

#[test]
fn test_physical_converter_handles_three_table_join() {
    use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
    use mantis::planner::physical::PhysicalPlanner;
    use mantis::semantic::graph::{
        query::ColumnRef, Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource,
        SizeCategory, UnifiedGraph,
    };

    let mut graph = UnifiedGraph::new();

    // Create three tables: orders -> customers -> regions
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    let customers = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let customers_idx = graph.add_test_entity(customers);

    let regions = EntityNode {
        name: "regions".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(10),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let regions_idx = graph.add_test_entity(regions);

    // Add relationships
    let orders_customers = JoinsToEdge {
        from_entity: "orders".to_string(),
        to_entity: "customers".to_string(),
        join_columns: vec![("customer_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(orders_idx, customers_idx, orders_customers);

    let customers_regions = JoinsToEdge {
        from_entity: "customers".to_string(),
        to_entity: "regions".to_string(),
        join_columns: vec![("region_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(customers_idx, regions_idx, customers_regions);

    // Create a 3-table join plan
    let orders_customers_join = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "customer_id".to_string()),
            ColumnRef::new("customers".to_string(), "id".to_string()),
        )]),
        join_type: JoinType::Inner,
        cardinality: Some(Cardinality::ManyToOne),
    });

    let full_join = LogicalPlan::Join(JoinNode {
        left: Box::new(orders_customers_join),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "regions".to_string(),
        })),
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("customers".to_string(), "region_id".to_string()),
            ColumnRef::new("regions".to_string(), "id".to_string()),
        )]),
        join_type: JoinType::Inner,
        cardinality: Some(Cardinality::ManyToOne),
    });

    // PhysicalConverter should handle 3-table joins using DP optimizer
    let planner = PhysicalPlanner::new(&graph);
    let physical_plans = planner.generate_candidates(&full_join);

    assert!(
        physical_plans.is_ok(),
        "Should successfully generate physical plans for 3 tables"
    );
    let plans = physical_plans.unwrap();
    assert!(
        !plans.is_empty(),
        "Should generate at least one plan for 3 tables"
    );
}
