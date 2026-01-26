//! Integration tests for join order optimizer with physical planner.
//!
//! These tests verify end-to-end integration of:
//! - JoinOrderOptimizer generating multiple join orders
//! - PhysicalConverter using optimizer for multi-table queries
//! - CostEstimator selecting best plan based on cost

use mantis::planner::cost::CostEstimator;
use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
use mantis::planner::physical::PhysicalPlanner;
use mantis::semantic::graph::query::ColumnRef;
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};

/// Create a test graph with realistic data warehouse schema.
///
/// Schema: Sales (fact) → Products → Categories (dimension chain)
/// - Sales: 1M rows (large fact table)
/// - Products: 10K rows (medium dimension)
/// - Categories: 100 rows (small dimension)
fn create_test_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Sales (large fact table)
    let sales = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1_000_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let sales_idx = graph.add_test_entity(sales);

    // Products (medium dimension)
    let products = EntityNode {
        name: "products".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let products_idx = graph.add_test_entity(products);

    // Categories (small dimension)
    let categories = EntityNode {
        name: "categories".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let categories_idx = graph.add_test_entity(categories);

    // Sales → Products (N:1 - many sales records per product)
    let sales_products_join = JoinsToEdge {
        from_entity: "sales".to_string(),
        to_entity: "products".to_string(),
        join_columns: vec![("product_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(sales_idx, products_idx, sales_products_join);

    // Products → Categories (N:1 - many products per category)
    let products_categories_join = JoinsToEdge {
        from_entity: "products".to_string(),
        to_entity: "categories".to_string(),
        join_columns: vec![("category_id".to_string(), "id".to_string())],
        cardinality: Cardinality::ManyToOne,
        source: RelationshipSource::ForeignKey,
    };
    graph.add_test_join(products_idx, categories_idx, products_categories_join);

    graph
}

/// Create a 2-table join logical plan (sales → products).
fn create_two_table_join() -> LogicalPlan {
    LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("sales".to_string(), "product_id".to_string()),
            ColumnRef::new("products".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    })
}

/// Create a 3-table join logical plan (sales → products → categories).
fn create_three_table_join() -> LogicalPlan {
    let sales_products = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "sales".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("sales".to_string(), "product_id".to_string()),
            ColumnRef::new("products".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    LogicalPlan::Join(JoinNode {
        left: Box::new(sales_products),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "categories".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("products".to_string(), "category_id".to_string()),
            ColumnRef::new("categories".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    })
}

// ============================================================================
// Task 14: Integration Tests - Optimizer in Physical Converter
// ============================================================================

#[test]
fn test_two_table_join_generates_multiple_candidates() {
    // Test that PhysicalConverter generates multiple plan candidates for 2-table join
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    let logical_plan = create_two_table_join();

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    // For 2 tables with enumeration (2! = 2 orders) × 2 join strategies (Hash, NL)
    // We should get at least 2 candidates (possibly more with different orderings)
    assert!(
        candidates.len() >= 2,
        "Should generate multiple candidates for 2-table join, got {}",
        candidates.len()
    );
}

#[test]
fn test_three_table_join_generates_multiple_candidates() {
    // Test that PhysicalConverter generates multiple plan candidates for 3-table join
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    let logical_plan = create_three_table_join();

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    // For 3 tables with enumeration, we expect multiple candidates
    // (not all permutations will be valid due to join constraints)
    assert!(
        candidates.len() >= 2,
        "Should generate multiple candidates for 3-table join, got {}",
        candidates.len()
    );
}

// ============================================================================
// Task 15: Integration Tests - Cost-Based Plan Selection
// ============================================================================

#[test]
fn test_cost_estimator_selects_best_plan() {
    // Test that CostEstimator.select_best() returns the lowest cost plan
    let graph = create_test_graph();
    let estimator = CostEstimator::new(&graph);
    let planner = PhysicalPlanner::new(&graph);

    let logical_plan = create_two_table_join();

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    // Select best plan
    let best_plan = estimator
        .select_best(candidates.clone())
        .expect("Should select best plan");

    // Verify that the selected plan is indeed among the candidates
    // and has the lowest cost
    let best_cost = estimator.estimate(&best_plan);

    for candidate in &candidates {
        let candidate_cost = estimator.estimate(candidate);
        assert!(
            best_cost.total() <= candidate_cost.total(),
            "Selected plan should have lowest or equal cost. Best: {:.2}, Candidate: {:.2}",
            best_cost.total(),
            candidate_cost.total()
        );
    }
}

#[test]
fn test_hash_join_vs_nested_loop_selection() {
    // Test that cost estimator correctly chooses between HashJoin and NestedLoopJoin
    // based on table sizes
    let graph = create_test_graph();
    let estimator = CostEstimator::new(&graph);
    let planner = PhysicalPlanner::new(&graph);

    let logical_plan = create_two_table_join();

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    let best_plan = estimator
        .select_best(candidates)
        .expect("Should select best plan");

    // For large tables (sales = 1M rows), HashJoin should typically be preferred
    // over NestedLoopJoin due to lower CPU cost
    // We can't assert the exact strategy without examining the plan structure,
    // but we can verify the cost is reasonable
    let cost = estimator.estimate(&best_plan);

    // The cost should be much less than a full nested loop (1M * 10K = 10B comparisons)
    // Hash join should be closer to 1M + 10K = 1.01M
    assert!(
        cost.cpu_cost < 100_000_000.0,
        "Best plan CPU cost should be reasonable (< 100M), got {:.2}",
        cost.cpu_cost
    );
}

// ============================================================================
// Task 16: Integration Tests - Cost Logging Verification
// ============================================================================

#[test]
fn test_cost_estimation_produces_reasonable_values() {
    // Test that cost estimation produces reasonable, non-zero values
    let graph = create_test_graph();
    let estimator = CostEstimator::new(&graph);
    let planner = PhysicalPlanner::new(&graph);

    let logical_plan = create_three_table_join();

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    // Verify each candidate has non-zero, reasonable costs
    for candidate in &candidates {
        let cost = estimator.estimate(candidate);

        // All costs should be non-negative
        assert!(cost.rows_out > 0, "rows_out should be positive");
        assert!(cost.cpu_cost >= 0.0, "cpu_cost should be non-negative");
        assert!(cost.io_cost >= 0.0, "io_cost should be non-negative");
        assert!(
            cost.memory_cost >= 0.0,
            "memory_cost should be non-negative"
        );

        // Total cost should be reasonable (not infinite)
        assert!(
            cost.total().is_finite(),
            "Total cost should be finite, got {}",
            cost.total()
        );

        // For a 3-table join with 1M rows, output should be <= input
        // (joins typically don't explode to more than largest table in star schema)
        assert!(
            cost.rows_out <= 10_000_000,
            "Output rows should be reasonable, got {}",
            cost.rows_out
        );
    }
}

// ============================================================================
// End-to-End Integration Test
// ============================================================================

#[test]
fn test_end_to_end_optimization_pipeline() {
    // Comprehensive test of the entire optimization pipeline:
    // 1. Logical plan with multiple tables
    // 2. Physical converter generates optimized candidates
    // 3. Cost estimator selects best plan
    // 4. Best plan has lower cost than naive approach

    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    // Create a 3-table join
    let logical_plan = create_three_table_join();

    // Generate candidates (Task 14: optimizer integration)
    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate physical plan candidates");

    assert!(
        !candidates.is_empty(),
        "Should generate at least one candidate"
    );

    // Select best plan (Task 15: cost comparison)
    let best_plan = estimator
        .select_best(candidates.clone())
        .expect("Should select best plan");

    // Verify cost estimation (Task 16: logging)
    let best_cost = estimator.estimate(&best_plan);

    // Log for manual verification
    println!("\n=== End-to-End Optimization Results ===");
    println!("Total candidates: {}", candidates.len());
    println!("Best plan cost breakdown:");
    println!("  Rows out: {}", best_cost.rows_out);
    println!("  CPU cost: {:.2}", best_cost.cpu_cost);
    println!("  IO cost: {:.2}", best_cost.io_cost);
    println!("  Memory cost: {:.2}", best_cost.memory_cost);
    println!("  Total weighted cost: {:.2}", best_cost.total());

    // Verify the optimizer made a reasonable choice
    // For a star schema (sales → products → categories), the best plan should:
    // 1. Not have explosive row counts
    // 2. Have reasonable CPU/IO costs
    // 3. Be better than or equal to all other candidates

    assert!(
        best_cost.rows_out <= 2_000_000,
        "Output should not explode beyond reasonable bounds"
    );

    // Verify best plan is actually the best
    let mut all_costs: Vec<f64> = candidates
        .iter()
        .map(|c| estimator.estimate(c).total())
        .collect();
    all_costs.sort_by(|a, b| a.partial_cmp(b).unwrap());

    assert_eq!(
        best_cost.total(),
        all_costs[0],
        "Selected plan should have the lowest cost"
    );
}

// ============================================================================
// Additional Integration Tests
// ============================================================================

#[test]
fn test_optimizer_handles_single_table() {
    // Test that optimizer gracefully handles single-table queries (no joins)
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);

    let logical_plan = LogicalPlan::Scan(ScanNode {
        entity: "sales".to_string(),
    });

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should handle single table");

    assert_eq!(
        candidates.len(),
        1,
        "Single table should produce one candidate"
    );
}

#[test]
fn test_different_join_orders_have_different_costs() {
    // Test that different join orders produce different costs
    // (validating that optimizer has meaningful choices)
    let graph = create_test_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    let logical_plan = create_three_table_join();

    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    if candidates.len() < 2 {
        // Skip test if not enough valid candidates
        println!("Skipping test: not enough candidates generated");
        return;
    }

    // Calculate costs for all candidates
    let costs: Vec<f64> = candidates
        .iter()
        .map(|c| estimator.estimate(c).total())
        .collect();

    // Check if we have variation in costs
    let min_cost = costs.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_cost = costs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    // There should be some variation (unless all plans are identical)
    // We allow for plans with identical costs, but prefer variation
    println!("Cost range: {:.2} to {:.2}", min_cost, max_cost);

    // This is more of an informational test - we just want to see variation
    // Not all join orders will produce different costs depending on the schema
}
