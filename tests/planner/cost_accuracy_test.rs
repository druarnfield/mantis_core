//! Cost estimate accuracy validation tests.
//!
//! Task 19: Verify that cost estimates correlate with actual execution characteristics.
//!
//! Since we don't have a full execution engine, we validate that:
//! 1. Cost estimates are based on actual graph metadata (row counts, cardinality)
//! 2. Estimates are within reasonable bounds (not off by orders of magnitude)
//! 3. Relative ordering of plans by cost is correct (cheaper plans truly have better characteristics)

use mantis::planner::cost::CostEstimator;
use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
use mantis::planner::physical::{PhysicalPlan, PhysicalPlanner, TableScanStrategy};
use mantis::semantic::graph::query::ColumnRef;
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};

/// Create a test graph with known, realistic metadata.
fn create_test_graph_with_known_metadata() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Orders: 1M rows (large fact table)
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1_000_000),
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    // Customers: 10K rows (medium dimension)
    let customers = EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(10_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let customers_idx = graph.add_test_entity(customers);

    // Products: 5K rows (medium dimension)
    let products = EntityNode {
        name: "products".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(5_000),
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let products_idx = graph.add_test_entity(products);

    // Orders → Customers (N:1 - many orders per customer)
    graph.add_test_join(
        orders_idx,
        customers_idx,
        JoinsToEdge {
            from_entity: "orders".to_string(),
            to_entity: "customers".to_string(),
            join_columns: vec![("customer_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    // Orders → Products (N:1 - many orders per product)
    graph.add_test_join(
        orders_idx,
        products_idx,
        JoinsToEdge {
            from_entity: "orders".to_string(),
            to_entity: "products".to_string(),
            join_columns: vec![("product_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph
}

// ============================================================================
// Test 1: Verify cost estimates use actual row counts from graph
// ============================================================================

#[test]
fn test_table_scan_cost_reflects_actual_row_count() {
    // PURPOSE: Verify that TableScan cost estimates use actual row counts from UnifiedGraph,
    // not hardcoded defaults.

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);

    // Create table scans for tables with different sizes
    let large_scan = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let medium_scan = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let small_scan = PhysicalPlan::TableScan {
        table: "products".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let large_cost = estimator.estimate(&large_scan);
    let medium_cost = estimator.estimate(&medium_scan);
    let small_cost = estimator.estimate(&small_scan);

    // Verify estimates match actual row counts
    assert_eq!(
        large_cost.rows_out, 1_000_000,
        "Orders table should have 1M rows"
    );
    assert_eq!(
        medium_cost.rows_out, 10_000,
        "Customers table should have 10K rows"
    );
    assert_eq!(
        small_cost.rows_out, 5_000,
        "Products table should have 5K rows"
    );

    // Verify costs scale proportionally with table size
    assert!(
        large_cost.total() > medium_cost.total(),
        "Larger table should have higher cost"
    );
    assert!(
        medium_cost.total() > small_cost.total(),
        "Medium table should have higher cost than small table"
    );

    println!("\n=== Table Scan Cost Accuracy ===");
    println!("Orders (1M rows): {:.2}", large_cost.total());
    println!("Customers (10K rows): {:.2}", medium_cost.total());
    println!("Products (5K rows): {:.2}", small_cost.total());
}

// ============================================================================
// Test 2: Verify join cardinality estimates are within reasonable bounds
// ============================================================================

#[test]
fn test_join_cardinality_estimates_are_reasonable() {
    // PURPOSE: Verify that join cardinality estimates don't explode unreasonably
    // and respect the cardinality metadata (1:1, N:1, etc.)

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);

    // Create a N:1 join: Orders → Customers
    // Expected: Output rows ≈ Orders rows (1M) since it's many-to-one
    let orders_scan = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1_000_000),
    };

    let customers_scan = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(10_000),
    };

    let join_plan = PhysicalPlan::HashJoin {
        left: Box::new(orders_scan),
        right: Box::new(customers_scan),
        on: vec![("customer_id".to_string(), "id".to_string())],
        estimated_rows: None,
    };

    let join_cost = estimator.estimate(&join_plan);

    println!("\n=== Join Cardinality Accuracy (Orders ⋈ Customers) ===");
    println!("Input rows: Orders=1M, Customers=10K");
    println!("Cardinality: N:1 (many orders per customer)");
    println!("Estimated output rows: {}", join_cost.rows_out);

    // For N:1 join, output should be approximately the left side (many side)
    // Allow some tolerance, but should be within 2x of left side
    assert!(
        join_cost.rows_out >= 500_000 && join_cost.rows_out <= 2_000_000,
        "N:1 join output should be close to left side size (1M), got {}",
        join_cost.rows_out
    );

    // Verify output is NOT a cross product (which would be 1M * 10K = 10B)
    assert!(
        join_cost.rows_out < 10_000_000,
        "Join should not produce cross-product cardinality"
    );
}

// ============================================================================
// Test 3: Verify cost estimates correlate with plan characteristics
// ============================================================================

#[test]
fn test_hash_join_vs_nested_loop_cost_correlation() {
    // PURPOSE: Verify that HashJoin and NestedLoopJoin have different cost profiles
    // that correlate with their actual computational characteristics.
    //
    // HashJoin: Lower CPU cost, higher memory cost
    // NestedLoopJoin: Higher CPU cost (O(n*m)), lower memory cost

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);

    let orders_scan = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1_000_000),
    };

    let customers_scan = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(10_000),
    };

    // HashJoin
    let hash_join = PhysicalPlan::HashJoin {
        left: Box::new(orders_scan.clone()),
        right: Box::new(customers_scan.clone()),
        on: vec![("customer_id".to_string(), "id".to_string())],
        estimated_rows: None,
    };

    // NestedLoopJoin (same tables)
    let nested_loop_join = PhysicalPlan::NestedLoopJoin {
        left: Box::new(orders_scan),
        right: Box::new(customers_scan),
        on: vec![("customer_id".to_string(), "id".to_string())],
        estimated_rows: None,
    };

    let hash_cost = estimator.estimate(&hash_join);
    let nl_cost = estimator.estimate(&nested_loop_join);

    println!("\n=== Hash Join vs Nested Loop Cost Comparison ===");
    println!("HashJoin:");
    println!("  CPU: {:.2}", hash_cost.cpu_cost);
    println!("  Memory: {:.2}", hash_cost.memory_cost);
    println!("  Total: {:.2}", hash_cost.total());
    println!("\nNestedLoopJoin:");
    println!("  CPU: {:.2}", nl_cost.cpu_cost);
    println!("  Memory: {:.2}", nl_cost.memory_cost);
    println!("  Total: {:.2}", nl_cost.total());

    // ASSERTION 1: NestedLoop should have MUCH higher CPU cost (O(n*m) = 1M * 10K = 10B)
    assert!(
        nl_cost.cpu_cost > hash_cost.cpu_cost * 100.0,
        "NestedLoop CPU cost should be much higher than HashJoin. NL: {:.2}, Hash: {:.2}",
        nl_cost.cpu_cost,
        hash_cost.cpu_cost
    );

    // ASSERTION 2: HashJoin should have higher memory cost (stores hash table)
    assert!(
        hash_cost.memory_cost > nl_cost.memory_cost,
        "HashJoin should use more memory than NestedLoop. Hash: {:.2}, NL: {:.2}",
        hash_cost.memory_cost,
        nl_cost.memory_cost
    );

    // ASSERTION 3: Overall, HashJoin should be cheaper (lower total cost)
    assert!(
        hash_cost.total() < nl_cost.total(),
        "HashJoin should have lower total cost than NestedLoop for large tables"
    );
}

// ============================================================================
// Test 4: Verify estimates are within reasonable bounds (not off by orders of magnitude)
// ============================================================================

#[test]
fn test_cost_estimates_are_within_reasonable_bounds() {
    // PURPOSE: Verify that cost estimates are reasonable and not wildly off.
    // We use graph metadata as "ground truth" and check that estimates are within 2x.

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);
    let planner = PhysicalPlanner::new(&graph);

    // Create a simple 2-table join logical plan
    let logical_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "customer_id".to_string()),
            ColumnRef::new("customers".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Generate physical plan
    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    let best_plan = estimator
        .select_best(candidates)
        .expect("Should select best plan");

    let cost = estimator.estimate(&best_plan);

    // GROUND TRUTH: N:1 join should produce ~1M rows (same as orders table)
    let expected_rows = 1_000_000;

    println!("\n=== Cost Estimate Bounds Check ===");
    println!("Expected output rows: {}", expected_rows);
    println!("Estimated output rows: {}", cost.rows_out);
    println!(
        "Accuracy: {:.2}x",
        (cost.rows_out as f64) / (expected_rows as f64)
    );

    // ASSERTION: Estimate should be within 2x of ground truth
    let ratio = (cost.rows_out as f64) / (expected_rows as f64);
    assert!(
        ratio >= 0.5 && ratio <= 2.0,
        "Row estimate should be within 2x of expected. Expected: {}, Got: {} (ratio: {:.2}x)",
        expected_rows,
        cost.rows_out,
        ratio
    );

    // Verify CPU cost is proportional to data size
    // For a join of 1M + 10K rows, CPU cost should be in the millions
    // (not billions, which would indicate cross product)
    assert!(
        cost.cpu_cost >= 1_000_000.0 && cost.cpu_cost < 100_000_000.0,
        "CPU cost should be reasonable for join size. Got: {:.2}",
        cost.cpu_cost
    );
}

// ============================================================================
// Test 5: Verify relative ordering of plans by cost is correct
// ============================================================================

#[test]
fn test_relative_cost_ordering_is_correct() {
    // PURPOSE: Verify that when comparing multiple plans, the cost estimator
    // correctly identifies which plan has better characteristics.

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);

    // Plan A: Scan large table (Orders - 1M rows)
    let plan_a = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    // Plan B: Scan small table (Products - 5K rows)
    let plan_b = PhysicalPlan::TableScan {
        table: "products".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let cost_a = estimator.estimate(&plan_a);
    let cost_b = estimator.estimate(&plan_b);

    // ASSERTION: Smaller table scan should have lower cost
    assert!(
        cost_b.total() < cost_a.total(),
        "Smaller table scan should be cheaper. Products (5K): {:.2}, Orders (1M): {:.2}",
        cost_b.total(),
        cost_a.total()
    );

    // Verify the ratio is proportional to table sizes
    let size_ratio = 1_000_000.0 / 5_000.0; // 200x
    let cost_ratio = cost_a.total() / cost_b.total();

    println!("\n=== Relative Cost Ordering ===");
    println!("Size ratio (Orders/Products): {:.2}x", size_ratio);
    println!("Cost ratio (Orders/Products): {:.2}x", cost_ratio);

    // Cost ratio should be close to size ratio (within same order of magnitude)
    assert!(
        cost_ratio >= size_ratio * 0.5 && cost_ratio <= size_ratio * 2.0,
        "Cost ratio should be proportional to size ratio"
    );
}

// ============================================================================
// Test 6: Verify cost estimates improve with index scans
// ============================================================================

#[test]
fn test_index_scan_has_lower_cost_than_full_scan() {
    // PURPOSE: Verify that IndexScan strategy produces lower cost estimates than FullScan.

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);

    let full_scan = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: None,
    };

    let index_scan = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::IndexScan {
            index: "idx_orders_customer".to_string(),
        },
        estimated_rows: None,
    };

    let full_cost = estimator.estimate(&full_scan);
    let index_cost = estimator.estimate(&index_scan);

    println!("\n=== Index Scan vs Full Scan ===");
    println!("Full Scan IO cost: {:.2}", full_cost.io_cost);
    println!("Index Scan IO cost: {:.2}", index_cost.io_cost);

    // ASSERTION: Index scan should have lower IO cost (typically 10% of full scan)
    assert!(
        index_cost.io_cost < full_cost.io_cost,
        "Index scan should have lower IO cost than full scan"
    );

    // Verify IO cost reduction is significant (at least 5x better)
    let io_improvement = full_cost.io_cost / index_cost.io_cost;
    assert!(
        io_improvement >= 5.0,
        "Index scan should significantly reduce IO cost. Improvement: {:.2}x",
        io_improvement
    );
}

// ============================================================================
// Test 7: End-to-end accuracy validation
// ============================================================================

#[test]
fn test_end_to_end_cost_accuracy() {
    // PURPOSE: Comprehensive test that validates cost accuracy across
    // a realistic multi-table query plan.

    let graph = create_test_graph_with_known_metadata();
    let estimator = CostEstimator::new(&graph);
    let planner = PhysicalPlanner::new(&graph);

    // Create a 3-table join: Orders → Customers, Orders → Products
    let orders_customers = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "customer_id".to_string()),
            ColumnRef::new("customers".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    let three_table_join = LogicalPlan::Join(JoinNode {
        left: Box::new(orders_customers),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "product_id".to_string()),
            ColumnRef::new("products".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Generate and select best plan
    let candidates = planner
        .generate_candidates(&three_table_join)
        .expect("Should generate candidates");

    let best_plan = estimator
        .select_best(candidates)
        .expect("Should select best plan");

    let cost = estimator.estimate(&best_plan);

    println!("\n=== End-to-End Cost Accuracy ===");
    println!("3-table join: Orders ⋈ Customers ⋈ Products");
    println!("Input sizes: Orders=1M, Customers=10K, Products=5K");
    println!("Estimated output rows: {}", cost.rows_out);
    println!("Total cost: {:.2}", cost.total());
    println!("Breakdown:");
    println!("  CPU: {:.2}", cost.cpu_cost);
    println!("  IO: {:.2}", cost.io_cost);
    println!("  Memory: {:.2}", cost.memory_cost);

    // For two N:1 joins starting from Orders, output should be ~1M rows
    // (joins don't expand the result set in star schema)
    assert!(
        cost.rows_out >= 500_000 && cost.rows_out <= 2_000_000,
        "Output rows should be close to fact table size. Got: {}",
        cost.rows_out
    );

    // All cost components should be positive and finite
    assert!(cost.cpu_cost > 0.0 && cost.cpu_cost.is_finite());
    assert!(cost.io_cost > 0.0 && cost.io_cost.is_finite());
    assert!(cost.memory_cost >= 0.0 && cost.memory_cost.is_finite());
    assert!(cost.total().is_finite());

    // Total cost should reflect processing 1M+ rows
    assert!(
        cost.total() >= 1_000_000.0,
        "Total cost should reflect large data processing"
    );
}
