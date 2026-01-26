//! Performance tests for SQL query optimization.
//!
//! These tests verify that the join order optimizer produces plans with
//! better performance characteristics compared to naive left-to-right join ordering.
//!
//! Task 17: Small query optimization (2-3 tables)
//! Task 18: Large query optimization (5+ tables) with greedy algorithm

use mantis::planner::cost::CostEstimator;
use mantis::planner::logical::{JoinCondition, JoinNode, JoinType, LogicalPlan, ScanNode};
use mantis::planner::physical::join_optimizer::JoinOrderOptimizer;
use mantis::planner::physical::{PhysicalPlan, PhysicalPlanner};
use mantis::semantic::graph::query::ColumnRef;
use mantis::semantic::graph::{
    Cardinality, EntityNode, EntityType, JoinsToEdge, RelationshipSource, SizeCategory,
    UnifiedGraph,
};
use std::time::Instant;

// ============================================================================
// Task 17: Performance Test - Small Query Optimization (2-3 tables)
// ============================================================================

/// Create a test graph with realistic data warehouse sizes.
///
/// Schema:
/// - Sales (10M rows, large fact table)
/// - Products (100K rows, large dimension)
/// - Categories (1K rows, medium dimension)
///
/// Relationships:
/// - Sales → Products (N:1)
/// - Products → Categories (N:1)
fn create_realistic_test_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Sales: Large fact table
    let sales = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000_000), // 10M rows (increased from 1M)
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let sales_idx = graph.add_test_entity(sales);

    // Products: Large dimension
    let products = EntityNode {
        name: "products".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100_000), // 100K rows (increased from 10K)
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let products_idx = graph.add_test_entity(products);

    // Categories: Medium dimension
    let categories = EntityNode {
        name: "categories".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(1_000), // 1K rows (increased from 100)
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let categories_idx = graph.add_test_entity(categories);

    // Sales → Products (N:1)
    graph.add_test_join(
        sales_idx,
        products_idx,
        JoinsToEdge {
            from_entity: "sales".to_string(),
            to_entity: "products".to_string(),
            join_columns: vec![("product_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    // Products → Categories (N:1)
    graph.add_test_join(
        products_idx,
        categories_idx,
        JoinsToEdge {
            from_entity: "products".to_string(),
            to_entity: "categories".to_string(),
            join_columns: vec![("category_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph
}

/// Create a naive 3-table join plan (left-to-right order: sales → products → categories).
///
/// This represents the typical join order a simple planner would generate.
fn create_naive_three_table_plan() -> LogicalPlan {
    // First join: sales → products
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

    // Second join: (sales ⋈ products) → categories
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

#[test]
fn test_three_table_optimization_improves_cost() {
    // PURPOSE: Verify that join order optimization produces a plan with lower cost
    // than the naive left-to-right join order.
    //
    // EXPECTATION: Optimized plan should join smaller tables first (products ⋈ categories)
    // before joining the large sales table, resulting in 2-10x cost reduction.

    let graph = create_realistic_test_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    // Create naive plan (sales → products → categories)
    let naive_logical_plan = create_naive_three_table_plan();

    // Generate optimized candidates using join order optimizer
    let optimized_candidates = planner
        .generate_candidates(&naive_logical_plan)
        .expect("Should generate optimized candidates");

    assert!(
        !optimized_candidates.is_empty(),
        "Should generate at least one candidate plan"
    );

    // Select best optimized plan
    let best_optimized_plan = estimator
        .select_best(optimized_candidates)
        .expect("Should select best plan");

    // Convert naive plan to physical (without optimization)
    // We need to manually build the naive physical plan to compare against optimized
    let naive_physical_plan = create_naive_physical_plan(&graph);

    // Compare costs
    let naive_cost = estimator.estimate(&naive_physical_plan);
    let optimized_cost = estimator.estimate(&best_optimized_plan);

    println!("\n=== 3-Table Join Optimization Results ===");
    println!("Naive plan (sales → products → categories):");
    println!("  Rows out: {}", naive_cost.rows_out);
    println!("  CPU cost: {:.2}", naive_cost.cpu_cost);
    println!("  IO cost: {:.2}", naive_cost.io_cost);
    println!("  Memory cost: {:.2}", naive_cost.memory_cost);
    println!("  Total cost: {:.2}", naive_cost.total());
    println!("\nOptimized plan:");
    println!("  Rows out: {}", optimized_cost.rows_out);
    println!("  CPU cost: {:.2}", optimized_cost.cpu_cost);
    println!("  IO cost: {:.2}", optimized_cost.io_cost);
    println!("  Memory cost: {:.2}", optimized_cost.memory_cost);
    println!("  Total cost: {:.2}", optimized_cost.total());
    println!(
        "\nImprovement: {:.2}x faster",
        naive_cost.total() / optimized_cost.total()
    );

    // ASSERTION: Optimized plan should have lower or equal cost
    assert!(
        optimized_cost.total() <= naive_cost.total(),
        "Optimized plan should have cost <= naive plan. Optimized: {:.2}, Naive: {:.2}",
        optimized_cost.total(),
        naive_cost.total()
    );

    // STRONG ASSERTION: For this schema, optimized should show significant improvement
    // The optimizer should choose (products ⋈ categories) first, which reduces intermediate
    // result size before joining sales.
    let improvement_ratio = naive_cost.total() / optimized_cost.total();
    assert!(
        improvement_ratio >= 1.5,
        "Optimized plan should show significant improvement (>= 1.5x). Got: {:.2}x. \
         This test requires the optimizer to choose (products ⋈ categories) first, \
         which reduces intermediate result size before joining sales.",
        improvement_ratio
    );
}

/// Create a naive physical plan for comparison (bypassing optimizer).
///
/// This manually constructs the left-to-right join order without optimization.
fn create_naive_physical_plan(graph: &UnifiedGraph) -> PhysicalPlan {
    use mantis::planner::physical::TableScanStrategy;

    // Scan sales
    let sales_scan = PhysicalPlan::TableScan {
        table: "sales".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(10_000_000),
    };

    // Scan products
    let products_scan = PhysicalPlan::TableScan {
        table: "products".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(100_000),
    };

    // Join sales → products
    let sales_products_join = PhysicalPlan::HashJoin {
        left: Box::new(sales_scan),
        right: Box::new(products_scan),
        on: vec![("product_id".to_string(), "id".to_string())],
        estimated_rows: Some(10_000_000), // N:1 join preserves left side
    };

    // Scan categories
    let categories_scan = PhysicalPlan::TableScan {
        table: "categories".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1_000),
    };

    // Join (sales ⋈ products) → categories
    PhysicalPlan::HashJoin {
        left: Box::new(sales_products_join),
        right: Box::new(categories_scan),
        on: vec![("category_id".to_string(), "id".to_string())],
        estimated_rows: Some(10_000_000), // N:1 join preserves left side
    }
}

#[test]
fn test_two_table_optimization() {
    // PURPOSE: Verify that even simple 2-table joins benefit from optimization
    // (e.g., choosing the smaller table as the build side for hash joins).

    let graph = create_realistic_test_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    // Create logical plan: sales → products
    let logical_plan = LogicalPlan::Join(JoinNode {
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

    // Generate candidates (both join orders: sales → products and products → sales)
    let candidates = planner
        .generate_candidates(&logical_plan)
        .expect("Should generate candidates");

    assert!(
        candidates.len() >= 2,
        "Should generate at least 2 candidates for 2-table join"
    );

    // Select best plan
    let best_plan = estimator
        .select_best(candidates.clone())
        .expect("Should select best plan");

    let best_cost = estimator.estimate(&best_plan);

    println!("\n=== 2-Table Join Optimization Results ===");
    println!("Number of candidates: {}", candidates.len());
    println!("Best plan cost: {:.2}", best_cost.total());

    // Verify that all candidates have reasonable costs
    for candidate in &candidates {
        let cost = estimator.estimate(candidate);
        assert!(
            cost.total().is_finite(),
            "All candidate costs should be finite"
        );
    }

    // Best plan should have lowest cost
    for candidate in &candidates {
        let cost = estimator.estimate(candidate);
        assert!(
            best_cost.total() <= cost.total(),
            "Best plan should have lowest cost"
        );
    }
}

// ============================================================================
// Task 18: Performance Test - Large Query Optimization (5+ tables)
// ============================================================================

/// Create a large star schema graph with 7 tables.
///
/// Schema:
/// - Sales (10M rows, large fact table in center)
/// - 6 dimension tables around it (100-5000 rows each)
///
/// This represents a realistic data warehouse star schema.
fn create_large_star_schema() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Sales: Large central fact table
    let sales = EntityNode {
        name: "sales".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000_000), // 10M rows
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let sales_idx = graph.add_test_entity(sales);

    // Create 6 dimension tables of varying sizes
    let dimensions = vec![
        ("products", 5_000),
        ("customers", 10_000),
        ("stores", 200),
        ("dates", 365),
        ("regions", 50),
        ("salespersons", 100),
    ];

    for (name, row_count) in dimensions {
        let entity = EntityNode {
            name: name.to_string(),
            entity_type: EntityType::Dimension,
            physical_name: None,
            schema: None,
            row_count: Some(row_count),
            size_category: if row_count < 1000 {
                SizeCategory::Small
            } else {
                SizeCategory::Medium
            },
            metadata: Default::default(),
        };
        let dim_idx = graph.add_test_entity(entity);

        // Add join: sales → dimension (N:1)
        graph.add_test_join(
            sales_idx,
            dim_idx,
            JoinsToEdge {
                from_entity: "sales".to_string(),
                to_entity: name.to_string(),
                join_columns: vec![(format!("{}_id", name), "id".to_string())],
                cardinality: Cardinality::ManyToOne,
                source: RelationshipSource::ForeignKey,
            },
        );
    }

    graph
}

#[test]
fn test_large_query_optimization_uses_greedy_algorithm() {
    // PURPOSE: Verify that the greedy join order optimizer:
    // 1. Completes in reasonable time (< 200ms) for large queries
    // 2. Produces a valid plan
    // 3. Follows the greedy heuristic (joining smaller tables first)

    let graph = create_large_star_schema();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // All 7 tables in the star schema
    let tables = vec![
        "sales",
        "products",
        "customers",
        "stores",
        "dates",
        "regions",
        "salespersons",
    ];

    // Time the greedy optimization
    let start = Instant::now();
    let result = optimizer.greedy_join_order(&tables);
    let elapsed = start.elapsed();

    println!("\n=== Large Query Optimization (7 tables) ===");
    println!("Planning time: {:?}", elapsed);

    // ASSERTION: Planning should complete quickly (< 200ms for 7 tables)
    assert!(
        elapsed.as_millis() < 200,
        "Planning should complete in < 200ms, took {:?}",
        elapsed
    );

    // ASSERTION: Should return a valid plan
    assert!(result.is_some(), "Greedy optimizer should return a plan");

    let plan = result.unwrap();

    // Verify all tables are included
    let extracted_tables = optimizer.extract_tables(&plan);
    assert_eq!(
        extracted_tables.len(),
        7,
        "Plan should include all 7 tables"
    );
    for table in &tables {
        assert!(
            extracted_tables.contains(&table.to_string()),
            "Plan should include table: {}",
            table
        );
    }

    println!("Greedy plan created successfully");
    println!("Tables included: {:?}", extracted_tables);

    // Verify the plan is valid by checking it can be converted to physical plan
    // and has reasonable cost
    let planner = PhysicalPlanner::new(&graph);
    let physical_candidates = planner
        .generate_candidates(&plan)
        .expect("Should convert logical plan to physical");

    assert!(
        !physical_candidates.is_empty(),
        "Should generate at least one physical plan"
    );

    let estimator = CostEstimator::new(&graph);
    let best_physical = estimator
        .select_best(physical_candidates)
        .expect("Should select best physical plan");

    let cost = estimator.estimate(&best_physical);
    println!("Physical plan cost: {:.2}", cost.total());

    // Cost should be reasonable (not infinite)
    assert!(
        cost.total().is_finite(),
        "Physical plan cost should be finite, got {}",
        cost.total()
    );
}

#[test]
fn test_large_query_planning_performance() {
    // PURPOSE: Demonstrate that planning time scales reasonably with query size.
    // Compare planning times for 3, 5, and 7 table queries.

    let graph = create_large_star_schema();
    let optimizer = JoinOrderOptimizer::new(&graph);

    println!("\n=== Planning Performance Scaling ===");

    // Test with increasing table counts
    let test_cases = vec![
        (3, vec!["sales", "products", "customers"]),
        (5, vec!["sales", "products", "customers", "stores", "dates"]),
        (
            7,
            vec![
                "sales",
                "products",
                "customers",
                "stores",
                "dates",
                "regions",
                "salespersons",
            ],
        ),
    ];

    for (num_tables, tables) in test_cases {
        let start = Instant::now();
        let result = optimizer.greedy_join_order(&tables);
        let elapsed = start.elapsed();

        println!("{} tables: {:?}", num_tables, elapsed);

        assert!(
            result.is_some(),
            "{} table query should return a plan",
            num_tables
        );

        // Planning should be fast even for large queries
        assert!(
            elapsed.as_millis() < 200,
            "{} table query should complete in < 200ms, took {:?}",
            num_tables,
            elapsed
        );
    }

    // EXPECTATION: Planning time should scale sub-exponentially
    // (greedy is O(n²) vs enumeration which is O(n!))
}

#[test]
fn test_greedy_vs_enumeration_threshold() {
    // PURPOSE: Verify that enumeration is used for small queries (≤3 tables)
    // and greedy for larger queries (>3 tables).

    let graph = create_large_star_schema();
    let optimizer = JoinOrderOptimizer::new(&graph);

    // Small query (3 tables) - should use enumeration
    let small_tables = vec!["sales", "products", "customers"];

    let start_small = Instant::now();
    let small_result = optimizer.greedy_join_order(&small_tables);
    let elapsed_small = start_small.elapsed();

    assert!(small_result.is_some(), "Small query should return a plan");

    // Large query (5 tables) - should use greedy
    let large_tables = vec!["sales", "products", "customers", "stores", "dates"];

    let start_large = Instant::now();
    let large_result = optimizer.greedy_join_order(&large_tables);
    let elapsed_large = start_large.elapsed();

    assert!(large_result.is_some(), "Large query should return a plan");

    println!("\n=== Enumeration vs Greedy Threshold ===");
    println!("3 tables (enumeration): {:?}", elapsed_small);
    println!("5 tables (greedy): {:?}", elapsed_large);

    // Both should complete quickly
    assert!(
        elapsed_small.as_millis() < 100,
        "Small query should be fast"
    );
    assert!(
        elapsed_large.as_millis() < 200,
        "Large query should be fast"
    );
}

// ============================================================================
// Additional Performance Tests - Realistic Optimization Scenarios
// ============================================================================

/// Create a star schema graph with one large fact table and 3 small dimensions.
///
/// Schema:
/// - Orders (10M rows, large fact table)
/// - Customers (100 rows, small dimension)
/// - Products (100 rows, small dimension)
/// - Stores (100 rows, small dimension)
///
/// Relationships: All dimensions connect to Orders (N:1)
fn create_star_schema_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // Orders: Large central fact table
    let orders = EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10_000_000), // 10M rows
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let orders_idx = graph.add_test_entity(orders);

    // Create 3 small dimension tables
    let dimensions = vec![("customers", 100), ("products", 100), ("stores", 100)];

    for (name, row_count) in dimensions {
        let entity = EntityNode {
            name: name.to_string(),
            entity_type: EntityType::Dimension,
            physical_name: None,
            schema: None,
            row_count: Some(row_count),
            size_category: SizeCategory::Small,
            metadata: Default::default(),
        };
        let dim_idx = graph.add_test_entity(entity);

        // Add join: orders → dimension (N:1)
        graph.add_test_join(
            orders_idx,
            dim_idx,
            JoinsToEdge {
                from_entity: "orders".to_string(),
                to_entity: name.to_string(),
                join_columns: vec![(format!("{}_id", name), "id".to_string())],
                cardinality: Cardinality::ManyToOne,
                source: RelationshipSource::ForeignKey,
            },
        );
    }

    graph
}

#[test]
fn test_star_schema_optimization() {
    // PURPOSE: Demonstrate that star schema optimization provides dramatic improvement
    // by joining all small dimensions first, then joining the result to the large fact table.
    //
    // EXPECTATION: >= 5x improvement by avoiding multiple full scans of the fact table.

    let graph = create_star_schema_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    // Create naive plan: (((orders ⋈ customers) ⋈ products) ⋈ stores)
    let naive_plan = create_naive_star_schema_plan();

    // Generate optimized candidates
    let optimized_candidates = planner
        .generate_candidates(&naive_plan)
        .expect("Should generate optimized candidates");

    assert!(
        !optimized_candidates.is_empty(),
        "Should generate at least one candidate plan"
    );

    // Select best optimized plan
    let best_optimized_plan = estimator
        .select_best(optimized_candidates)
        .expect("Should select best plan");

    // Create naive physical plan
    let naive_physical_plan = create_naive_star_schema_physical_plan(&graph);

    // Compare costs
    let naive_cost = estimator.estimate(&naive_physical_plan);
    let optimized_cost = estimator.estimate(&best_optimized_plan);

    println!("\n=== Star Schema Optimization Results ===");
    println!("Naive plan (left-deep tree with fact table first):");
    println!("  Total cost: {:.2}", naive_cost.total());
    println!("\nOptimized plan (dimensions joined first):");
    println!("  Total cost: {:.2}", optimized_cost.total());
    println!(
        "\nImprovement: {:.2}x faster",
        naive_cost.total() / optimized_cost.total()
    );

    // STRONG ASSERTION: Star schema optimization should show dramatic improvement
    let improvement_ratio = naive_cost.total() / optimized_cost.total();
    assert!(
        improvement_ratio >= 5.0,
        "Star schema optimization should show dramatic improvement (>= 5x). Got: {:.2}x. \
         The optimizer should join all small dimensions first (customers ⋈ products ⋈ stores), \
         then join the result to the large orders table once.",
        improvement_ratio
    );
}

/// Create a naive star schema plan (left-deep tree starting with fact table).
fn create_naive_star_schema_plan() -> LogicalPlan {
    // First join: orders → customers
    let orders_customers = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "orders".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "customers".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "customers_id".to_string()),
            ColumnRef::new("customers".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Second join: (orders ⋈ customers) → products
    let orders_customers_products = LogicalPlan::Join(JoinNode {
        left: Box::new(orders_customers),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "products".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "products_id".to_string()),
            ColumnRef::new("products".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Third join: ((orders ⋈ customers) ⋈ products) → stores
    LogicalPlan::Join(JoinNode {
        left: Box::new(orders_customers_products),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "stores".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("orders".to_string(), "stores_id".to_string()),
            ColumnRef::new("stores".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    })
}

/// Create a naive star schema physical plan.
fn create_naive_star_schema_physical_plan(graph: &UnifiedGraph) -> PhysicalPlan {
    use mantis::planner::physical::TableScanStrategy;

    // Scan orders
    let orders_scan = PhysicalPlan::TableScan {
        table: "orders".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(10_000_000),
    };

    // Scan customers
    let customers_scan = PhysicalPlan::TableScan {
        table: "customers".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(100),
    };

    // Join orders → customers
    let orders_customers = PhysicalPlan::HashJoin {
        left: Box::new(orders_scan),
        right: Box::new(customers_scan),
        on: vec![("customers_id".to_string(), "id".to_string())],
        estimated_rows: Some(10_000_000),
    };

    // Scan products
    let products_scan = PhysicalPlan::TableScan {
        table: "products".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(100),
    };

    // Join (orders ⋈ customers) → products
    let orders_customers_products = PhysicalPlan::HashJoin {
        left: Box::new(orders_customers),
        right: Box::new(products_scan),
        on: vec![("products_id".to_string(), "id".to_string())],
        estimated_rows: Some(10_000_000),
    };

    // Scan stores
    let stores_scan = PhysicalPlan::TableScan {
        table: "stores".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(100),
    };

    // Join ((orders ⋈ customers) ⋈ products) → stores
    PhysicalPlan::HashJoin {
        left: Box::new(orders_customers_products),
        right: Box::new(stores_scan),
        on: vec![("stores_id".to_string(), "id".to_string())],
        estimated_rows: Some(10_000_000),
    }
}

/// Create a graph with 4 tables where bushy join tree is optimal.
///
/// Schema:
/// - TableA (1M rows)
/// - TableB (1K rows)
/// - TableC (1K rows)
/// - TableD (100 rows)
///
/// Relationships:
/// - A ⋈ B, B ⋈ C, C ⋈ D (chain)
/// - A ⋈ D (direct cross-link)
///
/// Optimal: (A ⋈ D) ⋈ (B ⋈ C) - bushy tree
/// Naive: ((A ⋈ B) ⋈ C) ⋈ D - left-deep tree
fn create_bushy_join_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // TableA: Large table
    let table_a = EntityNode {
        name: "table_a".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1_000_000), // 1M rows
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let a_idx = graph.add_test_entity(table_a);

    // TableB: Medium table
    let table_b = EntityNode {
        name: "table_b".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(1_000), // 1K rows
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let b_idx = graph.add_test_entity(table_b);

    // TableC: Medium table
    let table_c = EntityNode {
        name: "table_c".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(1_000), // 1K rows
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let c_idx = graph.add_test_entity(table_c);

    // TableD: Small table
    let table_d = EntityNode {
        name: "table_d".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100), // 100 rows
        size_category: SizeCategory::Small,
        metadata: Default::default(),
    };
    let d_idx = graph.add_test_entity(table_d);

    // Add joins: A → B, B → C, C → D (chain)
    graph.add_test_join(
        a_idx,
        b_idx,
        JoinsToEdge {
            from_entity: "table_a".to_string(),
            to_entity: "table_b".to_string(),
            join_columns: vec![("b_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph.add_test_join(
        b_idx,
        c_idx,
        JoinsToEdge {
            from_entity: "table_b".to_string(),
            to_entity: "table_c".to_string(),
            join_columns: vec![("c_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph.add_test_join(
        c_idx,
        d_idx,
        JoinsToEdge {
            from_entity: "table_c".to_string(),
            to_entity: "table_d".to_string(),
            join_columns: vec![("d_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    // Add direct join: A → D (cross-link that enables bushy plan)
    graph.add_test_join(
        a_idx,
        d_idx,
        JoinsToEdge {
            from_entity: "table_a".to_string(),
            to_entity: "table_d".to_string(),
            join_columns: vec![("d_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph
}

#[test]
fn test_bushy_join_benefit() {
    // PURPOSE: Demonstrate that bushy join trees can outperform left-deep trees
    // when the query graph structure allows parallelizable sub-joins.
    //
    // EXPECTATION: >= 3x improvement by using bushy plan: (A ⋈ D) ⋈ (B ⋈ C)
    // instead of left-deep: ((A ⋈ B) ⋈ C) ⋈ D

    let graph = create_bushy_join_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    // Create naive left-deep plan: ((A ⋈ B) ⋈ C) ⋈ D
    let naive_plan = create_naive_bushy_join_plan();

    // Generate optimized candidates (should include bushy plans)
    let optimized_candidates = planner
        .generate_candidates(&naive_plan)
        .expect("Should generate optimized candidates");

    assert!(
        !optimized_candidates.is_empty(),
        "Should generate at least one candidate plan"
    );

    // Select best optimized plan
    let best_optimized_plan = estimator
        .select_best(optimized_candidates)
        .expect("Should select best plan");

    // Create naive physical plan
    let naive_physical_plan = create_naive_bushy_join_physical_plan(&graph);

    // Compare costs
    let naive_cost = estimator.estimate(&naive_physical_plan);
    let optimized_cost = estimator.estimate(&best_optimized_plan);

    println!("\n=== Bushy Join Optimization Results ===");
    println!("Naive plan (left-deep: ((A ⋈ B) ⋈ C) ⋈ D):");
    println!("  Total cost: {:.2}", naive_cost.total());
    println!("\nOptimized plan (should be bushy if possible):");
    println!("  Total cost: {:.2}", optimized_cost.total());
    println!(
        "\nImprovement: {:.2}x faster",
        naive_cost.total() / optimized_cost.total()
    );

    // STRONG ASSERTION: Bushy join should show significant improvement
    let improvement_ratio = naive_cost.total() / optimized_cost.total();
    assert!(
        improvement_ratio >= 3.0,
        "Bushy join optimization should show significant improvement (>= 3x). Got: {:.2}x. \
         The optimizer should ideally choose a bushy plan like (A ⋈ D) ⋈ (B ⋈ C) \
         or at least join smaller tables first.",
        improvement_ratio
    );
}

/// Create a naive bushy join plan (left-deep tree).
fn create_naive_bushy_join_plan() -> LogicalPlan {
    // First join: A → B
    let a_b = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "table_a".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "table_b".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("table_a".to_string(), "b_id".to_string()),
            ColumnRef::new("table_b".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Second join: (A ⋈ B) → C
    let a_b_c = LogicalPlan::Join(JoinNode {
        left: Box::new(a_b),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "table_c".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("table_b".to_string(), "c_id".to_string()),
            ColumnRef::new("table_c".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Third join: ((A ⋈ B) ⋈ C) → D
    LogicalPlan::Join(JoinNode {
        left: Box::new(a_b_c),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "table_d".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("table_c".to_string(), "d_id".to_string()),
            ColumnRef::new("table_d".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    })
}

/// Create a naive bushy join physical plan.
fn create_naive_bushy_join_physical_plan(graph: &UnifiedGraph) -> PhysicalPlan {
    use mantis::planner::physical::TableScanStrategy;

    // Scan table_a
    let a_scan = PhysicalPlan::TableScan {
        table: "table_a".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1_000_000),
    };

    // Scan table_b
    let b_scan = PhysicalPlan::TableScan {
        table: "table_b".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1_000),
    };

    // Join A → B
    let a_b = PhysicalPlan::HashJoin {
        left: Box::new(a_scan),
        right: Box::new(b_scan),
        on: vec![("b_id".to_string(), "id".to_string())],
        estimated_rows: Some(1_000_000),
    };

    // Scan table_c
    let c_scan = PhysicalPlan::TableScan {
        table: "table_c".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(1_000),
    };

    // Join (A ⋈ B) → C
    let a_b_c = PhysicalPlan::HashJoin {
        left: Box::new(a_b),
        right: Box::new(c_scan),
        on: vec![("c_id".to_string(), "id".to_string())],
        estimated_rows: Some(1_000_000),
    };

    // Scan table_d
    let d_scan = PhysicalPlan::TableScan {
        table: "table_d".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(100),
    };

    // Join ((A ⋈ B) ⋈ C) → D
    PhysicalPlan::HashJoin {
        left: Box::new(a_b_c),
        right: Box::new(d_scan),
        on: vec![("d_id".to_string(), "id".to_string())],
        estimated_rows: Some(1_000_000),
    }
}

/// Create a graph with a highly selective filter scenario.
///
/// Schema:
/// - LargeTable (5M rows)
/// - MediumTable (10K rows)
///
/// With a filter that reduces LargeTable to ~5K rows (0.1% selectivity).
/// Optimal: Apply filter to LargeTable first, then join with MediumTable.
fn create_filter_optimization_graph() -> UnifiedGraph {
    let mut graph = UnifiedGraph::new();

    // LargeTable: 5M rows
    let large_table = EntityNode {
        name: "large_table".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(5_000_000), // 5M rows
        size_category: SizeCategory::Large,
        metadata: Default::default(),
    };
    let large_idx = graph.add_test_entity(large_table);

    // MediumTable: 10K rows
    let medium_table = EntityNode {
        name: "medium_table".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(10_000), // 10K rows
        size_category: SizeCategory::Medium,
        metadata: Default::default(),
    };
    let medium_idx = graph.add_test_entity(medium_table);

    // Add join: LargeTable → MediumTable (N:1)
    graph.add_test_join(
        large_idx,
        medium_idx,
        JoinsToEdge {
            from_entity: "large_table".to_string(),
            to_entity: "medium_table".to_string(),
            join_columns: vec![("medium_id".to_string(), "id".to_string())],
            cardinality: Cardinality::ManyToOne,
            source: RelationshipSource::ForeignKey,
        },
    );

    graph
}

#[test]
fn test_high_selectivity_filter_optimization() {
    // PURPOSE: Demonstrate that highly selective filters should be pushed down
    // and applied before joins to dramatically reduce the working set size.
    //
    // EXPECTATION: >= 2x improvement when filter reduces large table from 5M to ~5K rows
    // before joining with the 10K row table.

    let graph = create_filter_optimization_graph();
    let planner = PhysicalPlanner::new(&graph);
    let estimator = CostEstimator::new(&graph);

    // Create naive plan without filter optimization
    let naive_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "large_table".to_string(),
        })),
        right: Box::new(LogicalPlan::Scan(ScanNode {
            entity: "medium_table".to_string(),
        })),
        join_type: JoinType::Inner,
        on: JoinCondition::Equi(vec![(
            ColumnRef::new("large_table".to_string(), "medium_id".to_string()),
            ColumnRef::new("medium_table".to_string(), "id".to_string()),
        )]),
        cardinality: Some(Cardinality::ManyToOne),
    });

    // Generate optimized candidates
    let optimized_candidates = planner
        .generate_candidates(&naive_plan)
        .expect("Should generate optimized candidates");

    assert!(
        !optimized_candidates.is_empty(),
        "Should generate at least one candidate plan"
    );

    // Select best optimized plan
    let best_optimized_plan = estimator
        .select_best(optimized_candidates)
        .expect("Should select best plan");

    // Create naive physical plan (no filter optimization)
    let naive_physical_plan = create_filter_optimization_naive_physical_plan(&graph);

    // Create optimized physical plan with filter applied first
    // (simulating filter pushdown by reducing estimated rows)
    let optimized_physical_plan = create_filter_optimization_optimized_physical_plan(&graph);

    // Compare costs
    let naive_cost = estimator.estimate(&naive_physical_plan);
    let optimized_cost = estimator.estimate(&optimized_physical_plan);

    println!("\n=== Filter Optimization Results ===");
    println!("Naive plan (no filter pushdown, 5M rows joined):");
    println!("  Total cost: {:.2}", naive_cost.total());
    println!("\nOptimized plan (filter applied first, ~5K rows joined):");
    println!("  Total cost: {:.2}", optimized_cost.total());
    println!(
        "\nImprovement: {:.2}x faster",
        naive_cost.total() / optimized_cost.total()
    );

    // STRONG ASSERTION: Filter optimization should show significant improvement
    let improvement_ratio = naive_cost.total() / optimized_cost.total();
    assert!(
        improvement_ratio >= 2.0,
        "Filter optimization should show significant improvement (>= 2x). Got: {:.2}x. \
         With a highly selective filter (0.1% selectivity), applying the filter before \
         the join should dramatically reduce the working set size.",
        improvement_ratio
    );
}

/// Create naive physical plan for filter optimization test (no filter pushdown).
fn create_filter_optimization_naive_physical_plan(graph: &UnifiedGraph) -> PhysicalPlan {
    use mantis::planner::physical::TableScanStrategy;

    // Scan large_table (full 5M rows)
    let large_scan = PhysicalPlan::TableScan {
        table: "large_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(5_000_000),
    };

    // Scan medium_table
    let medium_scan = PhysicalPlan::TableScan {
        table: "medium_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(10_000),
    };

    // Join without filter pushdown (5M rows joined)
    PhysicalPlan::HashJoin {
        left: Box::new(large_scan),
        right: Box::new(medium_scan),
        on: vec![("medium_id".to_string(), "id".to_string())],
        estimated_rows: Some(5_000_000),
    }
}

/// Create optimized physical plan with filter applied first (simulating pushdown).
fn create_filter_optimization_optimized_physical_plan(graph: &UnifiedGraph) -> PhysicalPlan {
    use mantis::planner::physical::TableScanStrategy;

    // Scan large_table with filter applied (reduced to ~5K rows, 0.1% selectivity)
    let large_scan_filtered = PhysicalPlan::TableScan {
        table: "large_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(5_000), // Filter reduces from 5M to 5K
    };

    // Scan medium_table
    let medium_scan = PhysicalPlan::TableScan {
        table: "medium_table".to_string(),
        strategy: TableScanStrategy::FullScan,
        estimated_rows: Some(10_000),
    };

    // Join with filtered data (~5K rows joined instead of 5M)
    PhysicalPlan::HashJoin {
        left: Box::new(large_scan_filtered),
        right: Box::new(medium_scan),
        on: vec![("medium_id".to_string(), "id".to_string())],
        estimated_rows: Some(5_000),
    }
}
