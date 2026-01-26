// tests/planner/dp_optimizer_test.rs
use mantis::model::expr::{BinaryOp, Expr};
use mantis::planner::join_optimizer::dp_optimizer::*;
use mantis::planner::logical::LogicalPlan;
use mantis::semantic::graph::{EntityNode, EntityType, SizeCategory, UnifiedGraph};
use std::collections::{BTreeSet, HashMap};

#[test]
fn test_table_set_creation() {
    let mut tables = BTreeSet::new();
    tables.insert("orders".to_string());
    tables.insert("customers".to_string());

    let table_set = TableSet::new(tables.clone());

    assert_eq!(table_set.size(), 2);
    assert!(table_set.contains("orders"));
    assert!(table_set.contains("customers"));
    assert!(!table_set.contains("products"));
}

#[test]
fn test_table_set_from_vec() {
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let table_set = TableSet::from_vec(tables);

    assert_eq!(table_set.size(), 3);
}

#[test]
fn test_table_set_single() {
    let table_set = TableSet::single("orders");

    assert_eq!(table_set.size(), 1);
    assert!(table_set.contains("orders"));
}

#[test]
fn test_generate_subsets_size_1() {
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];

    let subsets = generate_subsets(&tables, 1);

    assert_eq!(subsets.len(), 3); // {A}, {B}, {C}
    assert!(subsets.iter().any(|s| s.size() == 1 && s.contains("A")));
    assert!(subsets.iter().any(|s| s.size() == 1 && s.contains("B")));
    assert!(subsets.iter().any(|s| s.size() == 1 && s.contains("C")));
}

#[test]
fn test_generate_subsets_size_2() {
    let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];

    let subsets = generate_subsets(&tables, 2);

    assert_eq!(subsets.len(), 3); // {A,B}, {A,C}, {B,C}
    assert!(subsets.iter().any(|s| s.contains("A") && s.contains("B")));
    assert!(subsets.iter().any(|s| s.contains("A") && s.contains("C")));
    assert!(subsets.iter().any(|s| s.contains("B") && s.contains("C")));
}

#[test]
fn test_generate_subsets_all() {
    let tables = vec!["A".to_string(), "B".to_string()];

    let subsets = generate_subsets(&tables, 2);

    assert_eq!(subsets.len(), 1); // {A,B}
}

#[test]
fn test_enumerate_splits_two_tables() {
    let subset = TableSet::from_vec(vec!["A".to_string(), "B".to_string()]);

    let splits = enumerate_splits(&subset);

    // Should have 1 split: ({A}, {B})
    // We deduplicate to avoid ({A}, {B}) and ({B}, {A})
    assert_eq!(splits.len(), 1);

    let (s1, s2) = &splits[0];
    assert_eq!(s1.size(), 1);
    assert_eq!(s2.size(), 1);
}

#[test]
fn test_enumerate_splits_three_tables() {
    let subset = TableSet::from_vec(vec!["A".to_string(), "B".to_string(), "C".to_string()]);

    let splits = enumerate_splits(&subset);

    // For {A,B,C}, we have:
    // Size 1: ({A}, {B,C}), ({B}, {A,C}), ({C}, {A,B})
    // We don't need size 2 because we'd get duplicates
    // Total: 3 unique splits
    assert_eq!(splits.len(), 3);
}

#[test]
fn test_classify_single_table_filter() {
    let graph = UnifiedGraph::new();
    let mut dp = DPOptimizer::new(&graph);

    // WHERE orders.amount > 1000
    let filter = Expr::binary(
        Expr::qualified_column("orders", "amount"),
        BinaryOp::Gt,
        Expr::int(1000),
    );

    let classified = dp.classify_filters(vec![filter]);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].referenced_tables.len(), 1);
    assert!(classified[0].referenced_tables.contains("orders"));
    assert!((classified[0].selectivity - 0.33).abs() < 0.01);
}

#[test]
fn test_classify_join_filter() {
    let graph = UnifiedGraph::new();
    let mut dp = DPOptimizer::new(&graph);

    // WHERE orders.customer_id = customers.id (join condition)
    let filter = Expr::binary(
        Expr::qualified_column("orders", "customer_id"),
        BinaryOp::Eq,
        Expr::qualified_column("customers", "id"),
    );

    let classified = dp.classify_filters(vec![filter]);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].referenced_tables.len(), 2);
    assert!(classified[0].referenced_tables.contains("orders"));
    assert!(classified[0].referenced_tables.contains("customers"));
}

#[test]
fn test_build_base_plan_no_filters() {
    let mut graph = UnifiedGraph::new();
    graph.add_test_entity(EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1000),
        size_category: SizeCategory::Medium,
        metadata: HashMap::new(),
    });

    let mut dp = DPOptimizer::new(&graph);
    dp.filters = vec![];

    let plan = dp.build_base_plan("orders");

    // Should be a simple scan
    match &plan.plan {
        LogicalPlan::Scan(scan) => {
            assert_eq!(scan.entity, "orders");
        }
        _ => panic!("Expected Scan node"),
    }

    assert_eq!(plan.estimated_rows, 1000);
}

#[test]
fn test_build_base_plan_with_filter() {
    let mut graph = UnifiedGraph::new();
    graph.add_test_entity(EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(1000),
        size_category: SizeCategory::Medium,
        metadata: HashMap::new(),
    });

    let mut dp = DPOptimizer::new(&graph);

    // WHERE orders.amount > 1000 (selectivity 0.33)
    let filter = Expr::binary(
        Expr::qualified_column("orders", "amount"),
        BinaryOp::Gt,
        Expr::int(1000),
    );

    dp.filters = dp.classify_filters(vec![filter]);

    let plan = dp.build_base_plan("orders");

    // Should be Scan wrapped in Filter
    match &plan.plan {
        LogicalPlan::Filter(filter_node) => {
            assert_eq!(filter_node.predicates.len(), 1);
            match &*filter_node.input {
                LogicalPlan::Scan(scan) => {
                    assert_eq!(scan.entity, "orders");
                }
                _ => panic!("Expected Scan inside Filter"),
            }
        }
        _ => panic!("Expected Filter node"),
    }

    // Estimated rows reduced by selectivity: 1000 * 0.33 ≈ 330
    assert!(plan.estimated_rows >= 300 && plan.estimated_rows <= 350);
}

// ============================================================================
// DP MAIN ALGORITHM INTEGRATION TESTS (Task 13)
// ============================================================================

use mantis::semantic::graph::{Cardinality, JoinsToEdge, RelationshipSource};

#[test]
fn test_two_table_dp_optimal() {
    let mut graph = UnifiedGraph::new();

    // Setup: orders (10K) -> customers (100)
    let orders_idx = graph.add_test_entity(EntityNode {
        name: "orders".to_string(),
        entity_type: EntityType::Fact,
        physical_name: None,
        schema: None,
        row_count: Some(10000),
        size_category: SizeCategory::Large,
        metadata: HashMap::new(),
    });

    let customers_idx = graph.add_test_entity(EntityNode {
        name: "customers".to_string(),
        entity_type: EntityType::Dimension,
        physical_name: None,
        schema: None,
        row_count: Some(100),
        size_category: SizeCategory::Small,
        metadata: HashMap::new(),
    });

    // Add relationship: orders.customer_id -> customers.id (N:1)
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

    let mut dp = DPOptimizer::new(&graph);

    let tables = vec!["orders".to_string(), "customers".to_string()];
    let filters = vec![];

    let optimized = dp.optimize(tables.clone(), filters);

    // Should return a join plan
    assert!(
        optimized.is_some(),
        "Expected optimization to return a plan"
    );

    // Should have both tables in memo
    assert!(dp.memo_contains("orders"), "Expected orders in memo");
    assert!(dp.memo_contains("customers"), "Expected customers in memo");

    // Should have final plan for both tables
    let all_tables = TableSet::from_vec(vec!["orders".to_string(), "customers".to_string()]);
    assert!(
        dp.memo_contains_set(&all_tables),
        "Expected final plan in memo"
    );
}
