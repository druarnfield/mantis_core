// tests/planner/dp_optimizer_test.rs
use mantis::planner::join_optimizer::dp_optimizer::*;
use std::collections::BTreeSet;

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
