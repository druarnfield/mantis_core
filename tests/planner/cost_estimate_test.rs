//! Tests for CostEstimate struct with multi-objective scoring

use mantis::planner::cost::CostEstimate;

#[test]
fn test_cost_estimate_total_with_default_weights() {
    // Create a CostEstimate with specific component costs
    let cost = CostEstimate {
        rows_out: 1000,
        cpu_cost: 100.0,
        io_cost: 50.0,
        memory_cost: 20.0,
    };

    // Default weights: CPU = 1.0, IO = 10.0 (IO weighted higher), memory = 0.1
    // Expected total: (100.0 * 1.0) + (50.0 * 10.0) + (20.0 * 0.1) = 100 + 500 + 2 = 602.0
    let total = cost.total();
    assert_eq!(total, 602.0);
}

#[test]
fn test_cost_estimate_individual_components_accessible() {
    let cost = CostEstimate {
        rows_out: 5000,
        cpu_cost: 250.0,
        io_cost: 100.0,
        memory_cost: 50.0,
    };

    assert_eq!(cost.rows_out, 5000);
    assert_eq!(cost.cpu_cost, 250.0);
    assert_eq!(cost.io_cost, 100.0);
    assert_eq!(cost.memory_cost, 50.0);
}

#[test]
fn test_cost_estimate_io_weighted_higher_than_cpu() {
    let cost = CostEstimate {
        rows_out: 1000,
        cpu_cost: 100.0,
        io_cost: 10.0, // Much lower raw cost
        memory_cost: 0.0,
    };

    // IO should still contribute significantly due to 10x weight
    // Total: (100.0 * 1.0) + (10.0 * 10.0) + 0 = 100 + 100 = 200.0
    let total = cost.total();
    assert_eq!(total, 200.0);
}

#[test]
fn test_cost_estimate_memory_weighted_lower_than_cpu() {
    let cost = CostEstimate {
        rows_out: 1000,
        cpu_cost: 10.0,
        io_cost: 0.0,
        memory_cost: 100.0, // Much higher raw cost
    };

    // Memory should contribute less due to 0.1x weight
    // Total: (10.0 * 1.0) + 0 + (100.0 * 0.1) = 10 + 10 = 20.0
    let total = cost.total();
    assert_eq!(total, 20.0);
}

#[test]
fn test_cost_estimate_zero_costs() {
    let cost = CostEstimate {
        rows_out: 0,
        cpu_cost: 0.0,
        io_cost: 0.0,
        memory_cost: 0.0,
    };

    assert_eq!(cost.total(), 0.0);
}
