//! Tests for report and pivot planners/emitters.

use std::collections::HashMap;

use crate::dialect::Dialect;
use crate::expr::Expr;
use crate::model::{
    Cardinality, DataType, FactDefinition, Model, PivotReport, Relationship, Report, SourceEntity,
};
use crate::semantic::error::PlanError;
use crate::semantic::model_graph::ModelGraph;

use super::emitter::ReportEmitter;
use super::pivot_emitter::PivotEmitter;
use super::pivot_planner::{PivotColumnValues, PivotDimension, PivotMeasure, PivotPlan, PivotPlanner, PivotTotals};
use super::planner::{FactCte, PlannedMeasure, ReportPlan, ReportPlanner};

fn sample_model() -> Model {
    Model::new()
        .with_source(
            SourceEntity::new("orders", "raw.orders")
                .with_required_column("order_id", DataType::Int64)
                .with_required_column("customer_id", DataType::Int64)
                .with_required_column("amount", DataType::Decimal(10, 2)),
        )
        .with_source(
            SourceEntity::new("customers", "raw.customers")
                .with_required_column("customer_id", DataType::Int64)
                .with_required_column("region", DataType::String),
        )
        .with_source(
            SourceEntity::new("inventory", "raw.inventory")
                .with_required_column("product_id", DataType::Int64)
                .with_required_column("stock_value", DataType::Decimal(10, 2)),
        )
        .with_source(
            SourceEntity::new("date", "raw.date")
                .with_required_column("date_id", DataType::Date)
                .with_required_column("month", DataType::String),
        )
        // orders -> customers
        .with_relationship(Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        ))
        // orders_fact -> customers
        .with_relationship(Relationship::new(
            "orders_fact",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        ))
        // orders_fact -> date
        .with_relationship(Relationship::new(
            "orders_fact",
            "date",
            "order_date",
            "date_id",
            Cardinality::ManyToOne,
        ))
        // inventory_fact -> date
        .with_relationship(Relationship::new(
            "inventory_fact",
            "date",
            "snapshot_date",
            "date_id",
            Cardinality::ManyToOne,
        ))
        // Note: inventory_fact has NO path to customers
        .with_fact(
            FactDefinition::new("orders_fact", "analytics.orders_fact")
                .with_grain("orders", "order_id")
                .with_sum("revenue", "amount")
                .with_count("order_count", "*"),
        )
        .with_fact(
            FactDefinition::new("inventory_fact", "analytics.inventory_fact")
                .with_grain("inventory", "product_id")
                .with_sum("stock_value", "stock_value"),
        )
}

#[test]
fn test_group_measures_by_fact() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let report = Report::new("test")
        .with_measure("orders_fact", "revenue")
        .with_measure("orders_fact", "order_count")
        .with_measure("inventory_fact", "stock_value");

    let grouped = planner.group_measures_by_fact(&report).unwrap();

    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped.get("orders_fact").unwrap().len(), 2);
    assert_eq!(grouped.get("inventory_fact").unwrap().len(), 1);
}

#[test]
fn test_filter_routing() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let mut filter_entities = HashMap::new();
    filter_entities.insert(
        "customers".to_string(),
        vec!["customers.region = 'EMEA'".to_string()],
    );

    // orders_fact has path to customers
    let orders_filters = planner.route_filters("orders_fact", &filter_entities);
    assert_eq!(orders_filters.len(), 1);

    // inventory_fact has NO path to customers
    let inventory_filters = planner.route_filters("inventory_fact", &filter_entities);
    assert_eq!(inventory_filters.len(), 0);
}

#[test]
fn test_plan_report() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let report = Report::new("executive_dashboard")
        .with_measure("orders_fact", "revenue")
        .with_measure("inventory_fact", "stock_value")
        .with_filter("customers.region = 'EMEA'")
        .with_group_by("date.month");

    let plan = planner.plan(&report).unwrap();

    // Should have 2 CTEs
    assert_eq!(plan.fact_ctes.len(), 2);

    // orders_fact CTE should have the region filter
    let orders_cte = plan
        .fact_ctes
        .iter()
        .find(|c| c.fact_name == "orders_fact")
        .unwrap();
    assert_eq!(orders_cte.applicable_filters.len(), 1);
    assert!(orders_cte.applicable_filters[0].contains("region"));

    // inventory_fact CTE should NOT have the region filter
    let inventory_cte = plan
        .fact_ctes
        .iter()
        .find(|c| c.fact_name == "inventory_fact")
        .unwrap();
    assert_eq!(inventory_cte.applicable_filters.len(), 0);
}

// ========================================================================
// Error Case Tests
// ========================================================================

#[test]
fn test_plan_report_unknown_fact() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let report = Report::new("test")
        .with_measure("nonexistent_fact", "revenue");

    let result = planner.plan(&report);
    assert!(result.is_err());
    assert!(matches!(result, Err(PlanError::UnknownEntity(_))));
}

#[test]
fn test_plan_report_unknown_measure() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let report = Report::new("test")
        .with_measure("orders_fact", "nonexistent_measure");

    let result = planner.plan(&report);
    assert!(result.is_err());
    assert!(matches!(result, Err(PlanError::UnknownField { .. })));
}

// ========================================================================
// Emitter Tests
// ========================================================================

#[test]
fn test_emit_single_fact_report() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let report = Report::new("simple_report")
        .with_measure("orders_fact", "revenue")
        .with_group_by("date.month");

    let plan = planner.plan(&report).unwrap();
    let emitter = ReportEmitter::new();
    let query = emitter.emit(&plan).unwrap();

    // Should have 1 CTE
    assert_eq!(query.with.len(), 1);
    assert_eq!(query.with[0].name, "orders_fact_metrics");

    // FROM should reference the CTE
    assert!(query.from.is_some());
}

#[test]
fn test_emit_multi_fact_report() {
    let model = sample_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = ReportPlanner::new(&model, &graph);

    let report = Report::new("multi_fact_report")
        .with_measure("orders_fact", "revenue")
        .with_measure("inventory_fact", "stock_value")
        .with_group_by("date.month");

    let plan = planner.plan(&report).unwrap();
    let emitter = ReportEmitter::new();
    let query = emitter.emit(&plan).unwrap();

    // Should have 2 CTEs
    assert_eq!(query.with.len(), 2);

    // Should have 1 JOIN (FULL OUTER)
    assert_eq!(query.joins.len(), 1);

    // Generate SQL and verify structure
    let sql = query.to_sql(Dialect::DuckDb);
    println!("Generated SQL:\n{}", sql);

    assert!(sql.contains("WITH"));
    assert!(sql.contains("orders_fact_metrics"));
    assert!(sql.contains("inventory_fact_metrics"));
    assert!(sql.contains("FULL OUTER JOIN"));
    assert!(sql.contains("COALESCE"));
}

#[test]
fn test_emit_empty_plan_fails() {
    let emitter = ReportEmitter::new();

    let empty_plan = ReportPlan {
        report_name: "empty".to_string(),
        fact_ctes: vec![],
        group_by: vec![],
        output_columns: vec![],
    };

    let result = emitter.emit(&empty_plan);
    assert!(result.is_err());
    assert!(matches!(result, Err(PlanError::InvalidModel(_))));
}

#[test]
fn test_emit_multi_fact_without_group_by_fails() {
    let emitter = ReportEmitter::new();

    // Create a plan with multiple CTEs but no group_by
    let plan = ReportPlan {
        report_name: "no_groupby".to_string(),
        fact_ctes: vec![
            FactCte {
                cte_name: "cte1".to_string(),
                fact_name: "fact1".to_string(),
                fact_table: "schema.table1".to_string(),
                measures: vec![PlannedMeasure {
                    alias: "m1".to_string(),
                    measure_name: "m1".to_string(),
                    aggregation: "SUM".to_string(),
                    source_expr: "col1".to_string(),
                }],
                applicable_filters: vec![],
                required_joins: vec![],
            },
            FactCte {
                cte_name: "cte2".to_string(),
                fact_name: "fact2".to_string(),
                fact_table: "schema.table2".to_string(),
                measures: vec![PlannedMeasure {
                    alias: "m2".to_string(),
                    measure_name: "m2".to_string(),
                    aggregation: "SUM".to_string(),
                    source_expr: "col2".to_string(),
                }],
                applicable_filters: vec![],
                required_joins: vec![],
            },
        ],
        group_by: vec![], // No group_by!
        output_columns: vec![],
    };

    let result = emitter.emit(&plan);
    assert!(result.is_err());
    // Multiple CTEs require group_by for joining
    let err_msg = format!("{:?}", result);
    assert!(err_msg.contains("group_by"));
}

#[test]
fn test_emit_aggregation_types() {
    let emitter = ReportEmitter::new();

    // Test each aggregation type
    let test_cases = vec![
        ("SUM", "col1", "SUM"),
        ("COUNT", "*", "COUNT"),
        ("COUNT DISTINCT", "user_id", "COUNT"),
        ("AVG", "amount", "AVG"),
        ("MIN", "created_at", "MIN"),
        ("MAX", "price", "MAX"),
    ];

    for (agg, source, expected_fn) in test_cases {
        let expr = emitter.build_aggregate_expr(agg, source);
        match expr {
            Expr::Function { name, .. } => {
                assert_eq!(name, expected_fn, "Expected {} for {}", expected_fn, agg);
            }
            _ => panic!("Expected function expression for {}", agg),
        }
    }
}

// ========================================================================
// Pivot Planner Tests
// ========================================================================

fn sample_pivot_model() -> Model {
    sample_model()
        .with_source(
            SourceEntity::new("time", "raw.time")
                .with_required_column("date_id", DataType::Date)
                .with_required_column("quarter", DataType::String)
                .with_required_column("month", DataType::String),
        )
        .with_relationship(Relationship::new(
            "orders_fact",
            "time",
            "order_date",
            "date_id",
            Cardinality::ManyToOne,
        ))
}

#[test]
fn test_pivot_plan_basic() {
    let model = sample_pivot_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = PivotPlanner::new(&model, &graph);

    let pivot = PivotReport::new("quarterly_sales")
        .with_row("customers.region")
        .with_columns_explicit("time.quarter", vec!["Q1".into(), "Q2".into(), "Q3".into(), "Q4".into()])
        .with_value("revenue", "orders_fact", "revenue");

    let plan = planner.plan(&pivot).unwrap();

    assert_eq!(plan.report_name, "quarterly_sales");
    assert_eq!(plan.row_dimensions.len(), 1);
    assert_eq!(plan.row_dimensions[0].entity, "customers");
    assert_eq!(plan.row_dimensions[0].column, "region");
    assert_eq!(plan.column_dimension.entity, "time");
    assert_eq!(plan.column_dimension.column, "quarter");
    assert!(matches!(plan.column_values, PivotColumnValues::Explicit(_)));
    assert_eq!(plan.value_measures.len(), 1);
}

#[test]
fn test_pivot_plan_unknown_measure() {
    let model = sample_pivot_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = PivotPlanner::new(&model, &graph);

    let pivot = PivotReport::new("bad_pivot")
        .with_row("customers.region")
        .with_columns("time.quarter")
        .with_value("bad", "orders_fact", "nonexistent");

    let result = planner.plan(&pivot);
    assert!(result.is_err());
    assert!(matches!(result, Err(PlanError::UnknownField { .. })));
}

#[test]
fn test_pivot_plan_invalid_dimension() {
    let model = sample_pivot_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = PivotPlanner::new(&model, &graph);

    let pivot = PivotReport::new("bad_pivot")
        .with_row("invalid_format") // No dot separator
        .with_columns("time.quarter")
        .with_value("revenue", "orders_fact", "revenue");

    let result = planner.plan(&pivot);
    assert!(result.is_err());
    assert!(matches!(result, Err(PlanError::InvalidReference(_))));
}

#[test]
fn test_pivot_emit_postgres() {
    let model = sample_pivot_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = PivotPlanner::new(&model, &graph);

    let pivot = PivotReport::new("quarterly_sales")
        .with_row("customers.region")
        .with_columns_explicit("time.quarter", vec!["Q1".into(), "Q2".into(), "Q3".into(), "Q4".into()])
        .with_value("revenue", "orders_fact", "revenue");

    let plan = planner.plan(&pivot).unwrap();
    let emitter = PivotEmitter::new();
    let sql = emitter.emit(&plan, Dialect::Postgres).unwrap();

    println!("PostgreSQL Pivot SQL:\n{}", sql);

    assert!(sql.contains("SELECT"));
    assert!(sql.contains("CASE WHEN"));
    assert!(sql.contains("Q1"));
    assert!(sql.contains("Q2"));
    assert!(sql.contains("GROUP BY"));
}

#[test]
fn test_pivot_emit_tsql() {
    let model = sample_pivot_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = PivotPlanner::new(&model, &graph);

    let pivot = PivotReport::new("quarterly_sales")
        .with_row("customers.region")
        .with_columns_explicit("time.quarter", vec!["Q1".into(), "Q2".into(), "Q3".into(), "Q4".into()])
        .with_value("revenue", "orders_fact", "revenue");

    let plan = planner.plan(&pivot).unwrap();
    let emitter = PivotEmitter::new();
    let sql = emitter.emit(&plan, Dialect::TSql).unwrap();

    println!("T-SQL Pivot SQL:\n{}", sql);

    assert!(sql.contains("PIVOT"));
    assert!(sql.contains("[Q1]"));
    assert!(sql.contains("[Q2]"));
    assert!(sql.contains("FOR"));
}

#[test]
fn test_pivot_emit_duckdb() {
    let model = sample_pivot_model();
    let graph = ModelGraph::from_model(model.clone()).unwrap();
    let planner = PivotPlanner::new(&model, &graph);

    let pivot = PivotReport::new("quarterly_sales")
        .with_row("customers.region")
        .with_columns("time.quarter")
        .with_value("revenue", "orders_fact", "revenue");

    let plan = planner.plan(&pivot).unwrap();
    let emitter = PivotEmitter::new();
    let sql = emitter.emit(&plan, Dialect::DuckDb).unwrap();

    println!("DuckDB Pivot SQL:\n{}", sql);

    assert!(sql.contains("PIVOT"));
    assert!(sql.contains("ON pivot_col"));
    assert!(sql.contains("USING"));
}

#[test]
fn test_pivot_emit_postgres_requires_explicit_columns() {
    let emitter = PivotEmitter::new();

    let plan = PivotPlan {
        report_name: "test".to_string(),
        row_dimensions: vec![PivotDimension {
            entity: "customers".to_string(),
            column: "region".to_string(),
            physical_table: None,
        }],
        column_dimension: PivotDimension {
            entity: "time".to_string(),
            column: "quarter".to_string(),
            physical_table: None,
        },
        column_values: PivotColumnValues::Dynamic, // Dynamic won't work for Postgres
        value_measures: vec![PivotMeasure {
            alias: "revenue".to_string(),
            aggregation: "SUM".to_string(),
            source_expr: "amount".to_string(),
        }],
        filters: vec![],
        totals: PivotTotals::default(),
        sort: None,
        source_fact: "orders_fact".to_string(),
        source_table: "analytics.orders_fact".to_string(),
    };

    let result = emitter.emit(&plan, Dialect::Postgres);
    assert!(result.is_err());
    let err_msg = format!("{:?}", result);
    assert!(err_msg.contains("explicit"));
}
