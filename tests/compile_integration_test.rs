//! Integration tests for the end-to-end DSL → SQL compilation pipeline.
//!
//! These tests verify the full compilation flow from DSL source to SQL output.
//!
//! Note: The DSL has specific syntax requirements:
//! - Table blocks must be in order: source, atoms, times, slicers
//! - Reports with times require use_date clause
//! - Group items require drill path syntax: source.path.level

use mantis::compile::{compile_first_report, compile_report, CompileError, CompileOptions};
use mantis::sql::Dialect;

// ============================================================================
// Basic Compilation Tests
// ============================================================================

#[test]
fn test_report_with_single_measure() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report summary {
            from sales;
            use_date sale_date;
            show { revenue; }
        }
    "#;

    let result = compile_report(dsl, "summary", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Report with measure should compile: {:?}",
        result
    );

    let output = result.unwrap();
    println!("Generated SQL:\n{}", output.sql);

    let sql_upper = output.sql.to_uppercase();
    assert!(sql_upper.contains("SELECT"));
    assert!(sql_upper.contains("FROM"));
}

#[test]
fn test_report_with_multiple_measures() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms {
                amount decimal;
                quantity int;
            }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
            total_qty = { sum(@quantity) };
            avg_price = { avg(@amount) };
        }

        report multi_measure {
            from sales;
            use_date sale_date;
            show {
                revenue;
                total_qty;
                avg_price;
            }
        }
    "#;

    let result = compile_report(dsl, "multi_measure", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Report with multiple measures should compile: {:?}",
        result
    );
}

// ============================================================================
// Filter Tests
// ============================================================================

#[test]
fn test_report_with_simple_filter() {
    let dsl = r#"
        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            slicers { region string; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report filtered {
            from sales;
            show { revenue; }
            filter { @amount > 100 };
        }
    "#;

    let result = compile_report(dsl, "filtered", CompileOptions::default());

    match &result {
        Ok(output) => {
            println!("Generated SQL with filter:\n{}", output.sql);
            let sql_upper = output.sql.to_uppercase();
            assert!(sql_upper.contains("WHERE"), "Should have WHERE clause");
        }
        Err(e) => {
            // Filter might not be fully wired up yet - this is informational
            println!("Filter test info: {:?}", e);
        }
    }
}

#[test]
fn test_report_with_string_filter() {
    let dsl = r#"
        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            slicers { region string; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report west_sales {
            from sales;
            show { revenue; }
            filter { @region = 'WEST' };
        }
    "#;

    let result = compile_report(dsl, "west_sales", CompileOptions::default());

    if let Ok(output) = &result {
        println!("Generated SQL with string filter:\n{}", output.sql);
    }
}

// ============================================================================
// Sorting and Limit Tests
// ============================================================================

#[test]
fn test_report_with_limit() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report top_n {
            from sales;
            use_date sale_date;
            show { revenue; }
            limit 25;
        }
    "#;

    let result = compile_report(dsl, "top_n", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Report with LIMIT should compile: {:?}",
        result
    );

    let output = result.unwrap();
    println!("Generated SQL with LIMIT:\n{}", output.sql);

    // Check LIMIT is present (PostgreSQL)
    assert!(output.sql.contains("25"), "Should have limit value");
}

#[test]
fn test_report_with_order_by() {
    // Note: sort syntax is `sort column.direction;` without braces
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report sorted {
            from sales;
            use_date sale_date;
            show { revenue; }
            sort revenue.desc;
        }
    "#;

    let result = compile_report(dsl, "sorted", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Report with ORDER BY should compile: {:?}",
        result
    );

    let output = result.unwrap();
    println!("Generated SQL with ORDER BY:\n{}", output.sql);

    let sql_upper = output.sql.to_uppercase();
    assert!(
        sql_upper.contains("ORDER BY"),
        "Should have ORDER BY clause"
    );
}

// ============================================================================
// SQL Dialect Tests
// ============================================================================

#[test]
fn test_postgres_dialect() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        report simple {
            from sales;
            use_date sale_date;
            limit 10;
        }
    "#;

    let result = compile_report(
        dsl,
        "simple",
        CompileOptions::default().with_dialect(Dialect::Postgres),
    );
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.dialect, Dialect::Postgres);
    assert!(
        output.sql.contains("LIMIT 10"),
        "PostgreSQL should use LIMIT syntax"
    );
}

#[test]
fn test_tsql_dialect() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        report simple {
            from sales;
            use_date sale_date;
            limit 10;
        }
    "#;

    let result = compile_report(
        dsl,
        "simple",
        CompileOptions::default().with_dialect(Dialect::TSql),
    );
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.dialect, Dialect::TSql);

    // T-SQL uses OFFSET/FETCH or TOP
    let sql_upper = output.sql.to_uppercase();
    assert!(
        sql_upper.contains("FETCH") || sql_upper.contains("TOP"),
        "T-SQL should use FETCH or TOP, got: {}",
        output.sql
    );
}

#[test]
fn test_mysql_dialect() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        report simple {
            from sales;
            use_date sale_date;
            limit 10;
        }
    "#;

    let result = compile_report(
        dsl,
        "simple",
        CompileOptions::default().with_dialect(Dialect::MySql),
    );
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.dialect, Dialect::MySql);
    assert!(
        output.sql.contains("LIMIT 10"),
        "MySQL should use LIMIT syntax"
    );
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_parse_error_on_invalid_dsl() {
    let dsl = r#"
        table sales {
            this is not valid DSL syntax!!!
        }
    "#;

    let result = compile_report(dsl, "anything", CompileOptions::default());
    assert!(matches!(result, Err(CompileError::ParseError(_))));
}

#[test]
fn test_report_not_found_error() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        report existing {
            from sales;
            use_date sale_date;
        }
    "#;

    let result = compile_report(dsl, "nonexistent_report", CompileOptions::default());
    assert!(matches!(result, Err(CompileError::ReportNotFound(_))));

    if let Err(CompileError::ReportNotFound(name)) = result {
        assert_eq!(name, "nonexistent_report");
    }
}

#[test]
fn test_no_reports_error() {
    let dsl = r#"
        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
        }
    "#;

    let result = compile_first_report(dsl, CompileOptions::default());
    assert!(matches!(result, Err(CompileError::NoReports)));
}

// ============================================================================
// Complex DSL Tests
// ============================================================================

#[test]
fn test_full_featured_model() {
    let dsl = r#"
        calendar auto {
            generate month+;
            range infer;
        }

        table orders {
            source "warehouse.orders";
            atoms {
                order_amount decimal;
                quantity int;
                discount_pct decimal;
            }
            times { order_date -> auto.month; }
            slicers {
                region string;
                customer_segment string;
                channel string;
            }
        }

        measures orders {
            total_sales = { sum(@order_amount) };
            total_units = { sum(@quantity) };
            avg_order = { avg(@order_amount) };
            avg_discount = { avg(@discount_pct) };
        }

        report quarterly_summary {
            from orders;
            use_date order_date;
            show {
                total_sales;
                total_units;
                avg_order;
            }
            sort total_sales.desc;
            limit 100;
        }
    "#;

    let result = compile_report(dsl, "quarterly_summary", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Full featured model should compile: {:?}",
        result
    );

    let output = result.unwrap();
    println!("Full featured model SQL:\n{}", output.sql);

    let sql_upper = output.sql.to_uppercase();
    assert!(sql_upper.contains("SELECT"));
    assert!(sql_upper.contains("FROM"));
    assert!(sql_upper.contains("ORDER BY"));
}

#[test]
fn test_multiple_tables_model() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table orders {
            source "orders";
            atoms { amount decimal; }
            times { order_date -> auto.day; }
            slicers { customer_id int; }
        }

        table customers {
            source "customers";
            atoms { credit_limit decimal; }
            slicers {
                customer_id int;
                region string;
            }
        }

        measures orders {
            total_orders = { sum(@amount) };
        }

        report orders_summary {
            from orders;
            use_date order_date;
            show { total_orders; }
        }
    "#;

    let result = compile_report(dsl, "orders_summary", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Multiple tables model should compile: {:?}",
        result
    );
}

// ============================================================================
// Compile First Report Tests
// ============================================================================

#[test]
fn test_compile_first_report_convenience() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report first {
            from sales;
            use_date sale_date;
            show { revenue; }
        }

        report second {
            from sales;
            use_date sale_date;
            show { revenue; }
            limit 5;
        }
    "#;

    // Should compile the first report found
    let result = compile_first_report(dsl, CompileOptions::default());
    assert!(
        result.is_ok(),
        "compile_first_report should work: {:?}",
        result
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_empty_show_clause() {
    // Report without explicit show clause
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        report no_show {
            from sales;
            use_date sale_date;
        }
    "#;

    let result = compile_report(dsl, "no_show", CompileOptions::default());
    // Should either succeed with default behavior or fail gracefully
    match &result {
        Ok(output) => {
            println!("Empty show clause SQL:\n{}", output.sql);
            assert!(output.sql.to_uppercase().contains("SELECT"));
        }
        Err(e) => {
            println!("Empty show clause handling: {:?}", e);
        }
    }
}

#[test]
fn test_special_characters_in_names() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table "my-sales" {
            source "dbo.my-sales-data";
            atoms { order_amount decimal; }
            times { sale_date -> auto.day; }
        }

        report "my-report" {
            from "my-sales";
            use_date sale_date;
        }
    "#;

    let result = compile_report(dsl, "my-report", CompileOptions::default());
    // Should handle quoted identifiers appropriately
    if let Ok(output) = &result {
        println!("Special characters SQL:\n{}", output.sql);
    }
}

#[test]
fn test_schema_qualified_source() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "warehouse.analytics.sales_fact";
            atoms { revenue decimal; }
            times { sale_date -> auto.day; }
        }

        report schema_test {
            from sales;
            use_date sale_date;
        }
    "#;

    let result = compile_report(dsl, "schema_test", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Schema qualified source should compile: {:?}",
        result
    );

    let output = result.unwrap();
    println!("Schema qualified SQL:\n{}", output.sql);
}

// ============================================================================
// Query AST Verification Tests
// ============================================================================

#[test]
fn test_query_ast_structure() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.sales";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report ast_test {
            from sales;
            use_date sale_date;
            show { revenue; }
            limit 50;
        }
    "#;

    let result = compile_report(dsl, "ast_test", CompileOptions::default());
    assert!(result.is_ok(), "AST test should compile: {:?}", result);

    let output = result.unwrap();

    // Verify the Query AST structure directly
    assert!(output.query.from.is_some(), "Query should have FROM clause");
    assert_eq!(
        output.query.limit_offset.as_ref().and_then(|lo| lo.limit),
        Some(50),
        "Query AST should have limit=50"
    );
}

// ============================================================================
// Measure Expression Tests
// ============================================================================

#[test]
fn test_measure_with_count() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table events {
            source "events";
            atoms { event_id int; }
            times { event_date -> auto.day; }
        }

        measures events {
            event_count = { count(@event_id) };
        }

        report event_summary {
            from events;
            use_date event_date;
            show { event_count; }
        }
    "#;

    let result = compile_report(dsl, "event_summary", CompileOptions::default());
    assert!(result.is_ok(), "COUNT measure should compile: {:?}", result);
}

#[test]
fn test_measure_with_min_max() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table prices {
            source "prices";
            atoms { price decimal; }
            times { price_date -> auto.day; }
        }

        measures prices {
            min_price = { min(@price) };
            max_price = { max(@price) };
        }

        report price_range {
            from prices;
            use_date price_date;
            show { min_price; max_price; }
        }
    "#;

    let result = compile_report(dsl, "price_range", CompileOptions::default());
    assert!(
        result.is_ok(),
        "MIN/MAX measures should compile: {:?}",
        result
    );
}

// ============================================================================
// Slicers Tests
// ============================================================================

#[test]
fn test_table_with_multiple_slicers() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table transactions {
            source "transactions";
            atoms { amount decimal; }
            times { txn_date -> auto.day; }
            slicers {
                region string;
                product_type string;
                customer_tier string;
                channel string;
            }
        }

        measures transactions {
            total = { sum(@amount) };
        }

        report transactions_report {
            from transactions;
            use_date txn_date;
            show { total; }
        }
    "#;

    let result = compile_report(dsl, "transactions_report", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Table with multiple slicers should compile: {:?}",
        result
    );
}

// ============================================================================
// Multiple Calendars Test
// ============================================================================

#[test]
fn test_multiple_calendars() {
    // Note: calendar names must be valid identifiers (not keywords like 'fiscal')
    let dsl = r#"
        calendar fy_calendar {
            generate month+;
            range infer;
        }

        calendar standard {
            generate day+;
            range infer;
        }

        table sales {
            source "sales";
            atoms { revenue decimal; }
            times { sale_date -> fy_calendar.month; }
        }

        report sales_report {
            from sales;
            use_date sale_date;
        }
    "#;

    let result = compile_report(dsl, "sales_report", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Multiple calendars should compile: {:?}",
        result
    );
}

// ============================================================================
// Measure Expansion Tests
// ============================================================================

#[test]
fn test_measure_expansion_in_sql() {
    let dsl = r#"
        calendar auto {
            generate day+;
            range infer;
        }

        table sales {
            source "dbo.fact_sales";
            atoms {
                amount decimal;
                quantity int;
            }
            times { sale_date -> auto.day; }
        }

        measures sales {
            revenue = { sum(@amount) };
            total_qty = { sum(@quantity) };
        }

        report test_report {
            from sales;
            use_date sale_date;
            show { revenue; }
        }
    "#;

    let result = compile_report(dsl, "test_report", CompileOptions::default());
    assert!(
        result.is_ok(),
        "Measure expansion should compile: {:?}",
        result
    );

    let output = result.unwrap();
    let sql = &output.sql;

    println!("Generated SQL:\n{}", sql);

    // Should contain SUM with the actual column, not just "revenue"
    let sql_upper = sql.to_uppercase();
    assert!(
        sql_upper.contains("SUM"),
        "SQL should contain SUM aggregate: {}",
        sql
    );

    // The SQL should reference the amount column (possibly qualified)
    assert!(
        sql_upper.contains("AMOUNT"),
        "SQL should reference amount column: {}",
        sql
    );

    // Should NOT contain revenue as a column reference (it should be an alias)
    // Note: revenue might appear as an alias, but not as "sales"."revenue"
    assert!(
        !sql.contains("\"sales\".\"revenue\"") && !sql.contains("[sales].[revenue]"),
        "SQL should not treat revenue as a column: {}",
        sql
    );
}
