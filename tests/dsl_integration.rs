//! Integration tests for the DSL parser.
//!
//! These tests parse complete, realistic DSL examples from the spec and verify
//! the entire parsing pipeline (lexer + parser) produces the expected AST structure.

use mantis::dsl::{self, CalendarBody, Item};

#[test]
fn test_spec_example_simple_csv_report() {
    let input = r#"
        calendar auto {
            generate day+;
            range infer min 2020-01-01 max 2030-12-31;
            drill_path standard { day -> week -> month -> quarter -> year };
            week_start Monday;
        }

        table sales_export {
            source "exports/q4_sales.csv";
            atoms {
                deal_value decimal;
                quantity int;
            }
            times {
                close_date -> auto.day;
            }
            slicers {
                sales_rep string;
                region string;
                product_name string;
                deal_stage string;
            }
        }

        measures sales_export {
            revenue = { sum(@deal_value) };
            deals = { count(*) };
            avg_deal = { revenue / deals };
            won_deals = { count(*) } where { deal_stage = "Won" };
            win_rate = { won_deals / deals * 100 };
        }

        report rep_performance {
            from sales_export;
            use_date close_date;
            period this_quarter;
            group {
                auto.standard.month as "Month";
            }
            show {
                revenue as "Revenue";
                revenue.mtd as "MTD Revenue";
                deals as "Deals";
                win_rate as "Win Rate %";
                avg_deal as "Avg Deal Size";
            }
            sort revenue.desc;
            limit 20;
        }
    "#;

    let result = dsl::parse(input);

    // Print errors if any
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.is_ok(), "Expected successful parse");
    let model = result.model.expect("Expected model");

    // Verify structure
    assert_eq!(model.items.len(), 4); // calendar, table, measures, report

    // Check calendar
    assert!(matches!(&model.items[0].value, Item::Calendar(_)));
    if let Item::Calendar(calendar) = &model.items[0].value {
        assert_eq!(calendar.name.value, "auto");
        assert!(matches!(&calendar.body.value, CalendarBody::Generated(_)));
    }

    // Check table
    if let Item::Table(table) = &model.items[1].value {
        assert_eq!(table.name.value, "sales_export");
        assert_eq!(table.source.value, "exports/q4_sales.csv");
        assert_eq!(table.atoms.len(), 2);
        assert_eq!(table.times.len(), 1);
        assert_eq!(table.slicers.len(), 4); // 4 inline slicers
    } else {
        panic!("Expected table");
    }

    // Check measures
    if let Item::MeasureBlock(measures) = &model.items[2].value {
        assert_eq!(measures.table.value, "sales_export");
        assert_eq!(measures.measures.len(), 5);
    } else {
        panic!("Expected measures");
    }

    // Check report
    if let Item::Report(report) = &model.items[3].value {
        assert_eq!(report.name.value, "rep_performance");
        assert_eq!(report.from.len(), 1);
        assert_eq!(report.from[0].value, "sales_export");
        assert!(report.period.is_some());
        assert_eq!(report.group.len(), 1);
        assert_eq!(report.show.len(), 5);
        assert!(report.limit.is_some());
        assert_eq!(report.limit.as_ref().unwrap().value, 20);
    } else {
        panic!("Expected report");
    }
}

#[test]
fn test_database_model_with_dimensions() {
    let input = r#"
        defaults {
            fiscal_year_start July;
            week_start Monday;
            null_handling coalesce_zero;
        }

        calendar dates "dbo.dim_date" {
            day = date_value;
            week = week_start_date;
            month = month_start_date;
            quarter = quarter_start_date;
            year = year_start_date;
            fiscal_month = fiscal_month_start_date;
            fiscal_quarter = fiscal_quarter_start_date;
            fiscal_year = fiscal_year_start_date;

            drill_path standard { day -> week -> month -> quarter -> year };
            drill_path fiscal_hierarchy { day -> week -> fiscal_month -> fiscal_quarter -> fiscal_year };

            fiscal_year_start July;
            week_start Monday;
        }

        dimension customers {
            source "dbo.dim_customers";
            key customer_id;

            attributes {
                customer_name string;
                segment string;
                region string;
                country string;
                state string;
            }

            drill_path geo { country -> region -> state };
            drill_path org { segment -> customer_name };
        }

        dimension products {
            source "dbo.dim_products";
            key product_id;

            attributes {
                product_name string;
                category string;
                subcategory string;
                brand string;
                unit_cost decimal;
            }

            drill_path category { category -> subcategory -> brand -> product_name };
        }

        table fact_sales {
            source "dbo.fact_sales";

            atoms {
                revenue decimal;
                cost decimal;
                quantity int;
            }

            times {
                order_date_id -> dates.day;
                ship_date_id -> dates.day;
            }

            slicers {
                customer_id -> customers.customer_id;
                product_id -> products.product_id;
                channel string;
                segment via customer_id;
                category via product_id;
            }
        }

        measures fact_sales {
            revenue = { sum(@revenue) };
            cost = { sum(@cost) };
            quantity = { sum(@quantity) };
            order_count = { count(*) };

            margin = { revenue - cost };
            margin_pct = { margin / revenue * 100 };
            aov = { revenue / order_count };

            enterprise_rev = { sum(@revenue) } where { customers.segment = "Enterprise" };
            smb_rev = { sum(@revenue) } where { customers.segment = "SMB" };
        }

        report quarterly_sales_review {
            from fact_sales;
            use_date order_date_id;
            period last_12_months;

            group {
                dates.standard.quarter as "Quarter";
                dates.standard.month as "Month";
                customers.org.segment as "Customer Segment";
                products.category.category as "Product Category";
            }

            show {
                revenue as "Revenue";
                revenue.prior_year as "Prior Year";
                revenue.yoy_growth as "YoY Growth %";
                margin_pct as "Margin %";
                order_count as "Orders";
                aov as "Avg Order Value";
            }

            filter { customers.country = "USA" and channel = "Online" };

            sort revenue.desc;
        }
    "#;

    let result = dsl::parse(input);

    // Print errors if any
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.is_ok(), "Expected successful parse");
    let model = result.model.expect("Expected model");

    // Verify model has defaults
    assert!(model.defaults.is_some());

    // Count items: calendar, 2 dimensions, table, measures, report = 6
    assert_eq!(model.items.len(), 6);

    // Check calendar is physical
    if let Item::Calendar(calendar) = &model.items[0].value {
        assert_eq!(calendar.name.value, "dates");
        assert!(matches!(&calendar.body.value, CalendarBody::Physical(_)));
        if let CalendarBody::Physical(physical) = &calendar.body.value {
            assert_eq!(physical.source.value, "dbo.dim_date");
            assert_eq!(physical.grain_mappings.len(), 8);
            assert_eq!(physical.drill_paths.len(), 2);
        }
    } else {
        panic!("Expected calendar");
    }

    // Check dimensions
    if let Item::Dimension(dim) = &model.items[1].value {
        assert_eq!(dim.name.value, "customers");
        assert_eq!(dim.source.value, "dbo.dim_customers");
        assert_eq!(dim.key.value, "customer_id");
        assert_eq!(dim.attributes.len(), 5);
        assert_eq!(dim.drill_paths.len(), 2);
    } else {
        panic!("Expected dimension");
    }

    if let Item::Dimension(dim) = &model.items[2].value {
        assert_eq!(dim.name.value, "products");
        assert_eq!(dim.attributes.len(), 5);
        assert_eq!(dim.drill_paths.len(), 1);
    } else {
        panic!("Expected dimension");
    }

    // Check table with FK slicers
    if let Item::Table(table) = &model.items[3].value {
        assert_eq!(table.name.value, "fact_sales");
        assert_eq!(table.atoms.len(), 3);
        assert_eq!(table.times.len(), 2);
        assert_eq!(table.slicers.len(), 5); // 2 FK, 1 inline, 2 via
    } else {
        panic!("Expected table");
    }

    // Check measures
    if let Item::MeasureBlock(measures) = &model.items[4].value {
        assert_eq!(measures.table.value, "fact_sales");
        assert_eq!(measures.measures.len(), 9);
    } else {
        panic!("Expected measures");
    }

    // Check report
    if let Item::Report(report) = &model.items[5].value {
        assert_eq!(report.name.value, "quarterly_sales_review");
        assert_eq!(report.group.len(), 4);
        assert_eq!(report.show.len(), 6);
        assert!(report.filter.is_some());
        assert_eq!(report.sort.len(), 1);
    } else {
        panic!("Expected report");
    }
}

#[test]
fn test_generated_calendar_with_fiscal() {
    let input = r#"
        calendar auto {
            generate month+;
            include fiscal[July];
            range infer min 2020-01-01 max 2030-12-31;

            drill_path standard { month -> quarter -> year };
            drill_path fiscal_hierarchy { fiscal_month -> fiscal_quarter -> fiscal_year };
        }

        table monthly_targets {
            source "finance/targets_2024.csv";

            atoms {
                target_revenue decimal;
                target_units int;
            }

            times {
                target_month -> auto.month;
            }

            slicers {
                region string;
                product_line string;
            }
        }

        measures monthly_targets {
            target = { sum(@target_revenue) };
            target_units = { sum(@target_units) };
        }

        report target_tracking {
            from monthly_targets;
            use_date target_month;
            period this_fiscal_year;

            group {
                auto.fiscal_hierarchy.fiscal_quarter as "Fiscal Quarter";
                auto.standard.month as "Month";
            }

            show {
                target as "Target";
                target.fiscal_ytd as "Fiscal YTD Target";
                target_units as "Target Units";
            }

            sort revenue.desc;
        }
    "#;

    let result = dsl::parse(input);

    // Print errors if any
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.is_ok(), "Expected successful parse");
    let model = result.model.expect("Expected model");

    // Verify structure: calendar, table, measures, report = 4
    assert_eq!(model.items.len(), 4);

    // Check generated calendar with fiscal
    if let Item::Calendar(calendar) = &model.items[0].value {
        assert_eq!(calendar.name.value, "auto");
        if let CalendarBody::Generated(gen) = &calendar.body.value {
            assert!(gen.fiscal.is_some());
            assert_eq!(gen.drill_paths.len(), 2);
        } else {
            panic!("Expected generated calendar");
        }
    } else {
        panic!("Expected calendar");
    }

    // Check table with monthly grain
    if let Item::Table(table) = &model.items[1].value {
        assert_eq!(table.name.value, "monthly_targets");
        assert_eq!(table.times.len(), 1);
    } else {
        panic!("Expected table");
    }

    // Check measures
    if let Item::MeasureBlock(measures) = &model.items[2].value {
        assert_eq!(measures.measures.len(), 2);
    } else {
        panic!("Expected measures");
    }

    // Check report with fiscal period
    if let Item::Report(report) = &model.items[3].value {
        assert_eq!(report.name.value, "target_tracking");
        assert!(report.period.is_some());
        assert_eq!(report.group.len(), 2);
        assert_eq!(report.show.len(), 3);
    } else {
        panic!("Expected report");
    }
}

#[test]
fn test_complex_multi_table_report() {
    let input = r#"
        defaults {
            week_start Monday;
            null_handling null_on_zero;
        }

        calendar auto {
            generate day+;
            range infer min 2020-01-01 max 2030-12-31;

            drill_path standard { day -> week -> month -> quarter -> year };
        }

        dimension customers {
            source "crm.accounts";
            key account_id;

            attributes {
                company_name string;
                industry string;
                region string;
                owner string;
                tier string;
            }

            drill_path geo { region };
            drill_path org { tier -> company_name };
        }

        dimension products {
            source "inventory.products";
            key sku;

            attributes {
                product_name string;
                category string;
                brand string;
            }

            drill_path category { category -> brand -> product_name };
        }

        table orders {
            source "erp.order_lines";

            atoms {
                line_total decimal;
                qty int;
                discount decimal;
            }

            times {
                order_date -> auto.day;
            }

            slicers {
                account_id -> customers.account_id;
                sku -> products.sku;
                order_status string;
                sales_channel string;
                region via account_id;
                tier via account_id;
                category via sku;
            }
        }

        table returns {
            source "support.return_requests";

            atoms {
                refund_amount decimal;
                return_qty int;
            }

            times {
                return_date -> auto.day;
            }

            slicers {
                account_id -> customers.account_id;
                sku -> products.sku;
                reason_code string;
                region via account_id;
                category via sku;
            }
        }

        measures orders {
            revenue = { sum(@line_total) };
            units = { sum(@qty) };
            order_count = { count(*) };
            avg_order = { revenue / order_count };
            discount_given = { sum(@discount) };
        }

        measures returns {
            refunds = { sum(@refund_amount) };
            return_units = { sum(@return_qty) };
            return_count = { count(*) };
        }

        report revenue_vs_returns {
            from orders, returns;
            use_date order_date, return_date;
            period last_6_months;

            group {
                auto.standard.month as "Month";
                products.category.category as "Category";
                customers.geo.region as "Region";
            }

            show {
                revenue as "Revenue";
                refunds as "Refunds";
                net_revenue = { revenue - refunds } as "Net Revenue";
                return_rate = { refunds / revenue * 100 } as "Return Rate %";
            }

            sort revenue.desc;
        }
    "#;

    let result = dsl::parse(input);

    // Print errors if any
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.is_ok(), "Expected successful parse");
    let model = result.model.expect("Expected model");

    // Verify model has defaults
    assert!(model.defaults.is_some());

    // Count items: calendar, 2 dimensions, 2 tables, 2 measures, 1 report = 8
    assert_eq!(model.items.len(), 8);

    // Check multi-table report
    if let Item::Report(report) = &model.items[7].value {
        assert_eq!(report.name.value, "revenue_vs_returns");
        assert_eq!(report.from.len(), 2); // 2 tables
        assert_eq!(report.from[0].value, "orders");
        assert_eq!(report.from[1].value, "returns");
        assert_eq!(report.use_date.len(), 2); // 2 date columns
        assert_eq!(report.use_date[0].value, "order_date");
        assert_eq!(report.use_date[1].value, "return_date");
        assert_eq!(report.group.len(), 3);
        assert_eq!(report.show.len(), 4); // 2 measures + 2 inline measures
    } else {
        panic!("Expected report");
    }
}

#[test]
fn test_compact_report_with_inline_measures() {
    let input = r#"
        calendar auto {
            generate day+;
            range infer min 2020-01-01 max 2030-12-31;
            drill_path standard { day -> week -> month -> quarter -> year };
        }

        table sales {
            source "data/sales.csv";
            atoms { amount decimal; }
            times { sale_date -> auto.day; }
            slicers { region string; }
        }

        measures sales {
            revenue = { sum(@amount) };
        }

        report quick_summary {
            from sales;
            use_date sale_date;
            period this_month;
            group { auto.standard.week; }
            show { revenue; revenue.wow_growth as "WoW Growth %"; }
            sort revenue.desc;
            limit 10;
        }
    "#;

    let result = dsl::parse(input);

    // Print errors if any
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.is_ok(), "Expected successful parse");
    let model = result.model.expect("Expected model");

    // Verify compact structure
    assert_eq!(model.items.len(), 4);

    // Check report
    if let Item::Report(report) = &model.items[3].value {
        assert_eq!(report.name.value, "quick_summary");
        assert_eq!(report.group.len(), 1);
        assert_eq!(report.show.len(), 2);
        assert!(report.limit.is_some());
        assert_eq!(report.limit.as_ref().unwrap().value, 10);
    } else {
        panic!("Expected report");
    }
}

#[test]
fn test_measure_filters_and_null_handling() {
    let input = r#"
        calendar auto {
            generate day+;
            range infer min 2020-01-01 max 2030-12-31;
            drill_path standard { day -> month -> year };
        }

        dimension customers {
            source "dbo.customers";
            key customer_id;
            attributes {
                segment string;
                country string;
            }
            drill_path org { segment };
        }

        table sales {
            source "dbo.sales";
            atoms { revenue decimal; quantity int; }
            times { order_date -> auto.day; }
            slicers {
                customer_id -> customers.customer_id;
                channel string;
                segment via customer_id;
            }
        }

        measures sales {
            revenue = { sum(@revenue) };
            order_count = { count(*) };
            aov = { revenue / order_count } null coalesce_zero;

            enterprise_rev = { sum(@revenue) } where { customers.segment = "Enterprise" };
            online_rev = { sum(@revenue) } where { channel = "Online" };
        }

        report filtered_metrics {
            from sales;
            use_date order_date;
            period last_month;
            group { auto.standard.month; }
            show { revenue; enterprise_rev; online_rev; aov; }
        }
    "#;

    let result = dsl::parse(input);

    // Print errors if any
    for diag in &result.diagnostics {
        eprintln!("{}", diag);
    }

    assert!(result.is_ok(), "Expected successful parse");
    let model = result.model.expect("Expected model");

    // Check measures with filters
    if let Item::MeasureBlock(measures) = &model.items[3].value {
        assert_eq!(measures.table.value, "sales");
        assert_eq!(measures.measures.len(), 5);

        // Check that some measures have filters
        let filtered_count = measures
            .measures
            .iter()
            .filter(|m| m.filter.is_some())
            .count();
        assert_eq!(filtered_count, 2); // enterprise_rev, online_rev
    } else {
        panic!("Expected measures");
    }
}
