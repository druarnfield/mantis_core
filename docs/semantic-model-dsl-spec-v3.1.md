# Semantic Model DSL Specification v3.1

A domain-specific language for defining semantic data models that work across flat files, wide tables, scattered tables, and star schemas. Designed for analysts who think in terms of "numbers, dates, and slicers" rather than "facts and dimensions."

## Design Philosophy

1. **Mental model over technical model** — Users think about atoms (numbers), times (when), and slicers (how to slice). Not facts and dimensions.
2. **Joins are optional** — Wide tables and CSVs work without any joins. Star schemas use explicit joins. Scattered tables mix both.
3. **Time intelligence is built-in** — YTD, YoY, rolling windows are suffixes on measures, not SQL.
4. **Expressions are SQL** — All expressions are wrapped in `{ }` and parsed as SQL, giving full expression power.
5. **Periods are compile-time** — No SQL parameterization. `last_12_months` evaluates to concrete dates.
6. **Grain safety** — Multi-fact queries automatically resolve to the lowest common grain.
7. **Explicit over implicit** — Scoping rules, NULL handling, and join semantics are always explicit.
8. **Drill paths are query parameters** — Named hierarchies define valid aggregation levels for reports.

---

## Syntax Conventions

- All statements are terminated with `;`
- Whitespace is not significant (statements can span multiple lines or be on one line)
- SQL expressions are always wrapped in `{ }` (measures, filters, conditions)
- Joins use `->` arrow syntax
- Labels use `as` followed by quoted text: `as "Customer Segment"`
- Identifiers are unquoted alphanumeric with underscores: `customer_id`, `revenue_ytd`
- Comments use `//` for single-line and `/* */` for multi-line

---

## Core Concepts

### Atoms

Numeric columns that can be aggregated. The raw material for measures. Atoms are always prefixed with `@` when referenced in expressions to distinguish them from measures.

```
atoms {
    amount decimal;
    quantity int;
    cost decimal;
}
```

**Types:** `int`, `decimal`, `float`

When referenced in measures:
```
measures my_table {
    total_amount = { sum(@amount) };      // @amount references the atom
    total_cost = { sum(@cost) };
    margin = { total_amount - total_cost }; // no @ means measure reference
}
```

### Times

Date/timestamp columns that enable time-based analysis. Bind to a calendar grain level for time intelligence.

```
times {
    order_date -> calendar.day;           // daily data, joined to calendar
    month_end -> calendar.month;          // monthly data, joined at month grain
}
```

The `->` syntax binds the column to a calendar grain level. The compiler uses this for joins and time intelligence operations.

### Slicers

Columns you slice data by. Can be inline (string/categorical), joined to dimension tables, or calculated from expressions.

```
slicers {
    region string;                        // inline slicer
    customer_id -> customers.customer_id; // FK to dimension
    deal_size string = { case when @amount < 1000 then 'Small' when @amount < 10000 then 'Medium' else 'Large' end };  // calculated
}
```

### Measures

Aggregations over atoms, with optional filters. Expressions are wrapped in `{ }` and parsed as SQL.

```
measures my_table {
    revenue = { sum(@amount) };
    order_count = { count(*) };
    margin = { revenue - cost };
    aov = { revenue / order_count };
    enterprise_rev = { sum(@amount) } where { segment = 'Enterprise' };
}
```

---

## Model Defaults

Optional block to set model-wide defaults. Reduces repetition across tables and reports.

```
defaults {
    calendar dates;                    // default calendar for time bindings
    fiscal_year_start July;            // fiscal year start month
    week_start Monday;                 // first day of week
    null_handling coalesce_zero;       // division NULL handling
    decimal_places 2;                  // default decimal precision
}
```

**`null_handling` options:**
- `coalesce_zero` — Division by zero/null returns 0
- `null_on_zero` — Division by zero/null returns NULL (default)
- `error_on_zero` — Division by zero raises a compile error if not explicitly handled

---

## Syntax Reference

### Calendar

Defines a date reference for time intelligence. Can reference a physical table or generate an ephemeral date spine.

Calendars expose grain levels (`day`, `week`, `month`, `quarter`, `year`) as join targets. Times columns bind to these grain levels directly.

#### Physical Calendar

References an existing date dimension table. Each grain level maps to a period-start column.

```
calendar <name> "<source_table>" {
    // Grain level mappings (column must contain period-start dates)
    day = <column>;
    week = <column>;
    month = <column>;
    quarter = <column>;
    year = <column>;
    
    // Optional fiscal grain levels
    fiscal_month = <column>;
    fiscal_quarter = <column>;
    fiscal_year = <column>;
    
    // Named drill paths define valid aggregation hierarchies
    drill_path <name> { <level> -> <level> -> ... };
    
    // Calendar settings
    fiscal_year_start <month>;
    week_start <day>;
}
```

**Example:**

```
calendar dates "dbo.dim_date" {
    day = date_value;
    week = week_start_date;
    month = month_start_date;
    quarter = quarter_start_date;
    year = year_start_date;
    fiscal_month = fiscal_month_start;
    fiscal_quarter = fiscal_quarter_start;
    fiscal_year = fiscal_year_start;
    
    drill_path standard { day -> week -> month -> quarter -> year };
    drill_path fiscal { day -> week -> fiscal_month -> fiscal_quarter -> fiscal_year };
    
    fiscal_year_start July;
    week_start Monday;
}
```

#### Generated Calendar (Ephemeral)

For flat files and sources without a physical date table. Generates a CTE date spine at query time.

```
calendar <name> {
    generate <grain>+;
    generate <grain>+ include fiscal[<month>];
    
    range <start> to <end>;
    range infer;
    range infer min <floor_date> max <ceiling_date>;
    
    drill_path <name> { <level> -> <level> -> ... };
    
    week_start <day>;
}
```

**Grain specifier:**

The `+` suffix indicates "this grain and all coarser grains":

| Specifier | Grains Generated |
|-----------|------------------|
| `minute+` | minute, hour, day, week, month, quarter, year |
| `hour+` | hour, day, week, month, quarter, year |
| `day+` | day, week, month, quarter, year |
| `week+` | week, month, quarter, year |
| `month+` | month, quarter, year |
| `quarter+` | quarter, year |
| `year+` | year |

**Fiscal grains:**

Use `include fiscal[<month>]` to add fiscal grain levels. The month specifies when the fiscal year starts:

| Specifier | Additional Grains |
|-----------|-------------------|
| `include fiscal[July]` | fiscal_month, fiscal_quarter, fiscal_year (July start) |
| `include fiscal[October]` | fiscal_month, fiscal_quarter, fiscal_year (October start) |

**Range inference rules:**

When `range infer` is specified, the compiler scans all `times` columns bound to this calendar and uses their min/max values. To protect against bad data:

- `range infer` — Uses raw min/max from data
- `range infer min 2020-01-01` — Floor the start date
- `range infer max 2030-12-31` — Ceiling the end date
- `range infer min 2020-01-01 max 2030-12-31` — Both bounds

**Examples:**

Daily data with standard grains:
```
calendar auto {
    generate day+;
    range infer min 2020-01-01 max 2030-12-31;
    
    drill_path standard { day -> week -> month -> quarter -> year };
    
    week_start Monday;
}
```

Monthly data with fiscal support:
```
calendar auto {
    generate month+ include fiscal[July];
    range infer min 2020-01-01 max 2030-12-31;
    
    drill_path standard { month -> quarter -> year };
    drill_path fiscal { fiscal_month -> fiscal_quarter -> fiscal_year };
}
```

Minute-level data for operational reporting:
```
calendar auto {
    generate minute+;
    range infer min 2024-01-01 max 2024-12-31;
    
    drill_path operational { minute -> hour -> day };
    drill_path standard { day -> week -> month -> quarter -> year };
}
```

---

### Dimension

Defines a dimension table for star schema joins.

```
dimension <name> {
    source "<schema.table>";
    key <column>;
    
    attributes {
        <name> <type>;
        ...
    }
    
    // Named drill paths define valid aggregation hierarchies
    drill_path <name> { <level> -> <level> -> ... };
}
```

**Types:** `string`, `int`, `decimal`, `float`, `bool`, `date`, `timestamp`

**Example:**

```
dimension customers {
    source "dbo.dim_customers";
    key customer_id;
    
    attributes {
        customer_name string;
        email string;
        segment string;
        region string;
        country string;
        state string;
        city string;
    }
    
    drill_path geo { country -> region -> state -> city };
    drill_path org { segment -> customer_name };
}

dimension products {
    source "dbo.dim_products";
    key product_id;
    
    attributes {
        product_name string;
        sku string;
        category string;
        subcategory string;
        brand string;
        unit_cost decimal;
    }
    
    drill_path category { category -> subcategory -> brand -> product_name };
}
```

---

### Table

The universal container for any data source. Works for CSVs, wide tables, fact tables, or any table with numbers.

```
table <name> {
    source "<schema.table>" | "<path/to/file.csv>";
    
    atoms {
        <name> <type>;
        ...
    }
    
    times {
        <name> -> <calendar>.<grain_level>;
        ...
    }
    
    slicers {
        <name> <type>;
        <name> -> <dimension>.<key_column>;
        <name> via <fk_slicer>;
        <name> <type> = { <sql_expression> };
        ...
    }
}
```

**Atom types:** `int`, `decimal`, `float`

**Slicer types:** `string`, `int`, `decimal`, `float`, `bool`, `date`, `timestamp`

#### The `via` Keyword

The `via` keyword creates a derived slicer that inherits through an existing dimension join. This avoids duplicating the FK relationship.

```
slicers {
    customer_id -> customers.customer_id;  // explicit FK join
    segment via customer_id;                // inherits through customer_id join
    region via customer_id;                 // also inherits through customer_id
}
```

The `via` column must be a slicer with a `->` join in the same table. The derived slicer's name must match an attribute in the target dimension.

#### Calculated Slicers

Calculated slicers use SQL expressions to derive categorical values from atoms or other columns:

```
slicers {
    deal_size string = { case when @amount < 1000 then 'Small' when @amount < 10000 then 'Medium' else 'Large' end };
    is_high_value bool = { @amount > 50000 };
    order_year int = { year(order_date) };
}
```

The type declaration is required and allows the compiler to validate the expression returns the expected type.

**Examples:**

Flat file (CSV, no joins):
```
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
        deal_size string = { case when @deal_value < 1000 then 'Small' when @deal_value < 10000 then 'Medium' else 'Large' end };
    }
}
```

Star schema fact table:
```
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
```

Monthly aggregated data:
```
table monthly_targets {
    source "finance/targets_2024.csv";
    
    atoms {
        target_revenue decimal;
        target_units int;
    }
    
    times {
        target_month -> auto.month;  // monthly grain
    }
    
    slicers {
        region string;
        product_line string;
    }
}
```

---

### Measures

Define aggregations and calculations for a table. Expressions are wrapped in `{ }` and parsed as SQL.

```
measures <table_name> {
    <name> = { <expression> };
    <name> = { <expression> } where { <condition> };
    ...
}
```

#### Atom vs Measure References

- **Atoms** are prefixed with `@`: `{ sum(@revenue) }`
- **Measures** are referenced by name: `{ margin / revenue * 100 }`

This eliminates ambiguity when an atom and measure share similar names.

#### Expression Types

| Type | Example | Notes |
|------|---------|-------|
| Sum | `{ sum(@amount) }` | Aggregate atom |
| Count | `{ count(*) }` | Row count |
| Count column | `{ count(@customer_id) }` | Non-null count |
| Count distinct | `{ count(distinct @customer_id) }` | Unique count |
| Average | `{ avg(@amount) }` | Mean of atom |
| Min/Max | `{ min(@date) }`, `{ max(@amount) }` | Extremes |
| Measure math | `{ revenue - cost }` | Combine measures |
| Division | `{ revenue / order_count }` | Ratios (see NULL handling) |
| Complex | `{ margin / revenue * 100 }` | Any valid SQL expression |
| Conditional | `{ sum(case when @amount > 1000 then @amount else 0 end) }` | CASE expressions |

#### Division and NULL Handling

Division by zero or NULL is controlled by the `null_handling` setting in `defaults` or can be overridden per-measure:

```
// Model-wide default
defaults {
    null_handling coalesce_zero;
}

// Per-measure override
measures fact_sales {
    aov = { revenue / order_count } null coalesce_zero;
    margin_pct = { margin / revenue * 100 } null null_on_zero;
}
```

The compiler wraps division expressions:
- `coalesce_zero`: `coalesce(revenue / nullif(order_count, 0), 0)`
- `null_on_zero`: `revenue / nullif(order_count, 0)`

**Example:**

```
measures fact_sales {
    revenue = { sum(@revenue) };
    cost = { sum(@cost) };
    quantity = { sum(@quantity) };
    order_count = { count(*) };
    unique_customers = { count(distinct @customer_id) };
    
    margin = { revenue - cost };
    margin_pct = { margin / revenue * 100 };
    aov = { revenue / order_count };
    
    // Filtered measures
    enterprise_rev = { sum(@revenue) } where { customers.segment = 'Enterprise' };
    online_rev = { sum(@revenue) } where { channel = 'Online' };
    large_orders = { count(*) } where { @revenue > 10000 };
    q4_revenue = { sum(@revenue) } where { dates.quarter = 4 };
}
```

---

### Report

Defines a report output with grouping, measures, filters, and sorting.

```
report <name> {
    from <table> [, <table>, ...];
    use_date <time_column> [, <time_column>, ...];
    
    period <period_expression>;
    
    group {
        <drill_path_reference>;
        ...
    }
    
    show {
        <measure>;
        <measure>.<time_suffix>;
        <measure> as "<label>";
        <measure>.<time_suffix> as "<label>";
        <name> = { <expression> };
        <name> = { <expression> } as "<label>";
        ...
    }
    
    filter { <sql_condition> };
    
    sort <column>.<asc|desc> [, ...];
    
    limit <n>;
}
```

#### Group Clause and Drill Paths

The `group` clause references named drill paths at specific levels. This ensures only valid aggregation hierarchies are used.

```
group {
    dates.standard.month as "Month";       // calendar drill path at month level
    dates.fiscal.fiscal_quarter;           // fiscal calendar path
    customers.geo.region as "Region";      // dimension drill path at region level
    products.category.category;            // dimension drill path at category level
}
```

You can also use inline slicers directly:
```
group {
    dates.standard.month;
    channel;                               // inline slicer from table
}
```

#### Labels

Labels use `as` followed by quoted text. Labels support any characters inside quotes.

```
group {
    customers.geo.region as "Customer Region";
    dates.standard.month as "Month";
}

show {
    revenue as "Total Revenue";
    revenue.yoy_growth as "YoY Growth %";
    margin_pct as "Margin %";
}
```

#### Filter Syntax

Filters are SQL expressions wrapped in `{ }`. The entire filter block is a single SQL boolean expression.

```
filter { country = 'USA' and channel = 'Online' and @revenue > 1000 }
```

**WHERE vs HAVING:** The compiler automatically routes predicates to the correct clause:
- References to atoms (`@column`) or slicers → WHERE clause
- References to measures → HAVING clause

```
filter { region = 'West' and revenue > 100000 }
//       ^^^^^^^^^^^^^^     ^^^^^^^^^^^^^^^^
//       slicer → WHERE     measure → HAVING
```

**Note:** OR conditions that span WHERE and HAVING are not supported. The compiler will error:
```
// ERROR: cannot mix slicer/atom and measure conditions with OR
filter { region = 'West' or revenue > 100000 }
```

#### Sort Behavior

Sort can reference:
1. Any column in `group`
2. Any measure in `show`
3. Any measure defined for the table (even if not in `show`)

```
sort dates.standard.month.asc, revenue.desc;
```

#### Limit

Limit the number of rows returned:

```
limit 10;  // Top 10 results
```

#### Inline Measures in `show`

Reports can define ad-hoc measures inline. These are scoped only to the report and can reference measures from any table in `from`.

```
show {
    revenue;
    refunds;
    net_revenue = { revenue - refunds };
    return_rate = { refunds / revenue * 100 } as "Return Rate %";
}
```

**Example:**

```
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
    
    filter { customers.country = 'USA' and channel = 'Online' }
    
    sort dates.standard.quarter.asc, dates.standard.month.asc, revenue.desc;
}
```

Compact single-line form:
```
report quick_summary {
    from fact_sales;
    use_date order_date_id;
    period this_month;
    group { customers.geo.region; channel; }
    show { revenue; order_count; }
    sort revenue.desc;
    limit 10;
}
```

---

### Multi-Source Reports

Reports can pull from multiple fact tables sharing common dimensions. The compiler generates symmetric aggregates (pre-aggregated CTEs joined on shared grain).

#### Date Column Semantics

When multiple tables are specified in `from`, each table's date column in `use_date` is filtered independently to the same period. The columns are listed in the same order as the tables.

```
report revenue_vs_returns {
    from orders, returns;
    use_date order_date, return_date;  // orders.order_date, returns.return_date
    period last_6_months;
    ...
}
```

Both `order_date` and `return_date` are filtered to `last_6_months`. This answers "orders placed in the last 6 months vs returns filed in the last 6 months."

#### Cross-Table Measure References

Inline measures in `show` can reference measures from any table in `from`. The compiler resolves which table each measure belongs to.

```
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
        revenue;                                  // from orders
        refunds;                                  // from returns
        net_revenue = { revenue - refunds };      // cross-table
        return_rate = { refunds / revenue * 100 } as "Return Rate %";
    }
    
    sort auto.standard.month.asc;
}
```

#### Grain Resolution

When joining facts at different grains, the compiler resolves to the lowest common grain. Daily data rolls up to monthly, but monthly data cannot be drilled to daily.

**Error example:**
```
Error: grain mismatch in report 'mixed_grain'
  → monthly_targets is at 'month' grain
  → group clause requests 'day' grain via dates.standard.day
  → cannot drill below the source grain
```

#### Common Dimension Requirement

All tables in a multi-source report must share at least one common dimension or calendar for the join to be valid.

---

## Time Intelligence

Time intelligence suffixes are applied to measures when the table has a time column bound to a calendar.

### Accumulations

| Suffix | Description |
|--------|-------------|
| `.ytd` | Year-to-date |
| `.qtd` | Quarter-to-date |
| `.mtd` | Month-to-date |
| `.wtd` | Week-to-date |
| `.fiscal_ytd` | Fiscal year-to-date |
| `.fiscal_qtd` | Fiscal quarter-to-date |

### Prior Period

| Suffix | Description |
|--------|-------------|
| `.prior_year` | Same period, previous year |
| `.prior_quarter` | Same period, previous quarter |
| `.prior_month` | Same period, previous month |
| `.prior_week` | Same period, previous week |

### Growth (Percentage)

| Suffix | Description |
|--------|-------------|
| `.yoy_growth` | Year-over-year % change |
| `.qoq_growth` | Quarter-over-quarter % change |
| `.mom_growth` | Month-over-month % change |
| `.wow_growth` | Week-over-week % change |

### Delta (Absolute)

| Suffix | Description |
|--------|-------------|
| `.yoy_delta` | Year-over-year absolute change |
| `.qoq_delta` | Quarter-over-quarter absolute change |
| `.mom_delta` | Month-over-month absolute change |
| `.wow_delta` | Week-over-week absolute change |

### Rolling

| Suffix | Description |
|--------|-------------|
| `.rolling_3m` | Rolling 3 month sum |
| `.rolling_6m` | Rolling 6 month sum |
| `.rolling_12m` | Rolling 12 month sum |
| `.rolling_3m_avg` | Rolling 3 month average |
| `.rolling_6m_avg` | Rolling 6 month average |
| `.rolling_12m_avg` | Rolling 12 month average |

---

## Period Expressions

Periods are evaluated at compile time to concrete date ranges.

### Relative Periods

| Expression | Description |
|------------|-------------|
| `today` | Current date only |
| `yesterday` | Previous date |
| `this_week` | Current week |
| `last_week` | Previous complete week |
| `this_month` | Current calendar month |
| `last_month` | Previous complete month |
| `this_quarter` | Current calendar quarter |
| `last_quarter` | Previous complete quarter |
| `this_year` | Current calendar year |
| `last_year` | Previous complete year |
| `ytd` | Year-to-date (Jan 1 to today) |
| `qtd` | Quarter-to-date |
| `mtd` | Month-to-date |

### Fiscal Periods

| Expression | Description |
|------------|-------------|
| `this_fiscal_year` | Current fiscal year |
| `last_fiscal_year` | Previous fiscal year |
| `this_fiscal_quarter` | Current fiscal quarter |
| `last_fiscal_quarter` | Previous fiscal quarter |
| `fiscal_ytd` | Fiscal year-to-date |

### Trailing Periods

| Expression | Description |
|------------|-------------|
| `last_N_days` | Past N days including today |
| `last_N_weeks` | Past N complete weeks |
| `last_N_months` | Past N complete months |
| `last_N_quarters` | Past N complete quarters |
| `last_N_years` | Past N complete years |

**Examples:** `last_7_days`, `last_12_months`, `last_4_quarters`

### Absolute Periods

| Expression | Description |
|------------|-------------|
| `range(2024-01-01, 2024-12-31)` | Specific date range |
| `month(2024-03)` | Specific month |
| `quarter(2024-Q2)` | Specific quarter |
| `year(2024)` | Specific year |

---

## Validation Rules

The compiler validates the model at parse time.

### Table Validation

- Source table/file must be resolvable
- Atom types must be numeric (`int`, `decimal`, `float`)
- Atom names must be unique within the table
- Time columns must reference a valid calendar and grain level
- Slicer columns with `->` must reference a valid dimension and its key column
- `via` slicers must reference an existing slicer with a `->` join
- `via` slicer names must match an attribute in the target dimension
- Calculated slicer expressions must be valid SQL

### Calendar Validation

- Physical calendars must map all grain levels to existing columns
- Grain level columns must contain period-start dates
- Drill paths must only reference defined grain levels
- Drill paths must be ordered from fine to coarse grain
- **Generated calendars: drill paths cannot reference grains below the generated base grain**

**Generated calendar grain validation example:**
```
Error: invalid drill path 'standard'
  → references 'day' grain but calendar generates from 'month+'
  → drill paths can only use: month, quarter, year
```

### Measure Validation

- Atom references must use `@` prefix
- Aggregation functions must wrap atoms (`@column`) or `*`, not measures
- Measure references (without `@`) must exist in the same table's measures
- Circular measure references are not allowed
- Filter conditions in `where` must reference valid columns/attributes
- Expressions must be valid SQL for the target dialect

### Report Validation

- `from` must reference valid table(s)
- `use_date` must reference time column(s) on the corresponding table(s)
- Number of `use_date` columns must match number of `from` tables (for multi-source)
- `group` must reference valid drill paths at valid levels
- `show` measures must be defined for one of the tables in `from`
- Time intelligence suffixes require a calendar binding on the table
- Filter expressions must be valid SQL
- Filter cannot use OR between slicer/atom conditions and measure conditions
- `sort` columns must appear in `group` or be valid measures
- Labels must be quoted strings

### Multi-Source Validation

- All tables must share at least one common dimension or calendar
- Grain must be resolvable to lowest common denominator
- Cannot drill below the coarsest source grain
- Measure names must be unambiguous across tables (or prefix with table name)

---

## Complete Examples

### Example 1: Simple CSV Report

```
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
        deal_size string = { case when @deal_value < 1000 then 'Small' when @deal_value < 10000 then 'Medium' else 'Large' end };
    }
}

measures sales_export {
    revenue = { sum(@deal_value) };
    deals = { count(*) };
    avg_deal = { revenue / deals };
    won_deals = { count(*) } where { deal_stage = 'Won' };
    win_rate = { won_deals / deals * 100 };
}

report rep_performance {
    from sales_export;
    use_date close_date;
    period this_quarter;
    
    group {
        auto.standard.month as "Month";
        region as "Region";
        sales_rep as "Sales Rep";
        deal_size as "Deal Size";
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
```

### Example 2: Star Schema Report

```
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
    fiscal_month = fiscal_month_start;
    fiscal_quarter = fiscal_quarter_start;
    fiscal_year = fiscal_year_start;
    
    drill_path standard { day -> week -> month -> quarter -> year };
    drill_path fiscal { day -> week -> fiscal_month -> fiscal_quarter -> fiscal_year };
    
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
    
    enterprise_rev = { sum(@revenue) } where { customers.segment = 'Enterprise' };
    smb_rev = { sum(@revenue) } where { customers.segment = 'SMB' };
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
    
    filter { customers.country = 'USA' and channel = 'Online' }
    
    sort dates.standard.quarter.asc, dates.standard.month.asc, revenue.desc;
}
```

### Example 3: Multi-Source Report

```
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
    
    sort auto.standard.month.asc;
}
```

### Example 4: Fiscal Calendar with Monthly Data

```
calendar auto {
    generate month+ include fiscal[July];
    range infer min 2020-01-01 max 2030-12-31;
    
    drill_path standard { month -> quarter -> year };
    drill_path fiscal { fiscal_month -> fiscal_quarter -> fiscal_year };
}

table monthly_targets {
    source "finance/targets_2024.csv";
    
    atoms {
        target_revenue decimal;
        target_units int;
    }
    
    times {
        target_month -> auto.month;  // data is at month grain
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
        auto.fiscal.fiscal_quarter as "Fiscal Quarter";
        auto.standard.month as "Month";
        region as "Region";
    }
    
    show {
        target as "Target";
        target.fiscal_ytd as "Fiscal YTD Target";
        target_units as "Target Units";
    }
    
    sort auto.standard.month.asc;
}
```

---

## Appendix A: Keyword Reference

| Keyword | Purpose |
|---------|---------|
| `defaults` | Model-wide default settings |
| `calendar` | Define a date reference (physical or generated) |
| `dimension` | Define a dimension table |
| `table` | Define a data source (fact, wide table, CSV) |
| `measures` | Define aggregations for a table |
| `report` | Define a report output |
| `source` | Table/file location |
| `key` | Primary key column |
| `atoms` | Numeric columns for aggregation |
| `times` | Date/time columns |
| `slicers` | Columns for slicing/grouping |
| `attributes` | Dimension attributes |
| `drill_path` | Define aggregation hierarchy |
| `generate` | Generate ephemeral date spine |
| `include` | Add optional grain sets (e.g., `include fiscal[July]`) |
| `range` | Date range (explicit or infer) |
| `fiscal_year_start` | Month fiscal year begins (physical calendars) |
| `week_start` | Day week begins |
| `null_handling` | Division by zero behavior |
| `from` | Source table(s) for report |
| `use_date` | Time column for period filtering |
| `period` | Time period for report |
| `group` | Grouping columns (drill path references) |
| `show` | Measures to display |
| `filter` | Filter conditions |
| `sort` | Sort order |
| `limit` | Maximum rows returned |
| `where` | Measure filter condition |
| `as` | Column/measure label alias |
| `via` | Inherit slicer through dimension join |
| `to` | Date range delimiter |
| `infer` | Auto-detect date range |
| `min` | Floor for inferred date range |
| `max` | Ceiling for inferred date range |
| `null` | Per-measure NULL handling override |
| `coalesce_zero` | Return 0 on division by zero |
| `null_on_zero` | Return NULL on division by zero |

---

## Appendix B: Type Reference

### Atom Types

| Type | Description | SQL Equivalent |
|------|-------------|----------------|
| `int` | Integer numbers | `INT`, `BIGINT` |
| `decimal` | Fixed-point numbers | `DECIMAL`, `NUMERIC` |
| `float` | Floating-point numbers | `FLOAT`, `REAL` |

### Attribute/Slicer Types

| Type | Description | SQL Equivalent |
|------|-------------|----------------|
| `string` | Text values | `VARCHAR`, `NVARCHAR` |
| `int` | Integer values | `INT`, `BIGINT` |
| `decimal` | Fixed-point numbers | `DECIMAL`, `NUMERIC` |
| `float` | Floating-point numbers | `FLOAT`, `REAL` |
| `bool` | Boolean values | `BIT`, `BOOLEAN` |
| `date` | Date values | `DATE` |
| `timestamp` | Date/time values | `DATETIME`, `TIMESTAMP` |

### Calendar Grain Levels

| Level | Description | Available In |
|-------|-------------|--------------|
| `minute` | Minute granularity | `minute+` |
| `hour` | Hourly granularity | `minute+`, `hour+` |
| `day` | Daily granularity | `minute+`, `hour+`, `day+` |
| `week` | Weekly granularity (uses week_start setting) | `minute+`, `hour+`, `day+`, `week+` |
| `month` | Monthly granularity | All |
| `quarter` | Quarterly granularity | All |
| `year` | Yearly granularity | All |
| `fiscal_month` | Fiscal month granularity | With `include fiscal[...]` |
| `fiscal_quarter` | Fiscal quarter granularity | With `include fiscal[...]` |
| `fiscal_year` | Fiscal year granularity | With `include fiscal[...]` |

---

## Appendix C: Scoping Rules

### Identifier Resolution Order

When an identifier is referenced, the compiler resolves it in this order:

1. **In measure expressions (`{ }` in `measures` block):**
   - `@name` → Atom in the current table
   - `name` → Measure in the current table's `measures` block
   - `dimension.attribute` → Attribute from joined dimension
   - `calendar.level` → Grain level from bound calendar

2. **In filter expressions (`{ }` in `filter` block):**
   - `@name` → Atom in the source table (routes to WHERE)
   - `name` → Slicer in source table (routes to WHERE) or Measure (routes to HAVING)
   - `dimension.attribute` → Attribute from joined dimension (routes to WHERE)
   - `calendar.level` → Column from bound calendar (routes to WHERE)

3. **In report `show` inline measures:**
   - `name` → Measure from any table in `from` (must be unambiguous)
   - `table.name` → Measure from specific table (for disambiguation)

### Ambiguity Errors

```
Error: ambiguous measure reference 'revenue' in report 'combined'
  → revenue exists in both 'orders' and 'invoices'
  → use 'orders.revenue' or 'invoices.revenue' to disambiguate
```

---

## Appendix D: Error Messages

### Parse Errors

```
Error: unexpected token 'foo' at line 23
  → expected one of: 'atoms', 'times', 'slicers', '}'
```

### Validation Errors

```
Error: unknown atom '@amount' in measure 'revenue'
  → table 'fact_sales' has atoms: @revenue, @cost, @quantity

Error: circular measure reference detected
  → margin references profit
  → profit references margin

Error: 'via' slicer 'region' not found in dimension 'customers'
  → customers has attributes: customer_name, segment, country, state

Error: grain mismatch in report 'mixed_grain'
  → monthly_targets is at 'month' grain
  → group clause requests 'day' grain via dates.standard.day
  → cannot drill below the source grain

Error: no calendar binding for time intelligence
  → measure 'revenue.ytd' requires calendar binding
  → table 'sales_export' times column 'close_date' is not bound to a calendar

Error: invalid drill path reference 'dates.standard.week'
  → dates.standard has levels: day, month, quarter, year
  → 'week' is not in this drill path

Error: cannot mix slicer/atom and measure conditions with OR
  → filter contains: region = 'West' or revenue > 100000
  → 'region' routes to WHERE, 'revenue' routes to HAVING
  → rewrite as separate conditions or use AND

Error: invalid drill path 'standard'
  → references 'day' grain but calendar generates from 'month+'
  → drill paths can only use: month, quarter, year

Error: invalid calculated slicer 'deal_size'
  → expression type mismatch: expected string, got int
```

---

## Appendix E: Migration from v3

### Changes in v3.1

1. **Generated calendar grain specifier**
   - Before: `generate;` (all grains generated)
   - After: `generate day+;` or `generate month+;` (specify base grain)

2. **Fiscal grains in generated calendars**
   - Before: Separate `fiscal_year_start` line
   - After: `generate month+ include fiscal[July];`

3. **Calculated slicers**
   - New: `deal_size string = { case when @amount < 1000 then 'Small' ... };`

4. **Drill path validation for generated calendars**
   - New: Compiler validates drill paths don't reference grains below the generated base

### Removed Features

1. `fiscal_year_start` inside generated calendar blocks — use `include fiscal[Month]` instead

### New Features

1. Grain specifier with `+` suffix: `minute+`, `hour+`, `day+`, `week+`, `month+`, `quarter+`, `year+`
2. Inline fiscal configuration: `include fiscal[July]`
3. Calculated slicers with type and expression
4. Stricter drill path validation for generated calendars
