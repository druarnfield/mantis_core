//! Auto-generated DSL symbols for LSP completions.
//!
//! Generated: 2026-01-08T05:11:49Z
//!
//! DO NOT EDIT - regenerate with:
//!   cd scripts/codegen && go run . -db ../../docs/dsl-schema.duckdb -out ../../src/lsp/analysis/symbols_generated.rs

use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, Documentation, InsertTextFormat, MarkupContent, MarkupKind,
};

/// DSL function definition.
#[derive(Debug, Clone)]
pub struct DslFunction {
    pub name: &'static str,
    pub description: &'static str,
    pub template: &'static str,
    pub category: &'static str,
}

/// DSL type definition.
#[derive(Debug, Clone)]
pub struct DslType {
    pub name: &'static str,
    pub description: &'static str,
    pub template: Option<&'static str>,
}

/// DSL constant definition.
#[derive(Debug, Clone)]
pub struct DslConstant {
    pub name: &'static str,
    pub description: &'static str,
    pub category: &'static str,
}

/// DSL method definition (chained method syntax).
#[derive(Debug, Clone)]
pub struct DslMethod {
    pub name: &'static str,
    pub description: &'static str,
    pub template: &'static str,
    pub entity_types: &'static [&'static str],
}

/// DSL block definition (top-level constructs).
#[derive(Debug, Clone)]
pub struct DslBlock {
    pub name: &'static str,
    pub description: &'static str,
    pub template: &'static str,
}

/// All DSL functions.
pub static FUNCTIONS: &[DslFunction] = &[
    DslFunction {
        name: "sum",
        description: "Sum aggregation measure.\n\ntotal_revenue = sum \"amount\"\n\nChain :where() for filtered aggregation:\nsum(\"amount\"):where(\"status = 'completed'\")",
        template: "sum \"${1:column}\"",
        category: "aggregation",
    },
    DslFunction {
        name: "count",
        description: "Count aggregation measure.\n\norder_count = count(\"*\")\nunique_customers = count_distinct(\"customer_id\")\n\nUse count(\"*\") for all rows, count(\"col\") for non-null.",
        template: "count(\"${1:*}\")",
        category: "aggregation",
    },
    DslFunction {
        name: "count_distinct",
        description: "Count unique values.\n\nunique_products = count_distinct \"product_id\"\n\nGenerates: COUNT(DISTINCT product_id)",
        template: "count_distinct \"${1:column}\"",
        category: "aggregation",
    },
    DslFunction {
        name: "avg",
        description: "Average aggregation measure.\n\navg_order_value = avg \"amount\"\n\nNULL values are excluded from calculation.",
        template: "avg \"${1:column}\"",
        category: "aggregation",
    },
    DslFunction {
        name: "min",
        description: "Minimum value measure.\n\nfirst_order_date = min \"order_date\"\nlowest_price = min \"price\"",
        template: "min \"${1:column}\"",
        category: "aggregation",
    },
    DslFunction {
        name: "max",
        description: "Maximum value measure.\n\nlast_order_date = max \"order_date\"\nhighest_price = max \"price\"",
        template: "max \"${1:column}\"",
        category: "aggregation",
    },
    DslFunction {
        name: "pk",
        description: "Mark column as primary key.\n\norder_id = pk(int64)\n\nPrimary keys are non-nullable and used for joins and deduplication.",
        template: "pk(${1:type})",
        category: "column",
    },
    DslFunction {
        name: "required",
        description: "Mark column as non-nullable.\n\nemail = required(string)\n\nValidation will fail if NULL values are found.",
        template: "required(${1:type})",
        category: "column",
    },
    DslFunction {
        name: "nullable",
        description: "Explicitly mark column as nullable.\n\nmiddle_name = nullable(string)\n\nColumns are nullable by default; use for documentation clarity.",
        template: "nullable(${1:type})",
        category: "column",
    },
    DslFunction {
        name: "describe",
        description: "Add description to a column definition.\n\nname = describe(string, \"Customer full name\")\namount = describe(decimal(10, 2), \"Order total in USD\")\n\nProvides documentation for data catalogs and schema introspection.",
        template: "describe(${1:type}, \"${2:description}\")",
        category: "column",
    },
    DslFunction {
        name: "rename",
        description: "Rename a column in output.\n\nrename(\"old_name\", \"new_name\")\n\nChanges the column alias without modifying the source.",
        template: "rename(\"${1:old_name}\", \"${2:new_name}\")",
        category: "column",
    },
    DslFunction {
        name: "compute",
        description: "Create a computed column.\n\ncompute(\"full_name\", sql(\"first_name || ' ' || last_name\"))\n\nAdds a derived column to the output.",
        template: "compute(\"${1:name}\", ${2:expression})",
        category: "column",
    },
    DslFunction {
        name: "col",
        description: "Reference a column for expression building.\n\ncol(\"amount\") -> amount\ncol(\"orders.total\") -> orders.total\n\nSupports method chaining: col(\"x\"):gt(lit(10))",
        template: "col(\"${1:column}\")",
        category: "expression",
    },
    DslFunction {
        name: "lit",
        description: "Create a literal value expression.\n\nlit(100) -> 100\nlit(\"active\") -> 'active'\nlit(true) -> TRUE\n\nUse with col() for comparisons: col(\"status\"):eq(lit(\"active\"))",
        template: "lit(${1:value})",
        category: "expression",
    },
    DslFunction {
        name: "sql",
        description: "Embed raw SQL expression.\n\nsql(\"EXTRACT(YEAR FROM order_date)\")\nsql(\"COALESCE(nickname, first_name, 'Anonymous')\")\n\nUse for complex expressions not covered by the DSL.",
        template: "sql(\"${1:sql_expression}\")",
        category: "expression",
    },
    DslFunction {
        name: "cast",
        description: "Cast expression to a different type.\n\ncast(col(\"amount\"), decimal(10, 2))\ncast(\"123\", int64)\n\nGenerates: CAST(amount AS DECIMAL(10, 2))",
        template: "cast(${1:expr}, ${2:type})",
        category: "expression",
    },
    DslFunction {
        name: "coalesce",
        description: "Return first non-null value.\n\ncoalesce(col(\"nickname\"), col(\"first_name\"), lit(\"Unknown\"))\n\nGenerates: COALESCE(nickname, first_name, 'Unknown')",
        template: "coalesce(${1:expr1}, ${2:expr2})",
        category: "expression",
    },
    DslFunction {
        name: "case",
        description: "Build CASE WHEN expressions with fluent syntax.\n\ncase()\n  :when(\"amount < 100\", \"'small'\")\n  :when(\"amount < 1000\", \"'medium'\")\n  :else_(\"'large'\")\n\nConditions and results are SQL. Use 'quotes' for string literals.",
        template: "case()\n\t:when(\"${1:condition}\", \"${2:result}\")\n\t:else_(\"${3:default}\")",
        category: "expression",
    },
    DslFunction {
        name: "nullif",
        description: "Return NULL if two expressions are equal.\n\nnullif(col(\"amount\"), lit(0))\n\nGenerates: NULLIF(amount, 0)\n\nCommonly used to prevent division by zero.",
        template: "nullif(${1:expr1}, ${2:expr2})",
        category: "expression",
    },
    DslFunction {
        name: "case_when",
        description: "CASE expression (legacy table syntax).\n\ncase_when({\n  { when = condition1, then_ = result1 },\n  { when = condition2, then_ = result2 }\n}, else_value)\n\nPrefer the fluent case() builder for better readability.",
        template: "case_when({\n\t{ when = ${1:condition}, then_ = ${2:result} }\n}, ${3:else_value})",
        category: "expression",
    },
    DslFunction {
        name: "row_number",
        description: "Sequential row number within partition.\n\nrow_number()\n  :partition_by(\"customer_id\")\n  :order_by(\"order_date\")\n\nGenerates: ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date)",
        template: "row_number()\n\t:partition_by(\"${1:col}\")\n\t:order_by(\"${2:col}\")",
        category: "window",
    },
    DslFunction {
        name: "rank",
        description: "Rank with gaps for ties.\n\nValues: 1, 2, 2, 4 (two items tied for 2nd, next is 4th)\n\nUse dense_rank() if you don't want gaps.",
        template: "rank()\n\t:partition_by(\"${1:col}\")\n\t:order_by(\"${2:col}\")",
        category: "window",
    },
    DslFunction {
        name: "dense_rank",
        description: "Rank without gaps for ties.\n\nValues: 1, 2, 2, 3 (two items tied for 2nd, next is 3rd)\n\nNo gaps in ranking sequence unlike rank().",
        template: "dense_rank()\n\t:partition_by(\"${1:col}\")\n\t:order_by(\"${2:col}\")",
        category: "window",
    },
    DslFunction {
        name: "lag",
        description: "Access previous row's value.\n\nlag(\"amount\", 1) -> previous row's amount\nlag(\"amount\", 2) -> two rows back\n\nUseful for period-over-period comparisons.",
        template: "lag(\"${1:col}\", ${2:1})\n\t:partition_by(\"${3:partition_col}\")\n\t:order_by(\"${4:order_col}\")",
        category: "window",
    },
    DslFunction {
        name: "lead",
        description: "Access next row's value.\n\nlead(\"order_date\", 1) -> next row's order_date\n\nUseful for calculating time between events.",
        template: "lead(\"${1:col}\", ${2:1})\n\t:partition_by(\"${3:partition_col}\")\n\t:order_by(\"${4:order_col}\")",
        category: "window",
    },
    DslFunction {
        name: "first_value",
        description: "Get first value in the window.\n\nfirst_value(\"order_date\")\n  :partition_by(\"customer_id\")\n  :order_by(\"order_date\")\n\nReturns customer's first order date on every row.",
        template: "first_value(\"${1:col}\")\n\t:partition_by(\"${2:partition_col}\")\n\t:order_by(\"${3:order_col}\")",
        category: "window",
    },
    DslFunction {
        name: "last_value",
        description: "Get last value in the window.\n\nImportant: Add :rows(\"unbounded preceding\", \"unbounded following\") to see the actual last value, otherwise only sees up to current row.",
        template: "last_value(\"${1:col}\")\n\t:partition_by(\"${2:partition_col}\")\n\t:order_by(\"${3:order_col}\")\n\t:rows(\"unbounded preceding\", \"unbounded following\")",
        category: "window",
    },
    DslFunction {
        name: "ntile",
        description: "Divide rows into n buckets.\n\nntile(4) -> quartiles (1, 2, 3, 4)\nntile(10) -> deciles (1-10)\n\nUseful for percentile analysis.",
        template: "ntile(${1:4})\n\t:partition_by(\"${2:col}\")\n\t:order_by(\"${3:col}\")",
        category: "window",
    },
    DslFunction {
        name: "nth_value",
        description: "Get the Nth value in the window.\n\nnth_value(\"amount\", 2)\n  :partition_by(\"customer_id\")\n  :order_by(\"order_date\")\n\nReturns the 2nd order amount for each customer.",
        template: "nth_value(\"${1:col}\", ${2:n})\n\t:partition_by(\"${3:partition_col}\")\n\t:order_by(\"${4:order_col}\")",
        category: "window",
    },
    DslFunction {
        name: "percent_rank",
        description: "Relative rank as percentage (0 to 1).\n\npercent_rank()\n  :partition_by(\"category\")\n  :order_by(\"sales\")\n\nFormula: (rank - 1) / (total_rows - 1)\nFirst row = 0, last row = 1.",
        template: "percent_rank()\n\t:partition_by(\"${1:col}\")\n\t:order_by(\"${2:col}\")",
        category: "window",
    },
    DslFunction {
        name: "cume_dist",
        description: "Cumulative distribution (0 to 1).\n\ncume_dist()\n  :partition_by(\"category\")\n  :order_by(\"sales\")\n\nFormula: rows_up_to_current / total_rows\nAlways reaches 1.0 at the last row.",
        template: "cume_dist()\n\t:partition_by(\"${1:col}\")\n\t:order_by(\"${2:col}\")",
        category: "window",
    },
    DslFunction {
        name: "partition_by",
        description: "Define groups for the window function.\n\n:partition_by(\"customer_id\")\n:partition_by(\"region\", \"year\")\n\nLike GROUP BY, but keeps all rows.",
        template: ":partition_by(\"${1:col}\")",
        category: "window_method",
    },
    DslFunction {
        name: "order_by",
        description: "Define sort order within partitions.\n\n:order_by(\"created_at\")\n:order_by(\"amount\", DESC)\n\nRequired for ranking and most window functions.",
        template: ":order_by(\"${1:col}\")",
        category: "window_method",
    },
    DslFunction {
        name: "rows",
        description: "Set ROWS frame for window.\n\n:rows(\"unbounded preceding\", \"current row\") -> running total\n:rows(\"2 preceding\", \"current row\") -> 3-row moving average\n\nCounts physical rows.",
        template: ":rows(\"${1:unbounded preceding}\", \"${2:current row}\")",
        category: "window_method",
    },
    DslFunction {
        name: "range",
        description: "Set RANGE frame for window.\n\nSimilar to :rows() but based on value ranges, not row counts.\nIncludes all rows with same ORDER BY value.",
        template: ":range(\"${1:unbounded preceding}\", \"${2:current row}\")",
        category: "window_method",
    },
    DslFunction {
        name: "measure",
        description: "Reference a measure by name in a query.\n\nmeasure \"revenue\"              -- Basic reference\nmeasure(\"revenue\"):as(\"rev\")   -- With alias (paren syntax)\nmeasure(\"revenue\"):where(...)  -- Filtered measure (paren syntax)\n\nMeasure name must be defined in a fact.",
        template: "measure \"${1:measure_name}\"",
        category: "query",
    },
    DslFunction {
        name: "derived",
        description: "Create a derived measure from other measures.\n\nderived(\"aov\", m(\"revenue\") / m(\"order_count\"))\nderived(\"margin\", (m(\"revenue\") - m(\"cost\")) / m(\"revenue\") * 100)\n\nSupports +, -, *, / operators and literals.",
        template: "derived(\"${1:alias}\", m(\"${2:measure1}\") ${3:/} m(\"${4:measure2}\"))",
        category: "query",
    },
    DslFunction {
        name: "m",
        description: "Reference a measure in a derived expression.\n\nm(\"revenue\") / m(\"order_count\")  -- Division\nm(\"total\") - m(\"discount\")       -- Subtraction\nm(\"amount\") * 100                 -- With literal\n\nReferences the output alias of another measure.",
        template: "m(\"${1:measure_alias}\")",
        category: "query",
    },
    DslFunction {
        name: "filter",
        description: "Create a filter condition for queries.\n\nfilter(customers.region, \"=\", \"APAC\")\nfilter(dates.year, \">=\", 2024)\nfilter(products.name, \"like\", \"Widget%\")\n\nOperators: =, !=, >, >=, <, <=, like, in, not_in",
        template: "filter(${1:entity}.${2:column}, \"${3:=}\", \"${4:value}\")",
        category: "query",
    },
    DslFunction {
        name: "between",
        description: "BETWEEN range filter.\n\nbetween(orders.amount, 100, 1000)\n\nGenerates: amount BETWEEN 100 AND 1000",
        template: "between(${1:entity}.${2:column}, ${3:low}, ${4:high})",
        category: "query",
    },
    DslFunction {
        name: "dim",
        description: "Reference a dimension column in a query.\n\ndim(\"customers.region\")\ndim(\"date.year\")\n\nShorthand for referencing dimension attributes in select/group_by.",
        template: "dim(\"${1:dimension}.${2:column}\")",
        category: "query",
    },
    DslFunction {
        name: "is_not_in",
        description: "NOT IN filter condition.\n\nis_not_in(customers.region, {\"NA\", \"EMEA\"})\n\nGenerates: region NOT IN ('NA', 'EMEA')",
        template: "is_not_in(${1:entity}.${2:column}, {${3:values}})",
        category: "query",
    },
    DslFunction {
        name: "asc",
        description: "Ascending sort order for ORDER BY.\n\norder_by = { asc(\"date\"), desc(\"amount\") }\n\nExplicit ascending (default if not specified).",
        template: "asc(\"${1:column}\")",
        category: "query",
    },
    DslFunction {
        name: "desc",
        description: "Descending sort order for ORDER BY.\n\norder_by = { desc(\"revenue\") }\n\nUse for \"most recent first\" or \"highest first\" ordering.",
        template: "desc(\"${1:column}\")",
        category: "query",
    },
    DslFunction {
        name: "ytd",
        description: "Year-to-date cumulative sum.\n\nderived(\"ytd_revenue\", ytd(m(\"revenue\")))\n\nGenerates:\nSUM(revenue) OVER (\n  PARTITION BY year\n  ORDER BY month\n  ROWS UNBOUNDED PRECEDING\n)",
        template: "ytd(m(\"${1:measure}\"))",
        category: "time_intel",
    },
    DslFunction {
        name: "qtd",
        description: "Quarter-to-date cumulative sum.\n\nderived(\"qtd_revenue\", qtd(m(\"revenue\")))\n\nPartitions by year + quarter, orders by period.",
        template: "qtd(m(\"${1:measure}\"))",
        category: "time_intel",
    },
    DslFunction {
        name: "mtd",
        description: "Month-to-date cumulative sum.\n\nderived(\"mtd_revenue\", mtd(m(\"revenue\")))\n\nPartitions by year + month, orders by day.",
        template: "mtd(m(\"${1:measure}\"))",
        category: "time_intel",
    },
    DslFunction {
        name: "prior_period",
        description: "Value from N periods ago (default: 1).\n\nprior_period(m(\"revenue\"))     -- Previous period\nprior_period(m(\"revenue\"), 3) -- 3 periods ago\n\nGenerates: LAG(revenue, n) OVER (ORDER BY period)",
        template: "prior_period(m(\"${1:measure}\")${2:, ${3:1}})",
        category: "time_intel",
    },
    DslFunction {
        name: "prior_year",
        description: "Same period in the prior year.\n\nderived(\"py_revenue\", prior_year(m(\"revenue\")))\n\nFor monthly data: LAG(revenue, 12)\nFor quarterly: LAG(revenue, 4)",
        template: "prior_year(m(\"${1:measure}\"))",
        category: "time_intel",
    },
    DslFunction {
        name: "prior_quarter",
        description: "Same period in the prior quarter.\n\nderived(\"pq_revenue\", prior_quarter(m(\"revenue\")))\n\nFor monthly data: LAG(revenue, 3)",
        template: "prior_quarter(m(\"${1:measure}\"))",
        category: "time_intel",
    },
    DslFunction {
        name: "rolling_sum",
        description: "Rolling sum over N periods.\n\nrolling_sum(m(\"revenue\"), 3)  -- 3-period rolling sum\n\nGenerates:\nSUM(revenue) OVER (\n  ORDER BY period\n  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW\n)",
        template: "rolling_sum(m(\"${1:measure}\"), ${2:3})",
        category: "time_intel",
    },
    DslFunction {
        name: "rolling_avg",
        description: "Rolling average over N periods.\n\nrolling_avg(m(\"revenue\"), 6)  -- 6-period moving average\n\nGenerates:\nAVG(revenue) OVER (\n  ORDER BY period\n  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW\n)",
        template: "rolling_avg(m(\"${1:measure}\"), ${2:6})",
        category: "time_intel",
    },
    DslFunction {
        name: "delta",
        description: "Difference between two values.\n\ndelta(m(\"revenue\"), prior_period(m(\"revenue\")))\n\nGenerates: current - previous\n\nUse for period-over-period absolute change.",
        template: "delta(m(\"${1:current}\"), ${2:prior_period(m(\"${3:measure}\"))})",
        category: "time_intel",
    },
    DslFunction {
        name: "growth",
        description: "Percentage change between two values.\n\ngrowth(m(\"revenue\"), prior_year(m(\"revenue\")))\n\nGenerates: (current - previous) / NULLIF(previous, 0) * 100\n\nIncludes NULLIF for safe division by zero.",
        template: "growth(m(\"${1:current}\"), ${2:prior_year(m(\"${3:measure}\"))})",
        category: "time_intel",
    },
    DslFunction {
        name: "concat",
        description: "Concatenate strings.\n\nconcat(col(\"first_name\"), lit(\" \"), col(\"last_name\"))\n\nGenerates: first_name || ' ' || last_name (dialect-aware)",
        template: "concat(${1:expr1}, ${2:expr2})",
        category: "sql_string",
    },
    DslFunction {
        name: "substring",
        description: "Extract substring from a string.\n\nsubstring(col(\"name\"), 1, 3)\n\nGenerates: SUBSTRING(name, 1, 3) or equivalent",
        template: "substring(${1:expr}, ${2:start}, ${3:length})",
        category: "sql_string",
    },
    DslFunction {
        name: "trim",
        description: "Remove leading and trailing whitespace.\n\ntrim(col(\"name\"))\n\nGenerates: TRIM(name)",
        template: "trim(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "ltrim",
        description: "Remove leading whitespace.\n\nltrim(col(\"name\"))\n\nGenerates: LTRIM(name)",
        template: "ltrim(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "rtrim",
        description: "Remove trailing whitespace.\n\nrtrim(col(\"name\"))\n\nGenerates: RTRIM(name)",
        template: "rtrim(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "upper",
        description: "Convert string to uppercase.\n\nupper(col(\"name\"))\n\nGenerates: UPPER(name)",
        template: "upper(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "lower",
        description: "Convert string to lowercase.\n\nlower(col(\"email\"))\n\nGenerates: LOWER(email)",
        template: "lower(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "initcap",
        description: "Capitalize first letter of each word.\n\ninitcap(col(\"name\"))\n\nGenerates: INITCAP(name) or equivalent",
        template: "initcap(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "length",
        description: "Get string length.\n\nlength(col(\"name\"))\n\nGenerates: LENGTH(name) or LEN(name)",
        template: "length(${1:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "replace",
        description: "Replace occurrences of a substring.\n\nreplace(col(\"phone\"), \"-\", \"\")\n\nGenerates: REPLACE(phone, '-', '')",
        template: "replace(${1:expr}, \"${2:search}\", \"${3:replacement}\")",
        category: "sql_string",
    },
    DslFunction {
        name: "regexp_replace",
        description: "Replace using regular expression.\n\nregexp_replace(col(\"text\"), \"[0-9]+\", \"X\")\n\nGenerates: REGEXP_REPLACE(text, '[0-9]+', 'X')",
        template: "regexp_replace(${1:expr}, \"${2:pattern}\", \"${3:replacement}\")",
        category: "sql_string",
    },
    DslFunction {
        name: "position",
        description: "Find position of substring.\n\nposition(\"@\", col(\"email\"))\n\nGenerates: POSITION('@' IN email) or equivalent",
        template: "position(\"${1:search}\", ${2:expr})",
        category: "sql_string",
    },
    DslFunction {
        name: "date_trunc",
        description: "Truncate date to specified precision.\n\ndate_trunc(\"month\", col(\"order_date\"))\n\nGenerates: DATE_TRUNC('month', order_date)",
        template: "date_trunc(\"${1:precision}\", ${2:expr})",
        category: "sql_date",
    },
    DslFunction {
        name: "datediff",
        description: "Calculate difference between dates.\n\ndatediff(\"day\", col(\"start_date\"), col(\"end_date\"))\n\nGenerates: DATEDIFF(day, start_date, end_date)",
        template: "datediff(\"${1:unit}\", ${2:start}, ${3:end})",
        category: "sql_date",
    },
    DslFunction {
        name: "dateadd",
        description: "Add interval to date.\n\ndateadd(\"day\", 7, col(\"order_date\"))\n\nGenerates: DATEADD(day, 7, order_date) or equivalent",
        template: "dateadd(\"${1:unit}\", ${2:amount}, ${3:date})",
        category: "sql_date",
    },
    DslFunction {
        name: "current_date",
        description: "Get current date.\n\ncurrent_date()\n\nGenerates: CURRENT_DATE",
        template: "current_date()",
        category: "sql_date",
    },
    DslFunction {
        name: "current_timestamp",
        description: "Get current timestamp.\n\ncurrent_timestamp()\n\nGenerates: CURRENT_TIMESTAMP",
        template: "current_timestamp()",
        category: "sql_date",
    },
    DslFunction {
        name: "abs",
        description: "Absolute value.\n\nabs(col(\"profit\"))\n\nGenerates: ABS(profit)",
        template: "abs(${1:expr})",
        category: "sql_math",
    },
    DslFunction {
        name: "round",
        description: "Round to specified decimal places.\n\nround(col(\"price\"), 2)\n\nGenerates: ROUND(price, 2)",
        template: "round(${1:expr}, ${2:decimals})",
        category: "sql_math",
    },
    DslFunction {
        name: "floor",
        description: "Round down to nearest integer.\n\nfloor(col(\"rating\"))\n\nGenerates: FLOOR(rating)",
        template: "floor(${1:expr})",
        category: "sql_math",
    },
    DslFunction {
        name: "ceil",
        description: "Round up to nearest integer.\n\nceil(col(\"quantity\"))\n\nGenerates: CEIL(quantity) or CEILING(quantity)",
        template: "ceil(${1:expr})",
        category: "sql_math",
    },
    DslFunction {
        name: "try_cast",
        description: "Cast with NULL on failure instead of error.\n\ntry_cast(col(\"value\"), int64)\n\nGenerates: TRY_CAST(value AS BIGINT) or equivalent\nReturns NULL if conversion fails.",
        template: "try_cast(${1:expr}, ${2:type})",
        category: "sql_math",
    },
    DslFunction {
        name: "eq",
        description: "Equality comparison.\n\ncol(\"status\"):eq(lit(\"active\"))\n\nGenerates: status = 'active'",
        template: "eq(${1:value})",
        category: "comparison",
    },
    DslFunction {
        name: "ne",
        description: "Not equal comparison.\n\ncol(\"status\"):ne(lit(\"deleted\"))\n\nGenerates: status != 'deleted' or status <> 'deleted'",
        template: "ne(${1:value})",
        category: "comparison",
    },
    DslFunction {
        name: "gt",
        description: "Greater than comparison.\n\ncol(\"amount\"):gt(lit(100))\n\nGenerates: amount > 100",
        template: "gt(${1:value})",
        category: "comparison",
    },
    DslFunction {
        name: "gte",
        description: "Greater than or equal comparison.\n\ncol(\"amount\"):gte(lit(100))\n\nGenerates: amount >= 100",
        template: "gte(${1:value})",
        category: "comparison",
    },
    DslFunction {
        name: "lt",
        description: "Less than comparison.\n\ncol(\"amount\"):lt(lit(1000))\n\nGenerates: amount < 1000",
        template: "lt(${1:value})",
        category: "comparison",
    },
    DslFunction {
        name: "lte",
        description: "Less than or equal comparison.\n\ncol(\"amount\"):lte(lit(1000))\n\nGenerates: amount <= 1000",
        template: "lte(${1:value})",
        category: "comparison",
    },
    DslFunction {
        name: "like",
        description: "SQL LIKE pattern matching.\n\ncol(\"name\"):like(lit(\"%Widget%\"))\n\nGenerates: name LIKE '%Widget%'",
        template: "like(${1:pattern})",
        category: "comparison",
    },
    DslFunction {
        name: "not_like",
        description: "SQL NOT LIKE pattern matching.\n\ncol(\"name\"):not_like(lit(\"Test%\"))\n\nGenerates: name NOT LIKE 'Test%'",
        template: "not_like(${1:pattern})",
        category: "comparison",
    },
    DslFunction {
        name: "is_null",
        description: "Check if value is NULL.\n\ncol(\"deleted_at\"):is_null()\n\nGenerates: deleted_at IS NULL",
        template: "is_null()",
        category: "comparison",
    },
    DslFunction {
        name: "is_not_null",
        description: "Check if value is not NULL.\n\ncol(\"email\"):is_not_null()\n\nGenerates: email IS NOT NULL",
        template: "is_not_null()",
        category: "comparison",
    },
    DslFunction {
        name: "is_in",
        description: "Check if value is in a list.\n\ncol(\"region\"):is_in({\"NA\", \"EMEA\", \"APAC\"})\n\nGenerates: region IN ('NA', 'EMEA', 'APAC')",
        template: "is_in({${1:values}})",
        category: "comparison",
    },
    DslFunction {
        name: "AND",
        description: "Logical AND operator.\n\nAND(condition1, condition2, condition3)\n\nCombines multiple conditions with AND.",
        template: "AND(${1:cond1}, ${2:cond2})",
        category: "comparison",
    },
    DslFunction {
        name: "OR",
        description: "Logical OR operator.\n\nOR(condition1, condition2)\n\nCombines multiple conditions with OR.",
        template: "OR(${1:cond1}, ${2:cond2})",
        category: "comparison",
    },
    DslFunction {
        name: "NOT",
        description: "Logical NOT operator.\n\nNOT(condition)\n\nNegates a condition.",
        template: "NOT(${1:condition})",
        category: "comparison",
    },
    DslFunction {
        name: "dedup",
        description: "Remove duplicate rows.\n\ndedup = {\n  partition_by = { \"user_id\" },\n  order_by = { { expr = col, dir = DESC } },\n  keep = FIRST\n}\n\nKeeps first or last row per partition based on order.",
        template: "dedup = {\n\tpartition_by = { \"${1:key}\" },\n\torder_by = { { expr = ${2:col}, dir = DESC } },\n\tkeep = FIRST,\n}",
        category: "utility",
    },
    DslFunction {
        name: "keep_first",
        description: "Keep first row per partition (dedup shorthand).\n\nkeep_first(\"user_id\", \"updated_at\")\n\nEquivalent to dedup with keep = FIRST.",
        template: "keep_first(\"${1:partition_key}\", \"${2:order_col}\")",
        category: "utility",
    },
    DslFunction {
        name: "keep_last",
        description: "Keep last row per partition (dedup shorthand).\n\nkeep_last(\"user_id\", \"updated_at\")\n\nEquivalent to dedup with keep = LAST.",
        template: "keep_last(\"${1:partition_key}\", \"${2:order_col}\")",
        category: "utility",
    },
    DslFunction {
        name: "link",
        description: "Shorthand for many-to-one relationship.\n\nlink(orders.customer_id, customers.id)\n\nCreates a MANY_TO_ONE relationship.",
        template: "link(${1:from}.${2:col}, ${3:to}.${4:col})",
        category: "utility",
    },
    DslFunction {
        name: "link_as",
        description: "Role-playing dimension relationship.\n\nlink_as(\"order_date\", orders.order_date_id, date.date_id)\nlink_as(\"ship_date\", orders.ship_date_id, date.date_id)\n\nAllows same dimension with different meanings.\nAccess via role name: order_date.month, ship_date.month",
        template: "link_as(\"${1:role_name}\", ${2:from}.${3:col}, ${4:to}.${5:col})",
        category: "utility",
    },
    DslFunction {
        name: "grain",
        description: "Define fact table grain.\n\ngrain = { orders.order_id }\n\nSpecifies the level of detail (one row per what).",
        template: "grain = { ${1:entity}.${2:column} }",
        category: "utility",
    },
    DslFunction {
        name: "ref",
        description: "Reference another entity.\n\nsource = ref(\"raw_orders\")\n\nCreates a dependency on another entity.",
        template: "ref(\"${1:entity_name}\")",
        category: "utility",
    },
    DslFunction {
        name: "merge",
        description: "Merge multiple arrays or tables.\n\nmerge(array1, array2)\n\nCombines collections into one.",
        template: "merge(${1:first}, ${2:second})",
        category: "utility",
    },
    DslFunction {
        name: "array",
        description: "Create an array literal.\n\narray(\"a\", \"b\", \"c\")\n\nCreates a list of values.",
        template: "array(${1:values})",
        category: "utility",
    },
    DslFunction {
        name: "contains",
        description: "Check if array contains a value.\n\ncontains(col(\"tags\"), lit(\"featured\"))\n\nGenerates array containment check.",
        template: "contains(${1:array}, ${2:value})",
        category: "utility",
    },
    DslFunction {
        name: "env",
        description: "Read environment variable.\n\nenv(\"DATABASE_URL\")\n\nReturns the value of an environment variable.",
        template: "env(\"${1:var_name}\")",
        category: "utility",
    },
    DslFunction {
        name: "env_switch",
        description: "Switch based on environment.\n\nenv_switch({\n  dev = \"dev_schema\",\n  prod = \"prod_schema\"\n})\n\nReturns different values based on current environment.",
        template: "env_switch({\n\t${1:dev} = \"${2:value1}\",\n\t${3:prod} = \"${4:value2}\",\n})",
        category: "utility",
    },
    DslFunction {
        name: "dump",
        description: "Debug print a value.\n\ndump(some_expression)\n\nPrints value during model evaluation for debugging.",
        template: "dump(${1:value})",
        category: "utility",
    },
];

/// All DSL types.
pub static TYPES: &[DslType] = &[
    DslType {
        name: "int64",
        description: "64-bit signed integer.\n\nRange: -9.2 quintillion to 9.2 quintillion\n\nUse for IDs, large counts.",
        template: None,
    },
    DslType {
        name: "int32",
        description: "32-bit signed integer.\n\nRange: -2.1 billion to 2.1 billion\n\nUse for smaller numbers, enum values.",
        template: None,
    },
    DslType {
        name: "int16",
        description: "16-bit signed integer.\n\nRange: -32,768 to 32,767\n\nUse for small enum values.",
        template: None,
    },
    DslType {
        name: "int8",
        description: "8-bit signed integer.\n\nRange: -128 to 127\n\nUse for tiny flags or status codes.",
        template: None,
    },
    DslType {
        name: "bigint",
        description: "Alias for int64.",
        template: None,
    },
    DslType {
        name: "integer",
        description: "Alias for int32.",
        template: None,
    },
    DslType {
        name: "int",
        description: "Alias for int32.",
        template: None,
    },
    DslType {
        name: "smallint",
        description: "Alias for int16.",
        template: None,
    },
    DslType {
        name: "tinyint",
        description: "Alias for int8.",
        template: None,
    },
    DslType {
        name: "float64",
        description: "64-bit floating point (double precision).\n\nUse for scientific calculations, coordinates.",
        template: None,
    },
    DslType {
        name: "float32",
        description: "32-bit floating point (single precision).\n\nUse when memory is constrained.",
        template: None,
    },
    DslType {
        name: "double",
        description: "Alias for float64.",
        template: None,
    },
    DslType {
        name: "real",
        description: "Alias for float64.",
        template: None,
    },
    DslType {
        name: "float",
        description: "Alias for float32.",
        template: None,
    },
    DslType {
        name: "string",
        description: "Variable-length text.\n\nNo length limit. Use varchar(n) if you need to enforce max length.",
        template: None,
    },
    DslType {
        name: "text",
        description: "Alias for string.\n\nVariable-length text with no limit.",
        template: None,
    },
    DslType {
        name: "varchar",
        description: "Variable-length string with max length.\n\nvarchar(255) -> up to 255 characters\n\nUse 'string' for unbounded text.",
        template: Some("varchar(${1:255})"),
    },
    DslType {
        name: "char",
        description: "Fixed-length string.\n\nchar(2) -> always 2 characters, padded if shorter.\n\nRarely used; prefer varchar.",
        template: Some("char(${1:1})"),
    },
    DslType {
        name: "nvarchar",
        description: "Unicode variable-length string.\n\nnvarchar(255) -> up to 255 Unicode characters.\n\nUse for international text.",
        template: Some("nvarchar(${1:255})"),
    },
    DslType {
        name: "date",
        description: "Calendar date (no time component).\n\nFormat: YYYY-MM-DD\n\nUse for birthdates, order dates without time.",
        template: None,
    },
    DslType {
        name: "time",
        description: "Time of day (no date component).\n\nFormat: HH:MM:SS.fff\n\nUse for scheduling, time-only fields.",
        template: None,
    },
    DslType {
        name: "timestamp",
        description: "Date and time.\n\nFormat: YYYY-MM-DD HH:MM:SS.fff\n\nUse for event times, created_at, updated_at.",
        template: None,
    },
    DslType {
        name: "datetime",
        description: "Alias for timestamp.\n\nDate and time without timezone.",
        template: None,
    },
    DslType {
        name: "timestamptz",
        description: "Timestamp with timezone.\n\nStores UTC, displays in local time.\n\nUse for global applications.",
        template: None,
    },
    DslType {
        name: "datetimeoffset",
        description: "Alias for timestamptz.\n\nSQL Server compatible timestamp with timezone.",
        template: None,
    },
    DslType {
        name: "bool",
        description: "Boolean true/false.\n\nUse for flags: is_active, has_shipped, etc.",
        template: None,
    },
    DslType {
        name: "boolean",
        description: "Alias for bool.\n\nBoolean true/false.",
        template: None,
    },
    DslType {
        name: "json",
        description: "JSON data type.\n\nStores structured data as JSON. Query with JSON functions.",
        template: None,
    },
    DslType {
        name: "jsonb",
        description: "Binary JSON (PostgreSQL).\n\nMore efficient for querying than json.\n\nPreferred for PostgreSQL.",
        template: None,
    },
    DslType {
        name: "uuid",
        description: "Universally unique identifier.\n\nFormat: 550e8400-e29b-41d4-a716-446655440000\n\nUse for distributed IDs.",
        template: None,
    },
    DslType {
        name: "binary",
        description: "Binary data.\n\nUse for raw bytes, file contents.",
        template: None,
    },
    DslType {
        name: "blob",
        description: "Binary large object.\n\nAlias for binary. Use for large binary data.",
        template: None,
    },
    DslType {
        name: "decimal",
        description: "Fixed-precision decimal type.\n\ndecimal(10, 2) -> up to 10 digits, 2 after decimal\ndecimal(18, 4) -> high precision for financial data\n\nUse for money, rates, precise calculations.",
        template: Some("decimal(${1:10}, ${2:2})"),
    },
    DslType {
        name: "numeric",
        description: "Alias for decimal.\n\nFixed-precision decimal type.",
        template: Some("numeric(${1:10}, ${2:2})"),
    },
];

/// All DSL constants.
pub static CONSTANTS: &[DslConstant] = &[
    DslConstant {
        name: "MANY_TO_ONE",
        description: "Many rows reference one row.\n\nExample: Many orders -> one customer\n\nMost common relationship type.",
        category: "cardinality",
    },
    DslConstant {
        name: "ONE_TO_MANY",
        description: "One row referenced by many rows.\n\nExample: One customer <- many orders\n\nReverse of MANY_TO_ONE.",
        category: "cardinality",
    },
    DslConstant {
        name: "ONE_TO_ONE",
        description: "Exactly one row matches one row.\n\nExample: User <-> user_profile\n\nRare - consider merging tables.",
        category: "cardinality",
    },
    DslConstant {
        name: "MANY_TO_MANY",
        description: "Many rows match many rows.\n\nExample: Students <-> Courses\n\nRequires a junction table.",
        category: "cardinality",
    },
    DslConstant {
        name: "M2O",
        description: "Shorthand for MANY_TO_ONE.",
        category: "cardinality",
    },
    DslConstant {
        name: "O2M",
        description: "Shorthand for ONE_TO_MANY.",
        category: "cardinality",
    },
    DslConstant {
        name: "O2O",
        description: "Shorthand for ONE_TO_ONE.",
        category: "cardinality",
    },
    DslConstant {
        name: "M2M",
        description: "Shorthand for MANY_TO_MANY.",
        category: "cardinality",
    },
    DslConstant {
        name: "TABLE",
        description: "Full table refresh.\n\nDrops and recreates the entire table each run.\n\nUse for small tables or when incremental isn't possible.",
        category: "materialization",
    },
    DslConstant {
        name: "VIEW",
        description: "Create as database view.\n\nNo data stored - query runs on access.\n\nUse for simple transformations or real-time needs.",
        category: "materialization",
    },
    DslConstant {
        name: "INCREMENTAL",
        description: "Process only new/changed data.\n\nRequires incremental_key (usually a date).\n\nFastest for large tables with append-only or slowly changing data.",
        category: "materialization",
    },
    DslConstant {
        name: "SNAPSHOT",
        description: "Point-in-time snapshot.\n\nCaptures full state at each run.\n\nUse for tracking historical states.",
        category: "materialization",
    },
    DslConstant {
        name: "SCD0",
        description: "Never update (Type 0).\n\nKeeps original value forever.\n\nUse for immutable attributes.",
        category: "scd",
    },
    DslConstant {
        name: "SCD1",
        description: "Overwrite changes (Type 1).\n\nNo history kept - current value only.\n\nSimplest approach, use when history isn't needed.",
        category: "scd",
    },
    DslConstant {
        name: "SCD2",
        description: "Track full history (Type 2).\n\nCreates new row for each change with effective dates.\n\nUse when you need historical analysis.",
        category: "scd",
    },
    DslConstant {
        name: "SCD_TYPE_0",
        description: "Alias for SCD0.",
        category: "scd",
    },
    DslConstant {
        name: "SCD_TYPE_1",
        description: "Alias for SCD1.",
        category: "scd",
    },
    DslConstant {
        name: "SCD_TYPE_2",
        description: "Alias for SCD2.",
        category: "scd",
    },
    DslConstant {
        name: "APPEND_ONLY",
        description: "Source only adds new rows.\n\nNo updates or deletes expected.\n\nMost efficient for event/log tables.",
        category: "change_tracking",
    },
    DslConstant {
        name: "CDC",
        description: "Change Data Capture.\n\nSource provides insert/update/delete operations.\n\nUse with CDC-enabled sources.",
        category: "change_tracking",
    },
    DslConstant {
        name: "FULL_SNAPSHOT",
        description: "Full table snapshot each sync.\n\nCompares snapshots to detect changes.\n\nUse when source doesn't track changes.",
        category: "change_tracking",
    },
    DslConstant {
        name: "CTE",
        description: "Common Table Expression.\n\nInlined into the query - no temp table.\n\nDefault for intermediates. Best for simple transforms.",
        category: "intermediate",
    },
    DslConstant {
        name: "TEMP_TABLE",
        description: "Temporary table.\n\nMaterializes intermediate results.\n\nUse for complex transforms referenced multiple times.",
        category: "intermediate",
    },
    DslConstant {
        name: "ASC",
        description: "Ascending sort order.\n\n1, 2, 3 or A, B, C\n\nDefault if not specified.",
        category: "sort",
    },
    DslConstant {
        name: "DESC",
        description: "Descending sort order.\n\n3, 2, 1 or C, B, A\n\nUse for \"most recent first\" ordering.",
        category: "sort",
    },
    DslConstant {
        name: "FIRST",
        description: "Keep first row per partition.\n\nWith ORDER BY date DESC -> keeps most recent.\nWith ORDER BY date ASC -> keeps oldest.",
        category: "dedup",
    },
    DslConstant {
        name: "LAST",
        description: "Keep last row per partition.\n\nOpposite of FIRST based on ORDER BY.",
        category: "dedup",
    },
];

/// All DSL methods.
pub static METHODS: &[DslMethod] = &[
    DslMethod {
        name: "from",
        description: "Set the source table name.",
        template: ":from(\"${1:schema.table}\")",
        entity_types: &["source"],
    },
    DslMethod {
        name: "columns",
        description: "Define column schema or select columns.",
        template: ":columns({\\n\\t$1\\n})",
        entity_types: &["source", "table", "fact", "dimension"],
    },
    DslMethod {
        name: "filter",
        description: "Apply WHERE clause filter.",
        template: ":filter(\"${1:condition}\")",
        entity_types: &["source", "table"],
    },
    DslMethod {
        name: "change_tracking",
        description: "Set change tracking mode for incremental processing.",
        template: ":change_tracking(${1:APPEND_ONLY}, \"${2:column}\")",
        entity_types: &["source"],
    },
    DslMethod {
        name: "dedup",
        description: "Configure deduplication.",
        template: ":dedup({\\n\\tpartition_by = { \"${1:column}\" },\\n\\torder_by = { { expr = col(\"${2:column}\"), dir = \"Desc\" } },\\n\\tkeep = \"First\",\\n})",
        entity_types: &["source", "table"],
    },
    DslMethod {
        name: "schema",
        description: "Override schema name.",
        template: ":schema(\"${1:schema_name}\")",
        entity_types: &["source"],
    },
    DslMethod {
        name: "description",
        description: "Add documentation.",
        template: ":description(\"${1:text}\")",
        entity_types: &["source", "table", "fact", "dimension", "query", "report"],
    },
    DslMethod {
        name: "primary_key",
        description: "Set primary key columns.",
        template: ":primary_key({ \"${1:column}\" })",
        entity_types: &["table", "dimension"],
    },
    DslMethod {
        name: "tags",
        description: "Add tags for filtering builds.",
        template: ":tags({ \"${1:tag}\" })",
        entity_types: &["table"],
    },
    DslMethod {
        name: "materialized",
        description: "Control materialization.",
        template: ":materialized(${1:true})",
        entity_types: &["table"],
    },
    DslMethod {
        name: "target",
        description: "Set output table name.",
        template: ":target(\"${1:schema.table}\")",
        entity_types: &["table", "fact", "dimension"],
    },
    DslMethod {
        name: "target_schema",
        description: "Set output schema.",
        template: ":target_schema(\"${1:schema}\")",
        entity_types: &["table"],
    },
    DslMethod {
        name: "group_by",
        description: "Add GROUP BY for pre-aggregation.",
        template: ":group_by({ \"${1:column}\" })",
        entity_types: &["table"],
    },
    DslMethod {
        name: "window_columns",
        description: "Add window function columns.",
        template: ":window_columns({\\n\\t$1\\n})",
        entity_types: &["table"],
    },
    DslMethod {
        name: "join",
        description: "Add a join to another entity.",
        template: ":join(\"${1:entity}\", \"${2:left}\", \"${3:condition}\")",
        entity_types: &["table"],
    },
    DslMethod {
        name: "strategy",
        description: "Set materialization strategy.",
        template: ":strategy({\\n\\ttype = \"${1:Incremental}\",\\n\\tincremental_key = \"${2:column}\",\\n})",
        entity_types: &["table", "fact"],
    },
    DslMethod {
        name: "grain",
        description: "Define fact table grain (level of detail).",
        template: ":grain({ ${1:entity}.${2:column} })",
        entity_types: &["fact"],
    },
    DslMethod {
        name: "measures",
        description: "Define measures (metrics) for the fact.",
        template: ":measures({\\n\\t$1\\n})",
        entity_types: &["fact"],
    },
    DslMethod {
        name: "includes",
        description: "Include dimension columns in the fact.",
        template: ":includes({\\n\\t${1:dim} = { ${2:columns} },\\n})",
        entity_types: &["fact", "dimension"],
    },
    DslMethod {
        name: "measure",
        description: "Add a measure to a fact.",
        template: ":measure(\"${1:name}\", ${2:sum(\"${3:column}\")})",
        entity_types: &["fact"],
    },
    DslMethod {
        name: "table_type",
        description: "Set table type (VIEW, TABLE, etc).",
        template: ":table_type(${1:VIEW})",
        entity_types: &["fact", "table"],
    },
    DslMethod {
        name: "source",
        description: "Set source entity for dimension.",
        template: ":source(\"${1:entity}\")",
        entity_types: &["dimension"],
    },
    DslMethod {
        name: "select",
        description: "Select columns for query output.",
        template: ":select({\\n\\t$1\\n})",
        entity_types: &["query", "report"],
    },
    DslMethod {
        name: "order_by",
        description: "Set sort order.",
        template: ":order_by({ \"${1:column}\" })",
        entity_types: &["query", "report"],
    },
    DslMethod {
        name: "limit",
        description: "Limit number of results.",
        template: ":limit(${1:100})",
        entity_types: &["query", "report"],
    },
];

/// All DSL blocks.
pub static BLOCKS: &[DslBlock] = &[
    DslBlock {
        name: "source",
        description: "Define a source entity that maps to a database table.\n\nSources are the entry points for data - they define the schema and primary keys for raw tables that Mantis will read from.",
        template: "source(\"${1:name}\")\n\t:from(\"${2:schema.table}\")\n\t:columns({\n\t\t${3:id} = pk(int64),\n\t})",
    },
    DslBlock {
        name: "fact",
        description: "Define a fact table for analytics.\n\nFacts are the center of star schemas - they contain measures (metrics) and foreign keys to dimensions.",
        template: "fact(\"${1:fact_name}\")\n\t:target(\"${2:analytics.fact_name}\")\n\t:grain({ ${3:entity}.${4:column} })\n\t:includes({\n\t\t${5:dim} = { ${6:columns} },\n\t})\n\t:measures({\n\t\t${7:measure} = sum(\"${8:column}\"),\n\t})",
    },
    DslBlock {
        name: "dimension",
        description: "Define a dimension table for descriptive attributes.\n\nDimensions contain the \"who, what, where, when\" context for facts.",
        template: "dimension(\"${1:dim_name}\")\n\t:target(\"${2:analytics.dim_name}\")\n\t:source(\"${3:source_entity}\")\n\t:columns({ ${4:columns} })\n\t:primary_key({ \"${5:id}\" })",
    },
    DslBlock {
        name: "relationship",
        description: "Define a relationship between two entities.\n\nRelationships enable automatic joins. Cardinality (MANY_TO_ONE, ONE_TO_MANY, etc.) determines join behavior and validation.",
        template: "relationship {\n\tfrom = ${1:from_entity}.${2:column},\n\tto = ${3:to_entity}.${4:column},\n\tcardinality = MANY_TO_ONE,\n}",
    },
    DslBlock {
        name: "table",
        description: "Define a table transformation for ETL.\n\nTables are the ETL layer - use them for staging, filtering, unions, pre-aggregation, and building reusable transformation layers.",
        template: "table(\"${1:name}\", { from = \"${2:source_entity}\" })\n\t:columns({ \"${3:col1}\", \"${4:col2}\" })",
    },
    DslBlock {
        name: "query",
        description: "Define a semantic query.\n\nThe 'from' field is optional - anchor facts are inferred from measures.\n\nQueries reference measures and dimensions from facts, with automatic joins based on the relationship graph.\n\nSupports filtered measures (measure():where()) and derived measures (derived()).",
        template: "query \"${1:query_name}\" {\n\tfrom = \"${2:fact_name}\",  -- Optional: inferred from measures\n\n\tselect = {\n\t\t${3:entity}.${4:column},\n\t\tmeasure \"${5:measure_name}\",\n\t},\n\n\twhere = {\n\t\tfilter(${6:entity}.${7:column}, \"=\", \"${8:value}\"),\n\t},\n\n\torder_by = { desc(\"${9:measure}\") },\n\tlimit = ${10:100},\n}",
    },
    DslBlock {
        name: "report",
        description: "Define a multi-fact report.\n\nReports combine measures from multiple facts into a single query. Filters are automatically routed to applicable facts based on the relationship graph.",
        template: "report \"${1:report_name}\" {\n\tmeasures = {\n\t\t\"${2:fact}.${3:measure}\",\n\t},\n\tfilters = {\n\t\t\"${4:filter_expression}\",\n\t},\n\tgroup_by = { ${5:group_columns} },\n}",
    },
    DslBlock {
        name: "pivot_report",
        description: "Define a pivot/cross-tab report.\n\nPivot reports create matrix output like Excel pivot tables, with row dimensions on the left, column dimension values as headers, and measures in cells.",
        template: "pivot_report \"${1:pivot_name}\" {\n\trows = {\n\t\t\"${2:dimension}.${3:column}\",\n\t},\n\tcolumns = \"${4:time.quarter}\",\n\tvalues = {\n\t\t${5:revenue} = { measure = \"${6:orders_fact}.${7:revenue}\" },\n\t},\n\tfilters = {\n\t\t\"${8:filter_expression}\",\n\t},\n}",
    },
    DslBlock {
        name: "link",
        description: "Shorthand for a many-to-one relationship.\n\nlink(orders.customer_id, customers.id)\n\nEquivalent to a relationship block with MANY_TO_ONE cardinality.",
        template: "link(${1:from}.${2:col}, ${3:to}.${4:col})",
    },
];

// =============================================================================
// Completion Item Conversion
// =============================================================================

impl DslFunction {
    /// Convert to LSP CompletionItem.
    pub fn to_completion_item(&self) -> CompletionItem {
        CompletionItem {
            label: self.name.to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some(format!("[{}] function", self.category)),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: self.description.to_string(),
            })),
            insert_text: Some(self.template.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }
}

impl DslType {
    /// Convert to LSP CompletionItem.
    pub fn to_completion_item(&self) -> CompletionItem {
        CompletionItem {
            label: self.name.to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("type".to_string()),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: self.description.to_string(),
            })),
            insert_text: self.template.map(|t| t.to_string()),
            insert_text_format: self.template.map(|_| InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }
}

impl DslConstant {
    /// Convert to LSP CompletionItem.
    pub fn to_completion_item(&self) -> CompletionItem {
        CompletionItem {
            label: self.name.to_string(),
            kind: Some(CompletionItemKind::CONSTANT),
            detail: Some(format!("[{}] constant", self.category)),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: self.description.to_string(),
            })),
            insert_text: Some(self.name.to_string()),
            insert_text_format: Some(InsertTextFormat::PLAIN_TEXT),
            ..Default::default()
        }
    }
}

impl DslMethod {
    /// Convert to LSP CompletionItem.
    pub fn to_completion_item(&self) -> CompletionItem {
        CompletionItem {
            label: self.name.to_string(),
            kind: Some(CompletionItemKind::METHOD),
            detail: Some(format!("method ({})", self.entity_types.join(", "))),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: self.description.to_string(),
            })),
            insert_text: Some(self.template.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }

    /// Check if this method applies to a given entity type.
    pub fn applies_to(&self, entity_type: &str) -> bool {
        self.entity_types.iter().any(|&t| t == entity_type)
    }
}

impl DslBlock {
    /// Convert to LSP CompletionItem.
    pub fn to_completion_item(&self) -> CompletionItem {
        CompletionItem {
            label: self.name.to_string(),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some("block".to_string()),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: self.description.to_string(),
            })),
            insert_text: Some(self.template.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }
}

// =============================================================================
// Lookup Helpers
// =============================================================================

/// Get all functions in a specific category.
pub fn functions_by_category(category: &str) -> Vec<&'static DslFunction> {
    FUNCTIONS.iter().filter(|f| f.category == category).collect()
}

/// Get all methods applicable to an entity type.
pub fn methods_for_entity(entity_type: &str) -> Vec<&'static DslMethod> {
    METHODS.iter().filter(|m| m.applies_to(entity_type)).collect()
}

/// Get all constants in a specific category.
pub fn constants_by_category(category: &str) -> Vec<&'static DslConstant> {
    CONSTANTS.iter().filter(|c| c.category == category).collect()
}

/// Find a function by name.
pub fn find_function(name: &str) -> Option<&'static DslFunction> {
    FUNCTIONS.iter().find(|f| f.name == name)
}

/// Find a type by name.
pub fn find_type(name: &str) -> Option<&'static DslType> {
    TYPES.iter().find(|t| t.name == name)
}

/// Find a constant by name.
pub fn find_constant(name: &str) -> Option<&'static DslConstant> {
    CONSTANTS.iter().find(|c| c.name == name)
}

/// Find a method by name.
pub fn find_method(name: &str) -> Option<&'static DslMethod> {
    METHODS.iter().find(|m| m.name == name)
}

/// Find a block by name.
pub fn find_block(name: &str) -> Option<&'static DslBlock> {
    BLOCKS.iter().find(|b| b.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_functions_loaded() {
        assert!(!FUNCTIONS.is_empty());
        assert!(find_function("sum").is_some());
    }

    #[test]
    fn test_types_loaded() {
        assert!(!TYPES.is_empty());
        assert!(find_type("int64").is_some());
    }

    #[test]
    fn test_constants_loaded() {
        assert!(!CONSTANTS.is_empty());
        assert!(find_constant("MANY_TO_ONE").is_some());
    }

    #[test]
    fn test_methods_loaded() {
        assert!(!METHODS.is_empty());
        let from_method = find_method("from");
        assert!(from_method.is_some());
        assert!(from_method.unwrap().applies_to("source"));
    }

    #[test]
    fn test_blocks_loaded() {
        assert!(!BLOCKS.is_empty());
        assert!(find_block("source").is_some());
    }

    #[test]
    fn test_methods_for_entity() {
        let source_methods = methods_for_entity("source");
        assert!(!source_methods.is_empty());
        assert!(source_methods.iter().any(|m| m.name == "from"));
    }

    #[test]
    fn test_to_completion_item() {
        let func = find_function("sum").unwrap();
        let item = func.to_completion_item();
        assert_eq!(item.label, "sum");
        assert_eq!(item.kind, Some(CompletionItemKind::FUNCTION));
    }
}
