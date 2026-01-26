# SQL Planner Wave 3: Time Intelligence Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable advanced semantic layer capabilities with time intelligence (YTD, prior period, rolling averages) and calculated measures.

**Architecture:** Extend logical and physical plans with TimeMeasure and InlineMeasure nodes. Generate window functions for modern SQL dialects, with self-join fallback for older databases. Parse time suffixes from ShowItem variants and resolve calendar dimensions from UnifiedGraph.

**Tech Stack:** Rust, SQL window functions (OVER, PARTITION BY, ORDER BY, frame bounds), existing model types, UnifiedGraph calendar metadata

---

## WAVE 3: TIME INTELLIGENCE

### Task 1: TimeMeasure Logical Plan Node

**Purpose:** Represent time calculations in logical plan

**Files:**
- Modify: `src/planner/logical/plan.rs`
- Test: `tests/planner/logical_time_measure_test.rs`

**Test:** Create TimeMeasureNode with base measure and time modifier, verify structure

**Implement:** Add TimeMeasureNode struct to LogicalPlan enum with fields: base_measure (MeasureRef), time_modifier (TimeModifier enum), calendar (String), input (Box<LogicalPlan>). Reference design doc "Component 1: TimeMeasure Support".

**Verify:** TimeMeasureNode can be constructed and pattern matched

**Commit:** `feat(planner): add TimeMeasure logical plan node`

---

### Task 2: TimeModifier and TimeUnit Enums

**Purpose:** Define types of time calculations supported

**Files:**
- Modify: `src/planner/logical/plan.rs`
- Test: `tests/planner/time_modifier_test.rs`

**Test:** Create each TimeModifier variant, verify fields accessible

**Implement:** Add TimeModifier enum with variants: YearToDate, QuarterToDate, MonthToDate, PriorPeriod { periods_back }, Rolling { window_size, window_unit, aggregation }, PeriodOverPeriod { periods_back }. Add TimeUnit enum: Day, Week, Month, Quarter, Year. Add RollingAgg enum: Sum, Avg, Min, Max. See design doc "Logical Plan Extension".

**Verify:** All variants constructible, fields accessible

**Commit:** `feat(planner): add TimeModifier and TimeUnit types`

---

### Task 3: WindowFunction Physical Plan Node

**Purpose:** Represent SQL window functions in physical plan

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/physical_window_test.rs`

**Test:** Create WindowFunction node with partition, order, frame, verify structure

**Implement:** Add WindowFunction variant to PhysicalPlan enum with fields: input, function (WindowFunc enum), partition_by (Vec<ColumnRef>), order_by (ColumnRef), frame (WindowFrame). Reference design doc "Physical Plan Extension".

**Verify:** WindowFunction node constructible with all fields

**Commit:** `feat(planner): add WindowFunction physical plan node`

---

### Task 4: WindowFunc and WindowFrame Types

**Purpose:** Define window function types and frame bounds

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/window_frame_test.rs`

**Test:** Create WindowFrame with different bound combinations, verify valid

**Implement:** Add WindowFunc enum: Sum, Avg, Min, Max, RowNumber, Rank, Lag { offset }, Lead { offset }. Add WindowFrame struct with start/end FrameBound. Add FrameBound enum: UnboundedPreceding, Preceding(usize), CurrentRow, Following(usize). See design doc "Physical Plan Extension".

**Verify:** All frame types constructible, bounds make sense

**Commit:** `feat(planner): add window function and frame types`

---

### Task 5: TimeSuffix Parsing from ShowItem

**Purpose:** Parse time suffixes like "_ytd", "_prior_year" from measure names

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Test: `tests/planner/time_suffix_parsing_test.rs`

**Test:** Parse "revenue_ytd" returns (revenue, YearToDate), "sales_rolling_3m" returns (sales, Rolling{3, Month, Avg})

**Implement:** Add `parse_time_suffix()` method that: 1) checks if ShowItem::MeasureWithSuffix, 2) extracts suffix, 3) maps to TimeModifier ("ytd" → YearToDate, "prior_year" → PriorPeriod{1}, "rolling_3m" → Rolling{3, Month, Avg}). Reference design doc "parse_time_suffix()".

**Verify:** All supported suffixes parse correctly, invalid returns error

**Commit:** `feat(planner): parse time suffixes from measure names`

---

### Task 6: Calendar Dimension Resolution

**Purpose:** Find date/timestamp column for time calculations

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Modify: `src/semantic/graph/mod.rs` (if needed)
- Test: `tests/planner/calendar_resolution_test.rs`

**Test:** Report with date column in entity resolves to correct calendar dimension

**Implement:** Add `find_calendar_dimension()` method that: 1) looks for explicit calendar in report metadata, 2) searches entity columns for date/timestamp type in graph, 3) returns first date column found. Reference design doc calendar dimension integration.

**Verify:** Calendar correctly resolved from entity metadata

**Commit:** `feat(planner): resolve calendar dimensions from graph`

---

### Task 7: Enhanced ShowItem Handling - MeasureWithSuffix

**Purpose:** Update PlanBuilder to handle ShowItem::MeasureWithSuffix

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Test: `tests/planner/show_item_handling_test.rs`

**Test:** Report with MeasureWithSuffix generates TimeMeasureNode in logical plan

**Implement:** In build_project(), match on ShowItem::MeasureWithSuffix, parse suffix to TimeModifier, wrap current plan in TimeMeasureNode with base measure and modifier. Reference design doc "Enhanced ShowItem Handling".

**Verify:** Logical plan contains TimeMeasureNode for time measures

**Commit:** `feat(planner): handle MeasureWithSuffix in logical plan building`

---

### Task 8: Physical Converter - TimeMeasure to WindowFunction

**Purpose:** Convert TimeMeasure logical nodes to WindowFunction physical nodes

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/time_measure_conversion_test.rs`

**Test:** TimeMeasureNode converts to WindowFunction with correct frame

**Implement:** Add convert_time_measure() that: 1) converts input plan, 2) builds WindowFrame from TimeModifier (YTD → UNBOUNDED PRECEDING AND CURRENT ROW, Rolling → PRECEDING N AND CURRENT ROW), 3) gets partition/order columns from calendar, 4) creates WindowFunction node. Reference design doc "Physical Strategy Selection".

**Verify:** Different time modifiers produce correct window frames

**Commit:** `feat(planner): convert TimeMeasure to WindowFunction`

---

### Task 9: Window Frame Building - Year/Quarter/Month To Date

**Purpose:** Build correct window frames for cumulative calculations

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/window_frame_building_test.rs`

**Test:** YearToDate creates frame UNBOUNDED PRECEDING to CURRENT ROW with PARTITION BY YEAR

**Implement:** In build_window_frame(), match on TimeModifier: YTD/QTD/MTD → WindowFrame { start: UnboundedPreceding, end: CurrentRow }, partition_by includes year/quarter/month extraction from date column. Reference design doc "build_window_frame()".

**Verify:** Cumulative frames span from period start to current row

**Commit:** `feat(planner): build window frames for cumulative calculations`

---

### Task 10: Window Frame Building - Rolling Windows

**Purpose:** Build correct window frames for moving averages/sums

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/window_frame_building_test.rs`

**Test:** Rolling { window_size: 3, unit: Month, agg: Avg } creates frame PRECEDING 2 to CURRENT ROW

**Implement:** In build_window_frame() for Rolling variant, create WindowFrame { start: Preceding(window_size - 1), end: CurrentRow }, aggregation from RollingAgg. Reference design doc rolling window frames.

**Verify:** Rolling windows span correct number of preceding rows

**Commit:** `feat(planner): build window frames for rolling calculations`

---

### Task 11: Window Frame Building - Prior Period Comparisons

**Purpose:** Build window frames for LAG/LEAD calculations

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/window_frame_building_test.rs`

**Test:** PriorPeriod { periods_back: 1 } creates LAG(1) window function

**Implement:** In convert_time_measure() for PriorPeriod, use WindowFunc::Lag { offset: periods_back } instead of aggregate function. Frame should be Preceding(offset) to Preceding(offset). Reference design doc window function types.

**Verify:** Prior period uses LAG correctly

**Commit:** `feat(planner): build window frames for prior period comparisons`

---

### Task 12: Window Function SQL Generation

**Purpose:** Generate SQL window function syntax from WindowFunction nodes

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Modify: `src/sql/expr.rs` (if Window variant doesn't exist)
- Test: `tests/planner/window_sql_generation_test.rs`

**Test:** WindowFunction.to_query() generates SELECT with OVER clause

**Implement:** In to_query() for WindowFunction: 1) get input query, 2) create SQL window expression with OVER clause, 3) build PARTITION BY from partition_by columns, 4) build ORDER BY from order_by column, 5) convert WindowFrame to SQL frame syntax, 6) add to SELECT. Reference design doc "Query Generation".

**Verify:** Generated SQL has valid window function syntax

**Commit:** `feat(planner): generate SQL window functions`

---

### Task 13: Window Frame SQL Conversion

**Purpose:** Convert WindowFrame to SQL frame clause syntax

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/window_frame_sql_test.rs`

**Test:** WindowFrame { Preceding(3), CurrentRow } generates "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"

**Implement:** Add convert_window_frame() helper that maps FrameBound variants to SQL strings: UnboundedPreceding → "UNBOUNDED PRECEDING", Preceding(n) → "N PRECEDING", CurrentRow → "CURRENT ROW", Following(n) → "N FOLLOWING". Build ROWS BETWEEN clause. Reference design doc SQL generation.

**Verify:** All frame bound combinations produce valid SQL

**Commit:** `feat(planner): convert window frames to SQL syntax`

---

### Task 14: InlineMeasure Logical Plan Node

**Purpose:** Represent user-defined calculated measures

**Files:**
- Modify: `src/planner/logical/plan.rs`
- Test: `tests/planner/inline_measure_test.rs`

**Test:** Create InlineMeasureNode with expression, verify fields accessible

**Implement:** Add InlineMeasureNode to LogicalPlan enum with fields: name (String), expression (model::Expr), input (Box<LogicalPlan>). Reference design doc "Component 2: InlineMeasure Support".

**Verify:** InlineMeasureNode constructible with arbitrary expressions

**Commit:** `feat(planner): add InlineMeasure logical plan node`

---

### Task 15: ComputedColumn Physical Plan Node

**Purpose:** Represent calculated columns in physical plan

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/computed_column_test.rs`

**Test:** Create ComputedColumn node, verify structure

**Implement:** Add ComputedColumn variant to PhysicalPlan enum with fields: input, name, expression. Reference design doc "Physical Plan Extension" for inline measures.

**Verify:** ComputedColumn node constructible

**Commit:** `feat(planner): add ComputedColumn physical plan node`

---

### Task 16: Enhanced ShowItem Handling - InlineMeasure

**Purpose:** Handle ShowItem::InlineMeasure in logical plan building

**Files:**
- Modify: `src/planner/logical/builder.rs`
- Test: `tests/planner/show_item_handling_test.rs`

**Test:** Report with ShowItem::InlineMeasure generates InlineMeasureNode

**Implement:** In build_project(), match on ShowItem::InlineMeasure, create InlineMeasureNode wrapping current plan with name and expression. Reference design doc "Enhanced ShowItem Handling".

**Verify:** Inline measures appear in logical plan

**Commit:** `feat(planner): handle InlineMeasure in logical plan building`

---

### Task 17: InlineMeasure to ComputedColumn Conversion

**Purpose:** Convert inline measure logical nodes to computed column physical nodes

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/inline_measure_conversion_test.rs`

**Test:** InlineMeasureNode converts to ComputedColumn

**Implement:** In convert() for InlineMeasure variant, create PhysicalPlan::ComputedColumn with same name and expression, converted input. Straightforward mapping. Reference design doc physical plan extension.

**Verify:** Logical inline measures become physical computed columns

**Commit:** `feat(planner): convert InlineMeasure to ComputedColumn`

---

### Task 18: ComputedColumn SQL Generation

**Purpose:** Generate SQL SELECT with calculated expressions

**Files:**
- Modify: `src/planner/physical/plan.rs`
- Test: `tests/planner/computed_column_sql_test.rs`

**Test:** ComputedColumn.to_query() generates SELECT with expression aliased as name

**Implement:** In to_query() for ComputedColumn: 1) get input query, 2) build QueryContext, 3) convert expression using ExprConverter, 4) add to SELECT as SelectExpr with alias = name. Reference design doc "Query Generation - InlineMeasure".

**Verify:** Generated SQL has computed column in SELECT with correct alias

**Commit:** `feat(planner): generate SQL for computed columns`

---

### Task 19: Self-Join Fallback Strategy (Optional)

**Purpose:** Support databases without window function capability

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/self_join_fallback_test.rs`

**Test:** When dialect doesn't support windows, TimeMeasure converts to self-join

**Implement:** Add convert_time_measure_self_join() that: 1) creates correlated subquery with same table, 2) adds WHERE clause for time range (YTD: WHERE date <= outer.date AND YEAR(date) = YEAR(outer.date)), 3) wraps in JOIN. Only implement if needed. Reference design doc fallback strategy.

**Verify:** Self-join produces equivalent results to window function

**Commit:** `feat(planner): add self-join fallback for time measures`

---

### Task 20: Wave 3 Integration Test - Time Intelligence Report

**Purpose:** End-to-end test of all time intelligence features

**Files:**
- Test: `tests/planner/wave3_integration_test.rs`

**Test:** Create report with: revenue measure, revenue_ytd (YTD), revenue_prior_year (prior period), revenue_rolling_3m (rolling), profit inline measure (revenue - cost). Verify logical plan has TimeMeasure and InlineMeasure nodes. Verify physical plan has WindowFunction and ComputedColumn nodes. Verify generated SQL has window functions and calculated columns.

**Implement:** Comprehensive test with realistic report using all time intelligence features. Build UnifiedGraph with calendar dimension. Verify SQL syntax valid and semantically correct. Reference design doc appendix example.

**Verify:** All time intelligence features work together end-to-end

**Commit:** `test(planner): add Wave 3 comprehensive time intelligence test`

---

## Wave 3 Additional Tasks

### Task 21: Period Over Period Calculations

**Purpose:** Support comparing current period to prior period (e.g., sales vs last_year_sales)

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/period_over_period_test.rs`

**Test:** PeriodOverPeriod generates window function with LAG and calculation

**Implement:** Extend build_window_frame() to handle PeriodOverPeriod variant. Generate two window functions: current value and LAG(value, periods_back), then compute difference/ratio. Reference design doc time modifier types.

**Verify:** Period-over-period calculations produce correct comparative values

**Commit:** `feat(planner): add period over period comparison calculations`

---

### Task 22: Multiple Time Measures in Single Query

**Purpose:** Support queries with multiple time calculations efficiently

**Files:**
- Modify: `src/planner/physical/converter.rs`
- Test: `tests/planner/multiple_time_measures_test.rs`

**Test:** Report with multiple _ytd, _mtd, _rolling suffixes generates multiple window functions in single query

**Implement:** Ensure each TimeMeasureNode converts independently, all window functions appear in SELECT. Avoid redundant subqueries. Reference design doc multiple measure handling.

**Verify:** Single SQL query with multiple OVER clauses, not nested subqueries

**Commit:** `feat(planner): optimize multiple time measures in single query`

---

### Task 23: Time Measure Cost Estimation

**Purpose:** Estimate cost of window function execution

**Files:**
- Modify: `src/planner/cost/estimator.rs`
- Test: `tests/planner/window_cost_test.rs`

**Test:** WindowFunction cost reflects sorting and aggregation overhead

**Implement:** In estimate_cost() for WindowFunction: rows_out = input.rows_out (window doesn't filter), CPU cost = input.cpu_cost + (rows * log(rows)) for sort + rows for aggregation, memory cost for sort buffer. Reference design doc cost estimation.

**Verify:** Window functions have higher CPU cost than simple scans

**Commit:** `feat(planner): estimate window function execution costs`

---

### Task 24: Time Intelligence Error Handling

**Purpose:** Handle missing calendar dimensions and invalid suffixes

**Files:**
- Modify: `src/planner/mod.rs`
- Modify: `src/planner/logical/builder.rs`
- Test: `tests/planner/time_intelligence_errors_test.rs`

**Test:** Report with time suffix but no date column returns clear error. Invalid suffix returns UnknownTimeSuffix error.

**Implement:** Add error variants: MissingCalendarDimension, UnknownTimeSuffix, InvalidWindowFrame. Return clear error messages with suggestions. Reference design doc "Error Handling".

**Verify:** All error cases produce helpful error messages

**Commit:** `feat(planner): add time intelligence error handling`

---

### Task 25: Time Intelligence Documentation Examples

**Purpose:** Document time intelligence features with examples

**Files:**
- Create: `docs/examples/time_intelligence.md`

**Test:** Manual review of documentation clarity

**Implement:** Write documentation showing: 1) supported time suffixes and their meanings, 2) example reports with time measures, 3) generated SQL for each type, 4) inline measure examples, 5) combining time and inline measures. Include copy-paste-ready examples.

**Verify:** Documentation clear and examples work

**Commit:** `docs(planner): add time intelligence examples and documentation`

---

## Wave 3 Completion Checklist

- ✅ TimeMeasure logical and physical plan nodes
- ✅ TimeModifier enum with all variants (YTD, QTD, MTD, PriorPeriod, Rolling, PeriodOverPeriod)
- ✅ WindowFunction physical nodes with frame support
- ✅ Window function SQL generation (OVER, PARTITION BY, ORDER BY, ROWS BETWEEN)
- ✅ Time suffix parsing from ShowItem
- ✅ Calendar dimension resolution from graph
- ✅ InlineMeasure logical and physical plan nodes
- ✅ Computed column SQL generation
- ✅ Multiple time measures in single query
- ✅ Self-join fallback (optional)
- ✅ Time measure cost estimation
- ✅ Comprehensive error handling
- ✅ Integration tests pass
- ✅ Documentation with examples

**Result:** Complete semantic layer with full time intelligence and calculated measure support.

---

## Example Generated SQL

### Input Report
```yaml
from: [sales]
show:
  - date
  - revenue
  - revenue_ytd
  - revenue_prior_year
  - revenue_rolling_3m
  - profit: revenue - cost  # inline measure
group:
  - date
```

### Generated SQL
```sql
SELECT 
    date,
    SUM(revenue) as revenue,
    SUM(SUM(revenue)) OVER (
        PARTITION BY YEAR(date)
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as revenue_ytd,
    LAG(SUM(revenue), 1) OVER (
        PARTITION BY YEAR(date)
        ORDER BY date
    ) as revenue_prior_year,
    AVG(SUM(revenue)) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as revenue_rolling_3m,
    (SUM(revenue) - SUM(cost)) as profit
FROM sales
GROUP BY date
ORDER BY date
```

**Explanation:**
- YTD: Cumulative sum within year partition
- Prior year: LAG function to get previous row
- Rolling 3m: Average of 3 most recent rows
- Profit: Computed expression from inline measure

---

## Success Metrics

### Functional Completeness
- ✅ All time suffixes supported (ytd, qtd, mtd, prior, rolling, pop)
- ✅ All ShowItem variants handled (Column, Measure, MeasureWithSuffix, InlineMeasure)
- ✅ Window functions generate valid SQL
- ✅ Inline measures support complex expressions
- ✅ Multiple time measures work together

### SQL Quality
- ✅ Window functions use correct PARTITION BY and ORDER BY
- ✅ Frame bounds match time calculation semantics
- ✅ Single query for multiple measures (no nested subqueries)
- ✅ Generated SQL passes syntax validation

### Error Handling
- ✅ Clear error for missing calendar dimension
- ✅ Clear error for invalid time suffix
- ✅ Graceful handling of unsupported window functions

---

**Implementation Complete:** All three waves provide a production-ready SQL planner with comprehensive query support, intelligent optimization, and advanced semantic layer capabilities.
