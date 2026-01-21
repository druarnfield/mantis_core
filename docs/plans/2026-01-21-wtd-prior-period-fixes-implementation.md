# WTD and Prior Period Time Function Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix semantic incorrectness in WTD and PriorMonth/PriorWeek time function mappings by adding proper TimeFunction enum variants.

**Architecture:** Add three new variants to the `TimeFunction` enum (`WeekToDate`, `PriorMonth`, `PriorWeek`) following the existing pattern, then update the translation layer to use them instead of incorrect mappings.

**Tech Stack:** Rust, existing semantic and translation modules.

---

## Task 1: Add WeekToDate Variant to TimeFunction Enum

**Files:**
- Modify: `src/semantic/planner/types.rs` (around line 250, in TimeFunction enum)

### Step 1: Locate the TimeFunction enum and find where to add WeekToDate

The enum should already have `YearToDate`, `QuarterToDate`, and `MonthToDate`. Add `WeekToDate` right after `MonthToDate`.

### Step 2: Add the WeekToDate variant

```rust
/// Week-to-date: cumulative sum from start of week.
///
/// SQL: `SUM(measure) OVER (PARTITION BY year, week ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
WeekToDate {
    /// The measure to accumulate
    measure: String,
    /// Column to partition by (year column from date dimension)
    year_column: Option<String>,
    /// Week column for partitioning
    week_column: Option<String>,
    /// Column to order by (day column from date dimension)
    day_column: Option<String>,
    /// Optional role override (e.g., "ship_date" instead of default "order_date")
    via: Option<String>,
},
```

### Step 3: Verify it compiles

Run: `cargo check`
Expected: May have compilation errors in other files that pattern-match on TimeFunction (we'll fix those next)

### Step 4: Commit

```bash
git add src/semantic/planner/types.rs
git commit -m "feat(semantic): add WeekToDate variant to TimeFunction enum"
```

---

## Task 2: Add PriorMonth and PriorWeek Variants to TimeFunction Enum

**Files:**
- Modify: `src/semantic/planner/types.rs` (in TimeFunction enum, after PriorQuarter)

### Step 1: Locate PriorYear and PriorQuarter variants

The enum should already have `PriorYear` and `PriorQuarter`. Add `PriorMonth` and `PriorWeek` right after `PriorQuarter`.

### Step 2: Add the PriorMonth and PriorWeek variants

```rust
/// Prior month: same period in the prior month.
///
/// For monthly grain: `LAG(measure, 1)`
/// For daily grain: uses date arithmetic or LAG with appropriate offset
PriorMonth {
    measure: String,
    via: Option<String>,
},

/// Prior week: same period in the prior week.
///
/// For weekly grain: `LAG(measure, 1)`
/// For daily grain: `LAG(measure, 7)` or date arithmetic
PriorWeek {
    measure: String,
    via: Option<String>,
},
```

### Step 3: Verify it compiles

Run: `cargo check`
Expected: Compilation errors in pattern matching (we'll fix next)

### Step 4: Commit

```bash
git add src/semantic/planner/types.rs
git commit -m "feat(semantic): add PriorMonth and PriorWeek variants to TimeFunction enum"
```

---

## Task 3: Update Translation Layer to Use WeekToDate

**Files:**
- Modify: `src/translation/mod.rs` (around line 273-283, in translate_time_suffix function)

### Step 1: Find the TimeSuffix::Wtd mapping

Look for the match arm that handles `crate::model::TimeSuffix::Wtd`. It currently uses `TimeFunction::MonthToDate` with a TODO comment.

### Step 2: Replace with WeekToDate variant

Replace the entire match arm:

```rust
crate::model::TimeSuffix::Wtd => {
    DerivedExpr::TimeFunction(TimeFunction::WeekToDate {
        measure: measure_name.to_string(),
        year_column: None,
        week_column: None,
        day_column: None,
        via: None,
    })
}
```

### Step 3: Verify it compiles

Run: `cargo check`
Expected: Should compile now

### Step 4: Run existing tests

Run: `cargo test --test translation_test test_translate_all_time_suffixes`
Expected: PASS (this test already includes WTD)

### Step 5: Commit

```bash
git add src/translation/mod.rs
git commit -m "fix(translation): use WeekToDate for WTD time suffix"
```

---

## Task 4: Update Translation Layer to Use PriorMonth

**Files:**
- Modify: `src/translation/mod.rs` (around line 316-328, in translate_time_suffix function)

### Step 1: Find the TimeSuffix::PriorMonth mapping

Look for the match arm that handles `crate::model::TimeSuffix::PriorMonth`. It currently uses `TimeFunction::PriorPeriod` with a TODO comment.

### Step 2: Replace with PriorMonth variant

Replace the entire match arm:

```rust
crate::model::TimeSuffix::PriorMonth => {
    DerivedExpr::TimeFunction(TimeFunction::PriorMonth {
        measure: measure_name.to_string(),
        via: None,
    })
}
```

### Step 3: Verify it compiles

Run: `cargo check`
Expected: Should compile

### Step 4: Commit

```bash
git add src/translation/mod.rs
git commit -m "fix(translation): use PriorMonth for PriorMonth time suffix"
```

---

## Task 5: Update Translation Layer to Use PriorWeek

**Files:**
- Modify: `src/translation/mod.rs` (around line 329-335, in translate_time_suffix function)

### Step 1: Find the TimeSuffix::PriorWeek mapping

Look for the match arm that handles `crate::model::TimeSuffix::PriorWeek`. It currently uses `TimeFunction::PriorPeriod` with a TODO comment.

### Step 2: Replace with PriorWeek variant

Replace the entire match arm:

```rust
crate::model::TimeSuffix::PriorWeek => {
    DerivedExpr::TimeFunction(TimeFunction::PriorWeek {
        measure: measure_name.to_string(),
        via: None,
    })
}
```

### Step 3: Verify it compiles

Run: `cargo check`
Expected: Should compile

### Step 4: Run all translation tests

Run: `cargo test --test translation_test`
Expected: All tests PASS

### Step 5: Commit

```bash
git add src/translation/mod.rs
git commit -m "fix(translation): use PriorWeek for PriorWeek time suffix"
```

---

## Task 6: Add Validation Test for Correct Variant Usage

**Files:**
- Modify: `tests/translation/translation_test.rs`

### Step 1: Write test to verify WTD uses WeekToDate

Add this test at the end of the file:

```rust
#[test]
fn test_wtd_uses_correct_time_function() {
    use mantis::model::{
        Table, MeasureBlock, Measure, SqlExpr, ShowItem, TimeSuffix,
        Atom, AtomType,
    };
    use mantis::dsl::span::Span;
    use mantis::semantic::planner::types::{DerivedExpr, TimeFunction};
    
    // Setup model (reuse pattern from other tests)
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );
    
    let mut tables = HashMap::new();
    tables.insert(
        "fact_sales".to_string(),
        Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        },
    );
    
    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert(
        "revenue".to_string(),
        Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    
    measures.insert(
        "fact_sales".to_string(),
        MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures: measure_map,
        },
    );
    
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };
    
    // Test WTD
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::MeasureWithSuffix {
            name: "revenue".to_string(),
            suffix: TimeSuffix::Wtd,
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    assert_eq!(query.derived.len(), 1);
    
    // Verify it uses WeekToDate, not MonthToDate
    match &query.derived[0].expression {
        DerivedExpr::TimeFunction(TimeFunction::WeekToDate { .. }) => {
            // Success - correct variant
        }
        _ => panic!("Expected WeekToDate variant for WTD suffix"),
    }
}

#[test]
fn test_prior_month_uses_correct_time_function() {
    use mantis::model::{
        Table, MeasureBlock, Measure, SqlExpr, ShowItem, TimeSuffix,
        Atom, AtomType,
    };
    use mantis::dsl::span::Span;
    use mantis::semantic::planner::types::{DerivedExpr, TimeFunction};
    
    // Setup (same as above, abbreviated for plan)
    let mut atoms = HashMap::new();
    atoms.insert("revenue".to_string(), Atom {
        name: "revenue".to_string(),
        data_type: AtomType::Decimal,
    });
    
    let mut tables = HashMap::new();
    tables.insert("fact_sales".to_string(), Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms,
        times: HashMap::new(),
        slicers: HashMap::new(),
    });
    
    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert("revenue".to_string(), Measure {
        name: "revenue".to_string(),
        expr: SqlExpr {
            sql: "sum(@revenue)".to_string(),
            span: Span::default(),
        },
        filter: None,
        null_handling: None,
    });
    
    measures.insert("fact_sales".to_string(), MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures: measure_map,
    });
    
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };
    
    // Test PriorMonth
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::MeasureWithSuffix {
            name: "revenue".to_string(),
            suffix: TimeSuffix::PriorMonth,
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    assert_eq!(query.derived.len(), 1);
    
    // Verify it uses PriorMonth, not generic PriorPeriod
    match &query.derived[0].expression {
        DerivedExpr::TimeFunction(TimeFunction::PriorMonth { .. }) => {
            // Success - correct variant
        }
        _ => panic!("Expected PriorMonth variant for PriorMonth suffix"),
    }
}

#[test]
fn test_prior_week_uses_correct_time_function() {
    use mantis::model::{
        Table, MeasureBlock, Measure, SqlExpr, ShowItem, TimeSuffix,
        Atom, AtomType,
    };
    use mantis::dsl::span::Span;
    use mantis::semantic::planner::types::{DerivedExpr, TimeFunction};
    
    // Setup (same as above, abbreviated)
    let mut atoms = HashMap::new();
    atoms.insert("revenue".to_string(), Atom {
        name: "revenue".to_string(),
        data_type: AtomType::Decimal,
    });
    
    let mut tables = HashMap::new();
    tables.insert("fact_sales".to_string(), Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms,
        times: HashMap::new(),
        slicers: HashMap::new(),
    });
    
    let mut measures = HashMap::new();
    let mut measure_map = HashMap::new();
    measure_map.insert("revenue".to_string(), Measure {
        name: "revenue".to_string(),
        expr: SqlExpr {
            sql: "sum(@revenue)".to_string(),
            span: Span::default(),
        },
        filter: None,
        null_handling: None,
    });
    
    measures.insert("fact_sales".to_string(), MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures: measure_map,
    });
    
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables,
        measures,
        reports: HashMap::new(),
    };
    
    // Test PriorWeek
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::MeasureWithSuffix {
            name: "revenue".to_string(),
            suffix: TimeSuffix::PriorWeek,
            label: None,
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    assert_eq!(query.derived.len(), 1);
    
    // Verify it uses PriorWeek, not generic PriorPeriod
    match &query.derived[0].expression {
        DerivedExpr::TimeFunction(TimeFunction::PriorWeek { .. }) => {
            // Success - correct variant
        }
        _ => panic!("Expected PriorWeek variant for PriorWeek suffix"),
    }
}
```

### Step 2: Run the new tests

Run: `cargo test --test translation_test test_wtd_uses_correct_time_function`
Expected: PASS

Run: `cargo test --test translation_test test_prior_month_uses_correct_time_function`
Expected: PASS

Run: `cargo test --test translation_test test_prior_week_uses_correct_time_function`
Expected: PASS

### Step 3: Run all tests to verify no regressions

Run: `cargo test`
Expected: All tests PASS

### Step 4: Commit

```bash
git add tests/translation/translation_test.rs
git commit -m "test(translation): add validation tests for WTD, PriorMonth, PriorWeek variants"
```

---

## Summary

**Tasks completed:**
- ✅ Task 1: Add WeekToDate variant to TimeFunction enum
- ✅ Task 2: Add PriorMonth and PriorWeek variants to TimeFunction enum
- ✅ Task 3: Update translation layer to use WeekToDate
- ✅ Task 4: Update translation layer to use PriorMonth
- ✅ Task 5: Update translation layer to use PriorWeek
- ✅ Task 6: Add validation tests

**Files modified:**
- `src/semantic/planner/types.rs` - Added 3 new TimeFunction variants
- `src/translation/mod.rs` - Updated 3 time suffix mappings, removed TODO comments
- `tests/translation/translation_test.rs` - Added 3 validation tests

**Impact:**
- WTD now correctly uses WeekToDate instead of MonthToDate
- PriorMonth and PriorWeek are now distinguishable (not both generic PriorPeriod)
- All TODO comments removed
- Semantic correctness achieved
- No breaking changes (purely additive)

**Next steps:**
- Phase 4: SQL compilation will need to handle these new variants
- Calendar integration for week start day configuration
