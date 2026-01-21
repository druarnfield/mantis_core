# WTD and Prior Period Time Function Fixes - Design Document

**Date:** 2026-01-21  
**Status:** Approved  
**Goal:** Fix semantic incorrectness in WTD and PriorMonth/PriorWeek time function mappings

## Problem Statement

The translation layer currently has two semantic issues:

1. **WTD (Week-to-Date)** uses `TimeFunction::MonthToDate` which is semantically incorrect
2. **PriorMonth and PriorWeek** both map to generic `TimeFunction::PriorPeriod { periods_back: 1 }` with no granularity distinction

These mappings work syntactically but are semantically wrong and will cause incorrect SQL generation.

## Solution Overview

Add three new variants to the `TimeFunction` enum following the existing pattern:
- `WeekToDate` - for WTD accumulations
- `PriorMonth` - for month-over-month comparisons
- `PriorWeek` - for week-over-week comparisons

This maintains consistency with existing variants like `YearToDate`, `QuarterToDate`, `MonthToDate`, `PriorYear`, and `PriorQuarter`.

## Design

### 1. TimeFunction Enum Extensions

**Location:** `src/semantic/planner/types.rs`

Add three new variants to the `TimeFunction` enum:

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

**Rationale:**
- Follows the exact pattern of existing variants
- Type-safe and explicit about intent
- Makes SQL generation straightforward
- No breaking changes to existing code

### 2. Translation Layer Updates

**Location:** `src/translation/mod.rs`

Update the `translate_time_suffix` function to use the new variants:

#### WTD Mapping

**Before:**
```rust
crate::model::TimeSuffix::Wtd => {
    // TODO: WTD currently uses MonthToDate which is semantically incorrect...
    DerivedExpr::TimeFunction(TimeFunction::MonthToDate {
        measure: measure_name.to_string(),
        year_column: None,
        month_column: None,
        day_column: None,
        via: None,
    })
}
```

**After:**
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

#### PriorMonth Mapping

**Before:**
```rust
crate::model::TimeSuffix::PriorMonth => {
    // TODO: PriorMonth and PriorWeek both map to PriorPeriod...
    DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
        measure: measure_name.to_string(),
        periods_back: 1,
        via: None,
    })
}
```

**After:**
```rust
crate::model::TimeSuffix::PriorMonth => {
    DerivedExpr::TimeFunction(TimeFunction::PriorMonth {
        measure: measure_name.to_string(),
        via: None,
    })
}
```

#### PriorWeek Mapping

**Before:**
```rust
crate::model::TimeSuffix::PriorWeek => {
    // TODO: See PriorMonth comment above...
    DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
        measure: measure_name.to_string(),
        periods_back: 1,
        via: None,
    })
}
```

**After:**
```rust
crate::model::TimeSuffix::PriorWeek => {
    DerivedExpr::TimeFunction(TimeFunction::PriorWeek {
        measure: measure_name.to_string(),
        via: None,
    })
}
```

**Impact:**
- Clean, straightforward 1:1 mappings
- Removes all TODO comments for these cases
- Makes the translation semantically correct

### 3. Testing & Validation

**Test Updates:**

1. **Existing tests continue to pass** - `test_translate_all_time_suffixes` already tests WTD, PriorMonth, and PriorWeek. No changes needed.

2. **Optional validation test** (recommended):
   ```rust
   #[test]
   fn test_wtd_uses_week_to_date() {
       // Verify WTD suffix maps to WeekToDate variant
       // Pattern match on TimeFunction enum to ensure correct variant
   }
   ```

**SQL Generation Impact:**

The SQL compilation phase (Phase 4, not yet implemented) will need to handle these new variants:
- `WeekToDate` → Generate week-based window function with week partitioning
- `PriorMonth` → Generate LAG with month-appropriate offset (1 for monthly grain, ~30 for daily)
- `PriorWeek` → Generate LAG with week-appropriate offset (1 for weekly grain, 7 for daily)

This is documented for future work but doesn't block this fix.

## Breaking Changes

**None** - This is purely additive:
- New enum variants added to `TimeFunction`
- Translation layer updated to use them
- Existing code unaffected (all changes are internal)
- Tests continue to pass

## Implementation Notes

1. Add the three new variants to `TimeFunction` enum
2. Update pattern matching in `translate_time_suffix`
3. Remove TODO comments
4. Run tests to verify no regressions
5. Commit with message: `fix(semantic): add WeekToDate, PriorMonth, PriorWeek time functions`

## Future Work

- SQL compilation for these new variants (Phase 4)
- Calendar integration for week start day configuration
- Fiscal week variants if needed
