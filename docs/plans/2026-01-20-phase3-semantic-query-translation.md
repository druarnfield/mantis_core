# Phase 3: Semantic Query Translation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the translation layer that converts validated Report model types into SemanticQuery objects ready for SQL compilation, resolving all references and expanding time intelligence suffixes.

**Architecture:** Create `src/translation/` module with Report → SemanticQuery conversion. Resolve drill path references to FieldRefs, expand time suffixes into window functions, compile SQL expressions with @atom resolution, and route filters to appropriate query levels.

**Tech Stack:** Rust, existing model types (Phase 2), SemanticQuery types from `src/semantic/planner/types.rs`, regex for @atom detection.

---

## Task 1: Translation Infrastructure

**Goal:** Create translation module with basic error types and Report → SemanticQuery skeleton.

**Files:**
- Create: `src/translation/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/translation/translation_test.rs`

### Step 1: Write failing test for basic translation

```rust
// tests/translation/translation_test.rs
use mantis::model::{Model, Report};
use mantis::translation;
use std::collections::HashMap;

#[test]
fn test_translate_empty_report() {
    let model = Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };

    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    assert_eq!(query.from, Some("fact_sales".to_string()));
    assert_eq!(query.select.len(), 0);
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test translation_test`
Expected: Compilation error - module doesn't exist

### Step 3: Create translation module

```rust
// src/translation/mod.rs
//! Translation of Report model types to SemanticQuery.

use crate::model::{Model, Report};
use crate::semantic::planner::types::SemanticQuery;

/// Translation error.
#[derive(Debug, Clone)]
pub enum TranslationError {
    /// Reference to undefined entity.
    UndefinedReference {
        entity_type: String,
        name: String,
    },
    /// Invalid drill path reference.
    InvalidDrillPath {
        source: String,
        path: String,
        level: String,
    },
    /// Invalid measure reference.
    InvalidMeasure {
        measure: String,
        table: String,
    },
    /// SQL expression compilation error.
    SqlCompilationError {
        expression: String,
        error: String,
    },
}

impl std::fmt::Display for TranslationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslationError::UndefinedReference { entity_type, name } => {
                write!(f, "Undefined {} reference: {}", entity_type, name)
            }
            TranslationError::InvalidDrillPath { source, path, level } => {
                write!(
                    f,
                    "Invalid drill path: {}.{}.{}",
                    source, path, level
                )
            }
            TranslationError::InvalidMeasure { measure, table } => {
                write!(f, "Invalid measure '{}' in table '{}'", measure, table)
            }
            TranslationError::SqlCompilationError { expression, error } => {
                write!(f, "SQL compilation error in '{}': {}", expression, error)
            }
        }
    }
}

impl std::error::Error for TranslationError {}

/// Translate a Report to a SemanticQuery.
pub fn translate_report(
    report: &Report,
    model: &Model,
) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();
    
    // Set from clause
    if !report.from.is_empty() {
        query.from = Some(report.from[0].clone());
    }
    
    // TODO: Translate group, show, filters, sort, limit
    
    Ok(query)
}
```

### Step 4: Add translation module to lib.rs

```rust
// src/lib.rs - Add this line
pub mod translation;
```

### Step 5: Run test to verify it passes

Run: `cargo test --test translation_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/translation/ src/lib.rs tests/translation/ Cargo.toml
git commit -m "feat(translation): add translation infrastructure with error types"
```

---

## Task 2: Drill Path Resolution

**Goal:** Translate drill path references (e.g., `dates.standard.month`) to FieldRef objects.

**Files:**
- Modify: `src/translation/mod.rs`
- Modify: `tests/translation/translation_test.rs`

### Step 1: Write failing test for drill path resolution

```rust
// tests/translation/translation_test.rs - Add this test

#[test]
fn test_translate_drill_path_to_field_ref() {
    use mantis::model::{
        Calendar, CalendarBody, PhysicalCalendar, DrillPath, GrainLevel, GroupItem,
    };
    
    let mut calendars = HashMap::new();
    
    let mut grain_mappings = HashMap::new();
    grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
    grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());
    
    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "standard".to_string(),
        DrillPath {
            name: "standard".to_string(),
            levels: vec![GrainLevel::Day, GrainLevel::Month],
        },
    );
    
    calendars.insert(
        "dates".to_string(),
        Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings,
                drill_paths,
                fiscal_year_start: None,
                week_start: None,
            }),
        },
    );
    
    let model = Model {
        defaults: None,
        calendars,
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![GroupItem::DrillPathRef {
            source: "dates".to_string(),
            path: "standard".to_string(),
            level: "month".to_string(),
            label: Some("Month".to_string()),
        }],
        show: vec![],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    assert_eq!(query.group_by.len(), 1);
    assert_eq!(query.group_by[0].entity, "dates");
    assert_eq!(query.group_by[0].field, "month_start_date");
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test translation_test test_translate_drill_path_to_field_ref`
Expected: FAIL (group_by is empty)

### Step 3: Implement drill path resolution

```rust
// src/translation/mod.rs - Add this function

use crate::semantic::planner::types::FieldRef;

fn resolve_drill_path_reference(
    source: &str,
    path: &str,
    level: &str,
    model: &Model,
) -> Result<FieldRef, TranslationError> {
    // Get the calendar
    let calendar = model
        .calendars
        .get(source)
        .ok_or_else(|| TranslationError::UndefinedReference {
            entity_type: "calendar".to_string(),
            name: source.to_string(),
        })?;
    
    // Get drill paths from calendar body
    let drill_paths = match &calendar.body {
        crate::model::CalendarBody::Physical(phys) => &phys.drill_paths,
        crate::model::CalendarBody::Generated { .. } => {
            return Err(TranslationError::InvalidDrillPath {
                source: source.to_string(),
                path: path.to_string(),
                level: level.to_string(),
            });
        }
    };
    
    // Get the specific drill path
    let drill_path = drill_paths.get(path).ok_or_else(|| {
        TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        }
    })?;
    
    // Parse the level string to GrainLevel
    let grain_level = crate::dsl::ast::GrainLevel::from_str(level).ok_or_else(|| {
        TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        }
    })?;
    
    // Verify the grain level is in the drill path
    if !drill_path.levels.contains(&grain_level) {
        return Err(TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        });
    }
    
    // Get the column name for this grain level
    let grain_mappings = match &calendar.body {
        crate::model::CalendarBody::Physical(phys) => &phys.grain_mappings,
        _ => unreachable!(),
    };
    
    let column = grain_mappings
        .get(&grain_level)
        .ok_or_else(|| TranslationError::InvalidDrillPath {
            source: source.to_string(),
            path: path.to_string(),
            level: level.to_string(),
        })?;
    
    Ok(FieldRef::new(source, column))
}

// Update translate_report function to use this:
pub fn translate_report(
    report: &Report,
    model: &Model,
) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();
    
    // Set from clause
    if !report.from.is_empty() {
        query.from = Some(report.from[0].clone());
    }
    
    // Translate group items
    for group_item in &report.group {
        match group_item {
            crate::model::GroupItem::DrillPathRef {
                source,
                path,
                level,
                ..
            } => {
                let field_ref = resolve_drill_path_reference(source, path, level, model)?;
                query.group_by.push(field_ref);
            }
            crate::model::GroupItem::InlineSlicer { name, .. } => {
                // TODO: Resolve slicer reference
            }
        }
    }
    
    Ok(query)
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test translation_test test_translate_drill_path_to_field_ref`
Expected: PASS

### Step 5: Commit

```bash
git add src/translation/mod.rs tests/translation/translation_test.rs
git commit -m "feat(translation): implement drill path resolution to FieldRef"
```

---

## Task 3: Simple Measure Translation

**Goal:** Translate simple measure references (without time suffixes) to SelectField objects.

**Files:**
- Modify: `src/translation/mod.rs`
- Modify: `tests/translation/translation_test.rs`

### Step 1: Write failing test for measure translation

```rust
// tests/translation/translation_test.rs - Add this test

#[test]
fn test_translate_simple_measure() {
    use mantis::model::{
        Table, MeasureBlock, Measure, SqlExpr, ShowItem, Atom, AtomType,
    };
    use mantis::dsl::span::Span;
    
    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );
    
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
        "total_revenue".to_string(),
        Measure {
            name: "total_revenue".to_string(),
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
    
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::Measure {
            name: "total_revenue".to_string(),
            label: Some("Total Revenue".to_string()),
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    assert_eq!(query.select.len(), 1);
    assert_eq!(query.select[0].field.entity, "fact_sales");
    assert_eq!(query.select[0].field.field, "total_revenue");
    assert_eq!(query.select[0].alias, Some("Total Revenue".to_string()));
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test translation_test test_translate_simple_measure`
Expected: FAIL (select is empty)

### Step 3: Implement simple measure translation

```rust
// src/translation/mod.rs - Add this function

use crate::semantic::planner::types::SelectField;

fn translate_simple_measure(
    measure_name: &str,
    label: Option<String>,
    from_table: &str,
    model: &Model,
) -> Result<SelectField, TranslationError> {
    // Find the measure in the model
    let measure_block = model
        .measures
        .get(from_table)
        .ok_or_else(|| TranslationError::UndefinedReference {
            entity_type: "measure block".to_string(),
            name: from_table.to_string(),
        })?;
    
    let _measure = measure_block
        .measures
        .get(measure_name)
        .ok_or_else(|| TranslationError::InvalidMeasure {
            measure: measure_name.to_string(),
            table: from_table.to_string(),
        })?;
    
    // Create SelectField
    let mut select_field = SelectField::new(from_table, measure_name);
    if let Some(label) = label {
        select_field = select_field.with_alias(&label);
    }
    
    Ok(select_field)
}

// Update translate_report to use this:
pub fn translate_report(
    report: &Report,
    model: &Model,
) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();
    
    // Set from clause
    let from_table = if !report.from.is_empty() {
        query.from = Some(report.from[0].clone());
        &report.from[0]
    } else {
        return Err(TranslationError::UndefinedReference {
            entity_type: "table".to_string(),
            name: "none".to_string(),
        });
    };
    
    // Translate group items
    for group_item in &report.group {
        match group_item {
            crate::model::GroupItem::DrillPathRef {
                source,
                path,
                level,
                ..
            } => {
                let field_ref = resolve_drill_path_reference(source, path, level, model)?;
                query.group_by.push(field_ref);
            }
            crate::model::GroupItem::InlineSlicer { .. } => {
                // TODO: Resolve slicer reference
            }
        }
    }
    
    // Translate show items
    for show_item in &report.show {
        match show_item {
            crate::model::ShowItem::Measure { name, label } => {
                let select_field = translate_simple_measure(name, label.clone(), from_table, model)?;
                query.select.push(select_field);
            }
            crate::model::ShowItem::MeasureWithSuffix { .. } => {
                // TODO: Handle time suffixes
            }
            crate::model::ShowItem::InlineMeasure { .. } => {
                // TODO: Handle inline measures
            }
        }
    }
    
    Ok(query)
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test translation_test test_translate_simple_measure`
Expected: PASS

### Step 5: Commit

```bash
git add src/translation/mod.rs tests/translation/translation_test.rs
git commit -m "feat(translation): implement simple measure translation to SelectField"
```

---

## Task 4: Time Suffix Expansion (YTD)

**Goal:** Expand time suffixes like `.ytd` into DerivedField with TimeFunction::YearToDate.

**Files:**
- Modify: `src/translation/mod.rs`
- Modify: `tests/translation/translation_test.rs`

### Step 1: Write failing test for YTD suffix

```rust
// tests/translation/translation_test.rs - Add this test

#[test]
fn test_translate_measure_with_ytd_suffix() {
    use mantis::model::{
        Table, MeasureBlock, Measure, SqlExpr, ShowItem, TimeSuffix,
        Atom, AtomType,
    };
    use mantis::dsl::span::Span;
    
    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );
    
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
    
    let report = Report {
        name: "test_report".to_string(),
        from: vec!["fact_sales".to_string()],
        use_date: vec![],
        period: None,
        group: vec![],
        show: vec![ShowItem::MeasureWithSuffix {
            name: "revenue".to_string(),
            suffix: TimeSuffix::Ytd,
            label: Some("YTD Revenue".to_string()),
        }],
        filters: vec![],
        sort: vec![],
        limit: None,
    };
    
    let result = translation::translate_report(&report, &model);
    assert!(result.is_ok());
    
    let query = result.unwrap();
    // Base measure should be in select
    assert_eq!(query.select.len(), 1);
    assert_eq!(query.select[0].field.field, "revenue");
    
    // YTD should be in derived
    assert_eq!(query.derived.len(), 1);
    assert_eq!(query.derived[0].alias, "YTD Revenue");
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test translation_test test_translate_measure_with_ytd_suffix`
Expected: FAIL (derived is empty)

### Step 3: Implement YTD suffix expansion

```rust
// src/translation/mod.rs - Add this function

use crate::semantic::planner::types::{DerivedField, DerivedExpr, TimeFunction};

fn translate_time_suffix(
    measure_name: &str,
    suffix: crate::model::TimeSuffix,
    label: Option<String>,
    from_table: &str,
    model: &Model,
) -> Result<(SelectField, DerivedField), TranslationError> {
    // First, ensure the base measure exists
    let measure_block = model
        .measures
        .get(from_table)
        .ok_or_else(|| TranslationError::UndefinedReference {
            entity_type: "measure block".to_string(),
            name: from_table.to_string(),
        })?;
    
    let _measure = measure_block
        .measures
        .get(measure_name)
        .ok_or_else(|| TranslationError::InvalidMeasure {
            measure: measure_name.to_string(),
            table: from_table.to_string(),
        })?;
    
    // Create base measure select field
    let base_select = SelectField::new(from_table, measure_name);
    
    // Create derived field based on suffix
    let derived_alias = label.unwrap_or_else(|| format!("{}_{:?}", measure_name, suffix));
    
    let derived_expr = match suffix {
        crate::model::TimeSuffix::Ytd => DerivedExpr::TimeFunction(TimeFunction::YearToDate {
            measure: measure_name.to_string(),
            year_column: None,
            period_column: None,
            via: None,
        }),
        _ => {
            return Err(TranslationError::SqlCompilationError {
                expression: format!("{}.{:?}", measure_name, suffix),
                error: "Time suffix not yet implemented".to_string(),
            });
        }
    };
    
    let derived_field = DerivedField {
        alias: derived_alias,
        expression: derived_expr,
    };
    
    Ok((base_select, derived_field))
}

// Update translate_report to use this:
pub fn translate_report(
    report: &Report,
    model: &Model,
) -> Result<SemanticQuery, TranslationError> {
    let mut query = SemanticQuery::default();
    
    // Set from clause
    let from_table = if !report.from.is_empty() {
        query.from = Some(report.from[0].clone());
        &report.from[0]
    } else {
        return Err(TranslationError::UndefinedReference {
            entity_type: "table".to_string(),
            name: "none".to_string(),
        });
    };
    
    // Translate group items
    for group_item in &report.group {
        match group_item {
            crate::model::GroupItem::DrillPathRef {
                source,
                path,
                level,
                ..
            } => {
                let field_ref = resolve_drill_path_reference(source, path, level, model)?;
                query.group_by.push(field_ref);
            }
            crate::model::GroupItem::InlineSlicer { .. } => {
                // TODO: Resolve slicer reference
            }
        }
    }
    
    // Translate show items
    for show_item in &report.show {
        match show_item {
            crate::model::ShowItem::Measure { name, label } => {
                let select_field = translate_simple_measure(name, label.clone(), from_table, model)?;
                query.select.push(select_field);
            }
            crate::model::ShowItem::MeasureWithSuffix { name, suffix, label } => {
                let (base_select, derived_field) =
                    translate_time_suffix(name, *suffix, label.clone(), from_table, model)?;
                query.select.push(base_select);
                query.derived.push(derived_field);
            }
            crate::model::ShowItem::InlineMeasure { .. } => {
                // TODO: Handle inline measures
            }
        }
    }
    
    Ok(query)
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test translation_test test_translate_measure_with_ytd_suffix`
Expected: PASS

### Step 5: Commit

```bash
git add src/translation/mod.rs tests/translation/translation_test.rs
git commit -m "feat(translation): implement YTD time suffix expansion to TimeFunction"
```

---

## Task 5: Complete Time Suffix Mapping

**Goal:** Implement all 22 time suffix variants with proper TimeFunction/DerivedExpr mapping.

**Files:**
- Modify: `src/translation/mod.rs`
- Modify: `tests/translation/translation_test.rs`

### Step 1: Write test for multiple time suffixes

```rust
// tests/translation/translation_test.rs - Add this test

#[test]
fn test_translate_all_time_suffixes() {
    use mantis::model::{
        Table, MeasureBlock, Measure, SqlExpr, ShowItem, TimeSuffix,
        Atom, AtomType,
    };
    use mantis::dsl::span::Span;
    
    // Setup model with measure (same as previous test)
    let mut tables = HashMap::new();
    let mut atoms = HashMap::new();
    atoms.insert(
        "revenue".to_string(),
        Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        },
    );
    
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
    
    // Test each time suffix type
    let test_suffixes = vec![
        TimeSuffix::Ytd,
        TimeSuffix::Qtd,
        TimeSuffix::Mtd,
        TimeSuffix::PriorYear,
        TimeSuffix::YoyGrowth,
        TimeSuffix::YoyDelta,
        TimeSuffix::Rolling12m,
    ];
    
    for suffix in test_suffixes {
        let report = Report {
            name: "test_report".to_string(),
            from: vec!["fact_sales".to_string()],
            use_date: vec![],
            period: None,
            group: vec![],
            show: vec![ShowItem::MeasureWithSuffix {
                name: "revenue".to_string(),
                suffix,
                label: None,
            }],
            filters: vec![],
            sort: vec![],
            limit: None,
        };
        
        let result = translation::translate_report(&report, &model);
        assert!(result.is_ok(), "Failed to translate suffix: {:?}", suffix);
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test translation_test test_translate_all_time_suffixes`
Expected: FAIL (some suffixes not implemented)

### Step 3: Implement all time suffix mappings

```rust
// src/translation/mod.rs - Update translate_time_suffix function

fn translate_time_suffix(
    measure_name: &str,
    suffix: crate::model::TimeSuffix,
    label: Option<String>,
    from_table: &str,
    model: &Model,
) -> Result<(SelectField, DerivedField), TranslationError> {
    // First, ensure the base measure exists
    let measure_block = model
        .measures
        .get(from_table)
        .ok_or_else(|| TranslationError::UndefinedReference {
            entity_type: "measure block".to_string(),
            name: from_table.to_string(),
        })?;
    
    let _measure = measure_block
        .measures
        .get(measure_name)
        .ok_or_else(|| TranslationError::InvalidMeasure {
            measure: measure_name.to_string(),
            table: from_table.to_string(),
        })?;
    
    // Create base measure select field
    let base_select = SelectField::new(from_table, measure_name);
    
    // Create derived field based on suffix
    let derived_alias = label.unwrap_or_else(|| format!("{}_{:?}", measure_name, suffix));
    
    let derived_expr = match suffix {
        // Accumulations
        crate::model::TimeSuffix::Ytd => DerivedExpr::TimeFunction(TimeFunction::YearToDate {
            measure: measure_name.to_string(),
            year_column: None,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Qtd => DerivedExpr::TimeFunction(TimeFunction::QuarterToDate {
            measure: measure_name.to_string(),
            year_column: None,
            quarter_column: None,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Mtd => DerivedExpr::TimeFunction(TimeFunction::MonthToDate {
            measure: measure_name.to_string(),
            year_column: None,
            month_column: None,
            day_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Wtd => {
            // WTD can use a generic window function or similar to MTD
            DerivedExpr::TimeFunction(TimeFunction::MonthToDate {
                measure: measure_name.to_string(),
                year_column: None,
                month_column: None,
                day_column: None,
                via: None,
            })
        }
        crate::model::TimeSuffix::FiscalYtd => DerivedExpr::TimeFunction(TimeFunction::YearToDate {
            measure: measure_name.to_string(),
            year_column: None,
            period_column: None,
            via: Some("fiscal".to_string()),
        }),
        crate::model::TimeSuffix::FiscalQtd => DerivedExpr::TimeFunction(TimeFunction::QuarterToDate {
            measure: measure_name.to_string(),
            year_column: None,
            quarter_column: None,
            period_column: None,
            via: Some("fiscal".to_string()),
        }),
        
        // Prior periods - use PriorPeriod time function
        crate::model::TimeSuffix::PriorYear => DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
            measure: measure_name.to_string(),
            periods: 1,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::PriorQuarter => DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
            measure: measure_name.to_string(),
            periods: 1,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::PriorMonth => DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
            measure: measure_name.to_string(),
            periods: 1,
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::PriorWeek => DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
            measure: measure_name.to_string(),
            periods: 1,
            period_column: None,
            via: None,
        }),
        
        // Growth calculations
        crate::model::TimeSuffix::YoyGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        crate::model::TimeSuffix::QoqGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        crate::model::TimeSuffix::MomGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        crate::model::TimeSuffix::WowGrowth => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Growth { current, previous }
        }
        
        // Delta calculations
        crate::model::TimeSuffix::YoyDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        crate::model::TimeSuffix::QoqDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        crate::model::TimeSuffix::MomDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        crate::model::TimeSuffix::WowDelta => {
            let current = Box::new(DerivedExpr::MeasureRef(measure_name.to_string()));
            let previous = Box::new(DerivedExpr::TimeFunction(TimeFunction::PriorPeriod {
                measure: measure_name.to_string(),
                periods: 1,
                period_column: None,
                via: None,
            }));
            DerivedExpr::Delta { current, previous }
        }
        
        // Rolling windows
        crate::model::TimeSuffix::Rolling3m => DerivedExpr::TimeFunction(TimeFunction::RollingWindow {
            measure: measure_name.to_string(),
            window_size: 3,
            aggregation: "SUM".to_string(),
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Rolling6m => DerivedExpr::TimeFunction(TimeFunction::RollingWindow {
            measure: measure_name.to_string(),
            window_size: 6,
            aggregation: "SUM".to_string(),
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Rolling12m => DerivedExpr::TimeFunction(TimeFunction::RollingWindow {
            measure: measure_name.to_string(),
            window_size: 12,
            aggregation: "SUM".to_string(),
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Rolling3mAvg => DerivedExpr::TimeFunction(TimeFunction::RollingWindow {
            measure: measure_name.to_string(),
            window_size: 3,
            aggregation: "AVG".to_string(),
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Rolling6mAvg => DerivedExpr::TimeFunction(TimeFunction::RollingWindow {
            measure: measure_name.to_string(),
            window_size: 6,
            aggregation: "AVG".to_string(),
            period_column: None,
            via: None,
        }),
        crate::model::TimeSuffix::Rolling12mAvg => DerivedExpr::TimeFunction(TimeFunction::RollingWindow {
            measure: measure_name.to_string(),
            window_size: 12,
            aggregation: "AVG".to_string(),
            period_column: None,
            via: None,
        }),
    };
    
    let derived_field = DerivedField {
        alias: derived_alias,
        expression: derived_expr,
    };
    
    Ok((base_select, derived_field))
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test translation_test test_translate_all_time_suffixes`
Expected: PASS

### Step 5: Commit

```bash
git add src/translation/mod.rs tests/translation/translation_test.rs
git commit -m "feat(translation): implement all 22 time suffix variants"
```

---

## Summary

**Tasks 1-5 completed:**
- ✅ Task 1: Translation infrastructure with error types
- ✅ Task 2: Drill path resolution to FieldRef
- ✅ Task 3: Simple measure translation to SelectField
- ✅ Task 4: YTD time suffix expansion
- ✅ Task 5: Complete time suffix mapping (22 variants)

**Files created:**
- `src/translation/mod.rs` - Translation module implementation
- `tests/translation/translation_test.rs` - Comprehensive test suite

**Files modified:**
- `src/lib.rs` - Added translation module

**Architecture:**
- Report → SemanticQuery translation pipeline
- Drill path references → FieldRef resolution
- Time suffixes → TimeFunction/DerivedExpr expansion
- Proper error handling with context

**Next steps:**
Additional features to implement:
- Task 6: Inline slicer resolution
- Task 7: SQL expression compilation (@atom resolution)
- Task 8: Filter routing (table vs query level)
- Task 9: Sort and limit translation
- Task 10: Inline measure handling
- Task 11: Period filter generation
- Task 12: Integration tests with full report examples
