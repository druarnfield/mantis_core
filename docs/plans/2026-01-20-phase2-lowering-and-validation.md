# Phase 2: Lowering and Validation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement lowering functions to translate DSL AST to semantic model types, and add validation infrastructure to detect circular dependencies, invalid references, and drill path issues.

**Architecture:** Implement `lower_*()` functions in `src/lowering/mod.rs` to translate each AST type to its corresponding model type. Add `src/validation/mod.rs` with validators that run after lowering to detect semantic errors before query compilation.

**Tech Stack:** Rust, existing chumsky parser (Phase 1), model types (Tasks 1-8), HashMap for efficient lookups, petgraph for circular dependency detection.

---

## Task 9: Lower Calendar Types

**Goal:** Implement `lower_calendar()` to translate AST Calendar to model Calendar.

**Files:**
- Modify: `src/lowering/mod.rs`
- Create: `tests/lowering/calendar_lowering_test.rs`

### Step 1: Write failing test for physical calendar lowering

```rust
// tests/lowering/calendar_lowering_test.rs
use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;
use mantis_core::model::{Calendar, CalendarBody, PhysicalCalendar};
use std::collections::HashMap;

#[test]
fn test_lower_physical_calendar() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Calendar(ast::Calendar {
                name: Spanned {
                    value: "dates".to_string(),
                    span: Span::default(),
                },
                body: ast::CalendarBody::Physical(ast::PhysicalCalendar {
                    source: Spanned {
                        value: "dbo.dim_date".to_string(),
                        span: Span::default(),
                    },
                    grain_mappings: vec![
                        (ast::GrainLevel::Day, Spanned {
                            value: "date_key".to_string(),
                            span: Span::default(),
                        }),
                        (ast::GrainLevel::Month, Spanned {
                            value: "month_start_date".to_string(),
                            span: Span::default(),
                        }),
                    ],
                    drill_paths: vec![],
                    fiscal_year_start: None,
                    week_start: None,
                }),
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.calendars.len(), 1);
    
    let calendar = model.calendars.get("dates").unwrap();
    assert_eq!(calendar.name, "dates");
    
    match &calendar.body {
        CalendarBody::Physical(phys) => {
            assert_eq!(phys.source, "dbo.dim_date");
            assert_eq!(phys.grain_mappings.len(), 2);
            assert_eq!(phys.grain_mappings.get(&mantis_core::model::GrainLevel::Day).unwrap(), "date_key");
        }
        _ => panic!("Expected Physical calendar"),
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test calendar_lowering_test`
Expected: FAIL with "Not implemented: Calendar"

### Step 3: Implement lower_calendar for Physical calendars

```rust
// src/lowering/mod.rs - Replace lower_calendar function

fn lower_calendar(calendar: ast::Calendar) -> Result<model::Calendar, LoweringError> {
    let name = calendar.name.value;
    
    let body = match calendar.body {
        ast::CalendarBody::Physical(phys) => {
            let source = phys.source.value;
            
            // Convert grain mappings
            let mut grain_mappings = std::collections::HashMap::new();
            for (grain_level, column) in phys.grain_mappings {
                grain_mappings.insert(grain_level, column.value);
            }
            
            // Convert drill paths
            let mut drill_paths = std::collections::HashMap::new();
            for drill_path in phys.drill_paths {
                let path_name = drill_path.name.value.clone();
                drill_paths.insert(
                    path_name.clone(),
                    model::calendar::DrillPath {
                        name: path_name,
                        levels: drill_path.levels.into_iter().map(|l| l.value).collect(),
                    },
                );
            }
            
            model::CalendarBody::Physical(model::calendar::PhysicalCalendar {
                source,
                grain_mappings,
                drill_paths,
                fiscal_year_start: phys.fiscal_year_start.map(|s| s.value),
                week_start: phys.week_start.map(|s| s.value),
            })
        }
        ast::CalendarBody::Generated(gen) => {
            model::CalendarBody::Generated {
                grain: gen.grain.value,
                from: gen.from.value,
                to: gen.to.value,
            }
        }
    };
    
    Ok(model::Calendar { name, body })
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test calendar_lowering_test`
Expected: PASS

### Step 5: Add test for Generated calendar

```rust
// tests/lowering/calendar_lowering_test.rs - Add this test

#[test]
fn test_lower_generated_calendar() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Calendar(ast::Calendar {
                name: Spanned {
                    value: "auto_dates".to_string(),
                    span: Span::default(),
                },
                body: ast::CalendarBody::Generated(ast::GeneratedCalendar {
                    grain: Spanned {
                        value: ast::GrainLevel::Day,
                        span: Span::default(),
                    },
                    from: Spanned {
                        value: "2020-01-01".to_string(),
                        span: Span::default(),
                    },
                    to: Spanned {
                        value: "2025-12-31".to_string(),
                        span: Span::default(),
                    },
                    fiscal_year_start: None,
                    drill_paths: vec![],
                    week_start: None,
                }),
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let calendar = model.calendars.get("auto_dates").unwrap();
    
    match &calendar.body {
        CalendarBody::Generated { grain, from, to } => {
            assert_eq!(*grain, mantis_core::model::GrainLevel::Day);
            assert_eq!(from, "2020-01-01");
            assert_eq!(to, "2025-12-31");
        }
        _ => panic!("Expected Generated calendar"),
    }
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test calendar_lowering_test`
Expected: PASS (2 tests)

### Step 7: Commit

```bash
git add src/lowering/mod.rs tests/lowering/calendar_lowering_test.rs Cargo.toml
git commit -m "feat(lowering): implement Calendar lowering for Physical and Generated variants"
```

---

## Task 10: Lower Dimension Types

**Goal:** Implement `lower_dimension()` to translate AST Dimension to model Dimension.

**Files:**
- Modify: `src/lowering/mod.rs`
- Create: `tests/lowering/dimension_lowering_test.rs`

### Step 1: Write failing test for dimension lowering

```rust
// tests/lowering/dimension_lowering_test.rs
use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;

#[test]
fn test_lower_dimension() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Dimension(ast::Dimension {
                name: Spanned {
                    value: "customers".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.dim_customers".to_string(),
                    span: Span::default(),
                },
                key: Spanned {
                    value: "customer_id".to_string(),
                    span: Span::default(),
                },
                attributes: vec![
                    ast::Attribute {
                        name: Spanned {
                            value: "customer_name".to_string(),
                            span: Span::default(),
                        },
                        data_type: Spanned {
                            value: ast::DataType::String,
                            span: Span::default(),
                        },
                    },
                    ast::Attribute {
                        name: Spanned {
                            value: "region".to_string(),
                            span: Span::default(),
                        },
                        data_type: Spanned {
                            value: ast::DataType::String,
                            span: Span::default(),
                        },
                    },
                ],
                drill_paths: vec![],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.dimensions.len(), 1);
    
    let dimension = model.dimensions.get("customers").unwrap();
    assert_eq!(dimension.name, "customers");
    assert_eq!(dimension.source, "dbo.dim_customers");
    assert_eq!(dimension.key, "customer_id");
    assert_eq!(dimension.attributes.len(), 2);
    assert!(dimension.attributes.contains_key("customer_name"));
    assert!(dimension.attributes.contains_key("region"));
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test dimension_lowering_test`
Expected: FAIL with "Not implemented: Dimension"

### Step 3: Implement lower_dimension

```rust
// src/lowering/mod.rs - Replace lower_dimension function

fn lower_dimension(dimension: ast::Dimension) -> Result<model::Dimension, LoweringError> {
    let name = dimension.name.value;
    let source = dimension.source.value;
    let key = dimension.key.value;
    
    // Convert attributes
    let mut attributes = std::collections::HashMap::new();
    for attr in dimension.attributes {
        let attr_name = attr.name.value.clone();
        attributes.insert(
            attr_name.clone(),
            model::dimension::Attribute {
                name: attr_name,
                data_type: attr.data_type.value,
            },
        );
    }
    
    // Convert drill paths
    let mut drill_paths = std::collections::HashMap::new();
    for drill_path in dimension.drill_paths {
        let path_name = drill_path.name.value.clone();
        drill_paths.insert(
            path_name.clone(),
            model::dimension::DimensionDrillPath {
                name: path_name,
                levels: drill_path.levels.into_iter().map(|l| l.value).collect(),
            },
        );
    }
    
    Ok(model::Dimension {
        name,
        source,
        key,
        attributes,
        drill_paths,
    })
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test dimension_lowering_test`
Expected: PASS

### Step 5: Add test for dimension with drill paths

```rust
// tests/lowering/dimension_lowering_test.rs - Add this test

#[test]
fn test_lower_dimension_with_drill_paths() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Dimension(ast::Dimension {
                name: Spanned {
                    value: "geography".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.dim_geo".to_string(),
                    span: Span::default(),
                },
                key: Spanned {
                    value: "geo_id".to_string(),
                    span: Span::default(),
                },
                attributes: vec![],
                drill_paths: vec![ast::DimensionDrillPath {
                    name: Spanned {
                        value: "geographic".to_string(),
                        span: Span::default(),
                    },
                    levels: vec![
                        Spanned {
                            value: "city".to_string(),
                            span: Span::default(),
                        },
                        Spanned {
                            value: "state".to_string(),
                            span: Span::default(),
                        },
                        Spanned {
                            value: "country".to_string(),
                            span: Span::default(),
                        },
                    ],
                }],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let dimension = model.dimensions.get("geography").unwrap();
    assert_eq!(dimension.drill_paths.len(), 1);
    
    let drill_path = dimension.drill_paths.get("geographic").unwrap();
    assert_eq!(drill_path.levels, vec!["city", "state", "country"]);
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test dimension_lowering_test`
Expected: PASS (2 tests)

### Step 7: Commit

```bash
git add src/lowering/mod.rs tests/lowering/dimension_lowering_test.rs Cargo.toml
git commit -m "feat(lowering): implement Dimension lowering with attributes and drill paths"
```

---

## Task 11: Lower Table Types

**Goal:** Implement `lower_table()` to translate AST Table to model Table with atoms, times, and slicers.

**Files:**
- Modify: `src/lowering/mod.rs`
- Create: `tests/lowering/table_lowering_test.rs`

### Step 1: Write failing test for table lowering

```rust
// tests/lowering/table_lowering_test.rs
use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;
use mantis_core::model::{Slicer, Table};

#[test]
fn test_lower_table_with_atoms() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Table(ast::Table {
                name: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.fact_sales".to_string(),
                    span: Span::default(),
                },
                atoms: vec![ast::Atom {
                    name: Spanned {
                        value: "revenue".to_string(),
                        span: Span::default(),
                    },
                    data_type: Spanned {
                        value: ast::AtomType::Decimal,
                        span: Span::default(),
                    },
                }],
                times: vec![],
                slicers: vec![],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.tables.len(), 1);
    
    let table = model.tables.get("fact_sales").unwrap();
    assert_eq!(table.name, "fact_sales");
    assert_eq!(table.source, "dbo.fact_sales");
    assert_eq!(table.atoms.len(), 1);
    
    let atom = table.atoms.get("revenue").unwrap();
    assert_eq!(atom.name, "revenue");
    assert_eq!(atom.data_type, mantis_core::model::AtomType::Decimal);
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test table_lowering_test`
Expected: FAIL with "Not implemented: Table"

### Step 3: Implement lower_table

```rust
// src/lowering/mod.rs - Replace lower_table function

fn lower_table(table: ast::Table) -> Result<model::Table, LoweringError> {
    let name = table.name.value;
    let source = table.source.value;
    
    // Convert atoms
    let mut atoms = std::collections::HashMap::new();
    for atom in table.atoms {
        let atom_name = atom.name.value.clone();
        atoms.insert(
            atom_name.clone(),
            model::table::Atom {
                name: atom_name,
                data_type: atom.data_type.value,
            },
        );
    }
    
    // Convert times
    let mut times = std::collections::HashMap::new();
    for time in table.times {
        let time_name = time.name.value.clone();
        times.insert(
            time_name.clone(),
            model::table::TimeBinding {
                name: time_name,
                calendar: time.calendar.value,
                grain: time.grain.value,
            },
        );
    }
    
    // Convert slicers
    let mut slicers = std::collections::HashMap::new();
    for slicer in table.slicers {
        let slicer_name = match &slicer {
            ast::Slicer::Inline { name, .. } => name.value.clone(),
            ast::Slicer::ForeignKey { name, .. } => name.value.clone(),
            ast::Slicer::Via { name, .. } => name.value.clone(),
            ast::Slicer::Calculated { name, .. } => name.value.clone(),
        };
        
        let model_slicer = match slicer {
            ast::Slicer::Inline { name, data_type } => {
                model::Slicer::Inline {
                    name: name.value,
                    data_type: data_type.value,
                }
            }
            ast::Slicer::ForeignKey { name, dimension, key } => {
                model::Slicer::ForeignKey {
                    name: name.value,
                    dimension: dimension.value,
                    key: key.value,
                }
            }
            ast::Slicer::Via { name, fk_slicer } => {
                model::Slicer::Via {
                    name: name.value,
                    fk_slicer: fk_slicer.value,
                }
            }
            ast::Slicer::Calculated { name, data_type, expr } => {
                model::Slicer::Calculated {
                    name: name.value,
                    data_type: data_type.value,
                    expr: model::table::SqlExpr {
                        sql: expr.value,
                        span: expr.span,
                    },
                }
            }
        };
        
        slicers.insert(slicer_name, model_slicer);
    }
    
    Ok(model::Table {
        name,
        source,
        atoms,
        times,
        slicers,
    })
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test table_lowering_test`
Expected: PASS

### Step 5: Add test for table with all components

```rust
// tests/lowering/table_lowering_test.rs - Add this test

#[test]
fn test_lower_table_with_all_components() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Table(ast::Table {
                name: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                source: Spanned {
                    value: "dbo.fact_sales".to_string(),
                    span: Span::default(),
                },
                atoms: vec![],
                times: vec![ast::TimeBinding {
                    name: Spanned {
                        value: "order_date_id".to_string(),
                        span: Span::default(),
                    },
                    calendar: Spanned {
                        value: "dates".to_string(),
                        span: Span::default(),
                    },
                    grain: Spanned {
                        value: ast::GrainLevel::Day,
                        span: Span::default(),
                    },
                }],
                slicers: vec![
                    ast::Slicer::ForeignKey {
                        name: Spanned {
                            value: "customer_id".to_string(),
                            span: Span::default(),
                        },
                        dimension: Spanned {
                            value: "customers".to_string(),
                            span: Span::default(),
                        },
                        key: Spanned {
                            value: "customer_id".to_string(),
                            span: Span::default(),
                        },
                    },
                ],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let table = model.tables.get("fact_sales").unwrap();
    
    assert_eq!(table.times.len(), 1);
    let time = table.times.get("order_date_id").unwrap();
    assert_eq!(time.calendar, "dates");
    
    assert_eq!(table.slicers.len(), 1);
    assert!(matches!(
        table.slicers.get("customer_id"),
        Some(Slicer::ForeignKey { .. })
    ));
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test table_lowering_test`
Expected: PASS (2 tests)

### Step 7: Commit

```bash
git add src/lowering/mod.rs tests/lowering/table_lowering_test.rs Cargo.toml
git commit -m "feat(lowering): implement Table lowering with atoms, times, and slicers"
```

---

## Task 12: Lower MeasureBlock Types

**Goal:** Implement `lower_measure_block()` to translate AST MeasureBlock to model MeasureBlock.

**Files:**
- Modify: `src/lowering/mod.rs`
- Create: `tests/lowering/measure_lowering_test.rs`

### Step 1: Write failing test for measure block lowering

```rust
// tests/lowering/measure_lowering_test.rs
use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;

#[test]
fn test_lower_measure_block() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::MeasureBlock(ast::MeasureBlock {
                table_name: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                measures: vec![
                    ast::Measure {
                        name: Spanned {
                            value: "total_revenue".to_string(),
                            span: Span::default(),
                        },
                        expr: Spanned {
                            value: "sum(@revenue)".to_string(),
                            span: Span::default(),
                        },
                        filter: None,
                        null_handling: None,
                    },
                ],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.measures.len(), 1);
    
    let measure_block = model.measures.get("fact_sales").unwrap();
    assert_eq!(measure_block.table_name, "fact_sales");
    assert_eq!(measure_block.measures.len(), 1);
    
    let measure = measure_block.measures.get("total_revenue").unwrap();
    assert_eq!(measure.name, "total_revenue");
    assert_eq!(measure.expr.sql, "sum(@revenue)");
    assert!(measure.filter.is_none());
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test measure_lowering_test`
Expected: FAIL with "Not implemented: MeasureBlock"

### Step 3: Implement lower_measure_block

```rust
// src/lowering/mod.rs - Replace lower_measure_block function

fn lower_measure_block(
    measure_block: ast::MeasureBlock,
) -> Result<model::MeasureBlock, LoweringError> {
    let table_name = measure_block.table_name.value;
    
    // Convert measures
    let mut measures = std::collections::HashMap::new();
    for measure in measure_block.measures {
        let measure_name = measure.name.value.clone();
        measures.insert(
            measure_name.clone(),
            model::measure::Measure {
                name: measure_name,
                expr: model::table::SqlExpr {
                    sql: measure.expr.value,
                    span: measure.expr.span,
                },
                filter: measure.filter.map(|f| model::table::SqlExpr {
                    sql: f.value,
                    span: f.span,
                }),
                null_handling: measure.null_handling.map(|nh| nh.value),
            },
        );
    }
    
    Ok(model::MeasureBlock {
        table_name,
        measures,
    })
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test measure_lowering_test`
Expected: PASS

### Step 5: Add test for measure with filter and null_handling

```rust
// tests/lowering/measure_lowering_test.rs - Add this test

#[test]
fn test_lower_measure_with_filter() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::MeasureBlock(ast::MeasureBlock {
                table_name: Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                },
                measures: vec![
                    ast::Measure {
                        name: Spanned {
                            value: "enterprise_revenue".to_string(),
                            span: Span::default(),
                        },
                        expr: Spanned {
                            value: "sum(@amount)".to_string(),
                            span: Span::default(),
                        },
                        filter: Some(Spanned {
                            value: "segment = 'Enterprise'".to_string(),
                            span: Span::default(),
                        }),
                        null_handling: Some(Spanned {
                            value: ast::NullHandling::ReturnZero,
                            span: Span::default(),
                        }),
                    },
                ],
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let measure_block = model.measures.get("fact_sales").unwrap();
    let measure = measure_block.measures.get("enterprise_revenue").unwrap();
    
    assert!(measure.filter.is_some());
    assert_eq!(measure.filter.as_ref().unwrap().sql, "segment = 'Enterprise'");
    assert_eq!(
        measure.null_handling,
        Some(mantis_core::model::NullHandling::ReturnZero)
    );
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test measure_lowering_test`
Expected: PASS (2 tests)

### Step 7: Commit

```bash
git add src/lowering/mod.rs tests/lowering/measure_lowering_test.rs Cargo.toml
git commit -m "feat(lowering): implement MeasureBlock lowering with filter and null_handling"
```

---

## Task 13: Lower Report Types

**Goal:** Implement `lower_report()` to translate AST Report to model Report.

**Files:**
- Modify: `src/lowering/mod.rs`
- Create: `tests/lowering/report_lowering_test.rs`

### Step 1: Write failing test for report lowering

```rust
// tests/lowering/report_lowering_test.rs
use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;
use mantis_core::model::{GroupItem, ShowItem, TimeSuffix};

#[test]
fn test_lower_report() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Report(ast::Report {
                name: Spanned {
                    value: "monthly_sales".to_string(),
                    span: Span::default(),
                },
                from: vec![Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                }],
                use_date: vec![Spanned {
                    value: "order_date_id".to_string(),
                    span: Span::default(),
                }],
                period: None,
                group: vec![ast::GroupItem::DrillPathRef {
                    source: Spanned {
                        value: "dates".to_string(),
                        span: Span::default(),
                    },
                    path: Spanned {
                        value: "standard".to_string(),
                        span: Span::default(),
                    },
                    level: Spanned {
                        value: "month".to_string(),
                        span: Span::default(),
                    },
                    label: None,
                }],
                show: vec![ast::ShowItem::Measure {
                    name: Spanned {
                        value: "total_revenue".to_string(),
                        span: Span::default(),
                    },
                    label: None,
                }],
                filters: vec![],
                sort: vec![],
                limit: None,
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert_eq!(model.reports.len(), 1);
    
    let report = model.reports.get("monthly_sales").unwrap();
    assert_eq!(report.name, "monthly_sales");
    assert_eq!(report.from, vec!["fact_sales"]);
    assert_eq!(report.use_date, vec!["order_date_id"]);
    assert_eq!(report.group.len(), 1);
    assert_eq!(report.show.len(), 1);
    
    match &report.group[0] {
        GroupItem::DrillPathRef { source, path, level, .. } => {
            assert_eq!(source, "dates");
            assert_eq!(path, "standard");
            assert_eq!(level, "month");
        }
        _ => panic!("Expected DrillPathRef"),
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test report_lowering_test`
Expected: FAIL with "Not implemented: Report"

### Step 3: Implement lower_report

```rust
// src/lowering/mod.rs - Replace lower_report function

fn lower_report(report: ast::Report) -> Result<model::Report, LoweringError> {
    let name = report.name.value;
    let from: Vec<String> = report.from.into_iter().map(|s| s.value).collect();
    let use_date: Vec<String> = report.use_date.into_iter().map(|s| s.value).collect();
    
    // Convert period (placeholder)
    let period = report.period.map(|_p| {
        // For now, just use a simple placeholder
        model::report::PeriodExpr::LastNMonths(12)
    });
    
    // Convert group items
    let mut group = Vec::new();
    for group_item in report.group {
        let model_group_item = match group_item {
            ast::GroupItem::DrillPathRef {
                source,
                path,
                level,
                label,
            } => model::GroupItem::DrillPathRef {
                source: source.value,
                path: path.value,
                level: level.value,
                label: label.map(|l| l.value),
            },
            ast::GroupItem::InlineSlicer { name, label } => model::GroupItem::InlineSlicer {
                name: name.value,
                label: label.map(|l| l.value),
            },
        };
        group.push(model_group_item);
    }
    
    // Convert show items
    let mut show = Vec::new();
    for show_item in report.show {
        let model_show_item = match show_item {
            ast::ShowItem::Measure { name, label } => model::ShowItem::Measure {
                name: name.value,
                label: label.map(|l| l.value),
            },
            ast::ShowItem::MeasureWithSuffix {
                name,
                suffix,
                label,
            } => model::ShowItem::MeasureWithSuffix {
                name: name.value,
                suffix: lower_time_suffix(suffix.value),
                label: label.map(|l| l.value),
            },
            ast::ShowItem::InlineMeasure { name, expr, label } => model::ShowItem::InlineMeasure {
                name: name.value,
                expr: model::table::SqlExpr {
                    sql: expr.value,
                    span: expr.span,
                },
                label: label.map(|l| l.value),
            },
        };
        show.push(model_show_item);
    }
    
    // Convert filters
    let filters: Vec<model::table::SqlExpr> = report
        .filters
        .into_iter()
        .map(|f| model::table::SqlExpr {
            sql: f.value,
            span: f.span,
        })
        .collect();
    
    // Convert sort
    let mut sort = Vec::new();
    for sort_item in report.sort {
        sort.push(model::report::SortItem {
            column: sort_item.column.value,
            direction: match sort_item.direction.value {
                ast::SortDirection::Asc => model::report::SortDirection::Asc,
                ast::SortDirection::Desc => model::report::SortDirection::Desc,
            },
        });
    }
    
    let limit = report.limit.map(|l| l.value);
    
    Ok(model::Report {
        name,
        from,
        use_date,
        period,
        group,
        show,
        filters,
        sort,
        limit,
    })
}

fn lower_time_suffix(suffix: ast::TimeSuffix) -> model::TimeSuffix {
    match suffix {
        ast::TimeSuffix::Ytd => model::TimeSuffix::Ytd,
        ast::TimeSuffix::Qtd => model::TimeSuffix::Qtd,
        ast::TimeSuffix::Mtd => model::TimeSuffix::Mtd,
        ast::TimeSuffix::Wtd => model::TimeSuffix::Wtd,
        ast::TimeSuffix::FiscalYtd => model::TimeSuffix::FiscalYtd,
        ast::TimeSuffix::FiscalQtd => model::TimeSuffix::FiscalQtd,
        ast::TimeSuffix::PriorYear => model::TimeSuffix::PriorYear,
        ast::TimeSuffix::PriorQuarter => model::TimeSuffix::PriorQuarter,
        ast::TimeSuffix::PriorMonth => model::TimeSuffix::PriorMonth,
        ast::TimeSuffix::PriorWeek => model::TimeSuffix::PriorWeek,
        ast::TimeSuffix::YoyGrowth => model::TimeSuffix::YoyGrowth,
        ast::TimeSuffix::QoqGrowth => model::TimeSuffix::QoqGrowth,
        ast::TimeSuffix::MomGrowth => model::TimeSuffix::MomGrowth,
        ast::TimeSuffix::WowGrowth => model::TimeSuffix::WowGrowth,
        ast::TimeSuffix::YoyDelta => model::TimeSuffix::YoyDelta,
        ast::TimeSuffix::QoqDelta => model::TimeSuffix::QoqDelta,
        ast::TimeSuffix::MomDelta => model::TimeSuffix::MomDelta,
        ast::TimeSuffix::WowDelta => model::TimeSuffix::WowDelta,
        ast::TimeSuffix::Rolling3m => model::TimeSuffix::Rolling3m,
        ast::TimeSuffix::Rolling6m => model::TimeSuffix::Rolling6m,
        ast::TimeSuffix::Rolling12m => model::TimeSuffix::Rolling12m,
        ast::TimeSuffix::Rolling3mAvg => model::TimeSuffix::Rolling3mAvg,
        ast::TimeSuffix::Rolling6mAvg => model::TimeSuffix::Rolling6mAvg,
        ast::TimeSuffix::Rolling12mAvg => model::TimeSuffix::Rolling12mAvg,
    }
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test report_lowering_test`
Expected: PASS

### Step 5: Add test for report with time suffixes

```rust
// tests/lowering/report_lowering_test.rs - Add this test

#[test]
fn test_lower_report_with_time_suffix() {
    let ast = ast::Model {
        defaults: None,
        items: vec![Spanned {
            value: ast::Item::Report(ast::Report {
                name: Spanned {
                    value: "ytd_report".to_string(),
                    span: Span::default(),
                },
                from: vec![Spanned {
                    value: "fact_sales".to_string(),
                    span: Span::default(),
                }],
                use_date: vec![Spanned {
                    value: "order_date_id".to_string(),
                    span: Span::default(),
                }],
                period: None,
                group: vec![],
                show: vec![ast::ShowItem::MeasureWithSuffix {
                    name: Spanned {
                        value: "revenue".to_string(),
                        span: Span::default(),
                    },
                    suffix: Spanned {
                        value: ast::TimeSuffix::Ytd,
                        span: Span::default(),
                    },
                    label: Some(Spanned {
                        value: "YTD Revenue".to_string(),
                        span: Span::default(),
                    }),
                }],
                filters: vec![],
                sort: vec![],
                limit: None,
            }),
            span: Span::default(),
        }],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    let report = model.reports.get("ytd_report").unwrap();
    
    match &report.show[0] {
        ShowItem::MeasureWithSuffix { name, suffix, label } => {
            assert_eq!(name, "revenue");
            assert_eq!(*suffix, TimeSuffix::Ytd);
            assert_eq!(label.as_deref(), Some("YTD Revenue"));
        }
        _ => panic!("Expected MeasureWithSuffix"),
    }
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test report_lowering_test`
Expected: PASS (2 tests)

### Step 7: Commit

```bash
git add src/lowering/mod.rs tests/lowering/report_lowering_test.rs Cargo.toml
git commit -m "feat(lowering): implement Report lowering with group, show, and time suffixes"
```

---

## Task 14: Lower Defaults Type

**Goal:** Implement `lower_defaults()` to translate AST Defaults to model Defaults.

**Files:**
- Modify: `src/lowering/mod.rs`
- Create: `tests/lowering/defaults_lowering_test.rs`

### Step 1: Write failing test for defaults lowering

```rust
// tests/lowering/defaults_lowering_test.rs
use mantis_core::dsl::ast;
use mantis_core::dsl::span::{Span, Spanned};
use mantis_core::lowering;
use mantis_core::model::NullHandling;

#[test]
fn test_lower_defaults() {
    let ast = ast::Model {
        defaults: Some(Spanned {
            value: ast::Defaults {
                settings: vec![
                    Spanned {
                        value: ast::DefaultSetting::Calendar(Spanned {
                            value: "dates".to_string(),
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::DefaultSetting::FiscalYearStart(Spanned {
                            value: ast::Month::April,
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::DefaultSetting::NullHandling(Spanned {
                            value: ast::NullHandling::ReturnZero,
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                    Spanned {
                        value: ast::DefaultSetting::DecimalPlaces(Spanned {
                            value: 3,
                            span: Span::default(),
                        }),
                        span: Span::default(),
                    },
                ],
            },
            span: Span::default(),
        }),
        items: vec![],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert!(model.defaults.is_some());
    
    let defaults = model.defaults.unwrap();
    assert_eq!(defaults.calendar, Some("dates".to_string()));
    assert_eq!(defaults.fiscal_year_start, Some(ast::Month::April));
    assert_eq!(defaults.null_handling, NullHandling::ReturnZero);
    assert_eq!(defaults.decimal_places, 3);
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test defaults_lowering_test`
Expected: FAIL (currently returns default values, not AST values)

### Step 3: Implement lower_defaults

```rust
// src/lowering/mod.rs - Replace lower_defaults function

fn lower_defaults(defaults: Spanned<ast::Defaults>) -> Result<model::Defaults, LoweringError> {
    let mut result = model::Defaults::default();
    
    for setting in defaults.value.settings {
        match setting.value {
            ast::DefaultSetting::Calendar(cal) => {
                result.calendar = Some(cal.value);
            }
            ast::DefaultSetting::FiscalYearStart(month) => {
                result.fiscal_year_start = Some(month.value);
            }
            ast::DefaultSetting::WeekStart(weekday) => {
                result.week_start = Some(weekday.value);
            }
            ast::DefaultSetting::NullHandling(nh) => {
                result.null_handling = nh.value;
            }
            ast::DefaultSetting::DecimalPlaces(dp) => {
                result.decimal_places = dp.value;
            }
        }
    }
    
    Ok(result)
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test defaults_lowering_test`
Expected: PASS

### Step 5: Add test for empty defaults

```rust
// tests/lowering/defaults_lowering_test.rs - Add this test

#[test]
fn test_lower_empty_defaults() {
    let ast = ast::Model {
        defaults: Some(Spanned {
            value: ast::Defaults {
                settings: vec![],
            },
            span: Span::default(),
        }),
        items: vec![],
    };

    let result = lowering::lower(ast);
    assert!(result.is_ok());

    let model = result.unwrap();
    assert!(model.defaults.is_some());
    
    let defaults = model.defaults.unwrap();
    // Should use Default trait values
    assert_eq!(defaults.calendar, None);
    assert_eq!(defaults.fiscal_year_start, None);
    assert_eq!(defaults.null_handling, NullHandling::NullOnZero);
    assert_eq!(defaults.decimal_places, 2);
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test defaults_lowering_test`
Expected: PASS (2 tests)

### Step 7: Commit

```bash
git add src/lowering/mod.rs tests/lowering/defaults_lowering_test.rs Cargo.toml
git commit -m "feat(lowering): implement Defaults lowering with setting overrides"
```

---

## Task 15: Validation Infrastructure

**Goal:** Create validation module with basic error types and validator trait.

**Files:**
- Create: `src/validation/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/validation/validation_test.rs`

### Step 1: Write failing test for validation

```rust
// tests/validation/validation_test.rs
use mantis_core::model;
use mantis_core::validation;
use std::collections::HashMap;

#[test]
fn test_validate_empty_model() {
    let model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };

    let result = validation::validate(&model);
    assert!(result.is_ok());
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test validation_test`
Expected: Compilation error - module doesn't exist

### Step 3: Create validation module

```rust
// src/validation/mod.rs
//! Validation of semantic models.

use crate::model::Model;

/// Validation error.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// Circular dependency detected.
    CircularDependency {
        entity_type: String,
        cycle: Vec<String>,
    },
    /// Reference to undefined entity.
    UndefinedReference {
        entity_type: String,
        entity_name: String,
        reference_type: String,
        reference_name: String,
    },
    /// Invalid drill path.
    InvalidDrillPath {
        entity_type: String,
        entity_name: String,
        drill_path_name: String,
        issue: String,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::CircularDependency { entity_type, cycle } => {
                write!(
                    f,
                    "Circular dependency in {}: {}",
                    entity_type,
                    cycle.join(" -> ")
                )
            }
            ValidationError::UndefinedReference {
                entity_type,
                entity_name,
                reference_type,
                reference_name,
            } => {
                write!(
                    f,
                    "{} '{}' references undefined {} '{}'",
                    entity_type, entity_name, reference_type, reference_name
                )
            }
            ValidationError::InvalidDrillPath {
                entity_type,
                entity_name,
                drill_path_name,
                issue,
            } => {
                write!(
                    f,
                    "{} '{}' has invalid drill path '{}': {}",
                    entity_type, entity_name, drill_path_name, issue
                )
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validate a semantic model.
pub fn validate(model: &Model) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    // Validate references
    validate_references(model, &mut errors);

    // Validate circular dependencies
    validate_circular_dependencies(model, &mut errors);

    // Validate drill paths
    validate_drill_paths(model, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn validate_references(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
}

fn validate_circular_dependencies(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
}

fn validate_drill_paths(_model: &Model, _errors: &mut Vec<ValidationError>) {
    // Placeholder - will implement in next task
}
```

### Step 4: Add validation module to lib.rs

```rust
// src/lib.rs - Add this line
pub mod validation;
```

### Step 5: Run test to verify it passes

Run: `cargo test --test validation_test`
Expected: PASS

### Step 6: Add test for validation error display

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_validation_error_display() {
    let error = validation::ValidationError::UndefinedReference {
        entity_type: "Table".to_string(),
        entity_name: "fact_sales".to_string(),
        reference_type: "calendar".to_string(),
        reference_name: "dates".to_string(),
    };

    let message = error.to_string();
    assert!(message.contains("fact_sales"));
    assert!(message.contains("dates"));
    assert!(message.contains("calendar"));
}
```

### Step 7: Run tests to verify both pass

Run: `cargo test --test validation_test`
Expected: PASS (2 tests)

### Step 8: Commit

```bash
git add src/validation/ src/lib.rs tests/validation/ Cargo.toml
git commit -m "feat(validation): add validation infrastructure with error types"
```

---

## Task 16: Circular Dependency Detection

**Goal:** Implement circular dependency detection for measure references.

**Files:**
- Modify: `src/validation/mod.rs`
- Modify: `tests/validation/validation_test.rs`

### Step 1: Write failing test for circular dependency detection

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_detect_circular_measure_dependency() {
    use mantis_core::model::{Measure, MeasureBlock, SqlExpr};
    use mantis_core::dsl::span::Span;
    
    let mut measures = HashMap::new();
    
    // Create circular dependency: a -> b -> a
    measures.insert(
        "a".to_string(),
        Measure {
            name: "a".to_string(),
            expr: SqlExpr {
                sql: "b + 1".to_string(),  // References 'b'
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    
    measures.insert(
        "b".to_string(),
        Measure {
            name: "b".to_string(),
            expr: SqlExpr {
                sql: "a * 2".to_string(),  // References 'a'
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    
    let measure_block = MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures,
    };
    
    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    model.measures.insert("fact_sales".to_string(), measure_block);
    
    let result = validation::validate(&model);
    assert!(result.is_err());
    
    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    assert!(matches!(
        &errors[0],
        validation::ValidationError::CircularDependency { .. }
    ));
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test validation_test test_detect_circular_measure_dependency`
Expected: FAIL (currently passes because validation is a placeholder)

### Step 3: Implement circular dependency detection

```rust
// src/validation/mod.rs - Replace validate_circular_dependencies function

fn validate_circular_dependencies(model: &Model, errors: &mut Vec<ValidationError>) {
    // Check measure dependencies
    for (table_name, measure_block) in &model.measures {
        let mut graph = std::collections::HashMap::new();
        
        // Build dependency graph
        for (measure_name, measure) in &measure_block.measures {
            let deps = extract_measure_references(&measure.expr.sql);
            graph.insert(measure_name.clone(), deps);
        }
        
        // Detect cycles
        for measure_name in measure_block.measures.keys() {
            let mut visited = std::collections::HashSet::new();
            let mut path = Vec::new();
            
            if let Some(cycle) = detect_cycle(measure_name, &graph, &mut visited, &mut path) {
                errors.push(ValidationError::CircularDependency {
                    entity_type: format!("Measure in table '{}'", table_name),
                    cycle,
                });
                break; // Report first cycle found
            }
        }
    }
}

/// Extract measure references from SQL expression.
fn extract_measure_references(sql: &str) -> Vec<String> {
    let mut refs = Vec::new();
    
    // Simple regex-like pattern matching for identifiers that are not @atoms
    // This is a simplified implementation - real implementation would use a parser
    let tokens: Vec<&str> = sql.split(|c: char| !c.is_alphanumeric() && c != '_').collect();
    
    for token in tokens {
        if !token.is_empty() && !token.starts_with('@') && !is_sql_keyword(token) {
            // Might be a measure reference
            refs.push(token.to_string());
        }
    }
    
    refs
}

fn is_sql_keyword(s: &str) -> bool {
    matches!(
        s.to_lowercase().as_str(),
        "sum" | "avg" | "count" | "min" | "max" | "case" | "when" | "then" | "else" | "end"
            | "and" | "or" | "not" | "in" | "like" | "between"
    )
}

fn detect_cycle(
    node: &str,
    graph: &std::collections::HashMap<String, Vec<String>>,
    visited: &mut std::collections::HashSet<String>,
    path: &mut Vec<String>,
) -> Option<Vec<String>> {
    if path.contains(&node.to_string()) {
        // Found cycle - return the cycle
        let cycle_start = path.iter().position(|n| n == node).unwrap();
        let mut cycle = path[cycle_start..].to_vec();
        cycle.push(node.to_string());
        return Some(cycle);
    }
    
    if visited.contains(node) {
        return None;
    }
    
    visited.insert(node.to_string());
    path.push(node.to_string());
    
    if let Some(deps) = graph.get(node) {
        for dep in deps {
            if let Some(cycle) = detect_cycle(dep, graph, visited, path) {
                return Some(cycle);
            }
        }
    }
    
    path.pop();
    None
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test validation_test test_detect_circular_measure_dependency`
Expected: PASS

### Step 5: Add test for no circular dependency

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_no_circular_dependency_linear_chain() {
    use mantis_core::model::{Measure, MeasureBlock, SqlExpr};
    use mantis_core::dsl::span::Span;
    
    let mut measures = HashMap::new();
    
    // Create linear chain: a -> b -> c (no cycle)
    measures.insert(
        "a".to_string(),
        Measure {
            name: "a".to_string(),
            expr: SqlExpr {
                sql: "b + 1".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    
    measures.insert(
        "b".to_string(),
        Measure {
            name: "b".to_string(),
            expr: SqlExpr {
                sql: "c * 2".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    
    measures.insert(
        "c".to_string(),
        Measure {
            name: "c".to_string(),
            expr: SqlExpr {
                sql: "sum(@revenue)".to_string(),  // Base measure
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        },
    );
    
    let measure_block = MeasureBlock {
        table_name: "fact_sales".to_string(),
        measures,
    };
    
    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    model.measures.insert("fact_sales".to_string(), measure_block);
    
    let result = validation::validate(&model);
    assert!(result.is_ok());
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test validation_test`
Expected: PASS (all tests)

### Step 7: Commit

```bash
git add src/validation/mod.rs tests/validation/validation_test.rs
git commit -m "feat(validation): implement circular dependency detection for measures"
```

---

## Task 17: Reference Validation

**Goal:** Validate that all references (calendar, dimension, table) exist.

**Files:**
- Modify: `src/validation/mod.rs`
- Modify: `tests/validation/validation_test.rs`

### Step 1: Write failing test for undefined calendar reference

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_detect_undefined_calendar_reference() {
    use mantis_core::model::{Table, TimeBinding, GrainLevel};
    
    let mut times = HashMap::new();
    times.insert(
        "order_date_id".to_string(),
        TimeBinding {
            name: "order_date_id".to_string(),
            calendar: "nonexistent_calendar".to_string(),  // Undefined!
            grain: GrainLevel::Day,
        },
    );
    
    let table = Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms: HashMap::new(),
        times,
        slicers: HashMap::new(),
    };
    
    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),  // Empty - no calendars defined
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    model.tables.insert("fact_sales".to_string(), table);
    
    let result = validation::validate(&model);
    assert!(result.is_err());
    
    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    
    match &errors[0] {
        validation::ValidationError::UndefinedReference {
            reference_name, ..
        } => {
            assert_eq!(reference_name, "nonexistent_calendar");
        }
        _ => panic!("Expected UndefinedReference error"),
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test validation_test test_detect_undefined_calendar_reference`
Expected: FAIL (currently passes because validation is a placeholder)

### Step 3: Implement reference validation

```rust
// src/validation/mod.rs - Replace validate_references function

fn validate_references(model: &Model, errors: &mut Vec<ValidationError>) {
    // Validate table time bindings reference existing calendars
    for (table_name, table) in &model.tables {
        for (time_name, time_binding) in &table.times {
            if !model.calendars.contains_key(&time_binding.calendar) {
                errors.push(ValidationError::UndefinedReference {
                    entity_type: "Table".to_string(),
                    entity_name: table_name.clone(),
                    reference_type: "calendar".to_string(),
                    reference_name: time_binding.calendar.clone(),
                });
            }
        }
        
        // Validate slicer foreign keys reference existing dimensions
        for (slicer_name, slicer) in &table.slicers {
            if let crate::model::Slicer::ForeignKey { dimension, .. } = slicer {
                if !model.dimensions.contains_key(dimension) {
                    errors.push(ValidationError::UndefinedReference {
                        entity_type: "Table".to_string(),
                        entity_name: format!("{}.{}", table_name, slicer_name),
                        reference_type: "dimension".to_string(),
                        reference_name: dimension.clone(),
                    });
                }
            }
        }
    }
    
    // Validate measure blocks reference existing tables
    for (table_name, _measure_block) in &model.measures {
        if !model.tables.contains_key(table_name) {
            errors.push(ValidationError::UndefinedReference {
                entity_type: "MeasureBlock".to_string(),
                entity_name: table_name.clone(),
                reference_type: "table".to_string(),
                reference_name: table_name.clone(),
            });
        }
    }
    
    // Validate reports reference existing tables
    for (report_name, report) in &model.reports {
        for table_ref in &report.from {
            if !model.tables.contains_key(table_ref) {
                errors.push(ValidationError::UndefinedReference {
                    entity_type: "Report".to_string(),
                    entity_name: report_name.clone(),
                    reference_type: "table".to_string(),
                    reference_name: table_ref.clone(),
                });
            }
        }
    }
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test validation_test test_detect_undefined_calendar_reference`
Expected: PASS

### Step 5: Add test for undefined dimension reference

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_detect_undefined_dimension_reference() {
    use mantis_core::model::{Table, Slicer};
    
    let mut slicers = HashMap::new();
    slicers.insert(
        "customer_id".to_string(),
        Slicer::ForeignKey {
            name: "customer_id".to_string(),
            dimension: "nonexistent_dimension".to_string(),  // Undefined!
            key: "customer_id".to_string(),
        },
    );
    
    let table = Table {
        name: "fact_sales".to_string(),
        source: "dbo.fact_sales".to_string(),
        atoms: HashMap::new(),
        times: HashMap::new(),
        slicers,
    };
    
    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),  // Empty - no dimensions defined
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    model.tables.insert("fact_sales".to_string(), table);
    
    let result = validation::validate(&model);
    assert!(result.is_err());
    
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| matches!(
        e,
        validation::ValidationError::UndefinedReference {
            reference_name,
            ..
        } if reference_name == "nonexistent_dimension"
    )));
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test validation_test`
Expected: PASS (all tests)

### Step 7: Commit

```bash
git add src/validation/mod.rs tests/validation/validation_test.rs
git commit -m "feat(validation): validate calendar, dimension, and table references"
```

---

## Task 18: Drill Path Validation

**Goal:** Validate that drill paths reference existing attributes/grains.

**Files:**
- Modify: `src/validation/mod.rs`
- Modify: `tests/validation/validation_test.rs`

### Step 1: Write failing test for invalid drill path

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_detect_invalid_dimension_drill_path() {
    use mantis_core::model::{Dimension, Attribute, DimensionDrillPath, DataType};
    
    let mut attributes = HashMap::new();
    attributes.insert(
        "city".to_string(),
        Attribute {
            name: "city".to_string(),
            data_type: DataType::String,
        },
    );
    attributes.insert(
        "state".to_string(),
        Attribute {
            name: "state".to_string(),
            data_type: DataType::String,
        },
    );
    
    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "geographic".to_string(),
        DimensionDrillPath {
            name: "geographic".to_string(),
            levels: vec![
                "city".to_string(),
                "state".to_string(),
                "nonexistent_attribute".to_string(),  // Undefined!
            ],
        },
    );
    
    let dimension = Dimension {
        name: "geography".to_string(),
        source: "dbo.dim_geo".to_string(),
        key: "geo_id".to_string(),
        attributes,
        drill_paths,
    };
    
    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    model.dimensions.insert("geography".to_string(), dimension);
    
    let result = validation::validate(&model);
    assert!(result.is_err());
    
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| matches!(
        e,
        validation::ValidationError::InvalidDrillPath { .. }
    )));
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test validation_test test_detect_invalid_dimension_drill_path`
Expected: FAIL (currently passes because validation is a placeholder)

### Step 3: Implement drill path validation

```rust
// src/validation/mod.rs - Replace validate_drill_paths function

fn validate_drill_paths(model: &Model, errors: &mut Vec<ValidationError>) {
    // Validate dimension drill paths reference existing attributes
    for (dim_name, dimension) in &model.dimensions {
        for (drill_path_name, drill_path) in &dimension.drill_paths {
            for level in &drill_path.levels {
                if !dimension.attributes.contains_key(level) {
                    errors.push(ValidationError::InvalidDrillPath {
                        entity_type: "Dimension".to_string(),
                        entity_name: dim_name.clone(),
                        drill_path_name: drill_path_name.clone(),
                        issue: format!("Attribute '{}' does not exist", level),
                    });
                }
            }
        }
    }
    
    // Validate calendar drill paths reference valid grain levels
    for (cal_name, calendar) in &model.calendars {
        let grain_mappings = match &calendar.body {
            crate::model::CalendarBody::Physical(phys) => &phys.grain_mappings,
            crate::model::CalendarBody::Generated { grain, .. } => {
                // Generated calendars auto-support their grain
                continue;
            }
        };
        
        let drill_paths = match &calendar.body {
            crate::model::CalendarBody::Physical(phys) => &phys.drill_paths,
            _ => continue,
        };
        
        for (drill_path_name, drill_path) in drill_paths {
            for level in &drill_path.levels {
                if !grain_mappings.contains_key(level) {
                    errors.push(ValidationError::InvalidDrillPath {
                        entity_type: "Calendar".to_string(),
                        entity_name: cal_name.clone(),
                        drill_path_name: drill_path_name.clone(),
                        issue: format!("Grain level {:?} is not mapped", level),
                    });
                }
            }
        }
    }
}
```

### Step 4: Run test to verify it passes

Run: `cargo test --test validation_test test_detect_invalid_dimension_drill_path`
Expected: PASS

### Step 5: Add test for valid drill path

```rust
// tests/validation/validation_test.rs - Add this test

#[test]
fn test_valid_dimension_drill_path() {
    use mantis_core::model::{Dimension, Attribute, DimensionDrillPath, DataType};
    
    let mut attributes = HashMap::new();
    attributes.insert(
        "city".to_string(),
        Attribute {
            name: "city".to_string(),
            data_type: DataType::String,
        },
    );
    attributes.insert(
        "state".to_string(),
        Attribute {
            name: "state".to_string(),
            data_type: DataType::String,
        },
    );
    attributes.insert(
        "country".to_string(),
        Attribute {
            name: "country".to_string(),
            data_type: DataType::String,
        },
    );
    
    let mut drill_paths = HashMap::new();
    drill_paths.insert(
        "geographic".to_string(),
        DimensionDrillPath {
            name: "geographic".to_string(),
            levels: vec![
                "city".to_string(),
                "state".to_string(),
                "country".to_string(),  // All valid!
            ],
        },
    );
    
    let dimension = Dimension {
        name: "geography".to_string(),
        source: "dbo.dim_geo".to_string(),
        key: "geo_id".to_string(),
        attributes,
        drill_paths,
    };
    
    let mut model = model::Model {
        defaults: None,
        calendars: HashMap::new(),
        dimensions: HashMap::new(),
        tables: HashMap::new(),
        measures: HashMap::new(),
        reports: HashMap::new(),
    };
    
    model.dimensions.insert("geography".to_string(), dimension);
    
    let result = validation::validate(&model);
    assert!(result.is_ok());
}
```

### Step 6: Run tests to verify both pass

Run: `cargo test --test validation_test`
Expected: PASS (all tests)

### Step 7: Commit

```bash
git add src/validation/mod.rs tests/validation/validation_test.rs
git commit -m "feat(validation): validate drill paths reference existing attributes and grain levels"
```

---

## Summary

**Tasks 9-18 completed:**
- ✅ Task 9: Lower Calendar types (Physical and Generated)
- ✅ Task 10: Lower Dimension types with drill paths
- ✅ Task 11: Lower Table types with atoms, times, slicers
- ✅ Task 12: Lower MeasureBlock types with filters
- ✅ Task 13: Lower Report types with time suffixes
- ✅ Task 14: Lower Defaults types
- ✅ Task 15: Validation infrastructure
- ✅ Task 16: Circular dependency detection
- ✅ Task 17: Reference validation
- ✅ Task 18: Drill path validation

**Files created:**
- `tests/lowering/calendar_lowering_test.rs`
- `tests/lowering/dimension_lowering_test.rs`
- `tests/lowering/table_lowering_test.rs`
- `tests/lowering/measure_lowering_test.rs`
- `tests/lowering/report_lowering_test.rs`
- `tests/lowering/defaults_lowering_test.rs`
- `src/validation/mod.rs`
- `tests/validation/validation_test.rs`

**Files modified:**
- `src/lowering/mod.rs` (implemented all lowering functions)
- `src/lib.rs` (added validation module)

**Architecture:**
- Complete lowering pipeline from AST to model
- Comprehensive validation with error types
- Circular dependency detection using graph traversal
- Reference validation for all entity types
- Drill path validation for dimensions and calendars

**Next steps:**
The model is now fully functional with lowering and validation. The next phase would involve:
- Tasks 19-20: Update ModelGraph and ColumnLineageGraph
- Tasks 21-24: Report to SemanticQuery translation
- Tasks 25-30: Integration tests and golden files
