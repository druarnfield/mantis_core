# Phase 2: Semantic Model Bridge - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build new DSL-first model types, lowering from DSL AST, validation, and integration with existing semantic infrastructure.

**Architecture:** New `src/model/` with types matching DSL concepts exactly. Lowering in `src/lowering/`. Validation detects circular dependencies, invalid references. Reports map to `SemanticQuery` via translation layer.

**Tech Stack:** Rust, chumsky parser (Phase 1 complete), existing semantic planner, regex for @atom detection.

---

## Task 1: Core Model Types Foundation

**Goal:** Create the foundational model types module with shared types.

**Files:**
- Create: `src/model/mod.rs`
- Create: `src/model/types.rs`

### Step 1: Write test for GrainLevel

```rust
// tests/model/types_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::types::GrainLevel;
    
    #[test]
    fn test_grain_level_is_fiscal() {
        assert!(GrainLevel::FiscalMonth.is_fiscal());
        assert!(GrainLevel::FiscalQuarter.is_fiscal());
        assert!(GrainLevel::FiscalYear.is_fiscal());
        assert!(!GrainLevel::Month.is_fiscal());
        assert!(!GrainLevel::Day.is_fiscal());
    }
    
    #[test]
    fn test_grain_level_from_str() {
        assert_eq!(GrainLevel::from_str("day").unwrap(), GrainLevel::Day);
        assert_eq!(GrainLevel::from_str("month").unwrap(), GrainLevel::Month);
        assert_eq!(GrainLevel::from_str("fiscal_year").unwrap(), GrainLevel::FiscalYear);
        assert!(GrainLevel::from_str("invalid").is_err());
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test types_test`
Expected: Compilation error - module doesn't exist

### Step 3: Create src/model/types.rs with minimal types

```rust
// src/model/types.rs
use std::fmt;

/// Calendar grain levels for time intelligence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrainLevel {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    FiscalMonth,
    FiscalQuarter,
    FiscalYear,
}

impl GrainLevel {
    pub fn is_fiscal(&self) -> bool {
        matches!(self, GrainLevel::FiscalMonth | GrainLevel::FiscalQuarter | GrainLevel::FiscalYear)
    }
    
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "minute" => Ok(GrainLevel::Minute),
            "hour" => Ok(GrainLevel::Hour),
            "day" => Ok(GrainLevel::Day),
            "week" => Ok(GrainLevel::Week),
            "month" => Ok(GrainLevel::Month),
            "quarter" => Ok(GrainLevel::Quarter),
            "year" => Ok(GrainLevel::Year),
            "fiscal_month" => Ok(GrainLevel::FiscalMonth),
            "fiscal_quarter" => Ok(GrainLevel::FiscalQuarter),
            "fiscal_year" => Ok(GrainLevel::FiscalYear),
            _ => Err(format!("Unknown grain level: {}", s)),
        }
    }
}

impl fmt::Display for GrainLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrainLevel::Minute => write!(f, "minute"),
            GrainLevel::Hour => write!(f, "hour"),
            GrainLevel::Day => write!(f, "day"),
            GrainLevel::Week => write!(f, "week"),
            GrainLevel::Month => write!(f, "month"),
            GrainLevel::Quarter => write!(f, "quarter"),
            GrainLevel::Year => write!(f, "year"),
            GrainLevel::FiscalMonth => write!(f, "fiscal_month"),
            GrainLevel::FiscalQuarter => write!(f, "fiscal_quarter"),
            GrainLevel::FiscalYear => write!(f, "fiscal_year"),
        }
    }
}

/// NULL handling mode for division operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullHandling {
    /// Return NULL if denominator is NULL or zero
    ReturnNull,
    /// Return zero if denominator is NULL or zero
    ReturnZero,
}

/// Data type for atoms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomType {
    Int,
    Decimal,
    Float,
}

/// General data type for attributes and slicers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    String,
    Int,
    Decimal,
    Float,
    Boolean,
    Date,
    Timestamp,
}
```

### Step 4: Create src/model/mod.rs

```rust
// src/model/mod.rs
//! DSL-first semantic model types.

pub mod types;

pub use types::{AtomType, DataType, GrainLevel, NullHandling};

use std::collections::HashMap;

/// The new DSL-first semantic model.
#[derive(Debug, Clone)]
pub struct Model {
    /// Model-wide defaults
    pub defaults: Option<Defaults>,
    
    /// Calendars (physical and generated)
    pub calendars: HashMap<String, Calendar>,
    
    /// Dimensions (optional rich dimensions with drill paths)
    pub dimensions: HashMap<String, Dimension>,
    
    /// Tables (universal: sources, facts, wide tables, CSVs)
    pub tables: HashMap<String, Table>,
    
    /// Measure blocks (one per table)
    pub measures: HashMap<String, MeasureBlock>,
    
    /// Reports
    pub reports: HashMap<String, Report>,
}

// Placeholder types - will implement in later tasks
#[derive(Debug, Clone)]
pub struct Defaults;

#[derive(Debug, Clone)]
pub struct Calendar;

#[derive(Debug, Clone)]
pub struct Dimension;

#[derive(Debug, Clone)]
pub struct Table;

#[derive(Debug, Clone)]
pub struct MeasureBlock;

#[derive(Debug, Clone)]
pub struct Report;
```

### Step 5: Add model module to lib.rs

```rust
// src/lib.rs
pub mod dsl;
pub mod model;  // ← Add this
pub mod semantic;
pub mod sql;
```

### Step 6: Run test to verify it passes

Run: `cargo test --test types_test`
Expected: PASS

### Step 7: Commit

```bash
git add src/model/ tests/model/ src/lib.rs
git commit -m "feat(model): add core model types foundation with GrainLevel"
```

---

## Task 2: Table Types with Atoms, Times, Slicers

**Goal:** Implement Table type with Atom, TimeBinding, and Slicer variants.

**Files:**
- Create: `src/model/table.rs`
- Modify: `src/model/mod.rs`
- Create: `tests/model/table_test.rs`

### Step 1: Write test for Table construction

```rust
// tests/model/table_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{Table, Atom, AtomType, TimeBinding, Slicer, GrainLevel};
    use std::collections::HashMap;
    
    #[test]
    fn test_table_with_atoms() {
        let mut atoms = HashMap::new();
        atoms.insert("revenue".to_string(), Atom {
            name: "revenue".to_string(),
            data_type: AtomType::Decimal,
        });
        
        let table = Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms,
            times: HashMap::new(),
            slicers: HashMap::new(),
        };
        
        assert_eq!(table.name, "fact_sales");
        assert_eq!(table.atoms.len(), 1);
        assert!(table.atoms.contains_key("revenue"));
    }
    
    #[test]
    fn test_table_with_time_binding() {
        let mut times = HashMap::new();
        times.insert("order_date_id".to_string(), TimeBinding {
            name: "order_date_id".to_string(),
            calendar: "dates".to_string(),
            grain: GrainLevel::Day,
        });
        
        let table = Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times,
            slicers: HashMap::new(),
        };
        
        assert_eq!(table.times.len(), 1);
        let time = table.times.get("order_date_id").unwrap();
        assert_eq!(time.calendar, "dates");
        assert_eq!(time.grain, GrainLevel::Day);
    }
    
    #[test]
    fn test_table_with_slicers() {
        let mut slicers = HashMap::new();
        slicers.insert("customer_id".to_string(), Slicer::ForeignKey {
            name: "customer_id".to_string(),
            dimension: "customers".to_string(),
            key: "customer_id".to_string(),
        });
        slicers.insert("region".to_string(), Slicer::Via {
            name: "region".to_string(),
            fk_slicer: "customer_id".to_string(),
        });
        
        let table = Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times: HashMap::new(),
            slicers,
        };
        
        assert_eq!(table.slicers.len(), 2);
        assert!(matches!(table.slicers.get("customer_id"), Some(Slicer::ForeignKey { .. })));
        assert!(matches!(table.slicers.get("region"), Some(Slicer::Via { .. })));
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test table_test`
Expected: Compilation error - types don't exist

### Step 3: Create src/model/table.rs

```rust
// src/model/table.rs
use crate::model::types::{AtomType, DataType, GrainLevel};
use crate::dsl::span::Span;
use std::collections::HashMap;

/// A table (universal: CSV, wide table, fact, etc.).
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    /// Source table/file
    pub source: String,
    /// Atoms (numeric columns for aggregation)
    pub atoms: HashMap<String, Atom>,
    /// Times (date columns bound to calendars)
    pub times: HashMap<String, TimeBinding>,
    /// Slicers (columns for slicing/grouping)
    pub slicers: HashMap<String, Slicer>,
}

/// An atom (numeric column for aggregation).
#[derive(Debug, Clone, PartialEq)]
pub struct Atom {
    pub name: String,
    pub data_type: AtomType,
}

/// A time binding (date column bound to a calendar).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeBinding {
    pub name: String,
    /// Calendar name
    pub calendar: String,
    /// Grain level
    pub grain: GrainLevel,
}

/// A slicer (dimension column).
#[derive(Debug, Clone, PartialEq)]
pub enum Slicer {
    /// Inline slicer (column in the table)
    Inline {
        name: String,
        data_type: DataType,
    },
    /// Foreign key to a dimension
    ForeignKey {
        name: String,
        dimension: String,
        key: String,
    },
    /// Via another slicer (inherit relationship)
    Via {
        name: String,
        fk_slicer: String,
    },
    /// Calculated slicer (SQL expression)
    Calculated {
        name: String,
        data_type: DataType,
        expr: SqlExpr,
    },
}

/// A SQL expression with span for error reporting.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlExpr {
    /// Raw SQL with @atom references preserved
    pub sql: String,
    /// Span for error reporting
    pub span: Span,
}
```

### Step 4: Update src/model/mod.rs

```rust
// src/model/mod.rs
pub mod types;
pub mod table;  // ← Add this

pub use types::{AtomType, DataType, GrainLevel, NullHandling};
pub use table::{Atom, Slicer, SqlExpr, Table, TimeBinding};  // ← Add this

// ... rest of Model struct unchanged, but update Table from placeholder:

#[derive(Debug, Clone)]
pub struct Model {
    pub defaults: Option<Defaults>,
    pub calendars: HashMap<String, Calendar>,
    pub dimensions: HashMap<String, Dimension>,
    pub tables: HashMap<String, table::Table>,  // ← Use real type
    pub measures: HashMap<String, MeasureBlock>,
    pub reports: HashMap<String, Report>,
}
```

### Step 5: Run test to verify it passes

Run: `cargo test --test table_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/model/table.rs src/model/mod.rs tests/model/table_test.rs
git commit -m "feat(model): add Table type with atoms, times, and slicers"
```

---

## Task 3: Calendar Types (Physical and Generated)

**Goal:** Implement Calendar types with drill paths.

**Files:**
- Create: `src/model/calendar.rs`
- Modify: `src/model/mod.rs`
- Create: `tests/model/calendar_test.rs`

### Step 1: Write test for Calendar types

```rust
// tests/model/calendar_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{Calendar, CalendarBody, PhysicalCalendar, DrillPath, GrainLevel};
    use std::collections::HashMap;
    
    #[test]
    fn test_physical_calendar() {
        let mut grain_mappings = HashMap::new();
        grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
        grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());
        grain_mappings.insert(GrainLevel::Year, "year_start_date".to_string());
        
        let mut drill_paths = HashMap::new();
        drill_paths.insert("standard".to_string(), DrillPath {
            name: "standard".to_string(),
            levels: vec![GrainLevel::Day, GrainLevel::Month, GrainLevel::Year],
        });
        
        let calendar = Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings,
                drill_paths,
                fiscal_year_start: None,
                week_start: None,
            }),
        };
        
        assert_eq!(calendar.name, "dates");
        
        // Test supports_grain
        assert!(calendar.supports_grain(GrainLevel::Day));
        assert!(calendar.supports_grain(GrainLevel::Month));
        assert!(!calendar.supports_grain(GrainLevel::Week)); // Not in mappings
    }
    
    #[test]
    fn test_grain_column_lookup() {
        let mut grain_mappings = HashMap::new();
        grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
        grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());
        
        let calendar = Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings,
                drill_paths: HashMap::new(),
                fiscal_year_start: None,
                week_start: None,
            }),
        };
        
        assert_eq!(calendar.grain_column(GrainLevel::Day).unwrap(), "date_key");
        assert_eq!(calendar.grain_column(GrainLevel::Month).unwrap(), "month_start_date");
        assert!(calendar.grain_column(GrainLevel::Week).is_err());
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test calendar_test`
Expected: Compilation error

### Step 3: Create src/model/calendar.rs

```rust
// src/model/calendar.rs
use crate::model::types::GrainLevel;
use crate::dsl::ast::{Month, Weekday};
use std::collections::HashMap;

/// A calendar (date reference for time intelligence).
#[derive(Debug, Clone, PartialEq)]
pub struct Calendar {
    pub name: String,
    pub body: CalendarBody,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CalendarBody {
    Physical(PhysicalCalendar),
    Generated(GeneratedCalendar),
}

/// Physical calendar referencing an existing date dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCalendar {
    /// Source table (e.g., "dbo.dim_date")
    pub source: String,
    /// Grain level mappings (day = date_key, month = month_start_date, etc.)
    pub grain_mappings: HashMap<GrainLevel, String>,
    /// Named drill paths
    pub drill_paths: HashMap<String, DrillPath>,
    /// Fiscal year start
    pub fiscal_year_start: Option<Month>,
    /// Week start
    pub week_start: Option<Weekday>,
}

/// Generated ephemeral calendar (CTE date spine).
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedCalendar {
    /// Base grain with + suffix (day+, month+)
    pub base_grain: GrainLevel,
    /// Include fiscal grains with this start month
    pub fiscal: Option<Month>,
    /// Date range
    pub range: CalendarRange,
    /// Named drill paths
    pub drill_paths: HashMap<String, DrillPath>,
    /// Week start
    pub week_start: Option<Weekday>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CalendarRange {
    Explicit { start: String, end: String },
    Infer { min: Option<String>, max: Option<String> },
}

/// A drill path defining a valid aggregation hierarchy.
#[derive(Debug, Clone, PartialEq)]
pub struct DrillPath {
    pub name: String,
    /// Ordered levels from fine to coarse
    pub levels: Vec<GrainLevel>,
}

impl Calendar {
    /// Get the column name for a grain level.
    pub fn grain_column(&self, grain: GrainLevel) -> Result<String, String> {
        match &self.body {
            CalendarBody::Physical(phys) => {
                phys.grain_mappings.get(&grain)
                    .cloned()
                    .ok_or_else(|| format!("Grain {} not mapped in calendar {}", grain, self.name))
            }
            CalendarBody::Generated(_gen) => {
                // Generated calendars auto-generate column names
                Ok(generated_grain_column_name(grain))
            }
        }
    }
    
    /// Check if calendar supports a grain level.
    pub fn supports_grain(&self, grain: GrainLevel) -> bool {
        match &self.body {
            CalendarBody::Physical(phys) => phys.grain_mappings.contains_key(&grain),
            CalendarBody::Generated(gen) => {
                // Generated calendars support base_grain and coarser
                grain_is_coarser_or_equal(grain, gen.base_grain) ||
                (gen.fiscal.is_some() && grain.is_fiscal())
            }
        }
    }
    
    /// Get all drill paths for this calendar.
    pub fn drill_paths(&self) -> &HashMap<String, DrillPath> {
        match &self.body {
            CalendarBody::Physical(phys) => &phys.drill_paths,
            CalendarBody::Generated(gen) => &gen.drill_paths,
        }
    }
}

fn generated_grain_column_name(grain: GrainLevel) -> String {
    match grain {
        GrainLevel::Minute => "minute".to_string(),
        GrainLevel::Hour => "hour".to_string(),
        GrainLevel::Day => "date".to_string(),
        GrainLevel::Week => "week_start_date".to_string(),
        GrainLevel::Month => "month_start_date".to_string(),
        GrainLevel::Quarter => "quarter_start_date".to_string(),
        GrainLevel::Year => "year_start_date".to_string(),
        GrainLevel::FiscalMonth => "fiscal_month_start_date".to_string(),
        GrainLevel::FiscalQuarter => "fiscal_quarter_start_date".to_string(),
        GrainLevel::FiscalYear => "fiscal_year_start_date".to_string(),
    }
}

fn grain_is_coarser_or_equal(grain: GrainLevel, base: GrainLevel) -> bool {
    // Simplified - just check if same or if grain is in typical hierarchy
    grain == base
}
```

### Step 4: Update src/model/mod.rs to export Calendar

```rust
// src/model/mod.rs
pub mod types;
pub mod table;
pub mod calendar;  // ← Add this

pub use calendar::{Calendar, CalendarBody, CalendarRange, DrillPath, GeneratedCalendar, PhysicalCalendar};  // ← Add this

// Update Model to use real Calendar type:
pub struct Model {
    pub defaults: Option<Defaults>,
    pub calendars: HashMap<String, calendar::Calendar>,  // ← Use real type
    pub dimensions: HashMap<String, Dimension>,
    pub tables: HashMap<String, table::Table>,
    pub measures: HashMap<String, MeasureBlock>,
    pub reports: HashMap<String, Report>,
}
```

### Step 5: Run test to verify it passes

Run: `cargo test --test calendar_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/model/calendar.rs src/model/mod.rs tests/model/calendar_test.rs
git commit -m "feat(model): add Calendar types with drill paths"
```

---

## Task 4: Measure Types with @atom Syntax

**Goal:** Implement MeasureBlock and Measure types that preserve @atom syntax.

**Files:**
- Create: `src/model/measure.rs`
- Modify: `src/model/mod.rs`
- Create: `tests/model/measure_test.rs`

### Step 1: Write test for Measure types

```rust
// tests/model/measure_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{MeasureBlock, Measure, SqlExpr, NullHandling};
    use mantis_core::dsl::span::Span;
    use std::collections::HashMap;
    
    #[test]
    fn test_measure_with_atom_syntax() {
        let expr = SqlExpr {
            sql: "sum(@revenue)".to_string(),
            span: Span::default(),
        };
        
        let measure = Measure {
            name: "total_revenue".to_string(),
            expr,
            filter: None,
            null_handling: None,
        };
        
        assert_eq!(measure.name, "total_revenue");
        assert!(measure.expr.sql.contains("@revenue"));
        assert!(measure.filter.is_none());
    }
    
    #[test]
    fn test_measure_block() {
        let mut measures = HashMap::new();
        
        measures.insert("revenue".to_string(), Measure {
            name: "revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@amount)".to_string(),
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        });
        
        measures.insert("margin".to_string(), Measure {
            name: "margin".to_string(),
            expr: SqlExpr {
                sql: "revenue - cost".to_string(),  // References other measures
                span: Span::default(),
            },
            filter: None,
            null_handling: None,
        });
        
        let measure_block = MeasureBlock {
            table_name: "fact_sales".to_string(),
            measures,
        };
        
        assert_eq!(measure_block.table_name, "fact_sales");
        assert_eq!(measure_block.measures.len(), 2);
        assert!(measure_block.measures.contains_key("revenue"));
        assert!(measure_block.measures.contains_key("margin"));
    }
    
    #[test]
    fn test_measure_with_filter() {
        let measure = Measure {
            name: "enterprise_revenue".to_string(),
            expr: SqlExpr {
                sql: "sum(@amount)".to_string(),
                span: Span::default(),
            },
            filter: Some(SqlExpr {
                sql: "segment = 'Enterprise'".to_string(),
                span: Span::default(),
            }),
            null_handling: Some(NullHandling::ReturnZero),
        };
        
        assert!(measure.filter.is_some());
        assert_eq!(measure.null_handling, Some(NullHandling::ReturnZero));
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test measure_test`
Expected: Compilation error

### Step 3: Create src/model/measure.rs

```rust
// src/model/measure.rs
use crate::model::table::SqlExpr;
use crate::model::types::NullHandling;
use std::collections::HashMap;

/// A measure block for a table.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasureBlock {
    pub table_name: String,
    pub measures: HashMap<String, Measure>,
}

/// A measure definition with @atom syntax preserved.
#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    pub name: String,
    /// SQL expression with @atom references preserved
    pub expr: SqlExpr,
    /// Optional filter condition
    pub filter: Option<SqlExpr>,
    /// Optional NULL handling override
    pub null_handling: Option<NullHandling>,
}
```

### Step 4: Update src/model/mod.rs

```rust
// src/model/mod.rs
pub mod types;
pub mod table;
pub mod calendar;
pub mod measure;  // ← Add this

pub use measure::{Measure, MeasureBlock};  // ← Add this

// Update Model:
pub struct Model {
    pub defaults: Option<Defaults>,
    pub calendars: HashMap<String, calendar::Calendar>,
    pub dimensions: HashMap<String, Dimension>,
    pub tables: HashMap<String, table::Table>,
    pub measures: HashMap<String, measure::MeasureBlock>,  // ← Use real type
    pub reports: HashMap<String, Report>,
}
```

### Step 5: Run test to verify it passes

Run: `cargo test --test measure_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/model/measure.rs src/model/mod.rs tests/model/measure_test.rs
git commit -m "feat(model): add Measure types with @atom syntax preservation"
```

---

## Task 5: Dimension Types

**Goal:** Implement Dimension type with drill paths.

**Files:**
- Create: `src/model/dimension.rs`
- Modify: `src/model/mod.rs`
- Create: `tests/model/dimension_test.rs`

### Step 1: Write test for Dimension

```rust
// tests/model/dimension_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{Dimension, Attribute, DrillPath, DataType, GrainLevel};
    use std::collections::HashMap;
    
    #[test]
    fn test_dimension_with_attributes() {
        let mut attributes = HashMap::new();
        attributes.insert("customer_name".to_string(), Attribute {
            name: "customer_name".to_string(),
            data_type: DataType::String,
        });
        attributes.insert("region".to_string(), Attribute {
            name: "region".to_string(),
            data_type: DataType::String,
        });
        
        let dimension = Dimension {
            name: "customers".to_string(),
            source: "dbo.dim_customers".to_string(),
            key: "customer_id".to_string(),
            attributes,
            drill_paths: HashMap::new(),
        };
        
        assert_eq!(dimension.name, "customers");
        assert_eq!(dimension.key, "customer_id");
        assert_eq!(dimension.attributes.len(), 2);
    }
    
    #[test]
    fn test_dimension_with_drill_path() {
        let mut drill_paths = HashMap::new();
        drill_paths.insert("geo".to_string(), DrillPath {
            name: "geo".to_string(),
            levels: vec![GrainLevel::Day, GrainLevel::Month], // Simplified for test
        });
        
        let dimension = Dimension {
            name: "customers".to_string(),
            source: "dbo.dim_customers".to_string(),
            key: "customer_id".to_string(),
            attributes: HashMap::new(),
            drill_paths,
        };
        
        assert_eq!(dimension.drill_paths.len(), 1);
        assert!(dimension.drill_paths.contains_key("geo"));
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test dimension_test`
Expected: Compilation error

### Step 3: Create src/model/dimension.rs

```rust
// src/model/dimension.rs
use crate::model::calendar::DrillPath;
use crate::model::types::DataType;
use std::collections::HashMap;

/// A dimension (optional - for rich dimensions with drill paths).
#[derive(Debug, Clone, PartialEq)]
pub struct Dimension {
    pub name: String,
    /// Source table
    pub source: String,
    /// Primary key column
    pub key: String,
    /// Attributes
    pub attributes: HashMap<String, Attribute>,
    /// Named drill paths
    pub drill_paths: HashMap<String, DrillPath>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
}
```

### Step 4: Update src/model/mod.rs

```rust
// src/model/mod.rs
pub mod types;
pub mod table;
pub mod calendar;
pub mod measure;
pub mod dimension;  // ← Add this

pub use dimension::{Attribute, Dimension};  // ← Add this

// Update Model:
pub struct Model {
    pub defaults: Option<Defaults>,
    pub calendars: HashMap<String, calendar::Calendar>,
    pub dimensions: HashMap<String, dimension::Dimension>,  // ← Use real type
    pub tables: HashMap<String, table::Table>,
    pub measures: HashMap<String, measure::MeasureBlock>,
    pub reports: HashMap<String, Report>,
}
```

### Step 5: Run test to verify it passes

Run: `cargo test --test dimension_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/model/dimension.rs src/model/mod.rs tests/model/dimension_test.rs
git commit -m "feat(model): add Dimension type with drill paths"
```

---

## Task 6: Report Types

**Goal:** Implement Report type with drill path refs and time suffixes.

**Files:**
- Create: `src/model/report.rs`
- Modify: `src/model/mod.rs`
- Create: `tests/model/report_test.rs`

### Step 1: Write test for Report

```rust
// tests/model/report_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{Report, GroupItem, ShowItem, TimeSuffix};
    
    #[test]
    fn test_report_with_drill_path_group() {
        let group = vec![
            GroupItem::DrillPathRef {
                source: "dates".to_string(),
                path: "standard".to_string(),
                level: "month".to_string(),
                label: Some("Month".to_string()),
            },
        ];
        
        let report = Report {
            name: "test".to_string(),
            from: vec!["fact_sales".to_string()],
            use_date: vec!["order_date_id".to_string()],
            period: None,
            group,
            show: vec![],
            filters: vec![],
            sort: vec![],
            limit: None,
        };
        
        assert_eq!(report.name, "test");
        assert_eq!(report.group.len(), 1);
    }
    
    #[test]
    fn test_report_with_time_suffix() {
        let show = vec![
            ShowItem::MeasureWithSuffix {
                name: "revenue".to_string(),
                suffix: TimeSuffix::Ytd,
                label: Some("YTD Revenue".to_string()),
            },
        ];
        
        let report = Report {
            name: "test".to_string(),
            from: vec!["fact_sales".to_string()],
            use_date: vec!["order_date_id".to_string()],
            period: None,
            group: vec![],
            show,
            filters: vec![],
            sort: vec![],
            limit: None,
        };
        
        assert_eq!(report.show.len(), 1);
        assert!(matches!(report.show[0], ShowItem::MeasureWithSuffix { .. }));
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test report_test`
Expected: Compilation error

### Step 3: Create src/model/report.rs

```rust
// src/model/report.rs
use crate::model::table::SqlExpr;

/// A report definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub name: String,
    /// Source tables (maps to SemanticQuery.from)
    pub from: Vec<String>,
    /// Time columns for period filtering (one per table)
    pub use_date: Vec<String>,
    /// Time period (compile-time evaluated to date range filter)
    pub period: Option<PeriodExpr>,
    /// Grouping (drill path references) - resolved to FieldRef
    pub group: Vec<GroupItem>,
    /// Measures to show (simple, with time suffix, or inline)
    pub show: Vec<ShowItem>,
    /// Filter conditions
    pub filters: Vec<SqlExpr>,
    /// Sort order
    pub sort: Vec<SortItem>,
    /// Row limit
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupItem {
    /// Drill path ref: dates.standard.month
    DrillPathRef {
        source: String,
        path: String,
        level: String,
        label: Option<String>,
    },
    /// Inline slicer: region
    InlineSlicer {
        name: String,
        label: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowItem {
    /// Simple measure: revenue
    Measure {
        name: String,
        label: Option<String>,
    },
    /// Measure with time suffix: revenue.ytd
    MeasureWithSuffix {
        name: String,
        suffix: TimeSuffix,
        label: Option<String>,
    },
    /// Inline measure: net = { revenue - cost }
    InlineMeasure {
        name: String,
        expr: SqlExpr,
        label: Option<String>,
    },
}

/// Time intelligence suffixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSuffix {
    // Accumulations
    Ytd,
    Qtd,
    Mtd,
    Wtd,
    FiscalYtd,
    FiscalQtd,
    // Prior periods
    PriorYear,
    PriorQuarter,
    PriorMonth,
    PriorWeek,
    // Growth
    YoyGrowth,
    QoqGrowth,
    MomGrowth,
    WowGrowth,
    // Deltas
    YoyDelta,
    QoqDelta,
    MomDelta,
    WowDelta,
    // Rolling
    Rolling3m,
    Rolling6m,
    Rolling12m,
    Rolling3mAvg,
    Rolling6mAvg,
    Rolling12mAvg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeriodExpr {
    // Placeholder - will implement later
    LastNMonths(u32),
}
```

### Step 4: Update src/model/mod.rs

```rust
// src/model/mod.rs
pub mod types;
pub mod table;
pub mod calendar;
pub mod measure;
pub mod dimension;
pub mod report;  // ← Add this

pub use report::{GroupItem, PeriodExpr, Report, ShowItem, SortDirection, SortItem, TimeSuffix};  // ← Add this

// Update Model:
pub struct Model {
    pub defaults: Option<Defaults>,
    pub calendars: HashMap<String, calendar::Calendar>,
    pub dimensions: HashMap<String, dimension::Dimension>,
    pub tables: HashMap<String, table::Table>,
    pub measures: HashMap<String, measure::MeasureBlock>,
    pub reports: HashMap<String, report::Report>,  // ← Use real type
}
```

### Step 5: Run test to verify it passes

Run: `cargo test --test report_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/model/report.rs src/model/mod.rs tests/model/report_test.rs
git commit -m "feat(model): add Report type with drill paths and time suffixes"
```

---

## Task 7: Defaults Type

**Goal:** Implement Defaults type for model-wide settings.

**Files:**
- Create: `src/model/defaults.rs`
- Modify: `src/model/mod.rs`
- Create: `tests/model/defaults_test.rs`

### Step 1: Write test for Defaults

```rust
// tests/model/defaults_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{Defaults, NullHandling};
    use mantis_core::dsl::ast::{Month, Weekday};
    
    #[test]
    fn test_defaults() {
        let defaults = Defaults {
            calendar: Some("dates".to_string()),
            fiscal_year_start: Some(Month::April),
            week_start: Some(Weekday::Monday),
            null_handling: NullHandling::ReturnZero,
            decimal_places: 2,
        };
        
        assert_eq!(defaults.calendar, Some("dates".to_string()));
        assert_eq!(defaults.fiscal_year_start, Some(Month::April));
        assert_eq!(defaults.decimal_places, 2);
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test defaults_test`
Expected: Compilation error

### Step 3: Create src/model/defaults.rs

```rust
// src/model/defaults.rs
use crate::dsl::ast::{Month, Weekday};
use crate::model::types::NullHandling;

/// Model-wide defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Defaults {
    /// Default calendar for time bindings
    pub calendar: Option<String>,
    /// Fiscal year start month
    pub fiscal_year_start: Option<Month>,
    /// First day of week
    pub week_start: Option<Weekday>,
    /// Division NULL handling
    pub null_handling: NullHandling,
    /// Default decimal precision
    pub decimal_places: u8,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            calendar: None,
            fiscal_year_start: None,
            week_start: None,
            null_handling: NullHandling::ReturnNull,
            decimal_places: 2,
        }
    }
}
```

### Step 4: Update src/model/mod.rs

```rust
// src/model/mod.rs
pub mod types;
pub mod table;
pub mod calendar;
pub mod measure;
pub mod dimension;
pub mod report;
pub mod defaults;  // ← Add this

pub use defaults::Defaults;  // ← Add this, and remove placeholder

// Update Model:
pub struct Model {
    pub defaults: Option<defaults::Defaults>,  // ← Use real type
    pub calendars: HashMap<String, calendar::Calendar>,
    pub dimensions: HashMap<String, dimension::Dimension>,
    pub tables: HashMap<String, table::Table>,
    pub measures: HashMap<String, measure::MeasureBlock>,
    pub reports: HashMap<String, report::Report>,
}
```

### Step 5: Run test to verify it passes

Run: `cargo test --test defaults_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/model/defaults.rs src/model/mod.rs tests/model/defaults_test.rs
git commit -m "feat(model): add Defaults type for model-wide settings"
```

---

## Task 8: Lowering Infrastructure

**Goal:** Create lowering module with basic structure and error types.

**Files:**
- Create: `src/lowering/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/lowering/lowering_test.rs`

### Step 1: Write test for lowering

```rust
// tests/lowering/lowering_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::dsl::ast;
    use mantis_core::lowering;
    
    #[test]
    fn test_lower_empty_model() {
        let ast = ast::Model {
            defaults: None,
            items: vec![],
        };
        
        let result = lowering::lower(ast);
        assert!(result.is_ok());
        
        let model = result.unwrap();
        assert!(model.calendars.is_empty());
        assert!(model.tables.is_empty());
        assert!(model.measures.is_empty());
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test lowering_test`
Expected: Compilation error

### Step 3: Create src/lowering/mod.rs

```rust
// src/lowering/mod.rs
//! Lowering DSL AST to semantic model.

use crate::dsl::ast;
use crate::dsl::span::Spanned;
use crate::model;

/// Lower DSL AST to semantic model.
pub fn lower(ast: ast::Model) -> Result<model::Model, LoweringError> {
    let mut model = model::Model {
        defaults: None,
        calendars: std::collections::HashMap::new(),
        dimensions: std::collections::HashMap::new(),
        tables: std::collections::HashMap::new(),
        measures: std::collections::HashMap::new(),
        reports: std::collections::HashMap::new(),
    };
    
    // Lower defaults
    if let Some(defaults_ast) = ast.defaults {
        model.defaults = Some(lower_defaults(defaults_ast)?);
    }
    
    // Lower items
    for item in ast.items {
        match item.value {
            ast::Item::Calendar(cal) => {
                let calendar = lower_calendar(cal)?;
                model.calendars.insert(calendar.name.clone(), calendar);
            }
            ast::Item::Dimension(dim) => {
                let dimension = lower_dimension(dim)?;
                model.dimensions.insert(dimension.name.clone(), dimension);
            }
            ast::Item::Table(tbl) => {
                let table = lower_table(tbl)?;
                model.tables.insert(table.name.clone(), table);
            }
            ast::Item::MeasureBlock(meas) => {
                let measure_block = lower_measure_block(meas)?;
                model.measures.insert(measure_block.table_name.clone(), measure_block);
            }
            ast::Item::Report(rep) => {
                let report = lower_report(rep)?;
                model.reports.insert(report.name.clone(), report);
            }
        }
    }
    
    Ok(model)
}

fn lower_defaults(_defaults: Spanned<ast::Defaults>) -> Result<model::Defaults, LoweringError> {
    // Placeholder - will implement properly later
    Ok(model::Defaults::default())
}

fn lower_calendar(_calendar: ast::Calendar) -> Result<model::Calendar, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("calendar lowering".to_string()))
}

fn lower_dimension(_dimension: ast::Dimension) -> Result<model::Dimension, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("dimension lowering".to_string()))
}

fn lower_table(_table: ast::Table) -> Result<model::Table, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("table lowering".to_string()))
}

fn lower_measure_block(_measure_block: ast::MeasureBlock) -> Result<model::MeasureBlock, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("measure_block lowering".to_string()))
}

fn lower_report(_report: ast::Report) -> Result<model::Report, LoweringError> {
    // Placeholder
    Err(LoweringError::NotImplemented("report lowering".to_string()))
}

#[derive(Debug, Clone)]
pub enum LoweringError {
    NotImplemented(String),
}

impl std::fmt::Display for LoweringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoweringError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
        }
    }
}

impl std::error::Error for LoweringError {}
```

### Step 4: Add lowering module to lib.rs

```rust
// src/lib.rs
pub mod dsl;
pub mod lowering;  // ← Add this
pub mod model;
pub mod semantic;
pub mod sql;
```

### Step 5: Run test to verify it passes

Run: `cargo test --test lowering_test`
Expected: PASS

### Step 6: Commit

```bash
git add src/lowering/ src/lib.rs tests/lowering/
git commit -m "feat(lowering): add lowering infrastructure with error types"
```

---

## Next Steps

The remaining tasks follow this pattern:

- **Task 9-14**: Implement lowering for each type (calendar, dimension, table, measure, report, defaults)
- **Task 15-18**: Implement validation (circular dependency detection, reference validation, drill path validation)
- **Task 19**: Update ModelGraph::from_model()
- **Task 20**: Update ColumnLineageGraph::from_model()
- **Task 21-24**: Implement Report::to_semantic_query() with time suffix expansion and filter routing
- **Task 25-27**: Multi-grain query validation tests
- **Task 28-30**: Integration tests and golden file tests

Each task follows the TDD pattern shown above.

---

## Plan complete and saved

**Two execution options:**

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
