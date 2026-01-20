# Phase 2: Semantic Model Bridge - DSL-First Architecture

**Date:** 2026-01-20  
**Status:** Draft  
**Dependencies:** Phase 1 (DSL Parser) complete  
**Goal:** Build a new DSL-first model that replaces the existing Lua-based model, with calendars, time intelligence, and atoms/slicers as first-class concepts.

---

## Overview

Phase 2 creates a **new `src/model/`** with DSL-native types, replacing the old Lua-based model. The existing `src/semantic/` infrastructure (ModelGraph, ColumnLineageGraph, QueryPlanner) will be updated to work with the new model via new `from_model()` implementations.

**Key Principles:**
1. **The DSL is the source of truth** - Build the model to match DSL concepts exactly
2. **Reports are queries** - DSL reports map directly to existing `SemanticQuery` infrastructure via `Report::to_semantic_query()`, not separate execution paths
3. **Leverage existing planner** - The query planner already handles multi-fact queries, time functions, derived measures, and symmetric aggregates

**Architecture:**
```
src/
├── dsl/           # Phase 1 ✓ - Parser, AST  
├── model/         # Phase 2 - NEW (replaces old model/)
│   ├── mod.rs         # Model, Calendar, Dimension, Table
│   ├── calendar.rs    # Calendar types
│   ├── dimension.rs   # Dimension with drill paths
│   ├── table.rs       # Table with atoms/times/slicers
│   ├── measure.rs     # MeasureBlock with @atom syntax
│   ├── report.rs      # Report with drill paths, time suffixes
│   └── types.rs       # Shared types
├── lowering/      # Phase 2 - DSL AST → model
│   └── mod.rs
└── semantic/      # UPDATED - New from_model() implementations
    ├── model_graph/    # Add from_model(&new_model::Model)
    ├── column_lineage/ # Add from_model(&new_model::Model)
    └── planner/        # Extend for drill paths, time intelligence
```

---

## Current State Analysis

### What Exists Today

1. **Model Structure:**
   - `SourceEntity` - Raw tables with columns
   - `FactDefinition` - Denormalized fact tables with measures
   - `DimensionDefinition` - Conformed dimensions
   - `Relationship` - FK relationships with cardinality

2. **Limited Time Intelligence:**
   - `DimensionRole` - Role-playing dates (order_date, ship_date)
   - `GrainColumns` - Year/quarter/month/day detection
   - `TimeFunction` enum - YTD/QTD/prior period patterns
   - `TimeEmitter` - SQL window function generation

3. **Measure System:**
   - `MeasureDefinition` - SQL expressions with aggregation type
   - No atom concept - measures reference raw columns directly
   - No distinction between base atoms and derived measures

### What DSL Needs

1. **Universal Table Concept:**
   - DSL `table` works for CSVs, wide tables, fact tables, anything
   - Not separate source/fact/dimension types

2. **Atoms, Times, Slicers:**
   - Atoms: numeric columns for aggregation (@revenue, @quantity)
   - Times: date columns bound to calendars
   - Slicers: dimensions (inline, FK, via, calculated)

3. **First-Class Calendars:**
   - Physical calendars (existing date dimension tables)
   - Generated calendars (ephemeral CTEs)
   - Grain levels: minute → hour → day → week → month → quarter → year
   - Fiscal grains: fiscal_month, fiscal_quarter, fiscal_year
   - Drill paths: named hierarchies (standard, fiscal, etc.)

4. **Comprehensive Time Intelligence:**
   - Accumulations: ytd, qtd, mtd, wtd, fiscal_ytd, fiscal_qtd
   - Prior periods: prior_year, prior_quarter, prior_month, prior_week
   - Growth: yoy_growth, qoq_growth, mom_growth, wow_growth
   - Deltas: yoy_delta, qoq_delta, mom_delta, wow_delta
   - Rolling: rolling_3m, rolling_6m, rolling_12m (+ averages)

5. **Measure Resolution:**
   - `@atom` syntax to reference atoms
   - Measure-to-measure references (no @ prefix)
   - Filtered measures: `{ sum(@amount) } where { segment = 'Enterprise' }`
   - NULL handling per measure or model-wide default

6. **Reports with Drill Paths:**
   - Drill path references: `dates.standard.month`, `customers.geo.region`
   - Multi-source reports (cross-fact)
   - Period expressions (compile-time date ranges)
   - Time suffix application: `revenue.ytd`, `revenue.yoy_growth`

---

## New Semantic Model Structure

### Design Principles

1. **One Table Type:** DSL tables map 1:1 to a unified `Table` type
2. **Explicit Calendars:** Separate `Calendar` entities with drill paths
3. **Explicit Dimensions:** Optional `Dimension` entities for rich dimensions with drill paths
4. **Measure Blocks:** Separate `MeasureBlock` per table (not embedded in table)
5. **Preserve @atom Syntax:** Keep `@atom` references through to SQL generation
6. **Time Intelligence as Transform:** Expand time suffixes during query planning

### Core Types

```rust
/// The new DSL-first semantic model.
pub struct SemanticModel {
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
    
    /// Reports (thin wrappers around SemanticQuery with DSL features)
    pub reports: HashMap<String, Report>,
    
    /// Entity graph (for join path finding - computed from tables/dimensions)
    graph: OnceCell<ModelGraph>,
    
    /// Column lineage (lazy, for validation)
    lineage: OnceCell<ColumnLineageGraph>,
}

/// Model-wide defaults.
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

/// A calendar (date reference for time intelligence).
pub struct Calendar {
    pub name: String,
    pub body: CalendarBody,
}

pub enum CalendarBody {
    Physical(PhysicalCalendar),
    Generated(GeneratedCalendar),
}

/// Physical calendar referencing an existing date dimension.
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

pub enum CalendarRange {
    Explicit { start: Date, end: Date },
    Infer { min: Option<Date>, max: Option<Date> },
}

/// A drill path defining a valid aggregation hierarchy.
pub struct DrillPath {
    pub name: String,
    /// Ordered levels from fine to coarse
    pub levels: Vec<GrainLevel>,
}

/// Calendar grain levels.
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

/// A dimension (optional - for rich dimensions with drill paths).
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

pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
}

/// A table (universal: CSV, wide table, fact, etc.).
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

pub struct Atom {
    pub name: String,
    pub data_type: AtomType, // Int, Decimal, Float
}

pub struct TimeBinding {
    pub name: String,
    /// Calendar name
    pub calendar: String,
    /// Grain level
    pub grain: GrainLevel,
}

pub enum Slicer {
    Inline { name: String, data_type: DataType },
    ForeignKey { name: String, dimension: String, key: String },
    Via { name: String, fk_slicer: String },
    Calculated { name: String, data_type: DataType, expr: SqlExpr },
}

/// A measure block for a table.
pub struct MeasureBlock {
    pub table_name: String,
    pub measures: HashMap<String, Measure>,
}

/// A measure definition with @atom syntax preserved.
pub struct Measure {
    pub name: String,
    /// SQL expression with @atom references preserved
    pub expr: SqlExpr,
    /// Optional filter condition
    pub filter: Option<SqlExpr>,
    /// Optional NULL handling override
    pub null_handling: Option<NullHandling>,
}

/// A SQL expression with @atom syntax support.
pub struct SqlExpr {
    /// Raw SQL with @atom references (e.g., "sum(@revenue)")
    pub sql: String,
    /// Span for error reporting
    pub span: Span,
}

/// A report definition.
///
/// Reports are thin wrappers around SemanticQuery that support DSL-specific
/// features like drill paths, time suffixes, and period expressions.
/// The Report::to_semantic_query() method translates to existing query infrastructure.
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

pub enum GroupItem {
    /// Drill path ref: dates.standard.month
    DrillPathRef { source: String, path: String, level: String, label: Option<String> },
    /// Inline slicer: region
    InlineSlicer { name: String, label: Option<String> },
}

pub enum ShowItem {
    /// Simple measure: revenue
    Measure { name: String, label: Option<String> },
    /// Measure with time suffix: revenue.ytd
    MeasureWithSuffix { name: String, suffix: TimeSuffix, label: Option<String> },
    /// Inline measure: net = { revenue - cost }
    InlineMeasure { name: String, expr: SqlExpr, label: Option<String> },
}

/// Time intelligence suffixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSuffix {
    // Accumulations
    Ytd, Qtd, Mtd, Wtd, FiscalYtd, FiscalQtd,
    // Prior period
    PriorYear, PriorQuarter, PriorMonth, PriorWeek,
    // Growth (percentage)
    YoyGrowth, QoqGrowth, MomGrowth, WowGrowth,
    // Delta (absolute)
    YoyDelta, QoqDelta, MomDelta, WowDelta,
    // Rolling
    Rolling3m, Rolling6m, Rolling12m,
    Rolling3mAvg, Rolling6mAvg, Rolling12mAvg,
}
```

---

## Lowering Strategy: DSL AST → SemanticModel

### Step 1: Direct Translation

The lowering process is a straightforward 1:1 mapping from DSL AST to SemanticModel:

```rust
pub fn lower_ast_to_semantic_model(ast: dsl::Model) -> Result<SemanticModel, LoweringError> {
    let mut model = SemanticModel::new();
    
    // Lower defaults
    if let Some(defaults_ast) = ast.defaults {
        model.defaults = Some(lower_defaults(defaults_ast)?);
    }
    
    // Lower items
    for item in ast.items {
        match item.value {
            dsl::Item::Calendar(cal) => {
                let calendar = lower_calendar(cal)?;
                model.calendars.insert(calendar.name.clone(), calendar);
            }
            dsl::Item::Dimension(dim) => {
                let dimension = lower_dimension(dim)?;
                model.dimensions.insert(dimension.name.clone(), dimension);
            }
            dsl::Item::Table(tbl) => {
                let table = lower_table(tbl)?;
                model.tables.insert(table.name.clone(), table);
            }
            dsl::Item::MeasureBlock(meas) => {
                let measure_block = lower_measure_block(meas)?;
                model.measures.insert(measure_block.table_name.clone(), measure_block);
            }
            dsl::Item::Report(rep) => {
                let report = lower_report(rep)?;
                model.reports.insert(report.name.clone(), report);
            }
        }
    }
    
    Ok(model)
}

fn lower_table(ast: dsl::Table) -> Result<Table, LoweringError> {
    Ok(Table {
        name: ast.name.value,
        source: ast.source.value,
        atoms: ast.atoms.into_iter()
            .map(|a| (a.value.name.value.clone(), lower_atom(a.value)))
            .collect(),
        times: ast.times.into_iter()
            .map(|t| (t.value.name.value.clone(), lower_time_binding(t.value)))
            .collect(),
        slicers: ast.slicers.into_iter()
            .map(|s| (slicer_name(&s.value), lower_slicer(s.value)))
            .collect(),
    })
}

fn lower_measure_block(ast: dsl::MeasureBlock) -> Result<MeasureBlock, LoweringError> {
    Ok(MeasureBlock {
        table_name: ast.table.value,
        measures: ast.measures.into_iter()
            .map(|m| {
                let name = m.value.name.value.clone();
                let measure = Measure {
                    name: name.clone(),
                    expr: m.value.expr.value, // Preserve @atom syntax
                    filter: m.value.filter.map(|f| f.value),
                    null_handling: m.value.null_handling.map(|nh| nh.value),
                };
                (name, measure)
            })
            .collect(),
    })
}
```

**Key Points:**
- No validation during lowering (validation is separate pass)
- Preserve `@atom` syntax in SQL expressions
- Keep all span information for error reporting

### Step 2: Validation Pass

After lowering, run a separate validation pass:

```rust
pub struct Validator<'a> {
    model: &'a SemanticModel,
    errors: Vec<ValidationError>,
}

impl<'a> Validator<'a> {
    pub fn validate(model: &'a SemanticModel) -> Result<(), Vec<ValidationError>> {
        let mut validator = Self { model, errors: vec![] };
        
        // Validate calendar references
        validator.validate_time_bindings();
        
        // Validate dimension references
        validator.validate_slicers();
        
        // Validate measure expressions
        validator.validate_measures();
        
        // Validate drill paths
        validator.validate_drill_paths();
        
        // Validate reports
        validator.validate_reports();
        
        if validator.errors.is_empty() {
            Ok(())
        } else {
            Err(validator.errors)
        }
    }
    
    fn validate_time_bindings(&mut self) {
        for table in self.model.tables.values() {
            for time in table.times.values() {
                // Check calendar exists
                if !self.model.calendars.contains_key(&time.calendar) {
                    self.errors.push(ValidationError::UnknownCalendar {
                        calendar: time.calendar.clone(),
                        context: format!("table '{}' time '{}'", table.name, time.name),
                    });
                }
                
                // Check grain level is valid for calendar
                if let Some(calendar) = self.model.calendars.get(&time.calendar) {
                    if !calendar.supports_grain(time.grain) {
                        self.errors.push(ValidationError::InvalidGrainLevel {
                            grain: time.grain,
                            calendar: time.calendar.clone(),
                        });
                    }
                }
            }
        }
    }
    
    fn validate_measures(&mut self) {
        for measure_block in self.model.measures.values() {
            // Check table exists
            if !self.model.tables.contains_key(&measure_block.table_name) {
                self.errors.push(ValidationError::UnknownTable {
                    table: measure_block.table_name.clone(),
                    context: format!("measures block"),
                });
                continue;
            }
            
            let table = &self.model.tables[&measure_block.table_name];
            
            for measure in measure_block.measures.values() {
                // Parse and validate @atom references
                self.validate_measure_expr(&measure.expr, table, measure_block);
                
                // Validate filter expression
                if let Some(ref filter) = measure.filter {
                    self.validate_filter_expr(filter, table);
                }
            }
        }
    }
    
    fn validate_measure_expr(&mut self, expr: &SqlExpr, table: &Table, measure_block: &MeasureBlock) {
        // Parse SQL expression
        // Find all @atom references (regex: @[a-z_][a-z0-9_]*)
        // Check each atom exists in table.atoms
        // Find all measure references (no @ prefix)
        // Check each measure exists in measure_block.measures
        // Check for circular dependencies
        
        // Implementation uses simple regex for @atom detection
        let atom_pattern = regex::Regex::new(r"@([a-z_][a-z0-9_]*)").unwrap();
        for cap in atom_pattern.captures_iter(&expr.sql) {
            let atom_name = &cap[1];
            if !table.atoms.contains_key(atom_name) {
                self.errors.push(ValidationError::UnknownAtom {
                    atom: atom_name.to_string(),
                    table: table.name.clone(),
                    span: expr.span.clone(),
                });
            }
        }
        
        // Similar for measure references (identifier not starting with @)
        // This is more complex - would use sqlparser-rs to extract identifiers
    }
}

pub enum ValidationError {
    UnknownCalendar { calendar: String, context: String },
    UnknownTable { table: String, context: String },
    UnknownAtom { atom: String, table: String, span: Span },
    UnknownMeasure { measure: String, table: String, span: Span },
    UnknownDimension { dimension: String, context: String },
    InvalidGrainLevel { grain: GrainLevel, calendar: String },
    CircularMeasureReference { measures: Vec<String> },
    InvalidDrillPath { path: String, reason: String },
}
```

---

## Measure Resolution with @atom Syntax

### Approach: Preserve Through to SQL Generation

Instead of resolving `@atom` references during lowering, we preserve them through to SQL generation. This provides:
1. Clear semantic distinction in stored expressions
2. Better error messages (can reference atoms by name)
3. Easier column lineage tracking
4. Future LSP features (go-to-definition on @atom)

### SQL Generation Process

```rust
pub struct MeasureResolver<'a> {
    model: &'a SemanticModel,
    table: &'a Table,
    measure_block: &'a MeasureBlock,
}

impl<'a> MeasureResolver<'a> {
    /// Resolve a measure expression to SQL.
    ///
    /// Replaces @atom references with qualified column names.
    /// Recursively expands measure-to-measure references.
    pub fn resolve_measure(&self, measure_name: &str) -> Result<Expr, ResolutionError> {
        let measure = self.measure_block.measures.get(measure_name)
            .ok_or_else(|| ResolutionError::UnknownMeasure(measure_name.to_string()))?;
        
        // Parse SQL expression
        let parsed = sqlparser::parse(&measure.expr.sql)?;
        
        // Walk AST and replace @atom references
        let resolved = self.resolve_expr(parsed)?;
        
        Ok(resolved)
    }
    
    fn resolve_expr(&self, expr: sqlparser::ast::Expr) -> Result<Expr, ResolutionError> {
        match expr {
            // @atom reference (parsed as identifier starting with @)
            sqlparser::ast::Expr::Identifier(ident) if ident.value.starts_with('@') => {
                let atom_name = &ident.value[1..]; // Strip @
                
                // Look up in table.atoms
                if !self.table.atoms.contains_key(atom_name) {
                    return Err(ResolutionError::UnknownAtom(atom_name.to_string()));
                }
                
                // Replace with qualified column reference
                Ok(Expr::Column {
                    table: Some(self.table.name.clone()),
                    name: atom_name.to_string(),
                })
            }
            
            // Measure reference (regular identifier)
            sqlparser::ast::Expr::Identifier(ident) => {
                // Check if it's a measure in this block
                if let Some(ref_measure) = self.measure_block.measures.get(&ident.value) {
                    // Recursively resolve the referenced measure
                    self.resolve_measure(&ident.value)
                } else {
                    // Not a measure reference, keep as-is (might be a column from a dimension)
                    Ok(Expr::Column {
                        table: None,
                        name: ident.value,
                    })
                }
            }
            
            // Recursively handle other expression types
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                Ok(Expr::BinaryOp {
                    left: Box::new(self.resolve_expr(*left)?),
                    op: convert_op(op),
                    right: Box::new(self.resolve_expr(*right)?),
                })
            }
            
            // ... handle other expression types
            _ => convert_expr(expr),
        }
    }
}
```

**Example Resolution:**

```
DSL:
measures fact_sales {
    revenue = { sum(@amount) };
    cost = { sum(@cost_amount) };
    margin = { revenue - cost };
    margin_pct = { margin / revenue * 100 };
}

Resolved SQL for margin_pct:
  ((SUM(fact_sales.amount) - SUM(fact_sales.cost_amount)) / SUM(fact_sales.amount)) * 100
```

---

## Time Intelligence Integration

### Current State

Existing `TimeFunction` enum supports:
- `YearToDate`, `QuarterToDate`, `MonthToDate`
- `PriorPeriod`, `PriorYear`, `PriorQuarter`
- `RollingSum`, `RollingAvg`

Existing `TimeEmitter` generates window functions.

### New Requirements

DSL time suffixes are more comprehensive:
- All existing + `Wtd`, `FiscalYtd`, `FiscalQtd`
- Growth calculations: `YoyGrowth`, `QoqGrowth`, `MomGrowth`, `WowGrowth`
- Deltas: `YoyDelta`, `QoqDelta`, `MomDelta`, `WowDelta`
- More rolling: `Rolling3m`, `Rolling6m`, `Rolling12m` (+ averages)

### Expansion Strategy

Time suffixes are expanded during query planning, not during lowering:

```rust
pub struct TimeIntelligenceExpander<'a> {
    model: &'a SemanticModel,
    report: &'a Report,
}

impl<'a> TimeIntelligenceExpander<'a> {
    /// Expand a measure with time suffix into SQL expression.
    pub fn expand(
        &self,
        measure_name: &str,
        suffix: TimeSuffix,
    ) -> Result<TimeIntelligenceExpr, ExpansionError> {
        // Find which table this measure belongs to
        let (table_name, measure_block) = self.find_measure_table(measure_name)?;
        let table = &self.model.tables[table_name];
        
        // Find the time binding for this table
        let time_binding = self.find_primary_time_binding(table, &self.report)?;
        
        // Get the calendar
        let calendar = &self.model.calendars[&time_binding.calendar];
        
        // Resolve base measure expression
        let base_expr = MeasureResolver::new(self.model, table, measure_block)
            .resolve_measure(measure_name)?;
        
        // Expand based on suffix
        match suffix {
            TimeSuffix::Ytd => {
                // SUM(base_measure) OVER (PARTITION BY year ORDER BY month ROWS UNBOUNDED PRECEDING)
                self.expand_accumulation(base_expr, calendar, GrainLevel::Year, time_binding.grain)
            }
            
            TimeSuffix::Qtd => {
                // PARTITION BY year, quarter ORDER BY month
                self.expand_accumulation(base_expr, calendar, GrainLevel::Quarter, time_binding.grain)
            }
            
            TimeSuffix::YoyGrowth => {
                // ((current - prior_year) / prior_year) * 100
                let prior = self.expand_prior_period(base_expr.clone(), 12)?; // 12 months back
                self.expand_growth(base_expr, prior)
            }
            
            TimeSuffix::Rolling3m => {
                // SUM(base_measure) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
                self.expand_rolling_sum(base_expr, 3, time_binding.grain)
            }
            
            // ... other suffixes
        }
    }
    
    fn expand_accumulation(
        &self,
        measure_expr: Expr,
        calendar: &Calendar,
        partition_grain: GrainLevel,
        order_grain: GrainLevel,
    ) -> Result<TimeIntelligenceExpr, ExpansionError> {
        // Get column names for partition and order grains from calendar
        let partition_col = calendar.grain_column(partition_grain)?;
        let order_col = calendar.grain_column(order_grain)?;
        
        Ok(TimeIntelligenceExpr {
            expr: Expr::Window {
                func: WindowFunc::Sum(Box::new(measure_expr)),
                partition_by: vec![Expr::Column {
                    table: Some(calendar.name.clone()),
                    name: partition_col,
                }],
                order_by: vec![OrderByExpr::asc(Expr::Column {
                    table: Some(calendar.name.clone()),
                    name: order_col,
                })],
                frame: Some(WindowFrame::rows_to_current()),
            },
            // Track which calendar columns are needed for GROUP BY
            required_group_by: vec![(calendar.name.clone(), partition_col), (calendar.name.clone(), order_col)],
        })
    }
    
    fn expand_growth(&self, current: Expr, prior: Expr) -> Result<TimeIntelligenceExpr, ExpansionError> {
        // ((current - prior) / prior) * 100
        Ok(TimeIntelligenceExpr {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(current),
                        op: BinaryOp::Subtract,
                        right: Box::new(prior.clone()),
                    }),
                    op: BinaryOp::Divide,
                    right: Box::new(prior),
                }),
                op: BinaryOp::Multiply,
                right: Box::new(Expr::Literal(Literal::Int(100))),
            },
            required_group_by: vec![], // Inherits from current/prior
        })
    }
}

pub struct TimeIntelligenceExpr {
    /// The expanded SQL expression
    pub expr: Expr,
    /// Columns that must be in GROUP BY for this to work
    pub required_group_by: Vec<(String, String)>, // (table_alias, column_name)
}
```

### Calendar Support for Time Intelligence

Calendars need to expose grain column mappings:

```rust
impl Calendar {
    /// Get the column name for a grain level.
    pub fn grain_column(&self, grain: GrainLevel) -> Result<String, CalendarError> {
        match &self.body {
            CalendarBody::Physical(phys) => {
                phys.grain_mappings.get(&grain)
                    .cloned()
                    .ok_or_else(|| CalendarError::GrainNotMapped { grain, calendar: self.name.clone() })
            }
            CalendarBody::Generated(gen) => {
                // Generated calendars auto-generate column names
                // day → "date", month → "month_start_date", etc.
                Ok(generated_grain_column_name(grain))
            }
        }
    }
    
    /// Check if calendar supports a grain level.
    pub fn supports_grain(&self, grain: GrainLevel) -> bool {
        match &self.body {
            CalendarBody::Physical(phys) => phys.grain_mappings.contains_key(&grain),
            CalendarBody::Generated(gen) => {
                // Check if grain is in the base_grain+ set or fiscal grains
                gen.base_grain.and_coarser().contains(&grain)
                    || (gen.fiscal.is_some() && grain.is_fiscal())
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

impl GrainLevel {
    pub fn is_fiscal(&self) -> bool {
        matches!(self, GrainLevel::FiscalMonth | GrainLevel::FiscalQuarter | GrainLevel::FiscalYear)
    }
}
```

---

## Implementation Plan

### Step 1: Create New Model Types

Create `src/model/` with DSL-native types:

**Files:**
```
src/model/
├── mod.rs           # Model (main entry point)
├── calendar.rs      # Calendar, PhysicalCalendar, GeneratedCalendar
├── dimension.rs     # Dimension with drill paths
├── table.rs         # Table with Atom, TimeBinding, Slicer
├── measure.rs       # MeasureBlock, Measure
├── report.rs        # Report with drill path refs, time suffixes
├── defaults.rs      # Defaults
└── types.rs         # Shared types (GrainLevel, Month, etc.)
```

**Core Model:**
```rust
// src/model/mod.rs
pub struct Model {
    pub defaults: Option<Defaults>,
    pub calendars: HashMap<String, Calendar>,
    pub dimensions: HashMap<String, Dimension>,
    pub tables: HashMap<String, Table>,
    pub measures: HashMap<String, MeasureBlock>,
    pub reports: HashMap<String, Report>,
}
```

See "New Semantic Model Structure" section below for detailed type definitions.

### Step 2: Implement Lowering

Create `src/lowering/mod.rs`:
```rust
pub fn lower(ast: dsl::Model) -> Result<model::Model, LoweringError> {
    // Direct 1:1 translation DSL AST → model
    // No validation here - just translation
    // Preserve spans for error reporting
}
```

**Strategy:** Straightforward mapping, preserve all span info for validation errors.

### Step 3: Implement Validation

Add validation to `src/model/mod.rs`:
```rust
impl Model {
    pub fn validate(&self) -> Result<(), Vec<ValidationError>> {
        // Check calendar references
        // Check dimension references  
        // Validate @atom references in measures
        // Check drill path validity
        // Detect circular measure dependencies
        // Validate report references
    }
}
```

Return all errors at once (don't fail on first error).

### Step 4: Update ModelGraph

Update `src/semantic/model_graph/mod.rs`:
```rust
impl ModelGraph {
    pub fn from_model(model: &model::Model) -> GraphResult<Self> {
        // Build graph from new model structure
        // Tables create nodes
        // FK slicers create edges
        // Dimensions create nodes
        // Calendars create nodes (for time joins)
        // Via slicers inherit edges from FK slicers
    }
}
```

**Reuse existing graph algorithms** - just change how nodes/edges are built.

### Step 5: Update ColumnLineageGraph

Update `src/semantic/column_lineage/mod.rs`:
```rust
impl ColumnLineageGraph {
    pub fn from_model(model: &model::Model) -> Self {
        // Track atoms → measures
        // Track measure → measure dependencies
        // Handle @atom syntax
        // Track via slicer chains
    }
}
```

### Step 6: Map DSL Reports to SemanticQuery

**Key Insight:** DSL reports should map directly to existing `SemanticQuery` infrastructure, not create separate report execution paths. The existing query planner is more robust and handles multi-fact queries, time functions, derived measures, and filters.

**Strategy:**
1. Create a translation layer: `DSL Report → SemanticQuery`
2. Leverage existing `QueryPlanner` to plan the SemanticQuery
3. Extend `SemanticQuery` types to support DSL-specific features

**Implementation in `src/model/report.rs`:**

```rust
impl Report {
    /// Convert DSL report to SemanticQuery.
    ///
    /// This translation layer maps DSL report concepts to the existing
    /// query infrastructure:
    /// - Drill path refs → FieldRef (entity.field)
    /// - Time suffixes → DerivedField with TimeFunction
    /// - Inline measures → DerivedField
    /// - Period expressions → FieldFilter on date columns
    pub fn to_semantic_query(&self, model: &Model) -> Result<SemanticQuery, ReportError> {
        let mut query = SemanticQuery::default();
        
        // 1. Set anchor fact(s)
        query.from = if self.from.len() == 1 {
            Some(self.from[0].clone())
        } else {
            None // Multi-source, let planner infer
        };
        
        // 2. Resolve drill path refs to FieldRef
        for group_item in &self.group {
            let field_ref = self.resolve_group_item(group_item, model)?;
            query.group_by.push(field_ref);
        }
        
        // 3. Map show items to select/derived fields
        for show_item in &self.show {
            match show_item {
                ShowItem::Measure { name, label } => {
                    // Simple measure: resolve to table.measure
                    let (table, measure) = self.resolve_measure(name, model)?;
                    query.select.push(SelectField {
                        field: FieldRef::new(&table, measure),
                        alias: label.clone(),
                        aggregation: None, // Already aggregated
                        measure_filter: None,
                    });
                }
                
                ShowItem::MeasureWithSuffix { name, suffix, label } => {
                    // Time intelligence: base measure + derived field
                    let (table, measure) = self.resolve_measure(name, model)?;
                    
                    // Add base measure to select
                    let base_alias = format!("_base_{}", name);
                    query.select.push(SelectField {
                        field: FieldRef::new(&table, measure),
                        alias: Some(base_alias.clone()),
                        aggregation: None,
                        measure_filter: None,
                    });
                    
                    // Add derived field with time function
                    let time_func = self.expand_time_suffix(&base_alias, *suffix, model)?;
                    query.derived.push(DerivedField {
                        alias: label.clone().unwrap_or_else(|| format!("{}_{:?}", name, suffix)),
                        expression: DerivedExpr::TimeFunction(time_func),
                    });
                }
                
                ShowItem::InlineMeasure { name, expr, label } => {
                    // Inline calculation: parse expr into DerivedExpr
                    let derived_expr = self.parse_inline_expr(expr, model)?;
                    query.derived.push(DerivedField {
                        alias: label.clone().unwrap_or_else(|| name.clone()),
                        expression: derived_expr,
                    });
                }
            }
        }
        
        // 4. Evaluate period expressions and add to filters
        if let Some(period) = &self.period {
            let date_filter = self.evaluate_period(period, model)?;
            query.filters.push(date_filter);
        }
        
        // 5. Add report filters
        for filter_expr in &self.filters {
            let field_filter = self.parse_filter(filter_expr, model)?;
            query.filters.push(field_filter);
        }
        
        // 6. Add sort order
        for sort_item in &self.sort {
            query.order_by.push(self.resolve_sort(sort_item, model)?);
        }
        
        // 7. Set limit
        query.limit = self.limit;
        
        Ok(query)
    }
    
    /// Resolve a drill path reference to a FieldRef.
    fn resolve_group_item(&self, item: &GroupItem, model: &Model) -> Result<FieldRef, ReportError> {
        match item {
            GroupItem::DrillPathRef { source, path, level, .. } => {
                // Look up calendar or dimension
                if let Some(calendar) = model.calendars.get(source) {
                    // Resolve to calendar grain column
                    let grain_level = GrainLevel::from_str(level)?;
                    let column = calendar.grain_column(grain_level)?;
                    Ok(FieldRef::new(source, &column))
                } else if let Some(dimension) = model.dimensions.get(source) {
                    // Resolve to dimension attribute
                    let attr = dimension.attributes.get(level)
                        .ok_or_else(|| ReportError::UnknownAttribute {
                            dimension: source.clone(),
                            attribute: level.clone(),
                        })?;
                    Ok(FieldRef::new(source, &attr.name))
                } else {
                    Err(ReportError::UnknownSource(source.clone()))
                }
            }
            
            GroupItem::InlineSlicer { name, .. } => {
                // Find which table has this slicer
                let table_name = self.find_table_for_slicer(name, model)?;
                Ok(FieldRef::new(&table_name, name))
            }
        }
    }
    
    /// Expand a time suffix to a TimeFunction.
    fn expand_time_suffix(
        &self,
        measure_ref: &str,
        suffix: TimeSuffix,
        model: &Model,
    ) -> Result<TimeFunction, ReportError> {
        // Map DSL time suffix to existing TimeFunction
        match suffix {
            TimeSuffix::Ytd => Ok(TimeFunction::ytd(measure_ref)),
            TimeSuffix::Qtd => Ok(TimeFunction::QuarterToDate {
                measure: measure_ref.to_string(),
                year_column: None,
                quarter_column: None,
                period_column: None,
                via: None,
            }),
            TimeSuffix::YoyGrowth => {
                // Growth = (current - prior_year) / prior_year * 100
                let prior = TimeFunction::prior_year(measure_ref);
                Ok(TimeFunction::PriorYear {
                    measure: measure_ref.to_string(),
                    via: None,
                })
                // Note: Growth calculation happens in DerivedExpr::Growth
                // This is simplified - full implementation needs Growth wrapper
            }
            TimeSuffix::Rolling3m => Ok(TimeFunction::rolling_sum(measure_ref, 3)),
            // ... map other suffixes
            _ => Err(ReportError::UnsupportedTimeSuffix(suffix)),
        }
    }
    
    /// Evaluate a period expression to a date range filter.
    fn evaluate_period(
        &self,
        period: &PeriodExpr,
        model: &Model,
    ) -> Result<FieldFilter, ReportError> {
        // Evaluate period at compile time to date range
        let (start, end) = period.evaluate()?;
        
        // Find the time column from use_date
        let time_field = &self.use_date[0]; // Simplified - handle multi-source
        let table = &self.from[0];
        
        // Create filter: time_field BETWEEN start AND end
        Ok(FieldFilter {
            field: FieldRef::new(table, time_field),
            op: FilterOp::Gte,
            value: FilterValue::String(start.to_string()),
            // Note: This is simplified - need compound filter for BETWEEN
        })
    }
}
```

**Reuse existing infrastructure:**
- `SemanticQuery` → query planner (already handles multi-fact, time functions)
- `DerivedField` → derived measures and time intelligence
- `TimeFunction` → window functions (YTD, prior period, rolling)
- Symmetric aggregates → multi-source reports (already works)

**Just add:**
- DSL Report → SemanticQuery translation
- Drill path resolution
- Period expression evaluation
- Time suffix mapping to TimeFunction

---

## Complete Time Suffix Implementation

All 20+ DSL time suffixes need to be implemented. The pattern is consistent across categories, so implementation follows a clear structure.

### Time Suffix Categories

#### 1. Accumulations (6 variants)
Window functions with `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`:

```rust
// Standard accumulations
TimeSuffix::Ytd  → TimeFunction::YearToDate
TimeSuffix::Qtd  → TimeFunction::QuarterToDate
TimeSuffix::Mtd  → TimeFunction::MonthToDate
TimeSuffix::Wtd  → TimeFunction::WeekToDate (NEW variant needed)

// Fiscal accumulations
TimeSuffix::FiscalYtd → TimeFunction::FiscalYearToDate (NEW variant needed)
TimeSuffix::FiscalQtd → TimeFunction::FiscalQuarterToDate (NEW variant needed)
```

**Implementation Pattern:**
```sql
-- ytd example:
SUM(measure) OVER (
    PARTITION BY calendar.year 
    ORDER BY calendar.month 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

#### 2. Prior Periods (4 variants)
LAG functions with period-specific offsets:

```rust
TimeSuffix::PriorYear    → TimeFunction::PriorYear (EXISTS)
TimeSuffix::PriorQuarter → TimeFunction::PriorQuarter (EXISTS)
TimeSuffix::PriorMonth   → TimeFunction::PriorPeriod { periods_back: 1 } (EXISTS)
TimeSuffix::PriorWeek    → TimeFunction::PriorPeriod { periods_back: 1 } (EXISTS)
```

**Implementation Pattern:**
```sql
-- prior_year for monthly grain:
LAG(measure, 12) OVER (ORDER BY calendar.month)

-- prior_quarter for monthly grain:
LAG(measure, 3) OVER (ORDER BY calendar.month)
```

#### 3. Growth (4 variants)
Percentage change calculations requiring `DerivedExpr::Growth` wrapper:

```rust
TimeSuffix::YoyGrowth → DerivedExpr::Growth { 
    current: measure_ref, 
    previous: TimeFunction::PriorYear 
}
TimeSuffix::QoqGrowth → DerivedExpr::Growth { 
    current: measure_ref, 
    previous: TimeFunction::PriorQuarter 
}
TimeSuffix::MomGrowth → DerivedExpr::Growth { 
    current: measure_ref, 
    previous: TimeFunction::PriorPeriod { periods_back: 1 } 
}
TimeSuffix::WowGrowth → DerivedExpr::Growth { 
    current: measure_ref, 
    previous: TimeFunction::PriorPeriod { periods_back: 1 } 
}
```

**Implementation Pattern:**
```sql
-- yoy_growth:
((measure - LAG(measure, 12) OVER (...)) / NULLIF(LAG(measure, 12) OVER (...), 0)) * 100
```

#### 4. Deltas (4 variants)
Absolute change calculations requiring `DerivedExpr::Delta` wrapper:

```rust
TimeSuffix::YoyDelta → DerivedExpr::Delta { 
    current: measure_ref, 
    previous: TimeFunction::PriorYear 
}
// Same pattern for QoqDelta, MomDelta, WowDelta
```

**Implementation Pattern:**
```sql
-- yoy_delta:
measure - LAG(measure, 12) OVER (ORDER BY calendar.month)
```

#### 5. Rolling Windows (6 variants)
Window functions with fixed-size frames:

```rust
TimeSuffix::Rolling3m    → TimeFunction::RollingSum { periods: 3 } (EXISTS)
TimeSuffix::Rolling6m    → TimeFunction::RollingSum { periods: 6 } (NEW)
TimeSuffix::Rolling12m   → TimeFunction::RollingSum { periods: 12 } (NEW)

TimeSuffix::Rolling3mAvg  → TimeFunction::RollingAvg { periods: 3 } (EXISTS)
TimeSuffix::Rolling6mAvg  → TimeFunction::RollingAvg { periods: 6 } (NEW)
TimeSuffix::Rolling12mAvg → TimeFunction::RollingAvg { periods: 12 } (NEW)
```

**Implementation Pattern:**
```sql
-- rolling_6m:
SUM(measure) OVER (
    ORDER BY calendar.month 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
)

-- rolling_6m_avg:
AVG(measure) OVER (
    ORDER BY calendar.month 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
)
```

### Required Extensions to Existing Types

#### 1. New TimeFunction Variants

Add to `src/semantic/planner/types.rs`:

```rust
pub enum TimeFunction {
    // Existing variants...
    YearToDate { ... },
    QuarterToDate { ... },
    MonthToDate { ... },
    PriorPeriod { ... },
    PriorYear { ... },
    PriorQuarter { ... },
    RollingSum { ... },
    RollingAvg { ... },
    
    // NEW variants needed:
    
    /// Week-to-date accumulation
    WeekToDate {
        measure: String,
        year_column: Option<String>,
        week_column: Option<String>,
        day_column: Option<String>,
        via: Option<String>,
    },
    
    /// Fiscal year-to-date (requires fiscal calendar)
    FiscalYearToDate {
        measure: String,
        fiscal_year_column: Option<String>,
        period_column: Option<String>,
        via: Option<String>,
    },
    
    /// Fiscal quarter-to-date
    FiscalQuarterToDate {
        measure: String,
        fiscal_year_column: Option<String>,
        fiscal_quarter_column: Option<String>,
        period_column: Option<String>,
        via: Option<String>,
    },
}
```

#### 2. New DerivedExpr Variants

Add to `src/semantic/planner/types.rs`:

```rust
pub enum DerivedExpr {
    // Existing variants...
    MeasureRef(String),
    Literal(f64),
    BinaryOp { ... },
    Negate(Box<DerivedExpr>),
    TimeFunction(TimeFunction),
    
    // Already exists (confirmed):
    Delta { current: Box<DerivedExpr>, previous: Box<DerivedExpr> },
    Growth { current: Box<DerivedExpr>, previous: Box<DerivedExpr> },
}
```

**NOTE:** Delta and Growth already exist in the codebase, so no new variants needed here.

### Implementation Mapping Table

Complete mapping from DSL suffix to implementation:

| DSL Suffix | TimeFunction/DerivedExpr | Status | Lag Offset |
|------------|-------------------------|--------|------------|
| `.ytd` | `TimeFunction::YearToDate` | EXISTS | - |
| `.qtd` | `TimeFunction::QuarterToDate` | EXISTS | - |
| `.mtd` | `TimeFunction::MonthToDate` | EXISTS | - |
| `.wtd` | `TimeFunction::WeekToDate` | **NEW** | - |
| `.fiscal_ytd` | `TimeFunction::FiscalYearToDate` | **NEW** | - |
| `.fiscal_qtd` | `TimeFunction::FiscalQuarterToDate` | **NEW** | - |
| `.prior_year` | `TimeFunction::PriorYear` | EXISTS | 12 |
| `.prior_quarter` | `TimeFunction::PriorQuarter` | EXISTS | 3 |
| `.prior_month` | `TimeFunction::PriorPeriod(1)` | EXISTS | 1 |
| `.prior_week` | `TimeFunction::PriorPeriod(1)` | EXISTS | 1 |
| `.yoy_growth` | `DerivedExpr::Growth + PriorYear` | EXISTS | - |
| `.qoq_growth` | `DerivedExpr::Growth + PriorQuarter` | EXISTS | - |
| `.mom_growth` | `DerivedExpr::Growth + PriorPeriod(1)` | EXISTS | - |
| `.wow_growth` | `DerivedExpr::Growth + PriorPeriod(1)` | EXISTS | - |
| `.yoy_delta` | `DerivedExpr::Delta + PriorYear` | EXISTS | - |
| `.qoq_delta` | `DerivedExpr::Delta + PriorQuarter` | EXISTS | - |
| `.mom_delta` | `DerivedExpr::Delta + PriorPeriod(1)` | EXISTS | - |
| `.wow_delta` | `DerivedExpr::Delta + PriorPeriod(1)` | EXISTS | - |
| `.rolling_3m` | `TimeFunction::RollingSum(3)` | EXISTS | - |
| `.rolling_6m` | `TimeFunction::RollingSum(6)` | **NEW** | - |
| `.rolling_12m` | `TimeFunction::RollingSum(12)` | **NEW** | - |
| `.rolling_3m_avg` | `TimeFunction::RollingAvg(3)` | EXISTS | - |
| `.rolling_6m_avg` | `TimeFunction::RollingAvg(6)` | **NEW** | - |
| `.rolling_12m_avg` | `TimeFunction::RollingAvg(12)` | **NEW** | - |

**Summary:** 
- 16 suffixes work with existing types
- 8 suffixes need new variants (3 accumulation, 2 rolling sum, 3 rolling avg - but rolling is just parameterized)
- **Actual new code needed:** 3 TimeFunction variants (WeekToDate, FiscalYearToDate, FiscalQuarterToDate)

### Complete expand_time_suffix Implementation

```rust
fn expand_time_suffix(
    &self,
    measure_ref: &str,
    suffix: TimeSuffix,
    model: &Model,
) -> Result<DerivedExpr, ReportError> {
    match suffix {
        // === Accumulations ===
        TimeSuffix::Ytd => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::ytd(measure_ref)))
        }
        TimeSuffix::Qtd => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::QuarterToDate {
                measure: measure_ref.to_string(),
                year_column: None,
                quarter_column: None,
                period_column: None,
                via: None,
            }))
        }
        TimeSuffix::Mtd => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::MonthToDate {
                measure: measure_ref.to_string(),
                year_column: None,
                month_column: None,
                day_column: None,
                via: None,
            }))
        }
        TimeSuffix::Wtd => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::WeekToDate {
                measure: measure_ref.to_string(),
                year_column: None,
                week_column: None,
                day_column: None,
                via: None,
            }))
        }
        TimeSuffix::FiscalYtd => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::FiscalYearToDate {
                measure: measure_ref.to_string(),
                fiscal_year_column: None,
                period_column: None,
                via: None,
            }))
        }
        TimeSuffix::FiscalQtd => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::FiscalQuarterToDate {
                measure: measure_ref.to_string(),
                fiscal_year_column: None,
                fiscal_quarter_column: None,
                period_column: None,
                via: None,
            }))
        }
        
        // === Prior Periods ===
        TimeSuffix::PriorYear => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::prior_year(measure_ref)))
        }
        TimeSuffix::PriorQuarter => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::PriorQuarter {
                measure: measure_ref.to_string(),
                via: None,
            }))
        }
        TimeSuffix::PriorMonth => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::prior_period(measure_ref, 1)))
        }
        TimeSuffix::PriorWeek => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::prior_period(measure_ref, 1)))
        }
        
        // === Growth (percentage change) ===
        TimeSuffix::YoyGrowth => {
            Ok(DerivedExpr::Growth {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::prior_year(measure_ref)
                )),
            })
        }
        TimeSuffix::QoqGrowth => {
            Ok(DerivedExpr::Growth {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::PriorQuarter {
                        measure: measure_ref.to_string(),
                        via: None,
                    }
                )),
            })
        }
        TimeSuffix::MomGrowth => {
            Ok(DerivedExpr::Growth {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::prior_period(measure_ref, 1)
                )),
            })
        }
        TimeSuffix::WowGrowth => {
            Ok(DerivedExpr::Growth {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::prior_period(measure_ref, 1)
                )),
            })
        }
        
        // === Deltas (absolute change) ===
        TimeSuffix::YoyDelta => {
            Ok(DerivedExpr::Delta {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::prior_year(measure_ref)
                )),
            })
        }
        TimeSuffix::QoqDelta => {
            Ok(DerivedExpr::Delta {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::PriorQuarter {
                        measure: measure_ref.to_string(),
                        via: None,
                    }
                )),
            })
        }
        TimeSuffix::MomDelta => {
            Ok(DerivedExpr::Delta {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::prior_period(measure_ref, 1)
                )),
            })
        }
        TimeSuffix::WowDelta => {
            Ok(DerivedExpr::Delta {
                current: Box::new(DerivedExpr::MeasureRef(measure_ref.to_string())),
                previous: Box::new(DerivedExpr::TimeFunction(
                    TimeFunction::prior_period(measure_ref, 1)
                )),
            })
        }
        
        // === Rolling Windows ===
        TimeSuffix::Rolling3m => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::rolling_sum(measure_ref, 3)))
        }
        TimeSuffix::Rolling6m => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::RollingSum {
                measure: measure_ref.to_string(),
                periods: 6,
                via: None,
            }))
        }
        TimeSuffix::Rolling12m => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::RollingSum {
                measure: measure_ref.to_string(),
                periods: 12,
                via: None,
            }))
        }
        TimeSuffix::Rolling3mAvg => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::rolling_avg(measure_ref, 3)))
        }
        TimeSuffix::Rolling6mAvg => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::RollingAvg {
                measure: measure_ref.to_string(),
                periods: 6,
                via: None,
            }))
        }
        TimeSuffix::Rolling12mAvg => {
            Ok(DerivedExpr::TimeFunction(TimeFunction::RollingAvg {
                measure: measure_ref.to_string(),
                periods: 12,
                via: None,
            }))
        }
    }
}
```

### Testing Strategy for Time Suffixes

Each suffix needs:

1. **Unit test** - Verify correct TimeFunction/DerivedExpr construction
2. **SQL generation test** - Verify correct SQL output
3. **Integration test** - Full DSL → SQL with actual data

**Test Template:**
```rust
#[test]
fn test_time_suffix_yoy_growth() {
    let report = Report::parse(r#"
        report test {
            from fact_sales;
            group { dates.standard.month }
            show { revenue.yoy_growth }
        }
    "#).unwrap();
    
    let query = report.to_semantic_query(&model).unwrap();
    
    // Verify structure
    assert_eq!(query.derived.len(), 1);
    assert!(matches!(query.derived[0].expression, DerivedExpr::Growth { .. }));
    
    // Verify SQL
    let sql = planner.plan(&query).unwrap().to_sql(Dialect::DuckDb);
    assert!(sql.contains("LAG(revenue, 12)"));
    assert!(sql.contains("/ NULLIF("));
    assert!(sql.contains("* 100"));
}
```

---

## Multi-Grain Query Validation

Before implementing explicit grain resolution logic, validate whether the existing symmetric aggregate planner handles multi-grain scenarios correctly.

### Test Scenarios

#### Scenario 1: Daily + Monthly Tables
```rust
table daily_sales {
    source "fact_sales";
    times { order_date_id -> dates.day }
    atoms { revenue decimal }
}

table monthly_budget {
    source "fact_budget";
    times { month_id -> dates.month }
    atoms { budget decimal }
}

report test_multi_grain {
    from daily_sales, monthly_budget;
    group { dates.standard.month }
    show { daily_sales.revenue, monthly_budget.budget }
}
```

**Expected Behavior:**
- `daily_sales` should aggregate to month level (SUM by month)
- `monthly_budget` stays at month level (already at target grain)
- Both join on month grain via symmetric aggregate

**SQL Pattern Expected:**
```sql
WITH sales_agg AS (
    SELECT 
        dates.month_start_date,
        SUM(daily_sales.revenue) as revenue
    FROM fact_sales
    JOIN dim_date dates ON daily_sales.order_date_id = dates.date_key
    GROUP BY dates.month_start_date
),
budget_agg AS (
    SELECT 
        dates.month_start_date,
        SUM(monthly_budget.budget) as budget
    FROM fact_budget
    JOIN dim_date dates ON monthly_budget.month_id = dates.month_key  -- Needs month grain join!
    GROUP BY dates.month_start_date
)
SELECT 
    COALESCE(s.month_start_date, b.month_start_date) as month,
    s.revenue,
    b.budget
FROM sales_agg s
FULL OUTER JOIN budget_agg b ON s.month_start_date = b.month_start_date
```

#### Scenario 2: Different Grouping Grain
```rust
report test_day_grouping {
    from daily_sales, monthly_budget;
    group { dates.standard.day }  // Finer than monthly_budget grain!
    show { daily_sales.revenue, monthly_budget.budget }
}
```

**Expected Behavior:**
- `daily_sales` stays at day level
- `monthly_budget` needs to fan out to day level (1 budget row → N days in that month)
- This is trickier - may need explicit broadcast join

**Potential Issue:**
If monthly_budget has $100 for January and there are 31 days:
- Should each day show $100? (broadcast)
- Should it be $100/31 per day? (prorate)
- Should it error? (grain mismatch)

**Resolution:** Check if existing planner handles this or errors appropriately.

#### Scenario 3: Incompatible Grains
```rust
table weekly_sales {
    times { week_id -> dates.week }
}

table monthly_budget {
    times { month_id -> dates.month }
}

report test_week_month {
    from weekly_sales, monthly_budget;
    group { dates.standard.week }
    show { weekly_sales.revenue, monthly_budget.budget }
}
```

**Expected Behavior:**
- Weeks don't align with months (week 5 spans Feb/Mar boundary)
- This should probably error: "Cannot resolve grain between week and month"

### Validation Tasks

Add to Step 6 (Map DSL Reports to SemanticQuery):

1. **Test existing planner with multi-grain tables**
   - Create test cases for Scenario 1 (daily + monthly)
   - Execute via existing `QueryPlanner`
   - Verify SQL output and behavior

2. **Document grain resolution behavior**
   - If it works: Document how symmetric aggregates handle it
   - If it fails: Add grain resolution as Phase 2 requirement

3. **Add grain validation if needed**
   - Detect when tables have incompatible grains
   - Error with helpful message: "Cannot mix week and month grains without explicit conversion"

4. **Test fan-out behavior**
   - If coarse grain fans out to fine grain (monthly → daily)
   - Verify broadcast behavior is correct
   - Document limitations

### Implementation If Validation Fails

If existing planner doesn't handle multi-grain correctly, add grain resolution logic:

```rust
/// Resolve target grain for multi-table query.
fn resolve_target_grain(
    tables: &[&Table],
    group_by: &[GroupItem],
    model: &Model,
) -> Result<GrainLevel, GrainResolutionError> {
    // 1. Extract grain from group_by (e.g., dates.standard.month → Month)
    let target_grain = extract_grain_from_group_by(group_by, model)?;
    
    // 2. For each table, check if its time binding is compatible
    for table in tables {
        let table_grain = table.primary_time_binding()?.grain;
        
        // Can we aggregate from table_grain to target_grain?
        if !can_aggregate(table_grain, target_grain) {
            return Err(GrainResolutionError::IncompatibleGrain {
                table: table.name.clone(),
                table_grain,
                target_grain,
            });
        }
    }
    
    Ok(target_grain)
}

/// Check if we can aggregate from source_grain to target_grain.
fn can_aggregate(source: GrainLevel, target: GrainLevel) -> bool {
    // Can only aggregate from finer to coarser grain
    // Day → Week, Month, Quarter, Year ✓
    // Week → Month? ✗ (not aligned)
    // Week → Year? ✗ (not aligned)
    
    use GrainLevel::*;
    match (source, target) {
        // Same grain is fine
        (s, t) if s == t => true,
        
        // Standard hierarchies
        (Day, Week | Month | Quarter | Year) => true,
        (Week, Year) => true, // Assuming ISO weeks
        (Month, Quarter | Year) => true,
        (Quarter, Year) => true,
        
        // Fiscal hierarchies
        (Day, FiscalMonth | FiscalQuarter | FiscalYear) => true,
        (FiscalMonth, FiscalQuarter | FiscalYear) => true,
        (FiscalQuarter, FiscalYear) => true,
        
        // Cross-hierarchy is problematic
        (Week, Month | Quarter) => false, // Weeks don't align with months
        (FiscalMonth, Quarter | Year) => false, // Fiscal ≠ standard
        (Month, FiscalQuarter | FiscalYear) => false,
        
        // Reverse (coarse to fine) requires fan-out
        _ => false, // Default: can't aggregate coarse to fine
    }
}
```

**Decision Point:** Only implement this if validation reveals the existing planner doesn't handle it.

---

## Filter Routing: WHERE vs HAVING

The DSL spec requires automatic routing of filter predicates based on what they reference:
- **WHERE clause**: Filters referencing atoms (pre-aggregation) or slicers (dimensions)
- **HAVING clause**: Filters referencing measures (post-aggregation)

### Implementation Strategy

**Decision:** Filter routing happens in the **query planner**, not during lowering or report translation.

**Rationale:**
1. The planner already understands aggregation context
2. The planner knows which expressions are measures vs atoms
3. Keeps the semantic model simple (filters are just expressions)
4. Allows for complex filters that reference both (split into WHERE + HAVING)

### Example Scenarios

#### Scenario 1: Simple Atom Filter
```rust
report test {
    from fact_sales;
    filter { @amount > 100 }  // References atom
    show { revenue }
}
```

**Routing:** WHERE clause (pre-aggregation)

**SQL:**
```sql
SELECT SUM(fact_sales.amount) as revenue
FROM fact_sales
WHERE fact_sales.amount > 100  -- ← Routed to WHERE
GROUP BY ...
```

#### Scenario 2: Measure Filter
```rust
report test {
    from fact_sales;
    filter { revenue > 1000 }  // References measure
    show { revenue }
}
```

**Routing:** HAVING clause (post-aggregation)

**SQL:**
```sql
SELECT SUM(fact_sales.amount) as revenue
FROM fact_sales
GROUP BY ...
HAVING SUM(fact_sales.amount) > 1000  -- ← Routed to HAVING
```

#### Scenario 3: Mixed Filter
```rust
report test {
    from fact_sales;
    filter { @amount > 100 AND revenue > 1000 }  // References both!
    show { revenue }
}
```

**Routing:** Split into WHERE + HAVING

**SQL:**
```sql
SELECT SUM(fact_sales.amount) as revenue
FROM fact_sales
WHERE fact_sales.amount > 100  -- ← Atom filter to WHERE
GROUP BY ...
HAVING SUM(fact_sales.amount) > 1000  -- ← Measure filter to HAVING
```

#### Scenario 4: Slicer Filter
```rust
report test {
    from fact_sales;
    filter { region = 'EMEA' }  // References slicer (dimension)
    show { revenue }
}
```

**Routing:** WHERE clause (dimension filter)

**SQL:**
```sql
SELECT SUM(fact_sales.amount) as revenue
FROM fact_sales
JOIN dim_customers ON fact_sales.customer_id = dim_customers.customer_id
WHERE dim_customers.region = 'EMEA'  -- ← Routed to WHERE
GROUP BY ...
```

### Implementation in Query Planner

Add filter routing logic to `src/semantic/planner/mod.rs`:

```rust
impl QueryPlanner<'_> {
    /// Route filters to WHERE or HAVING clauses based on content.
    fn route_filters(
        &self,
        filters: Vec<FieldFilter>,
        select_fields: &[SelectField],
    ) -> Result<(Vec<Expr>, Vec<Expr>), PlanError> {
        let mut where_filters = Vec::new();
        let mut having_filters = Vec::new();
        
        for filter in filters {
            if self.is_post_aggregation_filter(&filter, select_fields)? {
                // References a measure → HAVING
                having_filters.push(self.lower_filter_to_expr(&filter)?);
            } else {
                // References an atom or slicer → WHERE
                where_filters.push(self.lower_filter_to_expr(&filter)?);
            }
        }
        
        Ok((where_filters, having_filters))
    }
    
    /// Check if a filter references post-aggregation expressions (measures).
    fn is_post_aggregation_filter(
        &self,
        filter: &FieldFilter,
        select_fields: &[SelectField],
    ) -> Result<bool, PlanError> {
        // Look up the field being filtered
        let field_ref = &filter.field;
        
        // Check if it's in the select list and is a measure
        for select_field in select_fields {
            if select_field.field.entity == field_ref.entity 
               && select_field.field.field == field_ref.field {
                // Found it - check if it's aggregated
                return Ok(select_field.is_aggregate());
            }
        }
        
        // Not in select list - look up in model
        let table = self.model.tables.get(&field_ref.entity)
            .ok_or_else(|| PlanError::UnknownTable(field_ref.entity.clone()))?;
        
        // Check if it's an atom (pre-aggregation)
        if table.atoms.contains_key(&field_ref.field) {
            return Ok(false); // Atom → WHERE
        }
        
        // Check if it's a slicer (dimension)
        if table.slicers.contains_key(&field_ref.field) {
            return Ok(false); // Slicer → WHERE
        }
        
        // Must be a measure (post-aggregation)
        Ok(true)
    }
    
    /// Handle complex filters that reference both atoms and measures.
    fn split_complex_filter(
        &self,
        filter: &FilterExpr,
    ) -> Result<(Option<FilterExpr>, Option<FilterExpr>), PlanError> {
        // Parse filter expression and identify subexpressions
        // Split into WHERE-eligible and HAVING-eligible parts
        // This is more complex - requires expression tree walking
        
        match filter {
            FilterExpr::And(left, right) => {
                // Recursively split both sides
                let (left_where, left_having) = self.split_complex_filter(left)?;
                let (right_where, right_having) = self.split_complex_filter(right)?;
                
                // Combine WHERE parts with AND
                let where_part = match (left_where, right_where) {
                    (Some(l), Some(r)) => Some(FilterExpr::And(Box::new(l), Box::new(r))),
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                };
                
                // Combine HAVING parts with AND
                let having_part = match (left_having, right_having) {
                    (Some(l), Some(r)) => Some(FilterExpr::And(Box::new(l), Box::new(r))),
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                };
                
                Ok((where_part, having_part))
            }
            
            FilterExpr::Or(left, right) => {
                // OR is tricky - can only split if both sides go to same clause
                let left_is_having = self.references_measure(left)?;
                let right_is_having = self.references_measure(right)?;
                
                if left_is_having == right_is_having {
                    // Both go to same clause
                    if left_is_having {
                        Ok((None, Some(filter.clone())))
                    } else {
                        Ok((Some(filter.clone()), None))
                    }
                } else {
                    // Can't split OR across WHERE/HAVING boundary
                    Err(PlanError::UnsplittableFilter {
                        filter: filter.to_string(),
                        reason: "OR expression mixes pre- and post-aggregation predicates".to_string(),
                    })
                }
            }
            
            _ => {
                // Simple predicate - check if it references a measure
                if self.references_measure(filter)? {
                    Ok((None, Some(filter.clone())))
                } else {
                    Ok((Some(filter.clone()), None))
                }
            }
        }
    }
}
```

### Edge Cases

#### 1. Filtered Measures
```rust
measures fact_sales {
    enterprise_revenue = { sum(@amount) } where { segment = 'Enterprise' };
}

report test {
    filter { enterprise_revenue > 1000 }
}
```

**Routing:** HAVING (references a measure, even though measure has internal filter)

#### 2. Window Functions
```rust
report test {
    filter { revenue.ytd > 10000 }  // YTD is a window function
}
```

**Routing:** HAVING (or outer WHERE if using nested query)

**Note:** Window functions need special handling - may require nested query structure:
```sql
SELECT * FROM (
    SELECT revenue, SUM(revenue) OVER (...) as revenue_ytd
    FROM ...
) sub
WHERE revenue_ytd > 10000  -- ← Outer WHERE on window function result
```

#### 3. Calendar Grain Filters
```rust
report test {
    filter { dates.standard.year = 2024 }  // References calendar dimension
}
```

**Routing:** WHERE (dimension filter, even though it's from calendar)

### Testing Strategy

Each routing scenario needs tests:

```rust
#[test]
fn test_filter_routing_atom() {
    let report = parse_report(r#"
        report test {
            from fact_sales;
            filter { @amount > 100 }
            show { revenue }
        }
    "#);
    
    let query = report.to_semantic_query(&model).unwrap();
    let (where_filters, having_filters) = planner.route_filters(query.filters).unwrap();
    
    assert_eq!(where_filters.len(), 1);
    assert_eq!(having_filters.len(), 0);
}

#[test]
fn test_filter_routing_measure() {
    let report = parse_report(r#"
        report test {
            from fact_sales;
            filter { revenue > 1000 }
            show { revenue }
        }
    "#);
    
    let query = report.to_semantic_query(&model).unwrap();
    let (where_filters, having_filters) = planner.route_filters(query.filters).unwrap();
    
    assert_eq!(where_filters.len(), 0);
    assert_eq!(having_filters.len(), 1);
}

#[test]
fn test_filter_routing_mixed() {
    let report = parse_report(r#"
        report test {
            from fact_sales;
            filter { @amount > 100 AND revenue > 1000 }
            show { revenue }
        }
    "#);
    
    let query = report.to_semantic_query(&model).unwrap();
    let (where_filters, having_filters) = planner.route_filters(query.filters).unwrap();
    
    assert_eq!(where_filters.len(), 1);  // @amount > 100
    assert_eq!(having_filters.len(), 1); // revenue > 1000
}
```

---

## Circular Dependency Detection for Measures

Measures can reference other measures, creating a dependency graph. Circular dependencies must be detected during validation.

### Example Circular Dependencies

```rust
measures fact_sales {
    a = { b + 1 };      // a → b
    b = { c * 2 };      // b → c
    c = { a - 3 };      // c → a  ← CYCLE!
}

measures fact_orders {
    margin = { revenue - cost };     // margin → revenue, cost
    revenue = { quantity * price };  // revenue → quantity, price (OK - no cycle)
    cost = { margin * 0.3 };         // cost → margin  ← CYCLE with margin!
}
```

### Implementation Strategy

Use **depth-first search (DFS) with path tracking** to detect cycles.

```rust
/// Detect circular dependencies in measure definitions.
pub struct MeasureDependencyValidator<'a> {
    model: &'a Model,
    /// Dependencies: measure_name → [referenced_measures]
    dependencies: HashMap<String, Vec<String>>,
    /// Current DFS path for cycle detection
    path: Vec<String>,
    /// Visited set for cycle detection
    visiting: HashSet<String>,
    visited: HashSet<String>,
}

impl<'a> MeasureDependencyValidator<'a> {
    pub fn validate(model: &'a Model) -> Result<(), ValidationError> {
        let mut validator = Self {
            model,
            dependencies: HashMap::new(),
            path: Vec::new(),
            visiting: HashSet::new(),
            visited: HashSet::new(),
        };
        
        // 1. Build dependency graph
        validator.build_dependency_graph()?;
        
        // 2. Check for cycles using DFS
        for measure_block in model.measures.values() {
            for measure_name in measure_block.measures.keys() {
                let full_name = format!("{}.{}", measure_block.table_name, measure_name);
                if !validator.visited.contains(&full_name) {
                    validator.check_cycles(&full_name)?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Build the measure dependency graph.
    fn build_dependency_graph(&mut self) -> Result<(), ValidationError> {
        for measure_block in self.model.measures.values() {
            for (measure_name, measure) in &measure_block.measures {
                let full_name = format!("{}.{}", measure_block.table_name, measure_name);
                
                // Extract measure references from expression
                let refs = self.extract_measure_references(&measure.expr, &measure_block.table_name)?;
                
                self.dependencies.insert(full_name, refs);
            }
        }
        
        Ok(())
    }
    
    /// Extract measure references from an expression.
    fn extract_measure_references(
        &self,
        expr: &SqlExpr,
        table_name: &str,
    ) -> Result<Vec<String>, ValidationError> {
        let mut references = Vec::new();
        
        // Parse SQL expression to find identifiers
        // Identifiers without @ are potential measure references
        let ident_pattern = regex::Regex::new(r"\b(?!@)([a-z_][a-z0-9_]*)\b").unwrap();
        
        for cap in ident_pattern.captures_iter(&expr.sql) {
            let ident = &cap[1];
            
            // Check if this identifier is a measure in the same table
            if let Some(measure_block) = self.model.measures.get(table_name) {
                if measure_block.measures.contains_key(ident) {
                    let full_name = format!("{}.{}", table_name, ident);
                    references.push(full_name);
                }
            }
            
            // Could also be table.measure reference - would need more sophisticated parsing
        }
        
        Ok(references)
    }
    
    /// Check for cycles using DFS.
    fn check_cycles(&mut self, measure: &str) -> Result<(), ValidationError> {
        // Mark as currently visiting
        self.visiting.insert(measure.to_string());
        self.path.push(measure.to_string());
        
        // Check dependencies
        if let Some(deps) = self.dependencies.get(measure) {
            for dep in deps {
                if self.visiting.contains(dep) {
                    // Found a cycle! Build error message with path
                    let mut cycle_path = self.path.clone();
                    cycle_path.push(dep.clone());
                    
                    return Err(ValidationError::CircularMeasureReference {
                        cycle: cycle_path,
                    });
                }
                
                if !self.visited.contains(dep) {
                    self.check_cycles(dep)?;
                }
            }
        }
        
        // Done visiting
        self.visiting.remove(measure);
        self.visited.insert(measure.to_string());
        self.path.pop();
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ValidationError {
    // ... other errors
    
    /// Circular dependency detected in measure definitions.
    CircularMeasureReference {
        /// The cycle path: ["table.a", "table.b", "table.c", "table.a"]
        cycle: Vec<String>,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::CircularMeasureReference { cycle } => {
                write!(f, "Circular dependency in measures: ")?;
                for (i, measure) in cycle.iter().enumerate() {
                    if i > 0 {
                        write!(f, " → ")?;
                    }
                    write!(f, "{}", measure)?;
                }
                Ok(())
            }
            // ... other errors
        }
    }
}
```

### Error Message Example

```
Error: Circular dependency in measures: fact_sales.margin → fact_sales.revenue → fact_sales.cost → fact_sales.margin
  ┌─ sales_model.mantis:45:15
  │
45│     margin = { revenue - cost };
  │                ^^^^^^^ references revenue
  │
  = note: revenue references cost at line 46
  = note: cost references margin at line 47
```

### Testing

```rust
#[test]
fn test_circular_dependency_direct() {
    let model = parse_model(r#"
        measures fact_sales {
            a = { b + 1 };
            b = { a * 2 };  // ← cycle!
        }
    "#).unwrap();
    
    let result = MeasureDependencyValidator::validate(&model);
    assert!(matches!(result, Err(ValidationError::CircularMeasureReference { .. })));
}

#[test]
fn test_circular_dependency_transitive() {
    let model = parse_model(r#"
        measures fact_sales {
            a = { b + 1 };
            b = { c + 2 };
            c = { a + 3 };  // ← cycle via b!
        }
    "#).unwrap();
    
    let result = MeasureDependencyValidator::validate(&model);
    assert!(matches!(result, Err(ValidationError::CircularMeasureReference { .. })));
}

#[test]
fn test_no_circular_dependency() {
    let model = parse_model(r#"
        measures fact_sales {
            revenue = { sum(@amount) };
            cost = { sum(@cost_amount) };
            margin = { revenue - cost };  // OK - DAG
        }
    "#).unwrap();
    
    let result = MeasureDependencyValidator::validate(&model);
    assert!(result.is_ok());
}
```

---

## Testing Strategy

Phase 2 requires comprehensive testing across multiple levels.

### 1. Unit Tests

**Per Module:**
- `src/model/` - Type construction, validation
- `src/lowering/` - DSL AST → Model translation
- `src/semantic/model_graph/` - Graph construction from new model
- `src/semantic/column_lineage/` - Lineage tracking with @atom syntax
- `src/semantic/planner/` - Filter routing, time suffix expansion

**Coverage Target:** 85%+ line coverage per module

### 2. Integration Tests

**Full Pipeline Tests:**
```rust
#[test]
fn test_full_pipeline_simple_report() {
    // 1. Parse DSL
    let ast = parse_dsl(r#"
        calendar dates "dbo.dim_date" { ... }
        table fact_sales { ... }
        measures fact_sales { revenue = { sum(@amount) }; }
        report test {
            from fact_sales;
            group { dates.standard.month }
            show { revenue }
        }
    "#).unwrap();
    
    // 2. Lower to model
    let model = lower_ast_to_model(ast).unwrap();
    
    // 3. Validate
    model.validate().unwrap();
    
    // 4. Build semantic model
    let semantic_model = SemanticModel::new(model).unwrap();
    
    // 5. Translate report to query
    let report = &semantic_model.model.reports["test"];
    let query = report.to_semantic_query(&semantic_model.model).unwrap();
    
    // 6. Plan query
    let planner = QueryPlanner::new(&semantic_model);
    let sql = planner.plan(&query).unwrap();
    
    // 7. Verify SQL
    let sql_string = sql.to_sql(Dialect::DuckDb);
    assert!(sql_string.contains("SUM(fact_sales.amount)"));
    assert!(sql_string.contains("dates.month_start_date"));
}
```

### 3. Golden File Tests

**SQL Generation:**
- Store expected SQL outputs for various DSL inputs
- Compare generated SQL against golden files
- Catch unintended changes to SQL generation

```rust
#[test]
fn test_golden_files() {
    for entry in glob("tests/golden/**/*.mantis").unwrap() {
        let dsl_path = entry.unwrap();
        let sql_path = dsl_path.with_extension("sql");
        
        // Parse and generate SQL
        let ast = parse_dsl_file(&dsl_path).unwrap();
        let model = lower_ast_to_model(ast).unwrap();
        let semantic = SemanticModel::new(model).unwrap();
        let sql = generate_sql(&semantic).unwrap();
        
        // Compare with golden file
        let expected = std::fs::read_to_string(&sql_path).unwrap();
        assert_eq!(sql.trim(), expected.trim(), "Golden file mismatch: {:?}", dsl_path);
    }
}
```

### 4. Property-Based Tests

**Validation Properties:**
- Any valid DSL should lower without panic
- Lowering + validation should be deterministic
- Circular dependencies always detected
- Invalid references always caught

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_lowering_never_panics(dsl_source in arbitrary_dsl()) {
        // Should either succeed or return error, never panic
        let _ = parse_and_lower(&dsl_source);
    }
    
    #[test]
    fn test_validation_deterministic(dsl_source in valid_dsl()) {
        let result1 = parse_lower_validate(&dsl_source);
        let result2 = parse_lower_validate(&dsl_source);
        assert_eq!(result1.is_ok(), result2.is_ok());
    }
}
```

### 5. Error Message Tests

**User-Facing Errors:**
- Verify error messages are helpful
- Check that spans point to correct locations
- Ensure suggestions are provided

```rust
#[test]
fn test_unknown_atom_error() {
    let result = parse_and_validate(r#"
        measures fact_sales {
            revenue = { sum(@revenues) };  // Typo: should be @revenue
        }
    "#);
    
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Unknown atom"));
    assert!(err.to_string().contains("@revenues"));
    assert!(err.span().is_some());
}
```

### 6. Performance Benchmarks

**Scale Tests:**
- Model with 100 tables, 500 measures
- Report with 50 dimensions, 100 measures
- Time intelligence expansion overhead

```rust
#[bench]
fn bench_large_model_validation(b: &mut Bencher) {
    let model = generate_large_model(100, 500);
    b.iter(|| {
        model.validate().unwrap();
    });
}

#[bench]
fn bench_time_suffix_expansion(b: &mut Bencher) {
    let report = create_report_with_time_suffixes(20);
    b.iter(|| {
        report.to_semantic_query(&model).unwrap();
    });
}
```

### Test Organization

```
tests/
├── unit/
│   ├── model/
│   ├── lowering/
│   └── semantic/
├── integration/
│   ├── full_pipeline.rs
│   └── multi_table.rs
├── golden/
│   ├── simple_report.mantis
│   ├── simple_report.sql
│   ├── time_intelligence.mantis
│   └── time_intelligence.sql
├── property/
│   └── validation_properties.rs
└── benchmarks/
    └── scale_tests.rs
```

### Coverage Requirements

**Minimum Coverage:**
- Model types: 90%
- Lowering: 85%
- Validation: 90%
- Report translation: 85%
- Overall: 85%

**Critical Paths (100% coverage required):**
- Circular dependency detection
- Filter routing logic
- Time suffix expansion
- @atom resolution

---

### Step 7: Remove Old Model

Once new model is working and tested:
- Delete old `src/model/` (Lua-based types)
- Update imports throughout codebase
- Remove Lua loader

---

## Example: Full Flow

### DSL Input

```mantis
calendar dates "dbo.dim_date" {
    day = date_key;
    week = week_start_date;
    month = month_start_date;
    quarter = quarter_start_date;
    year = year_start_date;
    
    drill_path standard { day -> week -> month -> quarter -> year };
    
    week_start Monday;
}

dimension customers {
    source "dbo.dim_customers";
    key customer_id;
    attributes {
        customer_name string;
        segment string;
        region string;
    }
    drill_path geo { region };
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
    }
    
    slicers {
        customer_id -> customers.customer_id;
        segment via customer_id;
    }
}

measures fact_sales {
    revenue = { sum(@revenue) };
    cost = { sum(@cost) };
    margin = { revenue - cost };
    margin_pct = { margin / revenue * 100 };
}

report monthly_sales {
    from fact_sales;
    use_date order_date_id;
    period last_12_months;
    
    group {
        dates.standard.month as "Month";
        customers.geo.region as "Region";
    }
    
    show {
        revenue as "Revenue";
        revenue.ytd as "YTD Revenue";
        revenue.yoy_growth as "YoY Growth %";
        margin_pct as "Margin %";
    }
    
    sort dates.standard.month.asc;
}
```

### Lowered SemanticModel

```rust
SemanticModel {
    calendars: {
        "dates": Calendar {
            name: "dates",
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date",
                grain_mappings: {
                    Day: "date_key",
                    Week: "week_start_date",
                    Month: "month_start_date",
                    Quarter: "quarter_start_date",
                    Year: "year_start_date",
                },
                drill_paths: {
                    "standard": DrillPath {
                        name: "standard",
                        levels: [Day, Week, Month, Quarter, Year],
                    },
                },
                week_start: Some(Monday),
            }),
        },
    },
    
    dimensions: {
        "customers": Dimension {
            name: "customers",
            source: "dbo.dim_customers",
            key: "customer_id",
            attributes: {
                "customer_name": Attribute { name: "customer_name", data_type: String },
                "segment": Attribute { name: "segment", data_type: String },
                "region": Attribute { name: "region", data_type: String },
            },
            drill_paths: {
                "geo": DrillPath { name: "geo", levels: [Region] },
            },
        },
    },
    
    tables: {
        "fact_sales": Table {
            name: "fact_sales",
            source: "dbo.fact_sales",
            atoms: {
                "revenue": Atom { name: "revenue", data_type: Decimal },
                "cost": Atom { name: "cost", data_type: Decimal },
                "quantity": Atom { name: "quantity", data_type: Int },
            },
            times: {
                "order_date_id": TimeBinding {
                    name: "order_date_id",
                    calendar: "dates",
                    grain: Day,
                },
            },
            slicers: {
                "customer_id": Slicer::ForeignKey {
                    name: "customer_id",
                    dimension: "customers",
                    key: "customer_id",
                },
                "segment": Slicer::Via {
                    name: "segment",
                    fk_slicer: "customer_id",
                },
            },
        },
    },
    
    measures: {
        "fact_sales": MeasureBlock {
            table_name: "fact_sales",
            measures: {
                "revenue": Measure {
                    name: "revenue",
                    expr: SqlExpr { sql: "sum(@revenue)", span: ... },
                    filter: None,
                    null_handling: None,
                },
                "margin": Measure {
                    name: "margin",
                    expr: SqlExpr { sql: "revenue - cost", span: ... },
                    // Note: @atom syntax NOT used here - this references other measures
                },
                "margin_pct": Measure {
                    expr: SqlExpr { sql: "margin / revenue * 100", span: ... },
                },
            },
        },
    },
    
    reports: {
        "monthly_sales": Report {
            from: ["fact_sales"],
            use_date: ["order_date_id"],
            period: Some(PeriodExpr::Relative(RelativePeriod::Trailing {
                count: 12,
                unit: Months,
            })),
            group: [
                GroupItem::DrillPathRef {
                    source: "dates",
                    path: "standard",
                    level: "month",
                    label: Some("Month"),
                },
                GroupItem::DrillPathRef {
                    source: "customers",
                    path: "geo",
                    level: "region",
                    label: Some("Region"),
                },
            ],
            show: [
                ShowItem::Measure { name: "revenue", label: Some("Revenue") },
                ShowItem::MeasureWithSuffix { name: "revenue", suffix: Ytd, label: Some("YTD Revenue") },
                ShowItem::MeasureWithSuffix { name: "revenue", suffix: YoyGrowth, label: Some("YoY Growth %") },
                ShowItem::Measure { name: "margin_pct", label: Some("Margin %") },
            ],
            sort: [
                SortItem { column: "dates.standard.month", direction: Asc },
            ],
        },
    },
}
```

### Query Planning Output (Pseudo-SQL)

```sql
WITH date_filter AS (
    -- Evaluate period: last_12_months (compile-time)
    SELECT '2025-01-01'::date AS start_date, '2025-12-31'::date AS end_date
),
base_data AS (
    SELECT
        dates.month_start_date AS month,
        customers.region,
        SUM(fact_sales.revenue) AS revenue,
        SUM(fact_sales.cost) AS cost
    FROM dbo.fact_sales
    JOIN dbo.dim_date AS dates ON fact_sales.order_date_id = dates.date_key
    JOIN dbo.dim_customers AS customers ON fact_sales.customer_id = customers.customer_id
    CROSS JOIN date_filter
    WHERE dates.date_key BETWEEN date_filter.start_date AND date_filter.end_date
    GROUP BY dates.month_start_date, customers.region
)
SELECT
    month AS "Month",
    region AS "Region",
    revenue AS "Revenue",
    SUM(revenue) OVER (
        PARTITION BY dates.year_start_date 
        ORDER BY month 
        ROWS UNBOUNDED PRECEDING
    ) AS "YTD Revenue",
    ((revenue - LAG(revenue, 12) OVER (ORDER BY month)) / 
     LAG(revenue, 12) OVER (ORDER BY month)) * 100 AS "YoY Growth %",
    ((revenue - cost) / revenue * 100) AS "Margin %"
FROM base_data
JOIN dbo.dim_date AS dates ON base_data.month = dates.month_start_date
ORDER BY month ASC
```

---

## Success Criteria

Phase 2 is complete when:

1. **DSL AST can be lowered to SemanticModel** - All DSL constructs map to semantic model types
2. **Validation catches errors** - Undefined calendars, atoms, measures, dimensions detected
3. **Circular dependencies detected** - Measure-to-measure cycles caught with clear error messages
4. **@atom syntax preserved and resolved** - Measure expressions keep @atom through to SQL generation
5. **Reports map to SemanticQuery** - Report::to_semantic_query() correctly translates all DSL report features
6. **All 24 time suffixes work** - Every time suffix (ytd, qtd, mtd, wtd, fiscal variants, growth, delta, rolling) expands correctly
7. **Filter routing works** - WHERE vs HAVING routing based on atom/slicer vs measure references
8. **Drill paths resolve** - Drill path refs correctly resolve to calendar/dimension columns
9. **Period expressions evaluate** - Period expressions compile to date range filters
10. **Multi-grain queries validated** - Existing planner tested with multi-grain tables, grain resolution documented or implemented
11. **Reports execute via planner** - DSL reports execute through existing QueryPlanner infrastructure
12. **Multi-source reports work** - Symmetric aggregates handle cross-table reports correctly
13. **Tests pass** - 85%+ coverage with unit, integration, golden file, property-based, and benchmark tests

---

## Open Questions

1. **Generated Calendar SQL:** How to emit the CTE for generated calendars?
   - Use existing date spine generation code?
   - Build new date_spine() generator?
   - DuckDB's `generate_series()` + cross joins for multi-grain?

2. **Fiscal Calendar Logic:** How to compute fiscal periods?
   - Date arithmetic in SQL (DATEADD with fiscal offset)?
   - Pre-compute fiscal mappings in physical calendar?
   - Generate fiscal columns in generated calendar CTE?

3. **Time Suffix to TimeFunction Mapping:** Complete all 20+ time suffixes?
   - Some may need new TimeFunction variants
   - Growth/delta may need DerivedExpr::Growth/DerivedExpr::Delta wrappers
   - Fiscal variants need fiscal calendar awareness

4. **NULL Handling in Division:** Where to apply coalesce wrapping?
   - During measure resolution? (preserve @atom clarity)
   - During SQL emission? (closer to actual SQL generation)
   - Model-wide default vs per-measure override?

5. **Period Expression Evaluation:** Compile-time vs runtime evaluation?
   - `last_12_months` needs current date - evaluated when?
   - Absolute ranges (`range(2024-01-01, 2024-12-31)`) are compile-time safe
   - Support relative ranges in query parameters?

6. **Backward Compatibility:** Phase out old Lua model or maintain parallel paths?
   - Clean break: delete old model, DSL only
   - Migration period: support both, translate Lua → DSL
   - Long-term: DSL is source of truth

---

## Next Steps

After Phase 2 design is approved:

1. **Implement new semantic model types** (`src/model/`)
2. **Implement lowering** (`src/lowering/mod.rs`)
3. **Implement validation** (`src/model/mod.rs`)
   - Calendar/dimension/atom reference validation
   - Circular dependency detection (DFS-based)
4. **Extend measure resolution** for @atom syntax preservation
5. **Implement Report::to_semantic_query()** translation layer
6. **Add all 24 time suffix mappings** to TimeFunction/DerivedExpr
7. **Implement filter routing** (WHERE vs HAVING) in query planner
8. **Test multi-grain queries** with existing planner, implement grain resolution if needed
9. **Update ModelGraph and ColumnLineageGraph** with new `from_model()` implementations
10. **Write comprehensive tests** (unit, integration, golden file, property-based, benchmarks)
11. **Document validation errors** with span-based error messages

This sets the foundation for Phase 3 (Report Execution) and Phase 4 (Typst Output).

---

## Summary of Architectural Review Additions

Based on the architectural review, the following high and medium priority items were added to Phase 2:

### High Priority (ADDED)

1. **Complete Time Suffix Implementation** - All 24 suffixes mapped to TimeFunction/DerivedExpr
   - 6 accumulations (ytd, qtd, mtd, wtd, fiscal_ytd, fiscal_qtd)
   - 4 prior periods (prior_year, prior_quarter, prior_month, prior_week)
   - 4 growth calculations (yoy_growth, qoq_growth, mom_growth, wow_growth)
   - 4 delta calculations (yoy_delta, qoq_delta, mom_delta, wow_delta)
   - 6 rolling windows (rolling_3m, rolling_6m, rolling_12m + _avg variants)
   - Only 3 new TimeFunction variants needed (WeekToDate, FiscalYearToDate, FiscalQuarterToDate)

2. **Multi-Grain Query Validation** - Test-driven approach to grain resolution
   - Test daily + monthly table scenarios
   - Document existing planner behavior
   - Implement grain resolution only if validation fails
   - Handle incompatible grains (week + month) with clear errors

3. **Filter Routing Implementation** - WHERE vs HAVING logic in query planner
   - Automatic routing based on atom/slicer vs measure references
   - Complex filter splitting (AND can split, OR must error)
   - Edge cases: filtered measures, window functions, calendar filters

### Medium Priority (ADDED)

4. **Circular Dependency Detection** - DFS-based cycle detection with path tracking
   - Build measure dependency graph
   - Detect cycles in measure-to-measure references
   - Clear error messages showing full cycle path

5. **Comprehensive Testing Strategy** - Multiple testing levels
   - Unit tests (85%+ coverage per module)
   - Integration tests (full DSL → SQL pipeline)
   - Golden file tests (SQL generation regression)
   - Property-based tests (validation properties)
   - Error message tests (span-based errors)
   - Performance benchmarks (scale tests)

### Not Included (Per User Request)

- ~~Migration strategy from old model~~ - Clean break, no migration path needed
