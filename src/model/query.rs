//! Query definitions - semantic queries defined in the model.
//!
//! Queries defined in the model can be executed against the semantic layer
//! to produce SQL for a specific dialect.

use serde::{Deserialize, Serialize};

use super::Model;
use crate::semantic::error::SemanticError;
use crate::semantic::planner::types::{
    DerivedBinaryOp, DerivedExpr, DerivedField, FieldFilter, FieldRef, FilterOp, FilterValue,
    OrderField, SelectField, SemanticQuery, TimeFunction,
};

/// A query definition in the model.
///
/// This represents a semantic query as defined in Lua:
///
/// ```lua
/// query "sales_by_region" {
///     from = "orders",  -- Optional: can be inferred from measures
///     select = {
///         customers.region,
///         customers.segment,
///         "revenue",
///         "order_count",
///     },
///     where = {
///         gte(date.year, 2024),
///         eq(customers.segment, "Enterprise"),
///     },
///     order_by = { desc("revenue") },
///     limit = 100,
/// }
/// ```
///
/// ## Multi-Fact Queries
///
/// When `from` is omitted and measures come from multiple facts,
/// the query generates a symmetric aggregate pattern with CTEs:
///
/// ```lua
/// query "cross_fact_analysis" {
///     -- No 'from' - anchors inferred from measures
///     select = {
///         customers.region,
///         measure "orders.revenue",      -- From orders fact
///         measure "returns.return_amt",  -- From returns fact
///     },
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinition {
    /// Query name (unique identifier).
    pub name: String,

    /// Anchor entity to query from.
    ///
    /// Optional - if not specified, anchor facts are inferred from the
    /// measures in the query. When measures come from multiple facts,
    /// a symmetric aggregate pattern (CTEs + FULL OUTER JOIN) is used.
    pub from: Option<String>,

    /// Columns/dimensions to select.
    ///
    /// Can be:
    /// - Dimension references: "entity.column"
    /// - Measure names: "revenue" (resolved from anchor fact)
    pub select: Vec<QuerySelect>,

    /// Filter conditions (WHERE clause).
    pub filters: Vec<QueryFilter>,

    /// Parsed SQL filter expressions (WHERE clause).
    /// These are SQL expressions parsed from strings like "year >= 2023".
    #[serde(default, skip)]
    pub filter_exprs: Vec<super::Expr>,

    /// Group by columns.
    ///
    /// Dimensions that the results are grouped by.
    pub group_by: Vec<String>,

    /// Order by specifications.
    pub order_by: Vec<QueryOrderBy>,

    /// Maximum number of rows to return.
    pub limit: Option<u64>,

    /// Number of rows to skip.
    pub offset: Option<u64>,

    /// Optional description.
    pub description: Option<String>,
}

impl QueryDefinition {
    /// Create a new query definition with an explicit anchor.
    pub fn new(name: impl Into<String>, from: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            from: Some(from.into()),
            select: Vec::new(),
            filters: Vec::new(),
            filter_exprs: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            description: None,
        }
    }

    /// Create a new query definition without an explicit anchor.
    ///
    /// The anchor facts will be inferred from the measures in the query.
    /// Use this for multi-fact queries where measures come from different facts.
    pub fn new_inferred(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            from: None,
            select: Vec::new(),
            filters: Vec::new(),
            filter_exprs: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            description: None,
        }
    }

    /// Convert to a SemanticQuery for execution.
    ///
    /// Note: This basic conversion doesn't validate measures. Use
    /// `to_semantic_query_with_model` for full validation.
    pub fn to_semantic_query(&self) -> SemanticQuery {
        let mut select = Vec::new();
        let mut group_by = Vec::new();
        let mut derived = Vec::new();

        // Default entity for bare measure references (uses anchor if specified)
        let default_entity = self.from.as_deref().unwrap_or("");

        for sel in &self.select {
            match sel {
                QuerySelect::Dimension { entity, column } => {
                    select.push(SelectField::new(entity, column));
                    // Dimensions are automatically added to GROUP BY
                    group_by.push(FieldRef::new(entity, column));
                }
                QuerySelect::Measure {
                    entity,
                    name,
                    alias,
                } => {
                    // For measures, use explicit entity if specified, otherwise default
                    let measure_entity = entity.as_deref().unwrap_or(default_entity);
                    let mut field = SelectField::aggregate(measure_entity, name, "");
                    field.aggregation = None; // Will be resolved by planner
                    if let Some(a) = alias {
                        field = field.with_alias(a);
                    }
                    select.push(field);
                }
                QuerySelect::FilteredMeasure {
                    entity,
                    name,
                    alias,
                    filters,
                } => {
                    let measure_filters: Vec<FieldFilter> = filters
                        .iter()
                        .map(|f| f.to_field_filter())
                        .collect();

                    // Use explicit entity if specified, otherwise default
                    let measure_entity = entity.as_deref().unwrap_or(default_entity);
                    let mut field = SelectField::aggregate(measure_entity, name, "")
                        .with_filter(measure_filters);
                    field.aggregation = None; // Will be resolved by planner

                    let output_alias = alias.as_deref().unwrap_or(name);
                    field = field.with_alias(output_alias);
                    select.push(field);
                }
                QuerySelect::DerivedMeasure { alias, expression } => {
                    // Basic conversion without model validation
                    if let Some(expr) = self.try_convert_derived_expression_basic(expression) {
                        derived.push(DerivedField::new(alias, expr));
                    }
                }
            }
        }

        // Add explicit group_by entries
        for gb in &self.group_by {
            if let Some((entity, column)) = gb.split_once('.') {
                if !group_by.iter().any(|g| g.entity == entity && g.field == column) {
                    group_by.push(FieldRef::new(entity, column));
                }
            }
        }

        let filters = self.filters.iter().map(|f| f.to_field_filter()).collect();

        let order_by = self
            .order_by
            .iter()
            .map(|o| {
                if let Some((entity, column)) = o.field.split_once('.') {
                    OrderField {
                        field: FieldRef::new(entity, column),
                        descending: o.descending,
                    }
                } else {
                    // Bare field name - use anchor entity if available
                    OrderField {
                        field: FieldRef::new(default_entity, &o.field),
                        descending: o.descending,
                    }
                }
            })
            .collect();

        SemanticQuery {
            from: self.from.clone(),
            filters,
            group_by,
            select,
            derived,
            order_by,
            limit: self.limit,
        }
    }

    /// Basic conversion of derived expressions without model validation.
    fn try_convert_derived_expression_basic(&self, expr: &DerivedExpression) -> Option<DerivedExpr> {
        match expr {
            DerivedExpression::MeasureRef(name) => Some(DerivedExpr::MeasureRef(name.clone())),
            DerivedExpression::ColumnRef { .. } => None, // Not supported without model
            DerivedExpression::Literal(value) => {
                let f = match value {
                    QueryFilterValue::Int(n) => *n as f64,
                    QueryFilterValue::Float(f) => *f,
                    _ => return None,
                };
                Some(DerivedExpr::Literal(f))
            }
            DerivedExpression::BinaryOp { left, op, right } => {
                let left_expr = self.try_convert_derived_expression_basic(left)?;
                let right_expr = self.try_convert_derived_expression_basic(right)?;
                let semantic_op = match op {
                    DerivedOp::Add => DerivedBinaryOp::Add,
                    DerivedOp::Sub => DerivedBinaryOp::Sub,
                    DerivedOp::Mul => DerivedBinaryOp::Mul,
                    DerivedOp::Div => DerivedBinaryOp::Div,
                };
                Some(DerivedExpr::BinaryOp {
                    left: Box::new(left_expr),
                    op: semantic_op,
                    right: Box::new(right_expr),
                })
            }
            DerivedExpression::Negate(inner) => {
                let inner_expr = self.try_convert_derived_expression_basic(inner)?;
                Some(DerivedExpr::Negate(Box::new(inner_expr)))
            }
            DerivedExpression::Function { .. } => None, // Not supported yet
            DerivedExpression::TimeFunction(tf) => {
                let semantic_tf = self.try_convert_time_function_basic(tf)?;
                Some(DerivedExpr::TimeFunction(semantic_tf))
            }
            DerivedExpression::Delta { current, previous } => {
                let current_expr = self.try_convert_derived_expression_basic(current)?;
                let previous_expr = self.try_convert_derived_expression_basic(previous)?;
                Some(DerivedExpr::Delta {
                    current: Box::new(current_expr),
                    previous: Box::new(previous_expr),
                })
            }
            DerivedExpression::Growth { current, previous } => {
                let current_expr = self.try_convert_derived_expression_basic(current)?;
                let previous_expr = self.try_convert_derived_expression_basic(previous)?;
                Some(DerivedExpr::Growth {
                    current: Box::new(current_expr),
                    previous: Box::new(previous_expr),
                })
            }
        }
    }

    /// Convert a QueryTimeFunction to semantic TimeFunction without validation.
    fn try_convert_time_function_basic(&self, tf: &QueryTimeFunction) -> Option<TimeFunction> {
        Some(match tf {
            QueryTimeFunction::YearToDate { measure, year_column, period_column, via } => {
                TimeFunction::YearToDate {
                    measure: measure.clone(),
                    year_column: year_column.clone(),
                    period_column: period_column.clone(),
                    via: via.clone(),
                }
            }
            QueryTimeFunction::QuarterToDate { measure, year_column, quarter_column, period_column, via } => {
                TimeFunction::QuarterToDate {
                    measure: measure.clone(),
                    year_column: year_column.clone(),
                    quarter_column: quarter_column.clone(),
                    period_column: period_column.clone(),
                    via: via.clone(),
                }
            }
            QueryTimeFunction::MonthToDate { measure, year_column, month_column, day_column, via } => {
                TimeFunction::MonthToDate {
                    measure: measure.clone(),
                    year_column: year_column.clone(),
                    month_column: month_column.clone(),
                    day_column: day_column.clone(),
                    via: via.clone(),
                }
            }
            QueryTimeFunction::PriorPeriod { measure, periods_back, via } => {
                TimeFunction::PriorPeriod {
                    measure: measure.clone(),
                    periods_back: *periods_back,
                    via: via.clone(),
                }
            }
            QueryTimeFunction::PriorYear { measure, via } => {
                TimeFunction::PriorYear { measure: measure.clone(), via: via.clone() }
            }
            QueryTimeFunction::PriorQuarter { measure, via } => {
                TimeFunction::PriorQuarter { measure: measure.clone(), via: via.clone() }
            }
            QueryTimeFunction::RollingSum { measure, periods, via } => {
                TimeFunction::RollingSum {
                    measure: measure.clone(),
                    periods: *periods,
                    via: via.clone(),
                }
            }
            QueryTimeFunction::RollingAvg { measure, periods, via } => {
                TimeFunction::RollingAvg {
                    measure: measure.clone(),
                    periods: *periods,
                    via: via.clone(),
                }
            }
        })
    }

    /// Convert to a SemanticQuery with model-aware measure resolution.
    ///
    /// This method looks up measures in the model to:
    /// - Verify the measure exists
    /// - Find the correct entity if not on the anchor
    /// - Include proper aggregation information
    ///
    /// # Errors
    ///
    /// Returns `SemanticError::UnknownMeasure` if a measure is not found.
    /// Returns `SemanticError::UnknownEntity` if a referenced entity doesn't exist.
    pub fn to_semantic_query_with_model(&self, model: &Model) -> Result<SemanticQuery, SemanticError> {
        let mut select = Vec::new();
        let mut group_by = Vec::new();
        let mut derived = Vec::new();

        for sel in &self.select {
            match sel {
                QuerySelect::Dimension { entity, column } => {
                    // Validate entity exists
                    if !model.has_entity(entity) {
                        return Err(SemanticError::UnknownEntity(entity.clone()));
                    }
                    select.push(SelectField::new(entity, column));
                    group_by.push(FieldRef::new(entity, column));
                }
                QuerySelect::Measure {
                    entity: explicit_entity,
                    name,
                    alias,
                } => {
                    // Look up measure in the model to validate and get entity
                    // If explicit entity is provided, use it; otherwise search
                    let (entity, measure_def) = if let Some(e) = explicit_entity {
                        let measure_def = model
                            .facts
                            .get(e)
                            .and_then(|f| f.measures.get(name))
                            .ok_or_else(|| SemanticError::UnknownMeasure {
                                name: format!("{}.{}", e, name),
                            })?;
                        (e.clone(), measure_def)
                    } else {
                        self.find_measure_with_model(name, model)?
                    };

                    // Use measure name as the field - the planner resolves it
                    // We provide the aggregation hint for consistency
                    let mut field = SelectField::aggregate(
                        &entity,
                        name, // Use measure name, not source_column
                        &measure_def.aggregation.to_string(),
                    );
                    if let Some(a) = alias {
                        field = field.with_alias(a);
                    }
                    select.push(field);
                }
                QuerySelect::FilteredMeasure {
                    entity: explicit_entity,
                    name,
                    alias,
                    filters,
                } => {
                    // Look up measure in the model
                    let (entity, measure_def) = if let Some(e) = explicit_entity {
                        let measure_def = model
                            .facts
                            .get(e)
                            .and_then(|f| f.measures.get(name))
                            .ok_or_else(|| SemanticError::UnknownMeasure {
                                name: format!("{}.{}", e, name),
                            })?;
                        (e.clone(), measure_def)
                    } else {
                        self.find_measure_with_model(name, model)?
                    };

                    // Create measure with filter conditions
                    let measure_filters: Vec<FieldFilter> = filters
                        .iter()
                        .map(|f| f.to_field_filter())
                        .collect();

                    let mut field = SelectField::aggregate(
                        &entity,
                        name,
                        &measure_def.aggregation.to_string(),
                    )
                    .with_filter(measure_filters);

                    // Use provided alias, or default to measure name
                    let output_alias = alias.as_deref().unwrap_or(name);
                    field = field.with_alias(output_alias);
                    select.push(field);
                }
                QuerySelect::DerivedMeasure { alias, expression } => {
                    // Convert the DerivedExpression to DerivedExpr for the semantic layer
                    let expr = self.convert_derived_expression(expression, model)?;
                    derived.push(DerivedField::new(alias, expr));
                }
            }
        }

        // Add explicit group_by entries
        for gb in &self.group_by {
            if let Some((entity, column)) = gb.split_once('.') {
                if !model.has_entity(entity) {
                    return Err(SemanticError::UnknownEntity(entity.to_string()));
                }
                if !group_by.iter().any(|g| g.entity == entity && g.field == column) {
                    group_by.push(FieldRef::new(entity, column));
                }
            }
        }

        let filters = self.filters.iter().map(|f| f.to_field_filter()).collect();

        // Default entity for bare field names (uses anchor if specified)
        let default_entity = self.from.as_deref().unwrap_or("");

        let order_by = self
            .order_by
            .iter()
            .map(|o| {
                if let Some((entity, column)) = o.field.split_once('.') {
                    OrderField {
                        field: FieldRef::new(entity, column),
                        descending: o.descending,
                    }
                } else {
                    // Bare field name - use anchor entity if available
                    OrderField {
                        field: FieldRef::new(default_entity, &o.field),
                        descending: o.descending,
                    }
                }
            })
            .collect();

        Ok(SemanticQuery {
            from: self.from.clone(),
            filters,
            group_by,
            select,
            derived,
            order_by,
            limit: self.limit,
        })
    }

    /// Convert a DerivedExpression from the query definition to a DerivedExpr
    /// for the semantic layer.
    fn convert_derived_expression(
        &self,
        expr: &DerivedExpression,
        model: &Model,
    ) -> Result<DerivedExpr, SemanticError> {
        match expr {
            DerivedExpression::MeasureRef(name) => {
                // Validate measure exists
                self.find_measure_with_model(name, model)?;
                Ok(DerivedExpr::MeasureRef(name.clone()))
            }
            DerivedExpression::ColumnRef { entity, column: _ } => {
                // Validate entity exists
                if !model.has_entity(entity) {
                    return Err(SemanticError::UnknownEntity(entity.clone()));
                }
                // For now, column refs in derived expressions are treated as measure refs
                // if they resolve to a measure on that entity
                Err(SemanticError::InvalidReference(format!(
                    "Column references in derived measures not yet supported: {}.{}",
                    entity, entity
                )))
            }
            DerivedExpression::Literal(value) => {
                let f = match value {
                    QueryFilterValue::Int(n) => *n as f64,
                    QueryFilterValue::Float(f) => *f,
                    _ => {
                        return Err(SemanticError::InvalidReference(
                            "Only numeric literals allowed in derived expressions".into(),
                        ));
                    }
                };
                Ok(DerivedExpr::Literal(f))
            }
            DerivedExpression::BinaryOp { left, op, right } => {
                let left_expr = self.convert_derived_expression(left, model)?;
                let right_expr = self.convert_derived_expression(right, model)?;
                let semantic_op = match op {
                    DerivedOp::Add => DerivedBinaryOp::Add,
                    DerivedOp::Sub => DerivedBinaryOp::Sub,
                    DerivedOp::Mul => DerivedBinaryOp::Mul,
                    DerivedOp::Div => DerivedBinaryOp::Div,
                };
                Ok(DerivedExpr::BinaryOp {
                    left: Box::new(left_expr),
                    op: semantic_op,
                    right: Box::new(right_expr),
                })
            }
            DerivedExpression::Negate(inner) => {
                let inner_expr = self.convert_derived_expression(inner, model)?;
                Ok(DerivedExpr::Negate(Box::new(inner_expr)))
            }
            DerivedExpression::Function { name, args: _ } => {
                Err(SemanticError::InvalidReference(format!(
                    "Function '{}' in derived expressions not yet supported",
                    name
                )))
            }
            DerivedExpression::TimeFunction(tf) => {
                let semantic_tf = self.convert_time_function(tf, model)?;
                Ok(DerivedExpr::TimeFunction(semantic_tf))
            }
            DerivedExpression::Delta { current, previous } => {
                let current_expr = self.convert_derived_expression(current, model)?;
                let previous_expr = self.convert_derived_expression(previous, model)?;
                Ok(DerivedExpr::Delta {
                    current: Box::new(current_expr),
                    previous: Box::new(previous_expr),
                })
            }
            DerivedExpression::Growth { current, previous } => {
                let current_expr = self.convert_derived_expression(current, model)?;
                let previous_expr = self.convert_derived_expression(previous, model)?;
                Ok(DerivedExpr::Growth {
                    current: Box::new(current_expr),
                    previous: Box::new(previous_expr),
                })
            }
        }
    }

    /// Convert a QueryTimeFunction to the semantic layer's TimeFunction.
    fn convert_time_function(
        &self,
        tf: &QueryTimeFunction,
        model: &Model,
    ) -> Result<TimeFunction, SemanticError> {
        match tf {
            QueryTimeFunction::YearToDate {
                measure,
                year_column,
                period_column,
                via,
            } => {
                // Validate measure exists
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::YearToDate {
                    measure: measure.clone(),
                    year_column: year_column.clone(),
                    period_column: period_column.clone(),
                    via: via.clone(),
                })
            }
            QueryTimeFunction::QuarterToDate {
                measure,
                year_column,
                quarter_column,
                period_column,
                via,
            } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::QuarterToDate {
                    measure: measure.clone(),
                    year_column: year_column.clone(),
                    quarter_column: quarter_column.clone(),
                    period_column: period_column.clone(),
                    via: via.clone(),
                })
            }
            QueryTimeFunction::MonthToDate {
                measure,
                year_column,
                month_column,
                day_column,
                via,
            } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::MonthToDate {
                    measure: measure.clone(),
                    year_column: year_column.clone(),
                    month_column: month_column.clone(),
                    day_column: day_column.clone(),
                    via: via.clone(),
                })
            }
            QueryTimeFunction::PriorPeriod {
                measure,
                periods_back,
                via,
            } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::PriorPeriod {
                    measure: measure.clone(),
                    periods_back: *periods_back,
                    via: via.clone(),
                })
            }
            QueryTimeFunction::PriorYear { measure, via } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::PriorYear {
                    measure: measure.clone(),
                    via: via.clone(),
                })
            }
            QueryTimeFunction::PriorQuarter { measure, via } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::PriorQuarter {
                    measure: measure.clone(),
                    via: via.clone(),
                })
            }
            QueryTimeFunction::RollingSum {
                measure,
                periods,
                via,
            } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::RollingSum {
                    measure: measure.clone(),
                    periods: *periods,
                    via: via.clone(),
                })
            }
            QueryTimeFunction::RollingAvg {
                measure,
                periods,
                via,
            } => {
                self.find_measure_with_model(measure, model)?;
                Ok(TimeFunction::RollingAvg {
                    measure: measure.clone(),
                    periods: *periods,
                    via: via.clone(),
                })
            }
        }
    }

    /// Find a measure definition in the model.
    ///
    /// First checks the anchor fact (if specified), then searches all facts.
    fn find_measure_with_model<'a>(
        &self,
        name: &str,
        model: &'a Model,
    ) -> Result<(String, &'a super::MeasureDefinition), SemanticError> {
        // First, check the anchor fact if specified
        if let Some(ref from) = self.from {
            if let Some(measure) = model.find_measure_in_fact(from, name) {
                return Ok((from.clone(), measure));
            }
        }

        // If not found in anchor (or no anchor), search all facts
        if let Some((fact_name, measure)) = model.find_measure(name) {
            return Ok((fact_name.to_string(), measure));
        }

        Err(SemanticError::UnknownMeasure { name: name.into() })
    }

    /// Validate all entity references in the query.
    ///
    /// Returns a list of validation errors if any references are invalid.
    pub fn validate(&self, model: &Model) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Check anchor entity if specified
        if let Some(ref from) = self.from {
            if !model.has_entity(from) {
                errors.push(format!("Unknown anchor entity: '{}'", from));
            }
        }

        // Check dimension references in select
        for sel in &self.select {
            match sel {
                QuerySelect::Dimension { entity, .. } => {
                    if !model.has_entity(entity) {
                        errors.push(format!("Unknown entity in select: '{}'", entity));
                    }
                }
                QuerySelect::Measure { name, .. } => {
                    if model.find_measure(name).is_none() {
                        errors.push(format!("Unknown measure: '{}'", name));
                    }
                }
                QuerySelect::FilteredMeasure { name, filters, .. } => {
                    if model.find_measure(name).is_none() {
                        errors.push(format!("Unknown measure: '{}'", name));
                    }
                    // Check filter references
                    for filter in filters {
                        if let Some((entity, _)) = filter.field.split_once('.') {
                            if !model.has_entity(entity) {
                                errors.push(format!(
                                    "Unknown entity in measure filter: '{}'",
                                    entity
                                ));
                            }
                        }
                    }
                }
                QuerySelect::DerivedMeasure { expression, .. } => {
                    // Validate measure references in the expression
                    self.validate_derived_expression(expression, model, &mut errors);
                }
            }
        }

        // Check filter references
        for filter in &self.filters {
            if let Some((entity, _)) = filter.field.split_once('.') {
                if !model.has_entity(entity) {
                    errors.push(format!("Unknown entity in filter: '{}'", entity));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate measure references in a derived expression.
    #[allow(clippy::only_used_in_recursion)]
    fn validate_derived_expression(
        &self,
        expr: &DerivedExpression,
        model: &Model,
        errors: &mut Vec<String>,
    ) {
        match expr {
            DerivedExpression::MeasureRef(name) => {
                if model.find_measure(name).is_none() {
                    errors.push(format!("Unknown measure in derived expression: '{}'", name));
                }
            }
            DerivedExpression::ColumnRef { entity, .. } => {
                if !model.has_entity(entity) {
                    errors.push(format!(
                        "Unknown entity in derived expression: '{}'",
                        entity
                    ));
                }
            }
            DerivedExpression::Literal(_) => {}
            DerivedExpression::BinaryOp { left, right, .. } => {
                self.validate_derived_expression(left, model, errors);
                self.validate_derived_expression(right, model, errors);
            }
            DerivedExpression::Negate(inner) => {
                self.validate_derived_expression(inner, model, errors);
            }
            DerivedExpression::Function { args, .. } => {
                for arg in args {
                    self.validate_derived_expression(arg, model, errors);
                }
            }
            DerivedExpression::TimeFunction(tf) => {
                // Validate the measure referenced in the time function
                let measure = match tf {
                    QueryTimeFunction::YearToDate { measure, .. }
                    | QueryTimeFunction::QuarterToDate { measure, .. }
                    | QueryTimeFunction::MonthToDate { measure, .. }
                    | QueryTimeFunction::PriorPeriod { measure, .. }
                    | QueryTimeFunction::PriorYear { measure, .. }
                    | QueryTimeFunction::PriorQuarter { measure, .. }
                    | QueryTimeFunction::RollingSum { measure, .. }
                    | QueryTimeFunction::RollingAvg { measure, .. } => measure,
                };
                if model.find_measure(measure).is_none() {
                    errors.push(format!("Unknown measure in time function: '{}'", measure));
                }
            }
            DerivedExpression::Delta { current, previous } => {
                self.validate_derived_expression(current, model, errors);
                self.validate_derived_expression(previous, model, errors);
            }
            DerivedExpression::Growth { current, previous } => {
                self.validate_derived_expression(current, model, errors);
                self.validate_derived_expression(previous, model, errors);
            }
        }
    }
}

/// A select item in a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuerySelect {
    /// A dimension reference: entity.column
    Dimension { entity: String, column: String },
    /// A measure reference by name, optionally qualified with entity
    Measure {
        /// Optional entity (fact) name for disambiguation
        entity: Option<String>,
        name: String,
        alias: Option<String>,
    },
    /// A filtered measure: measure:where(condition)
    ///
    /// Generates SQL like: SUM(CASE WHEN condition THEN column END)
    FilteredMeasure {
        /// Optional entity (fact) name for disambiguation
        entity: Option<String>,
        name: String,
        alias: Option<String>,
        /// Filter conditions to apply (generates CASE WHEN)
        filters: Vec<QueryFilter>,
    },
    /// A derived measure: inline calculation from other measures
    ///
    /// e.g., `aov = revenue / order_count`
    DerivedMeasure {
        alias: String,
        expression: DerivedExpression,
    },
}

/// An expression for derived measures (inline calculations).
///
/// These are computed after aggregation from other measures or literals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DerivedExpression {
    /// Reference to another measure by name
    MeasureRef(String),
    /// Reference to a dimension column (entity.column)
    ColumnRef { entity: String, column: String },
    /// A literal value
    Literal(QueryFilterValue),
    /// Binary operation: left op right
    BinaryOp {
        left: Box<DerivedExpression>,
        op: DerivedOp,
        right: Box<DerivedExpression>,
    },
    /// Unary negation
    Negate(Box<DerivedExpression>),
    /// Function call (for future extension)
    Function {
        name: String,
        args: Vec<DerivedExpression>,
    },
    /// A time intelligence function (YTD, prior year, rolling, etc.)
    TimeFunction(QueryTimeFunction),
    /// Delta: difference between current and previous value
    Delta {
        current: Box<DerivedExpression>,
        previous: Box<DerivedExpression>,
    },
    /// Growth: percentage change from previous to current
    Growth {
        current: Box<DerivedExpression>,
        previous: Box<DerivedExpression>,
    },
}

/// Operators for derived measure expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DerivedOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Time intelligence functions for period-over-period analysis.
///
/// These functions generate window functions and lag calculations
/// for temporal analytics like YTD, prior period, and rolling windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryTimeFunction {
    /// Year-to-date: cumulative sum from start of year.
    YearToDate {
        measure: String,
        year_column: Option<String>,
        period_column: Option<String>,
        via: Option<String>,
    },
    /// Quarter-to-date: cumulative sum from start of quarter.
    QuarterToDate {
        measure: String,
        year_column: Option<String>,
        quarter_column: Option<String>,
        period_column: Option<String>,
        via: Option<String>,
    },
    /// Month-to-date: cumulative sum from start of month.
    MonthToDate {
        measure: String,
        year_column: Option<String>,
        month_column: Option<String>,
        day_column: Option<String>,
        via: Option<String>,
    },
    /// Prior period: value from N periods ago.
    PriorPeriod {
        measure: String,
        periods_back: u32,
        via: Option<String>,
    },
    /// Prior year: same period in the prior year.
    PriorYear {
        measure: String,
        via: Option<String>,
    },
    /// Prior quarter: same period in the prior quarter.
    PriorQuarter {
        measure: String,
        via: Option<String>,
    },
    /// Rolling sum: sum over the last N periods.
    RollingSum {
        measure: String,
        periods: u32,
        via: Option<String>,
    },
    /// Rolling average: average over the last N periods.
    RollingAvg {
        measure: String,
        periods: u32,
        via: Option<String>,
    },
}

impl DerivedOp {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "+" => Some(DerivedOp::Add),
            "-" => Some(DerivedOp::Sub),
            "*" => Some(DerivedOp::Mul),
            "/" => Some(DerivedOp::Div),
            _ => None,
        }
    }

    pub fn to_sql(&self) -> &'static str {
        match self {
            DerivedOp::Add => "+",
            DerivedOp::Sub => "-",
            DerivedOp::Mul => "*",
            DerivedOp::Div => "/",
        }
    }
}

impl QuerySelect {
    /// Parse a select string into a QuerySelect.
    ///
    /// If it contains a dot, it's a dimension reference.
    /// Otherwise, it's a measure name.
    pub fn parse(s: &str) -> Self {
        if let Some((entity, column)) = s.split_once('.') {
            QuerySelect::Dimension {
                entity: entity.to_string(),
                column: column.to_string(),
            }
        } else {
            QuerySelect::Measure {
                entity: None,
                name: s.to_string(),
                alias: None,
            }
        }
    }
}

/// A filter condition in a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    /// Field reference (entity.column format)
    pub field: String,
    /// Comparison operator
    pub op: QueryFilterOp,
    /// Value(s) to compare against
    pub value: QueryFilterValue,
}

impl QueryFilter {
    /// Convert to a FieldFilter for the semantic layer.
    pub fn to_field_filter(&self) -> FieldFilter {
        let (entity, column) = self
            .field
            .split_once('.')
            .unwrap_or(("", &self.field));

        FieldFilter {
            field: FieldRef::new(entity, column),
            op: self.op.to_filter_op(),
            value: self.value.to_filter_value(),
        }
    }
}

/// Filter operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryFilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    In,
    NotIn,
    Between,
    IsNull,
    IsNotNull,
}

impl QueryFilterOp {
    pub fn to_filter_op(&self) -> FilterOp {
        match self {
            QueryFilterOp::Eq => FilterOp::Eq,
            QueryFilterOp::Ne => FilterOp::Ne,
            QueryFilterOp::Gt => FilterOp::Gt,
            QueryFilterOp::Gte => FilterOp::Gte,
            QueryFilterOp::Lt => FilterOp::Lt,
            QueryFilterOp::Lte => FilterOp::Lte,
            QueryFilterOp::Like => FilterOp::Like,
            QueryFilterOp::In => FilterOp::In,
            QueryFilterOp::NotIn => FilterOp::In, // Will be negated
            QueryFilterOp::Between => FilterOp::Gte, // Simplified for now
            QueryFilterOp::IsNull => FilterOp::IsNull,
            QueryFilterOp::IsNotNull => FilterOp::IsNotNull,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "=" | "==" | "eq" => Some(QueryFilterOp::Eq),
            "!=" | "<>" | "ne" => Some(QueryFilterOp::Ne),
            ">" | "gt" => Some(QueryFilterOp::Gt),
            ">=" | "gte" => Some(QueryFilterOp::Gte),
            "<" | "lt" => Some(QueryFilterOp::Lt),
            "<=" | "lte" => Some(QueryFilterOp::Lte),
            "like" | "LIKE" => Some(QueryFilterOp::Like),
            "in" | "IN" => Some(QueryFilterOp::In),
            "not_in" | "NOT IN" => Some(QueryFilterOp::NotIn),
            "between" | "BETWEEN" => Some(QueryFilterOp::Between),
            "is_null" | "IS NULL" => Some(QueryFilterOp::IsNull),
            "is_not_null" | "IS NOT NULL" => Some(QueryFilterOp::IsNotNull),
            _ => None,
        }
    }
}

/// A filter value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryFilterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
    List(Vec<QueryFilterValue>),
}

impl QueryFilterValue {
    pub fn to_filter_value(&self) -> FilterValue {
        match self {
            QueryFilterValue::String(s) => FilterValue::String(s.clone()),
            QueryFilterValue::Int(n) => FilterValue::Int(*n),
            QueryFilterValue::Float(f) => FilterValue::Float(*f),
            QueryFilterValue::Bool(b) => FilterValue::Bool(*b),
            QueryFilterValue::Null => FilterValue::Null,
            QueryFilterValue::List(items) => {
                FilterValue::List(items.iter().map(|v| v.to_filter_value()).collect())
            }
        }
    }
}

/// An order by specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOrderBy {
    /// Field reference (can be "entity.column" or bare measure name)
    pub field: String,
    /// True for descending order
    pub descending: bool,
}

impl QueryOrderBy {
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: false,
        }
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: true,
        }
    }
}
