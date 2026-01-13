//! Phase 4: Emit
//!
//! This phase converts a logical plan into a SQL Query object.
//! It handles the translation from logical operations to physical SQL constructs.

use crate::expr::{avg, col, count_distinct, count_star, func, max, min, sum, table_col, Expr, ExprExt};
use crate::model::AggregationType;
use crate::query::{OrderByExpr, Query, SelectExpr, TableRef};
use crate::semantic::error::PlanResult;

use super::emit_time::TimeEmitter;
use super::logical::{LogicalJoinType, LogicalPlan};
use super::prune::PrunedColumns;
use super::resolved::{
    ResolvedColumn, ResolvedDerivedExpr, ResolvedEntity, ResolvedFilter, ResolvedMeasure,
    ResolvedOrder, ResolvedOrderExpr, ResolvedSelect,
};
use super::types::{DerivedBinaryOp, FilterOp, FilterValue};

/// Emitter - handles Phase 4 of query planning.
///
/// Optionally uses pruned column information to optimize generated SQL.
pub struct Emitter {
    /// Default schema to use when none is specified.
    default_schema: String,
    /// Pruned columns (minimal set needed for the query).
    /// Currently used for debugging/inspection; future: optimize SELECT.
    #[allow(dead_code)]
    pruned_columns: Option<PrunedColumns>,
}

impl Emitter {
    pub fn new() -> Self {
        Self {
            default_schema: "dbo".to_string(),
            pruned_columns: None,
        }
    }

    pub fn with_default_schema(mut self, schema: &str) -> Self {
        self.default_schema = schema.to_string();
        self
    }

    /// Set the pruned columns for this emitter.
    ///
    /// When set, the emitter can use this information to:
    /// - Optimize SELECT clauses in subqueries
    /// - Track which columns are actually needed
    /// - Provide debugging information
    pub fn with_pruned_columns(mut self, pruned: PrunedColumns) -> Self {
        self.pruned_columns = Some(pruned);
        self
    }

    /// Emit a SQL Query from a logical plan.
    pub fn emit(&self, plan: &LogicalPlan) -> PlanResult<Query> {
        // We need to traverse the plan and collect information
        let mut ctx = EmitContext::new();
        self.collect_plan_info(plan, &mut ctx);

        // Build the query
        let mut query = Query::new();

        // FROM clause
        if let Some(from) = &ctx.from {
            query = query.from(self.emit_table_ref(from));
        }

        // JOINs
        for join in &ctx.joins {
            let table_ref = self.emit_table_ref(&join.entity);
            let on_expr = table_col(&join.left_entity, &join.left_column)
                .eq(table_col(&join.right_entity, &join.right_column));

            query = match join.join_type {
                LogicalJoinType::Inner => query.inner_join(table_ref, on_expr),
                LogicalJoinType::Left => query.left_join(table_ref, on_expr),
                LogicalJoinType::Right => query.right_join(table_ref, on_expr),
                LogicalJoinType::Full => query.full_join(table_ref, on_expr),
            };
        }

        // WHERE clause
        for filter in &ctx.filters {
            let expr = self.emit_filter(filter);
            query = query.filter(expr);
        }

        // SELECT clause (includes group by columns)
        let mut select_exprs = Vec::new();

        // Add GROUP BY columns to SELECT first
        for col in &ctx.group_by {
            let expr = self.emit_column(col);
            select_exprs.push(SelectExpr::new(expr).with_alias(&col.logical_name));
        }

        // Add projections
        // Collect qualified group_by columns (entity_alias, column_name) for time function context
        let group_by_qualified: Vec<(&str, &str)> = ctx.group_by.iter()
            .map(|c| (c.entity_alias.as_str(), c.logical_name.as_str()))
            .collect();
        for projection in &ctx.projections {
            select_exprs.push(self.emit_select(projection, &group_by_qualified));
        }

        if !select_exprs.is_empty() {
            query = query.select(select_exprs);
        }

        // GROUP BY clause
        if !ctx.group_by.is_empty() {
            let group_exprs: Vec<Expr> = ctx.group_by.iter().map(|c| self.emit_column(c)).collect();
            query = query.group_by(group_exprs);
        }

        // ORDER BY clause
        if !ctx.order_by.is_empty() {
            let order_exprs: Vec<OrderByExpr> =
                ctx.order_by.iter().map(|o| self.emit_order(o)).collect();
            query = query.order_by(order_exprs);
        }

        // LIMIT clause
        if let Some(limit) = ctx.limit {
            query = query.limit(limit);
        }

        Ok(query)
    }

    /// Collect information from the logical plan tree.
    #[allow(clippy::only_used_in_recursion)]
    fn collect_plan_info(&self, plan: &LogicalPlan, ctx: &mut EmitContext) {
        match plan {
            LogicalPlan::Scan(scan) => {
                if ctx.from.is_none() {
                    ctx.from = Some(scan.entity.clone());
                }
            }

            LogicalPlan::Join(join) => {
                // Process left side first
                self.collect_plan_info(&join.left, ctx);

                // Extract the right entity from the scan
                if let LogicalPlan::Scan(scan) = join.right.as_ref() {
                    ctx.joins.push(JoinInfo {
                        entity: scan.entity.clone(),
                        left_entity: join.on.left_entity.clone(),
                        left_column: join.on.left_column.clone(),
                        right_entity: join.on.right_entity.clone(),
                        right_column: join.on.right_column.clone(),
                        join_type: join.join_type,
                    });
                }
            }

            LogicalPlan::Filter(filter) => {
                self.collect_plan_info(&filter.input, ctx);
                ctx.filters.extend(filter.predicates.clone());
            }

            LogicalPlan::Aggregate(agg) => {
                self.collect_plan_info(&agg.input, ctx);
                ctx.group_by = agg.group_by.clone();
                ctx.aggregates = agg.aggregates.clone();
            }

            LogicalPlan::Project(proj) => {
                self.collect_plan_info(&proj.input, ctx);
                // Filter out columns that are already in group_by
                for projection in &proj.projections {
                    match projection {
                        ResolvedSelect::Column { column, .. } => {
                            let in_group_by = ctx.group_by.iter().any(|g| {
                                g.entity_alias == column.entity_alias
                                    && g.physical_name == column.physical_name
                            });
                            if !in_group_by {
                                ctx.projections.push(projection.clone());
                            }
                        }
                        ResolvedSelect::Measure { .. }
                        | ResolvedSelect::Aggregate { .. }
                        | ResolvedSelect::Derived { .. } => {
                            ctx.projections.push(projection.clone());
                        }
                    }
                }
            }

            LogicalPlan::Sort(sort) => {
                self.collect_plan_info(&sort.input, ctx);
                ctx.order_by = sort.order_by.clone();
            }

            LogicalPlan::Limit(limit) => {
                self.collect_plan_info(&limit.input, ctx);
                ctx.limit = Some(limit.limit);
            }
        }
    }

    /// Emit a table reference.
    fn emit_table_ref(&self, entity: &ResolvedEntity) -> TableRef {
        let schema = entity
            .physical_schema
            .as_deref()
            .unwrap_or(&self.default_schema);

        TableRef::new(&entity.physical_table)
            .with_schema(schema)
            .with_alias(&entity.name)
    }

    /// Emit a column expression.
    fn emit_column(&self, column: &ResolvedColumn) -> Expr {
        table_col(&column.entity_alias, &column.physical_name)
    }

    /// Emit a measure (aggregate) expression.
    ///
    /// If the measure has filters, generates a conditional aggregate:
    /// `SUM(CASE WHEN condition THEN column END)`
    ///
    /// Handles both:
    /// - Query-time filters: Applied when measure is used in a query
    /// - Definition filters: Defined in the measure definition (e.g., `:where()`)
    fn emit_measure(&self, measure: &ResolvedMeasure) -> Expr {
        let source_col = if measure.source_column == "*" {
            None
        } else {
            Some(table_col(&measure.entity_alias, &measure.source_column))
        };

        // Check for filters - query-time filter or definition filter
        let has_query_filter = measure.filter.is_some();
        let has_def_filter = measure.definition_filter.is_some();

        // If there's a filter, wrap the source column in a CASE WHEN
        let aggregation_expr = if has_query_filter {
            // Query-time filter takes precedence
            let filters = measure.filter.as_ref().unwrap();
            if let Some(col) = source_col {
                // Build the condition from filters (AND them together)
                let condition = self.build_filter_condition(filters);
                // CASE WHEN condition THEN column END
                Expr::Case {
                    operand: None,
                    when_clauses: vec![(condition, col)],
                    else_clause: None,
                }
            } else {
                // For COUNT(*) with filter, we use CASE WHEN condition THEN 1 END
                let condition = self.build_filter_condition(filters);
                Expr::Case {
                    operand: None,
                    when_clauses: vec![(condition, crate::expr::lit_int(1))],
                    else_clause: None,
                }
            }
        } else if has_def_filter {
            // Definition-time filter (from measure definition)
            // Convert from model::expr::Expr to sql::expr::Expr
            // Pass the entity alias so unqualified column refs get properly qualified
            let condition = convert_model_expr_with_context(measure.definition_filter.as_ref().unwrap(), Some(&measure.entity_alias));
            if let Some(col) = source_col {
                // CASE WHEN condition THEN column END
                Expr::Case {
                    operand: None,
                    when_clauses: vec![(condition, col)],
                    else_clause: None,
                }
            } else {
                // For COUNT(*) with filter, we use CASE WHEN condition THEN 1 END
                Expr::Case {
                    operand: None,
                    when_clauses: vec![(condition, crate::expr::lit_int(1))],
                    else_clause: None,
                }
            }
        } else {
            match source_col {
                Some(col) => col,
                None => {
                    // COUNT(*) - return early with count_star()
                    return count_star();
                }
            }
        };

        // Apply aggregation function
        match measure.aggregation {
            AggregationType::Sum => sum(aggregation_expr),
            AggregationType::Count => {
                if has_query_filter || has_def_filter {
                    // For filtered count, we sum the CASE expression
                    sum(aggregation_expr)
                } else if measure.source_column == "*" {
                    count_star()
                } else {
                    func("COUNT", vec![aggregation_expr])
                }
            }
            AggregationType::CountDistinct => count_distinct(aggregation_expr),
            AggregationType::Avg => avg(aggregation_expr),
            AggregationType::Min => min(aggregation_expr),
            AggregationType::Max => max(aggregation_expr),
        }
    }

    /// Build a filter condition from resolved filters (AND them together).
    fn build_filter_condition(&self, filters: &[ResolvedFilter]) -> Expr {
        let exprs: Vec<Expr> = filters.iter().map(|f| self.emit_filter(f)).collect();
        if exprs.is_empty() {
            // No filters - always true
            crate::expr::lit_bool(true)
        } else if exprs.len() == 1 {
            exprs.into_iter().next().unwrap()
        } else {
            // AND all conditions together
            exprs
                .into_iter()
                .reduce(|a, b| a.and(b))
                .unwrap()
        }
    }

    /// Emit a SELECT expression.
    ///
    /// The `group_by_cols` parameter contains qualified column references `(entity_alias, column_name)`
    /// for use in time intelligence window functions.
    fn emit_select(&self, select: &ResolvedSelect, group_by_cols: &[(&str, &str)]) -> SelectExpr {
        match select {
            ResolvedSelect::Column { column, alias } => {
                let expr = self.emit_column(column);
                let output_alias = alias.as_deref().unwrap_or(&column.logical_name);
                SelectExpr::new(expr).with_alias(output_alias)
            }
            ResolvedSelect::Measure { measure, alias } => {
                let expr = self.emit_measure(measure);
                let output_alias = alias.as_deref().unwrap_or(&measure.name);
                SelectExpr::new(expr).with_alias(output_alias)
            }
            ResolvedSelect::Aggregate { column, aggregation, alias } => {
                let expr = self.emit_aggregate(column, aggregation);
                let output_alias = alias.as_deref().unwrap_or(&column.logical_name);
                SelectExpr::new(expr).with_alias(output_alias)
            }
            ResolvedSelect::Derived { alias, expression } => {
                let expr = self.emit_derived_expr(expression, group_by_cols);
                SelectExpr::new(expr).with_alias(alias)
            }
        }
    }

    /// Emit a derived expression (calculation from measures).
    ///
    /// The `group_by_cols` parameter provides qualified column context `(entity_alias, column_name)`
    /// for time intelligence functions that need to know the grouping columns to build proper
    /// window functions with table-qualified ORDER BY clauses.
    fn emit_derived_expr(&self, expr: &ResolvedDerivedExpr, group_by_cols: &[(&str, &str)]) -> Expr {
        match expr {
            ResolvedDerivedExpr::MeasureRef(measure) => {
                // Emit the full aggregate expression (not an alias reference)
                self.emit_measure(measure)
            }
            ResolvedDerivedExpr::Literal(value) => crate::expr::lit_float(*value),
            ResolvedDerivedExpr::BinaryOp { left, op, right } => {
                let left_expr = self.emit_derived_expr(left, group_by_cols);
                let right_expr = self.emit_derived_expr(right, group_by_cols);
                match op {
                    DerivedBinaryOp::Add => left_expr.add(right_expr),
                    DerivedBinaryOp::Sub => left_expr.sub(right_expr),
                    DerivedBinaryOp::Mul => left_expr.mul(right_expr),
                    DerivedBinaryOp::Div => left_expr.div(right_expr),
                }
            }
            ResolvedDerivedExpr::Negate(inner) => {
                let inner_expr = self.emit_derived_expr(inner, group_by_cols);
                crate::expr::lit_int(0).sub(inner_expr)
            }
            // Time intelligence functions - use TimeEmitter for window function generation
            ResolvedDerivedExpr::TimeFunction(time_fn) => {
                // The measure expression is a reference to the measure's output alias
                let measure_expr = col(time_fn.measure());
                TimeEmitter::emit(time_fn, measure_expr, group_by_cols)
            }
            ResolvedDerivedExpr::Delta { current, previous } => {
                // delta(a, b) = a - b
                let current_expr = self.emit_derived_expr(current, group_by_cols);
                let previous_expr = self.emit_derived_expr(previous, group_by_cols);
                current_expr.sub(previous_expr)
            }
            ResolvedDerivedExpr::Growth { current, previous } => {
                // growth(a, b) = (a - b) / NULLIF(b, 0) * 100
                let current_expr = self.emit_derived_expr(current, group_by_cols);
                let previous_expr = self.emit_derived_expr(previous, group_by_cols);
                let delta = current_expr.clone().sub(previous_expr.clone());
                let nullif_prev = func("NULLIF", vec![previous_expr, crate::expr::lit_int(0)]);
                delta.div(nullif_prev).mul(crate::expr::lit_int(100))
            }
        }
    }

    /// Emit an inline aggregate expression.
    fn emit_aggregate(&self, column: &ResolvedColumn, aggregation: &str) -> Expr {
        let source_col = table_col(&column.entity_alias, &column.physical_name);

        match aggregation.to_uppercase().as_str() {
            "SUM" => sum(source_col),
            "COUNT" => func("COUNT", vec![source_col]),
            "COUNT_DISTINCT" | "COUNT DISTINCT" => count_distinct(source_col),
            "AVG" => avg(source_col),
            "MIN" => min(source_col),
            "MAX" => max(source_col),
            other => func(other, vec![source_col]),
        }
    }

    /// Emit a filter expression.
    fn emit_filter(&self, filter: &ResolvedFilter) -> Expr {
        let column_expr = self.emit_column(&filter.column);
        let value_expr = self.emit_filter_value(&filter.value);

        match filter.op {
            FilterOp::Eq => column_expr.eq(value_expr),
            FilterOp::Ne => column_expr.ne(value_expr),
            FilterOp::Gt => column_expr.gt(value_expr),
            FilterOp::Gte => column_expr.gte(value_expr),
            FilterOp::Lt => column_expr.lt(value_expr),
            FilterOp::Lte => column_expr.lte(value_expr),
            FilterOp::Like => column_expr.like(value_expr),
            FilterOp::In => {
                if let FilterValue::List(values) = &filter.value {
                    let exprs: Vec<Expr> =
                        values.iter().map(|v| self.emit_filter_value(v)).collect();
                    column_expr.in_list(exprs)
                } else {
                    // Shouldn't happen if validation is correct
                    column_expr.eq(value_expr)
                }
            }
            FilterOp::IsNull => column_expr.is_null(),
            FilterOp::IsNotNull => column_expr.is_not_null(),
        }
    }

    /// Emit a filter value as an expression.
    fn emit_filter_value(&self, value: &FilterValue) -> Expr {
        match value {
            FilterValue::String(s) => crate::expr::lit_str(s),
            FilterValue::Int(n) => crate::expr::lit_int(*n),
            FilterValue::Float(f) => crate::expr::lit_float(*f),
            FilterValue::Bool(b) => crate::expr::lit_bool(*b),
            FilterValue::Null => crate::expr::lit_null(),
            FilterValue::List(_) => crate::expr::lit_null(), // Handled specially in In
        }
    }

    /// Emit an ORDER BY expression.
    fn emit_order(&self, order: &ResolvedOrder) -> OrderByExpr {
        let expr = match &order.expr {
            ResolvedOrderExpr::Column(col) => self.emit_column(col),
            ResolvedOrderExpr::Measure(measure) => self.emit_measure(measure),
        };

        if order.descending {
            OrderByExpr::desc(expr)
        } else {
            OrderByExpr::asc(expr)
        }
    }
}

impl Default for Emitter {
    fn default() -> Self {
        Self::new()
    }
}

/// Context for collecting plan information.
struct EmitContext {
    from: Option<ResolvedEntity>,
    joins: Vec<JoinInfo>,
    filters: Vec<ResolvedFilter>,
    group_by: Vec<ResolvedColumn>,
    aggregates: Vec<ResolvedMeasure>,
    projections: Vec<ResolvedSelect>,
    order_by: Vec<ResolvedOrder>,
    limit: Option<u64>,
}

impl EmitContext {
    fn new() -> Self {
        Self {
            from: None,
            joins: Vec::new(),
            filters: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            projections: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        }
    }
}

/// Information about a join to emit.
struct JoinInfo {
    entity: ResolvedEntity,
    left_entity: String,
    left_column: String,
    right_entity: String,
    right_column: String,
    join_type: LogicalJoinType,
}

// =============================================================================
// Expression Conversion
// =============================================================================

/// Convert a model expression to a SQL expression.
///
/// This converts the model's dialect-agnostic expression AST to the SQL
/// emitter's expression AST.
#[allow(dead_code)]
fn convert_model_expr(expr: &crate::model::expr::Expr) -> Expr {
    convert_model_expr_with_context(expr, None)
}

/// Convert a model expression to SQL expression with an optional default entity.
///
/// When `default_entity` is Some, unqualified column references will be
/// qualified with this entity name.
fn convert_model_expr_with_context(expr: &crate::model::expr::Expr, default_entity: Option<&str>) -> Expr {
    use crate::model::expr::Expr as ModelExpr;

    match expr {
        ModelExpr::Column { entity, column } => {
            if let Some(entity) = entity {
                table_col(entity, column)
            } else if let Some(default) = default_entity {
                table_col(default, column)
            } else {
                col(column)
            }
        }
        ModelExpr::Literal(lit) => convert_literal(lit),
        ModelExpr::BinaryOp { left, op, right } => {
            let left_expr = convert_model_expr_with_context(left, default_entity);
            let right_expr = convert_model_expr_with_context(right, default_entity);
            let sql_op = convert_binary_op(op);
            Expr::BinaryOp {
                left: Box::new(left_expr),
                op: sql_op,
                right: Box::new(right_expr),
            }
        }
        ModelExpr::UnaryOp { op, expr } => {
            use crate::model::expr::UnaryOp as ModelOp;
            let inner = convert_model_expr_with_context(expr, default_entity);
            match op {
                ModelOp::Not => Expr::UnaryOp {
                    op: crate::sql::expr::UnaryOperator::Not,
                    expr: Box::new(inner),
                },
                ModelOp::Neg => Expr::UnaryOp {
                    op: crate::sql::expr::UnaryOperator::Minus,
                    expr: Box::new(inner),
                },
                ModelOp::IsNull => inner.is_null(),
                ModelOp::IsNotNull => inner.is_not_null(),
            }
        }
        ModelExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand_expr = operand.as_ref().map(|e| Box::new(convert_model_expr_with_context(e, default_entity)));
            let whens: Vec<(Expr, Expr)> = when_clauses
                .iter()
                .map(|wc| (convert_model_expr_with_context(&wc.condition, default_entity), convert_model_expr_with_context(&wc.result, default_entity)))
                .collect();
            let else_expr = else_clause.as_ref().map(|e| Box::new(convert_model_expr_with_context(e, default_entity)));
            Expr::Case {
                operand: operand_expr,
                when_clauses: whens,
                else_clause: else_expr,
            }
        }
        ModelExpr::Function { func: fn_type, args } => {
            let func_name = convert_func(fn_type);
            let arg_exprs: Vec<Expr> = args.iter().map(|e| convert_model_expr_with_context(e, default_entity)).collect();
            func(&func_name, arg_exprs)
        }
        ModelExpr::Cast { expr, target_type } => {
            let inner = convert_model_expr_with_context(expr, default_entity);
            // Emit as CAST(expr AS type) using raw SQL
            let type_str = format!("{:?}", target_type);
            // Use a function call syntax for CAST
            func("CAST", vec![inner, crate::expr::lit_str(&type_str)])
        }
        ModelExpr::Window { .. } | ModelExpr::FilteredAgg { .. } => {
            // Window functions and filtered aggregates are complex - for now just emit a placeholder
            crate::expr::lit_str("UNSUPPORTED_EXPR")
        }
    }
}

fn convert_literal(lit: &crate::model::expr::Literal) -> Expr {
    use crate::model::expr::Literal as ModelLit;

    match lit {
        ModelLit::Null => Expr::Literal(crate::sql::expr::Literal::Null),
        ModelLit::Bool(b) => crate::expr::lit_bool(*b),
        ModelLit::Int(i) => crate::expr::lit_int(*i),
        ModelLit::Float(f) => Expr::Literal(crate::sql::expr::Literal::Float(*f)),
        ModelLit::String(s) => crate::expr::lit_str(s),
        // SQL expr doesn't have Date/Timestamp/Interval - emit as string literal for now
        ModelLit::Date(d) => crate::expr::lit_str(d),
        ModelLit::Timestamp(t) => crate::expr::lit_str(t),
        ModelLit::Interval { value, unit } => {
            // Emit as INTERVAL 'value' unit
            crate::expr::lit_str(&format!("INTERVAL '{}' {:?}", value, unit))
        }
    }
}

fn convert_binary_op(op: &crate::model::expr::BinaryOp) -> crate::sql::expr::BinaryOperator {
    use crate::model::expr::BinaryOp as ModelOp;
    use crate::sql::expr::BinaryOperator as SqlOp;

    match op {
        ModelOp::Add => SqlOp::Plus,
        ModelOp::Sub => SqlOp::Minus,
        ModelOp::Mul => SqlOp::Mul,
        ModelOp::Div => SqlOp::Div,
        ModelOp::Mod => SqlOp::Mod,
        ModelOp::Eq => SqlOp::Eq,
        ModelOp::Ne => SqlOp::Ne,
        ModelOp::Lt => SqlOp::Lt,
        ModelOp::Lte => SqlOp::Lte,
        ModelOp::Gt => SqlOp::Gt,
        ModelOp::Gte => SqlOp::Gte,
        ModelOp::And => SqlOp::And,
        ModelOp::Or => SqlOp::Or,
        ModelOp::Like => SqlOp::Like,
        ModelOp::ILike => SqlOp::Like, // Fallback to LIKE (dialect should handle case sensitivity)
        ModelOp::Concat => SqlOp::Concat,
        ModelOp::In => SqlOp::Eq,        // Placeholder - IN needs special handling
        ModelOp::NotIn => SqlOp::Ne,     // Placeholder - NOT IN needs special handling
        ModelOp::Between => SqlOp::Gte,  // Placeholder - BETWEEN needs special handling
        ModelOp::NotBetween => SqlOp::Lt, // Placeholder - NOT BETWEEN needs special handling
    }
}


fn convert_func(func: &crate::model::expr::Func) -> String {
    use crate::model::expr::Func;

    match func {
        // Aggregates
        Func::Count => "COUNT".to_string(),
        Func::Sum => "SUM".to_string(),
        Func::Avg => "AVG".to_string(),
        Func::Min => "MIN".to_string(),
        Func::Max => "MAX".to_string(),
        Func::CountDistinct => "COUNT".to_string(),
        // String
        Func::Upper => "UPPER".to_string(),
        Func::Lower => "LOWER".to_string(),
        Func::InitCap => "INITCAP".to_string(),
        Func::Trim => "TRIM".to_string(),
        Func::LTrim => "LTRIM".to_string(),
        Func::RTrim => "RTRIM".to_string(),
        Func::Left => "LEFT".to_string(),
        Func::Right => "RIGHT".to_string(),
        Func::Substring => "SUBSTRING".to_string(),
        Func::Length => "LENGTH".to_string(),
        Func::Replace => "REPLACE".to_string(),
        Func::Concat => "CONCAT".to_string(),
        Func::SplitPart => "SPLIT_PART".to_string(),
        Func::RegexpReplace => "REGEXP_REPLACE".to_string(),
        Func::RegexpExtract => "REGEXP_EXTRACT".to_string(),
        // Null handling
        Func::Coalesce => "COALESCE".to_string(),
        Func::NullIf => "NULLIF".to_string(),
        Func::IfNull => "IFNULL".to_string(),
        // Date/Time
        Func::DateTrunc => "DATE_TRUNC".to_string(),
        Func::Extract => "EXTRACT".to_string(),
        Func::DateAdd => "DATEADD".to_string(),
        Func::DateSub => "DATESUB".to_string(),
        Func::DateDiff => "DATEDIFF".to_string(),
        Func::CurrentDate => "CURRENT_DATE".to_string(),
        Func::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
        Func::ToDate => "DATE".to_string(),
        Func::Year => "YEAR".to_string(),
        Func::Month => "MONTH".to_string(),
        Func::Day => "DAY".to_string(),
        Func::Hour => "HOUR".to_string(),
        Func::Minute => "MINUTE".to_string(),
        Func::Second => "SECOND".to_string(),
        Func::DayOfWeek => "DAYOFWEEK".to_string(),
        Func::DayOfYear => "DAYOFYEAR".to_string(),
        Func::WeekOfYear => "WEEKOFYEAR".to_string(),
        Func::Quarter => "QUARTER".to_string(),
        Func::LastDay => "LAST_DAY".to_string(),
        Func::MakeDate => "MAKE_DATE".to_string(),
        Func::MakeTimestamp => "MAKE_TIMESTAMP".to_string(),
        // Numeric
        Func::Round => "ROUND".to_string(),
        Func::Floor => "FLOOR".to_string(),
        Func::Ceil => "CEIL".to_string(),
        Func::Abs => "ABS".to_string(),
        Func::Sign => "SIGN".to_string(),
        Func::Power => "POWER".to_string(),
        Func::Sqrt => "SQRT".to_string(),
        Func::Exp => "EXP".to_string(),
        Func::Ln => "LN".to_string(),
        Func::Log => "LOG".to_string(),
        Func::Log10 => "LOG10".to_string(),
        Func::Mod => "MOD".to_string(),
        Func::Truncate => "TRUNCATE".to_string(),
        Func::Random => "RANDOM".to_string(),
        // Conditional
        Func::If => "IF".to_string(),
        Func::Greatest => "GREATEST".to_string(),
        Func::Least => "LEAST".to_string(),
        // Type conversion
        Func::Cast => "CAST".to_string(),
        Func::TryCast => "TRY_CAST".to_string(),
        Func::ToChar => "TO_CHAR".to_string(),
        Func::ToNumber => "TO_NUMBER".to_string(),
        // Hash
        Func::Md5 => "MD5".to_string(),
        Func::Sha256 => "SHA256".to_string(),
        Func::Sha1 => "SHA1".to_string(),
        // Array
        Func::ArrayAgg => "ARRAY_AGG".to_string(),
        Func::StringAgg => "STRING_AGG".to_string(),
        Func::ArrayLength => "ARRAY_LENGTH".to_string(),
        // JSON
        Func::JsonExtract => "JSON_EXTRACT".to_string(),
        Func::JsonExtractText => "JSON_EXTRACT_SCALAR".to_string(),
        Func::JsonArrayLength => "JSON_ARRAY_LENGTH".to_string(),
    }
}
