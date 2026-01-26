use super::{PlanError, PlanResult};
use crate::model::expr::{
    AggregateFunc, BinaryOp as ModelBinaryOp, Expr as ModelExpr, Func, Literal as ModelLiteral,
    ScalarFunc, UnaryOp as ModelUnaryOp,
};
use crate::sql::expr::{
    BinaryOperator as SqlBinaryOp, Expr as SqlExpr, Literal as SqlLiteral,
    UnaryOperator as SqlUnaryOp,
};
use std::collections::HashMap;

/// Context for expression conversion - provides table aliases.
pub struct QueryContext {
    table_aliases: HashMap<String, String>,
}

impl QueryContext {
    pub fn new() -> Self {
        Self {
            table_aliases: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, entity: String, alias: String) {
        self.table_aliases.insert(entity, alias);
    }

    pub fn get_table_alias(&self, entity: &str) -> PlanResult<&str> {
        self.table_aliases
            .get(entity)
            .map(|s| s.as_str())
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", entity)))
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Stateless expression converter.
pub struct ExprConverter;

impl ExprConverter {
    /// Convert model expression to SQL expression.
    pub fn convert(expr: &ModelExpr, context: &QueryContext) -> PlanResult<SqlExpr> {
        match expr {
            ModelExpr::Column { entity, column } => {
                let table_alias = if let Some(ent) = entity {
                    Some(context.get_table_alias(ent)?.to_string())
                } else {
                    None
                };

                Ok(SqlExpr::Column {
                    table: table_alias,
                    column: column.clone(),
                })
            }

            ModelExpr::Literal(lit) => Ok(SqlExpr::Literal(Self::convert_literal(lit))),

            ModelExpr::BinaryOp { left, op, right } => {
                let left_sql = Self::convert(left, context)?;
                let right_sql = Self::convert(right, context)?;
                let op_sql = Self::convert_binary_op(op)?;

                Ok(SqlExpr::BinaryOp {
                    left: Box::new(left_sql),
                    op: op_sql,
                    right: Box::new(right_sql),
                })
            }

            ModelExpr::UnaryOp { op, expr } => {
                let expr_sql = Self::convert(expr, context)?;
                let op_sql = Self::convert_unary_op(op);

                Ok(SqlExpr::UnaryOp {
                    op: op_sql,
                    expr: Box::new(expr_sql),
                })
            }

            ModelExpr::Function { func, args } => {
                let args_sql: Result<Vec<_>, _> =
                    args.iter().map(|arg| Self::convert(arg, context)).collect();

                let func_name = Self::convert_function(func);

                Ok(SqlExpr::Function {
                    name: func_name,
                    args: args_sql?,
                    distinct: false,
                })
            }

            ModelExpr::Case {
                conditions,
                else_expr,
            } => {
                let when_clauses: Result<Vec<_>, _> = conditions
                    .iter()
                    .map(|(cond, result)| {
                        Ok((
                            Self::convert(cond, context)?,
                            Self::convert(result, context)?,
                        ))
                    })
                    .collect();

                let else_sql = if let Some(else_e) = else_expr {
                    Some(Box::new(Self::convert(else_e, context)?))
                } else {
                    None
                };

                Ok(SqlExpr::Case {
                    operand: None,
                    when_clauses: when_clauses?,
                    else_clause: else_sql,
                })
            }

            ModelExpr::AtomRef(_) => Err(PlanError::LogicalPlanError(
                "AtomRef should be resolved before conversion".to_string(),
            )),

            ModelExpr::Cast { expr, .. } => {
                let expr_sql = Self::convert(expr, context)?;
                // For now, just pass through the expression without CAST
                // Full CAST support requires data type mapping
                Ok(expr_sql)
            }
        }
    }

    fn convert_literal(lit: &ModelLiteral) -> SqlLiteral {
        match lit {
            ModelLiteral::Null => SqlLiteral::Null,
            ModelLiteral::Bool(b) => SqlLiteral::Bool(*b),
            ModelLiteral::Int(i) => SqlLiteral::Int(*i),
            ModelLiteral::Float(f) => SqlLiteral::Float(*f),
            ModelLiteral::String(s) => SqlLiteral::String(s.clone()),
            ModelLiteral::Date(d) => SqlLiteral::String(d.clone()),
            ModelLiteral::Timestamp(ts) => SqlLiteral::String(ts.clone()),
            ModelLiteral::Interval { value, unit } => {
                SqlLiteral::String(format!("INTERVAL '{}' {:?}", value, unit))
            }
        }
    }

    fn convert_binary_op(op: &ModelBinaryOp) -> PlanResult<SqlBinaryOp> {
        Ok(match op {
            // Arithmetic
            ModelBinaryOp::Add => SqlBinaryOp::Plus,
            ModelBinaryOp::Sub => SqlBinaryOp::Minus,
            ModelBinaryOp::Mul => SqlBinaryOp::Mul,
            ModelBinaryOp::Div => SqlBinaryOp::Div,
            ModelBinaryOp::Mod => SqlBinaryOp::Mod,

            // Comparison
            ModelBinaryOp::Eq => SqlBinaryOp::Eq,
            ModelBinaryOp::Ne => SqlBinaryOp::Ne,
            ModelBinaryOp::Lt => SqlBinaryOp::Lt,
            ModelBinaryOp::Gt => SqlBinaryOp::Gt,
            ModelBinaryOp::Lte => SqlBinaryOp::Lte,
            ModelBinaryOp::Gte => SqlBinaryOp::Gte,

            // Logical
            ModelBinaryOp::And => SqlBinaryOp::And,
            ModelBinaryOp::Or => SqlBinaryOp::Or,

            // String
            ModelBinaryOp::Concat => SqlBinaryOp::Concat,

            // Pattern
            ModelBinaryOp::Like => SqlBinaryOp::Like,
            ModelBinaryOp::ILike => {
                return Err(PlanError::LogicalPlanError(
                    "ILIKE not supported in SQL expr (use dialect-specific handling)".to_string(),
                ));
            }

            // Set membership - these need special handling
            ModelBinaryOp::In | ModelBinaryOp::NotIn => {
                return Err(PlanError::LogicalPlanError(
                    "IN/NOT IN requires special handling with InSubquery".to_string(),
                ));
            }

            // Range - needs special BETWEEN syntax
            ModelBinaryOp::Between | ModelBinaryOp::NotBetween => {
                return Err(PlanError::LogicalPlanError(
                    "BETWEEN requires special handling".to_string(),
                ));
            }
        })
    }

    fn convert_unary_op(op: &ModelUnaryOp) -> SqlUnaryOp {
        match op {
            ModelUnaryOp::Not => SqlUnaryOp::Not,
            ModelUnaryOp::Neg => SqlUnaryOp::Minus,
            ModelUnaryOp::IsNull => SqlUnaryOp::Not, // Will be handled differently
            ModelUnaryOp::IsNotNull => SqlUnaryOp::Not, // Will be handled differently
        }
    }

    fn convert_function(func: &Func) -> String {
        match func {
            Func::Aggregate(agg) => match agg {
                AggregateFunc::Count => "COUNT".to_string(),
                AggregateFunc::Sum => "SUM".to_string(),
                AggregateFunc::Avg => "AVG".to_string(),
                AggregateFunc::Min => "MIN".to_string(),
                AggregateFunc::Max => "MAX".to_string(),
                AggregateFunc::CountDistinct => "COUNT".to_string(),
                AggregateFunc::StringAgg => "STRING_AGG".to_string(),
                AggregateFunc::ArrayAgg => "ARRAY_AGG".to_string(),
            },
            Func::Scalar(scalar) => match scalar {
                ScalarFunc::Upper => "UPPER".to_string(),
                ScalarFunc::Lower => "LOWER".to_string(),
                ScalarFunc::InitCap => "INITCAP".to_string(),
                ScalarFunc::Trim => "TRIM".to_string(),
                ScalarFunc::LTrim => "LTRIM".to_string(),
                ScalarFunc::RTrim => "RTRIM".to_string(),
                ScalarFunc::Left => "LEFT".to_string(),
                ScalarFunc::Right => "RIGHT".to_string(),
                ScalarFunc::Substring => "SUBSTRING".to_string(),
                ScalarFunc::Length => "LENGTH".to_string(),
                ScalarFunc::Replace => "REPLACE".to_string(),
                ScalarFunc::Concat => "CONCAT".to_string(),
                ScalarFunc::SplitPart => "SPLIT_PART".to_string(),
                ScalarFunc::RegexpReplace => "REGEXP_REPLACE".to_string(),
                ScalarFunc::RegexpExtract => "REGEXP_EXTRACT".to_string(),
                ScalarFunc::Coalesce => "COALESCE".to_string(),
                ScalarFunc::NullIf => "NULLIF".to_string(),
                ScalarFunc::IfNull => "IFNULL".to_string(),
                ScalarFunc::DateTrunc => "DATE_TRUNC".to_string(),
                ScalarFunc::Extract => "EXTRACT".to_string(),
                ScalarFunc::DateAdd => "DATE_ADD".to_string(),
                ScalarFunc::DateSub => "DATE_SUB".to_string(),
                ScalarFunc::DateDiff => "DATEDIFF".to_string(),
                ScalarFunc::CurrentDate => "CURRENT_DATE".to_string(),
                ScalarFunc::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
                ScalarFunc::ToDate => "DATE".to_string(),
                ScalarFunc::Year => "YEAR".to_string(),
                ScalarFunc::Month => "MONTH".to_string(),
                ScalarFunc::Day => "DAY".to_string(),
                ScalarFunc::Hour => "HOUR".to_string(),
                ScalarFunc::Minute => "MINUTE".to_string(),
                ScalarFunc::Second => "SECOND".to_string(),
                ScalarFunc::DayOfWeek => "DAYOFWEEK".to_string(),
                ScalarFunc::DayOfYear => "DAYOFYEAR".to_string(),
                ScalarFunc::WeekOfYear => "WEEKOFYEAR".to_string(),
                ScalarFunc::Quarter => "QUARTER".to_string(),
                ScalarFunc::LastDay => "LAST_DAY".to_string(),
                ScalarFunc::MakeDate => "MAKE_DATE".to_string(),
                ScalarFunc::MakeTimestamp => "MAKE_TIMESTAMP".to_string(),
                ScalarFunc::Round => "ROUND".to_string(),
                ScalarFunc::Floor => "FLOOR".to_string(),
                ScalarFunc::Ceil => "CEIL".to_string(),
                ScalarFunc::Abs => "ABS".to_string(),
                ScalarFunc::Sign => "SIGN".to_string(),
                ScalarFunc::Power => "POWER".to_string(),
                ScalarFunc::Sqrt => "SQRT".to_string(),
                ScalarFunc::Exp => "EXP".to_string(),
                ScalarFunc::Ln => "LN".to_string(),
                ScalarFunc::Log => "LOG".to_string(),
                ScalarFunc::Log10 => "LOG10".to_string(),
                ScalarFunc::Mod => "MOD".to_string(),
                ScalarFunc::Truncate => "TRUNCATE".to_string(),
                ScalarFunc::Random => "RANDOM".to_string(),
                ScalarFunc::If => "IF".to_string(),
                ScalarFunc::Greatest => "GREATEST".to_string(),
                ScalarFunc::Least => "LEAST".to_string(),
                ScalarFunc::Cast => "CAST".to_string(),
                ScalarFunc::TryCast => "TRY_CAST".to_string(),
                ScalarFunc::ToChar => "TO_CHAR".to_string(),
                ScalarFunc::ToNumber => "TO_NUMBER".to_string(),
                ScalarFunc::Md5 => "MD5".to_string(),
                ScalarFunc::Sha256 => "SHA256".to_string(),
                ScalarFunc::Sha1 => "SHA1".to_string(),
                ScalarFunc::JsonExtract => "JSON_EXTRACT".to_string(),
                ScalarFunc::JsonExtractText => "JSON_EXTRACT_TEXT".to_string(),
                ScalarFunc::JsonArrayLength => "JSON_ARRAY_LENGTH".to_string(),
            },
        }
    }
}
