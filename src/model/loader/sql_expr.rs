//! SQL expression parser.
//!
//! Converts SQL expression strings into our internal Expr AST using sqlparser.
//! This allows users to write natural SQL expressions in their Lua model files:
//!
//! ```lua
//! source "orders" {
//!     filter = "status != 'cancelled' AND total > 0",
//! }
//!
//! intermediate "enriched" {
//!     columns = {
//!         line_total = "quantity * unit_price * (1 - discount / 100)",
//!     },
//! }
//! ```

use sqlparser::ast::{
    self as sql, BinaryOperator as SqlBinaryOp, Expr as SqlExpr, UnaryOperator as SqlUnaryOp,
    Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::model::{
    BinaryOp, DataType, Expr, FrameBound, FrameKind, Func, Literal, NullsOrder, OrderByExpr,
    SortDir, UnaryOp, WhenClause, WindowFrame, WindowFunc,
};

/// Error type for SQL expression parsing.
#[derive(Debug, Clone)]
pub struct SqlExprError {
    pub message: String,
    pub sql: String,
}

impl std::fmt::Display for SqlExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQL expression error: {} in '{}'", self.message, self.sql)
    }
}

impl std::error::Error for SqlExprError {}

/// Parse a SQL expression string into our Expr AST.
///
/// # Examples
///
/// ```ignore
/// let expr = parse_sql_expr("status = 'active'")?;
/// let expr = parse_sql_expr("quantity * unit_price")?;
/// let expr = parse_sql_expr("COALESCE(discount, 0)")?;
/// ```
pub fn parse_sql_expr(sql: &str) -> Result<Expr, SqlExprError> {
    let dialect = GenericDialect {};

    // Wrap in SELECT to make it a valid SQL statement
    let wrapped = format!("SELECT {}", sql);

    let statements = Parser::parse_sql(&dialect, &wrapped).map_err(|e| SqlExprError {
        message: e.to_string(),
        sql: sql.to_string(),
    })?;

    if statements.len() != 1 {
        return Err(SqlExprError {
            message: "Expected single expression".to_string(),
            sql: sql.to_string(),
        });
    }

    // Extract the expression from SELECT
    match &statements[0] {
        sql::Statement::Query(query) => match query.body.as_ref() {
            sql::SetExpr::Select(select) => {
                if select.projection.len() != 1 {
                    return Err(SqlExprError {
                        message: "Expected single expression".to_string(),
                        sql: sql.to_string(),
                    });
                }
                match &select.projection[0] {
                    sql::SelectItem::UnnamedExpr(expr) => convert_expr(expr, sql),
                    sql::SelectItem::ExprWithAlias { expr, .. } => convert_expr(expr, sql),
                    _ => Err(SqlExprError {
                        message: "Unexpected projection type".to_string(),
                        sql: sql.to_string(),
                    }),
                }
            }
            _ => Err(SqlExprError {
                message: "Expected SELECT expression".to_string(),
                sql: sql.to_string(),
            }),
        },
        _ => Err(SqlExprError {
            message: "Expected SELECT statement".to_string(),
            sql: sql.to_string(),
        }),
    }
}

/// Convert a sqlparser Expr to our Expr type.
fn convert_expr(expr: &SqlExpr, original_sql: &str) -> Result<Expr, SqlExprError> {
    match expr {
        // Column reference: identifier or compound identifier
        SqlExpr::Identifier(ident) => Ok(Expr::Column {
            entity: None,
            column: ident.value.clone(),
        }),

        SqlExpr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok(Expr::Column {
                    entity: Some(idents[0].value.clone()),
                    column: idents[1].value.clone(),
                })
            } else if idents.len() == 1 {
                Ok(Expr::Column {
                    entity: None,
                    column: idents[0].value.clone(),
                })
            } else {
                // For longer paths like schema.table.column, just use last two parts
                let len = idents.len();
                Ok(Expr::Column {
                    entity: Some(idents[len - 2].value.clone()),
                    column: idents[len - 1].value.clone(),
                })
            }
        }

        // Literals
        SqlExpr::Value(value) => convert_value(value, original_sql),

        // Binary operations
        SqlExpr::BinaryOp { left, op, right } => {
            let left_expr = convert_expr(left, original_sql)?;
            let right_expr = convert_expr(right, original_sql)?;
            let binary_op = convert_binary_op(op, original_sql)?;
            Ok(Expr::BinaryOp {
                left: Box::new(left_expr),
                op: binary_op,
                right: Box::new(right_expr),
            })
        }

        // Unary operations
        SqlExpr::UnaryOp { op, expr } => {
            let inner = convert_expr(expr, original_sql)?;
            let unary_op = convert_unary_op(op, original_sql)?;
            Ok(Expr::UnaryOp {
                op: unary_op,
                expr: Box::new(inner),
            })
        }

        // Negation (special case for negative numbers)
        SqlExpr::Nested(inner) => convert_expr(inner, original_sql),

        // Function calls
        SqlExpr::Function(func) => convert_function(func, original_sql),

        // CASE WHEN expressions
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand_expr = operand
                .as_ref()
                .map(|e| convert_expr(e, original_sql))
                .transpose()?
                .map(Box::new);

            let mut when_clauses = Vec::new();
            for (condition, result) in conditions.iter().zip(results.iter()) {
                when_clauses.push(WhenClause {
                    condition: convert_expr(condition, original_sql)?,
                    result: convert_expr(result, original_sql)?,
                });
            }

            let else_clause = else_result
                .as_ref()
                .map(|e| convert_expr(e, original_sql))
                .transpose()?
                .map(Box::new);

            Ok(Expr::Case {
                operand: operand_expr,
                when_clauses,
                else_clause,
            })
        }

        // CAST expressions
        SqlExpr::Cast {
            expr, data_type, ..
        } => {
            let inner = convert_expr(expr, original_sql)?;
            let target_type = convert_data_type(data_type, original_sql)?;
            Ok(Expr::Cast {
                expr: Box::new(inner),
                target_type,
            })
        }

        // IS NULL / IS NOT NULL
        SqlExpr::IsNull(inner) => Ok(Expr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(convert_expr(inner, original_sql)?),
        }),

        SqlExpr::IsNotNull(inner) => Ok(Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(convert_expr(inner, original_sql)?),
        }),

        // BETWEEN
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let inner = convert_expr(expr, original_sql)?;
            let low_expr = convert_expr(low, original_sql)?;
            let high_expr = convert_expr(high, original_sql)?;

            // BETWEEN is expr >= low AND expr <= high
            let low_cmp = Expr::BinaryOp {
                left: Box::new(inner.clone()),
                op: BinaryOp::Gte,
                right: Box::new(low_expr),
            };
            let high_cmp = Expr::BinaryOp {
                left: Box::new(inner),
                op: BinaryOp::Lte,
                right: Box::new(high_expr),
            };
            let combined = Expr::BinaryOp {
                left: Box::new(low_cmp),
                op: BinaryOp::And,
                right: Box::new(high_cmp),
            };

            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(combined),
                })
            } else {
                Ok(combined)
            }
        }

        // IN list
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let inner = convert_expr(expr, original_sql)?;

            // Convert to series of OR comparisons
            if list.is_empty() {
                return Ok(Expr::Literal(Literal::Bool(!negated)));
            }

            let mut result = Expr::BinaryOp {
                left: Box::new(inner.clone()),
                op: BinaryOp::Eq,
                right: Box::new(convert_expr(&list[0], original_sql)?),
            };

            for item in &list[1..] {
                let cmp = Expr::BinaryOp {
                    left: Box::new(inner.clone()),
                    op: BinaryOp::Eq,
                    right: Box::new(convert_expr(item, original_sql)?),
                };
                result = Expr::BinaryOp {
                    left: Box::new(result),
                    op: BinaryOp::Or,
                    right: Box::new(cmp),
                };
            }

            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(result),
                })
            } else {
                Ok(result)
            }
        }

        // LIKE
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let inner = convert_expr(expr, original_sql)?;
            let pattern_expr = convert_expr(pattern, original_sql)?;
            let like_expr = Expr::BinaryOp {
                left: Box::new(inner),
                op: BinaryOp::Like,
                right: Box::new(pattern_expr),
            };
            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(like_expr),
                })
            } else {
                Ok(like_expr)
            }
        }

        // ILike (case-insensitive LIKE)
        SqlExpr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let inner = convert_expr(expr, original_sql)?;
            let pattern_expr = convert_expr(pattern, original_sql)?;
            let ilike_expr = Expr::BinaryOp {
                left: Box::new(inner),
                op: BinaryOp::ILike,
                right: Box::new(pattern_expr),
            };
            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(ilike_expr),
                })
            } else {
                Ok(ilike_expr)
            }
        }

        // TRIM function
        SqlExpr::Trim { expr, .. } => {
            let inner = convert_expr(expr, original_sql)?;
            Ok(Expr::Function {
                func: Func::Trim,
                args: vec![inner],
            })
        }

        // Subquery and other unsupported expressions
        _ => Err(SqlExprError {
            message: format!("Unsupported expression type: {:?}", expr),
            sql: original_sql.to_string(),
        }),
    }
}

/// Convert a SQL value to our Literal type.
fn convert_value(value: &SqlValue, original_sql: &str) -> Result<Expr, SqlExprError> {
    match value {
        SqlValue::Number(n, _) => {
            // Try to parse as integer first, then float
            if let Ok(i) = n.parse::<i64>() {
                Ok(Expr::Literal(Literal::Int(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Expr::Literal(Literal::Float(f)))
            } else {
                Err(SqlExprError {
                    message: format!("Invalid number: {}", n),
                    sql: original_sql.to_string(),
                })
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Expr::Literal(Literal::String(s.clone())))
        }
        SqlValue::Boolean(b) => Ok(Expr::Literal(Literal::Bool(*b))),
        SqlValue::Null => Ok(Expr::Literal(Literal::Null)),
        _ => Err(SqlExprError {
            message: format!("Unsupported value type: {:?}", value),
            sql: original_sql.to_string(),
        }),
    }
}

/// Convert a SQL binary operator to our BinaryOp type.
fn convert_binary_op(op: &SqlBinaryOp, original_sql: &str) -> Result<BinaryOp, SqlExprError> {
    match op {
        SqlBinaryOp::Plus => Ok(BinaryOp::Add),
        SqlBinaryOp::Minus => Ok(BinaryOp::Sub),
        SqlBinaryOp::Multiply => Ok(BinaryOp::Mul),
        SqlBinaryOp::Divide => Ok(BinaryOp::Div),
        SqlBinaryOp::Modulo => Ok(BinaryOp::Mod),
        SqlBinaryOp::Eq => Ok(BinaryOp::Eq),
        SqlBinaryOp::NotEq => Ok(BinaryOp::Ne),
        SqlBinaryOp::Lt => Ok(BinaryOp::Lt),
        SqlBinaryOp::LtEq => Ok(BinaryOp::Lte),
        SqlBinaryOp::Gt => Ok(BinaryOp::Gt),
        SqlBinaryOp::GtEq => Ok(BinaryOp::Gte),
        SqlBinaryOp::And => Ok(BinaryOp::And),
        SqlBinaryOp::Or => Ok(BinaryOp::Or),
        SqlBinaryOp::StringConcat => Ok(BinaryOp::Concat),
        _ => Err(SqlExprError {
            message: format!("Unsupported binary operator: {:?}", op),
            sql: original_sql.to_string(),
        }),
    }
}

/// Convert a SQL unary operator to our UnaryOp type.
fn convert_unary_op(op: &SqlUnaryOp, original_sql: &str) -> Result<UnaryOp, SqlExprError> {
    match op {
        SqlUnaryOp::Not => Ok(UnaryOp::Not),
        SqlUnaryOp::Minus => Ok(UnaryOp::Neg),
        SqlUnaryOp::Plus => {
            // Unary plus is essentially a no-op, but we don't have it
            // Return an error for now
            Err(SqlExprError {
                message: "Unary plus not supported".to_string(),
                sql: original_sql.to_string(),
            })
        }
        _ => Err(SqlExprError {
            message: format!("Unsupported unary operator: {:?}", op),
            sql: original_sql.to_string(),
        }),
    }
}

/// Convert a SQL function call to our Expr type.
fn convert_function(func: &sql::Function, original_sql: &str) -> Result<Expr, SqlExprError> {
    let name = func.name.to_string().to_lowercase();

    // Convert arguments - handle FunctionArguments enum
    let args: Result<Vec<Expr>, SqlExprError> = match &func.args {
        sql::FunctionArguments::List(arg_list) => {
            arg_list.args.iter()
                .filter_map(|arg| match arg {
                    sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => {
                        Some(convert_expr(e, original_sql))
                    }
                    sql::FunctionArg::Named { arg: sql::FunctionArgExpr::Expr(e), .. } => {
                        Some(convert_expr(e, original_sql))
                    }
                    sql::FunctionArg::Named { .. } => None,
                    _ => None,
                })
                .collect()
        }
        sql::FunctionArguments::None => Ok(vec![]),
        sql::FunctionArguments::Subquery(_) => {
            return Err(SqlExprError {
                message: "Subquery arguments not supported".to_string(),
                sql: original_sql.to_string(),
            });
        }
    };
    let args = args?;

    // Check for window function with OVER clause
    if func.over.is_some() {
        return convert_window_function(&name, args, func, original_sql);
    }

    // Map function name to our Func enum
    let func_type = match name.as_str() {
        // Aggregates
        "count" => Func::Count,
        "sum" => Func::Sum,
        "avg" => Func::Avg,
        "min" => Func::Min,
        "max" => Func::Max,
        "count_distinct" => Func::CountDistinct,

        // String functions
        "upper" => Func::Upper,
        "lower" => Func::Lower,
        "initcap" => Func::InitCap,
        "trim" => Func::Trim,
        "ltrim" => Func::LTrim,
        "rtrim" => Func::RTrim,
        "length" | "len" => Func::Length,
        "concat" => Func::Concat,
        "substring" | "substr" => Func::Substring,
        "replace" => Func::Replace,
        "left" => Func::Left,
        "right" => Func::Right,
        "split_part" => Func::SplitPart,
        "regexp_replace" => Func::RegexpReplace,
        "regexp_extract" | "regexp_substr" => Func::RegexpExtract,

        // Date functions
        "date_trunc" | "datetrunc" => Func::DateTrunc,
        "date_part" | "datepart" | "extract" => Func::Extract,
        "date_add" | "dateadd" => Func::DateAdd,
        "date_sub" | "datesub" => Func::DateSub,
        "date_diff" | "datediff" => Func::DateDiff,
        "current_date" => Func::CurrentDate,
        "current_timestamp" | "now" | "getdate" => Func::CurrentTimestamp,
        "year" => Func::Year,
        "month" => Func::Month,
        "day" => Func::Day,
        "hour" => Func::Hour,
        "minute" => Func::Minute,
        "second" => Func::Second,
        "dayofweek" | "day_of_week" => Func::DayOfWeek,
        "dayofyear" | "day_of_year" => Func::DayOfYear,
        "weekofyear" | "week_of_year" | "week" => Func::WeekOfYear,
        "quarter" => Func::Quarter,
        "last_day" => Func::LastDay,
        "to_date" | "date" => Func::ToDate,
        "make_date" => Func::MakeDate,
        "make_timestamp" => Func::MakeTimestamp,

        // Numeric functions
        "round" => Func::Round,
        "floor" => Func::Floor,
        "ceil" | "ceiling" => Func::Ceil,
        "abs" => Func::Abs,
        "power" | "pow" => Func::Power,
        "sqrt" => Func::Sqrt,
        "log" => Func::Log,
        "log10" => Func::Log10,
        "ln" => Func::Ln,
        "exp" => Func::Exp,
        "sign" => Func::Sign,
        "mod" => Func::Mod,
        "truncate" | "trunc" => Func::Truncate,
        "random" | "rand" => Func::Random,

        // Conditional
        "if" | "iff" | "iif" => Func::If,
        "coalesce" => Func::Coalesce,
        "nullif" => Func::NullIf,
        "ifnull" | "isnull" | "nvl" => Func::IfNull,
        "greatest" => Func::Greatest,
        "least" => Func::Least,

        // Type conversion
        "cast" => Func::Cast,
        "try_cast" => Func::TryCast,
        "to_char" => Func::ToChar,
        "to_number" => Func::ToNumber,

        // Hash functions
        "md5" => Func::Md5,
        "sha256" | "sha2" => Func::Sha256,
        "sha1" => Func::Sha1,

        // Array/Aggregates
        "array_agg" => Func::ArrayAgg,
        "string_agg" | "listagg" | "group_concat" => Func::StringAgg,
        "array_length" | "cardinality" => Func::ArrayLength,

        // JSON
        "json_extract" | "json_value" => Func::JsonExtract,
        "json_extract_text" => Func::JsonExtractText,
        "json_array_length" => Func::JsonArrayLength,

        _ => {
            return Err(SqlExprError {
                message: format!("Unknown function: {}", name),
                sql: original_sql.to_string(),
            })
        }
    };

    Ok(Expr::Function {
        func: func_type,
        args,
    })
}

/// Convert a window function to our Expr::Window type.
fn convert_window_function(
    name: &str,
    args: Vec<Expr>,
    func: &sql::Function,
    original_sql: &str,
) -> Result<Expr, SqlExprError> {
    let window_func = match name {
        "row_number" => WindowFunc::RowNumber,
        "rank" => WindowFunc::Rank,
        "dense_rank" => WindowFunc::DenseRank,
        "ntile" => WindowFunc::NTile,
        "percent_rank" => WindowFunc::PercentRank,
        "cume_dist" => WindowFunc::CumeDist,
        "lag" => WindowFunc::Lag,
        "lead" => WindowFunc::Lead,
        "first_value" => WindowFunc::FirstValue,
        "last_value" => WindowFunc::LastValue,
        "nth_value" => WindowFunc::NthValue,
        "sum" => WindowFunc::Sum,
        "count" => WindowFunc::Count,
        "avg" => WindowFunc::Avg,
        "min" => WindowFunc::Min,
        "max" => WindowFunc::Max,
        _ => {
            return Err(SqlExprError {
                message: format!("Unknown window function: {}", name),
                sql: original_sql.to_string(),
            })
        }
    };

    let over = func.over.as_ref().unwrap();

    // Handle WindowType - we need to check if it's WindowSpec
    let (partition_by, order_by, frame) = match over {
        sql::WindowType::WindowSpec(spec) => {
            // Convert partition by
            let partition_by: Result<Vec<Expr>, SqlExprError> = spec
                .partition_by
                .iter()
                .map(|e| convert_expr(e, original_sql))
                .collect();
            let partition_by = partition_by?;

            // Convert order by
            let order_by: Result<Vec<OrderByExpr>, SqlExprError> = spec
                .order_by
                .iter()
                .map(|o| convert_order_by(o, original_sql))
                .collect();
            let order_by = order_by?;

            // Convert frame
            let frame = spec
                .window_frame
                .as_ref()
                .map(|f| convert_window_frame(f, original_sql))
                .transpose()?;

            (partition_by, order_by, frame)
        }
        sql::WindowType::NamedWindow(_name) => {
            // Named windows aren't supported in our DSL
            return Err(SqlExprError {
                message: "Named windows not supported".to_string(),
                sql: original_sql.to_string(),
            });
        }
    };

    Ok(Expr::Window {
        func: window_func,
        args,
        partition_by,
        order_by,
        frame,
    })
}

/// Convert a SQL ORDER BY expression to our OrderByExpr type.
fn convert_order_by(order: &sql::OrderByExpr, original_sql: &str) -> Result<OrderByExpr, SqlExprError> {
    let expr = convert_expr(&order.expr, original_sql)?;
    let dir = if order.asc == Some(false) {
        SortDir::Desc
    } else {
        SortDir::Asc
    };
    let nulls = order.nulls_first.map(|first| {
        if first {
            NullsOrder::First
        } else {
            NullsOrder::Last
        }
    });

    Ok(OrderByExpr { expr, dir, nulls })
}

/// Convert a SQL window frame to our WindowFrame type.
fn convert_window_frame(
    frame: &sql::WindowFrame,
    original_sql: &str,
) -> Result<WindowFrame, SqlExprError> {
    let kind = match frame.units {
        sql::WindowFrameUnits::Rows => FrameKind::Rows,
        sql::WindowFrameUnits::Range => FrameKind::Range,
        sql::WindowFrameUnits::Groups => FrameKind::Groups,
    };

    let start = convert_frame_bound(&frame.start_bound, original_sql)?;
    let end = frame
        .end_bound
        .as_ref()
        .map(|b| convert_frame_bound(b, original_sql))
        .transpose()?;

    Ok(WindowFrame { kind, start, end })
}

/// Convert a SQL frame bound to our FrameBound type.
fn convert_frame_bound(
    bound: &sql::WindowFrameBound,
    _original_sql: &str,
) -> Result<FrameBound, SqlExprError> {
    match bound {
        sql::WindowFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        sql::WindowFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
        sql::WindowFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        sql::WindowFrameBound::Preceding(Some(n)) => {
            // Try to extract numeric value
            if let SqlExpr::Value(SqlValue::Number(s, _)) = n.as_ref() {
                if let Ok(num) = s.parse::<u32>() {
                    return Ok(FrameBound::Preceding(num));
                }
            }
            Ok(FrameBound::Preceding(1)) // Default fallback
        }
        sql::WindowFrameBound::Following(Some(n)) => {
            if let SqlExpr::Value(SqlValue::Number(s, _)) = n.as_ref() {
                if let Ok(num) = s.parse::<u32>() {
                    return Ok(FrameBound::Following(num));
                }
            }
            Ok(FrameBound::Following(1)) // Default fallback
        }
    }
}

/// Convert a SQL data type to our DataType enum.
fn convert_data_type(dt: &sql::DataType, original_sql: &str) -> Result<DataType, SqlExprError> {
    match dt {
        sql::DataType::Int8(_) | sql::DataType::TinyInt(_) => Ok(DataType::Int8),
        sql::DataType::Int16 | sql::DataType::SmallInt(_) => Ok(DataType::Int16),
        sql::DataType::Int32 | sql::DataType::Int(_) | sql::DataType::Integer(_) => {
            Ok(DataType::Int32)
        }
        sql::DataType::Int64 | sql::DataType::BigInt(_) => Ok(DataType::Int64),
        sql::DataType::Float32 | sql::DataType::Real => Ok(DataType::Float32),
        sql::DataType::Float64 | sql::DataType::Double | sql::DataType::DoublePrecision => {
            Ok(DataType::Float64)
        }
        sql::DataType::Decimal(info) | sql::DataType::Numeric(info) => {
            let (precision, scale) = match info {
                sql::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                sql::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sql::ExactNumberInfo::None => (38, 9), // Default precision/scale
            };
            Ok(DataType::Decimal(precision, scale))
        }
        sql::DataType::Boolean => Ok(DataType::Bool),
        sql::DataType::Varchar(_) | sql::DataType::Char(_) | sql::DataType::Text => {
            Ok(DataType::String)
        }
        sql::DataType::String(_) => Ok(DataType::String),
        sql::DataType::Date => Ok(DataType::Date),
        sql::DataType::Time(_, _) => Ok(DataType::Time),
        sql::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        sql::DataType::Datetime(_) => Ok(DataType::Timestamp),
        sql::DataType::Uuid => Ok(DataType::Uuid),
        sql::DataType::JSON => Ok(DataType::Json),
        sql::DataType::Blob(_) | sql::DataType::Binary(_) | sql::DataType::Varbinary(_) => {
            Ok(DataType::Binary)
        }
        _ => Err(SqlExprError {
            message: format!("Unsupported data type: {:?}", dt),
            sql: original_sql.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_column() {
        let expr = parse_sql_expr("status").unwrap();
        assert!(matches!(
            expr,
            Expr::Column { entity: None, column } if column == "status"
        ));
    }

    #[test]
    fn test_qualified_column() {
        let expr = parse_sql_expr("orders.status").unwrap();
        assert!(matches!(
            expr,
            Expr::Column { entity: Some(e), column } if e == "orders" && column == "status"
        ));
    }

    #[test]
    fn test_string_literal() {
        let expr = parse_sql_expr("'active'").unwrap();
        assert!(matches!(
            expr,
            Expr::Literal(Literal::String(s)) if s == "active"
        ));
    }

    #[test]
    fn test_number_literal() {
        let expr = parse_sql_expr("42").unwrap();
        assert!(matches!(expr, Expr::Literal(Literal::Int(42))));

        let expr = parse_sql_expr("2.75").unwrap();
        assert!(matches!(expr, Expr::Literal(Literal::Float(f)) if (f - 2.75).abs() < 0.001));
    }

    #[test]
    fn test_comparison() {
        let expr = parse_sql_expr("status = 'active'").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Eq, .. }));

        let expr = parse_sql_expr("status != 'cancelled'").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Ne, .. }));

        let expr = parse_sql_expr("total > 100").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Gt, .. }));
    }

    #[test]
    fn test_arithmetic() {
        let expr = parse_sql_expr("quantity * unit_price").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Mul, .. }));

        let expr = parse_sql_expr("price - discount").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Sub, .. }));
    }

    #[test]
    fn test_complex_arithmetic() {
        let expr = parse_sql_expr("quantity * unit_price * (1 - discount / 100)").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Mul, .. }));
    }

    #[test]
    fn test_boolean_logic() {
        let expr = parse_sql_expr("status = 'active' AND total > 0").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::And, .. }));

        let expr = parse_sql_expr("status = 'pending' OR status = 'processing'").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Or, .. }));
    }

    #[test]
    fn test_function_call() {
        let expr = parse_sql_expr("UPPER(name)").unwrap();
        assert!(matches!(
            expr,
            Expr::Function { func: Func::Upper, .. }
        ));

        let expr = parse_sql_expr("COALESCE(discount, 0)").unwrap();
        assert!(matches!(
            expr,
            Expr::Function { func: Func::Coalesce, .. }
        ));
    }

    #[test]
    fn test_case_when() {
        let expr = parse_sql_expr(
            "CASE WHEN status = 'active' THEN 1 WHEN status = 'pending' THEN 2 ELSE 0 END",
        )
        .unwrap();
        assert!(matches!(expr, Expr::Case { .. }));
    }

    #[test]
    fn test_is_null() {
        let expr = parse_sql_expr("discount IS NULL").unwrap();
        assert!(matches!(
            expr,
            Expr::UnaryOp { op: UnaryOp::IsNull, .. }
        ));

        let expr = parse_sql_expr("name IS NOT NULL").unwrap();
        assert!(matches!(
            expr,
            Expr::UnaryOp { op: UnaryOp::IsNotNull, .. }
        ));
    }

    #[test]
    fn test_like() {
        let expr = parse_sql_expr("name LIKE '%test%'").unwrap();
        assert!(matches!(
            expr,
            Expr::BinaryOp { op: BinaryOp::Like, .. }
        ));
    }

    #[test]
    fn test_between() {
        let expr = parse_sql_expr("price BETWEEN 10 AND 100").unwrap();
        // BETWEEN is converted to >= AND <=
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::And, .. }));
    }

    #[test]
    fn test_in_list() {
        let expr = parse_sql_expr("status IN ('active', 'pending')").unwrap();
        // IN is converted to OR of equalities
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOp::Or, .. }));
    }

    #[test]
    fn test_cast() {
        let expr = parse_sql_expr("CAST(amount AS DECIMAL(10, 2))").unwrap();
        assert!(matches!(
            expr,
            Expr::Cast { target_type: DataType::Decimal(10, 2), .. }
        ));
    }

    #[test]
    fn test_aggregate() {
        let expr = parse_sql_expr("SUM(total)").unwrap();
        assert!(matches!(
            expr,
            Expr::Function { func: Func::Sum, .. }
        ));

        let expr = parse_sql_expr("COUNT(*)").unwrap();
        // COUNT(*) comes through without args
        assert!(matches!(
            expr,
            Expr::Function { func: Func::Count, .. }
        ));
    }
}
