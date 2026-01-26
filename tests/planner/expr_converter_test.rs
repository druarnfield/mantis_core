use mantis::model::expr::{
    AggregateFunc, BinaryOp as ModelBinaryOp, Expr as ModelExpr, Func, Literal as ModelLiteral,
    ScalarFunc, UnaryOp as ModelUnaryOp,
};
use mantis::planner::expr_converter::{ExprConverter, QueryContext};
use mantis::sql::expr::{BinaryOperator as SqlBinaryOp, Expr as SqlExpr, Literal as SqlLiteral};

#[test]
fn test_query_context_table_aliases() {
    let mut ctx = QueryContext::new();
    ctx.add_table("users".to_string(), "u".to_string());
    ctx.add_table("orders".to_string(), "o".to_string());

    assert_eq!(ctx.get_table_alias("users").unwrap(), "u");
    assert_eq!(ctx.get_table_alias("orders").unwrap(), "o");
    assert!(ctx.get_table_alias("unknown").is_err());
}

// Task 2: Column References

#[test]
fn test_convert_column_reference() {
    let mut context = QueryContext::new();
    context.add_table("sales".to_string(), "s".to_string());

    let model_expr = ModelExpr::Column {
        entity: Some("sales".to_string()),
        column: "amount".to_string(),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Column { table, column } => {
            assert_eq!(table, Some("s".to_string()));
            assert_eq!(column, "amount");
        }
        _ => panic!("Expected Column expression"),
    }
}

#[test]
fn test_convert_column_no_entity() {
    let context = QueryContext::new();

    let model_expr = ModelExpr::Column {
        entity: None,
        column: "total".to_string(),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Column { table, column } => {
            assert_eq!(table, None);
            assert_eq!(column, "total");
        }
        _ => panic!("Expected Column expression"),
    }
}

#[test]
fn test_convert_column_unknown_entity() {
    let context = QueryContext::new();

    let model_expr = ModelExpr::Column {
        entity: Some("unknown".to_string()),
        column: "amount".to_string(),
    };

    let result = ExprConverter::convert(&model_expr, &context);
    assert!(result.is_err());
}

// Task 3: Literals

#[test]
fn test_convert_int_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Int(42));

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Literal(SqlLiteral::Int(val)) => assert_eq!(val, 42),
        _ => panic!("Expected Integer literal"),
    }
}

#[test]
fn test_convert_float_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Float(3.14));

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Literal(SqlLiteral::Float(val)) => assert_eq!(val, 3.14),
        _ => panic!("Expected Float literal"),
    }
}

#[test]
fn test_convert_string_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::String("test".to_string()));

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Literal(SqlLiteral::String(val)) => assert_eq!(val, "test"),
        _ => panic!("Expected String literal"),
    }
}

#[test]
fn test_convert_bool_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Bool(true));

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Literal(SqlLiteral::Bool(val)) => assert_eq!(val, true),
        _ => panic!("Expected Boolean literal"),
    }
}

#[test]
fn test_convert_null_literal() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Literal(ModelLiteral::Null);

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    assert!(matches!(sql_expr, SqlExpr::Literal(SqlLiteral::Null)));
}

// Task 4: Binary Operations

#[test]
fn test_convert_binary_op_eq() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Literal(ModelLiteral::Int(1))),
        op: ModelBinaryOp::Eq,
        right: Box::new(ModelExpr::Literal(ModelLiteral::Int(2))),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::BinaryOp { left, op, right } => {
            assert!(matches!(op, SqlBinaryOp::Eq));
            assert!(matches!(*left, SqlExpr::Literal(SqlLiteral::Int(1))));
            assert!(matches!(*right, SqlExpr::Literal(SqlLiteral::Int(2))));
        }
        _ => panic!("Expected BinaryOp"),
    }
}

// Task 5: Unary Operations and Functions

#[test]
fn test_convert_unary_op_not() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::UnaryOp {
        op: ModelUnaryOp::Not,
        expr: Box::new(ModelExpr::Literal(ModelLiteral::Bool(true))),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::UnaryOp { .. } => {}
        _ => panic!("Expected UnaryOp"),
    }
}

#[test]
fn test_convert_unary_op_is_null() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::UnaryOp {
        op: ModelUnaryOp::IsNull,
        expr: Box::new(ModelExpr::Column {
            entity: None,
            column: "value".to_string(),
        }),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::UnaryOp { .. } => {}
        _ => panic!("Expected UnaryOp"),
    }
}

#[test]
fn test_convert_function_scalar() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Function {
        func: Func::Scalar(ScalarFunc::Upper),
        args: vec![ModelExpr::Literal(ModelLiteral::String("test".to_string()))],
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Function { name, args, .. } => {
            assert_eq!(name, "UPPER");
            assert_eq!(args.len(), 1);
        }
        _ => panic!("Expected Function"),
    }
}

#[test]
fn test_convert_function_aggregate() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Function {
        func: Func::Aggregate(AggregateFunc::Sum),
        args: vec![ModelExpr::Column {
            entity: None,
            column: "amount".to_string(),
        }],
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Function { name, args, .. } => {
            assert_eq!(name, "SUM");
            assert_eq!(args.len(), 1);
        }
        _ => panic!("Expected Function"),
    }
}

#[test]
fn test_convert_case_expression() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::Case {
        conditions: vec![
            (
                ModelExpr::Literal(ModelLiteral::Bool(true)),
                ModelExpr::Literal(ModelLiteral::Int(1)),
            ),
            (
                ModelExpr::Literal(ModelLiteral::Bool(false)),
                ModelExpr::Literal(ModelLiteral::Int(2)),
            ),
        ],
        else_expr: Some(Box::new(ModelExpr::Literal(ModelLiteral::Int(0)))),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::Case {
            when_clauses,
            else_clause,
            ..
        } => {
            assert_eq!(when_clauses.len(), 2);
            assert!(else_clause.is_some());
        }
        _ => panic!("Expected Case"),
    }
}

#[test]
fn test_convert_binary_op_and() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Literal(ModelLiteral::Bool(true))),
        op: ModelBinaryOp::And,
        right: Box::new(ModelExpr::Literal(ModelLiteral::Bool(false))),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::BinaryOp { op, .. } => {
            assert!(matches!(op, SqlBinaryOp::And));
        }
        _ => panic!("Expected BinaryOp"),
    }
}

#[test]
fn test_convert_binary_op_arithmetic() {
    let context = QueryContext::new();
    let model_expr = ModelExpr::BinaryOp {
        left: Box::new(ModelExpr::Literal(ModelLiteral::Int(10))),
        op: ModelBinaryOp::Add,
        right: Box::new(ModelExpr::Literal(ModelLiteral::Int(5))),
    };

    let sql_expr = ExprConverter::convert(&model_expr, &context).unwrap();

    match sql_expr {
        SqlExpr::BinaryOp { op, .. } => {
            assert!(matches!(op, SqlBinaryOp::Plus));
        }
        _ => panic!("Expected BinaryOp"),
    }
}
