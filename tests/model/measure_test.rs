#[cfg(test)]
mod tests {
    use mantis::model::expr::{AggregateFunc, BinaryOp, Expr, Func, Literal};
    use mantis::model::{Measure, MeasureBlock, NullHandling};
    use std::collections::HashMap;

    /// Helper to create a simple aggregate expression like sum(@atom)
    fn sum_atom(atom_name: &str) -> Expr {
        Expr::Function {
            func: Func::Aggregate(AggregateFunc::Sum),
            args: vec![Expr::AtomRef(atom_name.to_string())],
        }
    }

    /// Helper to create a column reference expression
    fn column_ref(name: &str) -> Expr {
        Expr::Column {
            entity: None,
            column: name.to_string(),
        }
    }

    /// Helper to create a binary subtraction expression
    fn subtract(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Sub,
            right: Box::new(right),
        }
    }

    /// Helper to create a simple comparison expression
    fn column_eq_string(column: &str, value: &str) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                entity: None,
                column: column.to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String(value.to_string()))),
        }
    }

    #[test]
    fn test_measure_with_atom_syntax() {
        let expr = sum_atom("revenue");

        let measure = Measure {
            name: "total_revenue".to_string(),
            expr,
            filter: None,
            null_handling: None,
        };

        assert_eq!(measure.name, "total_revenue");
        // Check that the expression contains an AtomRef to "revenue"
        match &measure.expr {
            Expr::Function { args, .. } => {
                assert!(matches!(&args[0], Expr::AtomRef(name) if name == "revenue"));
            }
            _ => panic!("Expected Function expression"),
        }
        assert!(measure.filter.is_none());
    }

    #[test]
    fn test_measure_block() {
        let mut measures = HashMap::new();

        measures.insert(
            "revenue".to_string(),
            Measure {
                name: "revenue".to_string(),
                expr: sum_atom("amount"),
                filter: None,
                null_handling: None,
            },
        );

        measures.insert(
            "margin".to_string(),
            Measure {
                name: "margin".to_string(),
                expr: subtract(column_ref("revenue"), column_ref("cost")), // References other measures
                filter: None,
                null_handling: None,
            },
        );

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
            expr: sum_atom("amount"),
            filter: Some(column_eq_string("segment", "Enterprise")),
            null_handling: Some(NullHandling::CoalesceZero),
        };

        assert!(measure.filter.is_some());
        assert_eq!(measure.null_handling, Some(NullHandling::CoalesceZero));
    }
}
