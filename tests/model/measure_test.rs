#[cfg(test)]
mod tests {
    use mantis::dsl::span::Span;
    use mantis::model::{Measure, MeasureBlock, NullHandling, SqlExpr};
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

        measures.insert(
            "revenue".to_string(),
            Measure {
                name: "revenue".to_string(),
                expr: SqlExpr {
                    sql: "sum(@amount)".to_string(),
                    span: Span::default(),
                },
                filter: None,
                null_handling: None,
            },
        );

        measures.insert(
            "margin".to_string(),
            Measure {
                name: "margin".to_string(),
                expr: SqlExpr {
                    sql: "revenue - cost".to_string(), // References other measures
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
