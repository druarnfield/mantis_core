#[cfg(test)]
mod tests {
    use mantis::model::{Atom, AtomType, GrainLevel, Slicer, Table, TimeBinding};
    use std::collections::HashMap;

    #[test]
    fn test_table_with_atoms() {
        let mut atoms = HashMap::new();
        atoms.insert(
            "revenue".to_string(),
            Atom {
                name: "revenue".to_string(),
                data_type: AtomType::Decimal,
            },
        );

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
        times.insert(
            "order_date_id".to_string(),
            TimeBinding {
                name: "order_date_id".to_string(),
                calendar: "dates".to_string(),
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

        assert_eq!(table.times.len(), 1);
        let time = table.times.get("order_date_id").unwrap();
        assert_eq!(time.calendar, "dates");
        assert_eq!(time.grain, GrainLevel::Day);
    }

    #[test]
    fn test_table_with_slicers() {
        let mut slicers = HashMap::new();
        slicers.insert(
            "customer_id".to_string(),
            Slicer::ForeignKey {
                name: "customer_id".to_string(),
                dimension: "customers".to_string(),
                key: "customer_id".to_string(),
            },
        );
        slicers.insert(
            "region".to_string(),
            Slicer::Via {
                name: "region".to_string(),
                fk_slicer: "customer_id".to_string(),
            },
        );

        let table = Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times: HashMap::new(),
            slicers,
        };

        assert_eq!(table.slicers.len(), 2);
        assert!(matches!(
            table.slicers.get("customer_id"),
            Some(Slicer::ForeignKey { .. })
        ));
        assert!(matches!(
            table.slicers.get("region"),
            Some(Slicer::Via { .. })
        ));
    }

    #[test]
    fn test_table_with_inline_slicer() {
        use mantis::model::DataType;

        let mut slicers = HashMap::new();
        slicers.insert(
            "status".to_string(),
            Slicer::Inline {
                name: "status".to_string(),
                data_type: DataType::String,
            },
        );

        let table = Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times: HashMap::new(),
            slicers,
        };

        assert!(matches!(
            table.slicers.get("status"),
            Some(Slicer::Inline { .. })
        ));
    }

    #[test]
    fn test_table_with_calculated_slicer() {
        use mantis::model::expr::{BinaryOp, Expr};
        use mantis::model::DataType;

        // Create an expression: @revenue + @tax
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::AtomRef("revenue".to_string())),
            op: BinaryOp::Add,
            right: Box::new(Expr::AtomRef("tax".to_string())),
        };

        let mut slicers = HashMap::new();
        slicers.insert(
            "total_amount".to_string(),
            Slicer::Calculated {
                name: "total_amount".to_string(),
                data_type: DataType::Decimal,
                expr,
            },
        );

        let table = Table {
            name: "fact_sales".to_string(),
            source: "dbo.fact_sales".to_string(),
            atoms: HashMap::new(),
            times: HashMap::new(),
            slicers,
        };

        assert!(matches!(
            table.slicers.get("total_amount"),
            Some(Slicer::Calculated { .. })
        ));
    }
}
