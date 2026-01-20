#[cfg(test)]
mod tests {
    use mantis_core::model::{Attribute, DataType, Dimension, DimensionDrillPath};
    use std::collections::HashMap;

    #[test]
    fn test_dimension_with_attributes() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "customer_name".to_string(),
            Attribute {
                name: "customer_name".to_string(),
                data_type: DataType::String,
            },
        );
        attributes.insert(
            "region".to_string(),
            Attribute {
                name: "region".to_string(),
                data_type: DataType::String,
            },
        );

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
        drill_paths.insert(
            "geo".to_string(),
            DimensionDrillPath {
                name: "geo".to_string(),
                levels: vec![
                    "city".to_string(),
                    "state".to_string(),
                    "country".to_string(),
                ],
            },
        );

        let dimension = Dimension {
            name: "customers".to_string(),
            source: "dbo.dim_customers".to_string(),
            key: "customer_id".to_string(),
            attributes: HashMap::new(),
            drill_paths,
        };

        assert_eq!(dimension.drill_paths.len(), 1);
        assert!(dimension.drill_paths.contains_key("geo"));

        // Verify it's attribute names, not grain levels
        let geo_path = dimension.drill_paths.get("geo").unwrap();
        assert_eq!(geo_path.levels, vec!["city", "state", "country"]);
    }
}
