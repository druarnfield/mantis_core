#[cfg(test)]
mod tests {
    use mantis_core::model::{Attribute, DataType, Dimension, DrillPath, GrainLevel};
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
            DrillPath {
                name: "geo".to_string(),
                levels: vec![GrainLevel::Day, GrainLevel::Month],
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
    }
}
