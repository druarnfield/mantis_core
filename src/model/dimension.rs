//! Dimension table definitions - conformed dimensions to materialize.

use serde::{Deserialize, Serialize};

use super::types::MaterializationStrategy;

/// Default value for `materialized` field in serde deserialization.
fn default_materialized() -> bool {
    true
}

/// A dimension table definition - a conformed dimension to materialize.
///
/// Dimensions are secondary targets of the transform planner. They define:
/// - Which source entity to base the dimension on
/// - Which columns to include
/// - SCD (Slowly Changing Dimension) strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionDefinition {
    /// Logical name for this dimension (e.g., "dim_customers")
    pub name: String,

    /// Physical target table (e.g., "analytics.dim_customers")
    pub target_table: String,

    /// Optional schema override
    pub target_schema: Option<String>,

    /// Whether this dimension is materialized as a physical table.
    ///
    /// When `true` (default), queries against this dimension read from the target table.
    /// When `false`, the dimension is "virtual" - queries are reconstructed from the
    /// source entity at query time.
    #[serde(default = "default_materialized")]
    pub materialized: bool,

    /// Source entity this dimension is based on
    pub source_entity: String,

    /// Columns to include in the dimension
    pub columns: Vec<DimensionColumn>,

    /// Primary key columns
    pub primary_key: Vec<String>,

    /// SCD (Slowly Changing Dimension) strategy
    pub scd_type: SCDType,

    /// How to materialize this dimension
    pub materialization: MaterializationStrategy,
}

/// A column in a dimension table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionColumn {
    /// Source column name
    pub source_column: String,

    /// Target column name (defaults to source_column)
    pub target_column: Option<String>,

    /// Optional description
    pub description: Option<String>,
}

/// Slowly Changing Dimension strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[derive(Default)]
pub enum SCDType {
    /// Type 0 - Fixed attributes, never change
    Type0,

    /// Type 1 - Overwrite, no history kept
    #[default]
    Type1,

    /// Type 2 - Add new row, maintain full history
    Type2 {
        /// Column for effective start date
        effective_from: String,
        /// Column for effective end date (NULL = current)
        effective_to: String,
        /// Column indicating current row (optional)
        is_current: Option<String>,
    },

    /// Type 3 - Add column for previous value (limited history)
    Type3 {
        /// Columns that track previous values
        /// Maps column name to previous value column name
        tracked_columns: Vec<(String, String)>,
    },

    /// Type 6 - Hybrid (Type 1 + Type 2 + Type 3)
    Type6 {
        /// Column for effective start date
        effective_from: String,
        /// Column for effective end date
        effective_to: String,
        /// Column indicating current row
        is_current: String,
        /// Columns that also keep current value in all rows
        current_columns: Vec<String>,
    },
}


impl DimensionDefinition {
    /// Create a new dimension definition.
    pub fn new(
        name: impl Into<String>,
        target_table: impl Into<String>,
        source_entity: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            target_table: target_table.into(),
            target_schema: None,
            materialized: true,
            source_entity: source_entity.into(),
            columns: vec![],
            primary_key: vec![],
            scd_type: SCDType::default(),
            materialization: MaterializationStrategy::default(),
        }
    }

    /// Set whether this dimension is materialized (physical table) or virtual.
    pub fn with_materialized(mut self, materialized: bool) -> Self {
        self.materialized = materialized;
        self
    }

    /// Set the target schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.target_schema = Some(schema.into());
        self
    }

    /// Add a column (same name in source and target).
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        let col = column.into();
        self.columns.push(DimensionColumn {
            source_column: col.clone(),
            target_column: None,
            description: None,
        });
        self
    }

    /// Add a column with rename.
    pub fn with_column_as(
        mut self,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        self.columns.push(DimensionColumn {
            source_column: source.into(),
            target_column: Some(target.into()),
            description: None,
        });
        self
    }

    /// Add multiple columns.
    pub fn with_columns(mut self, columns: Vec<impl Into<String>>) -> Self {
        for col in columns {
            let col = col.into();
            self.columns.push(DimensionColumn {
                source_column: col.clone(),
                target_column: None,
                description: None,
            });
        }
        self
    }

    /// Set the primary key.
    pub fn with_primary_key(mut self, columns: Vec<impl Into<String>>) -> Self {
        self.primary_key = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Set the SCD type.
    pub fn with_scd_type(mut self, scd_type: SCDType) -> Self {
        self.scd_type = scd_type;
        self
    }

    /// Set the materialization strategy.
    pub fn with_materialization(mut self, strategy: MaterializationStrategy) -> Self {
        self.materialization = strategy;
        self
    }

    /// Get the fully qualified target table name.
    pub fn qualified_target_name(&self) -> String {
        match &self.target_schema {
            Some(schema) => format!("{}.{}", schema, self.target_table),
            None => self.target_table.clone(),
        }
    }
}

impl DimensionColumn {
    /// Get the target column name (custom or derived from source).
    pub fn target_name(&self) -> &str {
        self.target_column.as_deref().unwrap_or(&self.source_column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dimension_definition_builder() {
        let dim = DimensionDefinition::new("dim_customers", "dim_customers", "customers")
            .with_schema("analytics")
            .with_columns(vec!["customer_id", "name", "region", "segment"])
            .with_primary_key(vec!["customer_id"])
            .with_scd_type(SCDType::Type1)
            .with_materialization(MaterializationStrategy::Table);

        assert_eq!(dim.name, "dim_customers");
        assert_eq!(dim.qualified_target_name(), "analytics.dim_customers");
        assert_eq!(dim.columns.len(), 4);
        assert_eq!(dim.primary_key, vec!["customer_id"]);
        assert_eq!(dim.scd_type, SCDType::Type1);
    }

    #[test]
    fn test_scd_type2() {
        let dim = DimensionDefinition::new("dim_products", "dim_products", "products")
            .with_scd_type(SCDType::Type2 {
                effective_from: "valid_from".into(),
                effective_to: "valid_to".into(),
                is_current: Some("is_current".into()),
            });

        match dim.scd_type {
            SCDType::Type2 { effective_from, effective_to, is_current } => {
                assert_eq!(effective_from, "valid_from");
                assert_eq!(effective_to, "valid_to");
                assert_eq!(is_current, Some("is_current".into()));
            }
            _ => panic!("Expected Type2"),
        }
    }

    #[test]
    fn test_column_rename() {
        let dim = DimensionDefinition::new("dim_test", "dim_test", "test")
            .with_column("id")
            .with_column_as("cust_name", "customer_name");

        assert_eq!(dim.columns[0].target_name(), "id");
        assert_eq!(dim.columns[1].target_name(), "customer_name");
    }

    #[test]
    fn test_scd_default() {
        assert_eq!(SCDType::default(), SCDType::Type1);
    }
}
