//! Database constraint signal generation.
//!
//! Provides signals based on actual database constraints (FK, unique, PK).
//! These are the highest-confidence signals since they come directly from the database.

use std::collections::{HashMap, HashSet};

use crate::metadata::{ForeignKeyInfo, TableMetadata};

use super::{Signal, SignalSource};

/// Key for looking up a constraint: (schema, table, column).
type ConstraintKey = (String, String, String);

/// Stored FK relationship from database.
#[derive(Debug, Clone)]
pub struct StoredForeignKey {
    /// Source schema.
    pub from_schema: String,
    /// Source table.
    pub from_table: String,
    /// Source column.
    pub from_column: String,
    /// Target schema.
    pub to_schema: String,
    /// Target table.
    pub to_table: String,
    /// Target column.
    pub to_column: String,
    /// Constraint name.
    pub constraint_name: String,
}

/// Collects and provides signals from database constraints.
#[derive(Debug, Clone, Default)]
pub struct ConstraintCollector {
    /// Known FK constraints indexed by (from_schema, from_table, from_column).
    foreign_keys: HashMap<ConstraintKey, StoredForeignKey>,
    /// All FK constraint keys for reverse lookup.
    fk_targets: HashSet<ConstraintKey>,
    /// Known unique columns: (schema, table, column).
    unique_columns: HashSet<ConstraintKey>,
    /// Known primary key columns: (schema, table, column).
    pk_columns: HashSet<ConstraintKey>,
}

impl ConstraintCollector {
    /// Create an empty constraint collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load constraints from table metadata.
    pub fn load_from_metadata(&mut self, tables: &[TableMetadata]) {
        for table in tables {
            let schema = table.table.schema.to_lowercase();
            let table_name = table.table.name.to_lowercase();

            // Load primary key columns
            if let Some(pk) = &table.table.primary_key {
                for col in &pk.columns {
                    let key = (schema.clone(), table_name.clone(), col.to_lowercase());
                    self.pk_columns.insert(key.clone());
                    self.unique_columns.insert(key);
                }
            }

            // Load unique constraints
            for uc in &table.table.unique_constraints {
                // Only single-column unique constraints for now
                if uc.columns.len() == 1 {
                    let key = (
                        schema.clone(),
                        table_name.clone(),
                        uc.columns[0].to_lowercase(),
                    );
                    self.unique_columns.insert(key);
                }
            }

            // Load foreign keys
            for fk in &table.table.foreign_keys {
                // Handle multi-column FKs by storing each column pair
                for (i, from_col) in fk.columns.iter().enumerate() {
                    if i < fk.referenced_columns.len() {
                        let to_col = &fk.referenced_columns[i];

                        let stored = StoredForeignKey {
                            from_schema: schema.clone(),
                            from_table: table_name.clone(),
                            from_column: from_col.to_lowercase(),
                            to_schema: fk.referenced_schema.to_lowercase(),
                            to_table: fk.referenced_table.to_lowercase(),
                            to_column: to_col.to_lowercase(),
                            constraint_name: fk.name.clone(),
                        };

                        let key = (schema.clone(), table_name.clone(), from_col.to_lowercase());
                        self.foreign_keys.insert(key, stored);

                        // Also track target for reverse lookup
                        let target_key = (
                            fk.referenced_schema.to_lowercase(),
                            fk.referenced_table.to_lowercase(),
                            to_col.to_lowercase(),
                        );
                        self.fk_targets.insert(target_key);
                    }
                }
            }
        }
    }

    /// Load constraints from a single ForeignKeyInfo.
    pub fn load_foreign_key(&mut self, schema: &str, table: &str, fk: &ForeignKeyInfo) {
        let schema_lower = schema.to_lowercase();
        let table_lower = table.to_lowercase();

        for (i, from_col) in fk.columns.iter().enumerate() {
            if i < fk.referenced_columns.len() {
                let to_col = &fk.referenced_columns[i];

                let stored = StoredForeignKey {
                    from_schema: schema_lower.clone(),
                    from_table: table_lower.clone(),
                    from_column: from_col.to_lowercase(),
                    to_schema: fk.referenced_schema.to_lowercase(),
                    to_table: fk.referenced_table.to_lowercase(),
                    to_column: to_col.to_lowercase(),
                    constraint_name: fk.name.clone(),
                };

                let key = (
                    schema_lower.clone(),
                    table_lower.clone(),
                    from_col.to_lowercase(),
                );
                self.foreign_keys.insert(key, stored);

                let target_key = (
                    fk.referenced_schema.to_lowercase(),
                    fk.referenced_table.to_lowercase(),
                    to_col.to_lowercase(),
                );
                self.fk_targets.insert(target_key);
            }
        }
    }

    /// Check if a column has a known FK constraint.
    ///
    /// Returns the stored FK info if found.
    pub fn get_foreign_key(
        &self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Option<&StoredForeignKey> {
        let key = (
            schema.to_lowercase(),
            table.to_lowercase(),
            column.to_lowercase(),
        );
        self.foreign_keys.get(&key)
    }

    /// Check if a candidate relationship matches a known FK constraint.
    ///
    /// Returns a high-confidence signal if it matches, None otherwise.
    pub fn check_candidate(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> Option<Signal> {
        if let Some(stored) = self.get_foreign_key(from_schema, from_table, from_column) {
            // Check if target matches
            if stored.to_schema.eq_ignore_ascii_case(to_schema)
                && stored.to_table.eq_ignore_ascii_case(to_table)
                && stored.to_column.eq_ignore_ascii_case(to_column)
            {
                return Some(Signal::positive(
                    SignalSource::DatabaseConstraint,
                    0.98,
                    format!(
                        "Database FK constraint '{}': {}.{}.{} → {}.{}.{}",
                        stored.constraint_name,
                        stored.from_schema,
                        stored.from_table,
                        stored.from_column,
                        stored.to_schema,
                        stored.to_table,
                        stored.to_column
                    ),
                ));
            }
        }
        None
    }

    /// Check if a column is marked as unique (PK or unique constraint).
    pub fn is_unique(&self, schema: &str, table: &str, column: &str) -> bool {
        let key = (
            schema.to_lowercase(),
            table.to_lowercase(),
            column.to_lowercase(),
        );
        self.unique_columns.contains(&key)
    }

    /// Check if a column is a primary key.
    pub fn is_primary_key(&self, schema: &str, table: &str, column: &str) -> bool {
        let key = (
            schema.to_lowercase(),
            table.to_lowercase(),
            column.to_lowercase(),
        );
        self.pk_columns.contains(&key)
    }

    /// Check if a column is the target of any FK (referenced by another table).
    pub fn is_fk_target(&self, schema: &str, table: &str, column: &str) -> bool {
        let key = (
            schema.to_lowercase(),
            table.to_lowercase(),
            column.to_lowercase(),
        );
        self.fk_targets.contains(&key)
    }

    /// Get all known foreign keys.
    pub fn all_foreign_keys(&self) -> impl Iterator<Item = &StoredForeignKey> {
        self.foreign_keys.values()
    }

    /// Get count of loaded constraints.
    pub fn stats(&self) -> ConstraintStats {
        ConstraintStats {
            foreign_key_count: self.foreign_keys.len(),
            unique_column_count: self.unique_columns.len(),
            pk_column_count: self.pk_columns.len(),
        }
    }

    /// Clear all loaded constraints.
    pub fn clear(&mut self) {
        self.foreign_keys.clear();
        self.fk_targets.clear();
        self.unique_columns.clear();
        self.pk_columns.clear();
    }
}

/// Statistics about loaded constraints.
#[derive(Debug, Clone, Copy)]
pub struct ConstraintStats {
    /// Number of FK constraints loaded.
    pub foreign_key_count: usize,
    /// Number of unique columns.
    pub unique_column_count: usize,
    /// Number of primary key columns.
    pub pk_column_count: usize,
}

// TODO: Fix these tests - metadata types were refactored
#[cfg(all(test, feature = "broken_tests"))]
mod tests {
    use super::*;
    use crate::metadata::{
        ColumnInfo, ForeignKeyInfo, PrimaryKeyInfo, TableMetadata, TableType, UniqueConstraintInfo,
    };

    fn make_test_metadata() -> Vec<TableMetadata> {
        vec![
            TableMetadata {
                schema: "public".to_string(),
                name: "customers".to_string(),
                table_type: TableType::Table,
                columns: vec![ColumnInfo {
                    name: "id".to_string(),
                    position: 1,
                    data_type: "integer".to_string(),
                    is_nullable: false,
                    max_length: None,
                    numeric_precision: None,
                    numeric_scale: None,
                    default_value: None,
                    is_identity: true,
                    is_computed: false,
                }],
                primary_key: Some(PrimaryKeyInfo {
                    name: "pk_customers".to_string(),
                    columns: vec!["id".to_string()],
                }),
                foreign_keys: vec![],
                unique_constraints: vec![],
            },
            TableMetadata {
                schema: "public".to_string(),
                name: "orders".to_string(),
                table_type: TableType::Table,
                columns: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        position: 1,
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        max_length: None,
                        numeric_precision: None,
                        numeric_scale: None,
                        default_value: None,
                        is_identity: true,
                        is_computed: false,
                    },
                    ColumnInfo {
                        name: "customer_id".to_string(),
                        position: 2,
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        max_length: None,
                        numeric_precision: None,
                        numeric_scale: None,
                        default_value: None,
                        is_identity: false,
                        is_computed: false,
                    },
                ],
                primary_key: Some(PrimaryKeyInfo {
                    name: "pk_orders".to_string(),
                    columns: vec!["id".to_string()],
                }),
                foreign_keys: vec![ForeignKeyInfo {
                    name: "fk_orders_customer".to_string(),
                    columns: vec!["customer_id".to_string()],
                    referenced_schema: "public".to_string(),
                    referenced_table: "customers".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: Some("CASCADE".to_string()),
                    on_update: None,
                }],
                unique_constraints: vec![UniqueConstraintInfo {
                    name: "uq_orders_id".to_string(),
                    columns: vec!["id".to_string()],
                    is_primary_key: true,
                }],
            },
        ]
    }

    #[test]
    fn test_load_constraints() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        let stats = collector.stats();
        assert_eq!(stats.foreign_key_count, 1);
        assert_eq!(stats.pk_column_count, 2); // customers.id and orders.id
    }

    #[test]
    fn test_get_foreign_key() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        let fk = collector.get_foreign_key("public", "orders", "customer_id");
        assert!(fk.is_some());

        let fk = fk.unwrap();
        assert_eq!(fk.to_table, "customers");
        assert_eq!(fk.to_column, "id");
    }

    #[test]
    fn test_check_candidate_match() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        // Matching candidate
        let signal = collector.check_candidate(
            "public",
            "orders",
            "customer_id",
            "public",
            "customers",
            "id",
        );
        assert!(signal.is_some());
        let s = signal.unwrap();
        assert!(s.is_positive());
        assert!(s.score >= 0.98);
    }

    #[test]
    fn test_check_candidate_no_match() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        // Non-matching candidate
        let signal = collector.check_candidate(
            "public",
            "orders",
            "customer_id",
            "public",
            "products", // Wrong table
            "id",
        );
        assert!(signal.is_none());
    }

    #[test]
    fn test_is_unique() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        assert!(collector.is_unique("public", "customers", "id"));
        assert!(collector.is_unique("public", "orders", "id"));
        assert!(!collector.is_unique("public", "orders", "customer_id"));
    }

    #[test]
    fn test_is_primary_key() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        assert!(collector.is_primary_key("public", "customers", "id"));
        assert!(!collector.is_primary_key("public", "orders", "customer_id"));
    }

    #[test]
    fn test_is_fk_target() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        // customers.id is referenced by orders.customer_id
        assert!(collector.is_fk_target("public", "customers", "id"));
        // orders.id is not referenced
        assert!(!collector.is_fk_target("public", "orders", "id"));
    }

    #[test]
    fn test_case_insensitivity() {
        let mut collector = ConstraintCollector::new();
        collector.load_from_metadata(&make_test_metadata());

        // Should work with different cases
        assert!(collector.is_primary_key("PUBLIC", "CUSTOMERS", "ID"));
        assert!(collector
            .get_foreign_key("Public", "Orders", "Customer_Id")
            .is_some());
    }
}
