//! Schema convention detection and signals.
//!
//! Analyzes a schema to detect naming conventions, then boosts confidence
//! for columns that match the schema's dominant patterns.

use std::collections::HashMap;

use inflector::Inflector;
use serde::{Deserialize, Serialize};

use super::{Signal, SignalSource};
use crate::semantic::inference::TableInfo;

/// Level at which to detect conventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ConventionScope {
    /// Detect conventions per-schema (default).
    /// Different schemas may have different patterns.
    #[default]
    Schema,
    /// Detect conventions globally across all schemas.
    /// Use when schemas share the same conventions.
    Global,
    /// Disable convention detection.
    Disabled,
}

/// Detected naming style in a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum NamingStyle {
    /// snake_case (e.g., customer_id)
    #[default]
    SnakeCase,
    /// camelCase (e.g., customerId)
    CamelCase,
    /// PascalCase (e.g., CustomerId)
    PascalCase,
    /// Mixed or unable to determine
    Mixed,
}

impl NamingStyle {
    /// Detect naming style from a collection of names.
    pub fn detect(names: &[&str]) -> Self {
        if names.is_empty() {
            return Self::Mixed;
        }

        let mut snake_count = 0;
        let mut camel_count = 0;
        let mut pascal_count = 0;

        for name in names {
            match Self::classify_single(name) {
                Self::SnakeCase => snake_count += 1,
                Self::CamelCase => camel_count += 1,
                Self::PascalCase => pascal_count += 1,
                Self::Mixed => {}
            }
        }

        let total = snake_count + camel_count + pascal_count;
        if total == 0 {
            return Self::Mixed;
        }

        // Require at least 60% dominance
        let threshold = (total as f64 * 0.6) as usize;

        if snake_count >= threshold {
            Self::SnakeCase
        } else if camel_count >= threshold {
            Self::CamelCase
        } else if pascal_count >= threshold {
            Self::PascalCase
        } else {
            Self::Mixed
        }
    }

    /// Classify a single name's style.
    fn classify_single(name: &str) -> Self {
        if name.is_empty() {
            return Self::Mixed;
        }

        let has_underscore = name.contains('_');
        let chars: Vec<char> = name.chars().collect();
        let first_char = chars[0];

        // Count uppercase letters (excluding first)
        let internal_uppercase = chars.iter().skip(1).filter(|c| c.is_uppercase()).count();

        if has_underscore {
            // If it has underscores, it's snake_case (or SCREAMING_SNAKE)
            if name.chars().all(|c| c.is_lowercase() || c == '_' || c.is_numeric()) {
                Self::SnakeCase
            } else {
                Self::Mixed // Mixed case with underscores
            }
        } else if first_char.is_uppercase() && internal_uppercase > 0 {
            Self::PascalCase
        } else if first_char.is_lowercase() && internal_uppercase > 0 {
            Self::CamelCase
        } else if name.chars().all(|c| c.is_lowercase() || c.is_numeric()) {
            // All lowercase, no underscores - could be single word snake_case
            Self::SnakeCase
        } else {
            Self::Mixed
        }
    }

    /// Check if a name matches this style.
    pub fn matches(&self, name: &str) -> bool {
        Self::classify_single(name) == *self
    }
}

/// Detected conventions for a schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaConventions {
    /// Schema name (empty for global conventions).
    pub schema: String,

    /// Detected PK patterns with count.
    /// e.g., {"id": 45, "{table}_id": 5}
    pub pk_patterns: HashMap<String, usize>,

    /// Detected FK suffix patterns with count.
    /// e.g., {"_id": 120, "_key": 15, "_fk": 5}
    pub fk_suffix_counts: HashMap<String, usize>,

    /// Common suffixes with frequency (0.0 to 1.0).
    pub common_suffixes: Vec<(String, f64)>,

    /// Dominant naming style.
    pub naming_style: NamingStyle,

    /// Total tables analyzed.
    pub table_count: usize,

    /// Total columns analyzed.
    pub column_count: usize,
}

impl SchemaConventions {
    /// Analyze tables to detect conventions.
    pub fn analyze(schema: &str, tables: &[TableInfo]) -> Self {
        let mut pk_patterns: HashMap<String, usize> = HashMap::new();
        let mut fk_suffix_counts: HashMap<String, usize> = HashMap::new();
        let mut all_column_names: Vec<&str> = Vec::new();
        let mut column_count = 0;

        for table in tables {
            // Analyze PK patterns
            for pk_col in &table.primary_key {
                let pattern = Self::extract_pk_pattern(pk_col, &table.name);
                *pk_patterns.entry(pattern).or_insert(0) += 1;
            }

            // Analyze column suffixes (potential FKs)
            for col in &table.columns {
                column_count += 1;
                all_column_names.push(&col.name);

                // Look for common FK suffixes
                for suffix in &["_id", "_key", "_code", "_fk", "_ref", "_num", "_no"] {
                    if col.name.to_lowercase().ends_with(suffix) {
                        *fk_suffix_counts.entry(suffix.to_string()).or_insert(0) += 1;
                        break;
                    }
                }
            }
        }

        // Calculate suffix frequencies
        let total_suffixed: usize = fk_suffix_counts.values().sum();
        let common_suffixes: Vec<(String, f64)> = if total_suffixed > 0 {
            let mut sorted: Vec<_> = fk_suffix_counts
                .iter()
                .map(|(s, &c)| (s.clone(), c as f64 / total_suffixed as f64))
                .filter(|(_, freq)| *freq >= 0.05) // At least 5%
                .collect();
            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            sorted
        } else {
            Vec::new()
        };

        // Detect naming style
        let naming_style = NamingStyle::detect(&all_column_names);

        Self {
            schema: schema.to_string(),
            pk_patterns,
            fk_suffix_counts,
            common_suffixes,
            naming_style,
            table_count: tables.len(),
            column_count,
        }
    }

    /// Extract PK pattern from column name.
    fn extract_pk_pattern(pk_col: &str, table_name: &str) -> String {
        let pk_lower = pk_col.to_lowercase();
        let table_lower = table_name.to_lowercase();
        let table_singular = table_lower.to_singular();

        if pk_lower == "id" {
            "id".to_string()
        } else if pk_lower == format!("{}_id", table_lower)
            || pk_lower == format!("{}_id", table_singular)
        {
            "{table}_id".to_string()
        } else if pk_lower.ends_with("_id") {
            "*_id".to_string()
        } else if pk_lower.ends_with("_key") {
            "*_key".to_string()
        } else if pk_lower.ends_with("_pk") {
            "*_pk".to_string()
        } else {
            "other".to_string()
        }
    }

    /// Get the dominant PK pattern.
    pub fn dominant_pk_pattern(&self) -> Option<(&str, f64)> {
        if self.pk_patterns.is_empty() {
            return None;
        }

        let total: usize = self.pk_patterns.values().sum();
        self.pk_patterns
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(pattern, count)| (pattern.as_str(), *count as f64 / total as f64))
    }

    /// Get the dominant FK suffix.
    pub fn dominant_fk_suffix(&self) -> Option<(&str, f64)> {
        self.common_suffixes.first().map(|(s, f)| (s.as_str(), *f))
    }

    /// Score how well a column matches schema conventions.
    ///
    /// Returns a score from 0.0 to 0.3 based on convention matching.
    pub fn score_column(&self, column_name: &str) -> f64 {
        let mut score = 0.0;
        let col_lower = column_name.to_lowercase();

        // Check suffix match
        for (suffix, frequency) in &self.common_suffixes {
            if col_lower.ends_with(suffix) {
                // Higher frequency = higher boost (up to 0.2)
                score += 0.1 + (frequency * 0.1);
                break;
            }
        }

        // Naming style match (small boost)
        if self.naming_style != NamingStyle::Mixed && self.naming_style.matches(column_name) {
            score += 0.05;
        }

        score.min(0.3) // Cap at 0.3
    }

    /// Generate a signal for convention matching.
    pub fn to_signal(&self, column_name: &str) -> Option<Signal> {
        let score = self.score_column(column_name);

        if score < 0.05 {
            return None;
        }

        let col_lower = column_name.to_lowercase();

        // Find which convention matched
        let convention_desc = self
            .common_suffixes
            .iter()
            .find(|(suffix, _)| col_lower.ends_with(suffix))
            .map(|(suffix, freq)| {
                format!(
                    "Suffix '{}' used in {:.0}% of FK columns",
                    suffix,
                    freq * 100.0
                )
            })
            .unwrap_or_else(|| "Matches schema naming conventions".to_string());

        Some(Signal::positive(
            SignalSource::convention(&convention_desc),
            score,
            convention_desc,
        ))
    }

    /// Check if this schema has consistent conventions.
    ///
    /// Returns true if there's a dominant pattern (>60% of columns).
    pub fn is_consistent(&self) -> bool {
        if let Some((_, freq)) = self.dominant_fk_suffix() {
            freq >= 0.6
        } else {
            false
        }
    }

    /// Generate a penalty signal for non-matching columns in consistent schemas.
    pub fn penalty_signal(&self, column_name: &str) -> Option<Signal> {
        // Only penalize in consistent schemas
        if !self.is_consistent() {
            return None;
        }

        let col_lower = column_name.to_lowercase();

        // Check if column uses any known FK suffix
        let uses_known_suffix = self
            .common_suffixes
            .iter()
            .any(|(suffix, _)| col_lower.ends_with(suffix));

        if uses_known_suffix {
            return None; // No penalty
        }

        // Check if it looks like it could be an FK
        let looks_like_fk = col_lower.ends_with("_id")
            || col_lower.ends_with("_key")
            || col_lower.ends_with("_fk")
            || col_lower.ends_with("_ref");

        if !looks_like_fk {
            return None; // Not an FK candidate
        }

        // Penalize FK-looking columns that don't match schema conventions
        let (dominant_suffix, freq) = self.dominant_fk_suffix()?;
        Some(Signal::negative(
            SignalSource::convention("non_standard_suffix"),
            0.1, // Small penalty
            format!(
                "Schema uses '{}' ({:.0}%) but column uses different suffix",
                dominant_suffix,
                freq * 100.0
            ),
        ))
    }
}

/// Registry of conventions by schema.
#[derive(Debug, Clone, Default)]
pub struct ConventionRegistry {
    /// Conventions per schema.
    by_schema: HashMap<String, SchemaConventions>,
    /// Global conventions (if scope is Global).
    global: Option<SchemaConventions>,
    /// Detection scope.
    scope: ConventionScope,
}

impl ConventionRegistry {
    /// Create a new registry with the given scope.
    pub fn new(scope: ConventionScope) -> Self {
        Self {
            by_schema: HashMap::new(),
            global: None,
            scope,
        }
    }

    /// Register conventions for a schema.
    pub fn register(&mut self, conventions: SchemaConventions) {
        match self.scope {
            ConventionScope::Schema => {
                self.by_schema
                    .insert(conventions.schema.clone(), conventions);
            }
            ConventionScope::Global => {
                // Merge into global conventions
                if let Some(ref mut global) = self.global {
                    Self::merge_conventions(global, &conventions);
                } else {
                    let mut global = conventions.clone();
                    global.schema = String::new();
                    self.global = Some(global);
                }
            }
            ConventionScope::Disabled => {}
        }
    }

    /// Get conventions for a schema.
    pub fn get(&self, schema: &str) -> Option<&SchemaConventions> {
        match self.scope {
            ConventionScope::Schema => self.by_schema.get(schema),
            ConventionScope::Global => self.global.as_ref(),
            ConventionScope::Disabled => None,
        }
    }

    /// Merge conventions from source into target.
    fn merge_conventions(target: &mut SchemaConventions, source: &SchemaConventions) {
        // Merge PK patterns
        for (pattern, count) in &source.pk_patterns {
            *target.pk_patterns.entry(pattern.clone()).or_insert(0) += count;
        }

        // Merge FK suffix counts
        for (suffix, count) in &source.fk_suffix_counts {
            *target.fk_suffix_counts.entry(suffix.clone()).or_insert(0) += count;
        }

        target.table_count += source.table_count;
        target.column_count += source.column_count;

        // Recalculate common_suffixes
        let total_suffixed: usize = target.fk_suffix_counts.values().sum();
        if total_suffixed > 0 {
            let mut sorted: Vec<_> = target
                .fk_suffix_counts
                .iter()
                .map(|(s, &c)| (s.clone(), c as f64 / total_suffixed as f64))
                .filter(|(_, freq)| *freq >= 0.05)
                .collect();
            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            target.common_suffixes = sorted;
        }
    }

    /// Check if any conventions are registered.
    pub fn is_empty(&self) -> bool {
        self.by_schema.is_empty() && self.global.is_none()
    }

    /// Get all registered schemas.
    pub fn schemas(&self) -> Vec<&str> {
        self.by_schema.keys().map(|s| s.as_str()).collect()
    }
}

// Re-export Inflector traits for use elsewhere
pub use inflector::Inflector as InflectorTrait;

// Re-export shared inflection utilities
pub use super::inflection::{pluralize, singularize};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic::inference::ColumnInfo;

    fn make_table(name: &str, columns: &[&str], pk: &[&str]) -> TableInfo {
        TableInfo {
            schema: "public".to_string(),
            name: name.to_string(),
            columns: columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.to_string(),
                    data_type: "integer".to_string(),
                    is_nullable: true,
                    is_unique: None,
                })
                .collect(),
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_naming_style_snake_case() {
        let names = vec!["customer_id", "order_date", "product_name", "created_at"];
        assert_eq!(NamingStyle::detect(&names), NamingStyle::SnakeCase);
    }

    #[test]
    fn test_naming_style_camel_case() {
        let names = vec!["customerId", "orderDate", "productName", "createdAt"];
        assert_eq!(NamingStyle::detect(&names), NamingStyle::CamelCase);
    }

    #[test]
    fn test_naming_style_pascal_case() {
        let names = vec!["CustomerId", "OrderDate", "ProductName", "CreatedAt"];
        assert_eq!(NamingStyle::detect(&names), NamingStyle::PascalCase);
    }

    #[test]
    fn test_naming_style_mixed() {
        // 2 of each style = no 60% majority
        let names = vec![
            "customer_id",
            "order_date",
            "OrderDate",
            "ProductName",
            "productName",
            "createdAt",
        ];
        assert_eq!(NamingStyle::detect(&names), NamingStyle::Mixed);
    }

    #[test]
    fn test_schema_conventions_analysis() {
        let tables = vec![
            make_table("customers", &["id", "name", "email"], &["id"]),
            make_table(
                "orders",
                &["id", "customer_id", "product_id", "status"],
                &["id"],
            ),
            make_table("products", &["id", "name", "category_id", "price"], &["id"]),
            make_table(
                "order_items",
                &["id", "order_id", "product_id", "quantity"],
                &["id"],
            ),
        ];

        let conventions = SchemaConventions::analyze("public", &tables);

        // Should detect "id" as dominant PK pattern
        assert_eq!(conventions.pk_patterns.get("id"), Some(&4));

        // Should detect "_id" as dominant FK suffix
        assert!(conventions.fk_suffix_counts.get("_id").unwrap_or(&0) >= &5);

        // Check naming style
        assert_eq!(conventions.naming_style, NamingStyle::SnakeCase);
    }

    #[test]
    fn test_score_column() {
        let tables = vec![
            make_table("customers", &["id", "name"], &["id"]),
            make_table("orders", &["id", "customer_id"], &["id"]),
            make_table("products", &["id", "category_id"], &["id"]),
        ];

        let conventions = SchemaConventions::analyze("public", &tables);

        // Columns with _id should score higher
        let score_id = conventions.score_column("customer_id");
        let score_other = conventions.score_column("customer");

        assert!(score_id > score_other);
        assert!(score_id > 0.1);
    }

    #[test]
    fn test_to_signal() {
        let tables = vec![
            make_table("customers", &["id", "name"], &["id"]),
            make_table("orders", &["id", "customer_id"], &["id"]),
        ];

        let conventions = SchemaConventions::analyze("public", &tables);
        let signal = conventions.to_signal("order_id");

        assert!(signal.is_some());
        let s = signal.unwrap();
        assert!(s.is_positive());
        assert!(s.explanation.contains("_id"));
    }

    #[test]
    fn test_convention_registry_schema_scope() {
        let mut registry = ConventionRegistry::new(ConventionScope::Schema);

        let tables1 = vec![make_table("users", &["id", "name"], &["id"])];
        let tables2 = vec![make_table("items", &["item_key", "name"], &["item_key"])];

        registry.register(SchemaConventions::analyze("public", &tables1));
        registry.register(SchemaConventions::analyze("inventory", &tables2));

        assert!(registry.get("public").is_some());
        assert!(registry.get("inventory").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_convention_registry_global_scope() {
        let mut registry = ConventionRegistry::new(ConventionScope::Global);

        let tables1 = vec![make_table("users", &["id", "user_id"], &["id"])];
        let tables2 = vec![make_table("items", &["id", "category_id"], &["id"])];

        registry.register(SchemaConventions::analyze("public", &tables1));
        registry.register(SchemaConventions::analyze("inventory", &tables2));

        // Both should return global conventions
        let global = registry.get("public");
        assert!(global.is_some());
        assert_eq!(global.unwrap().table_count, 2);
    }

    #[test]
    fn test_convention_registry_disabled() {
        let mut registry = ConventionRegistry::new(ConventionScope::Disabled);

        let tables = vec![make_table("users", &["id", "name"], &["id"])];
        registry.register(SchemaConventions::analyze("public", &tables));

        assert!(registry.get("public").is_none());
    }

    #[test]
    fn test_dominant_patterns() {
        let tables = vec![
            make_table("a", &["id", "b_id"], &["id"]),
            make_table("b", &["id", "c_id"], &["id"]),
            make_table("c", &["id", "a_id"], &["id"]),
        ];

        let conventions = SchemaConventions::analyze("public", &tables);

        let (pk_pattern, pk_freq) = conventions.dominant_pk_pattern().unwrap();
        assert_eq!(pk_pattern, "id");
        assert!(pk_freq > 0.9);

        let (fk_suffix, fk_freq) = conventions.dominant_fk_suffix().unwrap();
        assert_eq!(fk_suffix, "_id");
        assert!(fk_freq > 0.9);
    }

    #[test]
    fn test_is_consistent() {
        // Consistent schema (all _id)
        let consistent_tables = vec![
            make_table("a", &["id", "b_id", "c_id"], &["id"]),
            make_table("b", &["id", "a_id"], &["id"]),
        ];
        let consistent = SchemaConventions::analyze("public", &consistent_tables);
        assert!(consistent.is_consistent());

        // Inconsistent schema (mixed suffixes)
        let mixed_tables = vec![
            make_table("a", &["id", "b_id"], &["id"]),
            make_table("b", &["id", "a_key"], &["id"]),
            make_table("c", &["id", "b_code", "a_ref"], &["id"]),
        ];
        let mixed = SchemaConventions::analyze("public", &mixed_tables);
        assert!(!mixed.is_consistent());
    }

    #[test]
    fn test_inflector_pluralize() {
        // Test that Inflector handles more cases than the old implementation
        assert_eq!(pluralize("leaf"), "leaves");
        assert_eq!(pluralize("analysis"), "analyses");
        assert_eq!(pluralize("customer"), "customers");
        assert_eq!(pluralize("category"), "categories");
        assert_eq!(pluralize("person"), "people");
    }

    #[test]
    fn test_inflector_singularize() {
        assert_eq!(singularize("leaves"), "leaf");
        assert_eq!(singularize("analyses"), "analysis");
        assert_eq!(singularize("customers"), "customer");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("people"), "person");
    }
}
