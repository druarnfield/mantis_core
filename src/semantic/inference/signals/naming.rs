//! Naming convention signal collector.
//!
//! Expanded rule set with 12 rules for matching column names to potential
//! foreign key relationships based on naming patterns.

use std::collections::{HashMap, HashSet};

use super::{Signal, SignalSource};

/// A naming convention rule for FK inference.
#[derive(Debug, Clone)]
pub struct NamingRule {
    /// Rule identifier.
    pub name: &'static str,
    /// Human-readable description.
    pub description: &'static str,
    /// Base score for matches (0.0 to 1.0).
    pub base_score: f64,
    /// Priority (higher = checked first).
    pub priority: u8,
    /// The pattern to match.
    pattern: NamingPattern,
}

/// Types of naming patterns to match.
#[derive(Debug, Clone)]
enum NamingPattern {
    /// Column starts with prefix, then table name, then suffix.
    /// e.g., "fk_customer_id" matches FkTableSuffix("fk_", "_id")
    FkTableSuffix {
        prefix: &'static str,
        suffix: &'static str,
    },
    /// Column starts with prefix, then table name.
    /// e.g., "fk_customer" matches FkTable("fk_")
    FkTable {
        prefix: &'static str,
    },
    /// Column is {table}{suffix} where table references target.
    /// e.g., "customer_id" matches TableSuffix("_id", "id")
    TableSuffix {
        suffix: &'static str,
        target_column: &'static str,
    },
    /// Column is {table}{suffix} where target table has a prefix and target column is SAME as source.
    /// e.g., "employee_id" matches TableSuffixWithPrefixSameCol("_id", "dim_") → dim_employees.employee_id
    TableSuffixWithPrefixSameCol {
        suffix: &'static str,
        table_prefix: &'static str,
    },
    /// Column starts with prefix, then table name.
    /// e.g., "ref_customer" matches PrefixTable("ref_")
    PrefixTable {
        prefix: &'static str,
    },
    /// Column exactly matches another table's primary key name.
    PrimaryKeyMatch,
    /// Column contains table name embedded within it.
    /// e.g., "cust_id" might match "customers" table
    TableNameEmbed {
        suffix: &'static str,
    },
    /// Self-referential pattern like parent_id.
    SelfReference {
        prefix: &'static str,
        suffix: &'static str,
    },
}

/// Result of a naming rule match.
#[derive(Debug, Clone)]
pub struct NamingMatch {
    /// Target table name.
    pub target_table: String,
    /// Target column name.
    pub target_column: String,
    /// The rule that matched.
    pub rule_name: &'static str,
    /// Base score from the rule.
    pub base_score: f64,
}

impl NamingMatch {
    /// Convert this match to a Signal.
    pub fn to_signal(&self) -> Signal {
        Signal::positive(
            SignalSource::naming(self.rule_name),
            self.base_score,
            format!(
                "Column matches '{}' pattern → {}.{}",
                self.rule_name, self.target_table, self.target_column
            ),
        )
    }
}

/// Collector for naming convention signals.
#[derive(Debug, Clone)]
pub struct NamingSignalCollector {
    rules: Vec<NamingRule>,
}

impl Default for NamingSignalCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl NamingSignalCollector {
    /// Create a collector with the default 12 rules.
    pub fn new() -> Self {
        let mut rules = default_naming_rules();
        // Pre-sort rules by priority (descending) to avoid sorting on every call
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Self { rules }
    }

    /// Create a collector with custom rules.
    pub fn with_rules(mut rules: Vec<NamingRule>) -> Self {
        // Pre-sort rules by priority (descending) to avoid sorting on every call
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Self { rules }
    }

    /// Try to match a column name against all rules.
    ///
    /// # Arguments
    /// * `column_name` - The column to match
    /// * `source_table` - The table this column belongs to
    /// * `available_tables` - Set of all table names (lowercase)
    /// * `table_pk_columns` - Map of table -> primary key column names
    pub fn collect_signals(
        &self,
        column_name: &str,
        source_table: &str,
        available_tables: &HashSet<String>,
        table_pk_columns: &HashMap<String, Vec<String>>,
    ) -> Vec<NamingMatch> {
        let col_lower = column_name.to_lowercase();
        let mut matches = Vec::new();

        // Rules are pre-sorted by priority in constructor
        for rule in &self.rules {
            let rule_matches = rule.try_match(
                &col_lower,
                source_table,
                available_tables,
                table_pk_columns,
            );
            matches.extend(rule_matches);
        }

        // Deduplicate by (target_table, target_column), keeping highest score
        let mut best_matches: HashMap<(String, String), NamingMatch> = HashMap::new();
        for m in matches {
            let key = (m.target_table.clone(), m.target_column.clone());
            if let Some(existing) = best_matches.get(&key) {
                if m.base_score > existing.base_score {
                    best_matches.insert(key, m);
                }
            } else {
                best_matches.insert(key, m);
            }
        }

        best_matches.into_values().collect()
    }

    /// Get all rules.
    pub fn rules(&self) -> &[NamingRule] {
        &self.rules
    }
}

impl NamingRule {
    /// Find matching tables for a base name, trying both plural and singular forms.
    ///
    /// Returns matches for the plural form (full score) and singular form (95% score).
    fn find_table_matches(
        &self,
        base: &str,
        available_tables: &HashSet<String>,
        target_column: &str,
    ) -> Vec<NamingMatch> {
        let mut matches = Vec::new();

        // Try plural form (higher confidence)
        let plural = pluralize(base);
        if available_tables.contains(&plural) {
            matches.push(NamingMatch {
                target_table: plural,
                target_column: target_column.to_string(),
                rule_name: self.name,
                base_score: self.base_score,
            });
        }

        // Try singular form (slightly lower confidence)
        if available_tables.contains(base) {
            matches.push(NamingMatch {
                target_table: base.to_string(),
                target_column: target_column.to_string(),
                rule_name: self.name,
                base_score: self.base_score * 0.95,
            });
        }

        matches
    }

    /// Find matching tables with a prefix applied (e.g., dim_, fact_).
    fn find_prefixed_table_matches(
        &self,
        base: &str,
        table_prefix: &str,
        available_tables: &HashSet<String>,
        target_column: &str,
    ) -> Vec<NamingMatch> {
        let mut matches = Vec::new();

        // Try {prefix}{plural} form
        let plural = pluralize(base);
        let prefixed_plural = format!("{}{}", table_prefix, plural);
        if available_tables.contains(&prefixed_plural) {
            matches.push(NamingMatch {
                target_table: prefixed_plural,
                target_column: target_column.to_string(),
                rule_name: self.name,
                base_score: self.base_score,
            });
        }

        // Try {prefix}{singular} form
        let prefixed_singular = format!("{}{}", table_prefix, base);
        if available_tables.contains(&prefixed_singular) {
            matches.push(NamingMatch {
                target_table: prefixed_singular,
                target_column: target_column.to_string(),
                rule_name: self.name,
                base_score: self.base_score * 0.95,
            });
        }

        matches
    }

    /// Try to match this rule against a column name.
    fn try_match(
        &self,
        col_lower: &str,
        source_table: &str,
        available_tables: &HashSet<String>,
        table_pk_columns: &HashMap<String, Vec<String>>,
    ) -> Vec<NamingMatch> {
        match &self.pattern {
            NamingPattern::FkTableSuffix { prefix, suffix } => {
                self.match_fk_table_suffix(col_lower, prefix, suffix, available_tables)
            }
            NamingPattern::FkTable { prefix } => {
                self.match_fk_table(col_lower, prefix, available_tables)
            }
            NamingPattern::TableSuffix {
                suffix,
                target_column,
            } => self.match_table_suffix(col_lower, suffix, target_column, available_tables),
            NamingPattern::TableSuffixWithPrefixSameCol {
                suffix,
                table_prefix,
            } => self.match_table_suffix_with_prefix_same_col(col_lower, suffix, table_prefix, available_tables),
            NamingPattern::PrefixTable { prefix } => {
                self.match_prefix_table(col_lower, prefix, available_tables)
            }
            NamingPattern::PrimaryKeyMatch => {
                self.match_pk(col_lower, source_table, table_pk_columns)
            }
            NamingPattern::TableNameEmbed { suffix } => {
                self.match_table_embed(col_lower, suffix, available_tables)
            }
            NamingPattern::SelfReference { prefix, suffix } => {
                self.match_self_reference(col_lower, prefix, suffix, source_table, table_pk_columns)
            }
        }
    }

    /// Match fk_{table}_{suffix} pattern.
    fn match_fk_table_suffix(
        &self,
        col_lower: &str,
        prefix: &str,
        suffix: &str,
        available_tables: &HashSet<String>,
    ) -> Vec<NamingMatch> {
        if !col_lower.starts_with(prefix) || !col_lower.ends_with(suffix) {
            return vec![];
        }

        let middle = &col_lower[prefix.len()..col_lower.len() - suffix.len()];
        if middle.is_empty() {
            return vec![];
        }

        let target_column = suffix.trim_start_matches('_');
        self.find_table_matches(middle, available_tables, target_column)
    }

    /// Match fk_{table} pattern (no suffix).
    fn match_fk_table(
        &self,
        col_lower: &str,
        prefix: &str,
        available_tables: &HashSet<String>,
    ) -> Vec<NamingMatch> {
        if !col_lower.starts_with(prefix) {
            return vec![];
        }

        let table_part = &col_lower[prefix.len()..];
        if table_part.is_empty() {
            return vec![];
        }

        self.find_table_matches(table_part, available_tables, "id")
    }

    /// Match {table}{suffix} pattern.
    fn match_table_suffix(
        &self,
        col_lower: &str,
        suffix: &str,
        target_column: &str,
        available_tables: &HashSet<String>,
    ) -> Vec<NamingMatch> {
        if !col_lower.ends_with(suffix) {
            return vec![];
        }

        let base = &col_lower[..col_lower.len() - suffix.len()];
        if base.is_empty() {
            return vec![];
        }

        self.find_table_matches(base, available_tables, target_column)
    }

    /// Match {table}{suffix} pattern where target table has a prefix and target column is SAME as source.
    /// This matches Qlik-style inference where employee_id → dim_employees.employee_id
    fn match_table_suffix_with_prefix_same_col(
        &self,
        col_lower: &str,
        suffix: &str,
        table_prefix: &str,
        available_tables: &HashSet<String>,
    ) -> Vec<NamingMatch> {
        if !col_lower.ends_with(suffix) {
            return vec![];
        }

        let base = &col_lower[..col_lower.len() - suffix.len()];
        if base.is_empty() {
            return vec![];
        }

        // Use col_lower as target column (same column name)
        self.find_prefixed_table_matches(base, table_prefix, available_tables, col_lower)
    }

    /// Match {prefix}{table} pattern (e.g., ref_customer).
    fn match_prefix_table(
        &self,
        col_lower: &str,
        prefix: &str,
        available_tables: &HashSet<String>,
    ) -> Vec<NamingMatch> {
        if !col_lower.starts_with(prefix) {
            return vec![];
        }

        let table_part = &col_lower[prefix.len()..];
        if table_part.is_empty() {
            return vec![];
        }

        self.find_table_matches(table_part, available_tables, "id")
    }

    /// Match when column name exactly matches another table's PK.
    fn match_pk(
        &self,
        col_lower: &str,
        source_table: &str,
        table_pk_columns: &HashMap<String, Vec<String>>,
    ) -> Vec<NamingMatch> {
        let mut matches = vec![];
        let source_lower = source_table.to_lowercase();

        for (table, pk_cols) in table_pk_columns {
            // Skip self-references (handled by SelfReference pattern)
            if table.to_lowercase() == source_lower {
                continue;
            }

            for pk in pk_cols {
                if pk.to_lowercase() == *col_lower {
                    matches.push(NamingMatch {
                        target_table: table.clone(),
                        target_column: pk.clone(),
                        rule_name: self.name,
                        base_score: self.base_score,
                    });
                }
            }
        }

        matches
    }

    /// Match when table name is embedded in column.
    fn match_table_embed(
        &self,
        col_lower: &str,
        suffix: &str,
        available_tables: &HashSet<String>,
    ) -> Vec<NamingMatch> {
        if !col_lower.ends_with(suffix) {
            return vec![];
        }

        let base = &col_lower[..col_lower.len() - suffix.len()];
        if base.is_empty() {
            return vec![];
        }

        let mut matches = vec![];

        // Look for tables whose name contains this base as abbreviation
        // e.g., "cust_id" might match "customers" table
        for table in available_tables {
            let table_lower = table.to_lowercase();
            let singular = singularize(&table_lower);

            // Check if table name starts with the column base
            if singular.starts_with(base) && singular.len() > base.len() {
                // Require reasonable abbreviation (at least 3 chars, not more than half dropped)
                if base.len() >= 3 && base.len() * 2 >= singular.len() {
                    matches.push(NamingMatch {
                        target_table: table.clone(),
                        target_column: suffix.trim_start_matches('_').to_string(),
                        rule_name: self.name,
                        base_score: self.base_score,
                    });
                }
            }
        }

        matches
    }

    /// Match self-referential patterns like parent_id.
    fn match_self_reference(
        &self,
        col_lower: &str,
        prefix: &str,
        suffix: &str,
        source_table: &str,
        table_pk_columns: &HashMap<String, Vec<String>>,
    ) -> Vec<NamingMatch> {
        // Check if column matches parent_{suffix} pattern
        if !col_lower.starts_with(prefix) || !col_lower.ends_with(suffix) {
            return vec![];
        }

        // For self-reference, target is the same table
        let source_lower = source_table.to_lowercase();

        // Find PK column of source table, or fallback to "id"
        let (target_column, score) = table_pk_columns
            .get(source_table)
            .and_then(|cols| cols.first())
            .map(|pk| (pk.clone(), self.base_score))
            .unwrap_or_else(|| ("id".to_string(), self.base_score * 0.9));

        vec![NamingMatch {
            target_table: source_lower,
            target_column,
            rule_name: self.name,
            base_score: score,
        }]
    }
}

/// Returns the default 12 naming rules.
pub fn default_naming_rules() -> Vec<NamingRule> {
    vec![
        // Rule 1: fk_prefix - fk_{table}_{col}
        NamingRule {
            name: "fk_prefix",
            description: "Match fk_{table}_{column} pattern (e.g., fk_customer_id → customers.id)",
            base_score: 0.92,
            priority: 100,
            pattern: NamingPattern::FkTableSuffix {
                prefix: "fk_",
                suffix: "_id",
            },
        },
        // Rule 2: fk_simple_prefix - fk_{table}
        NamingRule {
            name: "fk_simple_prefix",
            description: "Match fk_{table} pattern (e.g., fk_customer → customers.id)",
            base_score: 0.88,
            priority: 95,
            pattern: NamingPattern::FkTable { prefix: "fk_" },
        },
        // Rule 3: suffix_id - {table}_id
        NamingRule {
            name: "suffix_id",
            description: "Match {table}_id pattern (e.g., customer_id → customers.id)",
            base_score: 0.85,
            priority: 90,
            pattern: NamingPattern::TableSuffix {
                suffix: "_id",
                target_column: "id",
            },
        },
        // Rule 3b: suffix_id with DW prefix - {table}_id → dim_{table}.{table}_id (same column name)
        NamingRule {
            name: "suffix_id_dim",
            description: "Match {table}_id pattern with dim_ prefix (e.g., employee_id → dim_employees.employee_id)",
            base_score: 0.82,
            priority: 89,
            pattern: NamingPattern::TableSuffixWithPrefixSameCol {
                suffix: "_id",
                table_prefix: "dim_",
            },
        },
        // Rule 4: suffix_key - {table}_key
        NamingRule {
            name: "suffix_key",
            description: "Match {table}_key pattern (e.g., customer_key → customers.key)",
            base_score: 0.80,
            priority: 85,
            pattern: NamingPattern::TableSuffix {
                suffix: "_key",
                target_column: "key",
            },
        },
        // Rule 4b: suffix_key with DW prefix - {table}_key → dim_{table}.{table}_key
        NamingRule {
            name: "suffix_key_dim",
            description: "Match {table}_key pattern with dim_ prefix (e.g., employee_key → dim_employees.employee_key)",
            base_score: 0.78,
            priority: 84,
            pattern: NamingPattern::TableSuffixWithPrefixSameCol {
                suffix: "_key",
                table_prefix: "dim_",
            },
        },
        // Rule 5: suffix_code - {table}_code
        NamingRule {
            name: "suffix_code",
            description: "Match {table}_code pattern (e.g., product_code → products.code)",
            base_score: 0.75,
            priority: 80,
            pattern: NamingPattern::TableSuffix {
                suffix: "_code",
                target_column: "code",
            },
        },
        // Rule 6: suffix_ref - {table}_ref
        NamingRule {
            name: "suffix_ref",
            description: "Match {table}_ref pattern (e.g., order_ref → orders.ref)",
            base_score: 0.75,
            priority: 78,
            pattern: NamingPattern::TableSuffix {
                suffix: "_ref",
                target_column: "ref",
            },
        },
        // Rule 7: suffix_num - {table}_num
        NamingRule {
            name: "suffix_num",
            description: "Match {table}_num pattern (e.g., invoice_num → invoices.num)",
            base_score: 0.70,
            priority: 75,
            pattern: NamingPattern::TableSuffix {
                suffix: "_num",
                target_column: "num",
            },
        },
        // Rule 8: suffix_no - {table}_no
        NamingRule {
            name: "suffix_no",
            description: "Match {table}_no pattern (e.g., po_no → purchase_orders.no)",
            base_score: 0.70,
            priority: 73,
            pattern: NamingPattern::TableSuffix {
                suffix: "_no",
                target_column: "no",
            },
        },
        // Rule 9: semantic_ref - ref_{table}
        NamingRule {
            name: "semantic_ref",
            description: "Match ref_{table} pattern (e.g., ref_customer → customers.id)",
            base_score: 0.70,
            priority: 70,
            pattern: NamingPattern::PrefixTable { prefix: "ref_" },
        },
        // Rule 10: parent_pattern - parent_{suffix}
        NamingRule {
            name: "parent_pattern",
            description:
                "Match parent_{column} pattern for self-reference (e.g., parent_id → same_table.id)",
            base_score: 0.75,
            priority: 68,
            pattern: NamingPattern::SelfReference {
                prefix: "parent_",
                suffix: "_id",
            },
        },
        // Rule 11: pk_match - exact PK name match
        NamingRule {
            name: "pk_match",
            description: "Match when column name exactly matches another table's primary key",
            base_score: 0.65,
            priority: 60,
            pattern: NamingPattern::PrimaryKeyMatch,
        },
        // Rule 12: table_embed - abbreviated table name in column
        NamingRule {
            name: "table_embed",
            description:
                "Match abbreviated table name in column (e.g., cust_id → customers.id)",
            base_score: 0.60,
            priority: 50,
            pattern: NamingPattern::TableNameEmbed { suffix: "_id" },
        },
    ]
}

// Import shared inflection utilities
use super::inflection::{pluralize, singularize};

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tables() -> HashSet<String> {
        ["customers", "orders", "products", "categories", "invoices"]
            .into_iter()
            .map(String::from)
            .collect()
    }

    fn test_pk_columns() -> HashMap<String, Vec<String>> {
        let mut map = HashMap::new();
        map.insert("customers".to_string(), vec!["id".to_string()]);
        map.insert("orders".to_string(), vec!["id".to_string()]);
        map.insert("products".to_string(), vec!["id".to_string(), "code".to_string()]);
        map.insert("categories".to_string(), vec!["id".to_string()]);
        map
    }

    #[test]
    fn test_fk_prefix_pattern() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("fk_customer_id", "orders", &tables, &pks);
        assert!(!matches.is_empty());
        let m = &matches[0];
        assert_eq!(m.target_table, "customers");
        assert_eq!(m.target_column, "id");
        assert_eq!(m.rule_name, "fk_prefix");
    }

    #[test]
    fn test_fk_simple_prefix() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("fk_customer", "orders", &tables, &pks);
        assert!(!matches.is_empty());
        let m = matches.iter().find(|m| m.rule_name == "fk_simple_prefix");
        assert!(m.is_some());
    }

    #[test]
    fn test_suffix_id() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("customer_id", "orders", &tables, &pks);
        assert!(!matches.is_empty());
        let m = &matches[0];
        assert_eq!(m.target_table, "customers");
        assert_eq!(m.target_column, "id");
    }

    #[test]
    fn test_suffix_key() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("order_key", "line_items", &tables, &pks);
        assert!(!matches.is_empty());
        let m = &matches[0];
        assert_eq!(m.target_table, "orders");
        assert_eq!(m.target_column, "key");
    }

    #[test]
    fn test_suffix_code() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("product_code", "orders", &tables, &pks);
        assert!(!matches.is_empty());
        let m = &matches[0];
        assert_eq!(m.target_table, "products");
        assert_eq!(m.target_column, "code");
    }

    #[test]
    fn test_parent_pattern() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("parent_id", "categories", &tables, &pks);
        assert!(!matches.is_empty());
        let m = matches.iter().find(|m| m.rule_name == "parent_pattern");
        assert!(m.is_some());
        let m = m.unwrap();
        assert_eq!(m.target_table, "categories");
    }

    #[test]
    fn test_pk_match() {
        let collector = NamingSignalCollector::new();
        let mut tables = test_tables();
        tables.insert("line_items".to_string());
        let mut pks = test_pk_columns();
        pks.insert("line_items".to_string(), vec!["id".to_string()]);

        // If a column named "id" exists and another table has "id" as PK
        let matches = collector.collect_signals("id", "orders", &tables, &pks);
        // Should match other tables with id as PK
        let pk_matches: Vec<_> = matches.iter().filter(|m| m.rule_name == "pk_match").collect();
        assert!(!pk_matches.is_empty());
    }

    #[test]
    fn test_table_embed() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        // "cust_id" should potentially match "customers" table
        let matches = collector.collect_signals("cust_id", "orders", &tables, &pks);
        let embed_matches: Vec<_> = matches
            .iter()
            .filter(|m| m.rule_name == "table_embed")
            .collect();
        assert!(!embed_matches.is_empty());
    }

    #[test]
    fn test_no_match() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        let matches = collector.collect_signals("random_column", "orders", &tables, &pks);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_to_signal() {
        let m = NamingMatch {
            target_table: "customers".to_string(),
            target_column: "id".to_string(),
            rule_name: "suffix_id",
            base_score: 0.85,
        };

        let signal = m.to_signal();
        assert_eq!(signal.score, 0.85);
        assert!(signal.is_positive());
        assert!(matches!(signal.source, SignalSource::NamingConvention { .. }));
    }

    #[test]
    fn test_deduplication() {
        let collector = NamingSignalCollector::new();
        let tables = test_tables();
        let pks = test_pk_columns();

        // Multiple rules might match the same target
        let matches = collector.collect_signals("customer_id", "orders", &tables, &pks);

        // Should be deduplicated - only one match per (table, column) pair
        let customer_id_matches: Vec<_> = matches
            .iter()
            .filter(|m| m.target_table == "customers" && m.target_column == "id")
            .collect();
        assert_eq!(customer_id_matches.len(), 1);
    }
}
