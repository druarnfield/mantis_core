//! Inference rules for relationship discovery.
//!
//! Each rule implements a heuristic for finding potential relationships based on
//! column naming conventions.

use std::collections::HashSet;

// Use shared inflection utilities and re-export for backward compatibility
pub use super::signals::inflection::{pluralize, singularize};

/// A rule that can match column names to potential target tables.
#[derive(Debug, Clone)]
pub struct InferenceRule {
    /// Rule identifier
    pub name: &'static str,
    /// Human-readable description
    pub description: &'static str,
    /// Base confidence score for matches from this rule
    pub base_confidence: f64,
    /// The matching function
    matcher: RuleMatcher,
}

/// The type of matching logic for a rule.
#[derive(Debug, Clone)]
enum RuleMatcher {
    /// Match columns ending with _id suffix (e.g., customer_id -> customers.id)
    SuffixId,
    /// Match columns ending with _key suffix (e.g., customer_key -> customers.key)
    SuffixKey,
    /// Match columns ending with _code suffix (e.g., product_code -> products.code)
    SuffixCode,
    /// Match columns with fk_ prefix (e.g., fk_customer_id -> customers.id)
    FkPrefix,
    /// Match columns with identical names across tables (e.g., product_id in both tables)
    ColumnNameMatch,
    /// Match when column name matches another table's primary key
    PkMatch,
}

/// A successful match from an inference rule.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleMatch {
    /// The matched target table name
    pub target_table: String,
    /// The matched target column name
    pub target_column: String,
    /// Base confidence from the rule
    pub base_confidence: f64,
    /// The rule that matched
    pub rule_name: &'static str,
}

impl InferenceRule {
    /// Try to match this rule against a column name.
    ///
    /// # Arguments
    /// * `column_name` - The column name to match
    /// * `available_tables` - Set of available table names (lowercase)
    /// * `table_pk_columns` - Map of table name -> primary key column names
    ///
    /// # Returns
    /// A list of potential matches (may be empty)
    pub fn try_match(
        &self,
        column_name: &str,
        available_tables: &HashSet<String>,
        table_pk_columns: &std::collections::HashMap<String, Vec<String>>,
    ) -> Vec<RuleMatch> {
        let col_lower = column_name.to_lowercase();

        match &self.matcher {
            RuleMatcher::SuffixId => self.match_suffix_id(&col_lower, available_tables),
            RuleMatcher::SuffixKey => self.match_suffix_key(&col_lower, available_tables),
            RuleMatcher::SuffixCode => self.match_suffix_code(&col_lower, available_tables),
            RuleMatcher::FkPrefix => self.match_fk_prefix(&col_lower, available_tables),
            RuleMatcher::ColumnNameMatch => self.match_column_name(&col_lower, available_tables, table_pk_columns),
            RuleMatcher::PkMatch => self.match_pk(&col_lower, table_pk_columns),
        }
    }

    /// Match columns ending with _id (e.g., customer_id -> customers.id)
    fn match_suffix_id(&self, col_lower: &str, available_tables: &HashSet<String>) -> Vec<RuleMatch> {
        if !col_lower.ends_with("_id") {
            return vec![];
        }

        let base = &col_lower[..col_lower.len() - 3]; // Remove "_id"
        let mut matches = vec![];

        // Try plural form (customer_id -> customers)
        let plural = pluralize(base);
        if available_tables.contains(&plural) {
            matches.push(RuleMatch {
                target_table: plural,
                target_column: "id".to_string(),
                base_confidence: self.base_confidence,
                rule_name: self.name,
            });
        }

        // Try singular form (customers_id -> customers)
        if available_tables.contains(base) {
            matches.push(RuleMatch {
                target_table: base.to_string(),
                target_column: "id".to_string(),
                base_confidence: self.base_confidence * 0.95, // Slightly lower confidence for singular
                rule_name: self.name,
            });
        }

        matches
    }

    /// Match columns ending with _key (e.g., customer_key -> customers.key)
    fn match_suffix_key(&self, col_lower: &str, available_tables: &HashSet<String>) -> Vec<RuleMatch> {
        if !col_lower.ends_with("_key") {
            return vec![];
        }

        let base = &col_lower[..col_lower.len() - 4]; // Remove "_key"
        let mut matches = vec![];

        let plural = pluralize(base);
        if available_tables.contains(&plural) {
            matches.push(RuleMatch {
                target_table: plural,
                target_column: "key".to_string(),
                base_confidence: self.base_confidence,
                rule_name: self.name,
            });
        }

        if available_tables.contains(base) {
            matches.push(RuleMatch {
                target_table: base.to_string(),
                target_column: "key".to_string(),
                base_confidence: self.base_confidence * 0.95,
                rule_name: self.name,
            });
        }

        matches
    }

    /// Match columns ending with _code (e.g., product_code -> products.code)
    fn match_suffix_code(&self, col_lower: &str, available_tables: &HashSet<String>) -> Vec<RuleMatch> {
        if !col_lower.ends_with("_code") {
            return vec![];
        }

        let base = &col_lower[..col_lower.len() - 5]; // Remove "_code"
        let mut matches = vec![];

        let plural = pluralize(base);
        if available_tables.contains(&plural) {
            matches.push(RuleMatch {
                target_table: plural.clone(),
                target_column: format!("{}_code", singularize(&plural)),
                base_confidence: self.base_confidence,
                rule_name: self.name,
            });
        }

        if available_tables.contains(base) {
            matches.push(RuleMatch {
                target_table: base.to_string(),
                target_column: format!("{}_code", base),
                base_confidence: self.base_confidence * 0.95,
                rule_name: self.name,
            });
        }

        matches
    }

    /// Match columns with fk_ prefix (e.g., fk_customer_id -> customers.id)
    fn match_fk_prefix(&self, col_lower: &str, available_tables: &HashSet<String>) -> Vec<RuleMatch> {
        if !col_lower.starts_with("fk_") {
            return vec![];
        }

        let rest = &col_lower[3..]; // Remove "fk_"

        // Try to parse fk_tablename_column or fk_tablename
        // Common patterns: fk_customer_id, fk_customers_id, fk_order_id

        let mut matches = vec![];

        // Check if rest ends with _id
        if let Some(base) = rest.strip_suffix("_id") {
            let plural = pluralize(base);

            if available_tables.contains(&plural) {
                matches.push(RuleMatch {
                    target_table: plural,
                    target_column: "id".to_string(),
                    base_confidence: self.base_confidence,
                    rule_name: self.name,
                });
            }
            if available_tables.contains(base) {
                matches.push(RuleMatch {
                    target_table: base.to_string(),
                    target_column: "id".to_string(),
                    base_confidence: self.base_confidence,
                    rule_name: self.name,
                });
            }
        }

        matches
    }

    /// Match columns with identical names across tables
    fn match_column_name(
        &self,
        col_lower: &str,
        available_tables: &HashSet<String>,
        table_pk_columns: &std::collections::HashMap<String, Vec<String>>,
    ) -> Vec<RuleMatch> {
        // Only match columns that look like join columns (end with _id, _key, _code, _sk, _bk)
        let join_suffixes = ["_id", "_key", "_code", "_sk", "_bk", "_fk"];
        if !join_suffixes.iter().any(|s| col_lower.ends_with(s)) {
            return vec![];
        }

        let mut matches = vec![];

        // Find tables that have a column with this exact name
        for (table, pk_cols) in table_pk_columns {
            if pk_cols.iter().any(|pk| pk.to_lowercase() == *col_lower) {
                matches.push(RuleMatch {
                    target_table: table.clone(),
                    target_column: col_lower.to_string(),
                    base_confidence: self.base_confidence,
                    rule_name: self.name,
                });
            }
        }

        // Also check tables whose name is embedded in the column
        // e.g., customer_id might join to a table named "customer" on customer_id
        for suffix in &join_suffixes {
            if let Some(base) = col_lower.strip_suffix(suffix) {
                if available_tables.contains(base) {
                    // Check if this table has the same column name
                    if let Some(cols) = table_pk_columns.get(base) {
                        if cols.iter().any(|c| c.to_lowercase() == *col_lower) {
                            matches.push(RuleMatch {
                                target_table: base.to_string(),
                                target_column: col_lower.to_string(),
                                base_confidence: self.base_confidence,
                                rule_name: self.name,
                            });
                        }
                    }
                }
            }
        }

        matches
    }

    /// Match when column name matches another table's primary key
    fn match_pk(
        &self,
        col_lower: &str,
        table_pk_columns: &std::collections::HashMap<String, Vec<String>>,
    ) -> Vec<RuleMatch> {
        let mut matches = vec![];

        for (table, pk_cols) in table_pk_columns {
            for pk in pk_cols {
                if pk.to_lowercase() == *col_lower {
                    matches.push(RuleMatch {
                        target_table: table.clone(),
                        target_column: pk.clone(),
                        base_confidence: self.base_confidence,
                        rule_name: self.name,
                    });
                }
            }
        }

        matches
    }
}

/// Returns the default set of inference rules, ordered by priority.
pub fn default_rules() -> Vec<InferenceRule> {
    vec![
        InferenceRule {
            name: "fk_prefix",
            description: "Match columns with fk_ prefix (e.g., fk_customer_id -> customers.id)",
            base_confidence: 0.90,
            matcher: RuleMatcher::FkPrefix,
        },
        InferenceRule {
            name: "suffix_id",
            description: "Match columns ending with _id (e.g., customer_id -> customers.id)",
            base_confidence: 0.85,
            matcher: RuleMatcher::SuffixId,
        },
        InferenceRule {
            name: "suffix_key",
            description: "Match columns ending with _key (e.g., customer_key -> customers.key)",
            base_confidence: 0.80,
            matcher: RuleMatcher::SuffixKey,
        },
        InferenceRule {
            name: "suffix_code",
            description: "Match columns ending with _code (e.g., product_code -> products.code)",
            base_confidence: 0.75,
            matcher: RuleMatcher::SuffixCode,
        },
        InferenceRule {
            name: "column_name_match",
            description: "Match identical column names with join suffixes across tables",
            base_confidence: 0.70,
            matcher: RuleMatcher::ColumnNameMatch,
        },
        InferenceRule {
            name: "pk_match",
            description: "Match when column name matches another table's primary key",
            base_confidence: 0.65,
            matcher: RuleMatcher::PkMatch,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suffix_id_rule() {
        let rule = InferenceRule {
            name: "suffix_id",
            description: "test",
            base_confidence: 0.85,
            matcher: RuleMatcher::SuffixId,
        };

        let mut tables = HashSet::new();
        tables.insert("customers".to_string());
        tables.insert("orders".to_string());
        tables.insert("products".to_string());

        let pk_cols = std::collections::HashMap::new();

        let matches = rule.try_match("customer_id", &tables, &pk_cols);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].target_table, "customers");
        assert_eq!(matches[0].target_column, "id");
    }

    #[test]
    fn test_fk_prefix_rule() {
        let rule = InferenceRule {
            name: "fk_prefix",
            description: "test",
            base_confidence: 0.90,
            matcher: RuleMatcher::FkPrefix,
        };

        let mut tables = HashSet::new();
        tables.insert("customers".to_string());
        tables.insert("orders".to_string());

        let pk_cols = std::collections::HashMap::new();

        let matches = rule.try_match("fk_customer_id", &tables, &pk_cols);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].target_table, "customers");
        assert_eq!(matches[0].target_column, "id");
    }
}
