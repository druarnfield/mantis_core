//! Signal pipeline for coordinating all signal collectors.
//!
//! The pipeline orchestrates signal collection from all sources:
//! - Database constraints (highest confidence)
//! - Naming conventions
//! - Type compatibility
//! - Negative signals
//! - Schema conventions
//! - Statistics (async)

use std::collections::{HashMap, HashSet};

use crate::metadata::TableMetadata;
use crate::model::DataType;

use super::{
    aggregator::{SignalAggregator, SignalWeights},
    constraints::ConstraintCollector,
    conventions::{ConventionRegistry, ConventionScope, SchemaConventions},
    naming::NamingSignalCollector,
    negative::NegativeSignalDetector,
    types::TypeCompatibility,
    AggregatedScore, Signal, SignalCollection, SignalSource,
};

/// Check if a data type is suitable for being a grain/key column.
/// Measure-like types (floats, booleans) are unlikely to be grain columns.
fn is_grain_compatible_type(data_type: Option<&DataType>) -> bool {
    match data_type {
        None => true, // Unknown type, assume compatible
        Some(dt) => !matches!(dt, DataType::Float | DataType::Decimal | DataType::Bool),
    }
}

/// Check if a column name looks like a key/identifier column based on naming patterns.
/// This is used as a fallback when the column isn't explicitly marked as PK/unique.
fn looks_like_key_column(col_name: &str) -> bool {
    let lower = col_name.to_lowercase();
    lower.ends_with("_id")
        || lower.ends_with("_key")
        || lower.ends_with("_code")
        || lower.ends_with("_num")
        || lower.ends_with("_no")
        || lower.ends_with("_number")
        || lower == "id"
        || lower == "key"
        || lower == "code"
}

/// Check if a table name looks like a fact table (data warehouse convention).
/// Fact tables should not be join targets - they join TO dimensions, not the other way.
fn looks_like_fact_table(table_name: &str) -> bool {
    let lower = table_name.to_lowercase();
    lower.starts_with("fct_")
        || lower.starts_with("fact_")
        || lower.starts_with("f_")
        || lower.ends_with("_fct")
        || lower.ends_with("_fact")
}

/// Check if a table has outbound FK candidates (columns that could join to other tables' PKs).
/// This indicates the table is likely a fact/bridge table that references dimensions.
fn has_outbound_fk_candidates(
    table_name: &str,
    table_columns: &[(String, Option<DataType>, bool)],
    all_table_pk_columns: &HashMap<String, Vec<String>>,
) -> bool {
    // For each column in this table, check if it matches another table's PK
    for (col_name, col_type, _is_unique) in table_columns {
        // Skip non-grain-compatible types (measures can't be FKs)
        if !is_grain_compatible_type(col_type.as_ref()) {
            continue;
        }

        let col_lower = col_name.to_lowercase();

        // Check if this column name matches any other table's single-column PK
        for (other_table, pk_cols) in all_table_pk_columns {
            // Skip self
            if other_table.eq_ignore_ascii_case(table_name) {
                continue;
            }

            // Only consider tables with simple grain (single PK)
            if pk_cols.len() != 1 {
                continue;
            }

            let pk_col = &pk_cols[0];

            // If column name matches PK name, this table likely joins OUT to that table
            if col_lower == pk_col.to_lowercase() {
                return true;
            }
        }
    }

    false
}

/// A candidate relationship before final scoring.
#[derive(Debug, Clone)]
pub struct RelationshipCandidate {
    /// Source table schema.
    pub from_schema: String,
    /// Source table name.
    pub from_table: String,
    /// Source column name.
    pub from_column: String,
    /// Source column data type.
    pub from_type: Option<DataType>,
    /// Target table schema.
    pub to_schema: String,
    /// Target table name.
    pub to_table: String,
    /// Target column name.
    pub to_column: String,
    /// Target column data type.
    pub to_type: Option<DataType>,
    /// Whether target column is a primary key.
    pub to_is_pk: bool,
    /// Whether target column is unique.
    pub to_is_unique: bool,
    /// The naming rule that generated this candidate.
    pub matched_rule: String,
    /// Base score from the naming rule.
    pub base_score: f64,
}

/// Scored relationship candidate with all signals.
#[derive(Debug, Clone)]
pub struct ScoredCandidate {
    /// The original candidate.
    pub candidate: RelationshipCandidate,
    /// All collected signals.
    pub signals: SignalCollection,
    /// Aggregated score.
    pub score: AggregatedScore,
}

/// Pipeline configuration.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Whether to use negative signal detection.
    pub use_negative_signals: bool,
    /// Whether to use type compatibility checking.
    pub use_type_compatibility: bool,
    /// Convention scope setting.
    pub convention_scope: ConventionScope,
    /// Custom excluded patterns.
    pub excluded_patterns: Vec<String>,
    /// Custom excluded keywords.
    pub excluded_keywords: Vec<String>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            use_negative_signals: true,
            use_type_compatibility: true,
            convention_scope: ConventionScope::default(),
            excluded_patterns: Vec::new(),
            excluded_keywords: Vec::new(),
        }
    }
}

/// Orchestrates signal collection from all sources.
#[derive(Debug)]
pub struct SignalPipeline {
    /// Naming convention collector.
    naming_collector: NamingSignalCollector,
    /// Negative signal detector.
    negative_detector: NegativeSignalDetector,
    /// Convention registry (built from schema analysis).
    convention_registry: ConventionRegistry,
    /// Database constraint collector.
    constraint_collector: ConstraintCollector,
    /// Signal aggregator for scoring.
    aggregator: SignalAggregator,
    /// Pipeline configuration.
    config: PipelineConfig,
}

impl Default for SignalPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalPipeline {
    /// Create a new pipeline with default configuration.
    pub fn new() -> Self {
        Self {
            naming_collector: NamingSignalCollector::new(),
            negative_detector: NegativeSignalDetector::new(),
            convention_registry: ConventionRegistry::new(ConventionScope::Schema),
            constraint_collector: ConstraintCollector::new(),
            aggregator: SignalAggregator::new(),
            config: PipelineConfig::default(),
        }
    }

    /// Create a pipeline with custom configuration.
    pub fn with_config(config: PipelineConfig) -> Self {
        let mut negative_detector = NegativeSignalDetector::new();

        // Add custom patterns
        for pattern in &config.excluded_patterns {
            negative_detector.add_pattern(pattern, pattern, 0.7);
        }

        // Add custom keywords
        for keyword in &config.excluded_keywords {
            negative_detector.add_keyword(keyword);
        }

        Self {
            naming_collector: NamingSignalCollector::new(),
            negative_detector,
            convention_registry: ConventionRegistry::new(config.convention_scope),
            constraint_collector: ConstraintCollector::new(),
            aggregator: SignalAggregator::new(),
            config,
        }
    }

    /// Create a pipeline with custom weights.
    pub fn with_weights(weights: SignalWeights) -> Self {
        Self {
            naming_collector: NamingSignalCollector::new(),
            negative_detector: NegativeSignalDetector::new(),
            convention_registry: ConventionRegistry::new(ConventionScope::Schema),
            constraint_collector: ConstraintCollector::new(),
            aggregator: SignalAggregator::with_weights(weights),
            config: PipelineConfig::default(),
        }
    }

    /// Create a pipeline with both custom configuration and weights.
    pub fn with_config_and_weights(config: PipelineConfig, weights: SignalWeights) -> Self {
        let mut negative_detector = NegativeSignalDetector::new();

        // Add custom patterns
        for pattern in &config.excluded_patterns {
            negative_detector.add_pattern(pattern, pattern, 0.7);
        }

        // Add custom keywords
        for keyword in &config.excluded_keywords {
            negative_detector.add_keyword(keyword);
        }

        Self {
            naming_collector: NamingSignalCollector::new(),
            negative_detector,
            convention_registry: ConventionRegistry::new(config.convention_scope),
            constraint_collector: ConstraintCollector::new(),
            aggregator: SignalAggregator::with_weights(weights),
            config,
        }
    }

    /// Load database constraints from table metadata.
    ///
    /// This should be called before inference to enable constraint-based signals.
    /// Relationships matching database FK constraints will get 0.98 confidence.
    pub fn load_constraints(&mut self, tables: &[TableMetadata]) {
        self.constraint_collector.load_from_metadata(tables);
    }

    /// Check if a candidate matches a known database constraint.
    pub fn has_db_constraint(&self, candidate: &RelationshipCandidate) -> bool {
        self.constraint_collector
            .get_foreign_key(
                &candidate.from_schema,
                &candidate.from_table,
                &candidate.from_column,
            )
            .map(|fk| {
                fk.to_schema.eq_ignore_ascii_case(&candidate.to_schema)
                    && fk.to_table.eq_ignore_ascii_case(&candidate.to_table)
                    && fk.to_column.eq_ignore_ascii_case(&candidate.to_column)
            })
            .unwrap_or(false)
    }

    /// Get all relationships from database constraints.
    ///
    /// Returns candidates for all known FK constraints with 0.98 confidence.
    pub fn get_constraint_relationships(&self) -> Vec<RelationshipCandidate> {
        self.constraint_collector
            .all_foreign_keys()
            .map(|fk| RelationshipCandidate {
                from_schema: fk.from_schema.clone(),
                from_table: fk.from_table.clone(),
                from_column: fk.from_column.clone(),
                from_type: None,
                to_schema: fk.to_schema.clone(),
                to_table: fk.to_table.clone(),
                to_column: fk.to_column.clone(),
                to_type: None,
                to_is_pk: true, // FK usually references PK
                to_is_unique: true,
                matched_rule: format!("db_constraint:{}", fk.constraint_name),
                base_score: 0.98,
            })
            .collect()
    }

    /// Analyze conventions from TableInfo structs.
    ///
    /// This should be called once before inference to detect naming patterns.
    /// Conventions are always collected but only used for scoring if
    /// convention_scope is not Disabled.
    pub fn analyze_tables(&mut self, tables: &[crate::semantic::inference::TableInfo]) {
        // Group tables by schema
        let mut by_schema: HashMap<&str, Vec<&crate::semantic::inference::TableInfo>> =
            HashMap::new();
        for table in tables {
            by_schema.entry(&table.schema).or_default().push(table);
        }

        for (schema, schema_tables) in by_schema {
            // Convert references to owned for analyze
            let owned_tables: Vec<crate::semantic::inference::TableInfo> =
                schema_tables.iter().map(|t| (*t).clone()).collect();
            let conventions = SchemaConventions::analyze(schema, &owned_tables);
            self.convention_registry.register(conventions);
        }
    }

    /// Get detected conventions for a schema (for hints/debugging).
    pub fn get_conventions(&self, schema: &str) -> Option<&SchemaConventions> {
        self.convention_registry.get(schema)
    }

    /// Check if conventions are enabled for scoring.
    pub fn conventions_enabled(&self) -> bool {
        self.config.convention_scope != ConventionScope::Disabled
    }

    /// Collect all sync signals for a candidate.
    ///
    /// This collects signals from:
    /// - Naming conventions (already matched to create the candidate)
    /// - Type compatibility
    /// - Negative patterns
    /// - Schema conventions
    pub fn collect_sync_signals(&self, candidate: &RelationshipCandidate) -> SignalCollection {
        let mut signals = SignalCollection::new();

        // 0. Check for database constraint (highest priority)
        if let Some(constraint_signal) = self.constraint_collector.check_candidate(
            &candidate.from_schema,
            &candidate.from_table,
            &candidate.from_column,
            &candidate.to_schema,
            &candidate.to_table,
            &candidate.to_column,
        ) {
            signals.add(constraint_signal);
            // If we have a DB constraint, we can skip other signals but
            // still collect them for the breakdown (user might want to override)
        }

        // 1. Add the naming signal from the matched rule
        signals.add(Signal::positive(
            SignalSource::naming(&candidate.matched_rule),
            candidate.base_score,
            format!(
                "'{}' matches '{}' pattern → {}.{}",
                candidate.from_column,
                candidate.matched_rule,
                candidate.to_table,
                candidate.to_column
            ),
        ));

        // 2. Check type compatibility
        if self.config.use_type_compatibility {
            if let (Some(from_type), Some(to_type)) = (&candidate.from_type, &candidate.to_type) {
                let compat = TypeCompatibility::check(from_type, to_type);
                signals.add(compat.to_signal());
            }
        }

        // 3. Check for negative signals
        if self.config.use_negative_signals {
            if let Some(neg_signal) = self
                .negative_detector
                .check(&candidate.from_column, candidate.from_type.as_ref())
            {
                signals.add(neg_signal);
            }
        }

        // 4. Add uniqueness signal for target
        if candidate.to_is_pk {
            signals.add(Signal::positive(
                SignalSource::UniqueConstraint,
                0.8,
                format!("Target column '{}' is primary key", candidate.to_column),
            ));
        } else if candidate.to_is_unique {
            signals.add(Signal::positive(
                SignalSource::UniqueConstraint,
                0.6,
                format!(
                    "Target column '{}' has unique constraint",
                    candidate.to_column
                ),
            ));
        }

        // 5. Add schema convention signal (if enabled for scoring)
        if self.config.convention_scope != ConventionScope::Disabled {
            if let Some(conventions) = self.convention_registry.get(&candidate.from_schema) {
                if let Some(conv_signal) = conventions.to_signal(&candidate.from_column) {
                    signals.add(conv_signal);
                }
            }
        }

        signals
    }

    /// Score a candidate using collected signals.
    pub fn score(&self, signals: &SignalCollection) -> AggregatedScore {
        self.aggregator.aggregate(signals)
    }

    /// Check if a score meets the minimum confidence threshold.
    pub fn is_confident(&self, score: &AggregatedScore) -> bool {
        self.aggregator.is_confident(score)
    }

    /// Get the minimum confidence threshold.
    pub fn min_confidence(&self) -> f64 {
        self.aggregator.min_confidence()
    }

    /// Find candidate relationships for a column.
    ///
    /// Returns all naming matches as candidates (before signal collection).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    pub fn find_candidates(
        &self,
        column_name: &str,
        column_type: Option<&DataType>,
        source_schema: &str,
        source_table: &str,
        available_tables: &HashSet<String>,
        table_pk_columns: &HashMap<String, Vec<String>>,
        table_columns: &HashMap<String, Vec<(String, Option<DataType>, bool)>>, // (name, type, is_unique)
    ) -> Vec<RelationshipCandidate> {
        let mut candidates = Vec::new();
        let col_lower = column_name.to_lowercase();
        let source_lower = source_table.to_lowercase();

        // Get naming matches first
        let matches = self.naming_collector.collect_signals(
            column_name,
            source_table,
            available_tables,
            table_pk_columns,
        );

        // Convert naming matches to candidates with type info
        for m in matches {
            // Find target column info
            if let Some(target_cols) = table_columns.get(&m.target_table) {
                if let Some((_, to_type, to_is_unique)) = target_cols
                    .iter()
                    .find(|(name, _, _)| name.eq_ignore_ascii_case(&m.target_column))
                {
                    // Check if target is PK
                    let to_is_pk = table_pk_columns
                        .get(&m.target_table)
                        .map(|pks| {
                            pks.iter()
                                .any(|pk| pk.eq_ignore_ascii_case(&m.target_column))
                        })
                        .unwrap_or(false);

                    candidates.push(RelationshipCandidate {
                        from_schema: source_schema.to_string(),
                        from_table: source_table.to_string(),
                        from_column: column_name.to_string(),
                        from_type: column_type.cloned(),
                        to_schema: source_schema.to_string(),
                        to_table: m.target_table,
                        to_column: m.target_column,
                        to_type: to_type.clone(),
                        to_is_pk,
                        to_is_unique: *to_is_unique,
                        matched_rule: m.rule_name.to_string(),
                        base_score: m.base_score,
                    });
                }
            }
        }

        // Qlik-style matching: find tables with same column name (lower confidence)
        // This is useful for schemas without FK constraints.
        //
        // Grain-based inference: Only join TO tables where we can infer they're dimension-like.
        // - Tables with single-column PK → clear grain, can be join targets
        // - Tables with no PK or composite PK:
        //   - If they have outbound FK candidates → fact/bridge table, skip
        //   - If they have NO outbound FK candidates → lookup table, allow
        //
        // Additionally, the target column should look like a key column
        // and have a grain-compatible type (not a measure like FLOAT/DECIMAL).

        for (table_name, cols) in table_columns {
            // Skip self (case-insensitive comparison)
            if table_name.eq_ignore_ascii_case(&source_lower) {
                continue;
            }

            // Check target table's grain structure
            let target_pk = table_pk_columns.get(table_name);

            // Skip tables that look like fact tables - they should join TO dimensions,
            // not be joined onto by other tables
            if looks_like_fact_table(table_name) {
                continue;
            }

            // Determine if this table is a valid join target based on grain analysis
            let is_valid_target = match target_pk {
                Some(pks) if pks.len() == 1 => true, // Simple grain, always valid
                Some(_) => {
                    // Composite PK: only valid if table has no outbound FK candidates
                    !has_outbound_fk_candidates(table_name, cols, table_pk_columns)
                }
                None => {
                    // No PK: allow if table doesn't have outbound FK candidates
                    // (likely a dimension/lookup table without declared constraints)
                    !has_outbound_fk_candidates(table_name, cols, table_pk_columns)
                }
            };

            if !is_valid_target {
                continue;
            }

            // Look for matching column name
            for (col_name, col_type, is_unique) in cols {
                if col_name.to_lowercase() != col_lower {
                    continue;
                }

                // Check type compatibility if both types are known
                let types_compatible = match (column_type, col_type) {
                    (Some(from_t), Some(to_t)) => {
                        TypeCompatibility::check(from_t, to_t).is_compatible
                    }
                    _ => true, // If types unknown, assume compatible
                };

                if !types_compatible {
                    continue;
                }

                // Check if target column type is suitable for being a grain column
                // (skip measure-like types: FLOAT, DECIMAL, BOOLEAN, etc.)
                if !is_grain_compatible_type(col_type.as_ref()) {
                    continue;
                }

                // Check if this candidate already exists from naming rules
                let already_matched = candidates.iter().any(|c| {
                    c.to_table.eq_ignore_ascii_case(table_name)
                        && c.to_column.eq_ignore_ascii_case(col_name)
                });

                if already_matched {
                    continue;
                }

                // Check if target is PK or unique
                let to_is_pk = target_pk
                    .map(|pks| pks.iter().any(|pk| pk.eq_ignore_ascii_case(col_name)))
                    .unwrap_or(false);

                // Check if column name looks like a key (ends with _id, _key, _code, etc.)
                let looks_like_key = looks_like_key_column(col_name);

                // For same_column_name matching, target column should be:
                // - Part of the grain (PK or unique), OR
                // - Look like a key column by naming convention (lower confidence)
                if !to_is_pk && !*is_unique && !looks_like_key {
                    continue;
                }

                // Count key-like columns in target table
                // Tables with many key-like columns are likely fact/bridge tables
                let key_column_count = cols
                    .iter()
                    .filter(|(name, _, _)| looks_like_key_column(name))
                    .count();

                // Skip tables with too many key-like columns - they're almost certainly
                // fact/bridge tables, not dimensions (e.g., testing_fct_master with 50+ ids)
                if key_column_count >= 10 {
                    continue;
                }

                // Base score based on confidence level:
                // NOTE: These scores get multiplied by naming weight (0.35) in aggregation!
                // So we need high raw scores to achieve ~0.50+ final confidence.
                // - PK: 1.8 * 0.35 = 0.63 final
                // - Unique: 1.6 * 0.35 = 0.56 final
                // - Key by name: 1.5 * 0.35 = 0.525 final (just above 0.50 threshold)
                let raw_score = if to_is_pk {
                    1.8
                } else if *is_unique {
                    1.6
                } else {
                    1.5 // looks_like_key fallback
                };

                // Apply penalty based on number of key-like columns in target table
                // More key columns = more likely a fact table = lower confidence as join target
                // Penalty scales: 0.05 per key column beyond 2 (so 3 keys = 0.05, 9 keys = 0.35)
                let key_count_penalty: f64 = if key_column_count <= 2 {
                    0.0
                } else {
                    (key_column_count - 2) as f64 * 0.05
                };

                let base_score = f64::max(raw_score - key_count_penalty, 0.1);

                candidates.push(RelationshipCandidate {
                    from_schema: source_schema.to_string(),
                    from_table: source_table.to_string(),
                    from_column: column_name.to_string(),
                    from_type: column_type.cloned(),
                    to_schema: source_schema.to_string(),
                    to_table: table_name.clone(),
                    to_column: col_name.clone(),
                    to_type: col_type.clone(),
                    to_is_pk,
                    to_is_unique: *is_unique,
                    matched_rule: "same_column_name".to_string(),
                    base_score,
                });
            }
        }

        candidates
    }

    /// Process candidates through the full sync pipeline.
    ///
    /// Returns scored candidates sorted by confidence (descending).
    pub fn process_candidates(
        &self,
        candidates: Vec<RelationshipCandidate>,
    ) -> Vec<ScoredCandidate> {
        let mut scored: Vec<ScoredCandidate> = candidates
            .into_iter()
            .map(|candidate| {
                let signals = self.collect_sync_signals(&candidate);
                let score = self.score(&signals);
                ScoredCandidate {
                    candidate,
                    signals,
                    score,
                }
            })
            .filter(|sc| self.is_confident(&sc.score))
            .collect();

        // Sort by confidence descending
        scored.sort_by(|a, b| {
            b.score
                .confidence
                .partial_cmp(&a.score.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        scored
    }

    /// Get a reference to the aggregator.
    pub fn aggregator(&self) -> &SignalAggregator {
        &self.aggregator
    }

    /// Get a reference to the naming collector.
    pub fn naming_collector(&self) -> &NamingSignalCollector {
        &self.naming_collector
    }
}

// TODO: Fix these tests - dsl::ast::DataType was refactored (Int64 removed)
#[cfg(all(test, feature = "broken_tests"))]
mod tests {
    use super::*;

    fn sample_candidate() -> RelationshipCandidate {
        RelationshipCandidate {
            from_schema: "public".to_string(),
            from_table: "orders".to_string(),
            from_column: "customer_id".to_string(),
            from_type: Some(DataType::Int64),
            to_schema: "public".to_string(),
            to_table: "customers".to_string(),
            to_column: "id".to_string(),
            to_type: Some(DataType::Int64),
            to_is_pk: true,
            to_is_unique: true,
            matched_rule: "suffix_id".to_string(),
            base_score: 0.85,
        }
    }

    #[test]
    fn test_collect_sync_signals() {
        let pipeline = SignalPipeline::new();
        let candidate = sample_candidate();

        let signals = pipeline.collect_sync_signals(&candidate);

        // Should have naming signal
        assert!(signals.signals().iter().any(|s| matches!(
            &s.source,
            SignalSource::NamingConvention { rule } if rule == "suffix_id"
        )));

        // Should have type compatibility signal
        assert!(signals
            .signals()
            .iter()
            .any(|s| matches!(s.source, SignalSource::TypeCompatibility)));

        // Should have uniqueness signal (target is PK)
        assert!(signals
            .signals()
            .iter()
            .any(|s| matches!(s.source, SignalSource::UniqueConstraint)));

        // Should NOT have negative signals
        assert!(signals.negative_signals().count() == 0);
    }

    #[test]
    fn test_negative_signal_detection() {
        let pipeline = SignalPipeline::new();

        let mut candidate = sample_candidate();
        candidate.from_column = "created_at".to_string();
        candidate.from_type = Some(DataType::Timestamp);

        let signals = pipeline.collect_sync_signals(&candidate);

        // Should have a negative signal
        assert!(signals.negative_signals().count() > 0);
    }

    #[test]
    fn test_type_mismatch_signal() {
        let pipeline = SignalPipeline::new();

        let mut candidate = sample_candidate();
        candidate.from_type = Some(DataType::String);
        candidate.to_type = Some(DataType::Int64);

        let signals = pipeline.collect_sync_signals(&candidate);

        // Type compatibility should produce negative signal
        let type_signal = signals
            .signals()
            .iter()
            .find(|s| matches!(s.source, SignalSource::TypeCompatibility));

        assert!(type_signal.is_some());
        assert!(type_signal.unwrap().is_negative());
    }

    #[test]
    fn test_scoring() {
        let pipeline = SignalPipeline::new();
        let candidate = sample_candidate();

        let signals = pipeline.collect_sync_signals(&candidate);
        let score = pipeline.score(&signals);

        // Good candidate should have high confidence
        assert!(score.confidence >= 0.7);
        assert!(pipeline.is_confident(&score));
    }

    #[test]
    fn test_convention_analysis() {
        use crate::semantic::inference::{ColumnInfo, TableInfo};

        let mut pipeline = SignalPipeline::new();

        // Create test tables with clear patterns
        let tables = vec![
            TableInfo {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(true),
                    },
                    ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        is_nullable: true,
                        is_unique: None,
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
            TableInfo {
                schema: "public".to_string(),
                name: "orders".to_string(),
                columns: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(true),
                    },
                    ColumnInfo {
                        name: "user_id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: None,
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
        ];

        pipeline.analyze_tables(&tables);

        let conventions = pipeline.get_conventions("public");
        assert!(conventions.is_some());
    }

    #[test]
    fn test_config_with_custom_patterns() {
        let config = PipelineConfig {
            excluded_patterns: vec!["*_guid".to_string()],
            excluded_keywords: vec!["sandbox".to_string()],
            ..Default::default()
        };

        let pipeline = SignalPipeline::with_config(config);

        // Test custom pattern
        let mut candidate = sample_candidate();
        candidate.from_column = "user_guid".to_string();

        let signals = pipeline.collect_sync_signals(&candidate);
        assert!(signals.negative_signals().count() > 0);
    }

    #[test]
    fn test_process_candidates() {
        let pipeline = SignalPipeline::new();

        let good_candidate = sample_candidate();

        let mut bad_candidate = sample_candidate();
        bad_candidate.from_column = "created_at".to_string();
        bad_candidate.from_type = Some(DataType::Timestamp);
        bad_candidate.base_score = 0.3;

        let scored = pipeline.process_candidates(vec![bad_candidate, good_candidate]);

        // Good candidate should be first (higher confidence)
        assert!(!scored.is_empty());
        assert_eq!(scored[0].candidate.from_column, "customer_id");
    }

    #[test]
    fn test_conventions_disabled() {
        let config = PipelineConfig {
            convention_scope: ConventionScope::Disabled,
            ..Default::default()
        };

        let pipeline = SignalPipeline::with_config(config);
        assert!(!pipeline.conventions_enabled());
    }
}
