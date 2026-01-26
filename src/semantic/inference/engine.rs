//! Relationship inference engine.
//!
//! The engine coordinates the inference process using a signal-based pipeline
//! that collects and aggregates multiple signals to score relationship candidates.

use std::collections::{HashMap, HashSet};

use crate::model::DataType;

use super::{
    signals::{
        aggregator::SignalWeights,
        conventions::ConventionScope,
        pipeline::{PipelineConfig, ScoredCandidate, SignalPipeline},
    },
    thresholds, Cardinality, InferredRelationship, RelationshipKey,
};

/// Metadata about a table, used for inference.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Schema name
    pub schema: String,
    /// Table name
    pub name: String,
    /// Columns in this table
    pub columns: Vec<ColumnInfo>,
    /// Primary key column names
    pub primary_key: Vec<String>,
}

impl From<&crate::worker::protocol::GetTableResponse> for TableInfo {
    fn from(resp: &crate::worker::protocol::GetTableResponse) -> Self {
        Self {
            schema: resp.table.schema.clone(),
            name: resp.table.name.clone(),
            columns: resp
                .table
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    is_nullable: c.is_nullable,
                    is_unique: None,
                })
                .collect(),
            primary_key: resp
                .table
                .primary_key
                .as_ref()
                .map(|pk| pk.columns.clone())
                .unwrap_or_default(),
        }
    }
}

/// Pre-computed lookup structures for table metadata.
///
/// Built in a single pass over tables for efficient inference.
#[derive(Debug, Default)]
struct TableLookup {
    /// Set of table names (lowercase)
    names: HashSet<String>,
    /// Map of table name -> primary key columns
    pk_columns: HashMap<String, Vec<String>>,
    /// Map of table name -> column info [(name, type, is_unique)]
    columns: HashMap<String, Vec<(String, Option<DataType>, bool)>>,
}

impl TableLookup {
    /// Build lookup structures from tables in a single pass.
    fn from_tables(tables: &[TableInfo]) -> Self {
        let mut lookup = Self::default();

        for table in tables {
            let name_lower = table.name.to_lowercase();

            // Add table name
            lookup.names.insert(name_lower.clone());

            // Add primary key columns
            lookup
                .pk_columns
                .insert(name_lower.clone(), table.primary_key.clone());

            // Add column info
            let cols: Vec<_> = table
                .columns
                .iter()
                .map(|c| {
                    let dt = DataType::parse(&c.data_type);
                    let is_unique = c.is_unique.unwrap_or(table.primary_key.contains(&c.name));
                    (c.name.clone(), dt, is_unique)
                })
                .collect();
            lookup.columns.insert(name_lower, cols);
        }

        lookup
    }
}

/// Metadata about a column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Data type (database-specific string)
    pub data_type: String,
    /// Whether the column is nullable
    pub is_nullable: bool,
    /// Whether this column is unique (distinct values = row count)
    pub is_unique: Option<bool>,
}

/// Preset weighting profiles for different use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WeightPreset {
    /// Balanced precision/recall (default).
    #[default]
    Balanced,
    /// Fewer false positives, may miss some relationships.
    HighPrecision,
    /// More relationships found, may include false positives.
    HighRecall,
}

/// Configuration for the inference engine.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Minimum confidence threshold for returned relationships
    pub min_confidence: f64,
    /// Whether to use cardinality validation (requires database queries)
    pub validate_cardinality: bool,
    /// Maximum number of candidates per source column
    pub max_candidates_per_column: usize,
    /// Weight preset for signal aggregation.
    pub weight_preset: WeightPreset,
    /// Whether to apply negative signal detection (timestamp, boolean columns).
    pub use_negative_signals: bool,
    /// Whether to check type compatibility between columns.
    pub use_type_compatibility: bool,
    /// Whether to check for self-referential relationships (parent_id patterns).
    pub detect_self_references: bool,
    /// Custom excluded column patterns (in addition to defaults).
    pub excluded_patterns: Vec<String>,
    /// Custom excluded keywords (e.g., "temp", "test").
    pub excluded_keywords: Vec<String>,
    /// Scope for convention detection (schema-level vs global).
    pub convention_scope: ConventionScope,
    /// Include signal breakdown in results (for explain mode).
    pub include_breakdown: bool,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            min_confidence: thresholds::confidence::BALANCED_MIN,
            validate_cardinality: true,
            max_candidates_per_column: 3,
            weight_preset: WeightPreset::default(),
            use_negative_signals: true,
            use_type_compatibility: true,
            detect_self_references: true,
            excluded_patterns: Vec::new(),
            excluded_keywords: Vec::new(),
            convention_scope: ConventionScope::default(),
            include_breakdown: false,
        }
    }
}

impl InferenceConfig {
    /// Create a high-precision configuration (fewer false positives).
    pub fn high_precision() -> Self {
        Self {
            min_confidence: thresholds::confidence::HIGH_PRECISION_MIN,
            weight_preset: WeightPreset::HighPrecision,
            max_candidates_per_column: 2,
            ..Default::default()
        }
    }

    /// Create a high-recall configuration (find more relationships).
    pub fn high_recall() -> Self {
        Self {
            min_confidence: thresholds::confidence::HIGH_RECALL_MIN,
            weight_preset: WeightPreset::HighRecall,
            max_candidates_per_column: 5,
            use_negative_signals: false,
            ..Default::default()
        }
    }

    /// Builder: set minimum confidence.
    pub fn with_min_confidence(mut self, threshold: f64) -> Self {
        self.min_confidence = threshold.clamp(0.0, 1.0);
        self
    }

    /// Builder: set weight preset.
    pub fn with_weight_preset(mut self, preset: WeightPreset) -> Self {
        self.weight_preset = preset;
        self
    }

    /// Builder: enable/disable negative signals.
    pub fn with_negative_signals(mut self, enabled: bool) -> Self {
        self.use_negative_signals = enabled;
        self
    }

    /// Builder: enable/disable type compatibility checking.
    pub fn with_type_compatibility(mut self, enabled: bool) -> Self {
        self.use_type_compatibility = enabled;
        self
    }

    /// Builder: add excluded patterns.
    pub fn with_excluded_patterns(mut self, patterns: Vec<String>) -> Self {
        self.excluded_patterns = patterns;
        self
    }

    /// Builder: add excluded keywords.
    pub fn with_excluded_keywords(mut self, keywords: Vec<String>) -> Self {
        self.excluded_keywords = keywords;
        self
    }

    /// Builder: set convention scope.
    pub fn with_convention_scope(mut self, scope: ConventionScope) -> Self {
        self.convention_scope = scope;
        self
    }

    /// Builder: enable signal breakdown in results.
    pub fn with_breakdown(mut self, enabled: bool) -> Self {
        self.include_breakdown = enabled;
        self
    }

    /// Convert to pipeline config.
    fn to_pipeline_config(&self) -> PipelineConfig {
        PipelineConfig {
            use_negative_signals: self.use_negative_signals,
            use_type_compatibility: self.use_type_compatibility,
            convention_scope: self.convention_scope,
            excluded_patterns: self.excluded_patterns.clone(),
            excluded_keywords: self.excluded_keywords.clone(),
        }
    }

    /// Get signal weights for this preset.
    fn signal_weights(&self) -> SignalWeights {
        match self.weight_preset {
            WeightPreset::Balanced => SignalWeights::new(),
            WeightPreset::HighPrecision => SignalWeights::high_precision(),
            WeightPreset::HighRecall => SignalWeights::high_recall(),
        }
    }
}

/// The relationship inference engine.
#[derive(Debug)]
pub struct InferenceEngine {
    pipeline: SignalPipeline,
    config: InferenceConfig,
}

impl Default for InferenceEngine {
    fn default() -> Self {
        Self::with_config(InferenceConfig::default())
    }
}

impl InferenceEngine {
    /// Create an engine with custom configuration.
    pub fn with_config(config: InferenceConfig) -> Self {
        let mut weights = config.signal_weights();
        weights.min_confidence = config.min_confidence;

        // Create pipeline with both config and weights
        let pipeline_config = config.to_pipeline_config();
        let pipeline = SignalPipeline::with_config_and_weights(pipeline_config, weights);

        Self { pipeline, config }
    }

    /// Prepare the engine for inference by analyzing schema conventions.
    ///
    /// Call this once before inference to detect naming patterns.
    pub fn prepare(&mut self, tables: &[TableInfo]) {
        self.pipeline.analyze_tables(tables);
    }

    /// Load database constraints for high-confidence FK detection.
    ///
    /// Call this with TableMetadata from introspection to enable
    /// constraint-based signals. Relationships matching DB FK constraints
    /// will get 0.98 confidence.
    pub fn load_constraints(&mut self, tables: &[crate::metadata::TableMetadata]) {
        self.pipeline.load_constraints(tables);
    }

    /// Check if a relationship matches a database constraint.
    pub fn has_db_constraint(
        &self,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> bool {
        use super::signals::pipeline::RelationshipCandidate;

        let candidate = RelationshipCandidate {
            from_schema: from_schema.to_string(),
            from_table: from_table.to_string(),
            from_column: from_column.to_string(),
            from_type: None,
            to_schema: to_schema.to_string(),
            to_table: to_table.to_string(),
            to_column: to_column.to_string(),
            to_type: None,
            to_is_pk: false,
            to_is_unique: false,
            matched_rule: String::new(),
            base_score: 0.0,
        };

        self.pipeline.has_db_constraint(&candidate)
    }

    /// Infer relationships for a single table.
    ///
    /// # Arguments
    /// * `table` - The table to find relationships from
    /// * `all_tables` - All available tables (for matching)
    ///
    /// # Returns
    /// List of inferred relationships, sorted by confidence (descending)
    #[must_use]
    pub fn infer_relationships(
        &self,
        table: &TableInfo,
        all_tables: &[TableInfo],
    ) -> Vec<InferredRelationship> {
        // Build lookup structures in a single pass
        let lookup = TableLookup::from_tables(all_tables);

        let mut all_candidates: Vec<ScoredCandidate> = Vec::new();

        // Check each column for potential FK relationships
        for column in &table.columns {
            // Skip if this column is the ONLY primary key column (single-column PK)
            // But allow columns that are part of a COMPOSITE primary key -
            // these are likely dimension keys in fact tables
            if table.primary_key.len() == 1 && table.primary_key.contains(&column.name) {
                continue;
            }

            // Parse column type
            let col_type = DataType::parse(&column.data_type);

            // Find candidates using the pipeline
            let candidates = self.pipeline.find_candidates(
                &column.name,
                col_type.as_ref(),
                &table.schema,
                &table.name,
                &lookup.names,
                &lookup.pk_columns,
                &lookup.columns,
            );

            // Skip self-references to the same column
            let filtered: Vec<_> = candidates
                .into_iter()
                .filter(|c| {
                    !(c.to_table.eq_ignore_ascii_case(&table.name)
                        && c.to_column.eq_ignore_ascii_case(&column.name))
                })
                .collect();

            // Process through pipeline
            let scored = self.pipeline.process_candidates(filtered);

            // Take top N per column
            let top_n: Vec<_> = scored
                .into_iter()
                .take(self.config.max_candidates_per_column)
                .collect();

            all_candidates.extend(top_n);
        }

        // Convert to InferredRelationship
        let mut relationships: Vec<InferredRelationship> = all_candidates
            .into_iter()
            .filter(|sc| sc.score.confidence >= self.config.min_confidence)
            .map(|sc| self.scored_to_relationship(sc))
            .collect();

        // Sort by confidence descending
        relationships.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Deduplicate (same from/to pair, keep highest confidence)
        let mut seen: HashSet<RelationshipKey> = HashSet::new();
        relationships.retain(|r| {
            let key = RelationshipKey::from_relationship(r);
            if seen.contains(&key) {
                false
            } else {
                seen.insert(key);
                true
            }
        });

        relationships
    }

    /// Infer all relationships across all tables.
    #[must_use]
    pub fn infer_all_relationships(&self, tables: &[TableInfo]) -> Vec<InferredRelationship> {
        let mut all_relationships = Vec::new();

        for table in tables {
            let rels = self.infer_relationships(table, tables);
            all_relationships.extend(rels);
        }

        // Build set of already-found relationships for deduplication
        let mut seen: HashSet<RelationshipKey> = HashSet::new();
        for r in &all_relationships {
            seen.insert(RelationshipKey::from_relationship(r));
        }

        // Add constraint-based relationships that weren't found by heuristics
        // This ensures 100% FK coverage from introspection
        for candidate in self.pipeline.get_constraint_relationships() {
            let key = RelationshipKey::new(
                &candidate.from_table,
                &candidate.from_column,
                &candidate.to_table,
                &candidate.to_column,
            );

            if !seen.contains(&key) {
                // This FK wasn't discovered by naming heuristics - add it directly
                let relationship = InferredRelationship {
                    from_schema: candidate.from_schema,
                    from_table: candidate.from_table,
                    from_column: candidate.from_column,
                    to_schema: candidate.to_schema,
                    to_table: candidate.to_table,
                    to_column: candidate.to_column,
                    confidence: candidate.base_score, // 0.98 from constraint
                    rule: candidate.matched_rule,
                    cardinality: Cardinality::ManyToOne, // FK usually many-to-one
                    signal_breakdown: None,
                    source: super::RelationshipSource::DatabaseConstraint,
                };
                all_relationships.push(relationship);
                seen.insert(key);
            }
        }

        // Sort by confidence descending
        all_relationships.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Deduplicate bidirectional relationships (keep only one direction, prefer higher confidence)
        let mut final_seen: HashSet<RelationshipKey> = HashSet::new();
        all_relationships.retain(|r| {
            let key = RelationshipKey::from_relationship(r);
            let reverse_key = key.reversed();

            if final_seen.contains(&key) || final_seen.contains(&reverse_key) {
                false
            } else {
                final_seen.insert(key);
                true
            }
        });

        all_relationships
    }

    /// Convert a scored candidate to an InferredRelationship.
    fn scored_to_relationship(&self, scored: ScoredCandidate) -> InferredRelationship {
        let candidate = scored.candidate;

        // Infer cardinality from uniqueness
        let from_is_unique = false; // FK side is usually not unique
        let to_is_unique = candidate.to_is_pk || candidate.to_is_unique;
        let cardinality = Cardinality::from_uniqueness(from_is_unique, to_is_unique);

        // Include breakdown if configured
        let signal_breakdown = if self.config.include_breakdown {
            Some(scored.score.breakdown)
        } else {
            None
        };

        // Determine source based on whether we have a DB constraint signal
        let source = if scored.signals.has_definitive_signal() {
            super::RelationshipSource::DatabaseConstraint
        } else {
            super::RelationshipSource::Inferred
        };

        InferredRelationship {
            from_schema: candidate.from_schema,
            from_table: candidate.from_table,
            from_column: candidate.from_column,
            to_schema: candidate.to_schema,
            to_table: candidate.to_table,
            to_column: candidate.to_column,
            confidence: scored.score.confidence,
            rule: candidate.matched_rule,
            cardinality,
            signal_breakdown,
            source,
        }
    }

    /// Update relationship confidence based on cardinality statistics.
    ///
    /// Call this after getting column stats from the worker to refine confidence.
    pub fn update_with_cardinality(
        &self,
        relationship: &mut InferredRelationship,
        overlap_percentage: f64,
        from_is_unique: bool,
        to_is_unique: bool,
    ) {
        // Adjust confidence based on overlap
        let overlap_boost = if overlap_percentage >= thresholds::overlap::EXCELLENT * 100.0 {
            thresholds::adjustment::MAJOR_BOOST
        } else if overlap_percentage >= thresholds::overlap::GOOD * 100.0 {
            thresholds::adjustment::MEDIUM_BOOST
        } else if overlap_percentage >= thresholds::overlap::ACCEPTABLE * 100.0 {
            thresholds::adjustment::MINOR_BOOST
        } else if overlap_percentage < thresholds::overlap::VERY_LOW * 100.0 {
            thresholds::adjustment::VERY_LOW_OVERLAP_PENALTY
        } else {
            0.0
        };

        relationship.confidence = (relationship.confidence + overlap_boost)
            .clamp(0.0, thresholds::confidence::DB_CONSTRAINT);

        // Update cardinality using the shared method
        // Note: ManyToMany is a special case when both sides are not unique and we have stats
        relationship.cardinality = match (from_is_unique, to_is_unique) {
            (false, false) => Cardinality::ManyToMany, // Override Unknown with stats
            _ => Cardinality::from_uniqueness(from_is_unique, to_is_unique),
        };
    }

    /// Update relationship confidence using full column statistics.
    ///
    /// This is the preferred method for async inference where ColumnStats and
    /// ValueOverlap are fetched from the database via the worker.
    ///
    /// # Arguments
    /// * `relationship` - The relationship to update
    /// * `from_stats` - Statistics for the source (FK) column
    /// * `to_stats` - Statistics for the target (PK) column
    /// * `overlap` - Value overlap between the columns
    ///
    /// # Returns
    /// `true` if the relationship is valid after statistics check, `false` if
    /// statistics indicate it's not a valid FK relationship.
    pub fn update_with_statistics(
        &self,
        relationship: &mut InferredRelationship,
        from_stats: &crate::metadata::ColumnStats,
        to_stats: &crate::metadata::ColumnStats,
        overlap: &crate::metadata::ValueOverlap,
    ) -> bool {
        use super::signals::statistics::StatisticsSignals;

        // Generate statistics signals
        let stats_signals = StatisticsSignals::from_stats(from_stats, to_stats, overlap);

        // Check if this appears to be a valid FK relationship
        if !stats_signals.is_valid_fk
            && overlap.overlap_percentage < thresholds::overlap::LOW * 100.0
        {
            // Very low overlap and not a valid superset - reject
            relationship.confidence = (relationship.confidence - 0.3).max(0.0);
            return false;
        }

        // Calculate confidence adjustment from statistics signals
        let mut confidence_delta = 0.0;

        // Uniqueness bonus
        confidence_delta += stats_signals.uniqueness_score * thresholds::adjustment::MAJOR_BOOST;

        // Overlap bonus/penalty
        if stats_signals.overlap_score >= thresholds::overlap::EXCELLENT {
            confidence_delta += thresholds::adjustment::MAJOR_BOOST;
        } else if stats_signals.overlap_score >= thresholds::overlap::GOOD {
            confidence_delta += thresholds::adjustment::MEDIUM_BOOST;
        } else if stats_signals.overlap_score >= thresholds::overlap::ACCEPTABLE {
            confidence_delta += thresholds::adjustment::MINOR_BOOST;
        } else if stats_signals.overlap_score < thresholds::overlap::LOW {
            confidence_delta += thresholds::adjustment::LOW_OVERLAP_PENALTY;
        }

        // Null rate penalty
        confidence_delta -= stats_signals.null_rate_penalty;

        // Update confidence
        relationship.confidence = (relationship.confidence + confidence_delta)
            .clamp(0.0, thresholds::confidence::DB_CONSTRAINT);

        // Update cardinality from statistics if known
        if let Some(cardinality) = stats_signals.cardinality_hint {
            if cardinality.is_known() {
                relationship.cardinality = cardinality;
            }
        }

        // Add statistics signals to breakdown if enabled
        if self.config.include_breakdown {
            let stat_signals = stats_signals.to_signals();
            let additional_breakdown: Vec<super::signals::ScoreBreakdown> = stat_signals
                .iter()
                .map(|s| super::signals::ScoreBreakdown {
                    source: s.source.kind().to_string(),
                    raw_score: s.score,
                    weight: s.weight,
                    contribution: s.contribution(),
                    explanation: s.explanation.clone(),
                })
                .collect();

            if let Some(ref mut breakdown) = relationship.signal_breakdown {
                breakdown.extend(additional_breakdown);
            } else {
                relationship.signal_breakdown = Some(additional_breakdown);
            }
        }

        true
    }

    /// Validate relationships using statistics and filter invalid ones.
    ///
    /// This is a convenience method for batch processing. For each relationship,
    /// if statistics indicate it's not a valid FK, it's removed from the list.
    pub fn validate_with_statistics<F>(
        &self,
        relationships: &mut Vec<InferredRelationship>,
        get_stats: F,
    ) where
        F: Fn(
            &str,
            &str,
            &str,
            &str,
            &str,
            &str,
        ) -> Option<(
            crate::metadata::ColumnStats,
            crate::metadata::ColumnStats,
            crate::metadata::ValueOverlap,
        )>,
    {
        relationships.retain_mut(|rel| {
            if let Some((from_stats, to_stats, overlap)) = get_stats(
                &rel.from_schema,
                &rel.from_table,
                &rel.from_column,
                &rel.to_schema,
                &rel.to_table,
                &rel.to_column,
            ) {
                self.update_with_statistics(rel, &from_stats, &to_stats, &overlap)
            } else {
                // No stats available, keep the relationship
                true
            }
        });
    }

    /// Get detected schema conventions (for hints/debugging).
    pub fn get_conventions(
        &self,
        schema: &str,
    ) -> Option<&super::signals::conventions::SchemaConventions> {
        self.pipeline.get_conventions(schema)
    }

    /// Get the current configuration.
    pub fn config(&self) -> &InferenceConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_tables() -> Vec<TableInfo> {
        vec![
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
                        name: "customer_id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                    ColumnInfo {
                        name: "product_id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                    ColumnInfo {
                        name: "status".to_string(),
                        data_type: "varchar".to_string(),
                        is_nullable: true,
                        is_unique: Some(false),
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
            TableInfo {
                schema: "public".to_string(),
                name: "customers".to_string(),
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
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
            TableInfo {
                schema: "public".to_string(),
                name: "products".to_string(),
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
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
        ]
    }

    #[test]
    fn test_infer_relationships_suffix_id() {
        let mut engine = InferenceEngine::default();
        let tables = make_test_tables();

        // Prepare conventions
        engine.prepare(&tables);

        let orders = &tables[0];
        let relationships = engine.infer_relationships(orders, &tables);

        // Should find customer_id -> customers.id and product_id -> products.id
        assert!(
            relationships.len() >= 2,
            "Found {} relationships",
            relationships.len()
        );

        let customer_rel = relationships
            .iter()
            .find(|r| r.to_table == "customers")
            .expect("Should find customer relationship");

        assert_eq!(customer_rel.from_column, "customer_id");
        assert_eq!(customer_rel.to_column, "id");
        assert!(customer_rel.confidence >= 0.50);
        assert_eq!(customer_rel.cardinality, Cardinality::ManyToOne);
    }

    #[test]
    fn test_infer_all_relationships() {
        let mut engine = InferenceEngine::default();
        let tables = make_test_tables();

        engine.prepare(&tables);
        let relationships = engine.infer_all_relationships(&tables);

        // Should find relationships from orders to customers and products
        assert!(relationships.len() >= 2);

        // Should not have duplicates
        let mut pairs: HashSet<_> = HashSet::new();
        for r in &relationships {
            let key = (
                r.from_table.clone(),
                r.from_column.clone(),
                r.to_table.clone(),
                r.to_column.clone(),
            );
            assert!(!pairs.contains(&key), "Duplicate relationship found");
            pairs.insert(key);
        }
    }

    #[test]
    fn test_min_confidence_filter() {
        let config = InferenceConfig {
            min_confidence: 0.90,
            ..Default::default()
        };
        let mut engine = InferenceEngine::with_config(config);
        let tables = make_test_tables();

        engine.prepare(&tables);

        let orders = &tables[0];
        let relationships = engine.infer_relationships(orders, &tables);

        // With high threshold, all results should meet it
        for r in &relationships {
            assert!(r.confidence >= 0.90);
        }
    }

    #[test]
    fn test_update_with_cardinality() {
        let engine = InferenceEngine::default();

        let mut rel = InferredRelationship {
            from_schema: "public".to_string(),
            from_table: "orders".to_string(),
            from_column: "customer_id".to_string(),
            to_schema: "public".to_string(),
            to_table: "customers".to_string(),
            to_column: "id".to_string(),
            confidence: 0.70,
            rule: "suffix_id".to_string(),
            cardinality: Cardinality::Unknown,
            signal_breakdown: None,
            source: crate::semantic::inference::RelationshipSource::Inferred,
        };

        engine.update_with_cardinality(
            &mut rel, 98.0,  // 98% overlap
            false, // from is not unique (FK side)
            true,  // to is unique (PK side)
        );

        // Confidence should increase with good overlap
        assert!(rel.confidence > 0.70);
        assert_eq!(rel.cardinality, Cardinality::ManyToOne);
    }

    #[test]
    fn test_include_breakdown() {
        let config = InferenceConfig::default().with_breakdown(true);
        let mut engine = InferenceEngine::with_config(config);
        let tables = make_test_tables();

        engine.prepare(&tables);

        let orders = &tables[0];
        let relationships = engine.infer_relationships(orders, &tables);

        // With breakdown enabled, should have signal details
        if let Some(rel) = relationships.first() {
            assert!(
                rel.signal_breakdown.is_some(),
                "Should include signal breakdown"
            );
        }
    }

    #[test]
    fn test_negative_signals_filter() {
        let mut engine = InferenceEngine::default();

        // Create tables with timestamp columns
        let tables = vec![
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
                        name: "created_at".to_string(),
                        data_type: "timestamp".to_string(),
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
            TableInfo {
                schema: "public".to_string(),
                name: "created".to_string(), // Table named "created"
                columns: vec![ColumnInfo {
                    name: "at".to_string(),
                    data_type: "integer".to_string(),
                    is_nullable: false,
                    is_unique: Some(true),
                }],
                primary_key: vec!["at".to_string()],
            },
        ];

        engine.prepare(&tables);

        let orders = &tables[0];
        let relationships = engine.infer_relationships(orders, &tables);

        // created_at should NOT match to created.at due to negative signals
        let bad_rel = relationships
            .iter()
            .find(|r| r.from_column == "created_at" && r.to_table == "created");

        assert!(
            bad_rel.is_none(),
            "Should not infer relationship for timestamp column"
        );
    }

    #[test]
    fn test_relationship_source_from_db_constraint() {
        use crate::metadata::{
            ColumnInfo as MetaColumnInfo, ForeignKeyInfo, PrimaryKeyInfo, TableMetadata, TableType,
        };

        // Create table metadata with a FK constraint
        let table_metadata = vec![
            TableMetadata {
                schema: "public".to_string(),
                name: "customers".to_string(),
                table_type: TableType::Table,
                columns: vec![MetaColumnInfo {
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
                    MetaColumnInfo {
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
                    MetaColumnInfo {
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
                    on_delete: None,
                    on_update: None,
                }],
                unique_constraints: vec![],
            },
        ];

        // Create TableInfo for inference
        let tables = vec![
            TableInfo {
                schema: "public".to_string(),
                name: "customers".to_string(),
                columns: vec![ColumnInfo {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    is_nullable: false,
                    is_unique: Some(true),
                }],
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
                        name: "customer_id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
        ];

        // Create engine and load constraints
        let mut engine = InferenceEngine::default();
        engine.prepare(&tables);
        engine.load_constraints(&table_metadata);

        // Infer relationships
        let relationships = engine.infer_all_relationships(&tables);

        // Find the customer_id -> customers.id relationship
        let customer_rel = relationships
            .iter()
            .find(|r| r.from_column == "customer_id" && r.to_table == "customers")
            .expect("Should find customer relationship");

        // Should be marked as DatabaseConstraint since we loaded the FK
        assert_eq!(
            customer_rel.source,
            crate::semantic::inference::RelationshipSource::DatabaseConstraint,
            "Relationship matching a DB constraint should have source DatabaseConstraint"
        );

        // Confidence should be reasonably high (aggregation may dilute constraint signal)
        assert!(
            customer_rel.confidence >= 0.70,
            "Constraint-backed relationship should have good confidence, got {}",
            customer_rel.confidence
        );
    }

    #[test]
    fn test_relationship_source_inferred_without_constraint() {
        let mut engine = InferenceEngine::default();
        let tables = make_test_tables();

        engine.prepare(&tables);
        // Note: NOT loading constraints

        let relationships = engine.infer_all_relationships(&tables);

        // All relationships should be Inferred since no constraints loaded
        for rel in &relationships {
            assert_eq!(
                rel.source,
                crate::semantic::inference::RelationshipSource::Inferred,
                "Relationship without DB constraint should have source Inferred"
            );
        }
    }

    #[test]
    fn test_excluded_patterns_config() {
        // Create config with excluded keyword "customer"
        let config =
            InferenceConfig::default().with_excluded_keywords(vec!["customer".to_string()]);

        let mut engine = InferenceEngine::with_config(config);
        let tables = make_test_tables();

        engine.prepare(&tables);
        let relationships = engine.infer_all_relationships(&tables);

        // customer_id should be filtered out due to excluded keyword
        let customer_rel = relationships
            .iter()
            .find(|r| r.from_column == "customer_id");

        // With "customer" as excluded keyword, this relationship should have lower confidence
        // or be filtered out entirely
        if let Some(rel) = customer_rel {
            // If found, confidence should be reduced by negative signal
            assert!(
                rel.confidence < 0.80,
                "Excluded keyword should reduce confidence, got {}",
                rel.confidence
            );
        }
        // If not found at all, that's also acceptable (filtered by min_confidence)
    }

    #[test]
    fn test_constraint_discovery_nonstandard_naming() {
        use crate::metadata::{
            ColumnInfo as MetaColumnInfo, ForeignKeyInfo, PrimaryKeyInfo, TableMetadata, TableType,
        };

        // Create table metadata with FK using non-standard naming
        // The column "buyer" doesn't follow "table_id" convention
        let table_metadata = vec![
            TableMetadata {
                schema: "public".to_string(),
                name: "users".to_string(),
                table_type: TableType::Table,
                columns: vec![MetaColumnInfo {
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
                    name: "pk_users".to_string(),
                    columns: vec!["id".to_string()],
                }),
                foreign_keys: vec![],
                unique_constraints: vec![],
            },
            TableMetadata {
                schema: "public".to_string(),
                name: "purchases".to_string(),
                table_type: TableType::Table,
                columns: vec![
                    MetaColumnInfo {
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
                    // Non-standard FK name - wouldn't be found by heuristics
                    MetaColumnInfo {
                        name: "buyer".to_string(),
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
                    name: "pk_purchases".to_string(),
                    columns: vec!["id".to_string()],
                }),
                foreign_keys: vec![ForeignKeyInfo {
                    name: "fk_purchases_buyer".to_string(),
                    columns: vec!["buyer".to_string()],
                    referenced_schema: "public".to_string(),
                    referenced_table: "users".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: None,
                    on_update: None,
                }],
                unique_constraints: vec![],
            },
        ];

        // Create TableInfo for inference
        let tables = vec![
            TableInfo {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: vec![ColumnInfo {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    is_nullable: false,
                    is_unique: Some(true),
                }],
                primary_key: vec!["id".to_string()],
            },
            TableInfo {
                schema: "public".to_string(),
                name: "purchases".to_string(),
                columns: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(true),
                    },
                    ColumnInfo {
                        name: "buyer".to_string(), // Non-standard name
                        data_type: "integer".to_string(),
                        is_nullable: false,
                        is_unique: Some(false),
                    },
                ],
                primary_key: vec!["id".to_string()],
            },
        ];

        // Create engine and load constraints
        let mut engine = InferenceEngine::default();
        engine.prepare(&tables);
        engine.load_constraints(&table_metadata);

        // Infer relationships
        let relationships = engine.infer_all_relationships(&tables);

        // Should find the buyer -> users.id relationship via constraint
        // even though "buyer" doesn't match "user_id" naming convention
        let buyer_rel = relationships
            .iter()
            .find(|r| r.from_column == "buyer" && r.to_table == "users")
            .expect("Should find buyer relationship from constraint");

        assert_eq!(
            buyer_rel.source,
            crate::semantic::inference::RelationshipSource::DatabaseConstraint,
            "Non-standard FK should be discovered via constraint"
        );

        assert!(
            buyer_rel.confidence >= 0.95,
            "Constraint relationship should have high confidence, got {}",
            buyer_rel.confidence
        );
    }

    #[test]
    fn test_update_with_statistics() {
        use crate::metadata::{ColumnStats, ValueOverlap};

        let engine = InferenceEngine::default();

        let mut rel = InferredRelationship {
            from_schema: "public".to_string(),
            from_table: "orders".to_string(),
            from_column: "customer_id".to_string(),
            to_schema: "public".to_string(),
            to_table: "customers".to_string(),
            to_column: "id".to_string(),
            confidence: 0.65,
            rule: "suffix_id".to_string(),
            cardinality: Cardinality::Unknown,
            signal_breakdown: None,
            source: crate::semantic::inference::RelationshipSource::Inferred,
        };

        // Good statistics: unique target, high overlap
        let from_stats = ColumnStats {
            total_count: 1000,
            distinct_count: 500,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        };
        let to_stats = ColumnStats {
            total_count: 500,
            distinct_count: 500,
            null_count: 0,
            is_unique: true,
            sample_values: vec![],
        };
        let overlap = ValueOverlap {
            left_sample_size: 100,
            left_total_distinct: 500,
            right_total_distinct: 500,
            overlap_count: 98,
            overlap_percentage: 98.0,
            right_is_superset: true,
            left_is_unique: false,
            right_is_unique: true,
        };

        let is_valid = engine.update_with_statistics(&mut rel, &from_stats, &to_stats, &overlap);

        assert!(is_valid, "Good FK relationship should be valid");
        assert!(
            rel.confidence > 0.65,
            "Confidence should increase with good stats, got {}",
            rel.confidence
        );
        assert_eq!(
            rel.cardinality,
            Cardinality::ManyToOne,
            "Cardinality should be inferred from stats"
        );
    }

    #[test]
    fn test_update_with_statistics_low_overlap() {
        use crate::metadata::{ColumnStats, ValueOverlap};

        let engine = InferenceEngine::default();

        let mut rel = InferredRelationship {
            from_schema: "public".to_string(),
            from_table: "orders".to_string(),
            from_column: "customer_id".to_string(),
            to_schema: "public".to_string(),
            to_table: "customers".to_string(),
            to_column: "id".to_string(),
            confidence: 0.65,
            rule: "suffix_id".to_string(),
            cardinality: Cardinality::Unknown,
            signal_breakdown: None,
            source: crate::semantic::inference::RelationshipSource::Inferred,
        };

        // Bad statistics: very low overlap
        let from_stats = ColumnStats {
            total_count: 1000,
            distinct_count: 500,
            null_count: 0,
            is_unique: false,
            sample_values: vec![],
        };
        let to_stats = ColumnStats {
            total_count: 500,
            distinct_count: 500,
            null_count: 0,
            is_unique: true,
            sample_values: vec![],
        };
        let overlap = ValueOverlap {
            left_sample_size: 100,
            left_total_distinct: 500,
            right_total_distinct: 500,
            overlap_count: 5,
            overlap_percentage: 5.0,
            right_is_superset: false,
            left_is_unique: false,
            right_is_unique: true,
        };

        let is_valid = engine.update_with_statistics(&mut rel, &from_stats, &to_stats, &overlap);

        assert!(!is_valid, "Low overlap FK should be invalid");
        assert!(
            rel.confidence < 0.65,
            "Confidence should decrease with bad stats, got {}",
            rel.confidence
        );
    }
}
