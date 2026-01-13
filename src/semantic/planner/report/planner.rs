//! Report Planner - Multi-fact query generation.
//!
//! Reports combine measures from multiple (potentially unrelated) facts.
//! The planner:
//! 1. Groups measures by their source fact
//! 2. Routes filters to applicable facts via `ModelGraph::validate_safe_path()`
//! 3. Generates a CTE for each fact with its measures and applicable filters
//! 4. Joins all CTEs with FULL OUTER JOIN on the group_by columns

use std::collections::{HashMap, HashSet};

use crate::model::{AggregationType, FactDefinition, MeasureRef, Model, Report};
use crate::semantic::error::{PlanError, PlanResult};
use crate::semantic::model_graph::ModelGraph;

/// Convert aggregation type to SQL keyword.
pub(crate) fn aggregation_to_sql(agg: &AggregationType) -> String {
    match agg {
        AggregationType::Sum => "SUM".to_string(),
        AggregationType::Count => "COUNT".to_string(),
        AggregationType::CountDistinct => "COUNT DISTINCT".to_string(),
        AggregationType::Avg => "AVG".to_string(),
        AggregationType::Min => "MIN".to_string(),
        AggregationType::Max => "MAX".to_string(),
    }
}

/// Result of planning a report.
#[derive(Debug, Clone)]
pub struct ReportPlan {
    /// The report being planned.
    pub report_name: String,

    /// CTEs for each fact, keyed by fact name.
    pub fact_ctes: Vec<FactCte>,

    /// The group_by columns (used for joining CTEs).
    pub group_by: Vec<String>,

    /// Column aliases for the final SELECT.
    pub output_columns: Vec<OutputColumn>,
}

/// A CTE for a single fact's contribution to the report.
#[derive(Debug, Clone)]
pub struct FactCte {
    /// CTE name (e.g., "orders_metrics").
    pub cte_name: String,

    /// The source fact name.
    pub fact_name: String,

    /// The fact's physical table.
    pub fact_table: String,

    /// Measures to aggregate in this CTE.
    pub measures: Vec<PlannedMeasure>,

    /// Filters that apply to this fact.
    pub applicable_filters: Vec<String>,

    /// Entities that need to be joined for filters/group_by.
    pub required_joins: Vec<String>,
}

/// A measure with its aggregation info.
#[derive(Debug, Clone)]
pub struct PlannedMeasure {
    /// Output column name.
    pub alias: String,

    /// The measure name in the fact.
    pub measure_name: String,

    /// Aggregation type (e.g., "SUM", "COUNT").
    pub aggregation: String,

    /// Source column expression.
    pub source_expr: String,
}

/// An output column in the final SELECT.
#[derive(Debug, Clone)]
pub struct OutputColumn {
    /// Column alias in output.
    pub alias: String,

    /// Source CTE name.
    pub source_cte: String,

    /// Column name in the source CTE.
    pub source_column: String,

    /// Whether this is a group_by column (needs COALESCE).
    pub is_group_by: bool,
}

/// Planner for Report entities.
pub struct ReportPlanner<'a> {
    model: &'a Model,
    graph: &'a ModelGraph,
}

impl<'a> ReportPlanner<'a> {
    /// Create a new report planner.
    pub fn new(model: &'a Model, graph: &'a ModelGraph) -> Self {
        Self { model, graph }
    }

    /// Plan a report into a ReportPlan.
    pub fn plan(&self, report: &Report) -> PlanResult<ReportPlan> {
        // Step 1: Group measures by fact
        let measures_by_fact = self.group_measures_by_fact(report)?;

        // Step 2: Extract entities referenced in filters
        let filter_entities = self.extract_filter_entities(&report.filters);

        // Step 3: Extract entities referenced in group_by
        let group_by_entities = self.extract_group_by_entities(&report.group_by);

        // Step 4: Build CTEs for each fact
        let mut fact_ctes = Vec::new();
        for (fact_name, measures) in &measures_by_fact {
            let cte = self.build_fact_cte(
                fact_name,
                measures,
                &report.filters,
                &filter_entities,
                &report.group_by,
                &group_by_entities,
            )?;
            fact_ctes.push(cte);
        }

        // Step 5: Build output columns
        let output_columns = self.build_output_columns(&fact_ctes, &report.group_by);

        Ok(ReportPlan {
            report_name: report.name.clone(),
            fact_ctes,
            group_by: report.group_by.clone(),
            output_columns,
        })
    }

    /// Group measures by their source fact.
    pub fn group_measures_by_fact<'b>(
        &self,
        report: &'b Report,
    ) -> PlanResult<HashMap<String, Vec<&'b MeasureRef>>> {
        let mut by_fact: HashMap<String, Vec<&'b MeasureRef>> = HashMap::new();

        for measure_ref in &report.measures {
            // Validate the fact exists
            if self.model.get_fact(&measure_ref.fact).is_none() {
                return Err(PlanError::UnknownEntity(format!(
                    "{} (referenced in report '{}' measure '{}')",
                    measure_ref.fact, report.name, measure_ref
                )));
            }

            by_fact
                .entry(measure_ref.fact.clone())
                .or_default()
                .push(measure_ref);
        }

        Ok(by_fact)
    }

    /// Extract entity names from filter expressions.
    ///
    /// Filters are SQL expressions like "customers.region = 'EMEA'".
    /// We extract "customers" to check path connectivity.
    fn extract_filter_entities(&self, filters: &[String]) -> HashMap<String, Vec<String>> {
        let mut entity_filters: HashMap<String, Vec<String>> = HashMap::new();

        for filter in filters {
            // Simple heuristic: look for "entity.column" patterns
            // This is a basic implementation - a real parser would be more robust
            if let Some(entity) = self.extract_entity_from_expr(filter) {
                entity_filters
                    .entry(entity)
                    .or_default()
                    .push(filter.clone());
            }
        }

        entity_filters
    }

    /// Extract entity name from an expression like "customers.region = 'EMEA'".
    fn extract_entity_from_expr(&self, expr: &str) -> Option<String> {
        // Look for word.word pattern at the start
        let trimmed = expr.trim();
        if let Some(dot_pos) = trimmed.find('.') {
            let potential_entity = &trimmed[..dot_pos];
            // Validate it's a word (alphanumeric + underscore)
            if potential_entity
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_')
            {
                // Check if this entity exists in the model
                if self.model.get_source(potential_entity).is_some()
                    || self.model.get_dimension(potential_entity).is_some()
                    || self.model.get_fact(potential_entity).is_some()
                {
                    return Some(potential_entity.to_string());
                }
            }
        }
        None
    }

    /// Extract entity names from group_by columns.
    fn extract_group_by_entities(&self, group_by: &[String]) -> HashSet<String> {
        let mut entities = HashSet::new();

        for col in group_by {
            if let Some(dot_pos) = col.find('.') {
                let entity = &col[..dot_pos];
                entities.insert(entity.to_string());
            }
        }

        entities
    }

    /// Build a CTE for a single fact.
    fn build_fact_cte(
        &self,
        fact_name: &str,
        measures: &[&MeasureRef],
        _all_filters: &[String],
        filter_entities: &HashMap<String, Vec<String>>,
        _group_by: &[String],
        group_by_entities: &HashSet<String>,
    ) -> PlanResult<FactCte> {
        let fact = self.model.get_fact(fact_name).ok_or_else(|| {
            PlanError::UnknownEntity(fact_name.to_string())
        })?;

        // Determine which filters apply to this fact
        let applicable_filters = self.route_filters(fact_name, filter_entities);

        // Determine which entities need to be joined
        let required_joins = self.compute_required_joins(
            fact_name,
            &applicable_filters,
            filter_entities,
            group_by_entities,
        );

        // Build planned measures
        let planned_measures = self.build_planned_measures(fact, measures)?;

        Ok(FactCte {
            cte_name: format!("{}_metrics", fact_name),
            fact_name: fact_name.to_string(),
            fact_table: fact.target_table.clone(),
            measures: planned_measures,
            applicable_filters,
            required_joins,
        })
    }

    /// Route filters to a fact based on safe path connectivity.
    ///
    /// A filter is only routed to a fact if there's a safe (no fan-out) path
    /// from the fact to the filter's entity. This prevents routing filters
    /// through 1:M relationships which would incorrectly restrict results.
    pub fn route_filters(
        &self,
        fact_name: &str,
        filter_entities: &HashMap<String, Vec<String>>,
    ) -> Vec<String> {
        let mut applicable = Vec::new();

        for (entity, filters) in filter_entities {
            // Check if there's a SAFE path from the fact to the filter's entity
            // Safe means no fan-out (all joins are many-to-one)
            if self.graph.validate_safe_path(fact_name, entity).is_ok() {
                applicable.extend(filters.clone());
            }
        }

        applicable
    }

    /// Compute which entities need to be joined for this fact's CTE.
    fn compute_required_joins(
        &self,
        fact_name: &str,
        applicable_filters: &[String],
        filter_entities: &HashMap<String, Vec<String>>,
        group_by_entities: &HashSet<String>,
    ) -> Vec<String> {
        let mut required = HashSet::new();

        // Add entities from applicable filters
        for (entity, filters) in filter_entities {
            if filters.iter().any(|f| applicable_filters.contains(f)) {
                required.insert(entity.clone());
            }
        }

        // Add entities from group_by (if safely reachable)
        for entity in group_by_entities {
            if self.graph.validate_safe_path(fact_name, entity).is_ok() {
                required.insert(entity.clone());
            }
        }

        required.into_iter().collect()
    }

    /// Build PlannedMeasure from fact definitions.
    fn build_planned_measures(
        &self,
        fact: &FactDefinition,
        measures: &[&MeasureRef],
    ) -> PlanResult<Vec<PlannedMeasure>> {
        let mut planned = Vec::new();

        for measure_ref in measures {
            let measure_def = fact.measures.get(&measure_ref.measure).ok_or_else(|| {
                PlanError::UnknownField {
                    entity: fact.name.clone(),
                    field: measure_ref.measure.clone(),
                }
            })?;

            planned.push(PlannedMeasure {
                alias: measure_ref.measure.clone(),
                measure_name: measure_ref.measure.clone(),
                aggregation: aggregation_to_sql(&measure_def.aggregation),
                source_expr: measure_def.source_column.clone(),
            });
        }

        Ok(planned)
    }

    /// Build output columns for the final SELECT.
    fn build_output_columns(&self, fact_ctes: &[FactCte], group_by: &[String]) -> Vec<OutputColumn> {
        let mut columns = Vec::new();

        // Add group_by columns (with COALESCE across CTEs)
        for col in group_by {
            // Extract just the column name for the alias
            let alias = col.split('.').next_back().unwrap_or(col).to_string();

            // Use the first CTE that has this group_by
            if let Some(first_cte) = fact_ctes.first() {
                columns.push(OutputColumn {
                    alias: alias.clone(),
                    source_cte: first_cte.cte_name.clone(),
                    source_column: alias,
                    is_group_by: true,
                });
            }
        }

        // Add measure columns from each CTE
        for cte in fact_ctes {
            for measure in &cte.measures {
                columns.push(OutputColumn {
                    alias: format!("{}_{}", cte.fact_name, measure.alias),
                    source_cte: cte.cte_name.clone(),
                    source_column: measure.alias.clone(),
                    is_group_by: false,
                });
            }
        }

        columns
    }
}
