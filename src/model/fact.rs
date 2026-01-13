//! Fact table definitions - denormalized tables to materialize.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::dimension_role::DateConfig;
use super::expr::{ColumnDef, Expr, OrderByExpr, WindowFrame, WindowFunc};
use super::types::{AggregationType, DataType, MaterializationStrategy};

/// Default value for `materialized` field in serde deserialization.
fn default_materialized() -> bool {
    true
}

/// A fact table definition - a denormalized table to be materialized.
///
/// Facts are the primary output of the transform planner. They define:
/// - The grain (what one row represents)
/// - Which dimension attributes to denormalize
/// - Which measures to include
/// - How to materialize (view, table, incremental)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactDefinition {
    /// Logical name for this fact (e.g., "fact_orders")
    pub name: String,

    /// Physical target table (e.g., "analytics.fact_orders")
    pub target_table: String,

    /// Optional schema override
    pub target_schema: Option<String>,

    /// Whether this fact is materialized as a physical table.
    ///
    /// When `true` (default), queries against this fact read from the target table.
    /// When `false`, the fact is "virtual" - queries are reconstructed from source
    /// entities at query time, joining grain sources and included dimensions.
    #[serde(default = "default_materialized")]
    pub materialized: bool,

    /// Optional source entity or intermediate to build from.
    ///
    /// When set, the fact is built from this entity instead of directly
    /// from the grain entities. Useful for building facts from intermediates.
    pub from: Option<String>,

    /// The grain - columns that define what one row represents
    /// Format: "source_entity.column" (e.g., "orders.order_id")
    pub grain: Vec<GrainColumn>,

    /// Dimension attributes to denormalize into this fact
    pub includes: HashMap<String, DimensionInclude>,

    /// Measures available on this fact
    pub measures: HashMap<String, MeasureDefinition>,

    /// Computed columns (renamed, cast, or calculated).
    ///
    /// These are non-aggregated column transformations:
    /// - Simple pass-through: `ColumnDef::Simple("order_id")`
    /// - Renamed: `ColumnDef::Renamed { source: "cust_id", target: "customer_id" }`
    /// - Computed: `ColumnDef::Computed { name: "is_large_order", expr: ..., data_type: ... }`
    pub columns: Vec<ColumnDef>,

    /// Window function columns (YTD, running totals, rankings, etc.).
    ///
    /// These are evaluated AFTER joins but BEFORE aggregation.
    pub window_columns: Vec<WindowColumnDef>,

    /// How to materialize this fact
    pub materialization: MaterializationStrategy,

    /// Date dimension configuration for role-playing dimensions and time intelligence.
    ///
    /// Configure when a fact has multiple foreign keys to the same date dimension
    /// (e.g., order_date, ship_date, delivery_date).
    ///
    /// # Example
    ///
    /// ```lua
    /// fact "orders" {
    ///     date_config = {
    ///         roles = {
    ///             order_date = "order_date_id",
    ///             ship_date = "ship_date_id",
    ///         },
    ///         primary_date = "order_date",
    ///         grain_columns = {
    ///             year = "year",
    ///             quarter = "quarter",
    ///             month = "month",
    ///         },
    ///     },
    /// }
    /// ```
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date_config: Option<DateConfig>,
}

/// A column that defines the grain of the fact table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainColumn {
    /// Source entity name (e.g., "orders")
    pub source_entity: String,

    /// Column in the source entity (e.g., "order_id")
    pub source_column: String,

    /// Name in the target fact table (defaults to source_column)
    pub target_name: Option<String>,
}

/// Dimension attributes to include (denormalize) in the fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionInclude {
    /// The entity to pull attributes from
    pub entity: String,

    /// Column selection mode
    pub selection: ColumnSelection,

    /// Optional prefix for column names in the fact (e.g., "customer_")
    pub prefix: Option<String>,
}

/// How to select columns from a dimension
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnSelection {
    /// Include specific columns
    Columns(Vec<String>),
    /// Include all columns from the entity
    All,
    /// Include all columns except these
    Except(Vec<String>),
}

/// A measure definition for the fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureDefinition {
    /// Name of the measure (e.g., "revenue")
    pub name: String,

    /// Aggregation type
    pub aggregation: AggregationType,

    /// Source column to aggregate (e.g., "total")
    /// Use "*" for COUNT(*)
    pub source_column: String,

    /// Optional filter expression (e.g., status = 'completed')
    pub filter: Option<Expr>,

    /// Optional description
    pub description: Option<String>,
}

/// A window function column definition.
///
/// Window functions compute values across rows without collapsing them.
/// Common use cases:
/// - Running totals: SUM(amount) OVER (ORDER BY date)
/// - Year-to-date: SUM(amount) OVER (PARTITION BY year ORDER BY date)
/// - Rankings: ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC)
/// - Lead/lag: LAG(value, 1) OVER (ORDER BY date)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowColumnDef {
    /// Name of the resulting column
    pub name: String,

    /// The window function to apply
    pub func: WindowFunc,

    /// Arguments to the window function (e.g., the column for SUM)
    pub args: Vec<Expr>,

    /// PARTITION BY columns
    pub partition_by: Vec<Expr>,

    /// ORDER BY specification
    pub order_by: Vec<OrderByExpr>,

    /// Window frame (ROWS/RANGE BETWEEN ...)
    pub frame: Option<WindowFrame>,

    /// Expected result data type (for type inference)
    pub data_type: Option<DataType>,
}

impl FactDefinition {
    /// Create a new fact definition.
    pub fn new(name: impl Into<String>, target_table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            target_table: target_table.into(),
            target_schema: None,
            materialized: true,
            from: None,
            grain: vec![],
            includes: HashMap::new(),
            measures: HashMap::new(),
            columns: vec![],
            window_columns: vec![],
            materialization: MaterializationStrategy::default(),
            date_config: None,
        }
    }

    /// Set whether this fact is materialized (physical table) or virtual.
    pub fn with_materialized(mut self, materialized: bool) -> Self {
        self.materialized = materialized;
        self
    }

    /// Set the date configuration for role-playing dimensions.
    pub fn with_date_config(mut self, config: DateConfig) -> Self {
        self.date_config = Some(config);
        self
    }

    /// Set the source entity to build this fact from.
    pub fn with_from(mut self, from: impl Into<String>) -> Self {
        self.from = Some(from.into());
        self
    }

    /// Set the target schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.target_schema = Some(schema.into());
        self
    }

    /// Add a grain column.
    pub fn with_grain(mut self, entity: impl Into<String>, column: impl Into<String>) -> Self {
        self.grain.push(GrainColumn {
            source_entity: entity.into(),
            source_column: column.into(),
            target_name: None,
        });
        self
    }

    /// Add a grain column with custom target name.
    pub fn with_grain_as(
        mut self,
        entity: impl Into<String>,
        column: impl Into<String>,
        target_name: impl Into<String>,
    ) -> Self {
        self.grain.push(GrainColumn {
            source_entity: entity.into(),
            source_column: column.into(),
            target_name: Some(target_name.into()),
        });
        self
    }

    /// Include specific dimension attributes.
    pub fn include(
        mut self,
        entity: impl Into<String>,
        columns: Vec<impl Into<String>>,
    ) -> Self {
        let entity_name = entity.into();
        self.includes.insert(
            entity_name.clone(),
            DimensionInclude {
                entity: entity_name,
                selection: ColumnSelection::Columns(columns.into_iter().map(Into::into).collect()),
                prefix: None,
            },
        );
        self
    }

    /// Include all columns from a dimension.
    pub fn include_all(mut self, entity: impl Into<String>) -> Self {
        let entity_name = entity.into();
        self.includes.insert(
            entity_name.clone(),
            DimensionInclude {
                entity: entity_name,
                selection: ColumnSelection::All,
                prefix: None,
            },
        );
        self
    }

    /// Include all columns except specified ones.
    pub fn include_except(
        mut self,
        entity: impl Into<String>,
        except: Vec<impl Into<String>>,
    ) -> Self {
        let entity_name = entity.into();
        self.includes.insert(
            entity_name.clone(),
            DimensionInclude {
                entity: entity_name,
                selection: ColumnSelection::Except(except.into_iter().map(Into::into).collect()),
                prefix: None,
            },
        );
        self
    }

    /// Include dimension attributes with a prefix.
    pub fn include_with_prefix(
        mut self,
        entity: impl Into<String>,
        columns: Vec<impl Into<String>>,
        prefix: impl Into<String>,
    ) -> Self {
        let entity_name = entity.into();
        self.includes.insert(
            entity_name.clone(),
            DimensionInclude {
                entity: entity_name,
                selection: ColumnSelection::Columns(columns.into_iter().map(Into::into).collect()),
                prefix: Some(prefix.into()),
            },
        );
        self
    }

    /// Add a SUM measure.
    pub fn with_sum(mut self, name: impl Into<String>, column: impl Into<String>) -> Self {
        let name = name.into();
        self.measures.insert(
            name.clone(),
            MeasureDefinition {
                name,
                aggregation: AggregationType::Sum,
                source_column: column.into(),
                filter: None,
                description: None,
            },
        );
        self
    }

    /// Add a COUNT measure.
    pub fn with_count(mut self, name: impl Into<String>, column: impl Into<String>) -> Self {
        let name = name.into();
        self.measures.insert(
            name.clone(),
            MeasureDefinition {
                name,
                aggregation: AggregationType::Count,
                source_column: column.into(),
                filter: None,
                description: None,
            },
        );
        self
    }

    /// Add a COUNT(*) measure.
    pub fn with_count_star(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.measures.insert(
            name.clone(),
            MeasureDefinition {
                name,
                aggregation: AggregationType::Count,
                source_column: "*".into(),
                filter: None,
                description: None,
            },
        );
        self
    }

    /// Add an AVG measure.
    pub fn with_avg(mut self, name: impl Into<String>, column: impl Into<String>) -> Self {
        let name = name.into();
        self.measures.insert(
            name.clone(),
            MeasureDefinition {
                name,
                aggregation: AggregationType::Avg,
                source_column: column.into(),
                filter: None,
                description: None,
            },
        );
        self
    }

    /// Add a generic measure.
    pub fn with_measure(mut self, measure: MeasureDefinition) -> Self {
        self.measures.insert(measure.name.clone(), measure);
        self
    }

    /// Set the materialization strategy.
    pub fn with_materialization(mut self, strategy: MaterializationStrategy) -> Self {
        self.materialization = strategy;
        self
    }

    /// Add a column definition.
    pub fn with_column(mut self, column: ColumnDef) -> Self {
        self.columns.push(column);
        self
    }

    /// Add a simple pass-through column.
    pub fn with_simple_column(mut self, name: impl Into<String>) -> Self {
        self.columns.push(ColumnDef::Simple(name.into()));
        self
    }

    /// Add a renamed column.
    pub fn with_renamed_column(
        mut self,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        self.columns.push(ColumnDef::Renamed {
            source: source.into(),
            target: target.into(),
        });
        self
    }

    /// Add a computed column.
    pub fn with_computed_column(
        mut self,
        name: impl Into<String>,
        expr: Expr,
        data_type: Option<DataType>,
    ) -> Self {
        self.columns.push(ColumnDef::Computed {
            name: name.into(),
            expr,
            data_type,
        });
        self
    }

    /// Add a window function column.
    pub fn with_window_column(mut self, window_col: WindowColumnDef) -> Self {
        self.window_columns.push(window_col);
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

impl GrainColumn {
    /// Get the target column name (custom or derived from source).
    pub fn target_column_name(&self) -> &str {
        self.target_name.as_deref().unwrap_or(&self.source_column)
    }
}

impl MeasureDefinition {
    /// Create a new measure definition.
    pub fn new(
        name: impl Into<String>,
        aggregation: AggregationType,
        column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            aggregation,
            source_column: column.into(),
            filter: None,
            description: None,
        }
    }

    /// Add a filter expression to this measure.
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Add a description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

impl WindowColumnDef {
    /// Create a new window column definition.
    pub fn new(name: impl Into<String>, func: WindowFunc) -> Self {
        Self {
            name: name.into(),
            func,
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            data_type: None,
        }
    }

    /// Set the arguments for the window function.
    pub fn with_args(mut self, args: Vec<Expr>) -> Self {
        self.args = args;
        self
    }

    /// Set a single argument (convenience for common case).
    pub fn with_arg(mut self, arg: Expr) -> Self {
        self.args = vec![arg];
        self
    }

    /// Set PARTITION BY columns.
    pub fn with_partition_by(mut self, partition_by: Vec<Expr>) -> Self {
        self.partition_by = partition_by;
        self
    }

    /// Set ORDER BY specification.
    pub fn with_order_by(mut self, order_by: Vec<OrderByExpr>) -> Self {
        self.order_by = order_by;
        self
    }

    /// Set the window frame.
    pub fn with_frame(mut self, frame: WindowFrame) -> Self {
        self.frame = Some(frame);
        self
    }

    /// Set the expected data type.
    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.data_type = Some(data_type);
        self
    }

    /// Create a running SUM window column.
    ///
    /// SUM(column) OVER (ORDER BY order_col)
    pub fn running_sum(
        name: impl Into<String>,
        column: Expr,
        order_by: Vec<OrderByExpr>,
    ) -> Self {
        Self::new(name, WindowFunc::Sum)
            .with_arg(column)
            .with_order_by(order_by)
    }

    /// Create a ROW_NUMBER window column.
    ///
    /// ROW_NUMBER() OVER (PARTITION BY partition_cols ORDER BY order_cols)
    pub fn row_number(
        name: impl Into<String>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
    ) -> Self {
        Self::new(name, WindowFunc::RowNumber)
            .with_partition_by(partition_by)
            .with_order_by(order_by)
    }

    /// Create a LAG window column.
    ///
    /// LAG(column, offset) OVER (ORDER BY order_col)
    pub fn lag(
        name: impl Into<String>,
        column: Expr,
        offset: i64,
        order_by: Vec<OrderByExpr>,
    ) -> Self {
        use super::expr::Literal;
        Self::new(name, WindowFunc::Lag)
            .with_args(vec![column, Expr::Literal(Literal::Int(offset))])
            .with_order_by(order_by)
    }

    /// Create a LEAD window column.
    ///
    /// LEAD(column, offset) OVER (ORDER BY order_col)
    pub fn lead(
        name: impl Into<String>,
        column: Expr,
        offset: i64,
        order_by: Vec<OrderByExpr>,
    ) -> Self {
        use super::expr::Literal;
        Self::new(name, WindowFunc::Lead)
            .with_args(vec![column, Expr::Literal(Literal::Int(offset))])
            .with_order_by(order_by)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_fact_definition_builder() {
        let fact = FactDefinition::new("fact_orders", "fact_orders")
            .with_schema("analytics")
            .with_grain("orders", "order_id")
            .with_grain("orders", "order_date")
            .include("customers", vec!["name", "region", "segment"])
            .with_sum("revenue", "total")
            .with_count_star("order_count")
            .with_materialization(MaterializationStrategy::Incremental {
                unique_key: vec!["order_id".into()],
                incremental_key: "order_date".into(),
                lookback: Some(Duration::from_secs(86400 * 3)),
            });

        assert_eq!(fact.name, "fact_orders");
        assert_eq!(fact.qualified_target_name(), "analytics.fact_orders");
        assert_eq!(fact.grain.len(), 2);
        assert_eq!(fact.includes.len(), 1);
        assert_eq!(fact.measures.len(), 2);

        let revenue = fact.measures.get("revenue").unwrap();
        assert_eq!(revenue.aggregation, AggregationType::Sum);
        assert_eq!(revenue.source_column, "total");
    }

    #[test]
    fn test_grain_column_target_name() {
        let grain_default = GrainColumn {
            source_entity: "orders".into(),
            source_column: "order_id".into(),
            target_name: None,
        };
        assert_eq!(grain_default.target_column_name(), "order_id");

        let grain_custom = GrainColumn {
            source_entity: "orders".into(),
            source_column: "order_id".into(),
            target_name: Some("id".into()),
        };
        assert_eq!(grain_custom.target_column_name(), "id");
    }

    #[test]
    fn test_include_with_prefix() {
        let fact = FactDefinition::new("fact_orders", "fact_orders")
            .include_with_prefix("customers", vec!["name", "region"], "customer_");

        let include = fact.includes.get("customers").unwrap();
        assert_eq!(include.prefix, Some("customer_".into()));
        assert_eq!(
            include.selection,
            ColumnSelection::Columns(vec!["name".into(), "region".into()])
        );
    }

    #[test]
    fn test_include_all() {
        let fact = FactDefinition::new("fact_orders", "fact_orders").include_all("customers");

        let include = fact.includes.get("customers").unwrap();
        assert_eq!(include.selection, ColumnSelection::All);
    }

    #[test]
    fn test_include_except() {
        let fact = FactDefinition::new("fact_orders", "fact_orders")
            .include_except("customers", vec!["email", "created_at"]);

        let include = fact.includes.get("customers").unwrap();
        assert_eq!(
            include.selection,
            ColumnSelection::Except(vec!["email".into(), "created_at".into()])
        );
    }

    #[test]
    fn test_measure_with_filter() {
        use super::super::expr::{BinaryOp, Literal};

        let filter_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                entity: None,
                column: "status".into(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("completed".into()))),
        };

        let measure = MeasureDefinition::new("completed_revenue", AggregationType::Sum, "total")
            .with_filter(filter_expr.clone())
            .with_description("Revenue from completed orders only");

        assert_eq!(measure.filter, Some(filter_expr));
        assert!(measure.description.is_some());
    }

    #[test]
    fn test_fact_with_columns() {
        use super::super::expr::{BinaryOp, Func, Literal};

        let fact = FactDefinition::new("fact_orders", "fact_orders")
            .with_simple_column("order_id")
            .with_renamed_column("cust_id", "customer_id")
            .with_computed_column(
                "is_large_order",
                Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        entity: None,
                        column: "total".into(),
                    }),
                    op: BinaryOp::Gt,
                    right: Box::new(Expr::Literal(Literal::Int(1000))),
                },
                Some(DataType::Bool),
            )
            .with_computed_column(
                "order_month",
                Expr::Function {
                    func: Func::DateTrunc,
                    args: vec![
                        Expr::Literal(Literal::String("month".into())),
                        Expr::Column {
                            entity: None,
                            column: "order_date".into(),
                        },
                    ],
                },
                Some(DataType::Date),
            );

        assert_eq!(fact.columns.len(), 4);

        // Check simple column
        assert!(matches!(&fact.columns[0], ColumnDef::Simple(name) if name == "order_id"));

        // Check renamed column
        assert!(matches!(&fact.columns[1], ColumnDef::Renamed { source, target }
            if source == "cust_id" && target == "customer_id"));

        // Check computed columns
        assert!(matches!(&fact.columns[2], ColumnDef::Computed { name, .. } if name == "is_large_order"));
        assert!(matches!(&fact.columns[3], ColumnDef::Computed { name, .. } if name == "order_month"));
    }

    #[test]
    fn test_fact_with_window_columns() {
        use super::super::expr::SortDir;

        let order_date_col = Expr::Column {
            entity: None,
            column: "order_date".into(),
        };

        let amount_col = Expr::Column {
            entity: None,
            column: "amount".into(),
        };

        let fact = FactDefinition::new("fact_orders", "fact_orders")
            .with_window_column(WindowColumnDef::running_sum(
                "running_total",
                amount_col.clone(),
                vec![OrderByExpr {
                    expr: order_date_col.clone(),
                    dir: SortDir::Asc,
                    nulls: None,
                }],
            ))
            .with_window_column(WindowColumnDef::row_number(
                "order_rank",
                vec![Expr::Column {
                    entity: None,
                    column: "customer_id".into(),
                }],
                vec![OrderByExpr {
                    expr: order_date_col.clone(),
                    dir: SortDir::Desc,
                    nulls: None,
                }],
            ))
            .with_window_column(WindowColumnDef::lag(
                "prev_amount",
                amount_col.clone(),
                1,
                vec![OrderByExpr {
                    expr: order_date_col.clone(),
                    dir: SortDir::Asc,
                    nulls: None,
                }],
            ));

        assert_eq!(fact.window_columns.len(), 3);

        // Check running sum
        assert_eq!(fact.window_columns[0].name, "running_total");
        assert_eq!(fact.window_columns[0].func, WindowFunc::Sum);
        assert_eq!(fact.window_columns[0].args.len(), 1);

        // Check row number
        assert_eq!(fact.window_columns[1].name, "order_rank");
        assert_eq!(fact.window_columns[1].func, WindowFunc::RowNumber);
        assert_eq!(fact.window_columns[1].partition_by.len(), 1);

        // Check lag
        assert_eq!(fact.window_columns[2].name, "prev_amount");
        assert_eq!(fact.window_columns[2].func, WindowFunc::Lag);
        assert_eq!(fact.window_columns[2].args.len(), 2);
    }

    #[test]
    fn test_window_column_builder() {
        use super::super::expr::{FrameBound, FrameKind, SortDir};

        let window_col = WindowColumnDef::new("ytd_sales", WindowFunc::Sum)
            .with_arg(Expr::Column {
                entity: None,
                column: "sales".into(),
            })
            .with_partition_by(vec![Expr::Column {
                entity: None,
                column: "year".into(),
            }])
            .with_order_by(vec![OrderByExpr {
                expr: Expr::Column {
                    entity: None,
                    column: "month".into(),
                },
                dir: SortDir::Asc,
                nulls: None,
            }])
            .with_frame(WindowFrame {
                kind: FrameKind::Rows,
                start: FrameBound::UnboundedPreceding,
                end: Some(FrameBound::CurrentRow),
            })
            .with_data_type(DataType::Decimal(18, 2));

        assert_eq!(window_col.name, "ytd_sales");
        assert_eq!(window_col.func, WindowFunc::Sum);
        assert!(window_col.frame.is_some());
        assert_eq!(window_col.data_type, Some(DataType::Decimal(18, 2)));
    }
}
