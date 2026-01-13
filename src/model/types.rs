//! Core types for the semantic model.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// SQL data types with precision/scale where applicable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// Boolean type
    Bool,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// Decimal with precision and scale
    Decimal(u8, u8),
    /// Variable-length string
    String,
    /// Fixed-length string
    Char(u16),
    /// Variable-length string with max length
    Varchar(u16),
    /// Date (no time component)
    Date,
    /// Time (no date component)
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    TimestampTz,
    /// Binary data
    Binary,
    /// JSON data
    Json,
    /// UUID
    Uuid,
}

impl DataType {
    /// Parse a type string like "decimal(10,2)" or "varchar(255)"
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.to_lowercase();
        let s = s.trim();

        // Handle parameterized types
        if let Some(inner) = s.strip_prefix("decimal(").and_then(|s| s.strip_suffix(')')) {
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() == 2 {
                let precision = parts[0].trim().parse().ok()?;
                let scale = parts[1].trim().parse().ok()?;
                return Some(DataType::Decimal(precision, scale));
            }
        }

        if let Some(inner) = s.strip_prefix("varchar(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Varchar(len));
        }

        // SQL Server nvarchar (Unicode varchar)
        if let Some(inner) = s.strip_prefix("nvarchar(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Varchar(len));
        }

        if let Some(inner) = s.strip_prefix("char(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Char(len));
        }

        // SQL Server nchar (Unicode char)
        if let Some(inner) = s.strip_prefix("nchar(").and_then(|s| s.strip_suffix(')')) {
            let len = inner.trim().parse().ok()?;
            return Some(DataType::Char(len));
        }

        // Simple types
        match s {
            "bool" | "boolean" => Some(DataType::Bool),
            "int8" | "tinyint" => Some(DataType::Int8),
            "int16" | "smallint" => Some(DataType::Int16),
            "int32" | "int" | "integer" => Some(DataType::Int32),
            "int64" | "bigint" => Some(DataType::Int64),
            "float32" | "float" | "real" => Some(DataType::Float32),
            "float64" | "double" => Some(DataType::Float64),
            "string" | "text" | "ntext" => Some(DataType::String),
            "date" => Some(DataType::Date),
            "time" => Some(DataType::Time),
            "timestamp" | "datetime" => Some(DataType::Timestamp),
            "timestamptz" | "datetimeoffset" => Some(DataType::TimestampTz),
            "binary" | "blob" | "varbinary" => Some(DataType::Binary),
            "json" | "jsonb" => Some(DataType::Json),
            "uuid" | "uniqueidentifier" => Some(DataType::Uuid),
            _ => None,
        }
    }
}

/// How a target table should be materialized.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[derive(Default)]
pub enum MaterializationStrategy {
    /// CREATE VIEW - always fresh, computed on read
    #[default]
    View,

    /// CREATE TABLE AS SELECT - full refresh each build
    Table,

    /// Incremental with MERGE - append/update new rows
    Incremental {
        /// Columns that uniquely identify a row
        unique_key: Vec<String>,
        /// Column used to detect new/changed rows
        incremental_key: String,
        /// Optional lookback window for late-arriving data
        lookback: Option<Duration>,
    },

    /// Snapshot for SCD Type 2 - track historical changes
    Snapshot {
        /// Columns that uniquely identify a row
        unique_key: Vec<String>,
        /// Column indicating when row was last updated
        updated_at: String,
    },
}


/// Physical table type for materialized entities.
///
/// Only relevant when `materialized = true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TableType {
    /// Physical table, full refresh each build
    #[default]
    Table,
    /// Physical table, incremental refresh (MERGE/upsert)
    Incremental,
    /// Database view (always current, computed on read)
    View,
}

/// Aggregation types for measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregationType {
    Sum,
    Count,
    CountDistinct,
    Avg,
    Min,
    Max,
}

impl std::fmt::Display for AggregationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationType::Sum => write!(f, "SUM"),
            AggregationType::Count => write!(f, "COUNT"),
            AggregationType::CountDistinct => write!(f, "COUNT_DISTINCT"),
            AggregationType::Avg => write!(f, "AVG"),
            AggregationType::Min => write!(f, "MIN"),
            AggregationType::Max => write!(f, "MAX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_parse_simple() {
        assert_eq!(DataType::parse("int64"), Some(DataType::Int64));
        assert_eq!(DataType::parse("bigint"), Some(DataType::Int64));
        assert_eq!(DataType::parse("string"), Some(DataType::String));
        assert_eq!(DataType::parse("date"), Some(DataType::Date));
        assert_eq!(DataType::parse("Bool"), Some(DataType::Bool)); // case insensitive
    }

    #[test]
    fn test_datatype_parse_parameterized() {
        assert_eq!(DataType::parse("decimal(10,2)"), Some(DataType::Decimal(10, 2)));
        assert_eq!(DataType::parse("varchar(255)"), Some(DataType::Varchar(255)));
        assert_eq!(DataType::parse("char(10)"), Some(DataType::Char(10)));
        // SQL Server Unicode types
        assert_eq!(DataType::parse("nvarchar(260)"), Some(DataType::Varchar(260)));
        assert_eq!(DataType::parse("nchar(50)"), Some(DataType::Char(50)));
    }

    #[test]
    fn test_datatype_parse_with_spaces() {
        assert_eq!(DataType::parse("decimal(10, 2)"), Some(DataType::Decimal(10, 2)));
        assert_eq!(DataType::parse("  int64  "), Some(DataType::Int64));
    }

    #[test]
    fn test_datatype_parse_invalid() {
        assert_eq!(DataType::parse("invalid"), None);
        assert_eq!(DataType::parse("decimal(10)"), None); // missing scale
    }

    #[test]
    fn test_materialization_default() {
        assert_eq!(MaterializationStrategy::default(), MaterializationStrategy::View);
    }
}
