//! Metadata provider module.
//!
//! This module provides abstractions for fetching database metadata from the worker
//! and performing local relationship inference.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      MetadataProvider                           │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │  Worker RPC (async)        │  Local Inference (sync)      │  │
//! │  │  - list_schemas()          │  - infer_relationships()     │  │
//! │  │  - list_tables()           │    (uses InferenceEngine)    │  │
//! │  │  - get_table()             │                              │  │
//! │  │  - get_foreign_keys()      │                              │  │
//! │  │  - get_column_stats()      │                              │  │
//! │  │  - check_value_overlap()   │                              │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//!                           │
//!                           ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      WorkerClient                               │
//! │              (NDJSON over stdin/stdout)                         │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use mantis::metadata::{MetadataProvider, WorkerMetadataProvider};
//! use mantis::worker::WorkerClient;
//!
//! // Create provider with worker client
//! let client = WorkerClient::spawn("./mantis-worker").await?;
//! let provider = WorkerMetadataProvider::new(client, "duckdb", "./data.duckdb");
//!
//! // Fetch metadata
//! let schemas = provider.list_schemas().await?;
//! let tables = provider.list_tables("main").await?;
//! let table = provider.get_table("main", "orders").await?;
//!
//! // Infer relationships locally (no worker call)
//! let inferred = provider.infer_relationships(&[table], Default::default());
//! ```

mod provider;
// mod types;  // Archived - old SourceEntity/SourceColumn types
mod worker_provider;

pub use provider::{MetadataProvider, MetadataProviderExt};
// pub use types::*;  // Archived
pub use worker_provider::WorkerMetadataProvider;

// Re-export types from worker protocol
pub use crate::worker::protocol::{ForeignKeyInfo, TableInfo};

// Type aliases for response types used by MetadataProvider trait
pub use crate::worker::protocol::ColumnStatsResponse as ColumnStats;
pub use crate::worker::protocol::GetTableResponse as TableMetadata;
pub use crate::worker::protocol::ValueOverlapResponse as ValueOverlap;

// Schema info type
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub name: String,
}

impl From<crate::worker::protocol::SchemaInfo> for SchemaInfo {
    fn from(info: crate::worker::protocol::SchemaInfo) -> Self {
        Self { name: info.name }
    }
}
