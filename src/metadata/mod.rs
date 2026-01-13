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
mod types;
mod worker_provider;

pub use provider::{MetadataProvider, MetadataProviderExt};
pub use types::*;
pub use worker_provider::WorkerMetadataProvider;
