//! Worker communication module.
//!
//! This module provides async communication with the Go database worker process.
//! The worker handles all database operations (metadata queries, query execution)
//! while the Rust compiler remains database-agnostic.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  Mantis Compiler (Rust + Tokio)                 │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │                    WorkerClient (Async)                   │  │
//! │  │  - Spawns Go worker as child process                      │  │
//! │  │  - NDJSON protocol over stdin/stdout                      │  │
//! │  │  - Request IDs for concurrent request correlation         │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! │                              │                                   │
//! │               stdin (NDJSON) │ stdout (NDJSON)                  │
//! │                              ▼                                   │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │              Go Worker (Long-Running Child Process)             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use mantis::worker::{WorkerClient, protocol::*};
//!
//! let client = WorkerClient::spawn("./mantis-worker").await?;
//!
//! // List schemas
//! let schemas: ListSchemasResponse = client.request(
//!     "metadata.list_schemas",
//!     ListSchemasParams {
//!         driver: "duckdb".to_string(),
//!         connection_string: "./data.duckdb".to_string(),
//!     }
//! ).await?;
//!
//! // Client is automatically shut down on drop
//! ```

mod client;
pub mod embedded;
mod error;
pub mod protocol;

pub use client::WorkerClient;
pub use embedded::{extract_worker, is_embedded_available};
pub use error::{WorkerError, WorkerResult};
