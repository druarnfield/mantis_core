//! Worker-specific error types.

use std::io;
use thiserror::Error;

/// Result type for worker operations.
pub type WorkerResult<T> = Result<T, WorkerError>;

/// Errors that can occur during worker communication.
#[derive(Error, Debug)]
pub enum WorkerError {
    /// Failed to spawn the worker process.
    #[error("failed to spawn worker process: {0}")]
    SpawnFailed(#[source] io::Error),

    /// Failed to write to worker stdin.
    #[error("failed to write to worker: {0}")]
    WriteFailed(#[source] io::Error),

    /// Failed to read from worker stdout.
    #[error("failed to read from worker: {0}")]
    ReadFailed(#[source] io::Error),

    /// Failed to serialize request to JSON.
    #[error("failed to serialize request: {0}")]
    SerializeFailed(#[source] serde_json::Error),

    /// Failed to deserialize response from JSON.
    #[error("failed to deserialize response: {0}")]
    DeserializeFailed(#[source] serde_json::Error),

    /// Request timed out waiting for response.
    #[error("request timed out after {0} seconds")]
    Timeout(u64),

    /// Worker process exited unexpectedly.
    #[error("worker process exited unexpectedly")]
    WorkerExited,

    /// Response channel was closed (internal error).
    #[error("response channel closed unexpectedly")]
    ChannelClosed,

    /// Worker returned an error response.
    #[error("worker error: {message} (code: {code})")]
    Remote {
        /// Error code from worker.
        code: String,
        /// Error message from worker.
        message: String,
    },

    /// Database driver not found.
    #[error("database driver not found: {0}")]
    DriverNotFound(String),

    /// Database connection failed.
    #[error("database connection failed: {0}")]
    ConnectionFailed(String),

    /// Invalid request parameters.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Method not found.
    #[error("method not found: {0}")]
    MethodNotFound(String),
}

impl WorkerError {
    /// Create a remote error from an error response.
    pub fn remote(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Remote {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Check if this error indicates the worker has exited.
    pub fn is_worker_exited(&self) -> bool {
        matches!(self, Self::WorkerExited | Self::ChannelClosed)
    }

    /// Check if this error is retriable.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Timeout(_) | Self::WorkerExited | Self::ChannelClosed
        )
    }
}

impl From<io::Error> for WorkerError {
    fn from(err: io::Error) -> Self {
        Self::WriteFailed(err)
    }
}

impl From<serde_json::Error> for WorkerError {
    fn from(err: serde_json::Error) -> Self {
        Self::DeserializeFailed(err)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for WorkerError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::ChannelClosed
    }
}
