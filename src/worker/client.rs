//! Async client for communicating with the Go worker process.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};

use super::error::{WorkerError, WorkerResult};
use super::protocol::{RequestEnvelope, ResponseEnvelope};
use crate::config::Settings;

/// Default timeout for requests (30 seconds).
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Async client for the Go database worker.
///
/// The client spawns the worker as a child process and communicates via
/// NDJSON (newline-delimited JSON) over stdin/stdout. Each request has a
/// unique ID for correlation with responses, enabling concurrent requests.
///
/// # Example
///
/// ```ignore
/// use mantis::worker::{WorkerClient, protocol::*};
///
/// let client = WorkerClient::spawn("./mantis-worker").await?;
///
/// let response: ListSchemasResponse = client.request(
///     "metadata.list_schemas",
///     ListSchemasParams { ... }
/// ).await?;
/// ```
pub struct WorkerClient {
    /// Writer for sending requests to worker stdin.
    stdin: Arc<Mutex<BufWriter<ChildStdin>>>,

    /// Map of pending request IDs to response channels.
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<ResponseEnvelope>>>>,

    /// Handle to the worker child process.
    _child: Child,

    /// Handle to the background reader task.
    _reader_task: tokio::task::JoinHandle<()>,

    /// Request timeout duration.
    timeout: Duration,
}

impl WorkerClient {
    /// Spawn a new worker process.
    ///
    /// # Arguments
    ///
    /// * `worker_path` - Path to the Go worker binary.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker process cannot be spawned.
    pub async fn spawn<P: AsRef<Path>>(worker_path: P) -> WorkerResult<Self> {
        Self::spawn_with_timeout(worker_path, Duration::from_secs(DEFAULT_TIMEOUT_SECS)).await
    }

    /// Spawn a worker using settings configuration.
    ///
    /// This method will:
    /// 1. Use embedded worker if available and configured
    /// 2. Fall back to configured worker path
    /// 3. Search common locations as last resort
    ///
    /// Pool settings from the configuration are passed to the worker.
    pub async fn spawn_with_settings(settings: &Settings) -> WorkerResult<Self> {
        let worker_path = Self::resolve_worker_path(settings)?;
        let pool_args = settings.worker.pool.to_worker_args();

        Self::spawn_with_args(&worker_path, &pool_args).await
    }

    /// Resolve the worker binary path from settings.
    fn resolve_worker_path(settings: &Settings) -> WorkerResult<PathBuf> {
        // Try embedded worker first if enabled
        if settings.worker.embedded && super::embedded::is_embedded_available() {
            return super::embedded::extract_worker();
        }

        // Try configured path
        if let Some(path) = settings.worker_path() {
            return Ok(path);
        }

        // Search common locations
        let candidates = [
            "mantis-worker",
            "./mantis-worker",
            "./worker/mantis-worker",
            "./worker/cmd/worker/worker",
        ];

        for candidate in candidates {
            let path = PathBuf::from(candidate);
            if path.exists() {
                return Ok(path);
            }
        }

        // Try PATH
        if let Ok(output) = std::process::Command::new("which")
            .arg("mantis-worker")
            .output()
        {
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path.is_empty() {
                    return Ok(PathBuf::from(path));
                }
            }
        }

        Err(WorkerError::SpawnFailed(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Worker binary not found. Set worker.path in config or build with --features embedded-worker",
        )))
    }

    /// Spawn a worker with command-line arguments (for pool settings).
    pub async fn spawn_with_args<P: AsRef<Path>>(
        worker_path: P,
        args: &[String],
    ) -> WorkerResult<Self> {
        Self::spawn_with_args_and_timeout(
            worker_path,
            args,
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        )
        .await
    }

    /// Spawn a worker with arguments and custom timeout.
    pub async fn spawn_with_args_and_timeout<P: AsRef<Path>>(
        worker_path: P,
        args: &[String],
        timeout: Duration,
    ) -> WorkerResult<Self> {
        let mut child = Command::new(worker_path.as_ref())
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(WorkerError::SpawnFailed)?;

        let stdin = child.stdin.take().expect("stdin not captured");
        let stdout = child.stdout.take().expect("stdout not captured");

        let stdin = Arc::new(Mutex::new(BufWriter::new(stdin)));
        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<ResponseEnvelope>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Spawn background reader task
        let reader_task = Self::spawn_reader_task(stdout, pending.clone());

        Ok(Self {
            stdin,
            pending,
            _child: child,
            _reader_task: reader_task,
            timeout,
        })
    }

    /// Spawn a new worker process with a custom timeout.
    ///
    /// # Arguments
    ///
    /// * `worker_path` - Path to the Go worker binary.
    /// * `timeout` - Timeout duration for requests.
    pub async fn spawn_with_timeout<P: AsRef<Path>>(
        worker_path: P,
        timeout: Duration,
    ) -> WorkerResult<Self> {
        let mut child = Command::new(worker_path.as_ref())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(WorkerError::SpawnFailed)?;

        let stdin = child.stdin.take().expect("stdin not captured");
        let stdout = child.stdout.take().expect("stdout not captured");

        let stdin = Arc::new(Mutex::new(BufWriter::new(stdin)));
        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<ResponseEnvelope>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Spawn background reader task
        let reader_task = Self::spawn_reader_task(stdout, pending.clone());

        Ok(Self {
            stdin,
            pending,
            _child: child,
            _reader_task: reader_task,
            timeout,
        })
    }

    /// Spawn the background task that reads responses from the worker.
    fn spawn_reader_task(
        stdout: ChildStdout,
        pending: Arc<Mutex<HashMap<String, oneshot::Sender<ResponseEnvelope>>>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF - worker exited
                        break;
                    }
                    Ok(_) => {
                        // Try to parse as response envelope
                        match serde_json::from_str::<ResponseEnvelope>(&line) {
                            Ok(resp) => {
                                let mut pending = pending.lock().await;
                                if let Some(tx) = pending.remove(&resp.id) {
                                    // Send response to waiting caller
                                    let _ = tx.send(resp);
                                }
                            }
                            Err(e) => {
                                // Log parse error but continue
                                eprintln!("worker: failed to parse response: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        // Read error - log and break
                        eprintln!("worker: read error: {}", e);
                        break;
                    }
                }
            }

            // Worker exited - notify all pending requests with error responses
            let mut pending = pending.lock().await;
            for (id, tx) in pending.drain() {
                let error_response = ResponseEnvelope {
                    id,
                    success: false,
                    result: None,
                    error: Some(super::protocol::ErrorInfo {
                        code: "WORKER_EXITED".to_string(),
                        message: "Worker process exited unexpectedly".to_string(),
                    }),
                };
                let _ = tx.send(error_response);
            }
        })
    }

    /// Send a request to the worker and wait for a response.
    ///
    /// # Type Parameters
    ///
    /// * `P` - Request parameters type (must implement Serialize).
    /// * `R` - Response type (must implement DeserializeOwned).
    ///
    /// # Arguments
    ///
    /// * `method` - The method name (e.g., "metadata.list_schemas").
    /// * `params` - The method-specific parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Serialization fails
    /// - Writing to the worker fails
    /// - The request times out
    /// - The worker returns an error response
    /// - Deserialization of the response fails
    pub async fn request<P, R>(&self, method: &str, params: P) -> WorkerResult<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let request = RequestEnvelope {
            id: id.clone(),
            method: method.to_string(),
            params: serde_json::to_value(params).map_err(WorkerError::SerializeFailed)?,
        };

        // Register response channel
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(id.clone(), tx);
        }

        // Send request
        {
            let mut stdin = self.stdin.lock().await;
            let line =
                serde_json::to_string(&request).map_err(WorkerError::SerializeFailed)? + "\n";
            stdin
                .write_all(line.as_bytes())
                .await
                .map_err(WorkerError::WriteFailed)?;
            stdin.flush().await.map_err(WorkerError::WriteFailed)?;
        }

        // Wait for response with timeout
        let response = match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => {
                // Channel closed - worker exited
                return Err(WorkerError::ChannelClosed);
            }
            Err(_) => {
                // Timeout - clean up pending request to prevent memory leak
                let mut pending = self.pending.lock().await;
                pending.remove(&id);
                return Err(WorkerError::Timeout(self.timeout.as_secs()));
            }
        };

        // Process response
        if response.success {
            let result = response.result.unwrap_or(serde_json::Value::Null);
            serde_json::from_value(result).map_err(WorkerError::DeserializeFailed)
        } else {
            let error = response.error.unwrap_or_else(|| super::protocol::ErrorInfo {
                code: "UNKNOWN".to_string(),
                message: "Unknown error".to_string(),
            });
            Err(Self::classify_error(&error.code, &error.message))
        }
    }

    /// Classify a worker error into a more specific error type.
    fn classify_error(code: &str, message: &str) -> WorkerError {
        match code {
            "DRIVER_NOT_FOUND" => WorkerError::DriverNotFound(message.to_string()),
            "CONNECTION_FAILED" => WorkerError::ConnectionFailed(message.to_string()),
            "INVALID_REQUEST" => WorkerError::InvalidRequest(message.to_string()),
            "METHOD_NOT_FOUND" => WorkerError::MethodNotFound(message.to_string()),
            _ => WorkerError::remote(code, message),
        }
    }

    /// Check if the worker is still running.
    ///
    /// Returns `true` if the worker process appears to be running,
    /// `false` if the reader task has finished (indicating worker exit).
    pub fn is_alive(&self) -> bool {
        // If the reader task has finished, the worker has exited
        !self._reader_task.is_finished()
    }

    /// Get the current request timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Set the request timeout.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}

// Convenience methods for common operations
impl WorkerClient {
    /// List all schemas in the database.
    pub async fn list_schemas(
        &self,
        driver: &str,
        connection_string: &str,
    ) -> WorkerResult<super::protocol::ListSchemasResponse> {
        use super::protocol::{methods, ConnectionParams, ListSchemasParams};

        self.request(
            methods::LIST_SCHEMAS,
            ListSchemasParams {
                connection: ConnectionParams {
                    driver: driver.to_string(),
                    connection_string: connection_string.to_string(),
                },
            },
        )
        .await
    }

    /// List all tables in a schema.
    pub async fn list_tables(
        &self,
        driver: &str,
        connection_string: &str,
        schema: Option<&str>,
    ) -> WorkerResult<super::protocol::ListTablesResponse> {
        use super::protocol::{methods, ConnectionParams, ListTablesParams};

        self.request(
            methods::LIST_TABLES,
            ListTablesParams {
                connection: ConnectionParams {
                    driver: driver.to_string(),
                    connection_string: connection_string.to_string(),
                },
                schema: schema.map(|s| s.to_string()),
            },
        )
        .await
    }

    /// Get detailed information about a table.
    pub async fn get_table(
        &self,
        driver: &str,
        connection_string: &str,
        schema: &str,
        table: &str,
    ) -> WorkerResult<super::protocol::GetTableResponse> {
        use super::protocol::{methods, ConnectionParams, GetTableParams};

        self.request(
            methods::GET_TABLE,
            GetTableParams {
                connection: ConnectionParams {
                    driver: driver.to_string(),
                    connection_string: connection_string.to_string(),
                },
                schema: schema.to_string(),
                table: table.to_string(),
            },
        )
        .await
    }

    /// Get column statistics for cardinality analysis.
    pub async fn get_column_stats(
        &self,
        driver: &str,
        connection_string: &str,
        schema: &str,
        table: &str,
        column: &str,
    ) -> WorkerResult<super::protocol::ColumnStatsResponse> {
        use super::protocol::{methods, ConnectionParams, GetColumnStatsParams};

        self.request(
            methods::GET_COLUMN_STATS,
            GetColumnStatsParams {
                connection: ConnectionParams {
                    driver: driver.to_string(),
                    connection_string: connection_string.to_string(),
                },
                schema: schema.to_string(),
                table: table.to_string(),
                column: column.to_string(),
                sample_size: None,
            },
        )
        .await
    }

    /// Check value overlap between two columns.
    #[allow(clippy::too_many_arguments)]
    pub async fn check_value_overlap(
        &self,
        driver: &str,
        connection_string: &str,
        left_schema: &str,
        left_table: &str,
        left_column: &str,
        right_schema: &str,
        right_table: &str,
        right_column: &str,
    ) -> WorkerResult<super::protocol::ValueOverlapResponse> {
        use super::protocol::{methods, CheckValueOverlapParams, ConnectionParams};

        self.request(
            methods::CHECK_VALUE_OVERLAP,
            CheckValueOverlapParams {
                connection: ConnectionParams {
                    driver: driver.to_string(),
                    connection_string: connection_string.to_string(),
                },
                left_schema: left_schema.to_string(),
                left_table: left_table.to_string(),
                left_column: left_column.to_string(),
                right_schema: right_schema.to_string(),
                right_table: right_table.to_string(),
                right_column: right_column.to_string(),
                sample_size: None,
            },
        )
        .await
    }

    /// Execute a SQL query.
    pub async fn execute_query(
        &self,
        driver: &str,
        connection_string: &str,
        sql: &str,
    ) -> WorkerResult<super::protocol::ExecuteQueryResponse> {
        use super::protocol::{methods, ConnectionParams, ExecuteQueryParams};

        self.request(
            methods::EXECUTE_QUERY,
            ExecuteQueryParams {
                connection: ConnectionParams {
                    driver: driver.to_string(),
                    connection_string: connection_string.to_string(),
                },
                sql: sql.to_string(),
                args: None,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_envelope_serialization() {
        let request = RequestEnvelope {
            id: "test-123".to_string(),
            method: "metadata.list_schemas".to_string(),
            params: serde_json::json!({
                "driver": "duckdb",
                "connection_string": "./test.db"
            }),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("test-123"));
        assert!(json.contains("metadata.list_schemas"));
        assert!(json.contains("duckdb"));
    }

    #[test]
    fn test_response_envelope_deserialization() {
        let json = r#"{
            "id": "test-123",
            "success": true,
            "result": {"schemas": [{"name": "main", "is_default": true}]}
        }"#;

        let response: ResponseEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(response.id, "test-123");
        assert!(response.success);
        assert!(response.result.is_some());
        assert!(response.error.is_none());
    }

    #[test]
    fn test_error_response_deserialization() {
        let json = r#"{
            "id": "test-456",
            "success": false,
            "error": {"code": "CONNECTION_FAILED", "message": "Unable to connect"}
        }"#;

        let response: ResponseEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(response.id, "test-456");
        assert!(!response.success);
        assert!(response.error.is_some());
        let error = response.error.unwrap();
        assert_eq!(error.code, "CONNECTION_FAILED");
    }

    #[test]
    fn test_error_classification() {
        assert!(matches!(
            WorkerClient::classify_error("DRIVER_NOT_FOUND", "test"),
            WorkerError::DriverNotFound(_)
        ));
        assert!(matches!(
            WorkerClient::classify_error("CONNECTION_FAILED", "test"),
            WorkerError::ConnectionFailed(_)
        ));
        assert!(matches!(
            WorkerClient::classify_error("INVALID_REQUEST", "test"),
            WorkerError::InvalidRequest(_)
        ));
        assert!(matches!(
            WorkerClient::classify_error("METHOD_NOT_FOUND", "test"),
            WorkerError::MethodNotFound(_)
        ));
        assert!(matches!(
            WorkerClient::classify_error("UNKNOWN_CODE", "test"),
            WorkerError::Remote { .. }
        ));
    }
}
