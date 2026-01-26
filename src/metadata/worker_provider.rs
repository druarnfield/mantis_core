//! WorkerMetadataProvider implementation.
//!
//! This module provides the primary MetadataProvider implementation that
//! uses the WorkerClient for async RPC calls to the Go worker.

use std::sync::Arc;

use async_trait::async_trait;

use super::provider::{MetadataProvider, MetadataResult};
use super::{ColumnStats, ForeignKeyInfo, SchemaInfo, TableInfo, TableMetadata, ValueOverlap};
use crate::worker::protocol::{self, methods, ConnectionParams, DatabaseInfo};
use crate::worker::WorkerClient;

/// MetadataProvider implementation that uses the WorkerClient.
///
/// This provider wraps a WorkerClient and uses it to make async RPC calls
/// to the Go worker for database metadata operations.
///
/// # Example
///
/// ```ignore
/// use mantis::worker::WorkerClient;
/// use mantis::metadata::WorkerMetadataProvider;
///
/// let client = WorkerClient::spawn("./mantis-worker").await?;
/// let provider = WorkerMetadataProvider::new(
///     Arc::new(client),
///     "duckdb",
///     "./data.duckdb",
/// );
///
/// let schemas = provider.list_schemas().await?;
/// ```
pub struct WorkerMetadataProvider {
    /// The worker client for RPC calls.
    client: Arc<WorkerClient>,
    /// Cached connection parameters to avoid repeated allocations.
    connection: ConnectionParams,
}

impl WorkerMetadataProvider {
    /// Create a new WorkerMetadataProvider.
    ///
    /// # Arguments
    ///
    /// * `client` - The worker client (shared reference).
    /// * `driver` - Database driver name (e.g., "duckdb", "mssql").
    /// * `connection_string` - Driver-specific connection string.
    pub fn new(
        client: Arc<WorkerClient>,
        driver: impl Into<String>,
        connection_string: impl Into<String>,
    ) -> Self {
        Self {
            client,
            connection: ConnectionParams {
                driver: driver.into(),
                connection_string: connection_string.into(),
            },
        }
    }

    /// Create a new WorkerMetadataProvider with an owned client.
    ///
    /// This is a convenience method that wraps the client in an Arc.
    pub fn with_client(
        client: WorkerClient,
        driver: impl Into<String>,
        connection_string: impl Into<String>,
    ) -> Self {
        Self::new(Arc::new(client), driver, connection_string)
    }

    /// Get the connection parameters for requests.
    ///
    /// Returns a clone of the cached connection parameters.
    #[inline]
    fn connection_params(&self) -> ConnectionParams {
        self.connection.clone()
    }

    /// Get the driver name.
    pub fn driver(&self) -> &str {
        &self.connection.driver
    }

    /// Get the connection string.
    pub fn connection_string(&self) -> &str {
        &self.connection.connection_string
    }
}

#[async_trait]
impl MetadataProvider for WorkerMetadataProvider {
    async fn list_schemas(&self) -> MetadataResult<Vec<SchemaInfo>> {
        let response: protocol::ListSchemasResponse = self
            .client
            .request(
                methods::LIST_SCHEMAS,
                protocol::ListSchemasParams {
                    connection: self.connection_params(),
                },
            )
            .await?;

        Ok(response.schemas.into_iter().map(Into::into).collect())
    }

    async fn list_tables(&self, schema: &str) -> MetadataResult<Vec<TableInfo>> {
        let response: protocol::ListTablesResponse = self
            .client
            .request(
                methods::LIST_TABLES,
                protocol::ListTablesParams {
                    connection: self.connection_params(),
                    schema: if schema.is_empty() {
                        None
                    } else {
                        Some(schema.to_string())
                    },
                },
            )
            .await?;

        Ok(response.tables.into_iter().map(Into::into).collect())
    }

    async fn get_table(&self, schema: &str, table: &str) -> MetadataResult<TableMetadata> {
        let response: protocol::GetTableResponse = self
            .client
            .request(
                methods::GET_TABLE,
                protocol::GetTableParams {
                    connection: self.connection_params(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                },
            )
            .await?;

        Ok(response)
    }

    async fn get_foreign_keys(
        &self,
        schema: &str,
        table: &str,
    ) -> MetadataResult<Vec<ForeignKeyInfo>> {
        let response: protocol::GetForeignKeysResponse = self
            .client
            .request(
                methods::GET_FOREIGN_KEYS,
                protocol::GetForeignKeysParams {
                    connection: self.connection_params(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                },
            )
            .await?;

        Ok(response.foreign_keys.into_iter().map(Into::into).collect())
    }

    async fn get_column_stats(
        &self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> MetadataResult<ColumnStats> {
        let response: protocol::ColumnStatsResponse = self
            .client
            .request(
                methods::GET_COLUMN_STATS,
                protocol::GetColumnStatsParams {
                    connection: self.connection_params(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                    column: column.to_string(),
                    sample_size: None,
                },
            )
            .await?;

        Ok(response.into())
    }

    async fn check_value_overlap(
        &self,
        left_schema: &str,
        left_table: &str,
        left_column: &str,
        right_schema: &str,
        right_table: &str,
        right_column: &str,
    ) -> MetadataResult<ValueOverlap> {
        let response: protocol::ValueOverlapResponse = self
            .client
            .request(
                methods::CHECK_VALUE_OVERLAP,
                protocol::CheckValueOverlapParams {
                    connection: self.connection_params(),
                    left_schema: left_schema.to_string(),
                    left_table: left_table.to_string(),
                    left_column: left_column.to_string(),
                    right_schema: right_schema.to_string(),
                    right_table: right_table.to_string(),
                    right_column: right_column.to_string(),
                    sample_size: None,
                },
            )
            .await?;

        Ok(response.into())
    }

    async fn get_database_info(&self) -> MetadataResult<DatabaseInfo> {
        let response: protocol::GetDatabaseInfoResponse = self
            .client
            .request(
                methods::GET_DATABASE_INFO,
                protocol::GetDatabaseInfoParams {
                    connection: self.connection_params(),
                },
            )
            .await?;

        Ok(response.database.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Integration tests require a running worker process.
    // These are unit tests for the type conversions and structure.

    #[test]
    fn test_provider_construction() {
        // This test would require a mock WorkerClient or actual worker
        // For now, just verify the types compile correctly
        fn _assert_provider_is_send_sync<T: Send + Sync>() {}
        _assert_provider_is_send_sync::<WorkerMetadataProvider>();
    }

    #[test]
    fn test_connection_params() {
        // Test ConnectionParams construction and access
        let driver = "duckdb".to_string();
        let conn_str = "./test.db".to_string();

        let params = ConnectionParams {
            driver: driver.clone(),
            connection_string: conn_str.clone(),
        };

        assert_eq!(params.driver, driver);
        assert_eq!(params.connection_string, conn_str);

        // Test that clone works correctly (used by connection_params())
        let cloned = params.clone();
        assert_eq!(cloned.driver, driver);
        assert_eq!(cloned.connection_string, conn_str);
    }
}
