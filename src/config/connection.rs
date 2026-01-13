//! Database connection configuration.
//!
//! Supports configuration via environment variables:
//! - `MANTIS_DB_DRIVER`: Database driver (mssql, duckdb)
//! - `MANTIS_DB_HOST`: Database server hostname
//! - `MANTIS_DB_NAME`: Database name
//! - `MANTIS_DB_PORT`: Port (optional, uses driver default)

use std::env;

/// Error type for connection configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),

    #[error("Unsupported driver: {0}. Supported: mssql, duckdb")]
    UnsupportedDriver(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Supported database drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Driver {
    /// Microsoft SQL Server
    MsSql,
    /// DuckDB (file or in-memory)
    DuckDb,
}

impl Driver {
    /// Parse driver from string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, ConnectionError> {
        match s.to_lowercase().as_str() {
            "mssql" | "sqlserver" | "sql_server" => Ok(Driver::MsSql),
            "duckdb" | "duck" => Ok(Driver::DuckDb),
            other => Err(ConnectionError::UnsupportedDriver(other.to_string())),
        }
    }

    /// Get the driver name for the worker.
    pub fn as_str(&self) -> &'static str {
        match self {
            Driver::MsSql => "mssql",
            Driver::DuckDb => "duckdb",
        }
    }

    /// Get the default port for this driver.
    pub fn default_port(&self) -> u16 {
        match self {
            Driver::MsSql => 1433,
            Driver::DuckDb => 0, // Not applicable
        }
    }
}

/// Database connection configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Database driver.
    pub driver: Driver,
    /// Server hostname.
    pub host: String,
    /// Database name.
    pub database: String,
    /// Port (optional).
    pub port: Option<u16>,
    /// Use Windows trusted connection (SQL Server).
    pub trusted_connection: bool,
    /// Username (if not using trusted connection).
    pub username: Option<String>,
    /// Password (if not using trusted connection).
    pub password: Option<String>,
}

impl ConnectionConfig {
    /// Create a new connection config for SQL Server with trusted connection.
    pub fn mssql_trusted(host: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            driver: Driver::MsSql,
            host: host.into(),
            database: database.into(),
            port: None,
            trusted_connection: true,
            username: None,
            password: None,
        }
    }

    /// Create a new connection config for DuckDB.
    pub fn duckdb(path: impl Into<String>) -> Self {
        Self {
            driver: Driver::DuckDb,
            host: path.into(), // For DuckDB, "host" is the file path
            database: String::new(),
            port: None,
            trusted_connection: false,
            username: None,
            password: None,
        }
    }

    /// Load configuration from environment variables.
    ///
    /// Required:
    /// - `MANTIS_DB_DRIVER`: mssql or duckdb
    /// - `MANTIS_DB_HOST`: Server hostname (or file path for DuckDB)
    /// - `MANTIS_DB_NAME`: Database name (not required for DuckDB)
    ///
    /// Optional:
    /// - `MANTIS_DB_PORT`: Server port
    /// - `MANTIS_DB_USER`: Username (if not using trusted connection)
    /// - `MANTIS_DB_PASSWORD`: Password (if not using trusted connection)
    pub fn from_env() -> Result<Self, ConnectionError> {
        let driver_str = env::var("MANTIS_DB_DRIVER")
            .map_err(|_| ConnectionError::MissingEnvVar("MANTIS_DB_DRIVER".to_string()))?;

        let driver = Driver::from_str(&driver_str)?;

        let host = env::var("MANTIS_DB_HOST")
            .map_err(|_| ConnectionError::MissingEnvVar("MANTIS_DB_HOST".to_string()))?;

        // Database name is required for SQL Server, optional for DuckDB
        let database = match driver {
            Driver::MsSql => env::var("MANTIS_DB_NAME")
                .map_err(|_| ConnectionError::MissingEnvVar("MANTIS_DB_NAME".to_string()))?,
            Driver::DuckDb => env::var("MANTIS_DB_NAME").unwrap_or_default(),
        };

        let port = env::var("MANTIS_DB_PORT")
            .ok()
            .and_then(|p| p.parse().ok());

        let username = env::var("MANTIS_DB_USER").ok();
        let password = env::var("MANTIS_DB_PASSWORD").ok();

        // Use trusted connection if no username/password provided (SQL Server only)
        let trusted_connection = driver == Driver::MsSql && username.is_none();

        Ok(Self {
            driver,
            host,
            database,
            port,
            trusted_connection,
            username,
            password,
        })
    }

    /// Build the connection string for the worker.
    pub fn to_connection_string(&self) -> String {
        match self.driver {
            Driver::MsSql => self.build_mssql_connection_string(),
            Driver::DuckDb => self.build_duckdb_connection_string(),
        }
    }

    fn build_mssql_connection_string(&self) -> String {
        let mut parts = [format!("sqlserver://{}", self.host)];

        // Add port if specified
        if let Some(port) = self.port {
            parts[0] = format!("sqlserver://{}:{}", self.host, port);
        }

        // Query parameters
        let mut params = vec![format!("database={}", self.database)];

        if self.trusted_connection {
            params.push("trusted_connection=true".to_string());
        } else if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            params.push(format!("user id={}", user));
            params.push(format!("password={}", pass));
        }

        format!("{}?{}", parts[0], params.join("&"))
    }

    fn build_duckdb_connection_string(&self) -> String {
        // For DuckDB, the connection string is just the file path
        // or ":memory:" for in-memory database
        if self.host.is_empty() || self.host == ":memory:" {
            ":memory:".to_string()
        } else {
            self.host.clone()
        }
    }

    /// Get the driver name for the worker.
    pub fn driver_name(&self) -> &'static str {
        self.driver.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mssql_trusted_connection() {
        let config = ConnectionConfig::mssql_trusted("localhost", "mydb");
        let conn_str = config.to_connection_string();

        assert!(conn_str.contains("sqlserver://localhost"));
        assert!(conn_str.contains("database=mydb"));
        assert!(conn_str.contains("trusted_connection=true"));
    }

    #[test]
    fn test_mssql_with_port() {
        let mut config = ConnectionConfig::mssql_trusted("localhost", "mydb");
        config.port = Some(1434);

        let conn_str = config.to_connection_string();
        assert!(conn_str.contains("sqlserver://localhost:1434"));
    }

    #[test]
    fn test_duckdb_file() {
        let config = ConnectionConfig::duckdb("/path/to/db.duckdb");
        assert_eq!(config.to_connection_string(), "/path/to/db.duckdb");
    }

    #[test]
    fn test_duckdb_memory() {
        let config = ConnectionConfig::duckdb(":memory:");
        assert_eq!(config.to_connection_string(), ":memory:");
    }

    #[test]
    fn test_driver_parsing() {
        assert_eq!(Driver::from_str("mssql").unwrap(), Driver::MsSql);
        assert_eq!(Driver::from_str("sqlserver").unwrap(), Driver::MsSql);
        assert_eq!(Driver::from_str("duckdb").unwrap(), Driver::DuckDb);
        assert!(Driver::from_str("postgres").is_err());
    }
}
