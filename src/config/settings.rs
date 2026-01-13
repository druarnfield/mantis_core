//! TOML-based configuration for Mantis.
//!
//! Supports a config file (mantis.toml) with environment variable expansion.
//!
//! Example configuration:
//! ```toml
//! [connections.production]
//! driver = "mssql"
//! connection_string = "${PROD_DB_CONNECTION_STRING}"
//!
//! [connections.dev]
//! driver = "duckdb"
//! connection_string = "./data/dev.duckdb"
//!
//! [worker]
//! embedded = true  # Use embedded worker binary
//!
//! [worker.pool]
//! max_idle_conns = 5
//! max_open_conns = 10
//! conn_max_lifetime = "5m"
//! conn_max_idle_time = "1m"
//!
//! [metadata]
//! cache_enabled = true
//! cache_ttl_seconds = 3600
//!
//! [metadata.inference]
//! enabled = true
//! min_confidence = 0.7
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use super::connection::Driver;

/// Error type for settings.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    #[error("Config file not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Failed to read config file: {0}")]
    ReadError(#[from] std::io::Error),

    #[error("Failed to parse config file: {0}")]
    ParseError(#[from] toml::de::Error),

    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),

    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    #[error("Invalid duration format: {0}")]
    InvalidDuration(String),

    #[error("Unsupported driver: {0}")]
    UnsupportedDriver(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Root configuration structure.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
#[derive(Default)]
pub struct Settings {
    /// Named database connections.
    #[serde(default)]
    pub connections: HashMap<String, ConnectionSettings>,

    /// Worker configuration.
    #[serde(default)]
    pub worker: WorkerSettings,

    /// Metadata configuration.
    #[serde(default)]
    pub metadata: MetadataSettings,
}


/// Connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConnectionSettings {
    /// Database driver (mssql, duckdb).
    pub driver: String,

    /// Connection string (supports ${ENV_VAR} expansion).
    pub connection_string: String,

    /// Default schema for this connection.
    #[serde(default)]
    pub default_schema: Option<String>,
}

impl ConnectionSettings {
    /// Get the driver type.
    pub fn driver_type(&self) -> Result<Driver, SettingsError> {
        Driver::from_str(&self.driver)
            .map_err(|_| SettingsError::UnsupportedDriver(self.driver.clone()))
    }

    /// Get the connection string with environment variables expanded.
    pub fn resolved_connection_string(&self) -> Result<String, SettingsError> {
        expand_env_vars(&self.connection_string)
    }
}

/// Worker configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct WorkerSettings {
    /// Path to worker binary (if not using embedded).
    pub path: Option<String>,

    /// Use embedded worker binary.
    pub embedded: bool,

    /// Connection pool settings.
    pub pool: PoolSettings,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            path: None,
            embedded: true,
            pool: PoolSettings::default(),
        }
    }
}

/// Connection pool settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PoolSettings {
    /// Maximum number of idle connections per pool.
    pub max_idle_conns: u32,

    /// Maximum number of open connections per pool.
    pub max_open_conns: u32,

    /// Maximum connection lifetime (e.g., "5m", "1h").
    pub conn_max_lifetime: String,

    /// Maximum connection idle time (e.g., "1m", "30s").
    pub conn_max_idle_time: String,
}

impl Default for PoolSettings {
    fn default() -> Self {
        Self {
            max_idle_conns: 5,
            max_open_conns: 10,
            conn_max_lifetime: "5m".to_string(),
            conn_max_idle_time: "1m".to_string(),
        }
    }
}

impl PoolSettings {
    /// Convert to worker command-line arguments.
    pub fn to_worker_args(&self) -> Vec<String> {
        vec![
            "-pool".to_string(),
            format!("-pool-max-idle={}", self.max_idle_conns),
            format!("-pool-max-open={}", self.max_open_conns),
            format!("-pool-conn-lifetime={}", self.conn_max_lifetime),
            format!("-pool-conn-idle={}", self.conn_max_idle_time),
        ]
    }
}

/// Metadata configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MetadataSettings {
    /// Enable metadata caching.
    pub cache_enabled: bool,

    /// Cache TTL in seconds.
    pub cache_ttl_seconds: u64,

    /// Introspection mode: "batch", "on_demand", "disabled".
    pub introspection_mode: String,

    /// Inference settings.
    pub inference: InferenceSettings,

    /// Default settings.
    pub defaults: MetadataDefaults,
}

impl Default for MetadataSettings {
    fn default() -> Self {
        Self {
            cache_enabled: true,
            cache_ttl_seconds: 3600,
            introspection_mode: "on_demand".to_string(),
            inference: InferenceSettings::default(),
            defaults: MetadataDefaults::default(),
        }
    }
}

/// Inference settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct InferenceSettings {
    /// Enable relationship inference.
    pub enabled: bool,

    /// Minimum confidence threshold (0.0 to 1.0).
    pub min_confidence: f64,

    /// Enabled inference rules.
    pub rules: Vec<String>,
}

impl Default for InferenceSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            min_confidence: 0.7,
            rules: vec![
                "suffix_id".to_string(),
                "suffix_key".to_string(),
                "fk_prefix".to_string(),
                "pk_match".to_string(),
                "same_column_name".to_string(),
            ],
        }
    }
}

/// Default metadata settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MetadataDefaults {
    /// Default schema name.
    pub default_schema: String,
}

impl Default for MetadataDefaults {
    fn default() -> Self {
        Self {
            default_schema: "dbo".to_string(),
        }
    }
}

impl Settings {
    /// Load settings from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, SettingsError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(SettingsError::FileNotFound(path.to_path_buf()));
        }

        let content = fs::read_to_string(path)?;
        let settings: Settings = toml::from_str(&content)?;
        Ok(settings)
    }

    /// Load settings from the default config file locations.
    ///
    /// Searches in order:
    /// 1. `./mantis.toml`
    /// 2. `~/.config/mantis/config.toml`
    /// 3. Environment variable `MANTIS_CONFIG`
    pub fn load() -> Result<Self, SettingsError> {
        // Check environment variable first
        if let Ok(path) = env::var("MANTIS_CONFIG") {
            return Self::from_file(&path);
        }

        // Check local directory
        let local_config = PathBuf::from("mantis.toml");
        if local_config.exists() {
            return Self::from_file(&local_config);
        }

        // Check user config directory
        if let Some(config_dir) = dirs::config_dir() {
            let user_config = config_dir.join("mantis").join("config.toml");
            if user_config.exists() {
                return Self::from_file(&user_config);
            }
        }

        // Return defaults if no config file found
        Ok(Settings::default())
    }

    /// Get a connection by name.
    pub fn get_connection(&self, name: &str) -> Result<&ConnectionSettings, SettingsError> {
        self.connections
            .get(name)
            .ok_or_else(|| SettingsError::ConnectionNotFound(name.to_string()))
    }

    /// Get the default connection (first one defined, or "default" if it exists).
    pub fn default_connection(&self) -> Option<(&str, &ConnectionSettings)> {
        // Check for explicit "default" connection first
        if let Some(conn) = self.connections.get("default") {
            return Some(("default", conn));
        }
        // Otherwise return the first connection
        self.connections.iter().next().map(|(k, v)| (k.as_str(), v))
    }

    /// Get the worker binary path.
    ///
    /// If embedded is true, returns None (caller should extract embedded binary).
    /// Otherwise returns the configured path or searches for it.
    pub fn worker_path(&self) -> Option<PathBuf> {
        if self.worker.embedded {
            return None;
        }

        if let Some(path) = &self.worker.path {
            let expanded = expand_env_vars(path).ok()?;
            return Some(PathBuf::from(expanded));
        }

        // Search common locations
        let candidates = [
            "mantis-worker",
            "./mantis-worker",
            "./worker/mantis-worker",
        ];

        for candidate in candidates {
            let path = PathBuf::from(candidate);
            if path.exists() {
                return Some(path);
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
                    return Some(PathBuf::from(path));
                }
            }
        }

        None
    }
}

/// Expand environment variables in a string.
///
/// Supports `${VAR}` and `$VAR` syntax.
pub fn expand_env_vars(s: &str) -> Result<String, SettingsError> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            // Check for ${VAR} or $VAR
            if chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let mut var_name = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == '}' {
                        chars.next(); // consume '}'
                        break;
                    }
                    var_name.push(chars.next().unwrap());
                }
                let value = env::var(&var_name)
                    .map_err(|_| SettingsError::MissingEnvVar(var_name.clone()))?;
                result.push_str(&value);
            } else {
                // $VAR (ends at non-alphanumeric/underscore)
                let mut var_name = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_alphanumeric() || ch == '_' {
                        var_name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if var_name.is_empty() {
                    // Just a lone $, keep it
                    result.push('$');
                } else {
                    let value = env::var(&var_name)
                        .map_err(|_| SettingsError::MissingEnvVar(var_name.clone()))?;
                    result.push_str(&value);
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_env_vars_braces() {
        env::set_var("TEST_VAR", "hello");
        assert_eq!(expand_env_vars("${TEST_VAR}").unwrap(), "hello");
        assert_eq!(expand_env_vars("prefix_${TEST_VAR}_suffix").unwrap(), "prefix_hello_suffix");
        env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_expand_env_vars_no_braces() {
        env::set_var("TEST_VAR2", "world");
        assert_eq!(expand_env_vars("$TEST_VAR2").unwrap(), "world");
        assert_eq!(expand_env_vars("$TEST_VAR2!").unwrap(), "world!");
        env::remove_var("TEST_VAR2");
    }

    #[test]
    fn test_expand_env_vars_missing() {
        let result = expand_env_vars("${NONEXISTENT_VAR_12345}");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_toml() {
        let toml = r#"
[connections.production]
driver = "mssql"
connection_string = "sqlserver://localhost?database=mydb"

[connections.dev]
driver = "duckdb"
connection_string = "./data/dev.duckdb"

[worker]
embedded = true

[worker.pool]
max_idle_conns = 10
max_open_conns = 20

[metadata]
cache_enabled = true
cache_ttl_seconds = 7200

[metadata.inference]
enabled = true
min_confidence = 0.8
"#;

        let settings: Settings = toml::from_str(toml).unwrap();

        assert_eq!(settings.connections.len(), 2);
        assert!(settings.connections.contains_key("production"));
        assert!(settings.connections.contains_key("dev"));

        let prod = &settings.connections["production"];
        assert_eq!(prod.driver, "mssql");

        assert!(settings.worker.embedded);
        assert_eq!(settings.worker.pool.max_idle_conns, 10);

        assert!(settings.metadata.cache_enabled);
        assert_eq!(settings.metadata.cache_ttl_seconds, 7200);
        assert_eq!(settings.metadata.inference.min_confidence, 0.8);
    }

    #[test]
    fn test_default_settings() {
        let settings = Settings::default();

        assert!(settings.worker.embedded);
        assert_eq!(settings.worker.pool.max_idle_conns, 5);
        assert!(settings.metadata.cache_enabled);
        assert!(settings.metadata.inference.enabled);
    }

    #[test]
    fn test_pool_args() {
        let pool = PoolSettings::default();
        let args = pool.to_worker_args();

        assert!(args.contains(&"-pool".to_string()));
        assert!(args.contains(&"-pool-max-idle=5".to_string()));
        assert!(args.contains(&"-pool-max-open=10".to_string()));
    }
}
