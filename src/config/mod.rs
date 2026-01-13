//! Configuration module for Mantis.
//!
//! Handles connection configuration, environment variables, and settings.

mod connection;
mod settings;

pub use connection::{ConnectionConfig, ConnectionError, Driver};
pub use settings::{
    expand_env_vars, ConnectionSettings, InferenceSettings, MetadataDefaults, MetadataSettings,
    PoolSettings, Settings, SettingsError, WorkerSettings,
};
