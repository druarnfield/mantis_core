//! Web server module for Mantis Playground UI
//!
//! Provides an embedded web interface for developing Mantis models.

#[cfg(feature = "ui")]
mod database;
#[cfg(feature = "ui")]
mod server;

#[cfg(feature = "ui")]
pub use database::*;
#[cfg(feature = "ui")]
pub use server::*;
