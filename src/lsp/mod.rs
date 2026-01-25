//! Language Server Protocol implementation for Mantis DSL

pub mod analysis;
pub mod backend;
pub mod capabilities;
// pub mod project;  // Archived - needs rebuilding with new DSL loader
pub mod transport;

// Re-export main entry points
pub use backend::LspBackend;
// pub use project::ProjectState;  // Archived
pub use transport::{run_stdio, run_websocket};
