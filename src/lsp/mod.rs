//! Language Server Protocol implementation for Mantis Lua DSL

pub mod analysis;
pub mod backend;
pub mod capabilities;
pub mod project;
pub mod transport;

// Re-export main entry points
pub use backend::LspBackend;
pub use project::ProjectState;
pub use transport::{run_stdio, run_websocket};
