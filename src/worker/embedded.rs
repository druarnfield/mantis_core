//! Embedded worker binary support.
//!
//! This module provides functionality to extract and run an embedded Go worker
//! binary. The binary is embedded at compile time using `include_bytes!`.
//!
//! # Build Requirements
//!
//! To embed the worker binary:
//! 1. Build the Go worker: `cd worker && go build -o mantis-worker ./cmd/worker`
//! 2. Build Rust with the `embedded-worker` feature: `cargo build --features embedded-worker`
//!
//! If the binary is not available at compile time, the feature will fail to compile.

use std::path::PathBuf;

use super::error::{WorkerError, WorkerResult};

/// Embedded worker binary (included at compile time).
#[cfg(feature = "embedded-worker")]
static EMBEDDED_WORKER: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/mantis-worker"));

/// Get the path where the embedded worker should be extracted.
#[cfg(feature = "embedded-worker")]
fn get_extraction_path() -> WorkerResult<PathBuf> {
    use std::fs;

    // Use a cache directory so we don't extract on every run
    let cache_dir = dirs::cache_dir()
        .ok_or_else(|| WorkerError::SpawnFailed(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not determine cache directory",
        )))?;

    let mantis_cache = cache_dir.join("mantis");
    fs::create_dir_all(&mantis_cache).map_err(WorkerError::SpawnFailed)?;

    // Include version in path for cache invalidation
    let version = env!("CARGO_PKG_VERSION");
    let worker_path = mantis_cache.join(format!("mantis-worker-{}", version));

    Ok(worker_path)
}

/// Extract the embedded worker binary to a temporary location.
///
/// Returns the path to the extracted binary.
///
/// # Caching
///
/// The binary is cached in the user's cache directory. If it already exists
/// and has the correct size, it will be reused.
#[cfg(feature = "embedded-worker")]
pub fn extract_worker() -> WorkerResult<PathBuf> {
    use std::fs;
    use std::io::Write;

    let worker_path = get_extraction_path()?;

    // Check if already extracted with correct size
    if worker_path.exists() {
        if let Ok(metadata) = fs::metadata(&worker_path) {
            if metadata.len() as usize == EMBEDDED_WORKER.len() {
                return Ok(worker_path);
            }
        }
    }

    // Extract the binary
    let mut file = fs::File::create(&worker_path).map_err(WorkerError::SpawnFailed)?;
    file.write_all(EMBEDDED_WORKER).map_err(WorkerError::SpawnFailed)?;

    // Set executable permissions (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = file.metadata().map_err(WorkerError::SpawnFailed)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&worker_path, permissions).map_err(WorkerError::SpawnFailed)?;
    }

    Ok(worker_path)
}

/// Extract the embedded worker binary (stub for when feature is disabled).
#[cfg(not(feature = "embedded-worker"))]
pub fn extract_worker() -> WorkerResult<PathBuf> {
    Err(WorkerError::SpawnFailed(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "Embedded worker not available. Build with --features embedded-worker or specify worker path.",
    )))
}

/// Check if the embedded worker is available.
#[cfg(feature = "embedded-worker")]
pub fn is_embedded_available() -> bool {
    true
}

/// Check if the embedded worker is available (stub for when feature is disabled).
#[cfg(not(feature = "embedded-worker"))]
pub fn is_embedded_available() -> bool {
    false
}

/// Clean up extracted worker binaries from the cache.
pub fn cleanup_cache() -> WorkerResult<()> {
    use std::fs;

    let cache_dir = dirs::cache_dir()
        .ok_or_else(|| WorkerError::SpawnFailed(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not determine cache directory",
        )))?;

    let mantis_cache = cache_dir.join("mantis");
    if mantis_cache.exists() {
        // Remove old worker binaries (keep current version)
        let current_version = env!("CARGO_PKG_VERSION");
        let current_name = format!("mantis-worker-{}", current_version);

        for entry in fs::read_dir(&mantis_cache).map_err(WorkerError::SpawnFailed)? {
            let entry = entry.map_err(WorkerError::SpawnFailed)?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.starts_with("mantis-worker-") && name_str != current_name {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "embedded-worker")]
    fn test_get_extraction_path() {
        let path = get_extraction_path().unwrap();
        assert!(path.to_string_lossy().contains("mantis-worker"));
    }

    #[test]
    fn test_is_embedded_available() {
        // Will be false unless built with the feature
        let available = is_embedded_available();
        #[cfg(feature = "embedded-worker")]
        assert!(available);
        #[cfg(not(feature = "embedded-worker"))]
        assert!(!available);
    }
}
