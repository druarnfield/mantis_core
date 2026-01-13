//! Build script for Mantis.
//!
//! This script handles:
//! 1. Building the Go worker binary (if embedded-worker feature is enabled)
//! 2. Copying the worker binary to OUT_DIR for embedding

fn main() {
    // Only build worker if embedded-worker feature is enabled
    #[cfg(feature = "embedded-worker")]
    build_embedded_worker();

    // Tell cargo to rerun if worker source changes
    println!("cargo:rerun-if-changed=worker/");
    println!("cargo:rerun-if-changed=build.rs");
}

#[cfg(feature = "embedded-worker")]
fn build_embedded_worker() {
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let worker_dir = manifest_dir.join("worker");
    let worker_binary = out_dir.join("mantis-worker");

    // Check if Go is available
    let go_available = Command::new("go")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !go_available {
        // Try to use pre-built binary
        let prebuilt_paths = [
            worker_dir.join("bin").join("mantis-worker"),
            worker_dir.join("cmd").join("worker").join("worker"),
            manifest_dir.join("mantis-worker"),
        ];

        for path in &prebuilt_paths {
            if path.exists() {
                fs::copy(path, &worker_binary).expect("Failed to copy pre-built worker binary");
                println!(
                    "cargo:warning=Using pre-built worker binary from {:?}",
                    path
                );
                return;
            }
        }

        panic!(
            "Cannot build embedded worker: Go is not installed and no pre-built binary found.\n\
             Either install Go or place a pre-built mantis-worker binary in the worker/ directory."
        );
    }

    // Build the Go worker
    println!("cargo:warning=Building Go worker binary...");

    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&worker_binary)
        .arg("./cmd/worker")
        .current_dir(&worker_dir)
        .status()
        .expect("Failed to execute go build");

    if !status.success() {
        panic!("Failed to build Go worker. Exit code: {:?}", status.code());
    }

    println!("cargo:warning=Worker binary built successfully");
}
