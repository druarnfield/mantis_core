//! Master key management.
//!
//! Key is either:
//! - Loaded from MANTIS_MASTER_KEY env var (persistent mode)
//! - Generated randomly at startup (ephemeral mode)

use std::sync::OnceLock;

static MASTER_KEY: OnceLock<MasterKeyState> = OnceLock::new();

/// Represents the master key state, including whether it's persistent.
#[derive(Debug, Clone)]
pub struct MasterKeyState {
    key: [u8; 32],
    persistent: bool,
}

impl MasterKeyState {
    /// Get the master key bytes.
    pub fn key(&self) -> &[u8; 32] {
        &self.key
    }

    /// Whether the key is persistent (from env var).
    pub fn is_persistent(&self) -> bool {
        self.persistent
    }
}

/// Initialize the master key.
///
/// Must be called once at startup. Returns the key state.
///
/// The key is loaded from the `MANTIS_MASTER_KEY` environment variable if set.
/// If the env var is not set or contains an invalid key, a random ephemeral key
/// is generated instead.
pub fn init() -> &'static MasterKeyState {
    MASTER_KEY.get_or_init(|| {
        match std::env::var("MANTIS_MASTER_KEY") {
            Ok(encoded) => {
                match super::decode_key(&encoded) {
                    Ok(key) => MasterKeyState { key, persistent: true },
                    Err(e) => {
                        eprintln!("Warning: Invalid MANTIS_MASTER_KEY, using ephemeral key: {}", e);
                        MasterKeyState {
                            key: super::generate_master_key(),
                            persistent: false,
                        }
                    }
                }
            }
            Err(_) => {
                MasterKeyState {
                    key: super::generate_master_key(),
                    persistent: false,
                }
            }
        }
    })
}

/// Get the current master key state.
///
/// # Panics
///
/// Panics if `init()` was not called.
pub fn get() -> &'static MasterKeyState {
    MASTER_KEY.get().expect("Master key not initialized. Call crypto::master_key::init() at startup.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_state_accessors() {
        let state = MasterKeyState {
            key: [1u8; 32],
            persistent: true,
        };
        assert_eq!(state.key().len(), 32);
        assert!(state.is_persistent());
    }

    #[test]
    fn test_key_state_ephemeral() {
        let state = MasterKeyState {
            key: [2u8; 32],
            persistent: false,
        };
        assert_eq!(state.key(), &[2u8; 32]);
        assert!(!state.is_persistent());
    }
}
