//! SQLite-based metadata cache.
//!
//! Provides persistent caching of database metadata to avoid repeated
//! introspection calls. The cache is stored in `~/.mantis/cache.db`.
//!
//! # Design
//!
//! - Simple key-value store with JSON values
//! - No TTL - cache persists until manually cleared
//! - Versioned - auto-clears on version mismatch
//!
//! # Key Format
//!
//! ```text
//! {conn_hash}:schemas                     -> ["main", "analytics", ...]
//! {conn_hash}:tables:{schema}             -> [TableInfo, ...]
//! {conn_hash}:metadata:{schema}.{table}   -> TableMetadata
//! {conn_hash}:fks:{schema}.{table}        -> [ForeignKeyInfo, ...]
//! {conn_hash}:stats:{schema}.{table}.{col}-> ColumnStats
//! ```

mod hash;
pub use hash::compute_hash;

use std::path::PathBuf;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::crypto;

/// Current cache schema version. Bump this when the cache format changes.
const CACHE_VERSION: i32 = 2;

/// Errors that can occur during cache operations.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Failed to determine cache directory")]
    NoCacheDir,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Crypto error: {0}")]
    Crypto(String),
}

pub type CacheResult<T> = Result<T, CacheError>;

/// A saved credential (metadata only, no secrets).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedCredential {
    pub id: String,
    pub driver: String,
    pub display_name: Option<String>,
    pub last_used_at: i64,
}

/// SQLite-based metadata cache.
pub struct MetadataCache {
    conn: Connection,
}

impl MetadataCache {
    /// Open or create the cache database.
    ///
    /// The cache is stored at `~/.mantis/cache.db`.
    /// If the cache version doesn't match, it's automatically cleared.
    pub fn open() -> CacheResult<Self> {
        let path = Self::cache_path()?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&path)?;
        let cache = Self { conn };
        cache.init()?;

        Ok(cache)
    }

    /// Open an in-memory cache (for testing).
    pub fn open_in_memory() -> CacheResult<Self> {
        let conn = Connection::open_in_memory()?;
        let cache = Self { conn };
        cache.init()?;
        Ok(cache)
    }

    /// Get the path to the cache database.
    pub fn cache_path() -> CacheResult<PathBuf> {
        let base = dirs::home_dir().ok_or(CacheError::NoCacheDir)?;
        Ok(base.join(".mantis").join("cache.db"))
    }

    /// Initialize the cache schema and check version.
    fn init(&self) -> CacheResult<()> {
        // Create tables if they don't exist
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS credentials (
                id TEXT PRIMARY KEY,
                driver TEXT NOT NULL,
                display_name TEXT,
                connection_string_encrypted TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL
            );
            ",
        )?;

        // Check version
        let stored_version: Option<i32> = self
            .conn
            .query_row("SELECT value FROM meta WHERE key = 'version'", [], |row| {
                let s: String = row.get(0)?;
                Ok(s.parse().unwrap_or(0))
            })
            .optional()?;

        match stored_version {
            Some(v) if v == CACHE_VERSION => {
                // Version matches, cache is valid
            }
            Some(_) => {
                // Version mismatch, clear cache
                self.clear_all()?;
                self.set_version()?;
            }
            None => {
                // No version set, initialize
                self.set_version()?;
            }
        }

        Ok(())
    }

    /// Set the cache version in metadata.
    fn set_version(&self) -> CacheResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
            params![CACHE_VERSION.to_string()],
        )?;
        Ok(())
    }

    /// Get a value from the cache.
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> CacheResult<Option<T>> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM cache WHERE key = ?",
                params![key],
                |row| row.get(0),
            )
            .optional()?;

        match json {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    /// Set a value in the cache.
    pub fn set<T: Serialize>(&self, key: &str, value: &T) -> CacheResult<()> {
        let json = serde_json::to_string(value)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO cache (key, value) VALUES (?, ?)",
            params![key, json],
        )?;
        Ok(())
    }

    /// Delete a value from the cache.
    pub fn delete(&self, key: &str) -> CacheResult<bool> {
        let rows = self
            .conn
            .execute("DELETE FROM cache WHERE key = ?", params![key])?;
        Ok(rows > 0)
    }

    /// Delete all entries matching a key prefix.
    pub fn delete_prefix(&self, prefix: &str) -> CacheResult<usize> {
        let pattern = format!("{}%", prefix);
        let rows = self
            .conn
            .execute("DELETE FROM cache WHERE key LIKE ?", params![pattern])?;
        Ok(rows)
    }

    /// Clear all cache entries (but keep metadata).
    pub fn clear_all(&self) -> CacheResult<()> {
        self.conn.execute("DELETE FROM cache", [])?;
        Ok(())
    }

    /// Clear cache entries for a specific connection.
    pub fn clear_connection(&self, conn_hash: &str) -> CacheResult<usize> {
        self.delete_prefix(&format!("{}:", conn_hash))
    }

    /// List all keys matching a prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> CacheResult<Vec<String>> {
        let pattern = format!("{}%", prefix);
        let mut stmt = self
            .conn
            .prepare("SELECT key FROM cache WHERE key LIKE ?")?;
        let keys = stmt
            .query_map(params![pattern], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(keys)
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheResult<CacheStats> {
        let entry_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM cache", [], |row| row.get(0))?;

        let total_size: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(LENGTH(value)), 0) FROM cache",
            [],
            |row| row.get(0),
        )?;

        Ok(CacheStats {
            entry_count: entry_count as usize,
            total_size_bytes: total_size as usize,
        })
    }

    // ===== Credential Storage Methods =====

    /// Save a credential with encrypted connection string.
    ///
    /// Returns the generated credential ID.
    pub fn save_credential(
        &self,
        master_key: &[u8; 32],
        driver: &str,
        connection_string: &str,
        display_name: Option<&str>,
    ) -> CacheResult<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let encrypted = crypto::encrypt(master_key, connection_string.as_bytes())
            .map_err(|e| CacheError::Crypto(e.to_string()))?;

        self.conn.execute(
            "INSERT INTO credentials (id, driver, display_name, connection_string_encrypted, created_at, last_used_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![id, driver, display_name, encrypted, now, now],
        )?;

        Ok(id)
    }

    /// List all saved credentials (metadata only, no secrets).
    pub fn list_credentials(&self) -> CacheResult<Vec<SavedCredential>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, driver, display_name, last_used_at FROM credentials ORDER BY last_used_at DESC",
        )?;

        let credentials = stmt
            .query_map([], |row| {
                Ok(SavedCredential {
                    id: row.get(0)?,
                    driver: row.get(1)?,
                    display_name: row.get(2)?,
                    last_used_at: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(credentials)
    }

    /// Get and decrypt a credential's connection string.
    ///
    /// Updates the last_used_at timestamp on success.
    /// If decryption fails, deletes the stale entry and returns None.
    pub fn get_credential_connection_string(
        &self,
        master_key: &[u8; 32],
        id: &str,
    ) -> CacheResult<Option<String>> {
        let encrypted: Option<String> = self
            .conn
            .query_row(
                "SELECT connection_string_encrypted FROM credentials WHERE id = ?",
                params![id],
                |row| row.get(0),
            )
            .optional()?;

        let Some(encrypted) = encrypted else {
            return Ok(None);
        };

        match crypto::decrypt(master_key, &encrypted) {
            Ok(decrypted) => {
                // Update last_used_at
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;

                self.conn.execute(
                    "UPDATE credentials SET last_used_at = ? WHERE id = ?",
                    params![now, id],
                )?;

                let conn_str =
                    String::from_utf8(decrypted).map_err(|e| CacheError::Crypto(e.to_string()))?;
                Ok(Some(conn_str))
            }
            Err(_) => {
                // Decryption failed - delete stale entry
                self.delete_credential(id)?;
                Ok(None)
            }
        }
    }

    /// Delete a saved credential.
    ///
    /// Returns true if a credential was deleted.
    pub fn delete_credential(&self, id: &str) -> CacheResult<bool> {
        let rows = self
            .conn
            .execute("DELETE FROM credentials WHERE id = ?", params![id])?;
        Ok(rows > 0)
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of entries in the cache.
    pub entry_count: usize,
    /// Total size of all values in bytes.
    pub total_size_bytes: usize,
}

/// Helper for generating cache keys.
pub struct CacheKey;

impl CacheKey {
    /// Hash a connection string for use in cache keys.
    pub fn hash_connection(driver: &str, conn_str: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        driver.hash(&mut hasher);
        conn_str.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Key for list of schemas.
    pub fn schemas(conn_hash: &str) -> String {
        format!("{}:schemas", conn_hash)
    }

    /// Key for list of tables in a schema.
    pub fn tables(conn_hash: &str, schema: &str) -> String {
        format!("{}:tables:{}", conn_hash, schema)
    }

    /// Key for table metadata.
    pub fn table_metadata(conn_hash: &str, schema: &str, table: &str) -> String {
        format!("{}:metadata:{}.{}", conn_hash, schema, table)
    }

    /// Key for foreign keys.
    pub fn foreign_keys(conn_hash: &str, schema: &str, table: &str) -> String {
        format!("{}:fks:{}.{}", conn_hash, schema, table)
    }

    /// Key for column statistics.
    pub fn column_stats(conn_hash: &str, schema: &str, table: &str, column: &str) -> String {
        format!("{}:stats:{}.{}.{}", conn_hash, schema, table, column)
    }

    /// Key for database info.
    pub fn database_info(conn_hash: &str) -> String {
        format!("{}:dbinfo", conn_hash)
    }

    /// Key for value overlap between two columns.
    pub fn value_overlap(
        conn_hash: &str,
        from_schema: &str,
        from_table: &str,
        from_column: &str,
        to_schema: &str,
        to_table: &str,
        to_column: &str,
    ) -> String {
        format!(
            "{}:overlap:{}.{}.{}->{}.{}.{}",
            conn_hash, from_schema, from_table, from_column, to_schema, to_table, to_column
        )
    }

    /// Key for column lineage graph.
    ///
    /// The `model_hash` should be a content hash of the model definition
    /// to invalidate the cache when the model changes.
    pub fn lineage(model_hash: &str) -> String {
        format!("lineage:{}", model_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_open_in_memory() {
        let cache = MetadataCache::open_in_memory().unwrap();
        let stats = cache.stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_cache_get_set() {
        let cache = MetadataCache::open_in_memory().unwrap();

        // Set a value
        cache.set("test:key", &vec!["a", "b", "c"]).unwrap();

        // Get it back
        let value: Option<Vec<String>> = cache.get("test:key").unwrap();
        assert_eq!(
            value,
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );

        // Get non-existent key
        let missing: Option<String> = cache.get("nonexistent").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_cache_delete() {
        let cache = MetadataCache::open_in_memory().unwrap();

        cache.set("test:key", &"value").unwrap();
        assert!(cache.get::<String>("test:key").unwrap().is_some());

        let deleted = cache.delete("test:key").unwrap();
        assert!(deleted);
        assert!(cache.get::<String>("test:key").unwrap().is_none());

        // Delete non-existent
        let deleted = cache.delete("nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_cache_delete_prefix() {
        let cache = MetadataCache::open_in_memory().unwrap();

        cache.set("conn1:tables:main", &"t1").unwrap();
        cache.set("conn1:tables:analytics", &"t2").unwrap();
        cache.set("conn1:metadata:main.orders", &"m1").unwrap();
        cache.set("conn2:tables:main", &"t3").unwrap();

        // Delete all conn1 entries
        let deleted = cache.delete_prefix("conn1:").unwrap();
        assert_eq!(deleted, 3);

        // conn2 should still exist
        assert!(cache.get::<String>("conn2:tables:main").unwrap().is_some());
    }

    #[test]
    fn test_cache_keys_with_prefix() {
        let cache = MetadataCache::open_in_memory().unwrap();

        cache.set("abc:1", &"v1").unwrap();
        cache.set("abc:2", &"v2").unwrap();
        cache.set("def:1", &"v3").unwrap();

        let keys = cache.keys_with_prefix("abc:").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"abc:1".to_string()));
        assert!(keys.contains(&"abc:2".to_string()));
    }

    #[test]
    fn test_cache_key_helpers() {
        let conn = CacheKey::hash_connection("duckdb", "./test.db");
        assert!(!conn.is_empty());

        let schemas = CacheKey::schemas(&conn);
        assert!(schemas.contains(":schemas"));

        let tables = CacheKey::tables(&conn, "main");
        assert!(tables.contains(":tables:main"));

        let metadata = CacheKey::table_metadata(&conn, "main", "orders");
        assert!(metadata.contains(":metadata:main.orders"));

        let lineage = CacheKey::lineage("abc123hash");
        assert_eq!(lineage, "lineage:abc123hash");
    }

    #[test]
    fn test_cache_stats() {
        let cache = MetadataCache::open_in_memory().unwrap();

        cache.set("key1", &"short").unwrap();
        cache.set("key2", &"a longer string value").unwrap();

        let stats = cache.stats().unwrap();
        assert_eq!(stats.entry_count, 2);
        assert!(stats.total_size_bytes > 0);
    }

    #[test]
    fn test_credentials_crud() {
        let cache = MetadataCache::open_in_memory().unwrap();
        let key = [0u8; 32]; // Test key

        // Save credential
        let id = cache
            .save_credential(
                &key,
                "mssql",
                "server=localhost;database=test",
                Some("Production"),
            )
            .unwrap();
        assert!(!id.is_empty());

        // List credentials
        let creds = cache.list_credentials().unwrap();
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].driver, "mssql");
        assert_eq!(creds[0].display_name, Some("Production".to_string()));

        // Get connection string
        let conn_str = cache.get_credential_connection_string(&key, &id).unwrap();
        assert_eq!(conn_str, Some("server=localhost;database=test".to_string()));

        // Delete
        let deleted = cache.delete_credential(&id).unwrap();
        assert!(deleted);
        assert!(cache.list_credentials().unwrap().is_empty());
    }
}
