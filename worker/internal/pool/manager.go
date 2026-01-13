// Package pool provides connection pool management for database connections.
package pool

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Config holds connection pool configuration options.
type Config struct {
	// MaxIdleConns is the maximum number of idle connections in the pool.
	MaxIdleConns int `json:"max_idle_conns"`

	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns int `json:"max_open_conns"`

	// ConnMaxLifetime is the maximum amount of time a connection may be reused.
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`

	// ConnMaxIdleTime is the maximum amount of time a connection may be idle.
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time"`
}

// DefaultConfig returns a sensible default pool configuration.
func DefaultConfig() Config {
	return Config{
		MaxIdleConns:    5,
		MaxOpenConns:    10,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 1 * time.Minute,
	}
}

// poolEntry holds a database connection pool and its metadata.
type poolEntry struct {
	db        *sql.DB
	driver    string
	createdAt time.Time
}

// Manager manages a collection of database connection pools.
// It creates one pool per unique (driver, connection string) combination.
type Manager struct {
	mu     sync.RWMutex
	pools  map[string]*poolEntry // keyed by driver + connection string hash
	config Config
	opener DBOpener // interface for opening database connections (allows testing)
}

// DBOpener is an interface for opening database connections.
// This allows for mocking in tests.
type DBOpener interface {
	Open(driver, connStr string) (*sql.DB, error)
}

// defaultDBOpener uses sql.Open to open database connections.
type defaultDBOpener struct{}

func (d *defaultDBOpener) Open(driver, connStr string) (*sql.DB, error) {
	return sql.Open(driver, connStr)
}

// NewManager creates a new connection pool manager with the given configuration.
func NewManager(config Config) *Manager {
	return &Manager{
		pools:  make(map[string]*poolEntry),
		config: config,
		opener: &defaultDBOpener{},
	}
}

// NewManagerWithOpener creates a new connection pool manager with a custom DB opener.
// This is primarily useful for testing.
func NewManagerWithOpener(config Config, opener DBOpener) *Manager {
	return &Manager{
		pools:  make(map[string]*poolEntry),
		config: config,
		opener: opener,
	}
}

// hashConnString creates a short hash of the connection string for use as a map key.
// This avoids storing sensitive connection strings as map keys.
func hashConnString(connStr string) string {
	h := sha256.Sum256([]byte(connStr))
	return hex.EncodeToString(h[:8]) // First 8 bytes for shorter key
}

// makeKey creates a unique key for a (driver, connection string) pair.
func makeKey(driver, connStr string) string {
	return driver + ":" + hashConnString(connStr)
}

// GetConnection returns a database connection from the pool, creating a new pool
// if necessary. The returned connection is from a pool and should not be closed
// by the caller.
func (m *Manager) GetConnection(ctx context.Context, driver, connStr string) (*sql.DB, error) {
	key := makeKey(driver, connStr)

	// Fast path: check if pool exists with read lock
	m.mu.RLock()
	if entry, ok := m.pools[key]; ok {
		m.mu.RUnlock()
		// Verify connection is still alive
		if err := entry.db.PingContext(ctx); err == nil {
			return entry.db, nil
		}
		// Connection dead, need to recreate (fall through to slow path)
	} else {
		m.mu.RUnlock()
	}

	// Slow path: create new pool with write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	if entry, ok := m.pools[key]; ok {
		if err := entry.db.PingContext(ctx); err == nil {
			return entry.db, nil
		}
		// Close dead pool
		entry.db.Close()
		delete(m.pools, key)
	}

	// Create new pool
	db, err := m.opener.Open(driver, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure pool settings
	db.SetMaxIdleConns(m.config.MaxIdleConns)
	db.SetMaxOpenConns(m.config.MaxOpenConns)
	db.SetConnMaxLifetime(m.config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(m.config.ConnMaxIdleTime)

	// Verify connection works
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	m.pools[key] = &poolEntry{
		db:        db,
		driver:    driver,
		createdAt: time.Now(),
	}

	return db, nil
}

// Close closes all connection pools managed by this manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for key, entry := range m.pools {
		if err := entry.db.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close pool %s: %w", key, err)
		}
		delete(m.pools, key)
	}
	return lastErr
}

// CloseConnection closes a specific connection pool identified by driver and connection string.
func (m *Manager) CloseConnection(driver, connStr string) error {
	key := makeKey(driver, connStr)

	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.pools[key]; ok {
		err := entry.db.Close()
		delete(m.pools, key)
		return err
	}
	return nil
}

// PoolStats contains statistics about a connection pool.
type PoolStats struct {
	Driver    string       `json:"driver"`
	CreatedAt time.Time    `json:"created_at"`
	Stats     sql.DBStats  `json:"stats"`
}

// Stats returns statistics about all managed connection pools.
func (m *Manager) Stats() map[string]PoolStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]PoolStats)
	for key, entry := range m.pools {
		stats[key] = PoolStats{
			Driver:    entry.driver,
			CreatedAt: entry.createdAt,
			Stats:     entry.db.Stats(),
		}
	}
	return stats
}

// PoolCount returns the number of active connection pools.
func (m *Manager) PoolCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pools)
}

// HasPool returns true if a pool exists for the given driver and connection string.
func (m *Manager) HasPool(driver, connStr string) bool {
	key := makeKey(driver, connStr)

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.pools[key]
	return ok
}
