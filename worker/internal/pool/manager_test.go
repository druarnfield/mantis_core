package pool

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb" // Register DuckDB driver for tests
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxIdleConns != 5 {
		t.Errorf("MaxIdleConns = %d, want 5", cfg.MaxIdleConns)
	}
	if cfg.MaxOpenConns != 10 {
		t.Errorf("MaxOpenConns = %d, want 10", cfg.MaxOpenConns)
	}
	if cfg.ConnMaxLifetime != 5*time.Minute {
		t.Errorf("ConnMaxLifetime = %v, want 5m", cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime != 1*time.Minute {
		t.Errorf("ConnMaxIdleTime = %v, want 1m", cfg.ConnMaxIdleTime)
	}
}

func TestNewManager(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)

	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	if m.pools == nil {
		t.Error("pools map is nil")
	}
	if m.PoolCount() != 0 {
		t.Errorf("PoolCount() = %d, want 0", m.PoolCount())
	}
}

func TestHashConnString(t *testing.T) {
	// Same input should produce same output
	hash1 := hashConnString("connection-string")
	hash2 := hashConnString("connection-string")
	if hash1 != hash2 {
		t.Errorf("Same input produced different hashes: %q vs %q", hash1, hash2)
	}

	// Different input should produce different output
	hash3 := hashConnString("different-string")
	if hash1 == hash3 {
		t.Error("Different inputs produced same hash")
	}

	// Hash should be 16 characters (8 bytes hex encoded)
	if len(hash1) != 16 {
		t.Errorf("Hash length = %d, want 16", len(hash1))
	}
}

func TestMakeKey(t *testing.T) {
	key1 := makeKey("duckdb", "conn1")
	key2 := makeKey("duckdb", "conn1")
	key3 := makeKey("duckdb", "conn2")
	key4 := makeKey("mssql", "conn1")

	// Same driver+conn should produce same key
	if key1 != key2 {
		t.Errorf("Same inputs produced different keys: %q vs %q", key1, key2)
	}

	// Different conn should produce different key
	if key1 == key3 {
		t.Error("Different connection strings produced same key")
	}

	// Different driver should produce different key
	if key1 == key4 {
		t.Error("Different drivers produced same key")
	}

	// Key should start with driver name
	if key1[:6] != "duckdb" {
		t.Errorf("Key should start with driver name, got %q", key1)
	}
}

func TestGetConnection_DuckDB(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()

	// First call should create pool
	db1, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}
	if db1 == nil {
		t.Fatal("GetConnection returned nil db")
	}

	if m.PoolCount() != 1 {
		t.Errorf("PoolCount() = %d, want 1", m.PoolCount())
	}

	// Second call with same params should return same pool
	db2, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Second GetConnection failed: %v", err)
	}
	if db1 != db2 {
		t.Error("Expected same db instance from pool")
	}

	if m.PoolCount() != 1 {
		t.Errorf("PoolCount() after second call = %d, want 1", m.PoolCount())
	}
}

func TestGetConnection_DifferentConnStrings(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()

	// Create temporary files for different databases
	tmpDir := t.TempDir()
	connStr1 := tmpDir + "/test1.duckdb"
	connStr2 := tmpDir + "/test2.duckdb"

	db1, err := m.GetConnection(ctx, "duckdb", connStr1)
	if err != nil {
		t.Fatalf("First GetConnection failed: %v", err)
	}

	db2, err := m.GetConnection(ctx, "duckdb", connStr2)
	if err != nil {
		t.Fatalf("Second GetConnection failed: %v", err)
	}

	// Should be different pool instances
	if db1 == db2 {
		t.Error("Different connection strings should create different pools")
	}

	if m.PoolCount() != 2 {
		t.Errorf("PoolCount() = %d, want 2", m.PoolCount())
	}
}

func TestGetConnection_Concurrent(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()
	const goroutines = 10

	var wg sync.WaitGroup
	dbs := make([]*sql.DB, goroutines)
	errs := make([]error, goroutines)

	// Concurrent access to same connection
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			db, err := m.GetConnection(ctx, "duckdb", ":memory:")
			dbs[idx] = db
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	// All should succeed
	for i, err := range errs {
		if err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}

	// All should get the same pool
	firstDB := dbs[0]
	for i, db := range dbs[1:] {
		if db != firstDB {
			t.Errorf("Goroutine %d got different db instance", i+1)
		}
	}

	// Should only have one pool
	if m.PoolCount() != 1 {
		t.Errorf("PoolCount() = %d, want 1", m.PoolCount())
	}
}

func TestClose(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)

	ctx := context.Background()

	// Create temporary files for different databases
	tmpDir := t.TempDir()
	connStr1 := tmpDir + "/close_test1.duckdb"
	connStr2 := tmpDir + "/close_test2.duckdb"

	// Create a few pools
	_, err := m.GetConnection(ctx, "duckdb", connStr1)
	if err != nil {
		t.Fatalf("GetConnection 1 failed: %v", err)
	}
	_, err = m.GetConnection(ctx, "duckdb", connStr2)
	if err != nil {
		t.Fatalf("GetConnection 2 failed: %v", err)
	}

	if m.PoolCount() != 2 {
		t.Errorf("PoolCount() before close = %d, want 2", m.PoolCount())
	}

	// Close all
	if err := m.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if m.PoolCount() != 0 {
		t.Errorf("PoolCount() after close = %d, want 0", m.PoolCount())
	}
}

func TestCloseConnection(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()

	// Create temporary files for different databases
	tmpDir := t.TempDir()
	connStr1 := tmpDir + "/close_conn_test1.duckdb"
	connStr2 := tmpDir + "/close_conn_test2.duckdb"

	// Create pools
	_, err := m.GetConnection(ctx, "duckdb", connStr1)
	if err != nil {
		t.Fatalf("GetConnection 1 failed: %v", err)
	}
	_, err = m.GetConnection(ctx, "duckdb", connStr2)
	if err != nil {
		t.Fatalf("GetConnection 2 failed: %v", err)
	}

	if m.PoolCount() != 2 {
		t.Errorf("PoolCount() = %d, want 2", m.PoolCount())
	}

	// Close specific connection
	err = m.CloseConnection("duckdb", connStr1)
	if err != nil {
		t.Errorf("CloseConnection failed: %v", err)
	}

	if m.PoolCount() != 1 {
		t.Errorf("PoolCount() after CloseConnection = %d, want 1", m.PoolCount())
	}

	// Should be able to get the other connection
	if !m.HasPool("duckdb", connStr2) {
		t.Error("Expected pool for connStr2 to still exist")
	}
}

func TestCloseConnection_NonExistent(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	// Should not error when closing non-existent connection
	err := m.CloseConnection("duckdb", "nonexistent")
	if err != nil {
		t.Errorf("CloseConnection for non-existent should not error: %v", err)
	}
}

func TestHasPool(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()

	// Initially no pools
	if m.HasPool("duckdb", ":memory:") {
		t.Error("HasPool should return false before creating pool")
	}

	// Create pool
	_, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}

	// Now should have pool
	if !m.HasPool("duckdb", ":memory:") {
		t.Error("HasPool should return true after creating pool")
	}

	// Different conn string should not have pool
	if m.HasPool("duckdb", "different") {
		t.Error("HasPool should return false for different connection string")
	}
}

func TestStats(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()

	// Create pool
	_, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}

	stats := m.Stats()

	if len(stats) != 1 {
		t.Errorf("Stats() returned %d entries, want 1", len(stats))
	}

	for _, poolStats := range stats {
		if poolStats.Driver != "duckdb" {
			t.Errorf("Driver = %q, want 'duckdb'", poolStats.Driver)
		}
		if poolStats.CreatedAt.IsZero() {
			t.Error("CreatedAt should not be zero")
		}
	}
}

// mockDBOpener implements DBOpener for testing error handling
type mockDBOpener struct {
	openFunc func(driver, connStr string) (*sql.DB, error)
	calls    int32
}

func (m *mockDBOpener) Open(driver, connStr string) (*sql.DB, error) {
	atomic.AddInt32(&m.calls, 1)
	if m.openFunc != nil {
		return m.openFunc(driver, connStr)
	}
	return sql.Open(driver, connStr)
}

func TestGetConnection_OpenError(t *testing.T) {
	cfg := DefaultConfig()
	opener := &mockDBOpener{
		openFunc: func(driver, connStr string) (*sql.DB, error) {
			return nil, errors.New("mock open error")
		},
	}
	m := NewManagerWithOpener(cfg, opener)
	defer m.Close()

	ctx := context.Background()

	_, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err == nil {
		t.Error("Expected error from GetConnection")
	}
	if m.PoolCount() != 0 {
		t.Errorf("PoolCount() = %d, want 0 after failed open", m.PoolCount())
	}
}

func TestGetConnection_ContextCanceled(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err == nil {
		t.Error("Expected error with canceled context")
	}
}

func TestPoolConfigApplied(t *testing.T) {
	cfg := Config{
		MaxIdleConns:    3,
		MaxOpenConns:    7,
		ConnMaxLifetime: 2 * time.Minute,
		ConnMaxIdleTime: 30 * time.Second,
	}
	m := NewManager(cfg)
	defer m.Close()

	ctx := context.Background()

	db, err := m.GetConnection(ctx, "duckdb", ":memory:")
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}

	stats := db.Stats()

	// Note: MaxOpenConnections is the only one we can verify via Stats()
	if stats.MaxOpenConnections != 7 {
		t.Errorf("MaxOpenConnections = %d, want 7", stats.MaxOpenConnections)
	}
}
