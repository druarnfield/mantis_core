package driver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mantis/worker/internal/protocol"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()

	// Create a mock driver
	mock := &mockDriver{driverName: "test"}

	// Register the driver
	reg.Register(mock)

	// Get the driver
	d, err := reg.Get("test")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if d.Name() != "test" {
		t.Errorf("Name() = %q, want %q", d.Name(), "test")
	}
}

func TestRegistry_GetNotFound(t *testing.T) {
	reg := NewRegistry()

	_, err := reg.Get("nonexistent")
	if err == nil {
		t.Error("Get should return error for nonexistent driver")
	}
}

func TestRegistry_Has(t *testing.T) {
	reg := NewRegistry()

	if reg.Has("test") {
		t.Error("Has should return false for nonexistent driver")
	}

	reg.Register(&mockDriver{driverName: "test"})

	if !reg.Has("test") {
		t.Error("Has should return true after registration")
	}
}

func TestRegistry_Names(t *testing.T) {
	reg := NewRegistry()

	reg.Register(&mockDriver{driverName: "driver1"})
	reg.Register(&mockDriver{driverName: "driver2"})

	names := reg.Names()
	if len(names) != 2 {
		t.Errorf("len(Names()) = %d, want 2", len(names))
	}

	// Check both drivers are present (order not guaranteed)
	hasDriver1, hasDriver2 := false, false
	for _, name := range names {
		if name == "driver1" {
			hasDriver1 = true
		}
		if name == "driver2" {
			hasDriver2 = true
		}
	}
	if !hasDriver1 || !hasDriver2 {
		t.Errorf("Names() = %v, want [driver1, driver2]", names)
	}
}

func TestRegistry_Replace(t *testing.T) {
	reg := NewRegistry()

	mock1 := &mockDriver{driverName: "test", version: 1}
	mock2 := &mockDriver{driverName: "test", version: 2}

	reg.Register(mock1)
	reg.Register(mock2)

	d, err := reg.Get("test")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}

	// Should get the second (replacement) driver
	md := d.(*mockDriver)
	if md.version != 2 {
		t.Errorf("version = %d, want 2", md.version)
	}
}

func TestDefaultRegistry(t *testing.T) {
	// Clear any existing registrations for test isolation
	DefaultRegistry = NewRegistry()

	if Has("testdefault") {
		t.Error("Has should return false before registration")
	}

	Register(&mockDriver{driverName: "testdefault"})

	if !Has("testdefault") {
		t.Error("Has should return true after registration")
	}

	d, err := Get("testdefault")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if d.Name() != "testdefault" {
		t.Errorf("Name() = %q, want %q", d.Name(), "testdefault")
	}
}

func TestNormalizeLimit(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{0, DefaultLimit},
		{-1, DefaultLimit},
		{5, 5},
		{100, 100},
	}

	for _, tt := range tests {
		got := NormalizeLimit(tt.input)
		if got != tt.want {
			t.Errorf("NormalizeLimit(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestBaseDriver_Name(t *testing.T) {
	base := NewBaseDriver("mydriver")
	if base.Name() != "mydriver" {
		t.Errorf("Name() = %q, want %q", base.Name(), "mydriver")
	}
}

// mockDriver is a minimal implementation for testing
type mockDriver struct {
	driverName string
	version    int
}

func (m *mockDriver) Name() string {
	return m.driverName
}

func (m *mockDriver) Connect(ctx context.Context, connectionString string) (*sql.DB, error) {
	return nil, nil
}

func (m *mockDriver) ListSchemas(ctx context.Context, db *sql.DB) (*protocol.ListSchemasResponse, error) {
	return nil, nil
}

func (m *mockDriver) ListTables(ctx context.Context, db *sql.DB, schema string) (*protocol.ListTablesResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetTable(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetTableResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetColumns(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetColumnsResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetPrimaryKey(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetPrimaryKeyResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetForeignKeys(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetForeignKeysResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetUniqueConstraints(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetUniqueConstraintsResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetIndexes(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetIndexesResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetRowCount(ctx context.Context, db *sql.DB, schema, table string, exact bool) (*protocol.RowCountResponse, error) {
	return nil, nil
}

func (m *mockDriver) SampleRows(ctx context.Context, db *sql.DB, schema, table string, limit int) (*protocol.SampleRowsResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetDatabaseInfo(ctx context.Context, db *sql.DB) (*protocol.GetDatabaseInfoResponse, error) {
	return nil, nil
}

func (m *mockDriver) ExecuteQuery(ctx context.Context, db *sql.DB, sqlQuery string, args []interface{}) (*protocol.ExecuteQueryResponse, error) {
	return nil, nil
}

func (m *mockDriver) GetColumnStats(ctx context.Context, db *sql.DB, schema, table, column string, sampleSize int) (*protocol.ColumnStatsResponse, error) {
	return nil, nil
}

func (m *mockDriver) CheckValueOverlap(ctx context.Context, db *sql.DB, leftSchema, leftTable, leftColumn, rightSchema, rightTable, rightColumn string, sampleSize int) (*protocol.ValueOverlapResponse, error) {
	return nil, nil
}
