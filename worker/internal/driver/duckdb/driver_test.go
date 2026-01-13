package duckdb

import (
	"context"
	"testing"

	"github.com/mantis/worker/internal/driver"
)

func TestNew(t *testing.T) {
	d := New()
	if d == nil {
		t.Fatal("New() returned nil")
	}
	if d.Name() != "duckdb" {
		t.Errorf("Name() = %q, want %q", d.Name(), "duckdb")
	}
}

func TestDriverImplementsInterface(t *testing.T) {
	// Compile-time check that Driver implements driver.Driver
	var _ driver.Driver = (*Driver)(nil)
}

func TestNormalizeTableType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"BASE TABLE", "TABLE"},
		{"VIEW", "VIEW"},
		{"LOCAL TEMPORARY", "TEMPORARY"},
		{"OTHER", "OTHER"},
	}

	for _, tt := range tests {
		got := normalizeTableType(tt.input)
		if got != tt.want {
			t.Errorf("normalizeTableType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseConstraintColumns(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"PRIMARY KEY(id)", []string{"id"}},
		{"PRIMARY KEY(col1, col2)", []string{"col1", "col2"}},
		{"UNIQUE(email)", []string{"email"}},
		{`PRIMARY KEY("Id", "Name")`, []string{"Id", "Name"}},
		{"INVALID", nil},
		{"", nil},
	}

	for _, tt := range tests {
		got := parseConstraintColumns(tt.input)
		if !stringSliceEqual(got, tt.want) {
			t.Errorf("parseConstraintColumns(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseIndexColumns(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"CREATE INDEX idx ON t(col1)", []string{"col1"}},
		{"CREATE INDEX idx ON t(col1, col2)", []string{"col1", "col2"}},
		{"CREATE INDEX idx ON t(col1 ASC)", []string{"col1"}},
		{"CREATE INDEX idx ON t(col1 DESC, col2 ASC)", []string{"col1", "col2"}},
		{`CREATE INDEX idx ON t("Col1")`, []string{"Col1"}},
		{"INVALID", nil},
	}

	for _, tt := range tests {
		got := parseIndexColumns(tt.input)
		if !stringSliceEqual(got, tt.want) {
			t.Errorf("parseIndexColumns(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseSchemaTable(t *testing.T) {
	tests := []struct {
		input         string
		defaultSchema string
		wantSchema    string
		wantTable     string
	}{
		// Simple table name (uses default schema)
		{"customers", "main", "main", "customers"},
		{"users", "dbo", "dbo", "users"},

		// Schema.table format
		{"other_schema.customers", "main", "other_schema", "customers"},
		{"sales.orders", "main", "sales", "orders"},

		// Quoted identifiers
		{`"customers"`, "main", "main", "customers"},
		{`"other_schema"."customers"`, "main", "other_schema", "customers"},
		{`"schema"."table"`, "main", "schema", "table"},

		// Mixed quotes
		{`other_schema."customers"`, "main", "other_schema", "customers"},
		{`"other_schema".customers`, "main", "other_schema", "customers"},

		// With whitespace
		{" customers ", "main", "main", "customers"},
		{" other.table ", "main", "other", "table"},
	}

	for _, tt := range tests {
		gotSchema, gotTable := parseSchemaTable(tt.input, tt.defaultSchema)
		if gotSchema != tt.wantSchema {
			t.Errorf("parseSchemaTable(%q, %q) schema = %q, want %q", tt.input, tt.defaultSchema, gotSchema, tt.wantSchema)
		}
		if gotTable != tt.wantTable {
			t.Errorf("parseSchemaTable(%q, %q) table = %q, want %q", tt.input, tt.defaultSchema, gotTable, tt.wantTable)
		}
	}
}

func TestParseForeignKey(t *testing.T) {
	tests := []struct {
		input         string
		wantNil       bool
		wantCols      []string
		wantRefSchema string
		wantRefCols   []string
		wantRefTab    string
	}{
		// Same schema (no schema prefix)
		{
			input:         "FOREIGN KEY(customer_id) REFERENCES customers(id)",
			wantCols:      []string{"customer_id"},
			wantRefSchema: "main",
			wantRefCols:   []string{"id"},
			wantRefTab:    "customers",
		},
		// Composite key, same schema
		{
			input:         "FOREIGN KEY(a, b) REFERENCES other(x, y)",
			wantCols:      []string{"a", "b"},
			wantRefSchema: "main",
			wantRefCols:   []string{"x", "y"},
			wantRefTab:    "other",
		},
		// Cross-schema reference
		{
			input:         "FOREIGN KEY(customer_id) REFERENCES sales.customers(id)",
			wantCols:      []string{"customer_id"},
			wantRefSchema: "sales",
			wantRefCols:   []string{"id"},
			wantRefTab:    "customers",
		},
		// Cross-schema with quotes
		{
			input:         `FOREIGN KEY(order_id) REFERENCES "other_schema"."orders"(id)`,
			wantCols:      []string{"order_id"},
			wantRefSchema: "other_schema",
			wantRefCols:   []string{"id"},
			wantRefTab:    "orders",
		},
		// Invalid
		{
			input:   "INVALID",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		got := parseForeignKey(tt.input, "main", "test", 0)
		if tt.wantNil {
			if got != nil {
				t.Errorf("parseForeignKey(%q) = %v, want nil", tt.input, got)
			}
			continue
		}
		if got == nil {
			t.Errorf("parseForeignKey(%q) = nil, want non-nil", tt.input)
			continue
		}
		if !stringSliceEqual(got.Columns, tt.wantCols) {
			t.Errorf("parseForeignKey(%q).Columns = %v, want %v", tt.input, got.Columns, tt.wantCols)
		}
		if got.ReferencedSchema != tt.wantRefSchema {
			t.Errorf("parseForeignKey(%q).ReferencedSchema = %q, want %q", tt.input, got.ReferencedSchema, tt.wantRefSchema)
		}
		if !stringSliceEqual(got.ReferencedColumns, tt.wantRefCols) {
			t.Errorf("parseForeignKey(%q).ReferencedColumns = %v, want %v", tt.input, got.ReferencedColumns, tt.wantRefCols)
		}
		if got.ReferencedTable != tt.wantRefTab {
			t.Errorf("parseForeignKey(%q).ReferencedTable = %q, want %q", tt.input, got.ReferencedTable, tt.wantRefTab)
		}
	}
}

func TestDriverRegistration(t *testing.T) {
	// The init() function should have registered the driver
	if !driver.Has("duckdb") {
		t.Error("DuckDB driver should be registered automatically")
	}

	d, err := driver.Get("duckdb")
	if err != nil {
		t.Fatalf("Get(\"duckdb\") error: %v", err)
	}
	if d.Name() != "duckdb" {
		t.Errorf("Name() = %q, want %q", d.Name(), "duckdb")
	}
}

// Integration tests that require an actual DuckDB connection

func TestConnect_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Verify we can query
	var result int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if result != 1 {
		t.Errorf("Query result = %d, want 1", result)
	}
}

func TestListSchemas_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	resp, err := d.ListSchemas(ctx, db)
	if err != nil {
		t.Fatalf("ListSchemas error: %v", err)
	}

	// Should have at least 'main' schema
	hasMain := false
	for _, s := range resp.Schemas {
		if s.Name == "main" {
			hasMain = true
			if !s.IsDefault {
				t.Error("main schema should be default")
			}
		}
	}
	if !hasMain {
		t.Error("ListSchemas should return 'main' schema")
	}
}

func TestListTables_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.ExecContext(ctx, `CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR)`)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}

	resp, err := d.ListTables(ctx, db, "main")
	if err != nil {
		t.Fatalf("ListTables error: %v", err)
	}

	hasTestTable := false
	for _, table := range resp.Tables {
		if table.Name == "test_table" {
			hasTestTable = true
			if table.Type != "TABLE" {
				t.Errorf("table type = %q, want TABLE", table.Type)
			}
		}
	}
	if !hasTestTable {
		t.Error("ListTables should return test_table")
	}
}

func TestGetColumns_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create a test table with various column types
	_, err = db.ExecContext(ctx, `
		CREATE TABLE test_columns (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10, 2),
			active BOOLEAN,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}

	resp, err := d.GetColumns(ctx, db, "main", "test_columns")
	if err != nil {
		t.Fatalf("GetColumns error: %v", err)
	}

	if len(resp.Columns) != 5 {
		t.Errorf("len(Columns) = %d, want 5", len(resp.Columns))
	}

	// Verify column order
	if resp.Columns[0].Name != "id" {
		t.Errorf("Columns[0].Name = %q, want 'id'", resp.Columns[0].Name)
	}
	if resp.Columns[0].Position != 1 {
		t.Errorf("Columns[0].Position = %d, want 1", resp.Columns[0].Position)
	}
}

func TestGetDatabaseInfo_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	resp, err := d.GetDatabaseInfo(ctx, db)
	if err != nil {
		t.Fatalf("GetDatabaseInfo error: %v", err)
	}

	if resp.Database.ProductName != "DuckDB" {
		t.Errorf("ProductName = %q, want 'DuckDB'", resp.Database.ProductName)
	}
	if resp.Database.ProductVersion == "" {
		t.Error("ProductVersion should not be empty")
	}
	if resp.Database.DefaultSchema != "main" {
		t.Errorf("DefaultSchema = %q, want 'main'", resp.Database.DefaultSchema)
	}
}

func TestExecuteQuery_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	resp, err := d.ExecuteQuery(ctx, db, "SELECT 1 AS num, 'hello' AS str", nil)
	if err != nil {
		t.Fatalf("ExecuteQuery error: %v", err)
	}

	if len(resp.Columns) != 2 {
		t.Errorf("len(Columns) = %d, want 2", len(resp.Columns))
	}
	if resp.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", resp.RowCount)
	}
	if len(resp.Rows) != 1 {
		t.Errorf("len(Rows) = %d, want 1", len(resp.Rows))
	}
}

func TestSampleRows_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create and populate a test table
	_, err = db.ExecContext(ctx, `CREATE TABLE test_sample (id INTEGER, name VARCHAR)`)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO test_sample VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	resp, err := d.SampleRows(ctx, db, "main", "test_sample", 2)
	if err != nil {
		t.Fatalf("SampleRows error: %v", err)
	}

	if len(resp.Columns) != 2 {
		t.Errorf("len(Columns) = %d, want 2", len(resp.Columns))
	}
	if resp.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", resp.RowCount)
	}
}

func TestGetRowCount_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create and populate a test table
	_, err = db.ExecContext(ctx, `CREATE TABLE test_count (id INTEGER)`)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO test_count VALUES (1), (2), (3), (4), (5)`)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	resp, err := d.GetRowCount(ctx, db, "main", "test_count", true)
	if err != nil {
		t.Fatalf("GetRowCount error: %v", err)
	}

	if resp.RowCount != 5 {
		t.Errorf("RowCount = %d, want 5", resp.RowCount)
	}
	if !resp.IsExact {
		t.Error("IsExact should be true for DuckDB")
	}
}

func TestGetColumnStats_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create and populate a test table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE test_stats (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			status VARCHAR
		)
	`)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO test_stats VALUES
			(1, 100, 'active'),
			(2, 100, 'active'),
			(3, 200, 'inactive'),
			(4, 200, NULL),
			(5, 300, 'active')
	`)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Test stats on unique column (id)
	resp, err := d.GetColumnStats(ctx, db, "main", "test_stats", "id", 5)
	if err != nil {
		t.Fatalf("GetColumnStats error: %v", err)
	}

	if resp.TotalCount != 5 {
		t.Errorf("TotalCount = %d, want 5", resp.TotalCount)
	}
	if resp.DistinctCount != 5 {
		t.Errorf("DistinctCount = %d, want 5", resp.DistinctCount)
	}
	if resp.NullCount != 0 {
		t.Errorf("NullCount = %d, want 0", resp.NullCount)
	}
	if !resp.IsUnique {
		t.Error("IsUnique should be true for id column")
	}

	// Test stats on non-unique column (customer_id)
	resp, err = d.GetColumnStats(ctx, db, "main", "test_stats", "customer_id", 5)
	if err != nil {
		t.Fatalf("GetColumnStats error: %v", err)
	}

	if resp.TotalCount != 5 {
		t.Errorf("TotalCount = %d, want 5", resp.TotalCount)
	}
	if resp.DistinctCount != 3 {
		t.Errorf("DistinctCount = %d, want 3", resp.DistinctCount)
	}
	if resp.IsUnique {
		t.Error("IsUnique should be false for customer_id column")
	}

	// Test stats on column with nulls
	resp, err = d.GetColumnStats(ctx, db, "main", "test_stats", "status", 5)
	if err != nil {
		t.Fatalf("GetColumnStats error: %v", err)
	}

	if resp.NullCount != 1 {
		t.Errorf("NullCount = %d, want 1", resp.NullCount)
	}
	if resp.DistinctCount != 2 {
		t.Errorf("DistinctCount = %d, want 2 (active, inactive)", resp.DistinctCount)
	}
}

func TestCheckValueOverlap_InMemory(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create orders table (FK side)
	_, err = db.ExecContext(ctx, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Create orders table error: %v", err)
	}

	// Create customers table (PK side)
	_, err = db.ExecContext(ctx, `
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name VARCHAR
		)
	`)
	if err != nil {
		t.Fatalf("Create customers table error: %v", err)
	}

	// Insert customers
	_, err = db.ExecContext(ctx, `
		INSERT INTO customers VALUES
			(1, 'Alice'),
			(2, 'Bob'),
			(3, 'Charlie')
	`)
	if err != nil {
		t.Fatalf("Insert customers error: %v", err)
	}

	// Insert orders - all customer_ids exist in customers
	_, err = db.ExecContext(ctx, `
		INSERT INTO orders VALUES
			(101, 1),
			(102, 1),
			(103, 2),
			(104, 3),
			(105, 1)
	`)
	if err != nil {
		t.Fatalf("Insert orders error: %v", err)
	}

	// Test overlap - all order customer_ids should exist in customers
	resp, err := d.CheckValueOverlap(ctx, db, "main", "orders", "customer_id", "main", "customers", "id", 100)
	if err != nil {
		t.Fatalf("CheckValueOverlap error: %v", err)
	}

	if resp.OverlapPercentage != 100.0 {
		t.Errorf("OverlapPercentage = %f, want 100.0", resp.OverlapPercentage)
	}
	if !resp.RightIsSuperset {
		t.Error("RightIsSuperset should be true")
	}
	if resp.LeftTotalDistinct != 3 {
		t.Errorf("LeftTotalDistinct = %d, want 3", resp.LeftTotalDistinct)
	}
	if resp.RightTotalDistinct != 3 {
		t.Errorf("RightTotalDistinct = %d, want 3", resp.RightTotalDistinct)
	}

	// Left (orders.customer_id) is not unique (multiple orders per customer)
	if resp.LeftIsUnique {
		t.Error("LeftIsUnique should be false (multiple orders per customer)")
	}
	// Right (customers.id) is unique (PK)
	if !resp.RightIsUnique {
		t.Error("RightIsUnique should be true (PK column)")
	}
}

func TestCheckValueOverlap_PartialOverlap(t *testing.T) {
	d := New()
	ctx := context.Background()

	db, err := d.Connect(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer db.Close()

	// Create two tables with partial overlap
	_, err = db.ExecContext(ctx, `
		CREATE TABLE table_a (id INTEGER PRIMARY KEY);
		CREATE TABLE table_b (id INTEGER PRIMARY KEY);
		INSERT INTO table_a VALUES (1), (2), (3), (4), (5);
		INSERT INTO table_b VALUES (3), (4), (5), (6), (7);
	`)
	if err != nil {
		t.Fatalf("Setup error: %v", err)
	}

	resp, err := d.CheckValueOverlap(ctx, db, "main", "table_a", "id", "main", "table_b", "id", 100)
	if err != nil {
		t.Fatalf("CheckValueOverlap error: %v", err)
	}

	// 3 out of 5 values overlap (3, 4, 5)
	expectedOverlap := 60.0
	if resp.OverlapPercentage != expectedOverlap {
		t.Errorf("OverlapPercentage = %f, want %f", resp.OverlapPercentage, expectedOverlap)
	}
	if resp.RightIsSuperset {
		t.Error("RightIsSuperset should be false (partial overlap)")
	}
	if resp.OverlapCount != 3 {
		t.Errorf("OverlapCount = %d, want 3", resp.OverlapCount)
	}
}

// Helper function
func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
