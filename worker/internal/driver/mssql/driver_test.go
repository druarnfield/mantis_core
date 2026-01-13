package mssql

import (
	"testing"

	"github.com/mantis/worker/internal/driver"
)

func TestNew(t *testing.T) {
	d := New()
	if d == nil {
		t.Fatal("New() returned nil")
	}
	if d.Name() != "mssql" {
		t.Errorf("Name() = %q, want %q", d.Name(), "mssql")
	}
}

func TestDriverImplementsInterface(t *testing.T) {
	// Compile-time check that Driver implements driver.Driver
	var _ driver.Driver = (*Driver)(nil)
}

func TestNormalizeRefAction(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"CASCADE", "CASCADE"},
		{"SET_NULL", "SET NULL"},
		{"SET_DEFAULT", "SET DEFAULT"},
		{"NO_ACTION", "NO ACTION"},
		{"UNKNOWN", "UNKNOWN"},
	}

	for _, tt := range tests {
		got := normalizeRefAction(tt.input)
		if got != tt.want {
			t.Errorf("normalizeRefAction(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestNormalizeIndexType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"CLUSTERED", "BTREE"},
		{"NONCLUSTERED", "BTREE"},
		{"HEAP", "HEAP"},
		{"OTHER", "OTHER"},
	}

	for _, tt := range tests {
		got := normalizeIndexType(tt.input)
		if got != tt.want {
			t.Errorf("normalizeIndexType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDriverRegistration(t *testing.T) {
	// The init() function should have registered the driver
	if !driver.Has("mssql") {
		t.Error("MSSQL driver should be registered automatically")
	}

	d, err := driver.Get("mssql")
	if err != nil {
		t.Fatalf("Get(\"mssql\") error: %v", err)
	}
	if d.Name() != "mssql" {
		t.Errorf("Name() = %q, want %q", d.Name(), "mssql")
	}
}
