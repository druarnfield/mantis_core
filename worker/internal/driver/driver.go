// Package driver defines the database driver interface and implementations.
//
// Each database type (MSSQL, DuckDB, etc.) implements the Driver interface,
// providing database-specific SQL queries for metadata retrieval and query execution.
package driver

import (
	"context"
	"database/sql"

	"github.com/mantis/worker/internal/protocol"
)

// Driver is the interface that database drivers must implement.
// Each method corresponds to a protocol method.
type Driver interface {
	// Name returns the driver identifier (e.g., "mssql", "duckdb")
	Name() string

	// Connect establishes a database connection.
	// The returned *sql.DB should be used for subsequent operations.
	Connect(ctx context.Context, connectionString string) (*sql.DB, error)

	// --- Metadata Operations ---

	// ListSchemas returns all schemas in the database.
	ListSchemas(ctx context.Context, db *sql.DB) (*protocol.ListSchemasResponse, error)

	// ListTables returns all tables in the specified schema.
	// If schema is empty, uses the default schema.
	ListTables(ctx context.Context, db *sql.DB, schema string) (*protocol.ListTablesResponse, error)

	// GetTable returns detailed metadata for a specific table.
	GetTable(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetTableResponse, error)

	// GetColumns returns column metadata for a specific table.
	GetColumns(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetColumnsResponse, error)

	// GetPrimaryKey returns the primary key constraint for a table.
	GetPrimaryKey(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetPrimaryKeyResponse, error)

	// GetForeignKeys returns foreign key constraints for a table.
	GetForeignKeys(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetForeignKeysResponse, error)

	// GetUniqueConstraints returns unique constraints for a table.
	GetUniqueConstraints(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetUniqueConstraintsResponse, error)

	// GetIndexes returns index information for a table.
	GetIndexes(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetIndexesResponse, error)

	// GetRowCount returns the row count for a table.
	// If exact is true, performs COUNT(*), otherwise uses estimated count.
	GetRowCount(ctx context.Context, db *sql.DB, schema, table string, exact bool) (*protocol.RowCountResponse, error)

	// SampleRows returns sample rows from a table.
	// If limit is 0, uses a default limit (typically 10).
	SampleRows(ctx context.Context, db *sql.DB, schema, table string, limit int) (*protocol.SampleRowsResponse, error)

	// GetDatabaseInfo returns database-level information.
	GetDatabaseInfo(ctx context.Context, db *sql.DB) (*protocol.GetDatabaseInfoResponse, error)

	// --- Cardinality Discovery ---

	// GetColumnStats returns cardinality statistics for a column.
	GetColumnStats(ctx context.Context, db *sql.DB, schema, table, column string, sampleSize int) (*protocol.ColumnStatsResponse, error)

	// CheckValueOverlap checks how many values from the left column exist in the right column.
	CheckValueOverlap(ctx context.Context, db *sql.DB, leftSchema, leftTable, leftColumn, rightSchema, rightTable, rightColumn string, sampleSize int) (*protocol.ValueOverlapResponse, error)

	// --- Query Execution ---

	// ExecuteQuery executes a SQL query and returns the results.
	ExecuteQuery(ctx context.Context, db *sql.DB, sql string, args []interface{}) (*protocol.ExecuteQueryResponse, error)
}

// BaseDriver provides common functionality that can be embedded by driver implementations.
// It provides default implementations for some methods that work across most databases.
type BaseDriver struct {
	name string
}

// NewBaseDriver creates a new BaseDriver with the given name.
func NewBaseDriver(name string) BaseDriver {
	return BaseDriver{name: name}
}

// Name returns the driver name.
func (d *BaseDriver) Name() string {
	return d.name
}

// MaxQueryRows is the maximum number of rows returned by ExecuteQuery.
// This prevents memory exhaustion from unbounded result sets.
const MaxQueryRows = 10000

// ExecuteQuery provides a generic query execution implementation.
// This works for most databases using database/sql interface.
// Results are limited to MaxQueryRows to prevent memory exhaustion.
func (d *BaseDriver) ExecuteQuery(ctx context.Context, db *sql.DB, sqlQuery string, args []interface{}) (*protocol.ExecuteQueryResponse, error) {
	rows, err := db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column information
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := make([]protocol.QueryResultColumn, len(colTypes))
	for i, ct := range colTypes {
		columns[i] = protocol.QueryResultColumn{
			Name:     ct.Name(),
			DataType: ct.DatabaseTypeName(),
		}
	}

	// Read rows up to limit
	resultRows := make([][]interface{}, 0)
	for rows.Next() {
		// Stop if we've hit the row limit
		if len(resultRows) >= MaxQueryRows {
			break
		}

		// Create a slice of interface{} to hold column values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Convert sql.RawBytes and similar types to standard types
		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = ConvertValue(v)
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &protocol.ExecuteQueryResponse{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

// ConvertValue converts database-specific types to JSON-serializable types.
// Exported for use by driver implementations.
func ConvertValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		// Convert byte slices to strings (common for VARCHAR, TEXT, etc.)
		return string(val)
	default:
		return val
	}
}

// DefaultLimit is the default number of rows to return for sample queries.
const DefaultLimit = 10

// NormalizeLimit returns the limit to use, applying default if needed.
func NormalizeLimit(limit int) int {
	if limit <= 0 {
		return DefaultLimit
	}
	return limit
}
