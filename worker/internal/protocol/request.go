package protocol

// ConnectionParams contains database connection details.
// These are passed with every request (stateless design).
type ConnectionParams struct {
	// Driver specifies which database driver to use (e.g., "mssql", "duckdb")
	Driver string `json:"driver"`

	// ConnectionString is the driver-specific connection string
	ConnectionString string `json:"connection_string"`
}

// --- Metadata Request Parameters ---

// ListSchemasParams contains parameters for metadata.list_schemas.
type ListSchemasParams struct {
	ConnectionParams
}

// ListTablesParams contains parameters for metadata.list_tables.
type ListTablesParams struct {
	ConnectionParams

	// Schema to list tables from (optional, uses default if empty)
	Schema string `json:"schema,omitempty"`
}

// GetTableParams contains parameters for metadata.get_table.
type GetTableParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get metadata for
	Table string `json:"table"`
}

// GetColumnsParams contains parameters for metadata.get_columns.
type GetColumnsParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get columns for
	Table string `json:"table"`
}

// GetPrimaryKeyParams contains parameters for metadata.get_primary_key.
type GetPrimaryKeyParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get primary key for
	Table string `json:"table"`
}

// GetForeignKeysParams contains parameters for metadata.get_foreign_keys.
type GetForeignKeysParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get foreign keys for
	Table string `json:"table"`
}

// GetUniqueConstraintsParams contains parameters for metadata.get_unique_constraints.
type GetUniqueConstraintsParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get unique constraints for
	Table string `json:"table"`
}

// GetIndexesParams contains parameters for metadata.get_indexes.
type GetIndexesParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get indexes for
	Table string `json:"table"`
}

// GetRowCountParams contains parameters for metadata.get_row_count.
type GetRowCountParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to get row count for
	Table string `json:"table"`

	// Exact specifies whether to use exact count (slower) or estimated (optional)
	Exact bool `json:"exact,omitempty"`
}

// SampleRowsParams contains parameters for metadata.sample_rows.
type SampleRowsParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name to sample rows from
	Table string `json:"table"`

	// Limit is the maximum number of rows to return (default: 10)
	Limit int `json:"limit,omitempty"`
}

// GetDatabaseInfoParams contains parameters for metadata.get_database_info.
type GetDatabaseInfoParams struct {
	ConnectionParams
}

// --- Cardinality Discovery Parameters ---

// GetColumnStatsParams contains parameters for metadata.get_column_stats.
type GetColumnStatsParams struct {
	ConnectionParams

	// Schema the table belongs to
	Schema string `json:"schema"`

	// Table name
	Table string `json:"table"`

	// Column name to get stats for
	Column string `json:"column"`

	// SampleSize is the number of sample values to return (default: 5)
	SampleSize int `json:"sample_size,omitempty"`
}

// CheckValueOverlapParams contains parameters for metadata.check_value_overlap.
type CheckValueOverlapParams struct {
	ConnectionParams

	// LeftSchema is the schema of the left table
	LeftSchema string `json:"left_schema"`

	// LeftTable is the name of the left table
	LeftTable string `json:"left_table"`

	// LeftColumn is the column in the left table
	LeftColumn string `json:"left_column"`

	// RightSchema is the schema of the right table
	RightSchema string `json:"right_schema"`

	// RightTable is the name of the right table
	RightTable string `json:"right_table"`

	// RightColumn is the column in the right table
	RightColumn string `json:"right_column"`

	// SampleSize limits the number of values checked (default: 1000)
	SampleSize int `json:"sample_size,omitempty"`
}

// --- Query Execution Parameters ---

// ExecuteQueryParams contains parameters for query.execute.
type ExecuteQueryParams struct {
	ConnectionParams

	// SQL is the query to execute
	SQL string `json:"sql"`

	// Args are positional parameters for the query (optional)
	Args []interface{} `json:"args,omitempty"`
}
