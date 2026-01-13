package protocol

// --- Metadata Response Types ---

// SchemaInfo represents a database schema.
type SchemaInfo struct {
	// Name is the schema name
	Name string `json:"name"`

	// IsDefault indicates if this is the default schema
	IsDefault bool `json:"is_default,omitempty"`
}

// ListSchemasResponse is returned by metadata.list_schemas.
type ListSchemasResponse struct {
	Schemas []SchemaInfo `json:"schemas"`
}

// TableInfo represents basic table information.
type TableInfo struct {
	// Schema the table belongs to
	Schema string `json:"schema"`

	// Name is the table name
	Name string `json:"name"`

	// Type is the table type ("TABLE", "VIEW", "MATERIALIZED_VIEW")
	Type string `json:"type"`
}

// ListTablesResponse is returned by metadata.list_tables.
type ListTablesResponse struct {
	Tables []TableInfo `json:"tables"`
}

// TableDetailInfo represents detailed table information.
type TableDetailInfo struct {
	// Schema the table belongs to
	Schema string `json:"schema"`

	// Name is the table name
	Name string `json:"name"`

	// Type is the table type
	Type string `json:"type"`

	// Columns in the table
	Columns []ColumnInfo `json:"columns"`

	// PrimaryKey columns (if any)
	PrimaryKey *PrimaryKeyInfo `json:"primary_key,omitempty"`

	// ForeignKeys referencing other tables
	ForeignKeys []ForeignKeyInfo `json:"foreign_keys,omitempty"`

	// UniqueConstraints on the table
	UniqueConstraints []UniqueConstraintInfo `json:"unique_constraints,omitempty"`
}

// GetTableResponse is returned by metadata.get_table.
type GetTableResponse struct {
	Table TableDetailInfo `json:"table"`
}

// ColumnInfo represents a table column.
type ColumnInfo struct {
	// Name is the column name
	Name string `json:"name"`

	// Position is the ordinal position (1-based)
	Position int `json:"position"`

	// DataType is the database-specific type name
	DataType string `json:"data_type"`

	// IsNullable indicates if NULL values are allowed
	IsNullable bool `json:"is_nullable"`

	// MaxLength for string types (optional)
	MaxLength *int `json:"max_length,omitempty"`

	// NumericPrecision for numeric types (optional)
	NumericPrecision *int `json:"numeric_precision,omitempty"`

	// NumericScale for numeric types (optional)
	NumericScale *int `json:"numeric_scale,omitempty"`

	// DefaultValue is the column default expression (optional)
	DefaultValue *string `json:"default_value,omitempty"`

	// IsIdentity indicates if this is an identity/auto-increment column
	IsIdentity bool `json:"is_identity,omitempty"`

	// IsComputed indicates if this is a computed column
	IsComputed bool `json:"is_computed,omitempty"`
}

// GetColumnsResponse is returned by metadata.get_columns.
type GetColumnsResponse struct {
	Columns []ColumnInfo `json:"columns"`
}

// PrimaryKeyInfo represents a primary key constraint.
type PrimaryKeyInfo struct {
	// Name is the constraint name
	Name string `json:"name"`

	// Columns in the primary key (ordered)
	Columns []string `json:"columns"`
}

// GetPrimaryKeyResponse is returned by metadata.get_primary_key.
type GetPrimaryKeyResponse struct {
	PrimaryKey *PrimaryKeyInfo `json:"primary_key"`
}

// ForeignKeyInfo represents a foreign key constraint.
type ForeignKeyInfo struct {
	// Name is the constraint name
	Name string `json:"name"`

	// Columns in the foreign key (ordered)
	Columns []string `json:"columns"`

	// ReferencedSchema is the schema of the referenced table
	ReferencedSchema string `json:"referenced_schema"`

	// ReferencedTable is the name of the referenced table
	ReferencedTable string `json:"referenced_table"`

	// ReferencedColumns in the referenced table (ordered, matches Columns)
	ReferencedColumns []string `json:"referenced_columns"`

	// OnDelete action (CASCADE, SET NULL, NO ACTION, etc.)
	OnDelete string `json:"on_delete,omitempty"`

	// OnUpdate action
	OnUpdate string `json:"on_update,omitempty"`
}

// GetForeignKeysResponse is returned by metadata.get_foreign_keys.
type GetForeignKeysResponse struct {
	ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
}

// UniqueConstraintInfo represents a unique constraint.
type UniqueConstraintInfo struct {
	// Name is the constraint name
	Name string `json:"name"`

	// Columns in the unique constraint (ordered)
	Columns []string `json:"columns"`

	// IsPrimaryKey indicates if this is also the primary key
	IsPrimaryKey bool `json:"is_primary_key,omitempty"`
}

// GetUniqueConstraintsResponse is returned by metadata.get_unique_constraints.
type GetUniqueConstraintsResponse struct {
	UniqueConstraints []UniqueConstraintInfo `json:"unique_constraints"`
}

// IndexInfo represents a database index.
type IndexInfo struct {
	// Name is the index name
	Name string `json:"name"`

	// Columns in the index (ordered)
	Columns []IndexColumnInfo `json:"columns"`

	// IsUnique indicates if the index enforces uniqueness
	IsUnique bool `json:"is_unique"`

	// IsPrimaryKey indicates if this backs the primary key
	IsPrimaryKey bool `json:"is_primary_key,omitempty"`

	// IsClustered indicates if this is a clustered index
	IsClustered bool `json:"is_clustered,omitempty"`

	// Type is the index type (BTREE, HASH, etc.)
	Type string `json:"type,omitempty"`
}

// IndexColumnInfo represents a column in an index.
type IndexColumnInfo struct {
	// Name is the column name
	Name string `json:"name"`

	// Position is the ordinal position in the index (1-based)
	Position int `json:"position"`

	// IsDescending indicates sort order
	IsDescending bool `json:"is_descending,omitempty"`

	// IsIncluded indicates if this is an included (non-key) column
	IsIncluded bool `json:"is_included,omitempty"`
}

// GetIndexesResponse is returned by metadata.get_indexes.
type GetIndexesResponse struct {
	Indexes []IndexInfo `json:"indexes"`
}

// RowCountResponse is returned by metadata.get_row_count.
type RowCountResponse struct {
	// RowCount is the number of rows
	RowCount int64 `json:"row_count"`

	// IsExact indicates if this is an exact count or estimate
	IsExact bool `json:"is_exact"`
}

// SampleRowsResponse is returned by metadata.sample_rows.
type SampleRowsResponse struct {
	// Columns is the list of column names in order
	Columns []string `json:"columns"`

	// Rows is the sampled data (each row is a list of values)
	Rows [][]interface{} `json:"rows"`

	// RowCount is the number of rows returned
	RowCount int `json:"row_count"`
}

// DatabaseInfo represents database-level information.
type DatabaseInfo struct {
	// ProductName is the database product (e.g., "Microsoft SQL Server")
	ProductName string `json:"product_name"`

	// ProductVersion is the version string
	ProductVersion string `json:"product_version"`

	// DatabaseName is the current database name
	DatabaseName string `json:"database_name"`

	// DefaultSchema is the default schema for the connection
	DefaultSchema string `json:"default_schema,omitempty"`

	// Collation is the database collation
	Collation string `json:"collation,omitempty"`
}

// GetDatabaseInfoResponse is returned by metadata.get_database_info.
type GetDatabaseInfoResponse struct {
	Database DatabaseInfo `json:"database"`
}

// --- Cardinality Discovery Response Types ---

// ColumnStatsResponse is returned by metadata.get_column_stats.
type ColumnStatsResponse struct {
	// TotalCount is the total number of rows in the table
	TotalCount int64 `json:"total_count"`

	// DistinctCount is the number of distinct values in the column
	DistinctCount int64 `json:"distinct_count"`

	// NullCount is the number of NULL values
	NullCount int64 `json:"null_count"`

	// IsUnique is true if all non-null values are unique
	IsUnique bool `json:"is_unique"`

	// SampleValues contains sample distinct values from the column
	SampleValues []interface{} `json:"sample_values,omitempty"`
}

// ValueOverlapResponse is returned by metadata.check_value_overlap.
type ValueOverlapResponse struct {
	// LeftSampleSize is the number of distinct values sampled from left
	LeftSampleSize int64 `json:"left_sample_size"`

	// LeftTotalDistinct is the total distinct values in left column
	LeftTotalDistinct int64 `json:"left_total_distinct"`

	// RightTotalDistinct is the total distinct values in right column
	RightTotalDistinct int64 `json:"right_total_distinct"`

	// OverlapCount is how many sampled left values exist in right
	OverlapCount int64 `json:"overlap_count"`

	// OverlapPercentage is (overlap_count / left_sample_size) * 100
	OverlapPercentage float64 `json:"overlap_percentage"`

	// RightIsSuperset is true if all sampled left values exist in right
	RightIsSuperset bool `json:"right_is_superset"`

	// LeftIsUnique indicates if left column has unique values
	LeftIsUnique bool `json:"left_is_unique"`

	// RightIsUnique indicates if right column has unique values
	RightIsUnique bool `json:"right_is_unique"`
}

// --- Query Execution Response Types ---

// QueryResultColumn describes a column in query results.
type QueryResultColumn struct {
	// Name is the column name or alias
	Name string `json:"name"`

	// DataType is the database-specific type
	DataType string `json:"data_type"`
}

// ExecuteQueryResponse is returned by query.execute.
type ExecuteQueryResponse struct {
	// Columns describes the result columns
	Columns []QueryResultColumn `json:"columns"`

	// Rows is the result data
	Rows [][]interface{} `json:"rows"`

	// RowCount is the number of rows returned
	RowCount int `json:"row_count"`

	// RowsAffected is set for INSERT/UPDATE/DELETE (optional)
	RowsAffected *int64 `json:"rows_affected,omitempty"`
}
