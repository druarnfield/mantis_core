// Package duckdb provides a DuckDB driver implementation.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb" // DuckDB driver

	"github.com/mantis/worker/internal/driver"
	"github.com/mantis/worker/internal/protocol"
)

// Driver implements the driver.Driver interface for DuckDB.
type Driver struct {
	driver.BaseDriver
}

// New creates a new DuckDB driver.
func New() *Driver {
	return &Driver{
		BaseDriver: driver.NewBaseDriver("duckdb"),
	}
}

// Connect establishes a connection to DuckDB.
func (d *Driver) Connect(ctx context.Context, connectionString string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// ListSchemas returns all schemas in the database.
func (d *Driver) ListSchemas(ctx context.Context, db *sql.DB) (*protocol.ListSchemasResponse, error) {
	query := `
		SELECT
			schema_name,
			CASE WHEN schema_name = current_schema() THEN true ELSE false END AS is_default
		FROM information_schema.schemata
		WHERE catalog_name = current_database()
		  AND schema_name NOT IN ('information_schema', 'pg_catalog')
		ORDER BY schema_name
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}
	defer rows.Close()

	var schemas []protocol.SchemaInfo
	for rows.Next() {
		var name string
		var isDefault bool
		if err := rows.Scan(&name, &isDefault); err != nil {
			return nil, fmt.Errorf("failed to scan schema: %w", err)
		}
		schemas = append(schemas, protocol.SchemaInfo{
			Name:      name,
			IsDefault: isDefault,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schemas: %w", err)
	}

	return &protocol.ListSchemasResponse{Schemas: schemas}, nil
}

// ListTables returns all tables in the specified schema.
func (d *Driver) ListTables(ctx context.Context, db *sql.DB, schema string) (*protocol.ListTablesResponse, error) {
	if schema == "" {
		schema = "main" // Default schema for DuckDB
	}

	// Validate schema identifier
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	query := `
		SELECT
			table_schema,
			table_name,
			table_type
		FROM information_schema.tables
		WHERE table_schema = $1
		ORDER BY table_name
	`

	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []protocol.TableInfo
	for rows.Next() {
		var tableSchema, tableName, tableType string
		if err := rows.Scan(&tableSchema, &tableName, &tableType); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}

		// Normalize type
		normalizedType := normalizeTableType(tableType)

		tables = append(tables, protocol.TableInfo{
			Schema: tableSchema,
			Name:   tableName,
			Type:   normalizedType,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return &protocol.ListTablesResponse{Tables: tables}, nil
}

// GetTable returns detailed metadata for a specific table.
func (d *Driver) GetTable(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetTableResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	// Get basic table info
	tableInfoQuery := `
		SELECT table_schema, table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_name = $2
	`

	var tableSchema, tableName, tableType string
	err := db.QueryRowContext(ctx, tableInfoQuery, schema, table).Scan(&tableSchema, &tableName, &tableType)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table not found: %s.%s", schema, table)
		}
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	// Get columns
	columnsResp, err := d.GetColumns(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}

	// Get primary key
	pkResp, err := d.GetPrimaryKey(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}

	// Get foreign keys
	fkResp, err := d.GetForeignKeys(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}

	// Get unique constraints
	ucResp, err := d.GetUniqueConstraints(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}

	return &protocol.GetTableResponse{
		Table: protocol.TableDetailInfo{
			Schema:            tableSchema,
			Name:              tableName,
			Type:              normalizeTableType(tableType),
			Columns:           columnsResp.Columns,
			PrimaryKey:        pkResp.PrimaryKey,
			ForeignKeys:       fkResp.ForeignKeys,
			UniqueConstraints: ucResp.UniqueConstraints,
		},
	}, nil
}

// GetColumns returns column metadata for a specific table.
func (d *Driver) GetColumns(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetColumnsResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	query := `
		SELECT
			column_name,
			ordinal_position,
			data_type,
			CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS is_nullable,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	var columns []protocol.ColumnInfo
	for rows.Next() {
		var col protocol.ColumnInfo
		var maxLength, precision, scale sql.NullInt64
		var defaultValue sql.NullString

		if err := rows.Scan(
			&col.Name,
			&col.Position,
			&col.DataType,
			&col.IsNullable,
			&maxLength,
			&precision,
			&scale,
			&defaultValue,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		if maxLength.Valid && maxLength.Int64 > 0 {
			ml := int(maxLength.Int64)
			col.MaxLength = &ml
		}
		if precision.Valid {
			p := int(precision.Int64)
			col.NumericPrecision = &p
		}
		if scale.Valid {
			s := int(scale.Int64)
			col.NumericScale = &s
		}
		if defaultValue.Valid {
			col.DefaultValue = &defaultValue.String
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return &protocol.GetColumnsResponse{Columns: columns}, nil
}

// GetPrimaryKey returns the primary key constraint for a table.
func (d *Driver) GetPrimaryKey(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetPrimaryKeyResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	// DuckDB stores constraints in duckdb_constraints()
	query := `
		SELECT
			constraint_text
		FROM duckdb_constraints()
		WHERE schema_name = $1
			AND table_name = $2
			AND constraint_type = 'PRIMARY KEY'
	`

	var constraintText sql.NullString
	err := db.QueryRowContext(ctx, query, schema, table).Scan(&constraintText)
	if err != nil {
		if err == sql.ErrNoRows {
			return &protocol.GetPrimaryKeyResponse{PrimaryKey: nil}, nil
		}
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}

	if !constraintText.Valid || constraintText.String == "" {
		return &protocol.GetPrimaryKeyResponse{PrimaryKey: nil}, nil
	}

	// Parse columns from constraint text like "PRIMARY KEY(col1, col2)"
	columns := parseConstraintColumns(constraintText.String)
	if len(columns) == 0 {
		return &protocol.GetPrimaryKeyResponse{PrimaryKey: nil}, nil
	}

	return &protocol.GetPrimaryKeyResponse{
		PrimaryKey: &protocol.PrimaryKeyInfo{
			Name:    fmt.Sprintf("pk_%s_%s", schema, table),
			Columns: columns,
		},
	}, nil
}

// GetForeignKeys returns foreign key constraints for a table.
func (d *Driver) GetForeignKeys(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetForeignKeysResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	// DuckDB stores foreign key info in duckdb_constraints()
	query := `
		SELECT
			constraint_text
		FROM duckdb_constraints()
		WHERE schema_name = $1
			AND table_name = $2
			AND constraint_type = 'FOREIGN KEY'
	`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer rows.Close()

	var foreignKeys []protocol.ForeignKeyInfo
	fkNum := 0
	for rows.Next() {
		var constraintText string
		if err := rows.Scan(&constraintText); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}

		fk := parseForeignKey(constraintText, schema, table, fkNum)
		if fk != nil {
			foreignKeys = append(foreignKeys, *fk)
			fkNum++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating foreign keys: %w", err)
	}

	return &protocol.GetForeignKeysResponse{ForeignKeys: foreignKeys}, nil
}

// GetUniqueConstraints returns unique constraints for a table.
func (d *Driver) GetUniqueConstraints(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetUniqueConstraintsResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	query := `
		SELECT
			constraint_type,
			constraint_text
		FROM duckdb_constraints()
		WHERE schema_name = $1
			AND table_name = $2
			AND constraint_type IN ('UNIQUE', 'PRIMARY KEY')
	`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get unique constraints: %w", err)
	}
	defer rows.Close()

	var constraints []protocol.UniqueConstraintInfo
	ucNum := 0
	for rows.Next() {
		var constraintType, constraintText string
		if err := rows.Scan(&constraintType, &constraintText); err != nil {
			return nil, fmt.Errorf("failed to scan unique constraint: %w", err)
		}

		columns := parseConstraintColumns(constraintText)
		if len(columns) > 0 {
			name := fmt.Sprintf("uc_%s_%s_%d", schema, table, ucNum)
			if constraintType == "PRIMARY KEY" {
				name = fmt.Sprintf("pk_%s_%s", schema, table)
			}
			constraints = append(constraints, protocol.UniqueConstraintInfo{
				Name:         name,
				Columns:      columns,
				IsPrimaryKey: constraintType == "PRIMARY KEY",
			})
			ucNum++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating unique constraints: %w", err)
	}

	return &protocol.GetUniqueConstraintsResponse{UniqueConstraints: constraints}, nil
}

// GetIndexes returns index information for a table.
func (d *Driver) GetIndexes(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetIndexesResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	// DuckDB stores index info in duckdb_indexes()
	query := `
		SELECT
			index_name,
			is_unique,
			is_primary,
			sql
		FROM duckdb_indexes()
		WHERE schema_name = $1
			AND table_name = $2
	`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer rows.Close()

	var indexes []protocol.IndexInfo
	for rows.Next() {
		var indexName string
		var isUnique, isPrimary bool
		var sqlText sql.NullString
		if err := rows.Scan(&indexName, &isUnique, &isPrimary, &sqlText); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		// Parse columns from SQL if available
		var columns []protocol.IndexColumnInfo
		if sqlText.Valid {
			colNames := parseIndexColumns(sqlText.String)
			for i, name := range colNames {
				columns = append(columns, protocol.IndexColumnInfo{
					Name:     name,
					Position: i + 1,
				})
			}
		}

		indexes = append(indexes, protocol.IndexInfo{
			Name:         indexName,
			Columns:      columns,
			IsUnique:     isUnique,
			IsPrimaryKey: isPrimary,
			Type:         "ART", // DuckDB uses Adaptive Radix Tree
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}

	return &protocol.GetIndexesResponse{Indexes: indexes}, nil
}

// GetRowCount returns the row count for a table.
func (d *Driver) GetRowCount(ctx context.Context, db *sql.DB, schema, table string, exact bool) (*protocol.RowCountResponse, error) {
	// Validate identifiers to prevent SQL injection
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	// DuckDB doesn't have estimated counts, always use exact
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, schema, table)

	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	return &protocol.RowCountResponse{
		RowCount: count,
		IsExact:  true,
	}, nil
}

// SampleRows returns sample rows from a table.
func (d *Driver) SampleRows(ctx context.Context, db *sql.DB, schema, table string, limit int) (*protocol.SampleRowsResponse, error) {
	// Validate identifiers to prevent SQL injection
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	limit = driver.NormalizeLimit(limit)

	// Get column names first
	columnsResp, err := d.GetColumns(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, len(columnsResp.Columns))
	for i, col := range columnsResp.Columns {
		columnNames[i] = col.Name
	}

	// Query sample rows using TABLESAMPLE for larger tables
	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" LIMIT %d`, schema, table, limit)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to sample rows: %w", err)
	}
	defer rows.Close()

	var resultRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = driver.ConvertValue(v)
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &protocol.SampleRowsResponse{
		Columns:  columnNames,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

// GetDatabaseInfo returns database-level information.
func (d *Driver) GetDatabaseInfo(ctx context.Context, db *sql.DB) (*protocol.GetDatabaseInfoResponse, error) {
	query := `
		SELECT
			version() AS product_version,
			current_database() AS database_name,
			current_schema() AS default_schema
	`

	var version, dbName, defaultSchema string
	if err := db.QueryRowContext(ctx, query).Scan(&version, &dbName, &defaultSchema); err != nil {
		return nil, fmt.Errorf("failed to get database info: %w", err)
	}

	return &protocol.GetDatabaseInfoResponse{
		Database: protocol.DatabaseInfo{
			ProductName:    "DuckDB",
			ProductVersion: version,
			DatabaseName:   dbName,
			DefaultSchema:  defaultSchema,
		},
	}, nil
}

// GetColumnStats returns cardinality statistics for a column.
func (d *Driver) GetColumnStats(ctx context.Context, db *sql.DB, schema, table, column string, sampleSize int) (*protocol.ColumnStatsResponse, error) {
	// Validate identifiers
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column: %w", err)
	}

	if sampleSize <= 0 {
		sampleSize = 5
	}

	// Get total count, distinct count, and null count in one query
	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_count,
			COUNT(DISTINCT "%s") AS distinct_count,
			COUNT(*) - COUNT("%s") AS null_count
		FROM "%s"."%s"
	`, column, column, schema, table)

	var totalCount, distinctCount, nullCount int64
	if err := db.QueryRowContext(ctx, statsQuery).Scan(&totalCount, &distinctCount, &nullCount); err != nil {
		return nil, fmt.Errorf("failed to get column stats: %w", err)
	}

	// Determine if column is unique (all non-null values are distinct)
	nonNullCount := totalCount - nullCount
	isUnique := nonNullCount > 0 && distinctCount == nonNullCount

	// Get sample values
	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT "%s"
		FROM "%s"."%s"
		WHERE "%s" IS NOT NULL
		LIMIT %d
	`, column, schema, table, column, sampleSize)

	rows, err := db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get sample values: %w", err)
	}
	defer rows.Close()

	var sampleValues []interface{}
	for rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("failed to scan sample value: %w", err)
		}
		sampleValues = append(sampleValues, driver.ConvertValue(val))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sample values: %w", err)
	}

	return &protocol.ColumnStatsResponse{
		TotalCount:    totalCount,
		DistinctCount: distinctCount,
		NullCount:     nullCount,
		IsUnique:      isUnique,
		SampleValues:  sampleValues,
	}, nil
}

// CheckValueOverlap checks how many values from the left column exist in the right column.
func (d *Driver) CheckValueOverlap(ctx context.Context, db *sql.DB, leftSchema, leftTable, leftColumn, rightSchema, rightTable, rightColumn string, sampleSize int) (*protocol.ValueOverlapResponse, error) {
	// Validate all identifiers
	if err := driver.ValidateSchemaTable(leftSchema, leftTable); err != nil {
		return nil, fmt.Errorf("invalid left table: %w", err)
	}
	if err := driver.ValidateSchemaTable(rightSchema, rightTable); err != nil {
		return nil, fmt.Errorf("invalid right table: %w", err)
	}
	if err := driver.ValidateIdentifier(leftColumn); err != nil {
		return nil, fmt.Errorf("invalid left column: %w", err)
	}
	if err := driver.ValidateIdentifier(rightColumn); err != nil {
		return nil, fmt.Errorf("invalid right column: %w", err)
	}

	if sampleSize <= 0 {
		sampleSize = 1000
	}

	// Get distinct counts for both columns
	leftStatsQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT "%s") FROM "%s"."%s" WHERE "%s" IS NOT NULL
	`, leftColumn, leftSchema, leftTable, leftColumn)

	rightStatsQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT "%s") FROM "%s"."%s" WHERE "%s" IS NOT NULL
	`, rightColumn, rightSchema, rightTable, rightColumn)

	var leftTotalDistinct, rightTotalDistinct int64
	if err := db.QueryRowContext(ctx, leftStatsQuery).Scan(&leftTotalDistinct); err != nil {
		return nil, fmt.Errorf("failed to get left distinct count: %w", err)
	}
	if err := db.QueryRowContext(ctx, rightStatsQuery).Scan(&rightTotalDistinct); err != nil {
		return nil, fmt.Errorf("failed to get right distinct count: %w", err)
	}

	// Check overlap using a sampled join
	// This query samples distinct values from the left and checks how many exist in the right
	overlapQuery := fmt.Sprintf(`
		WITH left_sample AS (
			SELECT DISTINCT "%s" AS val
			FROM "%s"."%s"
			WHERE "%s" IS NOT NULL
			LIMIT %d
		),
		overlap AS (
			SELECT ls.val
			FROM left_sample ls
			WHERE EXISTS (
				SELECT 1 FROM "%s"."%s" r
				WHERE r."%s" = ls.val
			)
		)
		SELECT
			(SELECT COUNT(*) FROM left_sample) AS sample_size,
			(SELECT COUNT(*) FROM overlap) AS overlap_count
	`, leftColumn, leftSchema, leftTable, leftColumn, sampleSize,
		rightSchema, rightTable, rightColumn)

	var leftSampleSize, overlapCount int64
	if err := db.QueryRowContext(ctx, overlapQuery).Scan(&leftSampleSize, &overlapCount); err != nil {
		return nil, fmt.Errorf("failed to check value overlap: %w", err)
	}

	// Calculate overlap percentage
	var overlapPercentage float64
	if leftSampleSize > 0 {
		overlapPercentage = float64(overlapCount) / float64(leftSampleSize) * 100.0
	}

	// Check if right is superset (all sampled left values exist in right)
	rightIsSuperset := leftSampleSize > 0 && overlapCount == leftSampleSize

	// Check uniqueness for cardinality hints
	leftCountQuery := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, leftSchema, leftTable)
	rightCountQuery := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, rightSchema, rightTable)

	var leftTotalCount, rightTotalCount int64
	if err := db.QueryRowContext(ctx, leftCountQuery).Scan(&leftTotalCount); err != nil {
		return nil, fmt.Errorf("failed to get left count: %w", err)
	}
	if err := db.QueryRowContext(ctx, rightCountQuery).Scan(&rightTotalCount); err != nil {
		return nil, fmt.Errorf("failed to get right count: %w", err)
	}

	leftIsUnique := leftTotalDistinct == leftTotalCount
	rightIsUnique := rightTotalDistinct == rightTotalCount

	return &protocol.ValueOverlapResponse{
		LeftSampleSize:     leftSampleSize,
		LeftTotalDistinct:  leftTotalDistinct,
		RightTotalDistinct: rightTotalDistinct,
		OverlapCount:       overlapCount,
		OverlapPercentage:  overlapPercentage,
		RightIsSuperset:    rightIsSuperset,
		LeftIsUnique:       leftIsUnique,
		RightIsUnique:      rightIsUnique,
	}, nil
}

// ExecuteQuery executes a SQL query using the base driver implementation.
func (d *Driver) ExecuteQuery(ctx context.Context, db *sql.DB, sqlQuery string, args []interface{}) (*protocol.ExecuteQueryResponse, error) {
	return d.BaseDriver.ExecuteQuery(ctx, db, sqlQuery, args)
}

// Helper functions

func normalizeTableType(t string) string {
	switch strings.ToUpper(t) {
	case "BASE TABLE":
		return "TABLE"
	case "VIEW":
		return "VIEW"
	case "LOCAL TEMPORARY":
		return "TEMPORARY"
	default:
		return t
	}
}

func parseConstraintColumns(constraintText string) []string {
	// Parse columns from constraint text like "PRIMARY KEY(col1, col2)" or "UNIQUE(col1)"
	start := strings.Index(constraintText, "(")
	end := strings.LastIndex(constraintText, ")")
	if start == -1 || end == -1 || end <= start {
		return nil
	}

	columnsPart := constraintText[start+1 : end]
	parts := strings.Split(columnsPart, ",")

	var columns []string
	for _, part := range parts {
		col := strings.TrimSpace(part)
		col = strings.Trim(col, `"`)
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
}

func parseForeignKey(constraintText, schema, table string, num int) *protocol.ForeignKeyInfo {
	// Parse FK text like "FOREIGN KEY(col) REFERENCES other_table(other_col)"
	// or "FOREIGN KEY(col) REFERENCES other_schema.other_table(other_col)"
	fkIdx := strings.Index(strings.ToUpper(constraintText), "FOREIGN KEY")
	refIdx := strings.Index(strings.ToUpper(constraintText), "REFERENCES")
	if fkIdx == -1 || refIdx == -1 {
		return nil
	}

	// Extract local columns
	localPart := constraintText[fkIdx+11 : refIdx]
	localCols := parseConstraintColumns(localPart)

	// Extract referenced table and columns
	refPart := constraintText[refIdx+10:]
	parenStart := strings.Index(refPart, "(")
	if parenStart == -1 {
		return nil
	}

	// Parse schema.table or just table
	refTablePart := strings.TrimSpace(refPart[:parenStart])
	refSchema, refTable := parseSchemaTable(refTablePart, schema)

	refCols := parseConstraintColumns(refPart)

	if len(localCols) == 0 || len(refCols) == 0 {
		return nil
	}

	return &protocol.ForeignKeyInfo{
		Name:              fmt.Sprintf("fk_%s_%s_%d", schema, table, num),
		Columns:           localCols,
		ReferencedSchema:  refSchema,
		ReferencedTable:   refTable,
		ReferencedColumns: refCols,
	}
}

// parseSchemaTable parses "schema.table" or "table" format.
// Returns (schema, table). If no schema is specified, uses defaultSchema.
func parseSchemaTable(s, defaultSchema string) (string, string) {
	s = strings.TrimSpace(s)

	// Check for schema.table format (with or without quotes)
	// Handle: schema.table, "schema"."table", "schema".table, schema."table"
	var schema, table string

	// Try to find the dot separator (not inside quotes)
	dotIdx := -1
	inQuotes := false
	for i, c := range s {
		if c == '"' {
			inQuotes = !inQuotes
		} else if c == '.' && !inQuotes {
			dotIdx = i
			break
		}
	}

	if dotIdx > 0 {
		schema = strings.Trim(s[:dotIdx], `" `)
		table = strings.Trim(s[dotIdx+1:], `" `)
	} else {
		schema = defaultSchema
		table = strings.Trim(s, `" `)
	}

	return schema, table
}

func parseIndexColumns(sqlText string) []string {
	// Parse columns from CREATE INDEX sql
	start := strings.LastIndex(sqlText, "(")
	end := strings.LastIndex(sqlText, ")")
	if start == -1 || end == -1 || end <= start {
		return nil
	}

	columnsPart := sqlText[start+1 : end]
	parts := strings.Split(columnsPart, ",")

	var columns []string
	for _, part := range parts {
		col := strings.TrimSpace(part)
		// Remove ASC/DESC
		col = strings.TrimSuffix(col, " ASC")
		col = strings.TrimSuffix(col, " DESC")
		col = strings.Trim(col, `"`)
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
}

// init registers the DuckDB driver with the default registry.
func init() {
	driver.Register(New())
}
