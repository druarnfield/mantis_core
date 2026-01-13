// Package mssql provides a Microsoft SQL Server driver implementation.
package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/microsoft/go-mssqldb" // MSSQL driver

	"github.com/mantis/worker/internal/driver"
	"github.com/mantis/worker/internal/protocol"
)

// quoteString safely escapes a string value for use in SQL queries.
// This is used for metadata queries where parameterized queries aren't working
// reliably with the go-mssqldb driver.
func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Driver implements the driver.Driver interface for MSSQL.
type Driver struct {
	driver.BaseDriver
}

// New creates a new MSSQL driver.
func New() *Driver {
	return &Driver{
		BaseDriver: driver.NewBaseDriver("mssql"),
	}
}

// Connect establishes a connection to MSSQL.
func (d *Driver) Connect(ctx context.Context, connectionString string) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", connectionString)
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
			s.name AS schema_name,
			CASE WHEN s.name = SCHEMA_NAME() THEN 1 ELSE 0 END AS is_default
		FROM sys.schemas s
		WHERE s.schema_id < 16384  -- Exclude system schemas
		  AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
		ORDER BY s.name
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
		schema = "dbo" // Default schema for MSSQL
	}

	query := fmt.Sprintf(`
		SELECT
			TABLE_SCHEMA,
			TABLE_NAME,
			TABLE_TYPE
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = %s
		ORDER BY TABLE_NAME
	`, quoteString(schema))

	rows, err := db.QueryContext(ctx, query)
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
		normalizedType := "TABLE"
		if tableType == "VIEW" {
			normalizedType = "VIEW"
		}

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
	// Get basic table info
	tableInfoQuery := fmt.Sprintf(`
		SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
	`, quoteString(schema), quoteString(table))

	var tableSchema, tableName, tableType string
	err := db.QueryRowContext(ctx, tableInfoQuery).Scan(&tableSchema, &tableName, &tableType)
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

	normalizedType := "TABLE"
	if tableType == "VIEW" {
		normalizedType = "VIEW"
	}

	return &protocol.GetTableResponse{
		Table: protocol.TableDetailInfo{
			Schema:            tableSchema,
			Name:              tableName,
			Type:              normalizedType,
			Columns:           columnsResp.Columns,
			PrimaryKey:        pkResp.PrimaryKey,
			ForeignKeys:       fkResp.ForeignKeys,
			UniqueConstraints: ucResp.UniqueConstraints,
		},
	}, nil
}

// GetColumns returns column metadata for a specific table.
func (d *Driver) GetColumns(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetColumnsResponse, error) {
	// Use bracket notation for OBJECT_ID to handle special characters
	objectID := fmt.Sprintf("[%s].[%s]", schema, table)
	query := fmt.Sprintf(`
		SELECT
			c.COLUMN_NAME,
			c.ORDINAL_POSITION,
			c.DATA_TYPE,
			CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS is_nullable,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE,
			c.COLUMN_DEFAULT,
			COLUMNPROPERTY(OBJECT_ID('%s'), c.COLUMN_NAME, 'IsIdentity') AS is_identity,
			COLUMNPROPERTY(OBJECT_ID('%s'), c.COLUMN_NAME, 'IsComputed') AS is_computed
		FROM INFORMATION_SCHEMA.COLUMNS c
		WHERE c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s
		ORDER BY c.ORDINAL_POSITION
	`, objectID, objectID, quoteString(schema), quoteString(table))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	var columns []protocol.ColumnInfo
	for rows.Next() {
		var col protocol.ColumnInfo
		var baseDataType string
		var maxLength, precision, scale sql.NullInt64
		var defaultValue sql.NullString
		var isIdentity, isComputed sql.NullInt64

		if err := rows.Scan(
			&col.Name,
			&col.Position,
			&baseDataType,
			&col.IsNullable,
			&maxLength,
			&precision,
			&scale,
			&defaultValue,
			&isIdentity,
			&isComputed,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		// Format full data type string (e.g., "nvarchar(260)", "decimal(10,2)")
		col.DataType = formatDataType(baseDataType, maxLength, precision, scale)

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
		if isIdentity.Valid && isIdentity.Int64 == 1 {
			col.IsIdentity = true
		}
		if isComputed.Valid && isComputed.Int64 == 1 {
			col.IsComputed = true
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
	query := fmt.Sprintf(`
		SELECT
			kc.CONSTRAINT_NAME,
			kc.COLUMN_NAME,
			kc.ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
			ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA
		WHERE tc.TABLE_SCHEMA = %s
			AND tc.TABLE_NAME = %s
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kc.ORDINAL_POSITION
	`, quoteString(schema), quoteString(table))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}
	defer rows.Close()

	var constraintName string
	var columns []string
	for rows.Next() {
		var name, colName string
		var position int
		if err := rows.Scan(&name, &colName, &position); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		constraintName = name
		columns = append(columns, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key: %w", err)
	}

	if len(columns) == 0 {
		return &protocol.GetPrimaryKeyResponse{PrimaryKey: nil}, nil
	}

	return &protocol.GetPrimaryKeyResponse{
		PrimaryKey: &protocol.PrimaryKeyInfo{
			Name:    constraintName,
			Columns: columns,
		},
	}, nil
}

// GetForeignKeys returns foreign key constraints for a table.
func (d *Driver) GetForeignKeys(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetForeignKeysResponse, error) {
	query := fmt.Sprintf(`
		SELECT
			fk.name AS constraint_name,
			COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
			SCHEMA_NAME(ref_t.schema_id) AS referenced_schema,
			ref_t.name AS referenced_table,
			COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column,
			fk.delete_referential_action_desc AS on_delete,
			fk.update_referential_action_desc AS on_update,
			fkc.constraint_column_id AS position
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.tables t ON fk.parent_object_id = t.object_id
		JOIN sys.tables ref_t ON fk.referenced_object_id = ref_t.object_id
		WHERE SCHEMA_NAME(t.schema_id) = %s AND t.name = %s
		ORDER BY fk.name, fkc.constraint_column_id
	`, quoteString(schema), quoteString(table))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer rows.Close()

	// Group columns by constraint name
	fkMap := make(map[string]*protocol.ForeignKeyInfo)
	var fkOrder []string

	for rows.Next() {
		var constraintName, colName, refSchema, refTable, refCol, onDelete, onUpdate string
		var position int
		if err := rows.Scan(&constraintName, &colName, &refSchema, &refTable, &refCol, &onDelete, &onUpdate, &position); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}

		if _, exists := fkMap[constraintName]; !exists {
			fkMap[constraintName] = &protocol.ForeignKeyInfo{
				Name:              constraintName,
				Columns:           []string{},
				ReferencedSchema:  refSchema,
				ReferencedTable:   refTable,
				ReferencedColumns: []string{},
				OnDelete:          normalizeRefAction(onDelete),
				OnUpdate:          normalizeRefAction(onUpdate),
			}
			fkOrder = append(fkOrder, constraintName)
		}

		fk := fkMap[constraintName]
		fk.Columns = append(fk.Columns, colName)
		fk.ReferencedColumns = append(fk.ReferencedColumns, refCol)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating foreign keys: %w", err)
	}

	var foreignKeys []protocol.ForeignKeyInfo
	for _, name := range fkOrder {
		foreignKeys = append(foreignKeys, *fkMap[name])
	}

	return &protocol.GetForeignKeysResponse{ForeignKeys: foreignKeys}, nil
}

// GetUniqueConstraints returns unique constraints for a table.
func (d *Driver) GetUniqueConstraints(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetUniqueConstraintsResponse, error) {
	query := fmt.Sprintf(`
		SELECT
			tc.CONSTRAINT_NAME,
			kc.COLUMN_NAME,
			kc.ORDINAL_POSITION,
			CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END AS is_pk
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
			ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA
		WHERE tc.TABLE_SCHEMA = %s
			AND tc.TABLE_NAME = %s
			AND tc.CONSTRAINT_TYPE IN ('UNIQUE', 'PRIMARY KEY')
		ORDER BY tc.CONSTRAINT_NAME, kc.ORDINAL_POSITION
	`, quoteString(schema), quoteString(table))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get unique constraints: %w", err)
	}
	defer rows.Close()

	ucMap := make(map[string]*protocol.UniqueConstraintInfo)
	var ucOrder []string

	for rows.Next() {
		var constraintName, colName string
		var position int
		var isPK bool
		if err := rows.Scan(&constraintName, &colName, &position, &isPK); err != nil {
			return nil, fmt.Errorf("failed to scan unique constraint: %w", err)
		}

		if _, exists := ucMap[constraintName]; !exists {
			ucMap[constraintName] = &protocol.UniqueConstraintInfo{
				Name:         constraintName,
				Columns:      []string{},
				IsPrimaryKey: isPK,
			}
			ucOrder = append(ucOrder, constraintName)
		}

		ucMap[constraintName].Columns = append(ucMap[constraintName].Columns, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating unique constraints: %w", err)
	}

	var constraints []protocol.UniqueConstraintInfo
	for _, name := range ucOrder {
		constraints = append(constraints, *ucMap[name])
	}

	return &protocol.GetUniqueConstraintsResponse{UniqueConstraints: constraints}, nil
}

// GetIndexes returns index information for a table.
func (d *Driver) GetIndexes(ctx context.Context, db *sql.DB, schema, table string) (*protocol.GetIndexesResponse, error) {
	query := fmt.Sprintf(`
		SELECT
			i.name AS index_name,
			c.name AS column_name,
			ic.key_ordinal AS position,
			ic.is_descending_key,
			ic.is_included_column,
			i.is_unique,
			i.is_primary_key,
			i.type_desc AS index_type,
			CASE WHEN i.type = 1 THEN 1 ELSE 0 END AS is_clustered
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		JOIN sys.tables t ON i.object_id = t.object_id
		WHERE SCHEMA_NAME(t.schema_id) = %s
			AND t.name = %s
			AND i.name IS NOT NULL
		ORDER BY i.name, ic.key_ordinal, ic.index_column_id
	`, quoteString(schema), quoteString(table))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer rows.Close()

	idxMap := make(map[string]*protocol.IndexInfo)
	var idxOrder []string

	for rows.Next() {
		var indexName, colName, indexType string
		var position int
		var isDesc, isIncluded, isUnique, isPK, isClustered bool
		if err := rows.Scan(&indexName, &colName, &position, &isDesc, &isIncluded, &isUnique, &isPK, &indexType, &isClustered); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		if _, exists := idxMap[indexName]; !exists {
			idxMap[indexName] = &protocol.IndexInfo{
				Name:         indexName,
				Columns:      []protocol.IndexColumnInfo{},
				IsUnique:     isUnique,
				IsPrimaryKey: isPK,
				IsClustered:  isClustered,
				Type:         normalizeIndexType(indexType),
			}
			idxOrder = append(idxOrder, indexName)
		}

		idxMap[indexName].Columns = append(idxMap[indexName].Columns, protocol.IndexColumnInfo{
			Name:         colName,
			Position:     position,
			IsDescending: isDesc,
			IsIncluded:   isIncluded,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}

	var indexes []protocol.IndexInfo
	for _, name := range idxOrder {
		indexes = append(indexes, *idxMap[name])
	}

	return &protocol.GetIndexesResponse{Indexes: indexes}, nil
}

// GetRowCount returns the row count for a table.
func (d *Driver) GetRowCount(ctx context.Context, db *sql.DB, schema, table string, exact bool) (*protocol.RowCountResponse, error) {
	// Validate identifiers to prevent SQL injection
	if err := driver.ValidateSchemaTable(schema, table); err != nil {
		return nil, err
	}

	var count int64
	var isExact bool

	if exact {
		// Exact count using COUNT(*)
		query := fmt.Sprintf("SELECT COUNT(*) FROM [%s].[%s]", schema, table)
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to get exact row count: %w", err)
		}
		isExact = true
	} else {
		// Estimated count from sys.dm_db_partition_stats
		query := fmt.Sprintf(`
			SELECT SUM(p.rows) AS row_count
			FROM sys.partitions p
			JOIN sys.tables t ON p.object_id = t.object_id
			WHERE SCHEMA_NAME(t.schema_id) = %s
				AND t.name = %s
				AND p.index_id IN (0, 1)  -- Heap or clustered index
		`, quoteString(schema), quoteString(table))
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to get estimated row count: %w", err)
		}
		isExact = false
	}

	return &protocol.RowCountResponse{
		RowCount: count,
		IsExact:  isExact,
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

	// Query sample rows
	query := fmt.Sprintf("SELECT TOP %d * FROM [%s].[%s]", limit, schema, table)
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
			SERVERPROPERTY('ProductVersion') AS product_version,
			DB_NAME() AS database_name,
			SCHEMA_NAME() AS default_schema,
			DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS collation
	`

	var version, dbName, defaultSchema string
	var collation sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&version, &dbName, &defaultSchema, &collation); err != nil {
		return nil, fmt.Errorf("failed to get database info: %w", err)
	}

	info := protocol.DatabaseInfo{
		ProductName:    "Microsoft SQL Server",
		ProductVersion: version,
		DatabaseName:   dbName,
		DefaultSchema:  defaultSchema,
	}

	if collation.Valid {
		info.Collation = collation.String
	}

	return &protocol.GetDatabaseInfoResponse{Database: info}, nil
}

// GetColumnStats returns cardinality statistics for a column.
// Uses SQL Server metadata and statistics for fast lookups without table scans.
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

	// First, check if column has a unique constraint or is part of unique index
	// This is very fast - just metadata lookup
	uniqueCheckQuery := fmt.Sprintf(`
		SELECT CASE WHEN EXISTS (
			SELECT 1
			FROM sys.indexes i
			JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			WHERE i.object_id = OBJECT_ID('[%s].[%s]')
			  AND c.name = '%s'
			  AND i.is_unique = 1
			  AND (SELECT COUNT(*) FROM sys.index_columns ic2
			       WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id) = 1
		) THEN 1 ELSE 0 END
	`, schema, table, column)

	var hasUniqueIndex int
	if err := db.QueryRowContext(ctx, uniqueCheckQuery).Scan(&hasUniqueIndex); err != nil {
		hasUniqueIndex = 0 // Continue even if this fails
	}

	// Get row count from partition stats (no table scan)
	rowCountQuery := fmt.Sprintf(`
		SELECT ISNULL(SUM(p.rows), 0)
		FROM sys.partitions p
		WHERE p.object_id = OBJECT_ID('[%s].[%s]')
		  AND p.index_id IN (0, 1)
	`, schema, table)

	var totalCount int64
	if err := db.QueryRowContext(ctx, rowCountQuery).Scan(&totalCount); err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// If column has unique index, we know distinct = total (minus nulls)
	if hasUniqueIndex == 1 {
		return &protocol.ColumnStatsResponse{
			TotalCount:    totalCount,
			DistinctCount: totalCount, // Unique means all distinct
			NullCount:     0,          // Estimate - unique columns rarely have many nulls
			IsUnique:      true,
			SampleValues:  []interface{}{},
		}, nil
	}

	// Try to get distinct count from statistics metadata (no table scan)
	// This uses existing auto-created or manual statistics
	statsMetaQuery := fmt.Sprintf(`
		SELECT TOP 1
			sp.rows,
			sp.rows_sampled,
			sp.unfiltered_rows
		FROM sys.stats s
		CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
		JOIN sys.stats_columns sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id
		JOIN sys.columns c ON sc.object_id = c.object_id AND sc.column_id = c.column_id
		WHERE s.object_id = OBJECT_ID('[%s].[%s]')
		  AND c.name = '%s'
		ORDER BY sp.last_updated DESC
	`, schema, table, column)

	var statRows, rowsSampled, unfilteredRows sql.NullInt64
	err := db.QueryRowContext(ctx, statsMetaQuery).Scan(&statRows, &rowsSampled, &unfilteredRows)

	// If we have statistics, estimate uniqueness from sampling ratio
	if err == nil && statRows.Valid && rowsSampled.Valid && rowsSampled.Int64 > 0 {
		// If sampled rows equals total rows, column is likely unique or near-unique
		samplingRatio := float64(rowsSampled.Int64) / float64(statRows.Int64)
		// High sampling ratio with no duplicates found suggests uniqueness
		isUnique := samplingRatio >= 0.95

		return &protocol.ColumnStatsResponse{
			TotalCount:    totalCount,
			DistinctCount: rowsSampled.Int64, // Use sampled distinct as estimate
			NullCount:     0,
			IsUnique:      isUnique,
			SampleValues:  []interface{}{},
		}, nil
	}

	// Fallback: For small tables or when no stats exist, do a quick sample-based check
	// Only scan if table is small (< 10000 rows) to avoid expensive queries
	if totalCount < 10000 {
		fallbackQuery := fmt.Sprintf(`
			SELECT
				COUNT(*) AS total_count,
				COUNT(DISTINCT [%s]) AS distinct_count,
				COUNT(*) - COUNT([%s]) AS null_count
			FROM [%s].[%s]
		`, column, column, schema, table)

		var distinctCount, nullCount int64
		if err := db.QueryRowContext(ctx, fallbackQuery).Scan(&totalCount, &distinctCount, &nullCount); err != nil {
			return nil, fmt.Errorf("failed to get column stats: %w", err)
		}

		nonNullCount := totalCount - nullCount
		isUnique := nonNullCount > 0 && distinctCount == nonNullCount

		return &protocol.ColumnStatsResponse{
			TotalCount:    totalCount,
			DistinctCount: distinctCount,
			NullCount:     nullCount,
			IsUnique:      isUnique,
			SampleValues:  []interface{}{},
		}, nil
	}

	// For large tables without stats, assume not unique (safer default)
	return &protocol.ColumnStatsResponse{
		TotalCount:    totalCount,
		DistinctCount: totalCount / 2, // Conservative estimate
		NullCount:     0,
		IsUnique:      false,
		SampleValues:  []interface{}{},
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
		SELECT COUNT(DISTINCT [%s]) FROM [%s].[%s] WHERE [%s] IS NOT NULL
	`, leftColumn, leftSchema, leftTable, leftColumn)

	rightStatsQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT [%s]) FROM [%s].[%s] WHERE [%s] IS NOT NULL
	`, rightColumn, rightSchema, rightTable, rightColumn)

	var leftTotalDistinct, rightTotalDistinct int64
	if err := db.QueryRowContext(ctx, leftStatsQuery).Scan(&leftTotalDistinct); err != nil {
		return nil, fmt.Errorf("failed to get left distinct count: %w", err)
	}
	if err := db.QueryRowContext(ctx, rightStatsQuery).Scan(&rightTotalDistinct); err != nil {
		return nil, fmt.Errorf("failed to get right distinct count: %w", err)
	}

	// Check overlap using a sampled join
	overlapQuery := fmt.Sprintf(`
		WITH left_sample AS (
			SELECT DISTINCT TOP %d [%s] AS val
			FROM [%s].[%s]
			WHERE [%s] IS NOT NULL
		),
		overlap AS (
			SELECT ls.val
			FROM left_sample ls
			WHERE EXISTS (
				SELECT 1 FROM [%s].[%s] r
				WHERE r.[%s] = ls.val
			)
		)
		SELECT
			(SELECT COUNT(*) FROM left_sample) AS sample_size,
			(SELECT COUNT(*) FROM overlap) AS overlap_count
	`, sampleSize, leftColumn, leftSchema, leftTable, leftColumn,
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
	leftCountQuery := fmt.Sprintf(`SELECT COUNT(*) FROM [%s].[%s]`, leftSchema, leftTable)
	rightCountQuery := fmt.Sprintf(`SELECT COUNT(*) FROM [%s].[%s]`, rightSchema, rightTable)

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

func normalizeRefAction(action string) string {
	switch action {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL":
		return "SET NULL"
	case "SET_DEFAULT":
		return "SET DEFAULT"
	case "NO_ACTION":
		return "NO ACTION"
	default:
		return action
	}
}

func normalizeIndexType(t string) string {
	switch t {
	case "CLUSTERED":
		return "BTREE"
	case "NONCLUSTERED":
		return "BTREE"
	case "HEAP":
		return "HEAP"
	default:
		return t
	}
}

// formatDataType formats the full SQL Server data type string including length/precision/scale.
// e.g., "nvarchar" + maxLength=260 -> "nvarchar(260)"
// e.g., "decimal" + precision=10, scale=2 -> "decimal(10,2)"
func formatDataType(baseType string, maxLength, precision, scale sql.NullInt64) string {
	baseType = strings.ToLower(baseType)

	switch baseType {
	// Character types with length
	case "char", "varchar", "nchar", "nvarchar", "binary", "varbinary":
		if maxLength.Valid {
			if maxLength.Int64 == -1 {
				// -1 means MAX in SQL Server
				return fmt.Sprintf("%s(max)", baseType)
			}
			return fmt.Sprintf("%s(%d)", baseType, maxLength.Int64)
		}
		return baseType

	// Numeric types with precision and scale
	case "decimal", "numeric":
		if precision.Valid && scale.Valid {
			return fmt.Sprintf("%s(%d,%d)", baseType, precision.Int64, scale.Int64)
		} else if precision.Valid {
			return fmt.Sprintf("%s(%d)", baseType, precision.Int64)
		}
		return baseType

	// Types that don't need parameters
	default:
		return baseType
	}
}

// init registers the MSSQL driver with the default registry.
func init() {
	driver.Register(New())
}
