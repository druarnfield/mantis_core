// Package handler provides the request handler that routes requests to drivers.
package handler

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/mantis/worker/internal/driver"
	"github.com/mantis/worker/internal/pool"
	"github.com/mantis/worker/internal/protocol"
)

// Handler processes protocol requests by routing to appropriate drivers.
type Handler struct {
	registry *driver.Registry
	pool     *pool.Manager // optional connection pool
}

// New creates a new Handler with the given driver registry.
func New(registry *driver.Registry) *Handler {
	return &Handler{registry: registry}
}

// NewWithPool creates a new Handler with the given driver registry and connection pool.
func NewWithPool(registry *driver.Registry, poolManager *pool.Manager) *Handler {
	return &Handler{
		registry: registry,
		pool:     poolManager,
	}
}

// NewWithDefaultRegistry creates a new Handler using the default driver registry.
func NewWithDefaultRegistry() *Handler {
	return New(driver.DefaultRegistry)
}

// NewWithDefaultRegistryAndPool creates a new Handler using the default driver registry
// and the given connection pool.
func NewWithDefaultRegistryAndPool(poolManager *pool.Manager) *Handler {
	return NewWithPool(driver.DefaultRegistry, poolManager)
}

// Handle processes a request and returns a response.
func (h *Handler) Handle(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	// Parse method to get category and operation
	parts := strings.SplitN(req.Method, ".", 2)
	if len(parts) != 2 {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeMethodNotFound,
			fmt.Sprintf("invalid method format: %s", req.Method), nil)
	}

	category := parts[0]
	operation := parts[1]

	switch category {
	case "metadata":
		return h.handleMetadata(ctx, req, operation)
	case "query":
		return h.handleQuery(ctx, req, operation)
	default:
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeMethodNotFound,
			fmt.Sprintf("unknown method category: %s", category), nil)
	}
}

// handleMetadata handles metadata.* methods.
func (h *Handler) handleMetadata(ctx context.Context, req *protocol.RequestEnvelope, operation string) *protocol.ResponseEnvelope {
	switch operation {
	case "list_schemas":
		return h.handleListSchemas(ctx, req)
	case "list_tables":
		return h.handleListTables(ctx, req)
	case "get_table":
		return h.handleGetTable(ctx, req)
	case "get_columns":
		return h.handleGetColumns(ctx, req)
	case "get_primary_key":
		return h.handleGetPrimaryKey(ctx, req)
	case "get_foreign_keys":
		return h.handleGetForeignKeys(ctx, req)
	case "get_unique_constraints":
		return h.handleGetUniqueConstraints(ctx, req)
	case "get_indexes":
		return h.handleGetIndexes(ctx, req)
	case "get_row_count":
		return h.handleGetRowCount(ctx, req)
	case "sample_rows":
		return h.handleSampleRows(ctx, req)
	case "get_database_info":
		return h.handleGetDatabaseInfo(ctx, req)
	case "get_column_stats":
		return h.handleGetColumnStats(ctx, req)
	case "check_value_overlap":
		return h.handleCheckValueOverlap(ctx, req)
	default:
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeMethodNotFound,
			fmt.Sprintf("unknown metadata operation: %s", operation), nil)
	}
}

// handleQuery handles query.* methods.
func (h *Handler) handleQuery(ctx context.Context, req *protocol.RequestEnvelope, operation string) *protocol.ResponseEnvelope {
	switch operation {
	case "execute":
		return h.handleExecuteQuery(ctx, req)
	default:
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeMethodNotFound,
			fmt.Sprintf("unknown query operation: %s", operation), nil)
	}
}

// connectResult holds the result of a connection attempt.
type connectResult struct {
	driver driver.Driver
	db     *sql.DB
	pooled bool // if true, caller should NOT close db
}

// connect returns a database connection, using the pool if available.
// The returned connectResult.pooled indicates whether the connection is from a pool.
// If pooled is true, the caller should NOT close the connection.
// If pooled is false, the caller should close the connection when done.
func (h *Handler) connect(ctx context.Context, driverName, connStr string) (*connectResult, error) {
	d, err := h.registry.Get(driverName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errDriverNotFound, driverName)
	}

	// Use pool if available
	if h.pool != nil {
		db, err := h.pool.GetConnection(ctx, driverName, connStr)
		if err != nil {
			// Sanitize error to avoid leaking connection string with passwords
			return nil, fmt.Errorf("%w: %s", errConnectionFailed, sanitizeError(err.Error()))
		}
		return &connectResult{driver: d, db: db, pooled: true}, nil
	}

	// No pool - create new connection
	db, err := d.Connect(ctx, connStr)
	if err != nil {
		// Sanitize error to avoid leaking connection string with passwords
		return nil, fmt.Errorf("%w: %s", errConnectionFailed, sanitizeError(err.Error()))
	}

	return &connectResult{driver: d, db: db, pooled: false}, nil
}

var (
	errDriverNotFound   = fmt.Errorf("driver not found")
	errConnectionFailed = fmt.Errorf("connection failed")
)

// sanitizeError removes sensitive information from error messages.
// This prevents leaking passwords from connection strings.
func sanitizeError(msg string) string {
	// Common password patterns in connection strings
	patterns := []string{
		`(?i)(password|pwd|passwd)=[^;& ]*`,
		`(?i)(secret|token|key)=[^;& ]*`,
	}

	result := msg
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, "${1}=***")
	}
	return result
}

// --- Metadata handlers ---

func (h *Handler) handleListSchemas(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.ListSchemasParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.ListSchemas(ctx, conn.db)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleListTables(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.ListTablesParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.ListTables(ctx, conn.db, params.Schema)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetTable(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetTableParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetTable(ctx, conn.db, params.Schema, params.Table)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetColumns(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetColumnsParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetColumns(ctx, conn.db, params.Schema, params.Table)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetPrimaryKey(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetPrimaryKeyParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetPrimaryKey(ctx, conn.db, params.Schema, params.Table)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetForeignKeys(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetForeignKeysParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetForeignKeys(ctx, conn.db, params.Schema, params.Table)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetUniqueConstraints(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetUniqueConstraintsParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetUniqueConstraints(ctx, conn.db, params.Schema, params.Table)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetIndexes(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetIndexesParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetIndexes(ctx, conn.db, params.Schema, params.Table)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetRowCount(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetRowCountParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetRowCount(ctx, conn.db, params.Schema, params.Table, params.Exact)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleSampleRows(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.SampleRowsParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.SampleRows(ctx, conn.db, params.Schema, params.Table, params.Limit)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleGetDatabaseInfo(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetDatabaseInfoParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetDatabaseInfo(ctx, conn.db)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

// --- Cardinality Discovery handlers ---

func (h *Handler) handleGetColumnStats(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.GetColumnStatsParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.GetColumnStats(ctx, conn.db, params.Schema, params.Table, params.Column, params.SampleSize)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

func (h *Handler) handleCheckValueOverlap(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.CheckValueOverlapParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.CheckValueOverlap(ctx, conn.db,
		params.LeftSchema, params.LeftTable, params.LeftColumn,
		params.RightSchema, params.RightTable, params.RightColumn,
		params.SampleSize)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

// --- Query handlers ---

func (h *Handler) handleExecuteQuery(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	var params protocol.ExecuteQueryParams
	if err := req.ParseParams(&params); err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInvalidRequest,
			fmt.Sprintf("invalid params: %v", err), nil)
	}

	conn, err := h.connect(ctx, params.Driver, params.ConnectionString)
	if err != nil {
		return h.errorResponse(req.ID, err)
	}
	if !conn.pooled {
		defer conn.db.Close()
	}

	result, err := conn.driver.ExecuteQuery(ctx, conn.db, params.SQL, params.Args)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeQueryFailed, err.Error(), nil)
	}

	resp, err := protocol.NewSuccessResponse(req.ID, result)
	if err != nil {
		return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, err.Error(), nil)
	}
	return resp
}

// errorResponse creates an appropriate error response based on the error type.
func (h *Handler) errorResponse(id string, err error) *protocol.ResponseEnvelope {
	switch {
	case strings.Contains(err.Error(), errDriverNotFound.Error()):
		return protocol.NewErrorResponse(id, protocol.ErrCodeDriverNotFound, err.Error(), nil)
	case strings.Contains(err.Error(), errConnectionFailed.Error()):
		return protocol.NewErrorResponse(id, protocol.ErrCodeConnectionFailed, err.Error(), nil)
	default:
		return protocol.NewErrorResponse(id, protocol.ErrCodeInternal, err.Error(), nil)
	}
}
