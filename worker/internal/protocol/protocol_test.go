package protocol

import (
	"encoding/json"
	"testing"
)

// --- Envelope Tests ---

func TestRequestEnvelope_Marshal(t *testing.T) {
	tests := []struct {
		name     string
		envelope RequestEnvelope
		want     string
	}{
		{
			name: "basic request without params",
			envelope: RequestEnvelope{
				ID:     "req-001",
				Method: "metadata.list_schemas",
			},
			want: `{"id":"req-001","method":"metadata.list_schemas"}`,
		},
		{
			name: "request with params",
			envelope: RequestEnvelope{
				ID:     "req-002",
				Method: "metadata.get_columns",
				Params: json.RawMessage(`{"driver":"mssql","schema":"dbo","table":"users"}`),
			},
			want: `{"id":"req-002","method":"metadata.get_columns","params":{"driver":"mssql","schema":"dbo","table":"users"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.envelope)
			if err != nil {
				t.Fatalf("Marshal error: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("Marshal() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestRequestEnvelope_Unmarshal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantID  string
		wantMet string
	}{
		{
			name:    "basic request",
			input:   `{"id":"req-001","method":"metadata.list_schemas"}`,
			wantID:  "req-001",
			wantMet: "metadata.list_schemas",
		},
		{
			name:    "request with params",
			input:   `{"id":"req-002","method":"metadata.get_table","params":{"schema":"dbo","table":"orders"}}`,
			wantID:  "req-002",
			wantMet: "metadata.get_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var env RequestEnvelope
			if err := json.Unmarshal([]byte(tt.input), &env); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if env.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", env.ID, tt.wantID)
			}
			if env.Method != tt.wantMet {
				t.Errorf("Method = %q, want %q", env.Method, tt.wantMet)
			}
		})
	}
}

func TestRequestEnvelope_ParseParams(t *testing.T) {
	input := `{"id":"req-001","method":"metadata.get_columns","params":{"driver":"mssql","connection_string":"server=localhost","schema":"dbo","table":"users"}}`

	var env RequestEnvelope
	if err := json.Unmarshal([]byte(input), &env); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	var params GetColumnsParams
	if err := env.ParseParams(&params); err != nil {
		t.Fatalf("ParseParams error: %v", err)
	}

	if params.Driver != "mssql" {
		t.Errorf("Driver = %q, want %q", params.Driver, "mssql")
	}
	if params.Schema != "dbo" {
		t.Errorf("Schema = %q, want %q", params.Schema, "dbo")
	}
	if params.Table != "users" {
		t.Errorf("Table = %q, want %q", params.Table, "users")
	}
}

func TestResponseEnvelope_Success(t *testing.T) {
	result := ListSchemasResponse{
		Schemas: []SchemaInfo{
			{Name: "dbo", IsDefault: true},
			{Name: "sales", IsDefault: false},
		},
	}

	env, err := NewSuccessResponse("req-001", result)
	if err != nil {
		t.Fatalf("NewSuccessResponse error: %v", err)
	}

	if env.ID != "req-001" {
		t.Errorf("ID = %q, want %q", env.ID, "req-001")
	}
	if !env.Success {
		t.Error("Success = false, want true")
	}
	if env.Error != nil {
		t.Errorf("Error = %v, want nil", env.Error)
	}

	// Verify JSON serialization
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Unmarshal and verify result
	var decoded ResponseEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	var schemas ListSchemasResponse
	if err := decoded.UnmarshalResult(&schemas); err != nil {
		t.Fatalf("UnmarshalResult error: %v", err)
	}

	if len(schemas.Schemas) != 2 {
		t.Errorf("len(Schemas) = %d, want 2", len(schemas.Schemas))
	}
	if schemas.Schemas[0].Name != "dbo" {
		t.Errorf("Schemas[0].Name = %q, want %q", schemas.Schemas[0].Name, "dbo")
	}
}

func TestResponseEnvelope_Error(t *testing.T) {
	env := NewErrorResponse("req-001", ErrCodeQueryFailed, "connection timeout", map[string]interface{}{
		"host":    "localhost",
		"timeout": 30,
	})

	if env.ID != "req-001" {
		t.Errorf("ID = %q, want %q", env.ID, "req-001")
	}
	if env.Success {
		t.Error("Success = true, want false")
	}
	if env.Error == nil {
		t.Fatal("Error = nil, want error")
	}
	if env.Error.Code != ErrCodeQueryFailed {
		t.Errorf("Error.Code = %q, want %q", env.Error.Code, ErrCodeQueryFailed)
	}
	if env.Error.Message != "connection timeout" {
		t.Errorf("Error.Message = %q, want %q", env.Error.Message, "connection timeout")
	}

	// Verify JSON serialization
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	want := `{"id":"req-001","success":false,"error":{"code":"QUERY_FAILED","message":"connection timeout","details":{"host":"localhost","timeout":30}}}`
	if string(data) != want {
		t.Errorf("Marshal() = %s, want %s", data, want)
	}
}

// --- Request Types Tests ---

func TestConnectionParams_JSON(t *testing.T) {
	params := ConnectionParams{
		Driver:           "mssql",
		ConnectionString: "server=localhost;database=testdb",
	}

	data, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ConnectionParams
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Driver != params.Driver {
		t.Errorf("Driver = %q, want %q", decoded.Driver, params.Driver)
	}
	if decoded.ConnectionString != params.ConnectionString {
		t.Errorf("ConnectionString = %q, want %q", decoded.ConnectionString, params.ConnectionString)
	}
}

func TestGetColumnsParams_JSON(t *testing.T) {
	input := `{"driver":"duckdb","connection_string":":memory:","schema":"main","table":"customers"}`

	var params GetColumnsParams
	if err := json.Unmarshal([]byte(input), &params); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if params.Driver != "duckdb" {
		t.Errorf("Driver = %q, want %q", params.Driver, "duckdb")
	}
	if params.ConnectionString != ":memory:" {
		t.Errorf("ConnectionString = %q, want %q", params.ConnectionString, ":memory:")
	}
	if params.Schema != "main" {
		t.Errorf("Schema = %q, want %q", params.Schema, "main")
	}
	if params.Table != "customers" {
		t.Errorf("Table = %q, want %q", params.Table, "customers")
	}
}

func TestSampleRowsParams_DefaultLimit(t *testing.T) {
	input := `{"driver":"mssql","connection_string":"server=localhost","schema":"dbo","table":"orders"}`

	var params SampleRowsParams
	if err := json.Unmarshal([]byte(input), &params); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	// Limit should be zero value when not specified
	if params.Limit != 0 {
		t.Errorf("Limit = %d, want 0 (default)", params.Limit)
	}
}

func TestSampleRowsParams_WithLimit(t *testing.T) {
	input := `{"driver":"mssql","connection_string":"server=localhost","schema":"dbo","table":"orders","limit":25}`

	var params SampleRowsParams
	if err := json.Unmarshal([]byte(input), &params); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if params.Limit != 25 {
		t.Errorf("Limit = %d, want 25", params.Limit)
	}
}

func TestExecuteQueryParams_WithArgs(t *testing.T) {
	params := ExecuteQueryParams{
		ConnectionParams: ConnectionParams{
			Driver:           "mssql",
			ConnectionString: "server=localhost",
		},
		SQL:  "SELECT * FROM users WHERE id = ? AND status = ?",
		Args: []interface{}{123, "active"},
	}

	data, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ExecuteQueryParams
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.SQL != params.SQL {
		t.Errorf("SQL = %q, want %q", decoded.SQL, params.SQL)
	}
	if len(decoded.Args) != 2 {
		t.Errorf("len(Args) = %d, want 2", len(decoded.Args))
	}
}

// --- Response Types Tests ---

func TestSchemaInfo_JSON(t *testing.T) {
	schemas := []SchemaInfo{
		{Name: "dbo", IsDefault: true},
		{Name: "sales", IsDefault: false},
		{Name: "hr"},
	}

	data, err := json.Marshal(schemas)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded []SchemaInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(decoded) != 3 {
		t.Fatalf("len(decoded) = %d, want 3", len(decoded))
	}
	if decoded[0].Name != "dbo" || !decoded[0].IsDefault {
		t.Errorf("decoded[0] = %+v, want {Name:dbo IsDefault:true}", decoded[0])
	}
	if decoded[1].Name != "sales" || decoded[1].IsDefault {
		t.Errorf("decoded[1] = %+v, want {Name:sales IsDefault:false}", decoded[1])
	}
}

func TestColumnInfo_JSON(t *testing.T) {
	maxLen := 255
	precision := 10
	scale := 2
	defaultVal := "GETDATE()"

	col := ColumnInfo{
		Name:             "created_at",
		Position:         5,
		DataType:         "datetime2",
		IsNullable:       false,
		MaxLength:        &maxLen,
		NumericPrecision: &precision,
		NumericScale:     &scale,
		DefaultValue:     &defaultVal,
		IsIdentity:       false,
		IsComputed:       true,
	}

	data, err := json.Marshal(col)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ColumnInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Name != col.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, col.Name)
	}
	if decoded.Position != col.Position {
		t.Errorf("Position = %d, want %d", decoded.Position, col.Position)
	}
	if decoded.DataType != col.DataType {
		t.Errorf("DataType = %q, want %q", decoded.DataType, col.DataType)
	}
	if decoded.IsNullable != col.IsNullable {
		t.Errorf("IsNullable = %v, want %v", decoded.IsNullable, col.IsNullable)
	}
	if decoded.MaxLength == nil || *decoded.MaxLength != maxLen {
		t.Errorf("MaxLength = %v, want %d", decoded.MaxLength, maxLen)
	}
	if decoded.NumericPrecision == nil || *decoded.NumericPrecision != precision {
		t.Errorf("NumericPrecision = %v, want %d", decoded.NumericPrecision, precision)
	}
	if decoded.DefaultValue == nil || *decoded.DefaultValue != defaultVal {
		t.Errorf("DefaultValue = %v, want %q", decoded.DefaultValue, defaultVal)
	}
	if !decoded.IsComputed {
		t.Error("IsComputed = false, want true")
	}
}

func TestColumnInfo_MinimalJSON(t *testing.T) {
	// Test that optional fields are omitted when nil
	col := ColumnInfo{
		Name:       "id",
		Position:   1,
		DataType:   "int",
		IsNullable: false,
	}

	data, err := json.Marshal(col)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Should not contain optional fields
	dataStr := string(data)
	if contains(dataStr, "max_length") {
		t.Error("JSON should not contain max_length when nil")
	}
	if contains(dataStr, "default_value") {
		t.Error("JSON should not contain default_value when nil")
	}
}

func TestForeignKeyInfo_JSON(t *testing.T) {
	fk := ForeignKeyInfo{
		Name:              "FK_orders_customers",
		Columns:          []string{"customer_id"},
		ReferencedSchema: "dbo",
		ReferencedTable:  "customers",
		ReferencedColumns: []string{"id"},
		OnDelete:         "CASCADE",
		OnUpdate:         "NO ACTION",
	}

	data, err := json.Marshal(fk)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ForeignKeyInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Name != fk.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, fk.Name)
	}
	if len(decoded.Columns) != 1 || decoded.Columns[0] != "customer_id" {
		t.Errorf("Columns = %v, want [customer_id]", decoded.Columns)
	}
	if decoded.ReferencedSchema != "dbo" {
		t.Errorf("ReferencedSchema = %q, want %q", decoded.ReferencedSchema, "dbo")
	}
	if decoded.OnDelete != "CASCADE" {
		t.Errorf("OnDelete = %q, want %q", decoded.OnDelete, "CASCADE")
	}
}

func TestForeignKeyInfo_CompositeKey(t *testing.T) {
	fk := ForeignKeyInfo{
		Name:              "FK_order_items_orders",
		Columns:          []string{"order_id", "line_number"},
		ReferencedSchema: "dbo",
		ReferencedTable:  "orders",
		ReferencedColumns: []string{"id", "line_num"},
	}

	data, err := json.Marshal(fk)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ForeignKeyInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(decoded.Columns) != 2 {
		t.Errorf("len(Columns) = %d, want 2", len(decoded.Columns))
	}
	if len(decoded.ReferencedColumns) != 2 {
		t.Errorf("len(ReferencedColumns) = %d, want 2", len(decoded.ReferencedColumns))
	}
}

func TestIndexInfo_JSON(t *testing.T) {
	idx := IndexInfo{
		Name: "IX_orders_date",
		Columns: []IndexColumnInfo{
			{Name: "order_date", Position: 1, IsDescending: true},
			{Name: "customer_id", Position: 2, IsDescending: false},
		},
		IsUnique:     false,
		IsPrimaryKey: false,
		IsClustered:  false,
		Type:         "BTREE",
	}

	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded IndexInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Name != idx.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, idx.Name)
	}
	if len(decoded.Columns) != 2 {
		t.Fatalf("len(Columns) = %d, want 2", len(decoded.Columns))
	}
	if decoded.Columns[0].Name != "order_date" {
		t.Errorf("Columns[0].Name = %q, want %q", decoded.Columns[0].Name, "order_date")
	}
	if !decoded.Columns[0].IsDescending {
		t.Error("Columns[0].IsDescending = false, want true")
	}
	if decoded.Type != "BTREE" {
		t.Errorf("Type = %q, want %q", decoded.Type, "BTREE")
	}
}

func TestSampleRowsResponse_JSON(t *testing.T) {
	resp := SampleRowsResponse{
		Columns:  []string{"id", "name", "active"},
		Rows: [][]interface{}{
			{1, "Alice", true},
			{2, "Bob", false},
			{3, nil, true},
		},
		RowCount: 3,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded SampleRowsResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(decoded.Columns) != 3 {
		t.Errorf("len(Columns) = %d, want 3", len(decoded.Columns))
	}
	if len(decoded.Rows) != 3 {
		t.Errorf("len(Rows) = %d, want 3", len(decoded.Rows))
	}
	if decoded.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", decoded.RowCount)
	}

	// Check nil handling
	if decoded.Rows[2][1] != nil {
		t.Errorf("Rows[2][1] = %v, want nil", decoded.Rows[2][1])
	}
}

func TestDatabaseInfo_JSON(t *testing.T) {
	info := DatabaseInfo{
		ProductName:    "Microsoft SQL Server",
		ProductVersion: "15.0.2000.5",
		DatabaseName:   "testdb",
		DefaultSchema:  "dbo",
		Collation:      "SQL_Latin1_General_CP1_CI_AS",
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded DatabaseInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.ProductName != info.ProductName {
		t.Errorf("ProductName = %q, want %q", decoded.ProductName, info.ProductName)
	}
	if decoded.ProductVersion != info.ProductVersion {
		t.Errorf("ProductVersion = %q, want %q", decoded.ProductVersion, info.ProductVersion)
	}
	if decoded.DatabaseName != info.DatabaseName {
		t.Errorf("DatabaseName = %q, want %q", decoded.DatabaseName, info.DatabaseName)
	}
}

func TestExecuteQueryResponse_JSON(t *testing.T) {
	affected := int64(5)
	resp := ExecuteQueryResponse{
		Columns: []QueryResultColumn{
			{Name: "id", DataType: "int"},
			{Name: "name", DataType: "varchar"},
		},
		Rows: [][]interface{}{
			{1, "test"},
		},
		RowCount:     1,
		RowsAffected: &affected,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ExecuteQueryResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(decoded.Columns) != 2 {
		t.Errorf("len(Columns) = %d, want 2", len(decoded.Columns))
	}
	if decoded.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", decoded.RowCount)
	}
	if decoded.RowsAffected == nil || *decoded.RowsAffected != 5 {
		t.Errorf("RowsAffected = %v, want 5", decoded.RowsAffected)
	}
}

// --- Full Protocol Round-Trip Tests ---

func TestFullRoundTrip_ListSchemas(t *testing.T) {
	// Simulate a complete request/response cycle
	requestJSON := `{"id":"test-001","method":"metadata.list_schemas","params":{"driver":"mssql","connection_string":"server=localhost"}}`

	var req RequestEnvelope
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		t.Fatalf("Request unmarshal error: %v", err)
	}

	if req.Method != "metadata.list_schemas" {
		t.Errorf("Method = %q, want %q", req.Method, "metadata.list_schemas")
	}

	var params ListSchemasParams
	if err := req.ParseParams(&params); err != nil {
		t.Fatalf("ParseParams error: %v", err)
	}

	if params.Driver != "mssql" {
		t.Errorf("Driver = %q, want %q", params.Driver, "mssql")
	}

	// Create response
	result := ListSchemasResponse{
		Schemas: []SchemaInfo{
			{Name: "dbo", IsDefault: true},
			{Name: "sys", IsDefault: false},
		},
	}

	resp, err := NewSuccessResponse(req.ID, result)
	if err != nil {
		t.Fatalf("NewSuccessResponse error: %v", err)
	}

	responseJSON, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Response marshal error: %v", err)
	}

	// Verify we can unmarshal the response
	var finalResp ResponseEnvelope
	if err := json.Unmarshal(responseJSON, &finalResp); err != nil {
		t.Fatalf("Response unmarshal error: %v", err)
	}

	if !finalResp.Success {
		t.Error("Success = false, want true")
	}

	var schemas ListSchemasResponse
	if err := finalResp.UnmarshalResult(&schemas); err != nil {
		t.Fatalf("UnmarshalResult error: %v", err)
	}

	if len(schemas.Schemas) != 2 {
		t.Errorf("len(Schemas) = %d, want 2", len(schemas.Schemas))
	}
}

func TestFullRoundTrip_Error(t *testing.T) {
	// Simulate error response
	resp := NewErrorResponse("test-002", ErrCodeConnectionFailed, "could not connect to server", map[string]interface{}{
		"server": "localhost",
		"port":   1433,
	})

	responseJSON, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Response marshal error: %v", err)
	}

	var finalResp ResponseEnvelope
	if err := json.Unmarshal(responseJSON, &finalResp); err != nil {
		t.Fatalf("Response unmarshal error: %v", err)
	}

	if finalResp.Success {
		t.Error("Success = true, want false")
	}
	if finalResp.Error == nil {
		t.Fatal("Error = nil, want error")
	}
	if finalResp.Error.Code != ErrCodeConnectionFailed {
		t.Errorf("Error.Code = %q, want %q", finalResp.Error.Code, ErrCodeConnectionFailed)
	}
}

func TestTableDetailInfo_FullMetadata(t *testing.T) {
	table := TableDetailInfo{
		Schema: "dbo",
		Name:   "orders",
		Type:   "TABLE",
		Columns: []ColumnInfo{
			{Name: "id", Position: 1, DataType: "int", IsNullable: false, IsIdentity: true},
			{Name: "customer_id", Position: 2, DataType: "int", IsNullable: false},
			{Name: "order_date", Position: 3, DataType: "datetime2", IsNullable: false},
		},
		PrimaryKey: &PrimaryKeyInfo{
			Name:    "PK_orders",
			Columns: []string{"id"},
		},
		ForeignKeys: []ForeignKeyInfo{
			{
				Name:              "FK_orders_customers",
				Columns:          []string{"customer_id"},
				ReferencedSchema: "dbo",
				ReferencedTable:  "customers",
				ReferencedColumns: []string{"id"},
				OnDelete:         "NO ACTION",
			},
		},
		UniqueConstraints: []UniqueConstraintInfo{
			{
				Name:         "UQ_orders_date_customer",
				Columns:      []string{"order_date", "customer_id"},
				IsPrimaryKey: false,
			},
		},
	}

	data, err := json.Marshal(table)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded TableDetailInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Schema != "dbo" {
		t.Errorf("Schema = %q, want %q", decoded.Schema, "dbo")
	}
	if len(decoded.Columns) != 3 {
		t.Errorf("len(Columns) = %d, want 3", len(decoded.Columns))
	}
	if decoded.PrimaryKey == nil {
		t.Fatal("PrimaryKey = nil, want non-nil")
	}
	if decoded.PrimaryKey.Name != "PK_orders" {
		t.Errorf("PrimaryKey.Name = %q, want %q", decoded.PrimaryKey.Name, "PK_orders")
	}
	if len(decoded.ForeignKeys) != 1 {
		t.Errorf("len(ForeignKeys) = %d, want 1", len(decoded.ForeignKeys))
	}
	if len(decoded.UniqueConstraints) != 1 {
		t.Errorf("len(UniqueConstraints) = %d, want 1", len(decoded.UniqueConstraints))
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
