package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/mantis/worker/internal/protocol"
)

func TestStdioTransport_Read(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantID  string
		wantMet string
		wantErr bool
	}{
		{
			name:    "valid request",
			input:   `{"id":"req-001","method":"metadata.list_schemas"}` + "\n",
			wantID:  "req-001",
			wantMet: "metadata.list_schemas",
		},
		{
			name:    "request with params",
			input:   `{"id":"req-002","method":"metadata.get_columns","params":{"schema":"dbo"}}` + "\n",
			wantID:  "req-002",
			wantMet: "metadata.get_columns",
		},
		{
			name:    "missing id",
			input:   `{"method":"metadata.list_schemas"}` + "\n",
			wantErr: true,
		},
		{
			name:    "missing method",
			input:   `{"id":"req-001"}` + "\n",
			wantErr: true,
		},
		{
			name:    "invalid json",
			input:   `{invalid json}` + "\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			trans := NewStdioTransport(reader, io.Discard)

			req, err := trans.Read(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Error("Read() should return error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Read() error: %v", err)
			}
			if req.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", req.ID, tt.wantID)
			}
			if req.Method != tt.wantMet {
				t.Errorf("Method = %q, want %q", req.Method, tt.wantMet)
			}
		})
	}
}

func TestStdioTransport_Read_WithBOM(t *testing.T) {
	// Test that UTF-8 BOM is stripped from input (common Windows issue)
	bom := "\xEF\xBB\xBF"
	input := bom + `{"id":"req-001","method":"metadata.list_schemas"}` + "\n"

	reader := strings.NewReader(input)
	trans := NewStdioTransport(reader, io.Discard)

	req, err := trans.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	if req.ID != "req-001" {
		t.Errorf("ID = %q, want %q", req.ID, "req-001")
	}
}

func TestStdioTransport_ReadMultiple(t *testing.T) {
	input := `{"id":"req-001","method":"method1"}
{"id":"req-002","method":"method2"}
{"id":"req-003","method":"method3"}
`
	reader := strings.NewReader(input)
	trans := NewStdioTransport(reader, io.Discard)

	// Read first
	req1, err := trans.Read(context.Background())
	if err != nil {
		t.Fatalf("Read 1 error: %v", err)
	}
	if req1.ID != "req-001" {
		t.Errorf("req1.ID = %q, want %q", req1.ID, "req-001")
	}

	// Read second
	req2, err := trans.Read(context.Background())
	if err != nil {
		t.Fatalf("Read 2 error: %v", err)
	}
	if req2.ID != "req-002" {
		t.Errorf("req2.ID = %q, want %q", req2.ID, "req-002")
	}

	// Read third
	req3, err := trans.Read(context.Background())
	if err != nil {
		t.Fatalf("Read 3 error: %v", err)
	}
	if req3.ID != "req-003" {
		t.Errorf("req3.ID = %q, want %q", req3.ID, "req-003")
	}

	// Read EOF
	_, err = trans.Read(context.Background())
	if err != io.EOF {
		t.Errorf("Read 4 should return io.EOF, got %v", err)
	}
}

func TestStdioTransport_Write(t *testing.T) {
	tests := []struct {
		name     string
		response *protocol.ResponseEnvelope
	}{
		{
			name: "success response",
			response: &protocol.ResponseEnvelope{
				ID:      "req-001",
				Success: true,
				Result:  json.RawMessage(`{"schemas":[]}`),
			},
		},
		{
			name: "error response",
			response: &protocol.ResponseEnvelope{
				ID:      "req-002",
				Success: false,
				Error: &protocol.ErrorResponse{
					Code:    protocol.ErrCodeQueryFailed,
					Message: "connection failed",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			trans := NewStdioTransport(strings.NewReader(""), &buf)

			err := trans.Write(context.Background(), tt.response)
			if err != nil {
				t.Fatalf("Write() error: %v", err)
			}

			// Verify output
			output := buf.String()
			if !strings.HasSuffix(output, "\n") {
				t.Error("Output should end with newline")
			}

			// Parse output
			var resp protocol.ResponseEnvelope
			if err := json.Unmarshal([]byte(strings.TrimSuffix(output, "\n")), &resp); err != nil {
				t.Fatalf("Output parse error: %v", err)
			}

			if resp.ID != tt.response.ID {
				t.Errorf("ID = %q, want %q", resp.ID, tt.response.ID)
			}
			if resp.Success != tt.response.Success {
				t.Errorf("Success = %v, want %v", resp.Success, tt.response.Success)
			}
		})
	}
}

func TestStdioTransport_WriteMultiple(t *testing.T) {
	var buf bytes.Buffer
	trans := NewStdioTransport(strings.NewReader(""), &buf)

	responses := []*protocol.ResponseEnvelope{
		{ID: "req-001", Success: true, Result: json.RawMessage(`{}`)},
		{ID: "req-002", Success: true, Result: json.RawMessage(`{}`)},
		{ID: "req-003", Success: false, Error: &protocol.ErrorResponse{Code: "ERR", Message: "test"}},
	}

	for _, resp := range responses {
		if err := trans.Write(context.Background(), resp); err != nil {
			t.Fatalf("Write() error: %v", err)
		}
	}

	// Verify output contains all responses
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("len(lines) = %d, want 3", len(lines))
	}
}

func TestStdioTransport_RoundTrip(t *testing.T) {
	// Simulate a full round-trip
	requestJSON := `{"id":"test-001","method":"metadata.list_schemas","params":{"driver":"duckdb"}}` + "\n"
	var outputBuf bytes.Buffer

	trans := NewStdioTransport(strings.NewReader(requestJSON), &outputBuf)

	// Read request
	req, err := trans.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}

	// Verify request
	if req.ID != "test-001" {
		t.Errorf("req.ID = %q, want %q", req.ID, "test-001")
	}

	// Create and write response
	resp, _ := protocol.NewSuccessResponse(req.ID, protocol.ListSchemasResponse{
		Schemas: []protocol.SchemaInfo{{Name: "main", IsDefault: true}},
	})

	if err := trans.Write(context.Background(), resp); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	// Parse and verify response
	var parsedResp protocol.ResponseEnvelope
	if err := json.Unmarshal(bytes.TrimSuffix(outputBuf.Bytes(), []byte("\n")), &parsedResp); err != nil {
		t.Fatalf("Response parse error: %v", err)
	}

	if parsedResp.ID != "test-001" {
		t.Errorf("resp.ID = %q, want %q", parsedResp.ID, "test-001")
	}
	if !parsedResp.Success {
		t.Error("resp.Success should be true")
	}
}

func TestStdioTransport_Close(t *testing.T) {
	trans := NewStdioTransport(strings.NewReader(""), io.Discard)

	err := trans.Close()
	if err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

// Mock handler for Serve tests
type mockHandler struct {
	responses []*protocol.ResponseEnvelope
	idx       int
}

func (h *mockHandler) Handle(ctx context.Context, req *protocol.RequestEnvelope) *protocol.ResponseEnvelope {
	if h.idx < len(h.responses) {
		resp := h.responses[h.idx]
		h.idx++
		return resp
	}
	return protocol.NewErrorResponse(req.ID, protocol.ErrCodeInternal, "no more responses", nil)
}

func TestServe(t *testing.T) {
	input := `{"id":"req-001","method":"test.method"}
{"id":"req-002","method":"test.method"}
`
	var outputBuf bytes.Buffer

	trans := NewStdioTransport(strings.NewReader(input), &outputBuf)
	handler := &mockHandler{
		responses: []*protocol.ResponseEnvelope{
			{ID: "req-001", Success: true, Result: json.RawMessage(`{"data":1}`)},
			{ID: "req-002", Success: true, Result: json.RawMessage(`{"data":2}`)},
		},
	}

	err := Serve(context.Background(), trans, handler)
	if err != io.EOF {
		t.Errorf("Serve() = %v, want io.EOF", err)
	}

	// Verify responses were written
	lines := strings.Split(strings.TrimSuffix(outputBuf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("len(lines) = %d, want 2", len(lines))
	}
}

func TestServe_ContextCanceled(t *testing.T) {
	// Test that Serve returns when context is canceled before reading
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	input := `{"id":"req-001","method":"test.method"}
`
	trans := NewStdioTransport(strings.NewReader(input), io.Discard)
	handler := &mockHandler{
		responses: []*protocol.ResponseEnvelope{
			{ID: "req-001", Success: true, Result: json.RawMessage(`{}`)},
		},
	}

	err := Serve(ctx, trans, handler)
	if err != context.Canceled {
		t.Errorf("Serve() = %v, want context.Canceled", err)
	}
}
