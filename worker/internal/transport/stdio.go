package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/mantis/worker/internal/protocol"
)

// StdioTransport implements Transport using stdin/stdout.
// Uses NDJSON (newline-delimited JSON) format.
type StdioTransport struct {
	reader  *bufio.Reader
	writer  io.Writer
	writeMu sync.Mutex
}

// NewStdioTransport creates a new StdioTransport.
func NewStdioTransport(reader io.Reader, writer io.Writer) *StdioTransport {
	return &StdioTransport{
		reader: bufio.NewReader(reader),
		writer: writer,
	}
}

// UTF-8 BOM bytes
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// Read reads the next request from stdin.
func (t *StdioTransport) Read(ctx context.Context) (*protocol.RequestEnvelope, error) {
	// Read a line (NDJSON format)
	line, err := t.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read request: %w", err)
	}

	// Strip UTF-8 BOM if present (common when input comes from Windows)
	if len(line) >= 3 && line[0] == utf8BOM[0] && line[1] == utf8BOM[1] && line[2] == utf8BOM[2] {
		line = line[3:]
	}

	// Parse JSON
	var req protocol.RequestEnvelope
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, fmt.Errorf("failed to parse request: %w", err)
	}

	// Validate required fields
	if req.ID == "" {
		return nil, fmt.Errorf("request missing required 'id' field")
	}
	if req.Method == "" {
		return nil, fmt.Errorf("request missing required 'method' field")
	}

	return &req, nil
}

// Write writes a response to stdout.
func (t *StdioTransport) Write(ctx context.Context, response *protocol.ResponseEnvelope) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	// Write JSON followed by newline (NDJSON format)
	if _, err := t.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	if _, err := t.writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

// Close is a no-op for stdio transport.
func (t *StdioTransport) Close() error {
	return nil
}
