// Package transport provides communication interfaces for the worker.
//
// Transports handle reading requests and writing responses. Currently supports:
// - Stdio: stdin/stdout communication for local development
// - (Future) Redis Streams: for deployed environments
package transport

import (
	"context"

	"github.com/mantis/worker/internal/protocol"
)

// Transport defines the interface for communication channels.
type Transport interface {
	// Read reads the next request from the transport.
	// Returns io.EOF when there are no more requests.
	Read(ctx context.Context) (*protocol.RequestEnvelope, error)

	// Write writes a response to the transport.
	Write(ctx context.Context, response *protocol.ResponseEnvelope) error

	// Close closes the transport and releases resources.
	Close() error
}

// Handler processes requests and returns responses.
type Handler interface {
	// Handle processes a request and returns a response.
	Handle(ctx context.Context, request *protocol.RequestEnvelope) *protocol.ResponseEnvelope
}

// Serve runs the request/response loop using the given transport and handler.
// It reads requests, processes them with the handler, and writes responses.
// Returns when the context is canceled or when transport.Read returns an error.
func Serve(ctx context.Context, t Transport, h Handler) error {
	for {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next request
		req, err := t.Read(ctx)
		if err != nil {
			return err
		}

		// Process request
		resp := h.Handle(ctx, req)

		// Write response
		if err := t.Write(ctx, resp); err != nil {
			return err
		}
	}
}
