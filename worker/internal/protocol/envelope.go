// Package protocol defines the JSON-based communication protocol for the worker.
//
// All communication uses NDJSON (newline-delimited JSON) format.
// Each request/response is a complete JSON object on a single line.
package protocol

import (
	"encoding/json"
	"fmt"
)

// RequestEnvelope wraps all incoming requests with metadata.
type RequestEnvelope struct {
	// ID is the request identifier for correlation (required)
	ID string `json:"id"`

	// Method is the operation to perform (required)
	// Format: "category.operation" (e.g., "metadata.list_schemas")
	Method string `json:"method"`

	// Params contains method-specific parameters (optional)
	Params json.RawMessage `json:"params,omitempty"`
}

// ResponseEnvelope wraps all outgoing responses.
type ResponseEnvelope struct {
	// ID matches the request ID for correlation (required)
	ID string `json:"id"`

	// Success indicates whether the operation succeeded
	Success bool `json:"success"`

	// Result contains the operation result (present if success=true)
	Result json.RawMessage `json:"result,omitempty"`

	// Error contains error details (present if success=false)
	Error *ErrorResponse `json:"error,omitempty"`
}

// ErrorResponse contains structured error information.
type ErrorResponse struct {
	// Code is a machine-readable error code
	Code string `json:"code"`

	// Message is a human-readable error description
	Message string `json:"message"`

	// Details contains additional error context (optional)
	Details map[string]interface{} `json:"details,omitempty"`
}

// Error codes for common failure scenarios.
const (
	ErrCodeInvalidRequest   = "INVALID_REQUEST"
	ErrCodeMethodNotFound   = "METHOD_NOT_FOUND"
	ErrCodeDriverNotFound   = "DRIVER_NOT_FOUND"
	ErrCodeConnectionFailed = "CONNECTION_FAILED"
	ErrCodeQueryFailed      = "QUERY_FAILED"
	ErrCodeTimeout          = "TIMEOUT"
	ErrCodeInternal         = "INTERNAL_ERROR"
)

// NewSuccessResponse creates a successful response envelope.
func NewSuccessResponse(id string, result interface{}) (*ResponseEnvelope, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	return &ResponseEnvelope{
		ID:      id,
		Success: true,
		Result:  data,
	}, nil
}

// NewErrorResponse creates an error response envelope.
func NewErrorResponse(id string, code string, message string, details map[string]interface{}) *ResponseEnvelope {
	return &ResponseEnvelope{
		ID:      id,
		Success: false,
		Error: &ErrorResponse{
			Code:    code,
			Message: message,
			Details: details,
		},
	}
}

// ParseParams unmarshals the params field into the provided type.
func (r *RequestEnvelope) ParseParams(v interface{}) error {
	if r.Params == nil {
		return nil
	}
	return json.Unmarshal(r.Params, v)
}

// UnmarshalResult unmarshals the result field into the provided type.
func (r *ResponseEnvelope) UnmarshalResult(v interface{}) error {
	if r.Result == nil {
		return fmt.Errorf("no result in response")
	}
	return json.Unmarshal(r.Result, v)
}
