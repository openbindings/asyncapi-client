package asyncapiclient

import (
	"context"
	"errors"
)

const (
	ErrCodeContextRequired   = "CONTEXT_REQUIRED"
	ErrCodeCancelled         = "ERR_CANCELLED"
	ErrCodeTimeout           = "ERR_TIMEOUT"
	ErrCodeInputClosed       = "ERR_INPUT_CLOSED"
	ErrCodeInvocationClosed  = "ERR_INVOCATION_CLOSED"
	ErrCodeMissingInput      = "ERR_MISSING_INPUT"
	ErrCodeProtocol          = "ERR_PROTOCOL"
	ErrCodeInvalidRef        = "ERR_INVALID_REF"
	ErrCodeRefNotFound       = "ERR_REF_NOT_FOUND"
	ErrCodeSourceLoadFailed  = "ERR_SOURCE_LOAD_FAILED"
	ErrCodeSourceConfigError = "ERR_SOURCE_CONFIG_ERROR"
	ErrCodeConnectFailed     = "ERR_CONNECT_FAILED"
	ErrCodeExecutionFailed   = "ERR_EXECUTION_FAILED"
	ErrCodeResponseError     = "ERR_RESPONSE_ERROR"
	ErrCodeStreamError       = "ERR_STREAM_ERROR"
	ErrCodeValidationFailed  = "ERR_VALIDATION_FAILED"
	ErrCodeRuntime           = "ERR_RUNTIME"
	ErrCodeDriverUnavailable = "DRIVER_UNAVAILABLE"
	ErrCodeDriverFailed      = "DRIVER_FAILED"
)

// ExecutionError is an SDK-neutral artifact execution failure. Details are
// application-facing facts defined by the artifact family; Diagnostics are
// an explicit protocol-aware escape hatch.
type ExecutionError struct {
	Code        string
	Message     string
	Details     any
	Diagnostics any
	Cause       error
}

func (e *ExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

func (e *ExecutionError) Unwrap() error { return e.Cause }

func asExecutionError(err error) *ExecutionError {
	if err == nil {
		return nil
	}
	var existing *ExecutionError
	if errors.As(err, &existing) {
		return existing
	}
	code := ErrCodeRuntime
	if errors.Is(err, context.Canceled) {
		code = ErrCodeCancelled
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = ErrCodeTimeout
	}
	return &ExecutionError{Code: code, Message: err.Error(), Cause: err}
}

func httpError(statusCode int, _ string) *ExecutionError {
	return &ExecutionError{
		Code:        ErrCodeExecutionFailed,
		Message:     "Invocation completed unsuccessfully",
		Diagnostics: map[string]any{"status": statusCode},
	}
}
