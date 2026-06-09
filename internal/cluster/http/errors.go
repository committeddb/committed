package http

import (
	"encoding/json"
	"errors"
	"fmt"
	httpgo "net/http"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// ErrorResponse is the JSON body returned for every non-2xx response.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// writeError writes a structured JSON error response. The message is
// treated as safe to return to callers — do not pass raw error strings
// that could contain SQL fragments, file paths, or stack traces.
func writeError(w httpgo.ResponseWriter, status int, code string, message string) {
	resp := ErrorResponse{
		Code:    code,
		Message: message,
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(httpgo.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(bs)

	zap.L().Warn("http error",
		zap.String("request_id", w.Header().Get("X-Request-ID")),
		zap.Int("status", status),
		zap.String("code", code),
		zap.String("message", message),
	)
}

// writeInternalError writes a 500 with a sanitized, client-safe message
// and logs the underlying cause server-side at Error. A 500 is the
// server's fault and its cause is deliberately withheld from the client
// (the message stays generic to avoid leaking internals), so the server
// log is the only place the cause is captured — and it is precisely the
// thing a fronting load balancer's access log cannot see. The LB records
// that a request returned 500; only this records why. cause may be nil.
func writeInternalError(w httpgo.ResponseWriter, message string, cause error) {
	resp := ErrorResponse{Code: "internal_error", Message: message}

	bs, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(httpgo.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpgo.StatusInternalServerError)
	_, _ = w.Write(bs)

	fields := []zap.Field{
		zap.String("request_id", w.Header().Get("X-Request-ID")),
		zap.Int("status", httpgo.StatusInternalServerError),
		zap.String("code", "internal_error"),
		zap.String("message", message),
	}
	if cause != nil {
		fields = append(fields, zap.Error(cause))
	}
	zap.L().Error("http internal error", fields...)
}

// writeErrorf is a convenience wrapper around writeError that formats
// the message with fmt.Sprintf.
func writeErrorf(w httpgo.ResponseWriter, status int, code string, format string, args ...any) {
	writeError(w, status, code, fmt.Sprintf(format, args...))
}

// writeProposeError maps a ProposeX error to the response shape
// shared by every config handler: ConfigError → 400,
// ErrProposalTooLarge → 413, else 500.
func writeProposeError(w httpgo.ResponseWriter, err error, resource, action string) {
	var configErr *cluster.ConfigError
	switch {
	case errors.As(err, &configErr):
		writeError(w, httpgo.StatusBadRequest, "invalid_"+resource+"_config", configErr.Error())
	case errors.Is(err, cluster.ErrProposalTooLarge):
		writeError(w, httpgo.StatusRequestEntityTooLarge, "proposal_too_large", resource+" configuration exceeds the configured proposal size limit")
	default:
		writeInternalError(w, "failed to "+action, err)
	}
}

// writeErrorWithDetails writes a structured JSON error response that
// includes a details field for additional context (e.g. validation errors).
func writeErrorWithDetails(w httpgo.ResponseWriter, status int, code string, message string, details any) {
	resp := ErrorResponse{
		Code:    code,
		Message: message,
		Details: details,
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(httpgo.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(bs)

	zap.L().Warn("http error",
		zap.String("request_id", w.Header().Get("X-Request-ID")),
		zap.Int("status", status),
		zap.String("code", code),
		zap.String("message", message),
	)
}
