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
// writeReadError responds to a request-body read/decode failure: an over-limit
// body (rejected by the maxBytes middleware's http.MaxBytesReader) is 413
// request_too_large — counted on committed.http.request_too_large and logged
// with its route so an operator can watch for the body cap false-rejecting
// legitimate proposals — and any other failure uses the caller's 400.
func (h *HTTP) writeReadError(w httpgo.ResponseWriter, r *httpgo.Request, err error, code, message string) {
	if mbe, ok := errors.AsType[*httpgo.MaxBytesError](err); ok {
		route := routePattern(r)
		if h.metrics != nil {
			h.metrics.HTTPRequestTooLarge(route)
		}
		writeErrorf(w, httpgo.StatusRequestEntityTooLarge, "request_too_large",
			"request body exceeds the %d-byte limit (route %s)", mbe.Limit, route)
		return
	}
	writeError(w, httpgo.StatusBadRequest, code, message)
}

// notFoundHandler and methodNotAllowedHandler replace chi's raw defaults (a
// text/plain 404 and an empty-body 405) so the router's own error paths honor the
// same JSON error-envelope contract every handler uses.
func notFoundHandler(w httpgo.ResponseWriter, _ *httpgo.Request) {
	writeError(w, httpgo.StatusNotFound, "not_found", "no matching route")
}

func methodNotAllowedHandler(w httpgo.ResponseWriter, _ *httpgo.Request) {
	writeError(w, httpgo.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for this route")
}

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
// shared by every config handler: RebuildRequiredError → 409 (with
// machine-readable code + details), ConfigError → 400, ErrProposalTooLarge →
// 413, ErrInsufficientStorage → 507, else 500.
func writeProposeError(w httpgo.ResponseWriter, err error, resource, action string) {
	var rebuildErr cluster.RebuildRequiredError
	var configErr *cluster.ConfigError
	switch {
	case errors.As(err, &rebuildErr):
		// 409 Conflict: this config change can't be applied in place and needs
		// a rebuild. Code + details are destination-defined (e.g. table +
		// changed columns) so a deploy pipeline can branch programmatically;
		// this layer stays agnostic to what they contain.
		writeErrorWithDetails(w, httpgo.StatusConflict, rebuildErr.Code(), rebuildErr.Error(), rebuildErr.Details())
	case errors.As(err, &configErr):
		// 400 Bad Request: the config didn't parse/validate. When the parser
		// pinned the failure to a specific TOML field, surface it as structured
		// details ({field, issue}) so a deploy pipeline can point at the offending
		// key without scraping the message.
		if configErr.Field != "" {
			writeErrorWithDetails(w, httpgo.StatusBadRequest, "invalid_"+resource+"_config", configErr.Error(),
				map[string]string{"field": configErr.Field, "issue": configErr.Issue})
		} else {
			writeError(w, httpgo.StatusBadRequest, "invalid_"+resource+"_config", configErr.Error())
		}
	case errors.Is(err, cluster.ErrProposalTooLarge):
		writeError(w, httpgo.StatusRequestEntityTooLarge, "proposal_too_large", resource+" configuration exceeds the configured proposal size limit")
	case errors.Is(err, cluster.ErrInsufficientStorage):
		writeError(w, httpgo.StatusInsufficientStorage, "insufficient_storage",
			"the cluster (or this node) is low on disk space and is rejecting writes; see GET /v1/node/status disk.admission, retry once disk space recovers")
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
