package http

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	httpgo "net/http"
	"runtime/debug"
	"strings"

	"go.uber.org/zap"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// RequestID is a middleware that generates a unique request ID for each
// incoming request and stores it in the request context. The ID is also
// set as a response header (X-Request-ID) and added to a zap logger
// stored in the context for downstream handlers.
func RequestID(next httpgo.Handler) httpgo.Handler {
	return httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		id := generateRequestID()
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		w.Header().Set("X-Request-ID", id)

		zap.L().Debug("request started",
			zap.String("request_id", id),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
		)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID returns the request ID from the context, or "" if not present.
func GetRequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

func generateRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// securityHeaders sets defense-in-depth headers on every response.
// HSTS is only emitted on TLS requests — sending it over plaintext
// would be a no-op at best and confusing at worst, and Chrome ignores
// it anyway when the scheme is http. The CSP here is the strict API
// default (default-src 'none'); handlers that need a looser policy
// (notably /docs) call w.Header().Set("Content-Security-Policy", ...)
// to override before writing the body.
func securityHeaders(next httpgo.Handler) httpgo.Handler {
	return httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		h := w.Header()
		if r.TLS != nil {
			h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// panicResponseWriter tracks whether the response has been started so
// the recovery net knows if a sanitized 500 can still be written. A
// Write without WriteHeader commits an implicit 200, so both mark it.
type panicResponseWriter struct {
	httpgo.ResponseWriter
	wrote bool
}

func (w *panicResponseWriter) WriteHeader(status int) {
	w.wrote = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *panicResponseWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer's
// optional interfaces (Flusher, Hijacker, deadlines) through the wrapper.
func (w *panicResponseWriter) Unwrap() httpgo.ResponseWriter { return w.ResponseWriter }

// recoverPanic is the recovery net for the whole API surface: a panic in
// any downstream middleware or handler becomes one Error-level log line
// (with the request ID and the stack — captured here and only here,
// server-side) plus a sanitized 500, instead of net/http's default of a
// dropped connection with an unstructured stderr trace. If the handler
// already started writing before panicking the headers are committed, so
// it only logs rather than corrupt the in-flight response.
// http.ErrAbortHandler re-panics: it is net/http's sentinel for a
// deliberate abort, not a failure to report.
func recoverPanic(next httpgo.Handler) httpgo.Handler {
	return httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		pw := &panicResponseWriter{ResponseWriter: w}
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == httpgo.ErrAbortHandler { //nolint:errorlint // sentinel panic value, compared as such (stdlib/chi convention)
				panic(v)
			}

			zap.L().Error("http handler panic",
				zap.String("request_id", GetRequestID(r.Context())),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Any("panic", v),
				zap.ByteString("stack", debug.Stack()),
			)

			if pw.wrote {
				return
			}

			// The ErrorResponse shape of writeInternalError, written
			// directly: that helper logs its own Error line and the
			// panic is already logged above with more context.
			bs, err := json.Marshal(ErrorResponse{Code: "internal_error", Message: "internal server error"})
			if err != nil {
				pw.WriteHeader(httpgo.StatusInternalServerError)
				return
			}
			pw.Header().Set("Content-Type", "application/json")
			pw.WriteHeader(httpgo.StatusInternalServerError)
			_, _ = pw.Write(bs)
		}()

		next.ServeHTTP(pw, r)
	})
}

// bearerAuth returns middleware that rejects requests whose
// Authorization header does not carry the expected bearer token.
// Comparison uses crypto/subtle to prevent timing side-channels.
func (h *HTTP) bearerAuth(next httpgo.Handler) httpgo.Handler {
	return httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		header := r.Header.Get("Authorization")
		if header == "" {
			writeError(w, httpgo.StatusUnauthorized, "unauthorized", "missing Authorization header")
			return
		}

		token, ok := strings.CutPrefix(header, "Bearer ")
		if !ok {
			writeError(w, httpgo.StatusUnauthorized, "unauthorized", "Authorization header must use Bearer scheme")
			return
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(h.bearerToken)) != 1 {
			writeError(w, httpgo.StatusUnauthorized, "unauthorized", "invalid bearer token")
			return
		}

		next.ServeHTTP(w, r)
	})
}
