package http

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	httpgo "net/http"
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
