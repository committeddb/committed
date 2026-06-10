package http

import (
	httpgo "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// A panic after the handler has started writing finds the headers
// committed: recoverPanic must log the panic but leave the in-flight
// response alone — no status rewrite, no JSON error appended to a body
// the client already partially received.
func TestRecoverPanic_PartialWriteLogsWithoutRewrite(t *testing.T) {
	tests := []struct {
		name       string
		handler    httpgo.HandlerFunc
		wantStatus int
	}{
		{
			name: "explicit-writeheader",
			handler: func(w httpgo.ResponseWriter, r *httpgo.Request) {
				w.WriteHeader(httpgo.StatusOK)
				_, _ = w.Write([]byte("partial"))
				panic("mid-stream failure")
			},
			wantStatus: httpgo.StatusOK,
		},
		{
			// A bare Write commits an implicit 200 — the tracker must
			// count that as written too.
			name: "implicit-200-via-write",
			handler: func(w httpgo.ResponseWriter, r *httpgo.Request) {
				_, _ = w.Write([]byte("partial"))
				panic("mid-stream failure")
			},
			wantStatus: httpgo.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			restore := zap.ReplaceGlobals(zap.New(core))
			defer restore()

			r := httptest.NewRequest(httpgo.MethodGet, "/stream", nil)
			w := httptest.NewRecorder()
			recoverPanic(tt.handler).ServeHTTP(w, r)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, "partial", w.Body.String(),
				"the already-sent body must not gain an error payload")
			require.Len(t, logs.FilterMessage("http handler panic").All(), 1)
		})
	}
}
