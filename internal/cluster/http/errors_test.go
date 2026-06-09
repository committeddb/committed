package http_test

import (
	"errors"
	"fmt"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// A 500 is the server's fault and the cause is deliberately withheld from
// the client. The server log is then the only place the cause is captured
// — the one thing a fronting load balancer's access log can't see — so it
// must be logged, at Error, and must not leak into the response body.
func TestInternalError_LogsCauseAtErrorWithoutLeaking(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	fake.ProposeDatabaseReturns(errors.New("boltdb: disk full"))
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodPost, "/v1/database/db-1", strings.NewReader("x = 1"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	// The client gets a sanitized message, never the underlying cause.
	require.NotContains(t, w.Body.String(), "disk full")

	entries := logs.FilterMessage("http internal error").All()
	require.Len(t, entries, 1)
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	require.Equal(t, "failed to propose database", fields["message"])
	// The cause is captured server-side.
	require.Contains(t, fmt.Sprint(fields["error"]), "disk full")
}

// A 4xx is the client's fault; its cause is already in the response and a
// Warn line. It must not be elevated to the Error-level internal-error log.
func TestClientError_NotLoggedAsInternalError(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake, http.WithBearerToken("secret"))

	// No Authorization header → 401, a client error.
	r := httptest.NewRequest(httpgo.MethodPost, "/v1/database/db-1", strings.NewReader("x = 1"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusUnauthorized, w.Code)
	require.Empty(t, logs.FilterMessage("http internal error").All())
}
