package http_test

import (
	"encoding/json"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

// The panic-recovery net, exercised through the REAL router and engine: the
// fixture's recorder parser doubles as the injection seam (its ParseStub
// panics), so a genuine handler panic travels the genuine middleware chain.

// panicOnParse arms the seam: the next syncable admission parse panics with v.
func panicOnParse(e *engine, v any) {
	e.syncParser.ParseStub = func(*cluster.ParsedConfig, cluster.DatabaseStorage) (cluster.Syncable, error) {
		panic(v)
	}
}

// TestRecoverPanic_PanickingHandlerReturns500: a panic mid-handler answers
// the JSON 500 envelope, and the log captures the panic value, request id,
// method, path, and stack. The stack and panic value stay server-side only.
func TestRecoverPanic_PanickingHandlerReturns500(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	e := newEngine(t)
	panicOnParse(e, "boom: handler invariant broken")

	w := e.doTOML(t, "POST", "/v1/syncable/s1", "[syncable]\nname = \"s1\"\ntype = \"recorder\"\n")
	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	var resp http.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "internal_error", resp.Code)
	require.NotContains(t, w.Body.String(), "boom", "the panic value must not leak to the client")

	entries := logs.FilterMessage("http handler panic").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.NotEmpty(t, fields["request_id"])
	require.Equal(t, httpgo.MethodPost, fields["method"])
	require.Equal(t, "/v1/syncable/s1", fields["path"])
	require.Contains(t, fields["panic"], "boom")
	stack, ok := fields["stack"].(string)
	require.True(t, ok)
	require.Contains(t, stack, "goroutine")
}

// http.ErrAbortHandler is net/http's sentinel for a deliberate abort. It
// must re-panic (net/http drops the connection, as requested) and must not
// be logged as a failure.
func TestRecoverPanic_ErrAbortHandlerStillAborts(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	e := newEngine(t)
	panicOnParse(e, httpgo.ErrAbortHandler)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/s1",
		strings.NewReader("[syncable]\nname = \"s1\"\ntype = \"recorder\"\n"))
	req.Header.Set("Content-Type", "text/toml")
	w := httptest.NewRecorder()
	require.PanicsWithValue(t, httpgo.ErrAbortHandler, func() { e.h.ServeHTTP(w, req) },
		"the abort sentinel must re-panic for net/http to drop the connection")
	require.Empty(t, logs.FilterMessage("http handler panic").All(),
		"a deliberate abort is not a failure and must not be logged as one")
}

// TestRecoverPanic_CoversAuthenticatedRoutes: the net sits UNDER the auth
// middleware, so a panic behind a bearer token still answers the envelope.
func TestRecoverPanic_CoversAuthenticatedRoutes(t *testing.T) {
	core, _ := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	e := newEngineHTTP(t, http.WithBearerToken("secret"))
	panicOnParse(e, "boom")

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/s1",
		strings.NewReader("[syncable]\nname = \"s1\"\ntype = \"recorder\"\n"))
	req.Header.Set("Content-Type", "text/toml")
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), "internal_error")
}
