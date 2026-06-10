package http_test

import (
	"encoding/json"
	"fmt"
	httpgo "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// A panicking handler must degrade to a sanitized 500 JSON response —
// not net/http's default of a dropped connection — and emit exactly one
// Error-level log carrying the request ID, method, path, panic value,
// and stack. The stack and panic value stay server-side only.
func TestRecoverPanic_PanickingHandlerReturns500(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	fake.DatabasesStub = func() ([]*cluster.Configuration, error) {
		panic("boom: handler invariant broken")
	}
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodGet, "/v1/database", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp http.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "internal_error", resp.Code)
	require.Equal(t, "internal server error", resp.Message)
	// Neither the panic value nor the stack leaks to the client.
	require.NotContains(t, w.Body.String(), "boom")
	require.NotContains(t, w.Body.String(), "goroutine")

	entries := logs.FilterMessage("http handler panic").All()
	require.Len(t, entries, 1)
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	require.NotEmpty(t, fields["request_id"])
	require.Equal(t, w.Header().Get("X-Request-ID"), fields["request_id"])
	require.Equal(t, httpgo.MethodGet, fields["method"])
	require.Equal(t, "/v1/database", fields["path"])
	require.Contains(t, fmt.Sprint(fields["panic"]), "boom")
	stack, ok := fields["stack"].(string)
	require.True(t, ok)
	require.Contains(t, stack, "goroutine")
}

// The motivating case from the ticket: rollback dereferences the
// looked-up config's .ID, trusting that a version lookup never returns
// (nil, nil). The real Storage upholds that; the counterfeiter fake's
// zero value breaks it — exactly what a future buggy implementation
// would do. The recovery net must turn that into a logged 500.
func TestRecoverPanic_RollbackNilConfigInvariant(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{} // DatabaseVersion zero-stub returns (nil, nil)
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodPost, "/v1/database/db-1/rollback?to=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	var resp http.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "internal_error", resp.Code)
	require.Len(t, logs.FilterMessage("http handler panic").All(), 1)
}

// http.ErrAbortHandler is net/http's sentinel for a deliberate abort.
// It must re-panic (net/http drops the connection, as requested) and
// must not be logged as a failure.
func TestRecoverPanic_ErrAbortHandlerStillAborts(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	fake.DatabasesStub = func() ([]*cluster.Configuration, error) {
		panic(httpgo.ErrAbortHandler)
	}
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodGet, "/v1/database", nil)
	w := httptest.NewRecorder()
	require.PanicsWithValue(t, httpgo.ErrAbortHandler, func() { h.ServeHTTP(w, r) })
	require.Empty(t, logs.FilterMessage("http handler panic").All())
}

// A panic in the auth middleware itself is also caught: recoverPanic is
// mounted before bearerAuth, so the whole authenticated surface — not
// just the handlers — degrades to a logged 500.
func TestRecoverPanic_CoversAuthenticatedRoutes(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	fake.SyncablesStub = func() ([]*cluster.Configuration, error) {
		panic("boom behind auth")
	}
	h := http.New(fake, http.WithBearerToken("secret"))

	r := httptest.NewRequest(httpgo.MethodGet, "/v1/syncable", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	require.Len(t, logs.FilterMessage("http handler panic").All(), 1)
}
