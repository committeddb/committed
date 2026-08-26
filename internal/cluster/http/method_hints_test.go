package http_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// Two 405s the field tripped over get route-specific guidance instead of
// the generic body: GET on the dead-letter POST path (the listing lives
// at .../errors — a natural wrong guess), and DELETE on a type (deletion
// is refused BY DESIGN — log entries reference types permanently — but
// the bare 405 read as an oversight rather than a posture).
func TestMethodHints(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{})
	do := func(method, path string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w.Result().StatusCode, w.Body.String()
	}

	t.Run("GET deadletter names the listing route", func(t *testing.T) {
		status, body := do("GET", "http://localhost/v1/syncable/s1/deadletter")
		require.Equal(t, 405, status)
		require.Contains(t, body, "/errors", "the 405 must name where the listing actually lives")
		require.Contains(t, body, "skips", "and say what POST here does")
	})

	t.Run("DELETE type states the version-only posture", func(t *testing.T) {
		status, body := do("DELETE", "http://localhost/v1/type/t1")
		require.Equal(t, 405, status)
		require.Contains(t, body, "version", "the 405 must state the append/version-only posture")
		require.Contains(t, body, "permanently", "and why: log entries reference types permanently")
	})
}
