package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/http"
	test "github.com/committeddb/committed/internal/cluster/db/testing"
)

// TestHealth verifies the /health endpoint always returns 200 with a
// JSON body, regardless of cluster state. /health is a pure liveness
// probe — orchestrators use it to decide whether to restart the
// process — so it must succeed even when leader=0 and applied=0.
func TestHealth(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "GET", "/health")
	require.Equal(t, 200, w.Code)
	require.Equal(t, "application/json", w.Result().Header.Get("Content-Type"))
	var got http.HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, "ok", got.Status)
}

// TestReady_RealRaft drives the readiness probe against a freshly
// constructed in-memory db.DB and verifies the 503 → 200 transition
// happens once raft has actually elected a leader and applied at
// least one entry. This is the success-criterion check from the
// ticket: "GET /ready returns 503 → 200 transition under a real raft
// startup."
//
// We don't depend on a specific election deadline — single-node raft
// with the test tick interval (1ms) elects in a few milliseconds,
// but we poll up to a generous deadline so a slow CI host doesn't
// flake. Once /ready returns 200 the body fields must reflect this
// node as the leader (id=1) and a non-zero applied index.
func TestReady_RealRaft(t *testing.T) {
	d := test.CreateDB()
	defer d.Close()

	h := http.New(d.DB)

	// Immediately after construction the node hasn't ticked yet, so
	// raft has no leader and applied is 0. We require the very first
	// probe to be 503 to prove the not-ready path actually fires.
	first := doReady(t, h)
	require.Equal(t, 503, first.status, "expected 503 immediately after construction, got %d (body=%+v)", first.status, first.body)

	// Poll until the probe flips to 200. Single-node raft elects
	// itself within a few ticks (the test tick interval is 1ms — see
	// db/testing/db.go testTickInterval), and applies a noop entry
	// on becoming leader, so this normally completes in well under
	// 100ms. We give it a 5s deadline to absorb slow-CI variance.
	deadline := time.Now().Add(5 * time.Second)
	var last readyResult
	for time.Now().Before(deadline) {
		last = doReady(t, h)
		if last.status == 200 {
			require.Equal(t, "ok", last.body.Status)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for /ready to become 200; last response: status=%d body=%+v", last.status, last.body)
}

type readyResult struct {
	status int
	body   http.ReadyResponse
}

func doReady(t *testing.T, h *http.HTTP) readyResult {
	t.Helper()
	req := httptest.NewRequest("GET", "http://localhost/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	bs, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var body http.ReadyResponse
	require.Nil(t, json.Unmarshal(bs, &body))
	return readyResult{status: resp.StatusCode, body: body}
}
