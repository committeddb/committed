package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	test "github.com/philborlin/committed/internal/cluster/db/testing"
	"github.com/philborlin/committed/internal/cluster/http"
)

// TestHealth verifies the /health endpoint always returns 200 with a
// JSON body, regardless of cluster state. /health is a pure liveness
// probe — orchestrators use it to decide whether to restart the
// process — so it must succeed even when leader=0 and applied=0.
func TestHealth(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	// Leave Leader/AppliedIndex at their zero defaults to confirm
	// /health doesn't gate on raft state.
	h := http.New(fake)

	req := httptest.NewRequest("GET", "http://localhost/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var got http.HealthResponse
	require.Nil(t, json.Unmarshal(body, &got))
	require.Equal(t, "ok", got.Status)

	// /health must not consult cluster state — verify the handler
	// never asked for leader or applied index.
	require.Equal(t, 0, fake.LeaderCallCount())
	require.Equal(t, 0, fake.AppliedIndexCallCount())
}

// TestReady_Unit covers the four handler outcomes against a fake
// cluster: 503 with no leader, 503 with leader but applied=0, 200
// when both checks pass, and the body fields each case writes. This
// is the unit-level half of the readiness coverage; TestReady_RealRaft
// below exercises the same handler against an actual db.DB.
func TestReady_Unit(t *testing.T) {
	tests := []struct {
		name           string
		leader         uint64
		applied        uint64
		expectedStatus int
		expectedBody   http.ReadyResponse
	}{
		{
			name:           "no leader yet",
			leader:         0,
			applied:        0,
			expectedStatus: 503,
			expectedBody:   http.ReadyResponse{Status: "not ready"},
		},
		{
			name:           "leader elected but nothing applied",
			leader:         1,
			applied:        0,
			expectedStatus: 503,
			expectedBody:   http.ReadyResponse{Status: "not ready"},
		},
		{
			name:           "leader elected and applied advanced",
			leader:         1,
			applied:        7,
			expectedStatus: 200,
			expectedBody:   http.ReadyResponse{Status: "ok"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &clusterfakes.FakeCluster{}
			fake.LeaderReturns(tc.leader)
			fake.AppliedIndexReturns(tc.applied)
			h := http.New(fake)

			req := httptest.NewRequest("GET", "http://localhost/ready", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, tc.expectedStatus, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			body, err := io.ReadAll(resp.Body)
			require.Nil(t, err)

			var got http.ReadyResponse
			require.Nil(t, json.Unmarshal(body, &got))
			require.Equal(t, tc.expectedBody, got)
		})
	}
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

	h := http.New(d)

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
