package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The syncable handlers hold the engine concretely, so error→response
// mappings live in free functions and are table-tested here directly —
// there is no seam to inject a wedged worker or a mid-flap ownership move
// through, and neither can be induced deterministically in a real
// single-node engine. The real-inducible rows (not-found, 409s, replay
// outcomes) are ALSO covered end to end in syncable_test.go; these tables
// pin the full matrix.

func decodeEnvelope(t *testing.T, w *httptest.ResponseRecorder) (int, string) {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return w.Code, body.Code
}

func TestWriteRebuildError_Mapping(t *testing.T) {
	for _, tc := range []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{cluster.ErrResourceNotFound, 404, "not_found"},
		{fmt.Errorf("%w: zone z-1 unserved", cluster.ErrZonePinUnsatisfiable), 503, "pin_unsatisfiable"},
		{fmt.Errorf("%w: moved", cluster.ErrNotSyncableOwner), 503, "not_syncable_owner"},
		{fmt.Errorf("%w: rebuild aborted", cluster.ErrWorkerWedged), 503, "worker_wedged"},
		{io.ErrUnexpectedEOF, 500, "internal_error"},
	} {
		t.Run(tc.wantCode, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeRebuildError(w, tc.err)
			status, code := decodeEnvelope(t, w)
			require.Equal(t, tc.wantStatus, status)
			require.Equal(t, tc.wantCode, code)
		})
	}
}

func TestWriteRematerializeError_Mapping(t *testing.T) {
	for _, tc := range []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{cluster.ErrResourceNotFound, 404, "not_found"},
		{fmt.Errorf("%w: keyless sink", cluster.ErrNotRematerializable), 409, "not_rematerializable"},
		{fmt.Errorf("%w: zone z-1 unserved", cluster.ErrZonePinUnsatisfiable), 503, "pin_unsatisfiable"},
		{fmt.Errorf("%w: moved", cluster.ErrNotSyncableOwner), 503, "not_syncable_owner"},
		{fmt.Errorf("%w: drain timed out", cluster.ErrWorkerWedged), 503, "worker_wedged"},
		{io.ErrUnexpectedEOF, 500, "internal_error"},
	} {
		t.Run(tc.wantCode, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeRematerializeError(w, tc.err)
			status, code := decodeEnvelope(t, w)
			require.Equal(t, tc.wantStatus, status)
			require.Equal(t, tc.wantCode, code)
		})
	}
}

func TestWriteReplayDeadLetterResult_Mapping(t *testing.T) {
	t.Run("nil is 200", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeReplayDeadLetterResult(w, nil)
		require.Equal(t, 200, w.Code)
	})
	t.Run("not dead-lettered is 404", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeReplayDeadLetterResult(w, cluster.ErrNotDeadLettered)
		status, code := decodeEnvelope(t, w)
		require.Equal(t, 404, status)
		require.Equal(t, "not_dead_lettered", code)
	})
	t.Run("replay failure is 502 with the cause", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeReplayDeadLetterResult(w, fmt.Errorf("%w: ERROR: value too long", cluster.ErrReplaySyncFailed))
		status, code := decodeEnvelope(t, w)
		require.Equal(t, 502, status)
		require.Equal(t, "replay_failed", code)
		require.Contains(t, w.Body.String(), "value too long",
			"the failure cause should be surfaced in details")
	})
	t.Run("unknown failure is 500", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeReplayDeadLetterResult(w, io.ErrUnexpectedEOF)
		require.Equal(t, 500, w.Code)
	})
}

// TestProgressFields pins the lag arithmetic, including the clamp: on a
// ?consistency=stale read of a lagging follower the replicated checkpoint can
// momentarily exceed the local head, which must clamp to 0, never underflow.
func TestProgressFields(t *testing.T) {
	for _, tc := range []struct {
		checkpoint, head uint64
		wantLag          uint64
		wantCaughtUp     bool
	}{
		{0, 0, 0, true},
		{900, 900, 0, true},
		{1234, 1240, 6, false},
		{60, 50, 0, true}, // checkpoint > head: clamp, never underflow
	} {
		lag, caughtUp := progressFields(tc.checkpoint, tc.head)
		require.Equal(t, tc.wantLag, lag, "checkpoint=%d head=%d", tc.checkpoint, tc.head)
		require.Equal(t, tc.wantCaughtUp, caughtUp, "checkpoint=%d head=%d", tc.checkpoint, tc.head)
	}
}

// TestApplyBuildDegraded pins the build-degraded override: this syncable's
// build failure flips workerState to degraded and carries the (already
// redacted) error, while records for other ids or other kinds never leak in.
func TestApplyBuildDegraded(t *testing.T) {
	errs := []cluster.ConfigBuildError{
		{Kind: "ingestable", ID: "rec-1", Error: "wrong kind"},
		{Kind: "syncable", ID: "other", Error: "wrong id"},
		{Kind: "syncable", ID: "rec-1", Error: "missing ${VAR}"},
	}

	resp := SyncableStatusResponse{WorkerState: cluster.WorkerStateRunning}
	applyBuildDegraded(&resp, "rec-1", errs)
	require.Equal(t, cluster.WorkerStateDegraded, resp.WorkerState)
	require.Equal(t, "missing ${VAR}", resp.Message)

	clean := SyncableStatusResponse{WorkerState: cluster.WorkerStateRunning}
	applyBuildDegraded(&clean, "rec-2", errs)
	require.Equal(t, cluster.WorkerStateRunning, clean.WorkerState,
		"other ids' and kinds' build errors must not leak")
	require.Empty(t, clean.Message)
}
