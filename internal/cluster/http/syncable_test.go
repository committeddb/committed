package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/http"
)

// TestDeleteSyncable threads the id and the keepData query param through to
// the cluster and returns 200. The route is leader-pinned (leaderRead), so the
// fake reports itself as the leader to serve locally.
func TestDeleteSyncable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		keepData bool
	}{
		{"default tears down", "", false},
		{"keepData=true", "?keepData=true", true},
		{"keepData=false", "?keepData=false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			fake.IDReturns(1)
			fake.LeaderReturns(1)

			req := httptest.NewRequest("DELETE", "http://localhost/v1/syncable/sync-1"+tc.query, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			require.Equal(t, 200, w.Code)
			require.Equal(t, 1, fake.DeleteSyncableCallCount())
			_, gotID, gotKeep := fake.DeleteSyncableArgsForCall(0)
			require.Equal(t, "sync-1", gotID)
			require.Equal(t, tc.keepData, gotKeep)

			var body struct {
				ID       string `json:"id"`
				KeepData bool   `json:"keepData"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, "sync-1", body.ID)
			require.Equal(t, tc.keepData, body.KeepData)
		})
	}
}

// A non-boolean keepData is a 400 and never reaches the cluster.
func TestDeleteSyncable_InvalidKeepData(t *testing.T) {
	h, fake := setupTest()
	fake.IDReturns(1)
	fake.LeaderReturns(1)

	req := httptest.NewRequest("DELETE", "http://localhost/v1/syncable/sync-1?keepData=maybe", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 400, w.Code)
	require.Equal(t, 0, fake.DeleteSyncableCallCount())
}

// TestRebuildSyncable returns 202 and threads the id to the cluster. The route
// is leader-pinned, so the fake reports itself as the leader.
func TestRebuildSyncable(t *testing.T) {
	h, fake := setupTest()
	fake.IDReturns(1)
	fake.LeaderReturns(1)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/rebuild", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 202, w.Code)
	require.Equal(t, 1, fake.RebuildSyncableCallCount())
	_, gotID := fake.RebuildSyncableArgsForCall(0)
	require.Equal(t, "sync-1", gotID)
}

// An unknown syncable rebuilds to 404.
func TestRebuildSyncable_NotFound(t *testing.T) {
	h, fake := setupTest()
	fake.IDReturns(1)
	fake.LeaderReturns(1)
	fake.RebuildSyncableReturns(cluster.ErrResourceNotFound)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/nope/rebuild", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 404, w.Code)
}

// TestGetSyncableErrors_Success asserts the handler renders dead-letter
// records as JSON and threads the since/limit cursor params through to
// the cluster.
func TestGetSyncableErrors_Success(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableDeadLettersReturns([]cluster.SyncableDeadLetter{
		{ID: "sync-1", Index: 7, TimestampUnixNano: 1_700_000_000_000_000_000, Kind: "permanent", Message: "constraint violation"},
		{ID: "sync-1", Index: 12, TimestampUnixNano: 1_700_000_001_000_000_000, Kind: "permanent", Message: "bad row"},
	}, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/sync-1/errors?since=5&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got []struct {
		Index     uint64 `json:"index"`
		Timestamp string `json:"timestamp"`
		Kind      string `json:"kind"`
		Message   string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	require.Len(t, got, 2)
	require.Equal(t, uint64(7), got[0].Index)
	require.Equal(t, "permanent", got[0].Kind)
	require.Equal(t, "constraint violation", got[0].Message)
	require.Equal(t, "2023-11-14T22:13:20Z", got[0].Timestamp, "nanos must render as RFC3339 UTC")

	// The cursor params reach the cluster verbatim.
	id, since, limit := fake.SyncableDeadLettersArgsForCall(0)
	require.Equal(t, "sync-1", id)
	require.Equal(t, uint64(5), since)
	require.Equal(t, 10, limit)
}

// TestGetSyncableErrors_Defaults asserts that with no query params the
// handler uses since=0 and the default page size, and an empty result
// serializes as [] (not null).
func TestGetSyncableErrors_Defaults(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableDeadLettersReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/sync-1/errors", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "[]", string(body))

	id, since, limit := fake.SyncableDeadLettersArgsForCall(0)
	require.Equal(t, "sync-1", id)
	require.Equal(t, uint64(0), since)
	require.Equal(t, 100, limit)
}

// TestGetSyncableErrors_BadParams asserts invalid cursor params are
// rejected with 400 before the cluster is consulted.
func TestGetSyncableErrors_BadParams(t *testing.T) {
	for _, path := range []string{
		"/v1/syncable/sync-1/errors?since=notanumber",
		"/v1/syncable/sync-1/errors?limit=0",
		"/v1/syncable/sync-1/errors?limit=-3",
		"/v1/syncable/sync-1/errors?limit=abc",
	} {
		t.Run(path, func(t *testing.T) {
			h, fake := setupTest()
			req := httptest.NewRequest("GET", "http://localhost"+path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			require.Equal(t, 400, w.Result().StatusCode)
			require.Zero(t, fake.SyncableDeadLettersCallCount(), "must not query the cluster on a bad request")
		})
	}
}

// TestGetSyncableErrors_InternalError maps a storage error to 500.
func TestGetSyncableErrors_InternalError(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableDeadLettersReturns(nil, io.ErrUnexpectedEOF)

	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/sync-1/errors", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 500, w.Result().StatusCode)
}

// TestDeadLetterStuckSyncableHandler_Accepted asserts the handler returns 202
// with the targeted index and threads the id through to the cluster.
func TestDeadLetterStuckSyncableHandler_Accepted(t *testing.T) {
	h, fake := setupTest()
	fake.DeadLetterStuckSyncableReturns(42, nil)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/deadletter", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 202, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var got struct {
		Index uint64 `json:"index"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, uint64(42), got.Index)

	require.Equal(t, 1, fake.DeadLetterStuckSyncableCallCount())
	_, gotID := fake.DeadLetterStuckSyncableArgsForCall(0)
	require.Equal(t, "sync-1", gotID)
}

// TestDeadLetterStuckSyncableHandler_NotStuck maps ErrSyncNotStuck to 409.
func TestDeadLetterStuckSyncableHandler_NotStuck(t *testing.T) {
	h, fake := setupTest()
	fake.DeadLetterStuckSyncableReturns(0, cluster.ErrSyncNotStuck)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/deadletter", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 409, w.Result().StatusCode)
}

// TestGetSyncableStatus_Stuck renders a blocked syncable's status.
func TestGetSyncableStatus_Stuck(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableStuckReturns(cluster.SyncableStuck{
		ID: "sync-1", Index: 9, SinceUnixNano: 1_700_000_000_000_000_000, Message: "downstream rejected the row",
	}, true, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/sync-1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var got struct {
		Stuck   bool   `json:"stuck"`
		Index   uint64 `json:"index"`
		Since   string `json:"since"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	require.True(t, got.Stuck)
	require.Equal(t, uint64(9), got.Index)
	require.Equal(t, "2023-11-14T22:13:20Z", got.Since)
	require.Equal(t, "downstream rejected the row", got.Message)
}

// TestGetSyncableStatus_NotStuck reports a healthy syncable as not stuck. The
// stuck-only fields are omitted, but the progress fields are always present.
func TestGetSyncableStatus_NotStuck(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableStuckReturns(cluster.SyncableStuck{}, false, nil)
	fake.SyncableProgressReturns(0, 0, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/sync-1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.JSONEq(t, `{"stuck":false,"checkpointIndex":0,"headIndex":0,"lag":0,"caughtUp":true}`, string(body))
}

// progressResponse decodes the full status response including the
// always-present progress fields.
type progressResponse struct {
	Stuck           bool   `json:"stuck"`
	CheckpointIndex uint64 `json:"checkpointIndex"`
	HeadIndex       uint64 `json:"headIndex"`
	Lag             uint64 `json:"lag"`
	CaughtUp        bool   `json:"caughtUp"`
}

func getStatus(t *testing.T, h *http.HTTP) progressResponse {
	t.Helper()
	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/sync-1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var got progressResponse
	require.NoError(t, json.Unmarshal(body, &got))
	return got
}

// TestGetSyncableStatus_Behind renders the "X of Y, N behind" case: a healthy
// (not stuck) syncable whose checkpoint trails the data head reports a
// positive lag and caughtUp=false. The progress fields are present even
// though the syncable is not stuck.
func TestGetSyncableStatus_Behind(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableStuckReturns(cluster.SyncableStuck{}, false, nil)
	fake.SyncableProgressReturns(1234, 1240, nil)

	got := getStatus(t, h)
	require.False(t, got.Stuck)
	require.Equal(t, uint64(1234), got.CheckpointIndex)
	require.Equal(t, uint64(1240), got.HeadIndex)
	require.Equal(t, uint64(6), got.Lag)
	require.False(t, got.CaughtUp)
}

// TestGetSyncableStatus_CaughtUp reports lag 0 / caughtUp true when the
// checkpoint has reached the head.
func TestGetSyncableStatus_CaughtUp(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableProgressReturns(900, 900, nil)

	got := getStatus(t, h)
	require.Equal(t, uint64(0), got.Lag)
	require.True(t, got.CaughtUp)
}

// TestGetSyncableStatus_LagClampedNonNegative guards the clamp: on a stale
// follower the checkpoint (replicated) can momentarily exceed the local head,
// which would underflow an unsigned subtraction. lag must clamp to 0.
func TestGetSyncableStatus_LagClampedNonNegative(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableProgressReturns(60, 50, nil) // checkpoint > head

	got := getStatus(t, h)
	require.Equal(t, uint64(0), got.Lag, "lag must clamp to 0, never underflow")
	require.True(t, got.CaughtUp)
}

// TestGetSyncableStatus_NoLeaderHop confirms the progress read is answerable
// locally: the handler must not consult leadership/membership to serve it.
func TestGetSyncableStatus_NoLeaderHop(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableProgressReturns(10, 20, nil)

	_ = getStatus(t, h)
	require.Zero(t, fake.MembershipCallCount(), "status read must not make a leader/membership hop")
}

// TestReplaySyncableDeadLetterHandler_Success maps a nil error to 200 and
// threads id + index through to the cluster.
func TestReplaySyncableDeadLetterHandler_Success(t *testing.T) {
	h, fake := setupTest()
	fake.ReplaySyncableDeadLetterReturns(nil)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/replay/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ReplaySyncableDeadLetterCallCount())
	_, gotID, gotIndex := fake.ReplaySyncableDeadLetterArgsForCall(0)
	require.Equal(t, "sync-1", gotID)
	require.Equal(t, uint64(7), gotIndex)
}

// TestReplaySyncableDeadLetterHandler_NotDeadLettered maps ErrNotDeadLettered
// to 404.
func TestReplaySyncableDeadLetterHandler_NotDeadLettered(t *testing.T) {
	h, fake := setupTest()
	fake.ReplaySyncableDeadLetterReturns(cluster.ErrNotDeadLettered)

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/replay/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 404, w.Result().StatusCode)
}

// TestReplaySyncableDeadLetterHandler_SyncFailed maps a wrapped
// ErrReplaySyncFailed to 502 with the cause in details.
func TestReplaySyncableDeadLetterHandler_SyncFailed(t *testing.T) {
	h, fake := setupTest()
	fake.ReplaySyncableDeadLetterReturns(fmt.Errorf("%w: ERROR: value too long", cluster.ErrReplaySyncFailed))

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/replay/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 502, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "value too long", "the failure cause should be surfaced in details")
}

// TestReplaySyncableDeadLetterHandler_BadIndex rejects a non-numeric index
// with 400 before consulting the cluster.
func TestReplaySyncableDeadLetterHandler_BadIndex(t *testing.T) {
	h, fake := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/sync-1/replay/notanumber", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 400, w.Result().StatusCode)
	require.Zero(t, fake.ReplaySyncableDeadLetterCallCount())
}
