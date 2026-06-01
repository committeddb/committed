package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
)

// TestGetSyncableErrors_Success asserts the handler renders dead-letter
// records as JSON and threads the since/limit cursor params through to
// the cluster.
func TestGetSyncableErrors_Success(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableDeadLettersReturns([]cluster.SyncableDeadLetter{
		{ID: "sync-1", Index: 7, TimestampUnixNano: 1_700_000_000_000_000_000, Kind: "permanent", Message: "constraint violation"},
		{ID: "sync-1", Index: 12, TimestampUnixNano: 1_700_000_001_000_000_000, Kind: "permanent", Message: "bad row"},
	}, nil)

	req := httptest.NewRequest("GET", "http://localhost/syncable/sync-1/errors?since=5&limit=10", nil)
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

	req := httptest.NewRequest("GET", "http://localhost/syncable/sync-1/errors", nil)
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
		"/syncable/sync-1/errors?since=notanumber",
		"/syncable/sync-1/errors?limit=0",
		"/syncable/sync-1/errors?limit=-3",
		"/syncable/sync-1/errors?limit=abc",
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

	req := httptest.NewRequest("GET", "http://localhost/syncable/sync-1/errors", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 500, w.Result().StatusCode)
}
