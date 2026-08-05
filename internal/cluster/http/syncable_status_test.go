package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

func doSyncableStatus(t *testing.T, fake *clusterfakes.FakeCluster) (http.SyncableStatusResponse, string) {
	t.Helper()
	h := http.New(fake)
	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/s1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var body http.SyncableStatusResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	return body, string(bs)
}

// A caught-up syncable that has skipped rows must not read as fully green:
// the status carries the dead-letter count and the latest skipped index, so
// the honest completeness check is caughtUp && deadLetters == 0. Field
// validation surfaced exactly this blind spot — an operator's convergence
// check passed while rows had been auto-skipped, discoverable only by
// scraping logs.
func TestSyncableStatus_SurfacesDeadLetters(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableProgressReturns(100, 100, nil) // caught up
	fake.SyncableDeadLetterStatsReturns(53, 4242, nil)

	body, _ := doSyncableStatus(t, fake)

	require.True(t, body.CaughtUp)
	require.Equal(t, uint64(53), body.DeadLetters,
		"a caught-up sink with skips must expose the count")
	require.Equal(t, uint64(4242), body.LastDeadLetterIndex)
}

// The clean case: deadLetters is always present (0), lastDeadLetterIndex is
// omitted entirely.
func TestSyncableStatus_NoDeadLetters(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableProgressReturns(100, 100, nil)
	// SyncableDeadLetterStats defaults to (0, 0, nil).

	body, raw := doSyncableStatus(t, fake)

	require.Zero(t, body.DeadLetters)
	require.Contains(t, raw, `"deadLetters":0`, "the zero count must still be present")
	require.False(t, strings.Contains(raw, "lastDeadLetterIndex"),
		"lastDeadLetterIndex must be omitted when nothing was skipped")
}
