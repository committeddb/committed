package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
	"github.com/philborlin/committed/internal/version"
)

// TestVersion verifies /version returns 200 with the build Info. It
// also asserts the handler never reads cluster state — /version is
// deliberately unauthenticated and state-free so it works during
// rolling upgrades before leader election, same contract as /health.
func TestVersion(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	req := httptest.NewRequest("GET", "http://localhost/version", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var got version.Info
	require.Nil(t, json.Unmarshal(body, &got))

	require.Equal(t, version.Version, got.Version)
	require.Equal(t, version.Commit, got.Commit)
	require.Equal(t, version.BuildDate, got.BuildDate)
	require.Equal(t, runtime.Version(), got.GoVersion)

	require.Equal(t, 0, fake.LeaderCallCount())
	require.Equal(t, 0, fake.AppliedIndexCallCount())
}
