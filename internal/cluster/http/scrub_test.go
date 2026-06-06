package http_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// TestScrub_Success verifies POST /v1/scrub calls Cluster.Scrub and returns 202
// (accepted; the rewrite runs in the background on each node).
func TestScrub_Success(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/scrub", "")

	require.Equal(t, 202, status)
	require.Equal(t, 1, fake.ScrubCallCount())
}

// TestScrub_Unconfirmed maps a context error (couldn't confirm the Scrub
// command committed before the deadline — e.g. no quorum) to 503.
func TestScrub_Unconfirmed(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ScrubReturns(context.DeadlineExceeded)
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/scrub", "")

	require.Equal(t, 503, status)
}

// TestScrub_InternalError maps a non-context error to 500.
func TestScrub_InternalError(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ScrubReturns(errors.New("boom"))
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/scrub", "")

	require.Equal(t, 500, status)
}
