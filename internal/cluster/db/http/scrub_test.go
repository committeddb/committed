package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestScrub_Success: POST /v1/scrub proposes a real Scrub command through
// the engine and returns 202 (accepted; the rewrite runs in the background
// on each node). The error legs — deadline 503, disk-full 507, default 500 —
// flow through the shared writeProposeError choke point, whose full branch
// table is pinned in status_mapping_test.go.
func TestScrub_Success(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "POST", "/v1/scrub")
	require.Equal(t, 202, w.Code, w.Body.String())
}
