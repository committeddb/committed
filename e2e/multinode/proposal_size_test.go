//go:build multinode

package multinode_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// largestProposalBytes is the largest COMMITTED_MAX_PROPOSAL_BYTES a node
// accepts (wal.MaxProposalBytes: a raft entry filling one 64 MiB event-log
// record). The multinode suite runs real binaries, so it is restated here.
const largestProposalBytes = 67108835

// postPadded posts one proposal whose data is a pad of n bytes and returns the
// response status and body.
func postPadded(t *testing.T, base, topic string, n int) (int, string) {
	t.Helper()
	body := fmt.Sprintf(`{"entities":[{"typeId":%q,"key":"big","data":{"pad":%q}}]}`, topic, strings.Repeat("x", n))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/proposal", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

// A cluster running at the largest proposal cap commits a proposal near it on
// every node, refuses one over it, and restarts cleanly: a committed proposal
// the event log could not store would crash every node on every restart.
func TestMultiNodeLargestProposalCommitsAndSurvivesRestart(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3, fmt.Sprintf("COMMITTED_MAX_PROPOSAL_BYTES=%d", largestProposalBytes))
	postType(t, nodes[0].base(), "big")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "big")
	}

	code, out := postPadded(t, nodes[0].base(), "big", largestProposalBytes-1024)
	require.Equalf(t, http.StatusOK, code, "a proposal just under the cap: %s", out)
	code, out = postPadded(t, nodes[0].base(), "big", largestProposalBytes+1)
	require.Equalf(t, http.StatusRequestEntityTooLarge, code, "a proposal over the cap: %s", out)
	require.Contains(t, out, "proposal_too_large")

	// Every node applied the large entry: each restarts over its data dir and
	// serves, and the cluster keeps committing.
	for _, n := range nodes {
		n.stopGraceful(t)
	}
	for _, n := range nodes {
		n.restart(t)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	postType(t, nodes[1].base(), "after-restart")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "after-restart")
	}
}
