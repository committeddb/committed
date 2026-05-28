//go:build docker

package harness

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// CapturedEntity is one row in one proposal returned by GET /proposal.
// The harness exposes this shape to the oracle (rather than the
// internal cluster.Entity) because the HTTP response is what we get
// out of the box and it carries everything the differ needs.
type CapturedEntity struct {
	TypeID   string `json:"typeId"`
	TypeName string `json:"typeName"`
	Key      string `json:"key"`
	Data     string `json:"data"`
}

// CapturedProposal mirrors one Raft-committed proposal as returned by
// committed's HTTP API. One Postgres COMMIT becomes one
// CapturedProposal (see postgres.go:332 in the ingestable — the
// CommitMessage handler flushes pending entities into a single
// proposal).
type CapturedProposal struct {
	Entities []CapturedEntity `json:"entities"`
}

// fetchProposals retrieves the most recent N proposals for one topic
// via GET /proposal?type=X&number=N. The HTTP handler walks the Raft
// log backward, so the returned slice is in reverse-log-order; we
// reverse it before returning so callers see proposals in commit
// order.
//
// We pull a large window (number=10000) per call because the HTTP
// handler has no cursor parameter — see internal/cluster/db/proposals.go.
// If a single test ever needs more than 10000 proposals for one topic,
// it should bump this or we should add ?since= to the handler.
func fetchProposals(t *testing.T, topic string) []CapturedProposal {
	t.Helper()
	const window = 10000
	u := committedURL("/proposal") + "?" + url.Values{
		"type":   {topic},
		"number": {fmt.Sprintf("%d", window)},
	}.Encode()

	resp, err := http.Get(u) //nolint:gosec // G107: u is built from a fixed in-process URL plus a hardcoded topic; not from untrusted input
	require.NoError(t, err, "GET /proposal")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /proposal: %d %s", resp.StatusCode, string(b))
	}

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read /proposal body")

	// The handler writes an empty array as `null` (writeArrayBody);
	// treat both as zero proposals.
	if len(body) == 0 || string(body) == "null\n" || string(body) == "null" {
		return nil
	}

	var raw []CapturedProposal
	require.NoError(t, json.Unmarshal(body, &raw), "decode /proposal body: %s", string(body))

	// Reverse to commit order.
	for i, j := 0, len(raw)-1; i < j; i, j = i+1, j-1 {
		raw[i], raw[j] = raw[j], raw[i]
	}
	return raw
}

// snapshotCounts records the current per-topic proposal counts.
// Captured before the mutation script runs so we can later trim away
// everything from the dataset-load prefix and isolate just the script's
// stream.
func (h *Harness) snapshotCounts(t *testing.T) map[string]int {
	t.Helper()
	out := make(map[string]int, len(h.topics))
	for _, topic := range h.topics {
		out[topic] = len(fetchProposals(t, topic))
	}
	return out
}
