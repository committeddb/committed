//go:build docker

package harness

import "testing"

// CapturedEntity is one entity of one committed Actual, as the harness
// collector decodes it off the webhook payload. The harness exposes this
// shape to the oracle (rather than the internal cluster.Entity) because it
// carries everything the differ needs and nothing it doesn't.
type CapturedEntity struct {
	TypeID   string `json:"typeId"`
	TypeName string `json:"typeName"`
	Key      string `json:"key"`
	Data     string `json:"data"`
}

// CapturedProposal mirrors one Raft-committed Actual as the collector
// received it from the per-topic webhook syncable. One Postgres COMMIT
// becomes one Actual (see postgres.go in the ingestable — the CommitMessage
// handler flushes pending entities into a single proposal), hence one
// CapturedProposal.
type CapturedProposal struct {
	Entities []CapturedEntity `json:"entities"`
}

// snapshotCounts records the current per-topic Actual counts, read from the
// collector. Captured before the mutation script runs so we can later trim
// away everything from the dataset-load prefix and isolate just the script's
// stream.
func (h *Harness) snapshotCounts(t *testing.T) map[string]int {
	t.Helper()
	out := make(map[string]int, len(h.topics))
	for _, topic := range h.topics {
		out[topic] = len(h.collector.proposals(topic))
	}
	return out
}
