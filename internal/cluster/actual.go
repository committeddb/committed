package cluster

import (
	"fmt"
	"strings"
)

// Actual is a committed fact — a Proposal that consensus has ordered and
// written to the log at a fixed Index. You propose a Proposal; the Actual is
// what the log actually holds. It is immutable, and its Index is its identity
// and its place in the single total order (the only ordering authority in the
// system).
//
// Actual is the counterpart of Proposal across the consensus boundary: you
// propose Proposals, and you sync Actuals. A Reader yields Actuals in Index
// order, and a Syncable consumes them — a Syncable never sees a Proposal.
//
// It carries only what a consumer needs: the Index and the committed
// Entities. The write-side metadata on Proposal (RequestID, IngestableID,
// SourceSeq) is intent/plumbing and stays there.
type Actual struct {
	Index    uint64
	Entities []*Entity
}

func (a *Actual) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Actual[%d]:\n", a.Index)
	for i, e := range a.Entities {
		fmt.Fprintf(&sb, "  [%d](%s) %v", i, e.Name, string(e.Key))
		if i < len(a.Entities)-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}
