package sql

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

// Provenance is the source-side identity every per-topic proposal of one
// flush shares: the transaction's commit time and id, and whether this
// dialect's checkpointing earns transaction-scoped dedup.
type Provenance struct {
	CommitUnixNano int64
	TxnID          string
	// TxnScopedDedup marks a dialect that checkpoints per transaction, so a
	// resume re-emits at most the in-flight transaction — the contract that
	// earns transaction-scoped dedup (Proposal.TxnScopedDedup). A dialect
	// that checkpoints per polling window leaves it false.
	TxnScopedDedup bool
}

// Flush proposes pending as one proposal per topic, in first-appearance
// order (deterministic for replay) — the shape every dialect's stream flush
// shares. Rows are stamped with the refresh epoch so a later
// refresh-boundary marker can sweep rows a re-snapshot left behind; every
// group carries the transaction's provenance; the bundled checkpoint
// (bundlePos, nil for a soft flush) rides the LAST group, so it commits
// atomically with the transaction's final part. A single-topic flush yields
// one group == the original buffer, byte-identical to the pre-routing
// single-proposal flush.
//
// seq assigns each proposal's SourceSeq — and the dedup flags that travel
// with it — in emission order: the dialect's coordinate arithmetic (the
// LSN plus a sub-index, the binlog coordinate with its per-coordinate
// sub-index, the change-tracking version with its window sub-index and
// overflow), called once per proposal. Returns how many proposals were
// emitted, so a commit path can fall back to an out-of-band checkpoint for
// a transaction that flushed nothing. The caller owns pending's reset.
func Flush(ctx context.Context, pending []*cluster.Entity, epoch uint64, prov Provenance, seq func(*cluster.Proposal), bundlePos []byte, pr chan<- *cluster.Proposal) (emitted int, err error) {
	if len(pending) == 0 {
		return 0, nil
	}
	StampGeneration(pending, epoch)
	groups := PartitionByTopic(pending)
	for i, group := range groups {
		p := &cluster.Proposal{
			Entities:             group,
			SourceCommitUnixNano: prov.CommitUnixNano,
			SourceTxnID:          prov.TxnID,
			TxnScopedDedup:       prov.TxnScopedDedup,
		}
		seq(p)
		if bundlePos != nil && i == len(groups)-1 {
			p.Position = bundlePos
		}
		select {
		case pr <- p:
		case <-ctx.Done():
			return i, ctx.Err()
		}
	}
	return len(groups), nil
}
