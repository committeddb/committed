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
	// OnDurable rides the proposal carrying the bundled checkpoint (see
	// cluster.Proposal.OnDurable): it reports the whole transaction durable.
	OnDurable func()
}

// Flush proposes pending as ONE proposal: a source transaction is one unit
// in the log (cluster.Proposal), whatever tables and topics it touched, with
// its rows in source order. The one exception is the caller's soft flush, a
// part of a transaction too large for one proposal. Rows are stamped with
// the refresh epoch so a later refresh-boundary marker can sweep rows a
// re-snapshot left behind; the proposal carries the transaction's provenance
// and, for a commit flush, the bundled checkpoint (bundlePos, nil for a soft
// flush), so the checkpoint commits atomically with the transaction.
//
// seq assigns the SourceSeq — and the dedup flags that travel with it — by
// the dialect's coordinate arithmetic (the LSN plus a sub-index, the binlog
// coordinate with its per-coordinate sub-index, the change-tracking version
// with its window sub-index and overflow). It is called once per topic in
// the flush, so the proposal carries the SourceSeq that topic's part had
// when a flush was split per topic (through 0.8.1). A resume across the
// upgrade, after the old binary committed some of those parts, then never
// drops the merged transaction as a duplicate (its SourceSeq is above every
// part's); it re-emits the parts already committed instead, which keyed
// destinations absorb. A single-topic flush calls seq once, as before.
// Returns how many proposals were emitted (0 or 1), so a commit path can
// fall back to an out-of-band checkpoint for a transaction that flushed
// nothing. The caller owns pending's reset.
func Flush(ctx context.Context, pending []*cluster.Entity, epoch uint64, prov Provenance, seq func(*cluster.Proposal), bundlePos []byte, pr chan<- *cluster.Proposal) (emitted int, err error) {
	if len(pending) == 0 {
		return 0, nil
	}
	StampGeneration(pending, epoch)
	p := &cluster.Proposal{
		Entities:             pending,
		SourceCommitUnixNano: prov.CommitUnixNano,
		SourceTxnID:          prov.TxnID,
		TxnScopedDedup:       prov.TxnScopedDedup,
	}
	for range countTopics(pending) {
		seq(p)
	}
	if bundlePos != nil {
		p.Position = bundlePos
		p.OnDurable = prov.OnDurable
	}
	select {
	case pr <- p:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return 1, nil
}

// countTopics is the number of distinct topics among entities.
func countTopics(entities []*cluster.Entity) int {
	seen := make(map[string]struct{}, 1)
	for _, e := range entities {
		id := ""
		if e.Type != nil {
			id = e.Type.ID
		}
		seen[id] = struct{}{}
	}
	return len(seen)
}
