package sql

// RefreshSnapshotEpoch chooses the generation to stamp on a FRESH full snapshot —
// one with no in-progress checkpoint to resume. checkpointEpoch is the epoch
// decoded from the position (0 when a DeleteIngestable cleared it); floor is the
// delete-surviving per-topic highwater (the highest generation the sink still
// carries, from TopicEpochReader).
//
// A genuine first snapshot (both 0) starts at epoch 1. When the topic has been
// refreshed before — a same-topic recreate leaves the sink holding rows up to the
// highwater while the cleared position reset the epoch to 0 — the snapshot must
// stamp STRICTLY ABOVE the highwater so its closing refresh-boundary marker's
// "generation < epoch" sweep reconciles the rows this upsert-only enumeration
// cannot re-emit (source-side deletes, RTBF-erased subjects). Without the bump the
// recreate re-stamps live rows at epoch 1 and the marker's "< 1" sweep is vacuous,
// stranding every stale row on the sink forever.
func RefreshSnapshotEpoch(checkpointEpoch, floor uint64) uint64 {
	if base := max(checkpointEpoch, floor); base >= 1 {
		return base + 1
	}
	return 1
}
