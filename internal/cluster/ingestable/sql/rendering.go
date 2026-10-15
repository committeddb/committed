package sql

// FeatureReader answers whether the whole cluster is at a feature level —
// the db's cluster-minimum gate. A dialect whose value rendering reaches
// entity keys reads it, so a mixed-version cluster never spells one key two
// ways.
type FeatureReader interface {
	FeatureEnabled(level uint64) bool
}

// SessionRendering decides one ingest session's rendering of a value family
// (SQL Server uniqueidentifier and temporal types, PostgreSQL text) from the
// checkpoint it resumes and that rendering's cluster gate — 0 the legacy
// rendering, 1 the new one — and whether the session must first re-snapshot
// (rerender): the checkpoint was rendered the OTHER way, so every such key
// and payload field on the sink is the old rendering and only a full
// re-emission at a bumped epoch (whose closing markers sweep the old rows on
// keyed sinks) re-keys it. The promotion is one-way: a promoted checkpoint
// stays promoted even if the gate reads closed (a member mid-join announces
// level 0 for a moment), so a membership change can never flip a sink back.
func SessionRendering(checkpoint uint32, hasCheckpoint, gateOpen bool) (rendering uint32, rerender bool) {
	if checkpoint == 1 {
		return 1, false
	}
	if gateOpen {
		return 1, hasCheckpoint
	}
	return 0, false
}
