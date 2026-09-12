package cluster

// RestatementDryRunReport is what rehearsing a restatement against the committed log
// revealed — the authoring loop for the highest-blast-radius config in the
// system. A restatement is append-only and changes how EVERY consumer reads a
// slice of history, so "valid config, wrong result" must cost minutes here,
// not a correction restatement plus a fleet of re-materializations. Nothing is
// admitted or stored; the fold used is the same interpretation fold the read
// path runs, so the rehearsal cannot diverge from what admission would do.
type RestatementDryRunReport struct {
	// ScanFrom/ScanTo echo the restatement's range — the region scanned.
	ScanFrom uint64 `json:"scanFrom"`
	ScanTo   uint64 `json:"scanTo"`
	// EntriesScanned is how many committed entries the scan read.
	EntriesScanned int `json:"entriesScanned"`
	// EntitiesOfType counts row entities of the restatement's type in the range —
	// the population the restatement's selectors choose from.
	EntitiesOfType int `json:"entitiesOfType"`
	// StampEligible counts entities the range + stamp selector accept
	// (before the predicate).
	StampEligible int `json:"stampEligible"`
	// Matched counts entities the restatement fully selects (range + stamp +
	// predicate). The number-one authoring check: 0 means the restatement does
	// nothing; == StampEligible with a predicate present means the predicate
	// filtered nothing.
	Matched int `json:"matched"`
	// Rebound counts matched entities whose READING actually changes versus
	// the current registry — the restatement's real effect. Matched - Rebound are
	// no-ops (the rebind target is already their effective reading).
	Rebound int `json:"rebound"`
	// PredicateErrors counts entities the predicate could not evaluate
	// (non-JSON payload, or a program the payload's shape breaks). On the
	// live path each of these dead-letters or wedges a worker — a nonzero
	// count is a red flag before admission.
	PredicateErrors int `json:"predicateErrors"`
	// ByStampedVersion breaks StampEligible down by the stamp actually on
	// the wire — the fact that catches a wrong fromVersion selector.
	ByStampedVersion map[int]int `json:"byStampedVersion,omitempty"`
	// Samples shows the first few REBOUND entities: the reading each has
	// today and the reading the restatement would give it.
	Samples []RestatementDryRunSample `json:"samples,omitempty"`
	// AffectedSyncables lists every syncable consuming the restatement's topic —
	// the re-materialization bill admission would incur: each one's
	// materialized rows become stale the moment the restatement commits.
	AffectedSyncables []RestatementDryRunSyncable `json:"affectedSyncables,omitempty"`
	// Overlaps names already-applied restatements whose range intersects this one
	// on the same type. They compose (later in the log wins) — a note for
	// the author, not a fault.
	Overlaps []string `json:"overlaps,omitempty"`
	// Coverage is "complete" when the whole [scanFrom, scanTo] range was
	// read; "partial" when the budget or deadline stopped the scan early
	// (Truncated says which) — counters then cover only what was scanned.
	Coverage  string `json:"coverage"`
	Truncated string `json:"truncated,omitempty"`
	// Findings are auto-flagged authoring signatures with the interpretation
	// and the next move; empty means nothing fired.
	Findings []string `json:"findings"`
	// Timings split "where did the time go".
	ParseMs    int64 `json:"parseMs"`
	ReadMs     int64 `json:"readMs"`
	DurationMs int64 `json:"durationMs"`
}

// RestatementDryRunSample is one rebound entity: the before/after reading pair.
type RestatementDryRunSample struct {
	Index          uint64 `json:"index"`
	Key            string `json:"key"`
	StampedVersion int    `json:"stampedVersion"`
	// CurrentReading is the effective version under the registry as it
	// stands; CandidateReading is under the registry plus this restatement.
	CurrentReading   int `json:"currentReading"`
	CandidateReading int `json:"candidateReading"`
}

// RestatementDryRunSyncable is one consumer of the restatement's topic and where its
// materialization is pinned.
type RestatementDryRunSyncable struct {
	ID string `json:"id"`
	// InterpretationPin is the coordinate the syncable's current
	// materialization was derived under (see SyncableInterpretation).
	InterpretationPin uint64 `json:"interpretationPin"`
	// AlreadyStale reports whether the syncable is stale even before this
	// restatement (a prior restatement or migration edit landed past its pin).
	AlreadyStale bool `json:"alreadyStale"`
}
