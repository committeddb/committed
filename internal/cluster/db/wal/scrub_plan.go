package wal

// scrubPlan captures application selections for one authorized scrub. Maps are
// private and are not changed after preparation. Storage mechanics consume the
// transform without interpreting tombstones, metadata, or subject-key policy.
type scrubPlan struct {
	bound      uint64
	selections map[string]uint64
	metadata   map[string]uint64
	eraseMax   uint64
	erase      *eraseOutcome
}

func (p *scrubPlan) transform(raw []byte) (bool, []byte, error) {
	return scrubFilterEntry(raw, p.selections, p.metadata, p.eraseMax)
}

// prepareScrubPlan derives selections from replicated state and the authorized
// prefix. Entries appended later lie beyond the selections and survive.
func (s *Storage) prepareScrubPlan(bound uint64, hash bool, cmdIndex uint64) (*scrubPlan, error) {
	// RTBF (user-tombstone) selection: max delete index <= bound per (type, key).
	// Captured once; deletes recorded after this point have index > bound and are
	// irrelevant, so the selection is frozen and identical across replicas.
	sel, err := s.tombstoneSelections(bound)
	if err != nil {
		return nil, err
	}

	// Delete-key erasure threshold (0 disables the pass): retained user-delete
	// entries at raft index <= eraseMax get their raw subject key rewritten to
	// cluster.ErasedKey. Computed like sel/msel as a pure function of
	// replicated state — see deleteKeyEraseGate.
	var erase *eraseOutcome
	var eraseMax uint64
	if hash {
		var eraseRaws []rawDelete
		eraseMax, eraseRaws, err = s.deleteKeyEraseGate(cmdIndex, bound)
		if err != nil {
			return nil, err
		}
		erase = &eraseOutcome{eligibleMax: eraseMax, raws: eraseRaws}
	}

	// Metadata-GC (system-tombstone) selection: max raft index <= bound per
	// system-tombstonable (type, key). The rewrite keeps only that latest entry
	// per key and drops earlier ones. Derived from the log prefix <= bound, so —
	// like sel — it is a pure function of (log bytes, bound), identical on every
	// replica. sel (RTBF) and msel (metadata GC) are NOT disjoint — a user
	// EntityKindSnapshot key with a delete appears in both — but scrubFilterEntry
	// ORs the two predicates and, where they overlap, they provably agree (RTBF
	// spares the delete-tombstone; metadata GC keeps the latest per key), so the
	// removal set is well-defined regardless. Do NOT re-derive an optimization
	// from a disjointness assumption.
	msel, err := s.metadataSupersessions(bound)
	if err != nil {
		return nil, err
	}
	if erase != nil {
		erase.msel = msel
	}

	return &scrubPlan{bound: bound, selections: sel, metadata: msel, eraseMax: eraseMax, erase: erase}, nil
}
