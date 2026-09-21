package wal

// appendRaw validates the entire batch before appending and preserves the exact
// input bytes, including protobuf unknown fields. Append failure can leave a
// durable prefix; callers must reopen and reconcile before retrying. This method
// does not implement Storage's applied-index or replay-deduplication protocol.
func (l *eventLogAdapter) appendRaw(payloads [][]byte) error {
	l.mutationMu.Lock()
	defer l.mutationMu.Unlock()
	l.mu.RLock()
	defer l.mu.RUnlock()
	records, err := eventEntryRecords(payloads)
	if err != nil {
		return err
	}
	return l.log.Append(records)
}

// eventIndex reports original durable append progress, including erased entries.
// It returns zero for a fresh log. A poisoned handle must be reopened first.
func (l *eventLogAdapter) eventIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.eventIndexLocked()
}

func (l *eventLogAdapter) eventIndexLocked() (uint64, error) {
	index, ok, err := l.log.LastAppended()
	if err != nil {
		return 0, err
	}
	if ok && index == 0 {
		return 0, ErrCorruptEntry
	}
	return index, nil
}

// appendCommittedRaw appends only the portion of a validated committed batch
// above recovered append progress. It is for replay of the SAME committed
// history, not conflict detection: erased entries cannot be compared with their
// original payloads. Skipped input is still decoded and checked for ordering.
// Empty/all-replayed batches make no writes and return the existing frontier.
// On error the returned index is unusable; reopen after storage failure before
// retrying. Success does not apply entries to BoltDB or advance AppliedIndex.
func (l *eventLogAdapter) appendCommittedRaw(payloads [][]byte) (uint64, error) {
	l.mutationMu.Lock()
	defer l.mutationMu.Unlock()
	l.mu.RLock()
	defer l.mu.RUnlock()
	records, err := eventEntryRecords(payloads)
	if err != nil {
		return 0, err
	}
	index, err := l.eventIndexLocked()
	if err != nil {
		return 0, err
	}
	first := 0
	for first < len(records) && records[first].ID <= index {
		first++
	}
	if first == len(records) {
		return index, nil
	}
	if err := l.log.Append(records[first:]); err != nil {
		return 0, err
	}
	return records[len(records)-1].ID, nil
}
