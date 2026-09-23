package wal

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// appendFetchedEntries receives logical records from the same scrub generation.
// Unlike AppendFetchedRecords, payloads contain protobuf bytes without native
// frames or physical sequences. This internal path does not implement a wire
// protocol, generation adoption, or metadata/snapshot installation. The caller
// holds BeginCatchUp through the eventual snapshot install and excludes Close.
// Storage errors can leave a durable prefix; reopen before retrying those errors.
// Validate even replayed entries; retain payload bytes without re-encoding.
func (s *Storage) appendFetchedEntries(generation uint64, payloads [][]byte) error {
	records, err := eventEntryRecords(payloads)
	if err != nil {
		return err
	}
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if !s.catchingUp.Load() {
		return eventlog.ErrInvalid
	}
	selected := s.EventLogGeneration()
	if s.eventLog.managed != nil {
		selected, err = s.eventLog.managed.Generation()
		if err != nil {
			return err
		}
	}
	if selected != generation {
		return fmt.Errorf("fetched generation %d differs from selected generation %d: %w", generation, selected, eventlog.ErrInvalid)
	}
	appender := s.eventAppenderLocked()
	frontier, hasHistory, err := appender.LastAppended()
	if err != nil {
		return err
	}
	first := 0
	for first < len(records) && records[first].ID <= frontier {
		first++
	}
	if first == len(records) {
		return nil
	}
	records = records[first:]
	if err := appender.Append(records); err != nil {
		return err
	}
	if !hasHistory {
		s.firstEventIndex.Store(records[0].ID)
	}
	s.eventIndex.Store(records[len(records)-1].ID)
	s.eventLogWriteOps.Add(1)
	return nil
}
