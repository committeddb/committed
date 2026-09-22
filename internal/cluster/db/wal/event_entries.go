package wal

import "github.com/committeddb/committed/internal/cluster/db/eventlog"

// entryStore binds opaque storage to application entry decoding. The caller
// holds the event publication lock throughout append and cursor use. Each cursor
// owns its position and retained decoding; Close retires the storage owner.
type entryStore interface {
	eventlog.Appender
	NewEntryCursor(uint64) entryCursor
	Close() error
}

type boundEntryStore struct {
	eventlog.Appender
	cursor func(uint64) entryCursor
	close  func() error
}

func (s *boundEntryStore) NewEntryCursor(index uint64) entryCursor { return s.cursor(index) }
func (s *boundEntryStore) Close() error                            { return s.close() }

// bindEventEntries supplies the same application contract for a shared EventLog.
func bindEventEntries(log eventlog.EventLog) entryStore {
	return &boundEntryStore{
		Appender: log,
		cursor: func(index uint64) entryCursor {
			return newEventEntryCursor(log.NewCursor(), index)
		},
		close: log.Close,
	}
}

// eventAppenderLocked returns the published logical writer. The caller holds
// eventAppendMu and eventMu, excluding replacement throughout the batch.
func (s *Storage) eventAppenderLocked() eventlog.Appender {
	return s.eventLog.entries
}
