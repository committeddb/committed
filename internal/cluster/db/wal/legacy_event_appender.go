package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// eventAppenderLocked wires production event writes to the logical-record
// boundary. Reader/recovery/peer protocols still use the native handle. Scrub
// and fetch can replace it; bind a new writer before the first subsequent append.
// The caller holds eventAppendMu and eventMu (read or write).
func (s *Storage) eventAppenderLocked() eventlog.Appender {
	if s.eventAppenderSource != s.eventLog || s.eventAppender == nil {
		s.eventAppender = tidwall.NewLegacyAppender(s.eventLog, tidwall.LegacyCodec{
			Encode: func(r eventlog.Record) ([]byte, error) {
				if _, err := decodeEventEntry(r, nil); err != nil {
					return nil, err
				}
				return frame(r.Payload), nil
			},
			Decode: func(raw []byte) (eventlog.Record, error) {
				payload, err := s.unframe(raw, "event_log")
				if err != nil {
					return eventlog.Record{}, err
				}
				records, err := eventEntryRecords([][]byte{payload})
				if err != nil {
					return eventlog.Record{}, err
				}
				return records[0], nil
			},
		})
		s.eventAppenderSource = s.eventLog
	}
	return s.eventAppender
}
