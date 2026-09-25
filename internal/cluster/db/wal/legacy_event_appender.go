package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// fetchedEventAppenderLocked accepts already-validated native records. Encode
// is deliberately the identity function: transfer preserves the verified
// checksum envelope and protobuf bytes verbatim, including unknown fields.
// The caller holds eventAppendMu and eventMu and has checked payload/index pairs.
func (s *Storage) fetchedEventAppenderLocked() eventlog.Appender {
	return s.eventLog.native.NewAppender(tidwall.LegacyCodec{
		Encode: func(r eventlog.Record) ([]byte, error) { return r.Payload, nil },
		Decode: s.decodeLegacyAppendRecord,
	})
}

func (s *Storage) decodeLegacyAppendRecord(raw []byte) (eventlog.Record, error) {
	payload, err := s.unframe(raw, "event_log")
	if err != nil {
		return eventlog.Record{}, err
	}
	records, err := eventEntryRecords([][]byte{payload})
	if err != nil {
		return eventlog.Record{}, err
	}
	return records[0], nil
}
