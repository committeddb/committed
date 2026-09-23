package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// bindLegacyEventLog selects the native codec once per storage lifetime. The
// writer retains its append frontier; each reader gets an independent positioner.
func bindLegacyEventLog(log *tidwall.LegacyLog, m *metrics.Metrics) *eventLogBinding {
	decodeFrame := func(raw []byte) ([]byte, error) {
		payload, err := unframe(raw)
		if err != nil && m != nil {
			m.WalCorruptEntry("event_log")
		}
		return payload, err
	}
	writer := log.NewAppender(tidwall.LegacyCodec{
		Encode: func(r eventlog.Record) ([]byte, error) {
			if _, err := decodeEventEntry(r, nil); err != nil {
				return nil, err
			}
			return frame(r.Payload), nil
		},
		Decode: func(raw []byte) (eventlog.Record, error) {
			payload, err := decodeFrame(raw)
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
	return &eventLogBinding{native: log, compressor: log, records: func() eventlog.Cursor {
		return tidwall.NewLegacyLogCursor(log, func(raw []byte) (uint64, eventlog.Record, error) {
			payload, err := decodeFrame(raw)
			if err != nil {
				return 0, eventlog.Record{}, err
			}
			entry := new(pb.Entry)
			if err := proto.Unmarshal(payload, entry); err != nil {
				return 0, eventlog.Record{}, err
			}
			if entry.GetIndex() == 0 || entry.GetIndex() == ^uint64(0) {
				return 0, eventlog.Record{}, ErrCorruptEntry
			}
			return entry.GetIndex(), eventlog.Record{ID: entry.GetIndex(), Payload: payload}, nil
		})
	}, entries: &boundEntryStore{
		Appender: writer,
		cursor: func(index uint64) entryCursor {
			raw := legacyPositioner(log, decodeFrame)
			return &decodedEntryCursor{target: index, seek: raw.Seek, close: raw.Close}
		},
		reverse: func(limit int, visit func(*pb.Entry) (bool, error)) (int, error) {
			raw := legacyPositioner(log, decodeFrame)
			defer func() { _ = raw.Close() }()
			return raw.ScanReverse(limit, visit)
		},
		close: log.Close,
	}}
}
