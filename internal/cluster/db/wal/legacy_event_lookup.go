package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// legacyEventLookupLocked binds each lookup to the current native handle.
// The caller holds eventMu through lookup and application decoding.
func (s *Storage) legacyEventLookupLocked() eventlog.Lookup {
	return tidwall.NewLegacyLookup(s.eventLog, s.decodeLegacyEvent)
}

func (s *Storage) decodeLegacyEvent(raw []byte) (eventlog.Record, error) {
	payload, err := s.unframe(raw, "event_log")
	if err != nil {
		return eventlog.Record{}, err
	}
	entry := new(pb.Entry)
	if err := proto.Unmarshal(payload, entry); err != nil {
		return eventlog.Record{}, err
	}
	return eventlog.Record{ID: entry.GetIndex(), Payload: payload}, nil
}
