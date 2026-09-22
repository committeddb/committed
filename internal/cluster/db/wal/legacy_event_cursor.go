package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// newLegacyPositioner binds native positioning to the application-owned codec.
// The caller excludes replacement and close throughout its use.
func newLegacyPositioner(s *Storage) *tidwall.LegacyCursor[*pb.Entry] {
	return legacyPositioner(s.eventLog.native, func(raw []byte) ([]byte, error) { return s.unframe(raw, "event_log") })
}

func legacyPositioner(log *tidwall.LegacyLog, decodeFrame func([]byte) ([]byte, error)) *tidwall.LegacyCursor[*pb.Entry] {
	return tidwall.NewLegacyLogCursor(log, func(raw []byte) (uint64, *pb.Entry, error) {
		payload, err := decodeFrame(raw)
		if err != nil {
			return 0, nil, err
		}
		entry := new(pb.Entry)
		if err := proto.Unmarshal(payload, entry); err != nil {
			return 0, nil, err
		}
		return entry.GetIndex(), entry, nil
	})
}
