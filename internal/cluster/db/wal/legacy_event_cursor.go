package wal

import (
	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// productionEventCursor binds a logical cursor to one native handle/generation.
// The caller holds the reader lock and eventMu throughout binding and seeking.
type productionEventCursor struct {
	entryCursor
	source     *native.Log
	generation uint64
}

func (r *Reader) eventCursorLocked() entryCursor {
	generation := r.s.scrubGen.Load()
	if r.cursor.entryCursor == nil || r.cursor.source != r.s.eventLog || r.cursor.generation != generation {
		if r.cursor.entryCursor != nil {
			_ = r.cursor.Close()
		}
		r.cursor = productionEventCursor{
			entryCursor: newLegacyEntryCursor(r.s, r.raftIndex+1),
			source:      r.s.eventLog,
			generation:  generation,
		}
	}
	return r.cursor.entryCursor
}

func newLegacyEntryCursor(s *Storage, index uint64) entryCursor {
	raw := newLegacyPositioner(s)
	return &decodedEntryCursor{target: index, seek: raw.Seek, close: raw.Close}
}

// newLegacyPositioner binds native positioning to the application-owned codec.
// The caller excludes replacement and close throughout its use.
func newLegacyPositioner(s *Storage) *tidwall.LegacyCursor[*pb.Entry] {
	return tidwall.NewLegacyCursor(s.eventLog, func(raw []byte) (uint64, *pb.Entry, error) {
		payload, err := s.unframe(raw, "event_log")
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
